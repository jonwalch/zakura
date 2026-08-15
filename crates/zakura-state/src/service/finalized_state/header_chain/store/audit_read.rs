use std::collections::HashMap;

use chrono::{DateTime, TimeZone, Utc};
use zakura_chain::{block, parameters::Network};
use zakura_header_chain::{
    AuxDelivery, EligibilityReason, EngineMetadata, EngineSnapshot, FinalityRecord, FinalitySource,
    Frontier, HeaderNode, StoreAuditRead, StoreError, ValidationContextRecord, ValidationLease,
};

use super::super::{
    FallibleDiskValue, FromDisk, FullStateBodyValidationEvidenceAuthorityDisk,
    HeaderAuxDeliveryKey, HeaderChainStoreError, HeaderChildKey, HeaderDeferredKey,
    HeaderEligibilityReasonDisk, HeaderEligibilityRootKey, HeaderHeightKey, HeaderNodeDisk,
    HeaderValidationContextDisk, IntoDisk, ReadDisk, HEADER_AUX_DELIVERY,
    HEADER_BODY_EVIDENCE_AUTHORITY, HEADER_CHILD, HEADER_CONSENSUS_INVALID_BODY_TOMBSTONE,
    HEADER_DEFERRED, HEADER_ELIGIBILITY_ROOT, HEADER_NODE_BY_HASH, HEADER_SELECTED,
    HEADER_VALIDATION_CONTEXT, HEADER_VERIFIED,
};
use super::{
    authenticated_context_headers, reason_evidence, reason_kind, store_error, HeaderChainStore,
};

impl HeaderChainStore {
    pub(crate) fn snapshot(&self) -> Result<EngineSnapshot, StoreError> {
        Ok(self.metadata()?.snapshot())
    }

    pub(crate) fn metadata(&self) -> Result<EngineMetadata, StoreError> {
        self.metadata_row()
            .map_err(store_error)?
            .ok_or(StoreError::Unavailable("header-chain metadata is absent"))
    }

    pub(in crate::service::finalized_state::header_chain) fn header_node(
        &self,
        hash: block::Hash,
    ) -> Result<Option<HeaderNode>, StoreError> {
        let value = self
            .get_value::<HeaderNodeDisk>(HEADER_NODE_BY_HASH, hash.0)
            .map_err(store_error)?;
        value
            .map(|value| {
                if value.hash != hash {
                    return Err(StoreError::Incoherent("node key/hash mismatch"));
                }
                let reasons = self.direct_reasons(hash).map_err(store_error)?;
                value
                    .into_domain(reasons)
                    .map_err(|_| StoreError::Incoherent("invalid durable node"))
            })
            .transpose()
    }

    pub(in crate::service::finalized_state::header_chain) fn selected_hash(
        &self,
        height: block::Height,
    ) -> Result<Option<block::Hash>, StoreError> {
        self.projection_hash(HEADER_SELECTED, height)
    }

    #[cfg(test)]
    pub(in crate::service::finalized_state::header_chain) fn verified_hash(
        &self,
        height: block::Height,
    ) -> Result<Option<block::Hash>, StoreError> {
        self.projection_hash(HEADER_VERIFIED, height)
    }

    pub(in crate::service::finalized_state::header_chain) fn validation_context(
        &self,
        parent: block::Hash,
        network: &Network,
    ) -> Result<ValidationLease, StoreError> {
        let metadata = self.metadata()?;
        let parent_node = self
            .header_node(parent)?
            .ok_or(StoreError::Incoherent("validation parent is not retained"))?;
        let parent_frontier = Frontier::new(parent_node.height, parent);
        let mut predecessors = vec![zakura_header_chain::HeaderContextFact {
            frontier: parent_frontier,
            header: parent_node.header.clone(),
        }];
        predecessors.extend(
            authenticated_context_headers(self, parent, None)?
                .into_iter()
                .rev()
                .map(|context| context.fact()),
        );
        Ok(ValidationLease::new(
            parent_frontier,
            predecessors,
            network.clone(),
            metadata.anchor_manifest_digest,
        ))
    }

    pub(in crate::service::finalized_state::header_chain) fn aux_deliveries(
        &self,
        hash: block::Hash,
    ) -> Result<Vec<zakura_header_chain::AuxDelivery>, StoreError> {
        let mut deliveries = Vec::new();
        for (key, value) in self
            .scan_prefix(HEADER_AUX_DELIVERY, &hash.0)
            .map_err(store_error)?
        {
            if key.len() != 64 {
                return Err(StoreError::Incoherent("invalid auxiliary key width"));
            }
            let delivery = AuxDelivery::decode(&value)
                .map_err(|_| StoreError::Incoherent("invalid auxiliary value"))?;
            if delivery.header_hash != hash || key[32..] != delivery.delivery_id.digest() {
                return Err(StoreError::Incoherent("auxiliary key/value mismatch"));
            }
            deliveries.push(delivery);
        }
        deliveries.sort_unstable_by_key(|delivery| delivery.delivery_id);
        Ok(deliveries)
    }

    pub(in crate::service::finalized_state::header_chain) fn is_migrated_finality_pin(
        &self,
        pin: Frontier,
    ) -> Result<bool, StoreError> {
        let mut found = false;
        self.visit_finality_records(&mut |record| {
            found |= record.current == pin
                && matches!(record.source, FinalitySource::MigratedHeadersOnly);
            Ok(())
        })?;
        Ok(found)
    }
}

impl StoreAuditRead for HeaderChainStore {
    fn snapshot(&self) -> Result<EngineSnapshot, StoreError> {
        HeaderChainStore::snapshot(self)
    }

    fn metadata(&self) -> Result<EngineMetadata, StoreError> {
        HeaderChainStore::metadata(self)
    }

    fn all_header_nodes(&self) -> Result<Vec<HeaderNode>, StoreError> {
        let mut reasons_by_hash: HashMap<block::Hash, Vec<EligibilityReason>> = HashMap::new();
        for (hash, reason) in self.all_reason_rows()? {
            reasons_by_hash.entry(hash).or_default().push(reason);
        }
        let mut nodes = Vec::new();
        for (key, value) in self.scan_raw(HEADER_NODE_BY_HASH).map_err(store_error)? {
            if key.len() != 32 {
                return Err(StoreError::Incoherent("invalid node key width"));
            }
            let hash = block::Hash(
                key.as_slice()
                    .try_into()
                    .map_err(|_| StoreError::Incoherent("invalid node hash key"))?,
            );
            let disk = HeaderNodeDisk::decode(&value)
                .map_err(|_| StoreError::Incoherent("invalid durable node value"))?;
            if disk.hash != hash {
                return Err(StoreError::Incoherent("node key/hash mismatch"));
            }
            let node = disk
                .into_domain(reasons_by_hash.remove(&hash).unwrap_or_default())
                .map_err(|_| StoreError::Incoherent("invalid durable node"))?;
            nodes.push(node);
        }
        if !reasons_by_hash.is_empty() {
            return Err(StoreError::Incoherent("eligibility root has no node"));
        }
        Ok(nodes)
    }

    fn all_consensus_invalid_body_tombstones(
        &self,
    ) -> Result<Vec<zakura_header_chain::ConsensusInvalidBodyTombstone>, StoreError> {
        let mut tombstones = Vec::new();
        for (key, value) in self
            .scan_raw(HEADER_CONSENSUS_INVALID_BODY_TOMBSTONE)
            .map_err(store_error)?
        {
            if key.len() != 32 {
                return Err(StoreError::Incoherent("invalid tombstone key width"));
            }
            let tombstone = zakura_header_chain::ConsensusInvalidBodyTombstone::decode(&value)
                .map_err(|_| StoreError::Incoherent("invalid tombstone value"))?;
            if key.as_slice() != tombstone.hash.0 {
                return Err(StoreError::Incoherent("tombstone key/hash mismatch"));
            }
            tombstones.push(tombstone);
        }
        Ok(tombstones)
    }

    fn full_state_attests_to_body_validation_state(
        &self,
        header_hash: block::Hash,
        body_validation_state: &zakura_header_chain::BodyValidationState,
    ) -> Result<bool, StoreError> {
        let authority = self
            .get_value::<FullStateBodyValidationEvidenceAuthorityDisk>(
                HEADER_BODY_EVIDENCE_AUTHORITY,
                header_hash.0,
            )
            .map_err(store_error)?;
        Ok(authority.is_some_and(|authority| {
            authority.attests_to_body_validation_state(header_hash, body_validation_state)
        }))
    }

    fn header_child_edges(&self) -> Result<Vec<(block::Hash, block::Hash)>, StoreError> {
        let mut edges = Vec::new();
        for (key, value) in self.scan_raw(HEADER_CHILD).map_err(store_error)? {
            if key.len() != 64 || !value.is_empty() {
                return Err(StoreError::Incoherent("invalid child-index row"));
            }
            let key = HeaderChildKey::from_bytes(&key);
            edges.push((key.parent, key.child));
        }
        Ok(edges)
    }

    fn selected_projection(&self) -> Result<Vec<Frontier>, StoreError> {
        self.projection_entries(HEADER_SELECTED)
    }

    fn verified_projection(&self) -> Result<Vec<Frontier>, StoreError> {
        self.projection_entries(HEADER_VERIFIED)
    }

    fn deferred_entries(&self) -> Result<Vec<(chrono::DateTime<Utc>, block::Hash)>, StoreError> {
        let mut entries = Vec::new();
        for (key, value) in self.scan_raw(HEADER_DEFERRED).map_err(store_error)? {
            if key.len() != 44 || !value.is_empty() {
                return Err(StoreError::Incoherent("invalid deferred-index row"));
            }
            let key = HeaderDeferredKey::try_from_bytes(&key)
                .map_err(|_| StoreError::Incoherent("invalid deferred-index key"))?;
            let until = Utc
                .timestamp_opt(key.seconds, key.nanoseconds)
                .single()
                .ok_or(StoreError::Incoherent("invalid deferred-index timestamp"))?;
            entries.push((until, key.hash));
        }
        Ok(entries)
    }

    fn eligibility_roots(&self) -> Result<Vec<(block::Hash, EligibilityReason)>, StoreError> {
        self.all_reason_rows()
    }

    fn all_aux_deliveries(&self) -> Result<Vec<AuxDelivery>, StoreError> {
        let mut deliveries = Vec::new();
        for (key, value) in self.scan_raw(HEADER_AUX_DELIVERY).map_err(store_error)? {
            if key.len() != 64 {
                return Err(StoreError::Incoherent("invalid auxiliary key width"));
            }
            let key = HeaderAuxDeliveryKey::from_bytes(&key);
            let delivery = AuxDelivery::decode(&value)
                .map_err(|_| StoreError::Incoherent("invalid auxiliary value"))?;
            if delivery.header_hash != key.header || delivery.delivery_id != key.delivery {
                return Err(StoreError::Incoherent("auxiliary key/value mismatch"));
            }
            deliveries.push(delivery);
        }
        Ok(deliveries)
    }

    fn validation_context_records(&self) -> Result<Vec<ValidationContextRecord>, StoreError> {
        let mut records = Vec::new();
        for (key, value) in self
            .scan_raw(HEADER_VALIDATION_CONTEXT)
            .map_err(store_error)?
        {
            if key.len() != 32 {
                return Err(StoreError::Incoherent(
                    "invalid validation-context key width",
                ));
            }
            let hash = block::Hash(
                key.as_slice()
                    .try_into()
                    .map_err(|_| StoreError::Incoherent("invalid validation-context key"))?,
            );
            let record = HeaderValidationContextDisk::decode(&value)
                .map_err(|_| StoreError::Incoherent("invalid validation-context value"))?;
            if record.header.hash() != hash {
                return Err(StoreError::Incoherent(
                    "validation-context key/hash mismatch",
                ));
            }
            records.push(ValidationContextRecord {
                header: record.header,
                height: record.height,
            });
        }
        Ok(records)
    }

    fn authenticated_canonical_hash(
        &self,
        height: block::Height,
    ) -> Result<Option<block::Hash>, StoreError> {
        let finalized = self.cf("hash_by_height").map_err(store_error)?;
        let hash: Option<block::Hash> = self.db.zs_get(&finalized, &height);
        if hash.is_some() {
            return Ok(hash);
        }
        let headers = self
            .cf("zakura_header_hash_by_height")
            .map_err(store_error)?;
        let hash: Option<block::Hash> = self.db.zs_get(&headers, &height);
        Ok(hash)
    }

    fn visit_finality_history(
        &self,
        visitor: &mut dyn FnMut(FinalityRecord) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        self.visit_finality_records(visitor)
    }
}

impl HeaderChainStore {
    pub(in crate::service::finalized_state::header_chain) fn earliest_deferred(
        &self,
    ) -> Result<Option<DateTime<Utc>>, StoreError> {
        let cf = self.cf(HEADER_DEFERRED).map_err(store_error)?;
        let Some((key, value)) = self
            .db
            .raw_first_cf(&cf)
            .map_err(HeaderChainStoreError::from)
            .map_err(store_error)?
        else {
            return Ok(None);
        };
        if key.len() != 44 || !value.is_empty() {
            return Err(StoreError::Incoherent("invalid deferred-index row"));
        }
        let key = HeaderDeferredKey::try_from_bytes(&key)
            .map_err(|_| StoreError::Incoherent("invalid deferred-index key"))?;
        Utc.timestamp_opt(key.seconds, key.nanoseconds)
            .single()
            .ok_or(StoreError::Incoherent("invalid deferred-index timestamp"))
            .map(Some)
    }

    pub(in crate::service::finalized_state::header_chain) fn all_reason_rows(
        &self,
    ) -> Result<Vec<(block::Hash, EligibilityReason)>, StoreError> {
        let mut reasons = Vec::new();
        for (key, value) in self
            .scan_raw(HEADER_ELIGIBILITY_ROOT)
            .map_err(store_error)?
        {
            let key = HeaderEligibilityRootKey::try_from_bytes(&key)
                .map_err(|_| StoreError::Incoherent("invalid eligibility-root key"))?;
            let reason = HeaderEligibilityReasonDisk::decode(&value)
                .map_err(|_| StoreError::Incoherent("invalid eligibility-root value"))?;
            let reason = reason.into_domain();
            if reason_kind(&reason) != key.kind || reason_evidence(&reason) != key.evidence {
                return Err(StoreError::Incoherent(
                    "eligibility-root key/value mismatch",
                ));
            }
            reasons.push((key.root, reason));
        }
        Ok(reasons)
    }

    pub(in crate::service::finalized_state::header_chain) fn projection_entries(
        &self,
        family: &'static str,
    ) -> Result<Vec<Frontier>, StoreError> {
        let mut projection = Vec::new();
        for (key, value) in self.scan_raw(family).map_err(store_error)? {
            if key.len() != 4 || value.len() != 32 {
                return Err(StoreError::Incoherent("invalid projection row width"));
            }
            let height = HeaderHeightKey::from_bytes(&key).0;
            let hash = block::Hash(
                value
                    .as_slice()
                    .try_into()
                    .map_err(|_| StoreError::Incoherent("invalid projection hash"))?,
            );
            projection.push(Frontier::new(height, hash));
        }
        projection.sort_unstable_by_key(|frontier| (frontier.height, frontier.hash.0));
        Ok(projection)
    }

    pub(in crate::service::finalized_state::header_chain) fn projection_hash(
        &self,
        family: &'static str,
        height: block::Height,
    ) -> Result<Option<block::Hash>, StoreError> {
        let cf = self.cf(family).map_err(store_error)?;
        let value = self
            .db
            .raw_get_cf(&cf, &HeaderHeightKey(height).as_bytes())
            .map_err(|_| StoreError::Unavailable("projection read failed"))?;
        value
            .map(|value| {
                value
                    .as_slice()
                    .try_into()
                    .map(block::Hash)
                    .map_err(|_| StoreError::Incoherent("invalid projection hash width"))
            })
            .transpose()
    }

    #[cfg(test)]
    pub(in crate::service::finalized_state::header_chain) fn projection_range(
        &self,
        family: &'static str,
        start: block::Height,
        end: block::Height,
    ) -> Result<Vec<Frontier>, HeaderChainStoreError> {
        if start > end {
            return Ok(Vec::new());
        }
        let lower = HeaderHeightKey(start).as_bytes();
        let upper = end
            .next()
            .ok()
            .map(|height| HeaderHeightKey(height).as_bytes());
        let rows = self.scan_range(family, &lower, upper.as_ref().map(AsRef::as_ref))?;
        let mut expected_height = start;
        let mut projection = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            if key.len() != 4 || value.len() != 32 {
                return Err(HeaderChainStoreError::Incoherent(
                    "invalid projection row width",
                ));
            }
            let height = HeaderHeightKey::from_bytes(&key).0;
            if height != expected_height {
                return Err(HeaderChainStoreError::Incoherent(
                    "projection range is not contiguous",
                ));
            }
            let hash =
                block::Hash(value.as_slice().try_into().map_err(|_| {
                    HeaderChainStoreError::Incoherent("invalid projection hash width")
                })?);
            projection.push(Frontier::new(height, hash));
            expected_height = height.next().unwrap_or(height);
        }
        if projection.last().map(|frontier| frontier.height) != Some(end) {
            return Err(HeaderChainStoreError::Incoherent(
                "projection range ended before the requested height",
            ));
        }
        Ok(projection)
    }
}
