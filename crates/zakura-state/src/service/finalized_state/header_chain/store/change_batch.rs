use zakura_chain::block;
use zakura_header_chain::{
    AuxDelta, ChangeSet, EligibilityReason, EngineMetadata, FinalityRecord, Frontier, HeaderNode,
    RecoveryPlan, RecoveryRepair, StoreError,
};
#[cfg(any(test, feature = "header-fuzz"))]
use zakura_header_chain::{EngineMode, EvidenceId, FinalitySource};

use super::super::{
    DiskWriteBatch, FallibleDiskValue, FullStateBodyValidationEvidenceAuthorityDisk,
    HeaderAuxDeliveryKey, HeaderChainStoreError, HeaderChainValueError, HeaderChildKey,
    HeaderDeferredKey, HeaderEligibilityReasonDisk, HeaderEligibilityRootKey, HeaderFinalityKey,
    HeaderHeightKey, HeaderNodeDisk, IntoDisk, RawBytes, RawVisitError, WriteDisk,
    HEADER_AUX_DELIVERY, HEADER_BODY_EVIDENCE_AUTHORITY, HEADER_CHILD,
    HEADER_CONSENSUS_INVALID_BODY_TOMBSTONE, HEADER_DEFERRED, HEADER_ELIGIBILITY_ROOT,
    HEADER_ENGINE_META, HEADER_FINALITY_HISTORY, HEADER_NODE_BY_HASH, HEADER_SELECTED,
    HEADER_VERIFIED, METADATA_KEY,
};
use super::{prefix_end, reason_evidence, reason_kind, store_error, HeaderChainStore};

impl HeaderChainStore {
    /// Bootstrap an empty header schema with one already-authenticated anchor.
    ///
    /// Migration calls this only before enabling publication and normal writers.
    #[cfg(any(test, feature = "header-fuzz"))]
    pub fn initialize(
        &self,
        metadata: EngineMetadata,
        anchor: HeaderNode,
    ) -> Result<(), HeaderChainStoreError> {
        let _writer = self
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?;
        if self.metadata_row()?.is_some() {
            return Err(HeaderChainStoreError::Incoherent(
                "header-chain metadata already exists",
            ));
        }
        if metadata.frontiers.finalized != Frontier::new(anchor.height, anchor.hash)
            || metadata.frontiers.header_best != metadata.frontiers.finalized
            || metadata.frontiers.verified_best != metadata.frontiers.finalized
            || metadata.state_version.get() == 0
        {
            return Err(HeaderChainStoreError::Incoherent(
                "initial metadata does not describe the anchor",
            ));
        }
        let change_set = ChangeSet {
            put_nodes: vec![anchor.clone()],
            delete_nodes: Vec::new(),
            put_consensus_invalid_body_tombstones: Vec::new(),
            index_changes: zakura_header_chain::IndexChanges {
                inserted: vec![metadata.frontiers.finalized],
                deleted: Vec::new(),
            },
            selected_projection: zakura_header_chain::ProjectionDelta {
                remove_before: None,
                remove_from: None,
                put: vec![metadata.frontiers.finalized],
            },
            verified_projection: zakura_header_chain::ProjectionDelta {
                remove_before: None,
                remove_from: None,
                put: vec![metadata.frontiers.finalized],
            },
            eligibility_changes: Vec::new(),
            aux_changes: Vec::new(),
            finality_append: Some(FinalityRecord {
                previous: metadata.work_origin,
                current: metadata.frontiers.finalized,
                source: match metadata.mode {
                    EngineMode::Integrated => FinalitySource::FullState {
                        evidence: EvidenceId::from_digest(metadata.anchor_manifest_digest),
                    },
                    EngineMode::HeadersOnly => FinalitySource::MigratedHeadersOnly,
                },
                epoch: metadata.finality_epoch,
            }),
            metadata: metadata.clone(),
        };
        self.db.write(self.batch_for(&change_set)?)?;
        Ok(())
    }

    pub(in crate::service::finalized_state::header_chain) fn batch_for(
        &self,
        changes: &ChangeSet,
    ) -> Result<DiskWriteBatch, HeaderChainStoreError> {
        self.batch_for_combined(changes, DiskWriteBatch::new())
    }

    pub(in crate::service::finalized_state::header_chain) fn batch_for_combined(
        &self,
        changes: &ChangeSet,
        mut batch: DiskWriteBatch,
    ) -> Result<DiskWriteBatch, HeaderChainStoreError> {
        let current_metadata = self.metadata_row()?;
        // Phase 1: update the finalized validation-context cache.
        self.enqueue_validation_context_update(
            &mut batch,
            current_metadata
                .as_ref()
                .map(|metadata| metadata.frontiers.finalized),
            changes.metadata.frontiers.finalized,
            &changes.put_nodes,
        )?;

        // Phase 2: encode authoritative nodes and their direct indexes.
        for hash in &changes.delete_nodes {
            if let Some(node) = self.header_node(*hash)? {
                self.delete_raw(&mut batch, HEADER_NODE_BY_HASH, hash.0)?;
                self.delete_raw(
                    &mut batch,
                    HEADER_CHILD,
                    HeaderChildKey {
                        parent: node.parent_hash,
                        child: *hash,
                    }
                    .as_bytes(),
                )?;
                self.delete_deferred_for(&mut batch, &node)?;
                self.delete_reason_rows(&mut batch, &node)?;
                if !matches!(
                    node.body_validation_state,
                    zakura_header_chain::BodyValidationState::ConsensusInvalid { .. }
                ) {
                    self.delete_raw(&mut batch, HEADER_BODY_EVIDENCE_AUTHORITY, hash.0)?;
                }
            }
            for (key, _) in self.scan_prefix(HEADER_CHILD, &hash.0)? {
                self.delete_raw(&mut batch, HEADER_CHILD, key)?;
            }
        }

        for node in &changes.put_nodes {
            if let Some(old) = self.header_node(node.hash)? {
                self.delete_deferred_for(&mut batch, &old)?;
                self.delete_reason_rows(&mut batch, &old)?;
            }
            self.put_value(
                &mut batch,
                HEADER_NODE_BY_HASH,
                node.hash.0,
                &HeaderNodeDisk::from_domain(node),
            )?;
            if let Some(authority) =
                FullStateBodyValidationEvidenceAuthorityDisk::from_body_validation_state(
                    node.hash,
                    &node.body_validation_state,
                )
            {
                self.put_value(
                    &mut batch,
                    HEADER_BODY_EVIDENCE_AUTHORITY,
                    node.hash.0,
                    &authority,
                )?;
            } else {
                self.delete_raw(&mut batch, HEADER_BODY_EVIDENCE_AUTHORITY, node.hash.0)?;
            }
            if node.hash != changes.metadata.frontiers.finalized.hash {
                self.put_empty(
                    &mut batch,
                    HEADER_CHILD,
                    HeaderChildKey {
                        parent: node.parent_hash,
                        child: node.hash,
                    }
                    .as_bytes(),
                )?;
            }
            if let zakura_header_chain::HeaderValidationState::DeferredUntil(until) =
                node.validation
            {
                let key = HeaderDeferredKey::new(
                    until.timestamp(),
                    until.timestamp_subsec_nanos(),
                    node.hash,
                )
                .map_err(|_| HeaderChainStoreError::Incoherent("invalid deferred timestamp"))?;
                self.put_empty(&mut batch, HEADER_DEFERRED, key.as_bytes())?;
            }
            for reason in &node.eligibility.direct_reasons {
                self.put_reason(&mut batch, node.hash, reason)?;
            }
        }

        // Phase 3: append immutable consensus-invalid body evidence.
        for tombstone in &changes.put_consensus_invalid_body_tombstones {
            if let Some(existing) = self
                .get_value::<zakura_header_chain::ConsensusInvalidBodyTombstone>(
                    HEADER_CONSENSUS_INVALID_BODY_TOMBSTONE,
                    tombstone.hash.0,
                )?
            {
                if existing != *tombstone {
                    return Err(HeaderChainStoreError::Incoherent(
                        "consensus-invalid tombstone changed",
                    ));
                }
                continue;
            }
            self.put_value(
                &mut batch,
                HEADER_CONSENSUS_INVALID_BODY_TOMBSTONE,
                tombstone.hash.0,
                tombstone,
            )?;
        }

        // Phase 4: update selected and verified projections.
        let selected_bounds = current_metadata.as_ref().map(|metadata| {
            (
                metadata.frontiers.finalized.height,
                metadata.frontiers.header_best.height,
            )
        });
        let verified_bounds = current_metadata.as_ref().map(|metadata| {
            (
                metadata.frontiers.finalized.height,
                metadata.frontiers.verified_best.height,
            )
        });
        self.apply_projection(
            &mut batch,
            HEADER_SELECTED,
            &changes.selected_projection,
            selected_bounds,
        )?;
        self.apply_projection(
            &mut batch,
            HEADER_VERIFIED,
            &changes.verified_projection,
            verified_bounds,
        )?;

        // Phase 5: update auxiliary deliveries.
        for delta in &changes.aux_changes {
            match delta {
                AuxDelta::Put(delivery) => self.put_value(
                    &mut batch,
                    HEADER_AUX_DELIVERY,
                    HeaderAuxDeliveryKey {
                        header: delivery.header_hash,
                        delivery: delivery.delivery_id,
                    }
                    .as_bytes(),
                    delivery.as_ref(),
                )?,
                AuxDelta::Delete {
                    header_hash,
                    delivery_id,
                } => self.delete_raw(
                    &mut batch,
                    HEADER_AUX_DELIVERY,
                    HeaderAuxDeliveryKey {
                        header: *header_hash,
                        delivery: *delivery_id,
                    }
                    .as_bytes(),
                )?,
            }
        }

        // Phase 6: append finality history.
        if let Some(record) = changes.finality_append {
            self.put_value(
                &mut batch,
                HEADER_FINALITY_HISTORY,
                HeaderFinalityKey(record.epoch).as_bytes(),
                &record,
            )?;
        }

        // Phase 7: enqueue metadata last as the logical commit marker.
        self.put_value(
            &mut batch,
            HEADER_ENGINE_META,
            METADATA_KEY,
            &changes.metadata,
        )?;
        Ok(batch)
    }

    pub(in crate::service::finalized_state::header_chain) fn recovery_batch(
        &self,
        plan: &RecoveryPlan,
    ) -> Result<DiskWriteBatch, HeaderChainStoreError> {
        let mut batch = DiskWriteBatch::new();
        if plan.repairs.contains(&RecoveryRepair::InheritedEligibility)
            || plan.repairs.contains(&RecoveryRepair::ElapsedDeferrals)
        {
            for node in &plan.header_nodes {
                self.put_value(
                    &mut batch,
                    HEADER_NODE_BY_HASH,
                    node.hash.0,
                    &HeaderNodeDisk::from_domain(node),
                )?;
            }
        }
        if plan.repairs.contains(&RecoveryRepair::ChildIndex) {
            self.clear_family(&mut batch, HEADER_CHILD)?;
            for (parent, child) in &plan.header_child_edges {
                self.put_empty(
                    &mut batch,
                    HEADER_CHILD,
                    HeaderChildKey {
                        parent: *parent,
                        child: *child,
                    }
                    .as_bytes(),
                )?;
            }
        }
        if plan.repairs.contains(&RecoveryRepair::DeferredIndex) {
            self.clear_family(&mut batch, HEADER_DEFERRED)?;
            for (until, hash) in &plan.deferred_entries {
                let key = HeaderDeferredKey::new(
                    until.timestamp(),
                    until.timestamp_subsec_nanos(),
                    *hash,
                )
                .map_err(|_| HeaderChainStoreError::Incoherent("invalid recovery timestamp"))?;
                self.put_empty(&mut batch, HEADER_DEFERRED, key.as_bytes())?;
            }
        }
        if plan.repairs.contains(&RecoveryRepair::SelectedProjection) {
            self.replace_projection(&mut batch, HEADER_SELECTED, &plan.selected_projection)?;
        }
        if plan.repairs.contains(&RecoveryRepair::VerifiedProjection) {
            self.replace_projection(&mut batch, HEADER_VERIFIED, &plan.verified_projection)?;
        }
        self.put_value(&mut batch, HEADER_ENGINE_META, METADATA_KEY, &plan.metadata)?;
        Ok(batch)
    }

    pub(in crate::service::finalized_state::header_chain) fn clear_family(
        &self,
        batch: &mut DiskWriteBatch,
        family: &'static str,
    ) -> Result<(), HeaderChainStoreError> {
        for (key, _) in self.scan_raw(family)? {
            self.delete_raw(batch, family, key)?;
        }
        Ok(())
    }

    pub(in crate::service::finalized_state::header_chain) fn replace_projection(
        &self,
        batch: &mut DiskWriteBatch,
        family: &'static str,
        projection: &[Frontier],
    ) -> Result<(), HeaderChainStoreError> {
        self.clear_family(batch, family)?;
        for frontier in projection {
            self.put_raw(
                batch,
                family,
                HeaderHeightKey(frontier.height).as_bytes(),
                frontier.hash.0,
            )?;
        }
        Ok(())
    }

    pub(in crate::service::finalized_state::header_chain) fn metadata_row(
        &self,
    ) -> Result<Option<EngineMetadata>, HeaderChainStoreError> {
        self.get_value::<EngineMetadata>(HEADER_ENGINE_META, METADATA_KEY)
    }

    pub(in crate::service::finalized_state::header_chain) fn direct_reasons(
        &self,
        hash: block::Hash,
    ) -> Result<Vec<EligibilityReason>, HeaderChainStoreError> {
        let mut reasons = Vec::new();
        for tag in 0..=4 {
            let mut prefix = Vec::with_capacity(33);
            prefix.push(tag);
            prefix.extend(hash.0);
            for (key, value) in self.scan_prefix(HEADER_ELIGIBILITY_ROOT, &prefix)? {
                if key.len() != 65 {
                    return Err(HeaderChainStoreError::Incoherent(
                        "invalid eligibility-root key width",
                    ));
                }
                let key = HeaderEligibilityRootKey::try_from_bytes(&key)
                    .map_err(|_| HeaderChainStoreError::Incoherent("invalid eligibility key"))?;
                let reason = HeaderEligibilityReasonDisk::decode(&value)?.into_domain();
                if reason_kind(&reason) != key.kind || reason_evidence(&reason) != key.evidence {
                    return Err(HeaderChainStoreError::Incoherent(
                        "eligibility key/value mismatch",
                    ));
                }
                reasons.push(reason);
            }
        }
        Ok(reasons)
    }

    pub(in crate::service::finalized_state::header_chain) fn delete_reason_rows(
        &self,
        batch: &mut DiskWriteBatch,
        node: &HeaderNode,
    ) -> Result<(), HeaderChainStoreError> {
        for reason in &node.eligibility.direct_reasons {
            let key = HeaderEligibilityRootKey {
                kind: reason_kind(reason),
                root: node.hash,
                evidence: reason_evidence(reason),
            };
            self.delete_raw(batch, HEADER_ELIGIBILITY_ROOT, key.as_bytes())?;
        }
        Ok(())
    }

    pub(in crate::service::finalized_state::header_chain) fn put_reason(
        &self,
        batch: &mut DiskWriteBatch,
        root: block::Hash,
        reason: &EligibilityReason,
    ) -> Result<(), HeaderChainStoreError> {
        let key = HeaderEligibilityRootKey {
            kind: reason_kind(reason),
            root,
            evidence: reason_evidence(reason),
        };
        self.put_value(
            batch,
            HEADER_ELIGIBILITY_ROOT,
            key.as_bytes(),
            &HeaderEligibilityReasonDisk::from_domain(reason),
        )
    }

    pub(in crate::service::finalized_state::header_chain) fn delete_deferred_for(
        &self,
        batch: &mut DiskWriteBatch,
        node: &HeaderNode,
    ) -> Result<(), HeaderChainStoreError> {
        if let zakura_header_chain::HeaderValidationState::DeferredUntil(until) = node.validation {
            let key = HeaderDeferredKey::new(
                until.timestamp(),
                until.timestamp_subsec_nanos(),
                node.hash,
            )
            .map_err(|_| HeaderChainStoreError::Incoherent("invalid deferred timestamp"))?;
            self.delete_raw(batch, HEADER_DEFERRED, key.as_bytes())?;
        }
        Ok(())
    }

    pub(in crate::service::finalized_state::header_chain) fn apply_projection(
        &self,
        batch: &mut DiskWriteBatch,
        family: &'static str,
        delta: &zakura_header_chain::ProjectionDelta,
        existing_bounds: Option<(block::Height, block::Height)>,
    ) -> Result<(), HeaderChainStoreError> {
        if delta.remove_before.is_some() || delta.remove_from.is_some() {
            let Some((first, last)) = existing_bounds else {
                return Err(HeaderChainStoreError::Incoherent(
                    "projection deletion has no existing bounds",
                ));
            };
            if first > last {
                return Err(HeaderChainStoreError::Incoherent(
                    "existing projection bounds are reversed",
                ));
            }
            if let Some(remove_before) = delta.remove_before {
                if remove_before < first {
                    return Err(HeaderChainStoreError::Incoherent(
                        "projection prefix deletion precedes its existing bounds",
                    ));
                }
                if remove_before > first {
                    let end = block::Height(remove_before.0 - 1).min(last);
                    self.delete_projection_rows(batch, family, first, end)?;
                }
            }
            if let Some(remove_from) = delta.remove_from {
                if remove_from < first {
                    return Err(HeaderChainStoreError::Incoherent(
                        "projection suffix deletion precedes its existing bounds",
                    ));
                }
                if remove_from <= last {
                    self.delete_projection_rows(batch, family, remove_from, last)?;
                }
            }
        }
        for frontier in &delta.put {
            self.put_raw(
                batch,
                family,
                HeaderHeightKey(frontier.height).as_bytes(),
                frontier.hash.0,
            )?;
        }
        Ok(())
    }

    pub(in crate::service::finalized_state::header_chain) fn delete_projection_rows(
        &self,
        batch: &mut DiskWriteBatch,
        family: &'static str,
        start: block::Height,
        end: block::Height,
    ) -> Result<(), HeaderChainStoreError> {
        let count = end
            .0
            .checked_sub(start.0)
            .and_then(|count| count.checked_add(1))
            .ok_or(HeaderChainStoreError::Incoherent(
                "projection deletion bounds are reversed",
            ))?;
        if usize::try_from(count)
            .ok()
            .is_none_or(|count| count > zakura_header_chain::MAX_NON_FINALIZED_NODES_V1)
        {
            return Err(HeaderChainStoreError::Incoherent(
                "projection deletion exceeds the retained-node bound",
            ));
        }
        for height in start.0..=end.0 {
            self.delete_raw(
                batch,
                family,
                HeaderHeightKey(block::Height(height)).as_bytes(),
            )?;
        }
        Ok(())
    }

    pub(in crate::service::finalized_state::header_chain) fn get_value<
        V: FallibleDiskValue<Error = HeaderChainValueError>,
    >(
        &self,
        family: &'static str,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<V>, HeaderChainStoreError> {
        let cf = self.cf(family)?;
        let value = self.db.raw_get_cf(&cf, key.as_ref())?;
        value
            .map(|value| V::decode(&value).map_err(Into::into))
            .transpose()
    }

    pub(in crate::service::finalized_state::header_chain) fn scan_raw(
        &self,
        family: &'static str,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, HeaderChainStoreError> {
        self.scan_range(family, &[], None)
    }

    pub(in crate::service::finalized_state::header_chain) fn scan_range(
        &self,
        family: &'static str,
        lower: &[u8],
        upper: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, HeaderChainStoreError> {
        let cf = self.cf(family)?;
        Ok(self.db.raw_range_cf(&cf, lower, upper)?)
    }

    pub(in crate::service::finalized_state::header_chain) fn scan_prefix(
        &self,
        family: &'static str,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, HeaderChainStoreError> {
        let cf = self.cf(family)?;
        let upper = prefix_end(prefix);
        Ok(self.db.raw_range_cf(&cf, prefix, upper.as_deref())?)
    }

    pub(in crate::service::finalized_state::header_chain) fn cf(
        &self,
        family: &'static str,
    ) -> Result<rocksdb::ColumnFamilyRef<'_>, HeaderChainStoreError> {
        self.db
            .cf_handle(family)
            .ok_or(HeaderChainStoreError::Uninitialized)
    }

    pub(in crate::service::finalized_state::header_chain) fn put_value<
        V: FallibleDiskValue<Error = HeaderChainValueError>,
    >(
        &self,
        batch: &mut DiskWriteBatch,
        family: &'static str,
        key: impl AsRef<[u8]>,
        value: &V,
    ) -> Result<(), HeaderChainStoreError> {
        self.put_raw(batch, family, key, value.encode()?)
    }

    pub(in crate::service::finalized_state::header_chain) fn put_empty(
        &self,
        batch: &mut DiskWriteBatch,
        family: &'static str,
        key: impl AsRef<[u8]>,
    ) -> Result<(), HeaderChainStoreError> {
        self.put_raw(batch, family, key, [])
    }

    pub(in crate::service::finalized_state::header_chain) fn put_raw(
        &self,
        batch: &mut DiskWriteBatch,
        family: &'static str,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<(), HeaderChainStoreError> {
        let cf = self.cf(family)?;
        batch.zs_insert(
            &cf,
            RawBytes::new_raw_bytes(key.as_ref().to_vec()),
            RawBytes::new_raw_bytes(value.as_ref().to_vec()),
        );
        Ok(())
    }

    pub(in crate::service::finalized_state::header_chain) fn delete_raw(
        &self,
        batch: &mut DiskWriteBatch,
        family: &'static str,
        key: impl AsRef<[u8]>,
    ) -> Result<(), HeaderChainStoreError> {
        let cf = self.cf(family)?;
        batch.zs_delete(&cf, RawBytes::new_raw_bytes(key.as_ref().to_vec()));
        Ok(())
    }

    pub(in crate::service::finalized_state::header_chain) fn visit_finality_records(
        &self,
        visitor: &mut dyn FnMut(FinalityRecord) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        let cf = self.cf(HEADER_FINALITY_HISTORY).map_err(store_error)?;
        self.db
            .raw_visit_cf(&cf, &mut |key, value| {
                if key.len() != 8 {
                    return Err(StoreError::Incoherent("invalid finality key width"));
                }
                let record = FinalityRecord::decode(value)
                    .map_err(|_| StoreError::Incoherent("invalid finality value"))?;
                if key != record.epoch.get().to_be_bytes() {
                    return Err(StoreError::Incoherent("finality key/value mismatch"));
                }
                visitor(record)?;
                Ok(())
            })
            .map_err(|error| match error {
                RawVisitError::RocksDb(error) => {
                    tracing::warn!(?error, "finality history iterator failed");
                    StoreError::Unavailable("finality history iterator failed")
                }
                RawVisitError::Visitor(error) => error,
            })
    }

    #[cfg(any(test, feature = "header-fuzz"))]
    pub(in crate::service::finalized_state::header_chain) fn finality_history(
        &self,
    ) -> Result<Vec<FinalityRecord>, StoreError> {
        let mut records = Vec::new();
        self.visit_finality_records(&mut |record| {
            records.push(record);
            Ok(())
        })?;
        Ok(records)
    }

    pub(in crate::service::finalized_state::header_chain) fn finality_rebase_history(
        &self,
        original_anchor: block::Hash,
        current_finalized: Frontier,
        max_records: u64,
    ) -> Result<Vec<FinalityRecord>, StoreError> {
        if original_anchor == current_finalized.hash {
            return Ok(Vec::new());
        }
        if max_records == 0 {
            return Ok(Vec::new());
        }

        let metadata = self.metadata()?;
        if metadata.frontiers.finalized != current_finalized {
            return Err(StoreError::Incoherent(
                "finality rebase frontier disagrees with durable metadata",
            ));
        }

        let mut reverse_path = Vec::new();
        let mut expected_current = current_finalized;
        let mut epoch = metadata.finality_epoch.get();
        for _ in 0..max_records {
            let key = HeaderFinalityKey(zakura_header_chain::FinalityEpoch::new(epoch));
            let Some(record) = self
                .get_value::<FinalityRecord>(HEADER_FINALITY_HISTORY, key.as_bytes())
                .map_err(store_error)?
            else {
                if epoch == 0 {
                    break;
                }
                return Err(StoreError::Incoherent(
                    "finality rebase history has a missing epoch",
                ));
            };
            if record.epoch.get() != epoch || record.current != expected_current {
                return Err(StoreError::Incoherent(
                    "finality rebase history is not contiguous",
                ));
            }
            reverse_path.push(record);
            if record.previous.hash == original_anchor {
                reverse_path.reverse();
                return Ok(reverse_path);
            }
            expected_current = record.previous;
            let Some(previous_epoch) = epoch.checked_sub(1) else {
                break;
            };
            epoch = previous_epoch;
        }
        Ok(Vec::new())
    }
}
