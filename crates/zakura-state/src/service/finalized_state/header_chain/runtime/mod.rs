use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use zakura_chain::block;
use zakura_header_chain::{
    EligibilityReason, EngineConfig, EvidenceId, Frontier, HeaderChainEngine, MemHeaderStore,
    StoreAuditRead, VerifiedHeaderRef,
};

use super::{
    CapturedSelectedProjection, HeaderChainReader, HeaderChainSnapshotPublisher, HeaderChainStore,
    HeaderChainStoreError, RetainedPathLeaseRegistry, SelectedAuxiliaryWindow,
    SelectedHeaderWithAuxiliaryDeliveries,
};

mod transition_commit;

pub(in crate::service::finalized_state::header_chain) use transition_commit::coherent_engine_aux_deliveries;
#[cfg(test)]
pub(in crate::service::finalized_state::header_chain) use transition_commit::combined_retention_references;

/// An audited durable store paired with its only production snapshot publisher.
///
/// Coherent operations acquire locks in the global order `writer -> transition_engine -> leases`
/// whenever more than one is held. Holding `writer` makes durable rows and the committed engine
/// one generation boundary; publication happens only after both represent that generation.
#[derive(Clone, Debug)]
pub struct HeaderChainRuntime {
    pub(in crate::service::finalized_state::header_chain) store: HeaderChainStore,
    pub(in crate::service::finalized_state::header_chain) config: EngineConfig,
    pub(in crate::service::finalized_state::header_chain) publisher: HeaderChainSnapshotPublisher,
    pub(in crate::service::finalized_state::header_chain) leases:
        Arc<Mutex<RetainedPathLeaseRegistry>>,
    pub(in crate::service::finalized_state::header_chain) transition_engine:
        Arc<Mutex<HeaderChainEngine>>,
}

#[derive(Copy, Clone)]
struct FullStateProjectionExpectation<'a> {
    verified: Option<Frontier>,
    staged: &'a [VerifiedHeaderRef],
}

impl FullStateProjectionExpectation<'_> {
    const NONE: Self = Self {
        verified: None,
        staged: &[],
    };
}

pub(in crate::service::finalized_state::header_chain) fn load_transition_engine(
    store: &HeaderChainStore,
) -> Result<HeaderChainEngine, HeaderChainStoreError> {
    let metadata = store.metadata()?;
    let graph = MemHeaderStore::reconstruct(zakura_header_chain::HeaderGraphReconstruction::new(
        metadata.frontiers.finalized,
        store.all_header_nodes()?,
        store.all_consensus_invalid_body_tombstones()?,
    ))
    .map_err(|_| HeaderChainStoreError::Incoherent("audited node graph is invalid"))?;
    HeaderChainEngine::from_audited_state(
        graph,
        metadata,
        store.selected_projection()?,
        store.verified_projection()?,
        store.all_aux_deliveries()?,
    )
    .map_err(|_| HeaderChainStoreError::Incoherent("audited engine state is invalid"))
}

pub(in crate::service::finalized_state::header_chain) fn restore_transition_engine_after_staging_error(
    store: &HeaderChainStore,
    engine: &mut HeaderChainEngine,
    original: HeaderChainStoreError,
) -> HeaderChainStoreError {
    match load_transition_engine(store) {
        Ok(restored) => {
            *engine = restored;
            original
        }
        Err(reload) => {
            tracing::error!(
                ?original,
                ?reload,
                "failed to restore the durable header engine after a staged transition error"
            );
            reload
        }
    }
}

impl HeaderChainRuntime {
    /// Captures the complete selected projection and the engine snapshot that produced it.
    ///
    /// The method rejects an engine whose projection bounds disagree with its snapshot.
    pub(crate) fn capture_selected_projection(
        &self,
    ) -> Result<CapturedSelectedProjection, HeaderChainStoreError> {
        let engine = self
            .transition_engine
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?;
        let engine_snapshot = engine.snapshot();
        let frontiers = engine.selected_projection().to_vec();
        if frontiers.first().copied() != Some(engine_snapshot.frontiers.finalized)
            || frontiers.last().copied() != Some(engine_snapshot.frontiers.header_best)
        {
            return Err(HeaderChainStoreError::Incoherent(
                "selected projection disagrees with its published bounds",
            ));
        }
        Ok(CapturedSelectedProjection {
            engine_snapshot,
            frontiers,
        })
    }

    /// Reads one auxiliary window from the current in-memory selected projection.
    ///
    /// The method returns `None` when the selected projection does not contain the requested
    /// height and hash.
    pub(crate) fn committed_selected_auxiliary_window(
        &self,
        height: block::Height,
        hash: block::Hash,
    ) -> Result<Option<SelectedAuxiliaryWindow>, HeaderChainStoreError> {
        let engine = self
            .transition_engine
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?;
        let Ok(index) = engine
            .selected_projection()
            .binary_search_by_key(&height, |frontier| frontier.height)
        else {
            return Ok(None);
        };
        Self::committed_selected_auxiliary_window_at_projection_index_locked(
            &engine,
            index,
            Frontier::new(height, hash),
        )
    }

    /// Reads one auxiliary window at an index from a previously captured selected projection.
    ///
    /// The method returns `None` when the live projection no longer contains the expected
    /// frontier at `projection_index`. The returned window carries a new engine snapshot so the
    /// caller can detect any generation change.
    pub(crate) fn committed_selected_auxiliary_window_at_projection_index(
        &self,
        projection_index: usize,
        expected_frontier: Frontier,
    ) -> Result<Option<SelectedAuxiliaryWindow>, HeaderChainStoreError> {
        let engine = self
            .transition_engine
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?;
        Self::committed_selected_auxiliary_window_at_projection_index_locked(
            &engine,
            projection_index,
            expected_frontier,
        )
    }

    pub(in crate::service::finalized_state::header_chain) fn committed_selected_auxiliary_window_at_projection_index_locked(
        engine: &HeaderChainEngine,
        projection_index: usize,
        expected_frontier: Frontier,
    ) -> Result<Option<SelectedAuxiliaryWindow>, HeaderChainStoreError> {
        let Some(current_frontier) = engine.selected_projection().get(projection_index).copied()
        else {
            return Ok(None);
        };
        if current_frontier != expected_frontier {
            return Ok(None);
        }
        let delivery_header_node = engine
            .graph()
            .header_node(expected_frontier.hash)
            .cloned()
            .ok_or(HeaderChainStoreError::Incoherent(
                "selected projection references a missing in-memory node",
            ))?;
        if delivery_header_node.height != expected_frontier.height
            || delivery_header_node.hash != expected_frontier.hash
        {
            return Err(HeaderChainStoreError::Incoherent(
                "selected projection disagrees with its in-memory node",
            ));
        }
        let delivery_auxiliary_deliveries =
            coherent_engine_aux_deliveries(engine, &delivery_header_node)?;
        let successor_header = if let Some(successor_frontier) =
            engine.selected_projection().get(projection_index + 1)
        {
            let expected_successor_height = expected_frontier.height.next().map_err(|_| {
                HeaderChainStoreError::Incoherent("selected auxiliary successor height overflowed")
            })?;
            let successor_header_node = engine
                .graph()
                .header_node(successor_frontier.hash)
                .cloned()
                .ok_or(HeaderChainStoreError::Incoherent(
                    "selected successor references a missing in-memory node",
                ))?;
            if successor_frontier.height != expected_successor_height
                || successor_header_node.height != expected_successor_height
                || successor_header_node.hash != successor_frontier.hash
                || successor_header_node.parent_hash != expected_frontier.hash
            {
                return Err(HeaderChainStoreError::Incoherent(
                    "selected in-memory successor is not contiguous",
                ));
            }
            let successor_auxiliary_deliveries =
                coherent_engine_aux_deliveries(engine, &successor_header_node)?;
            Some(SelectedHeaderWithAuxiliaryDeliveries {
                header_node: successor_header_node,
                auxiliary_deliveries: successor_auxiliary_deliveries,
            })
        } else {
            None
        };
        Ok(Some(SelectedAuxiliaryWindow {
            engine_snapshot: engine.snapshot(),
            delivery_header: SelectedHeaderWithAuxiliaryDeliveries {
                header_node: delivery_header_node,
                auxiliary_deliveries: delivery_auxiliary_deliveries,
            },
            successor_header,
        }))
    }

    pub(in crate::service) fn operator_invalidation_evidence(
        &self,
        target: block::Hash,
        id: zakura_header_chain::OperatorInvalidationId,
    ) -> Result<Option<EvidenceId>, HeaderChainStoreError> {
        let engine = self
            .transition_engine
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?;
        Ok(engine.graph().header_node(target).and_then(|node| {
            node.eligibility
                .direct_reasons
                .iter()
                .find_map(|reason| match reason {
                    EligibilityReason::OperatorInvalid {
                        id: existing,
                        evidence,
                        ..
                    } if *existing == id => Some(*evidence),
                    _ => None,
                })
        }))
    }

    /// Return the sole committed-snapshot publisher.
    pub fn publisher(&self) -> &HeaderChainSnapshotPublisher {
        &self.publisher
    }

    /// Return a read-only handle whose compound reads share the transition lock.
    pub(crate) fn reader(&self) -> HeaderChainReader {
        HeaderChainReader {
            store: self.store.clone(),
            config: Arc::new(self.config.clone()),
            leases: self.leases.clone(),
            transition_engine: self.transition_engine.clone(),
        }
    }

    /// Read the exact durable verified projection used to prove full-state finality.
    pub(in crate::service) fn verified_projection(
        &self,
    ) -> Result<Vec<Frontier>, HeaderChainStoreError> {
        self.store
            .verified_projection()
            .map_err(HeaderChainStoreError::Store)
    }

    /// Return the earliest durable deferred-header deadline.
    pub(in crate::service) fn earliest_deferred(
        &self,
    ) -> Result<Option<DateTime<Utc>>, HeaderChainStoreError> {
        self.store
            .earliest_deferred()
            .map_err(HeaderChainStoreError::Store)
    }
}
