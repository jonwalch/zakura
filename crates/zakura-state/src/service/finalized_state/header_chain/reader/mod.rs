use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

use zakura_chain::{block, parallel::commitment_aux::BlockCommitmentRoots};
use zakura_header_chain::{
    AuxDelivery, BodyWorkAuthority, BodyWorkOwner, EngineConfig, EvidenceId, Frontier,
    HeaderChainEngine, HeaderLocator, HeaderNode, StoreError, ValidationLease,
};

use super::{
    select_vct_auxiliary_delivery, HeaderChainStore, HeaderChainStoreError, HeaderNodeDisk,
    ReadDisk, HEADER_NODE_BY_HASH,
};
#[cfg(test)]
use super::{SelectedAuxiliaryWindow, SelectedHeaderWithAuxiliaryDeliveries, HEADER_SELECTED};

mod retained_path;

pub(in crate::service::finalized_state::header_chain) use retained_path::RetainedPathLeaseRegistry;

/// Read-only coherent queries serialized against durable header transitions.
#[derive(Clone, Debug)]
pub(crate) struct HeaderChainReader {
    pub(in crate::service::finalized_state::header_chain) store: HeaderChainStore,
    pub(in crate::service::finalized_state::header_chain) config: Arc<EngineConfig>,
    pub(in crate::service::finalized_state::header_chain) leases:
        Arc<Mutex<RetainedPathLeaseRegistry>>,
    pub(in crate::service::finalized_state::header_chain) transition_engine:
        Arc<Mutex<HeaderChainEngine>>,
}

impl HeaderChainReader {
    fn coherent_selected_node(
        &self,
        height: block::Height,
    ) -> Result<Option<HeaderNode>, StoreError> {
        let snapshot = self.store.snapshot()?;
        let selected_hash = self.store.selected_hash(height)?;
        if height < snapshot.frontiers.finalized.height
            || height > snapshot.frontiers.header_best.height
        {
            if selected_hash.is_some() {
                return Err(StoreError::Incoherent(
                    "selected projection contains a row outside its published bounds",
                ));
            }
            return Ok(None);
        }
        let Some(hash) = selected_hash else {
            if height >= snapshot.frontiers.finalized.height
                && height <= snapshot.frontiers.header_best.height
            {
                return Err(StoreError::Incoherent(
                    "selected projection has a gap within its published bounds",
                ));
            }
            return Ok(None);
        };
        let indexed_node = self.store.header_node(hash)?.ok_or(StoreError::Incoherent(
            "selected projection references a missing node",
        ))?;
        if indexed_node.height != height {
            return Err(StoreError::Incoherent(
                "selected projection node height disagrees with its index",
            ));
        }

        let finalized = snapshot.frontiers.finalized;
        if height == finalized.height {
            if hash != finalized.hash {
                return Err(StoreError::Incoherent(
                    "selected projection disagrees with the committed finalized frontier",
                ));
            }
            return Ok(Some(indexed_node));
        }

        let tip = snapshot.frontiers.header_best;
        let mut selected_ancestor =
            self.store
                .header_node(tip.hash)?
                .ok_or(StoreError::Incoherent(
                    "committed selected tip references a missing node",
                ))?;
        if selected_ancestor.height != tip.height {
            return Err(StoreError::Incoherent(
                "committed selected tip height disagrees with its node",
            ));
        }
        while selected_ancestor.height > height {
            let parent_height = block::Height(selected_ancestor.height.0.checked_sub(1).ok_or(
                StoreError::Incoherent("selected path reached a parent below height zero"),
            )?);
            let parent = self
                .store
                .header_node(selected_ancestor.parent_hash)?
                .ok_or(StoreError::Incoherent(
                    "selected path references a missing parent node",
                ))?;
            if parent.height != parent_height {
                return Err(StoreError::Incoherent(
                    "selected path parent height is not contiguous",
                ));
            }
            selected_ancestor = parent;
        }
        if selected_ancestor.height != height || selected_ancestor.hash != hash {
            return Err(StoreError::Incoherent(
                "selected projection node is not on the committed selected path",
            ));
        }
        Ok(Some(indexed_node))
    }

    fn coherent_aux_deliveries(
        &self,
        node: &HeaderNode,
    ) -> Result<Vec<AuxDelivery>, HeaderChainStoreError> {
        self.coherent_aux_deliveries_for(node.hash, &node.aux_delivery_ids)
    }

    fn coherent_aux_deliveries_for(
        &self,
        hash: block::Hash,
        aux_delivery_ids: &[EvidenceId],
    ) -> Result<Vec<AuxDelivery>, HeaderChainStoreError> {
        let deliveries = self.store.aux_deliveries(hash)?;
        let indexed: BTreeSet<_> = aux_delivery_ids.iter().copied().collect();
        let stored: BTreeSet<_> = deliveries
            .iter()
            .map(|delivery| delivery.delivery_id)
            .collect();
        if indexed.len() != aux_delivery_ids.len()
            || stored.len() != deliveries.len()
            || indexed != stored
        {
            return Err(HeaderChainStoreError::Store(StoreError::Incoherent(
                "retained node and auxiliary delivery index disagree",
            )));
        }
        Ok(deliveries)
    }

    fn retained_path_node(
        &self,
        hash: block::Hash,
    ) -> Result<Option<HeaderNodeDisk>, HeaderChainStoreError> {
        let Some(node) = self
            .store
            .get_value::<HeaderNodeDisk>(HEADER_NODE_BY_HASH, hash.0)?
        else {
            return Ok(None);
        };
        if node.hash != hash
            || node.header.hash() != hash
            || node.header.previous_block_hash != node.parent_hash
        {
            return Err(HeaderChainStoreError::Incoherent(
                "retained path node key and header fields disagree",
            ));
        }
        Ok(Some(node))
    }

    fn finalized_frontier(
        &self,
        hash: block::Hash,
    ) -> Result<Option<Frontier>, HeaderChainStoreError> {
        let height_by_hash = self.store.cf("height_by_hash")?;
        let height: Option<block::Height> = self.store.db.zs_get(&height_by_hash, &hash);
        let Some(height) = height else {
            return Ok(None);
        };
        let hash_by_height = self.store.cf("hash_by_height")?;
        let canonical_hash: Option<block::Hash> = self.store.db.zs_get(&hash_by_height, &height);
        if canonical_hash != Some(hash) {
            return Err(StoreError::Incoherent("finalized height/hash indexes disagree").into());
        }
        Ok(Some(Frontier::new(height, hash)))
    }

    fn finalized_header(
        &self,
        frontier: Frontier,
    ) -> Result<Arc<block::Header>, HeaderChainStoreError> {
        let block_header_by_height = self.store.cf("block_header_by_height")?;
        let header: Option<Arc<block::Header>> = self
            .store
            .db
            .zs_get(&block_header_by_height, &frontier.height);
        let header = header.ok_or(StoreError::Incoherent(
            "finalized header path has a missing header",
        ))?;
        if header.hash() != frontier.hash {
            return Err(StoreError::Incoherent(
                "finalized header disagrees with its canonical hash index",
            )
            .into());
        }
        Ok(header)
    }

    fn selected_aux_delivery(
        &self,
        node: &HeaderNode,
    ) -> Result<Option<AuxDelivery>, HeaderChainStoreError> {
        Ok(select_vct_auxiliary_delivery(
            self.coherent_aux_deliveries(node)?,
        ))
    }

    /// Return the contiguous selected-path auxiliary roots starting at `start`.
    ///
    /// A missing usable delivery ends the result without error, so a successful short read means
    /// only the returned prefix is currently authenticated. Durable gaps or contradictions fail.
    pub(crate) fn selected_block_roots(
        &self,
        start: block::Height,
        count: u32,
    ) -> Result<Vec<BlockCommitmentRoots>, HeaderChainStoreError> {
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?;
        if count == 0 {
            return Ok(Vec::new());
        }

        let snapshot = self.store.snapshot()?;
        if start < snapshot.frontiers.finalized.height
            || start > snapshot.frontiers.header_best.height
        {
            self.coherent_selected_node(start)?;
            return Ok(Vec::new());
        }
        let requested_end = block::Height(start.0.saturating_add(count.saturating_sub(1)));
        let end = requested_end.min(snapshot.frontiers.header_best.height);
        let mut selected = self
            .store
            .header_node(snapshot.frontiers.header_best.hash)?
            .ok_or(StoreError::Incoherent(
                "committed selected tip references a missing node",
            ))?;
        if selected.height != snapshot.frontiers.header_best.height {
            return Err(StoreError::Incoherent(
                "committed selected tip height disagrees with its node",
            )
            .into());
        }
        let mut selected_nodes = Vec::new();
        loop {
            if selected.height <= end {
                let projected_hash =
                    self.store
                        .selected_hash(selected.height)?
                        .ok_or(StoreError::Incoherent(
                            "selected projection has a gap within its published bounds",
                        ))?;
                if projected_hash != selected.hash {
                    return Err(StoreError::Incoherent(
                        "selected projection node is not on the committed selected path",
                    )
                    .into());
                }
                selected_nodes.push(selected.clone());
            }
            if selected.height == start {
                break;
            }
            let parent_height = block::Height(selected.height.0.checked_sub(1).ok_or(
                StoreError::Incoherent("selected path reached a parent below height zero"),
            )?);
            let parent =
                self.store
                    .header_node(selected.parent_hash)?
                    .ok_or(StoreError::Incoherent(
                        "selected path references a missing parent node",
                    ))?;
            if parent.height != parent_height {
                return Err(StoreError::Incoherent(
                    "selected path parent height is not contiguous",
                )
                .into());
            }
            selected = parent;
        }
        selected_nodes.reverse();

        let mut roots = Vec::new();
        for node in selected_nodes {
            let height = node.height;
            let hash = node.hash;
            let Some(delivery) = self.selected_aux_delivery(&node)? else {
                break;
            };
            let Some(aux) = delivery.tree_aux else {
                break;
            };
            if delivery.header_hash != hash || aux.height != height {
                return Err(StoreError::Incoherent(
                    "selected auxiliary root delivery disagrees with its header",
                )
                .into());
            }
            roots.push(BlockCommitmentRoots {
                height,
                sapling_root: aux.sapling_root,
                orchard_root: aux.orchard_root,
                ironwood_root: aux.ironwood_root,
                sapling_tx: aux.sapling_tx_count,
                orchard_tx: aux.orchard_tx_count,
                ironwood_tx: aux.ironwood_tx_count,
                auth_data_root: aux.auth_data_root,
            });
        }
        Ok(roots)
    }

    pub(crate) fn validation_context(
        &self,
        parent_hash: block::Hash,
    ) -> Result<Option<ValidationLease>, HeaderChainStoreError> {
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?;
        if self.store.header_node(parent_hash)?.is_none() {
            return Ok(None);
        }
        self.store
            .validation_context(parent_hash, &self.config.network)
            .map(Some)
            .map_err(HeaderChainStoreError::Store)
    }

    pub(crate) fn selected_header_tip(&self) -> Result<Frontier, HeaderChainStoreError> {
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?;
        Ok(self.store.snapshot()?.frontiers.header_best)
    }

    /// Capture full-state data and the durable selected projection from one transition generation.
    pub(crate) fn with_selected_projection<T>(
        &self,
        read_full_state: impl FnOnce() -> T,
    ) -> Result<(T, Vec<Frontier>), HeaderChainStoreError> {
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?;
        let engine = self
            .transition_engine
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?;
        let full_state = read_full_state();
        let snapshot = engine.snapshot();
        let projection = engine.selected_projection().to_vec();
        if projection.first().copied() != Some(snapshot.frontiers.finalized)
            || projection.last().copied() != Some(snapshot.frontiers.header_best)
        {
            return Err(StoreError::Incoherent(
                "selected projection disagrees with its published bounds",
            )
            .into());
        }
        Ok((full_state, projection))
    }

    #[cfg(test)]
    pub(crate) fn selected_hash(
        &self,
        height: block::Height,
    ) -> Result<Option<block::Hash>, HeaderChainStoreError> {
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?;
        self.coherent_selected_node(height)
            .map(|node| node.map(|node| node.hash))
            .map_err(HeaderChainStoreError::Store)
    }

    #[cfg(test)]
    pub(crate) fn selected_frontiers(
        &self,
        start: block::Height,
        count: u32,
    ) -> Result<Vec<Frontier>, HeaderChainStoreError> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?;
        let selected_tip = self.store.snapshot()?.frontiers.header_best;
        if start > selected_tip.height {
            return Ok(Vec::new());
        }
        let end = block::Height(
            start
                .0
                .saturating_add(count.saturating_sub(1))
                .min(selected_tip.height.0),
        );
        self.store.projection_range(HEADER_SELECTED, start, end)
    }

    #[cfg(test)]
    pub(crate) fn selected_successor(
        &self,
        height: block::Height,
        hash: block::Hash,
    ) -> Result<Option<HeaderNode>, HeaderChainStoreError> {
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?;
        if self
            .coherent_selected_node(height)?
            .is_none_or(|node| node.hash != hash)
        {
            return Ok(None);
        }
        let Ok(successor_height) = height.next() else {
            return Ok(None);
        };
        let Some(successor) = self.coherent_selected_node(successor_height)? else {
            return Ok(None);
        };
        if successor.parent_hash != hash {
            return Err(StoreError::Incoherent(
                "selected successor does not extend its selected predecessor",
            )
            .into());
        }
        Ok(Some(successor))
    }

    /// Reads one selected header and its direct successor under the writer lock.
    ///
    /// The writer lock prevents a concurrent transition from mixing projection entries with
    /// auxiliary deliveries from different engine versions.
    #[cfg(test)]
    pub(crate) fn durable_selected_auxiliary_window(
        &self,
        height: block::Height,
        hash: block::Hash,
    ) -> Result<Option<SelectedAuxiliaryWindow>, HeaderChainStoreError> {
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?;
        let Some(delivery_header_node) = self.coherent_selected_node(height)? else {
            return Ok(None);
        };
        if delivery_header_node.hash != hash {
            return Ok(None);
        }
        let delivery_auxiliary_deliveries = self.coherent_aux_deliveries(&delivery_header_node)?;
        let successor_header = match height.next() {
            Ok(successor_height) => match self.coherent_selected_node(successor_height)? {
                Some(successor_header_node) => {
                    if successor_header_node.parent_hash != hash {
                        return Err(StoreError::Incoherent(
                            "selected auxiliary successor does not extend the requested header",
                        )
                        .into());
                    }
                    let successor_auxiliary_deliveries =
                        self.coherent_aux_deliveries(&successor_header_node)?;
                    Some(SelectedHeaderWithAuxiliaryDeliveries {
                        header_node: successor_header_node,
                        auxiliary_deliveries: successor_auxiliary_deliveries,
                    })
                }
                None => None,
            },
            Err(_) => None,
        };
        Ok(Some(SelectedAuxiliaryWindow {
            engine_snapshot: self
                .store
                .snapshot()
                .map_err(HeaderChainStoreError::Store)?,
            delivery_header: SelectedHeaderWithAuxiliaryDeliveries {
                header_node: delivery_header_node,
                auxiliary_deliveries: delivery_auxiliary_deliveries,
            },
            successor_header,
        }))
    }

    #[cfg(test)]
    pub(crate) fn selected_locator(&self) -> Result<HeaderLocator, HeaderChainStoreError> {
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?;
        let snapshot = self
            .store
            .snapshot()
            .map_err(HeaderChainStoreError::Store)?;
        HeaderLocator::for_selected_path(&snapshot, |height| {
            self.coherent_selected_node(height)
                .map(|node| node.map(|node| node.hash))
        })
        .map_err(HeaderChainStoreError::Store)
    }

    pub(crate) fn committed_selected_locator(
        &self,
    ) -> Result<HeaderLocator, HeaderChainStoreError> {
        let engine = self
            .transition_engine
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?;
        let snapshot = engine.snapshot();
        HeaderLocator::for_selected_path(&snapshot, |height| {
            let index = engine
                .selected_projection()
                .binary_search_by_key(&height, |frontier| frontier.height)
                .map_err(|_| StoreError::Incoherent("committed selected projection has a gap"))?;
            let frontier = engine.selected_projection()[index];
            let node = engine
                .graph()
                .header_node(frontier.hash)
                .ok_or(StoreError::Incoherent(
                    "committed selected projection references a missing node",
                ))?;
            if node.height != height || node.hash != frontier.hash {
                return Err(StoreError::Incoherent(
                    "committed selected projection disagrees with its node",
                ));
            }
            Ok(Some(frontier.hash))
        })
        .map_err(HeaderChainStoreError::Store)
    }

    /// Resolve an exact, still-current VCT repair owner to one selected header request.
    pub(crate) fn vct_repair_context(
        &self,
        owner: BodyWorkOwner,
        height: block::Height,
    ) -> Result<Option<zakura_header_chain::VctRepairContext>, HeaderChainStoreError> {
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?;
        let snapshot = self
            .store
            .snapshot()
            .map_err(HeaderChainStoreError::Store)?;
        if owner.authority != BodyWorkAuthority::for_snapshot(&snapshot)
            || height <= snapshot.frontiers.finalized.height
            || height > snapshot.frontiers.header_best.height
        {
            return Ok(None);
        }
        let Some(target) = self.coherent_selected_node(height)? else {
            return Err(StoreError::Incoherent(
                "VCT repair height is absent from the selected projection",
            )
            .into());
        };
        let target_hash = target.hash;
        let parent_height = block::Height(height.0.checked_sub(1).ok_or(
            StoreError::Incoherent("non-finalized VCT repair header has no predecessor height"),
        )?);
        if self
            .coherent_selected_node(parent_height)?
            .map(|node| node.hash)
            != Some(target.parent_hash)
        {
            return Err(StoreError::Incoherent(
                "selected VCT repair header does not extend its selected predecessor",
            )
            .into());
        }
        let parent = Frontier::new(parent_height, target.parent_hash);
        Ok(Some(zakura_header_chain::VctRepairContext {
            target: Frontier::new(height, target_hash),
            locator: HeaderLocator::for_continuation(parent),
        }))
    }
}
