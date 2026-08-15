use std::collections::{HashMap, HashSet};

use zakura_chain::block;
use zakura_header_chain::{Frontier, HeaderNode, StoreAuditRead, StoreError};

use super::super::{
    DiskWriteBatch, HeaderChainStoreError, HeaderValidationContextDisk, HEADER_VALIDATION_CONTEXT,
};
use super::{store_error, HeaderChainStore};

impl HeaderChainStore {
    /// Enqueue the exact validation-context delta for a finalized-frontier change.
    pub(super) fn enqueue_validation_context_update(
        &self,
        batch: &mut DiskWriteBatch,
        previous_finalized: Option<Frontier>,
        current_finalized: Frontier,
        staged_nodes: &[HeaderNode],
    ) -> Result<(), HeaderChainStoreError> {
        let Some(previous_finalized) =
            previous_finalized.filter(|previous| *previous != current_finalized)
        else {
            return Ok(());
        };
        let staged_nodes: HashMap<_, _> =
            staged_nodes.iter().map(|node| (node.hash, node)).collect();

        if let Some((incoming, outgoing)) =
            self.incremental_context_slide(previous_finalized, current_finalized, &staged_nodes)?
        {
            self.put_value(
                batch,
                HEADER_VALIDATION_CONTEXT,
                incoming.header.hash().0,
                &incoming,
            )?;
            if let Some(hash) = outgoing {
                self.delete_raw(batch, HEADER_VALIDATION_CONTEXT, hash.0)?;
            }
            return Ok(());
        }

        let previous_contexts: Vec<_> =
            authenticated_context_headers(self, previous_finalized.hash, None)?
                .into_iter()
                .map(|context| (context.header.hash(), context))
                .collect();
        let current_contexts: Vec<_> =
            authenticated_context_headers(self, current_finalized.hash, Some(&staged_nodes))?
                .into_iter()
                .map(|context| (context.header.hash(), context))
                .collect();
        let previous_hashes: HashSet<_> = previous_contexts.iter().map(|(hash, _)| *hash).collect();
        let current_hashes: HashSet<_> = current_contexts.iter().map(|(hash, _)| *hash).collect();

        for (hash, _) in previous_contexts {
            if !current_hashes.contains(&hash) {
                self.delete_raw(batch, HEADER_VALIDATION_CONTEXT, hash.0)?;
            }
        }
        for (hash, context) in current_contexts {
            if !previous_hashes.contains(&hash) {
                self.put_value(batch, HEADER_VALIDATION_CONTEXT, hash.0, &context)?;
            }
        }
        Ok(())
    }

    /// Return the exact context-table delta for one authenticated finalized-parent advance.
    fn incremental_context_slide(
        &self,
        previous: Frontier,
        current: Frontier,
        staged_nodes: &HashMap<block::Hash, &HeaderNode>,
    ) -> Result<Option<(HeaderValidationContextDisk, Option<block::Hash>)>, HeaderChainStoreError>
    {
        if current.height.0 != previous.height.0.saturating_add(1) {
            return Ok(None);
        }
        let Some(previous_node) = self.header_node(previous.hash)? else {
            return Ok(None);
        };
        let stored_current = if staged_nodes.contains_key(&current.hash) {
            None
        } else {
            self.header_node(current.hash)?
        };
        let current_node = staged_nodes
            .get(&current.hash)
            .copied()
            .or(stored_current.as_ref());
        let Some(current_node) = current_node else {
            return Ok(None);
        };
        if previous_node.hash != previous.hash
            || previous_node.header.hash() != previous.hash
            || previous_node.height != previous.height
            || current_node.hash != current.hash
            || current_node.header.hash() != current.hash
            || current_node.height != current.height
            || current_node.parent_hash != previous.hash
            || current_node.header.previous_block_hash != previous.hash
        {
            return Ok(None);
        }

        let predecessor_span = u32::try_from(zakura_header_chain::POW_PREDECESSOR_CONTEXT_SPAN)
            .map_err(|_| {
                HeaderChainStoreError::Incoherent("validation context bound does not fit in u32")
            })?;
        let outgoing = if previous.height.0 >= predecessor_span {
            let outgoing_height = block::Height(previous.height.0 - predecessor_span);
            let Some(outgoing_hash) = self.authenticated_canonical_hash(outgoing_height)? else {
                return Ok(None);
            };
            let Some(outgoing_context) = self.get_value::<HeaderValidationContextDisk>(
                HEADER_VALIDATION_CONTEXT,
                outgoing_hash.0,
            )?
            else {
                return Ok(None);
            };
            if outgoing_context.header.hash() != outgoing_hash
                || outgoing_context.height != outgoing_height
            {
                return Ok(None);
            }
            Some(outgoing_hash)
        } else {
            None
        };

        Ok(Some((
            HeaderValidationContextDisk {
                header: previous_node.header,
                height: previous_node.height,
            },
            outgoing,
        )))
    }
}

pub(in crate::service::finalized_state::header_chain) fn authenticated_context_headers(
    store: &HeaderChainStore,
    parent: block::Hash,
    staged_nodes: Option<&HashMap<block::Hash, &HeaderNode>>,
) -> Result<Vec<HeaderValidationContextDisk>, StoreError> {
    let staged_parent = staged_nodes.and_then(|nodes| nodes.get(&parent).copied());
    let stored_parent = if staged_parent.is_none() {
        store.header_node(parent)?
    } else {
        None
    };
    let parent_node = staged_parent
        .or(stored_parent.as_ref())
        .ok_or(StoreError::Incoherent("validation parent is not retained"))?;
    let predecessor_span = u32::try_from(zakura_header_chain::POW_PREDECESSOR_CONTEXT_SPAN)
        .map_err(|_| StoreError::Incoherent("validation context bound does not fit in u32"))?;
    let required = usize::try_from(parent_node.height.0.min(predecessor_span))
        .map_err(|_| StoreError::Incoherent("validation context bound does not fit in usize"))?;
    let mut contexts = Vec::with_capacity(required);
    let mut current_hash = parent_node.parent_hash;
    let mut expected_height = parent_node.height;
    for _ in 0..required {
        expected_height = expected_height
            .previous()
            .map_err(|_| StoreError::Incoherent("validation context height underflow"))?;
        let staged_node = staged_nodes.and_then(|nodes| nodes.get(&current_hash).copied());
        let stored_node = if staged_node.is_none() {
            store.header_node(current_hash)?
        } else {
            None
        };
        let context = if let Some(node) = staged_node.or(stored_node.as_ref()) {
            HeaderValidationContextDisk {
                header: node.header.clone(),
                height: node.height,
            }
        } else {
            store
                .get_value::<HeaderValidationContextDisk>(HEADER_VALIDATION_CONTEXT, current_hash.0)
                .map_err(store_error)?
                .ok_or(StoreError::Incoherent("validation context has a gap"))?
        };
        if context.header.hash() != current_hash || context.height != expected_height {
            return Err(StoreError::Incoherent(
                "invalid immutable validation context",
            ));
        }
        current_hash = context.header.previous_block_hash;
        contexts.push(context);
    }
    contexts.reverse();
    Ok(contexts)
}
