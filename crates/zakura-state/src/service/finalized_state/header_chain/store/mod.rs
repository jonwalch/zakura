use std::sync::{Arc, Mutex};

use sha2::{Digest, Sha256};
use zakura_header_chain::{EligibilityReason, EvidenceId, StoreError};

use super::{DiskDb, EligibilityReasonKind, HeaderChainStoreError};

mod audit_read;
mod change_batch;
mod validation_context;

pub(in crate::service::finalized_state::header_chain) use validation_context::authenticated_context_headers;

/// One RocksDB-backed header DAG with a process-local serialized writer.
///
/// The writer mutex is the first lock in the runtime's global lock order.
#[derive(Clone, Debug)]
pub struct HeaderChainStore {
    pub(in crate::service::finalized_state::header_chain) db: DiskDb,
    pub(in crate::service::finalized_state::header_chain) writer: Arc<Mutex<()>>,
}

impl HeaderChainStore {
    /// Attach the header-chain adapter to the existing finalized-state database.
    pub fn new(db: DiskDb) -> Self {
        Self {
            db,
            writer: Arc::new(Mutex::new(())),
        }
    }

    pub(in crate::service) fn is_initialized(&self) -> Result<bool, HeaderChainStoreError> {
        Ok(self.metadata_row()?.is_some())
    }
}

fn reason_kind(reason: &EligibilityReason) -> EligibilityReasonKind {
    match reason {
        EligibilityReason::SettledUpgradeConflict { .. } => EligibilityReasonKind::SettledUpgrade,
        EligibilityReason::CheckpointConflict { .. } => EligibilityReasonKind::LocalCheckpoint,
        EligibilityReason::FinalityConflict { .. } => EligibilityReasonKind::Finality,
        EligibilityReason::ConsensusBodyInvalid { .. } => EligibilityReasonKind::ConsensusBody,
        EligibilityReason::OperatorInvalid { .. } => EligibilityReasonKind::Operator,
    }
}

fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    for index in (0..end.len()).rev() {
        if end[index] != u8::MAX {
            end[index] = end[index].saturating_add(1);
            end.truncate(index + 1);
            return Some(end);
        }
    }
    None
}

fn reason_evidence(reason: &EligibilityReason) -> EvidenceId {
    if let EligibilityReason::ConsensusBodyInvalid { evidence, .. } = reason {
        return *evidence;
    }
    let mut hasher = Sha256::new();
    hasher.update(b"zakura-header-chain-eligibility-reason-v1");
    hasher.update([reason_tag(reason)]);
    match reason {
        EligibilityReason::SettledUpgradeConflict { height, expected }
        | EligibilityReason::CheckpointConflict { height, expected } => {
            hasher.update(height.0.to_be_bytes());
            hasher.update(expected.0);
        }
        EligibilityReason::FinalityConflict { finalized } => {
            hasher.update(finalized.height.0.to_be_bytes());
            hasher.update(finalized.hash.0);
        }
        EligibilityReason::OperatorInvalid {
            id,
            reason_digest,
            evidence,
        } => {
            hasher.update(id.bytes());
            hasher.update(reason_digest);
            hasher.update(evidence.digest());
        }
        EligibilityReason::ConsensusBodyInvalid { .. } => unreachable!("returned above"),
    }
    EvidenceId::from_digest(hasher.finalize().into())
}

fn reason_tag(reason: &EligibilityReason) -> u8 {
    match reason {
        EligibilityReason::SettledUpgradeConflict { .. } => 0,
        EligibilityReason::CheckpointConflict { .. } => 1,
        EligibilityReason::FinalityConflict { .. } => 2,
        EligibilityReason::ConsensusBodyInvalid { .. } => 3,
        EligibilityReason::OperatorInvalid { .. } => 4,
    }
}

fn store_error(error: HeaderChainStoreError) -> StoreError {
    match error {
        HeaderChainStoreError::Store(error) => error,
        HeaderChainStoreError::Uninitialized => StoreError::Unavailable("store is uninitialized"),
        HeaderChainStoreError::RocksDb(_) => {
            StoreError::Unavailable("durable header-chain database read failed")
        }
        HeaderChainStoreError::SynchronizationPoisoned => {
            StoreError::Unavailable("header-chain synchronization is unavailable")
        }
        _ => StoreError::Incoherent("durable header-chain read failed"),
    }
}

#[cfg(test)]
mod tests {
    use super::{store_error, HeaderChainStoreError, StoreError};

    #[test]
    fn store_error_preserves_existing_store_error() {
        assert!(matches!(
            store_error(HeaderChainStoreError::Store(StoreError::Unavailable(
                "injected durable read failure"
            ))),
            StoreError::Unavailable("injected durable read failure")
        ));
    }

    #[test]
    fn store_error_keeps_availability_failures_out_of_incoherence() {
        assert!(matches!(
            store_error(HeaderChainStoreError::Uninitialized),
            StoreError::Unavailable(_)
        ));
        assert!(matches!(
            store_error(HeaderChainStoreError::SynchronizationPoisoned),
            StoreError::Unavailable(_)
        ));
    }
}
