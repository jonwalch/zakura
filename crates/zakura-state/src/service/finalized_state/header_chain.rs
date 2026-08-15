//! Durable adapter for the fork-aware header-chain transition engine.

use std::time::Duration;

use zakura_header_chain::AuxDelivery;

#[cfg(test)]
use chrono::{TimeZone, Utc};
#[cfg(test)]
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};
#[cfg(test)]
use tokio::{sync::watch, time::Instant};
#[cfg(test)]
use zakura_chain::block;
#[cfg(test)]
use zakura_header_chain::{
    audit_store, ApplyResult, ChangeSet, CommittedStallReceipt, EngineMetadata, EngineSnapshot,
    EvidenceId, FinalityRecord, FinalitySource, Frontier, FullStateFinalized, HeaderNode,
    RecoveryFailure, RecoveryRepair, StoreAuditRead, StoreError, TransitionContext,
    TransitionFailure, TransitionRequest, VerifiedHeaderRef,
};

#[cfg(test)]
use crate::{RetainedPathLeaseOutcome, RetainedPathReadOutcome, MAX_RETAINED_PATH_LEASES};

use super::{
    disk_db::RawVisitError,
    disk_format::{
        header_chain::{
            EligibilityReasonKind, HeaderAuxDeliveryKey, HeaderChildKey, HeaderDeferredKey,
            HeaderEligibilityRootKey, HeaderFinalityKey, HeaderHeightKey,
        },
        header_chain_values::{
            FullStateBodyValidationEvidenceAuthorityDisk, HeaderChainValueError,
            HeaderEligibilityReasonDisk, HeaderNodeDisk, HeaderReconstructionPhaseDisk,
            HeaderReconstructionProgressDisk, HeaderValidationContextDisk,
        },
        FallibleDiskValue, FromDisk, IntoDisk, RawBytes,
    },
    DiskDb, DiskWriteBatch, ReadDisk, WriteDisk, HEADER_AUX_DELIVERY,
    HEADER_BODY_EVIDENCE_AUTHORITY, HEADER_CHILD, HEADER_CONSENSUS_INVALID_BODY_TOMBSTONE,
    HEADER_DEFERRED, HEADER_ELIGIBILITY_ROOT, HEADER_ENGINE_META, HEADER_FINALITY_HISTORY,
    HEADER_NODE_BY_HASH, HEADER_SELECTED, HEADER_VALIDATION_CONTEXT, HEADER_VERIFIED,
};

const METADATA_KEY: &[u8] = b"";
const RECONSTRUCTION_PROGRESS_KEY: &[u8] = b"reconstruction-progress-v1";
const RETAINED_PATH_LEASE_IDLE: Duration = Duration::from_secs(30);

mod publication;
mod reader;
mod runtime;
mod startup;
mod store;
mod types;

pub use publication::HeaderChainSnapshotPublisher;
pub(crate) use reader::HeaderChainReader;
use reader::RetainedPathLeaseRegistry;
use runtime::load_transition_engine;
pub use runtime::HeaderChainRuntime;
pub use store::HeaderChainStore;
pub(crate) use types::{
    CapturedSelectedProjection, SelectedAuxiliaryWindow, SelectedHeaderWithAuxiliaryDeliveries,
};
pub use types::{HeaderChainStoreError, StartupReport};

#[cfg(test)]
use runtime::combined_retention_references;
#[cfg(test)]
use startup::preserve_headers_only_pin;
#[cfg(test)]
use store::authenticated_context_headers;
#[cfg(test)]
pub use types::FaultPoint;

#[cfg(test)]
#[path = "header_chain/coherence.rs"]
mod coherence;
#[cfg(any(test, feature = "header-fuzz"))]
mod fuzz;
pub(in crate::service) mod migration;
#[cfg(any(test, feature = "header-fuzz"))]
pub use fuzz::{replay_recovery_rows_bytes, RecoveryRowsReplaySummary};

/// Selects one usable VCT auxiliary delivery for a retained header.
///
/// The selector excludes deliveries without tree data and rejected deliveries. It prefers
/// authenticated, unauthenticated, and disputed deliveries in that order. The delivery ID breaks
/// ties deterministically.
pub(crate) fn select_vct_auxiliary_delivery(deliveries: Vec<AuxDelivery>) -> Option<AuxDelivery> {
    deliveries
        .into_iter()
        .filter(|delivery| {
            delivery.tree_aux.is_some()
                && !matches!(
                    delivery.authentication,
                    zakura_header_chain::AuxAuthentication::Rejected { .. }
                )
        })
        .min_by_key(|delivery| {
            (
                match delivery.authentication {
                    zakura_header_chain::AuxAuthentication::Authenticated { .. } => 0,
                    zakura_header_chain::AuxAuthentication::Unauthenticated => 1,
                    zakura_header_chain::AuxAuthentication::Disputed { .. } => 2,
                    zakura_header_chain::AuxAuthentication::Rejected { .. } => 3,
                },
                delivery.delivery_id,
            )
        })
}

#[cfg(test)]
mod tests;
