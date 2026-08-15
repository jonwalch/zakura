use std::collections::BTreeSet;

use thiserror::Error;
use zakura_chain::block;
use zakura_header_chain::{
    AuxDelivery, CommittedStallReceipt, CounterExhausted, EngineSnapshot, Frontier, HeaderNode,
    RecoveryFailure, RecoveryRepair, StateVersion, StoreError, TransitionFailure,
};

use super::HeaderChainValueError;

/// Failure at the durable header-chain boundary.
#[derive(Debug, Error)]
pub enum HeaderChainStoreError {
    /// Migration or bootstrap has not initialized the database yet.
    #[error("header-chain metadata is not initialized")]
    Uninitialized,
    /// The decoder found a malformed or internally contradictory durable key or value.
    #[error("incoherent durable header-chain rows: {0}")]
    Incoherent(&'static str),
    /// Stable value encoding failed before RocksDB committed the batch.
    #[error(transparent)]
    Codec(#[from] HeaderChainValueError),
    /// Pure transition planning rejected the request before commit.
    #[error(transparent)]
    Transition(#[from] TransitionFailure),
    /// The writer could not install a committed transition in memory.
    /// The writer must fail closed.
    #[error(transparent)]
    CommittedTransition(#[from] zakura_header_chain::CommittedTransitionError),
    /// A runtime durable read failed before transition planning.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// RocksDB rejected the one atomic write batch.
    #[error("header-chain atomic write failed: {0}")]
    RocksDb(#[from] rocksdb::Error),
    /// A prior panic poisoned one of the mutexes that enforces coherent transitions.
    #[error("header-chain synchronization mutex is poisoned")]
    SynchronizationPoisoned,
    /// Authenticated full state lacked a canonical header during reconstruction.
    #[error("authenticated full state is missing canonical header {0:?}")]
    MissingCanonicalHeader(block::Height),
    /// A staged full-state value disagreed with the header plan derived from the same evidence.
    #[error("staged full-state verified frontier {expected:?} differs from projected header frontier {actual:?}")]
    VerifiedFrontierMismatch {
        /// Exact staged full-state winner.
        expected: Frontier,
        /// Header transition result derived before any write.
        actual: Frontier,
    },
    /// A staged full-state branch lost a required header or parent relation in the projected DAG.
    #[error(
        "staged full-state header {hash:?} is absent or incoherent in the projected header DAG"
    )]
    StagedPathMismatch {
        /// Exact full-state header that the transition did not preserve.
        hash: block::Hash,
    },
    /// A prepared full-state mutation lost its exact serialized header-chain authority.
    #[error(
        "prepared full-state/header transition became stale at durable version {current_version:?}"
    )]
    StaleFullStateTransition {
        /// Current durable version observed instead of committing.
        current_version: StateVersion,
    },
    /// Retention pressure rejected a prepared full-state mutation before it could commit.
    #[error("prepared full-state/header transition was rejected by retention pressure")]
    FullStateResourceStalled {
        /// Durable alarm-only result committed instead of the caller mutation.
        receipt: CommittedStallReceipt,
    },
    /// Exhaustive startup audit or deterministic reconstruction failed.
    #[error(transparent)]
    Recovery(#[from] RecoveryFailure),
    /// An explicit store migration exhausted a monotonic durable counter.
    #[error(transparent)]
    Counter(#[from] CounterExhausted),
    /// Deterministic body validation refuted an imported headers-only trust pin.
    /// The operator must destroy and resync this store.
    #[error(
        "header_chain_migrated_pin_refuted at {pin:?}; delete the migrated header store and resync"
    )]
    MigratedPinRefuted {
        /// Exact preserved pin contradicted by deterministic body validation.
        pin: Frontier,
    },
    /// A test injected a crash at a named durable or publication boundary.
    #[cfg(test)]
    #[error("injected header-chain crash at {0:?}")]
    InjectedCrash(FaultPoint),
}

/// One successful startup audit and optional atomic repair.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartupReport {
    /// Snapshot read before any reconstructible repair.
    pub previous: EngineSnapshot,
    /// Snapshot that the startup audit approved for publication.
    pub current: EngineSnapshot,
    /// Exact reconstructible categories repaired in one batch.
    pub repairs: BTreeSet<RecoveryRepair>,
}

/// One selected header and every auxiliary delivery attached to it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SelectedHeaderWithAuxiliaryDeliveries {
    /// Header node from the selected projection.
    pub(crate) header_node: HeaderNode,
    /// Auxiliary deliveries attached to `header_node`.
    pub(crate) auxiliary_deliveries: Vec<AuxDelivery>,
}

/// One atomically read selected header and its direct selected successor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SelectedAuxiliaryWindow {
    /// Engine snapshot that identifies the selected projection generation.
    pub(crate) engine_snapshot: EngineSnapshot,
    /// Selected header whose auxiliary delivery the caller will verify.
    pub(crate) delivery_header: SelectedHeaderWithAuxiliaryDeliveries,
    /// Direct selected successor that supplies the authentication boundary.
    pub(crate) successor_header: Option<SelectedHeaderWithAuxiliaryDeliveries>,
}

/// One selected projection captured with the engine snapshot that produced it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CapturedSelectedProjection {
    /// Engine snapshot that identifies the projection generation and bounds.
    pub(crate) engine_snapshot: EngineSnapshot,
    /// Complete selected projection in ascending height order.
    pub(crate) frontiers: Vec<Frontier>,
}

/// Deterministic state-writer and observer boundaries used by the crash harness.
#[cfg(test)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum FaultPoint {
    BeforeCommit,
    AfterCommit,
    AfterMemorySwap,
    AfterPublish,
}

#[cfg(test)]
impl FaultPoint {
    /// Complete ordered state-writer crash surface used by deterministic recovery tests.
    pub const ALL: [Self; 4] = [
        Self::BeforeCommit,
        Self::AfterCommit,
        Self::AfterMemorySwap,
        Self::AfterPublish,
    ];

    /// Ordered crash surface reached by a transition with no header-chain changes.
    pub const NO_CHANGE: [Self; 3] = [Self::BeforeCommit, Self::AfterCommit, Self::AfterMemorySwap];

    pub(in crate::service::finalized_state::header_chain) const fn commit_completed(self) -> bool {
        matches!(
            self,
            Self::AfterCommit | Self::AfterMemorySwap | Self::AfterPublish
        )
    }

    pub(in crate::service::finalized_state::header_chain) const fn memory_swap_completed(
        self,
    ) -> bool {
        matches!(self, Self::AfterMemorySwap | Self::AfterPublish)
    }

    pub(in crate::service::finalized_state::header_chain) const fn publication_completed(
        self,
    ) -> bool {
        matches!(self, Self::AfterPublish)
    }
}
