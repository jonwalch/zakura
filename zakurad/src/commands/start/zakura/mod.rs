use std::time::Duration;

use serde_json::{Map, Number, Value};
use zakura_chain::block;
use zakura_network::zakura::{
    commit_state_trace as cs_trace, zakura_trace_peer_label, BlockApplyResult, ZakuraPeerId,
    ZakuraTrace, COMMIT_STATE_TABLE,
};

/// Serializes block-apply ownership across initial checkpoint bootstrap, native
/// Zakura block sync, and legacy `ChainSync` fallback.
///
/// Two sync engines submitting bulk commits concurrently race in the applying
/// queue. A fresh state therefore stays legacy-owned until checkpoint semantic
/// handoff, and fallback is a later commit barrier: while yielded back to legacy
/// sync, the block-sync driver starts no new applies and the watchdog waits for
/// in-flight applies to finish. A fallback lease restores ownership to Zakura on
/// every exit path. The Zakura reactors stay alive throughout; only bulk body
/// applies are gated.
#[derive(Debug)]
pub(crate) struct BlockSyncHandoff {
    owner: std::sync::atomic::AtomicU8,
    in_flight: std::sync::atomic::AtomicUsize,
    drained: tokio::sync::Notify,
    owner_changed: tokio::sync::Notify,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(u8)]
enum BlockApplyOwner {
    LegacyBootstrap = 0,
    Zakura = 1,
    LegacyFallback = 2,
}

/// An invalid or competing block-apply ownership transition.
#[derive(Copy, Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum BlockSyncHandoffError {
    /// Initial checkpoint ownership was already transferred or fallback is active.
    #[error("legacy checkpoint bootstrap does not own block applies")]
    BootstrapNotOwned,
    /// Native sync is not currently available to yield to one fallback round.
    #[error("Zakura does not currently own block applies")]
    ZakuraNotOwned,
}

/// Tracks one in-flight Zakura block apply; dropping it releases the slot and
/// wakes a pending [`BlockSyncHandoff::acquire_legacy_fallback`].
#[derive(Debug)]
pub(crate) struct BlockApplyPermit(std::sync::Arc<BlockSyncHandoff>);

/// Exclusive authorization for one fully drained legacy fallback round.
///
/// This guard is created before the drain starts, so cancelling or panicking
/// while waiting restores native ownership. It is returned to the caller only
/// after every native apply permit has been released.
#[derive(Debug)]
pub(crate) struct LegacyFallbackLease {
    handoff: std::sync::Arc<BlockSyncHandoff>,
}

impl BlockSyncHandoff {
    pub(crate) fn new() -> std::sync::Arc<Self> {
        Self::new_with_owner(BlockApplyOwner::Zakura)
    }

    /// Starts with the legacy-compatible downloader owning block applies until
    /// the checkpoint verifier publishes the durable semantic-handoff snapshot.
    pub(crate) fn new_legacy_bootstrap() -> std::sync::Arc<Self> {
        Self::new_with_owner(BlockApplyOwner::LegacyBootstrap)
    }

    fn new_with_owner(owner: BlockApplyOwner) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            owner: std::sync::atomic::AtomicU8::new(owner as u8),
            in_flight: std::sync::atomic::AtomicUsize::new(0),
            drained: tokio::sync::Notify::new(),
            owner_changed: tokio::sync::Notify::new(),
        })
    }

    /// Whether the pipeline has been yielded to legacy sync.
    pub(crate) fn is_yielded_to_legacy(&self) -> bool {
        self.owner.load(std::sync::atomic::Ordering::SeqCst)
            == BlockApplyOwner::LegacyFallback as u8
    }

    /// Whether native Zakura sync currently owns block applies.
    pub(crate) fn zakura_owns_applies(&self) -> bool {
        self.owner.load(std::sync::atomic::Ordering::SeqCst) == BlockApplyOwner::Zakura as u8
    }

    /// Transfers initial block-apply ownership to native Zakura sync exactly once.
    pub(crate) fn finish_legacy_bootstrap(&self) -> Result<(), BlockSyncHandoffError> {
        self.owner
            .compare_exchange(
                BlockApplyOwner::LegacyBootstrap as u8,
                BlockApplyOwner::Zakura as u8,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            )
            .map_err(|_| BlockSyncHandoffError::BootstrapNotOwned)?;
        self.owner_changed.notify_waiters();
        Ok(())
    }

    /// Waits until initial legacy bootstrap transfers apply ownership to Zakura.
    pub(crate) async fn wait_for_zakura_ownership(&self) {
        loop {
            let changed = self.owner_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.zakura_owns_applies() || self.is_yielded_to_legacy() {
                return;
            }
            changed.await;
        }
    }

    /// Returns a permit for one native block apply only while Zakura owns the pipeline.
    pub(crate) fn begin_apply(self: &std::sync::Arc<Self>) -> Option<BlockApplyPermit> {
        if !self.zakura_owns_applies() {
            return None;
        }

        self.in_flight
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // Load-bearing invariant: reserve before the second yielded check so a
        // concurrent fallback either sees this apply in `in_flight` or rejects
        // the permit and releases it here. That makes the drain a real commit
        // barrier without locking the hot path.
        if !self.zakura_owns_applies() {
            self.release();
            return None;
        }

        Some(BlockApplyPermit(self.clone()))
    }

    fn release(&self) {
        if self
            .in_flight
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
            == 1
        {
            self.drained.notify_waiters();
        }
    }

    /// Stop new native applies and acquire one legacy fallback lease after all
    /// already-authorized applies finish.
    ///
    /// `diagnostic_interval` bounds each wait only for observability. A warning
    /// never authorizes legacy work while an underlying native commit is alive.
    pub(crate) async fn acquire_legacy_fallback(
        self: &std::sync::Arc<Self>,
        diagnostic_interval: Duration,
    ) -> Result<LegacyFallbackLease, BlockSyncHandoffError> {
        self.owner
            .compare_exchange(
                BlockApplyOwner::Zakura as u8,
                BlockApplyOwner::LegacyFallback as u8,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            )
            .map_err(|_| BlockSyncHandoffError::ZakuraNotOwned)?;
        self.owner_changed.notify_waiters();
        let lease = LegacyFallbackLease {
            handoff: self.clone(),
        };
        self.wait_for_applies(diagnostic_interval).await;
        metrics::gauge!("sync.zakura.legacy_fallback.active").set(1.0);
        Ok(lease)
    }

    async fn wait_for_applies(&self, diagnostic_interval: Duration) {
        let diagnostic_interval = if diagnostic_interval.is_zero() {
            Duration::from_secs(1)
        } else {
            diagnostic_interval
        };
        let started = tokio::time::Instant::now();
        loop {
            let drained = self.drained.notified();
            tokio::pin!(drained);
            drained.as_mut().enable();

            let in_flight = self.in_flight.load(std::sync::atomic::Ordering::SeqCst);
            if in_flight == 0 {
                return;
            }

            if tokio::time::timeout(diagnostic_interval, drained)
                .await
                .is_err()
            {
                tracing::warn!(
                    in_flight,
                    elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                    "Zakura block applies remain active while legacy fallback waits for its \
                     exclusive apply lease"
                );
            }
        }
    }
}

impl Drop for BlockApplyPermit {
    fn drop(&mut self) {
        self.0.release();
    }
}

impl Drop for LegacyFallbackLease {
    fn drop(&mut self) {
        let restored = self
            .handoff
            .owner
            .compare_exchange(
                BlockApplyOwner::LegacyFallback as u8,
                BlockApplyOwner::Zakura as u8,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            )
            .is_ok();
        if restored {
            self.handoff.owner_changed.notify_waiters();
        } else {
            tracing::error!(
                "legacy fallback lease lost exclusive apply ownership before it was dropped"
            );
        }
        metrics::gauge!("sync.zakura.legacy_fallback.active").set(0.0);
    }
}

pub(crate) mod block_sync_driver;
pub(crate) mod frontier;
pub(crate) mod header_sync_driver;
pub(crate) mod throughput_probe;

pub(crate) use block_sync_driver::drive_block_sync_actions;
#[cfg(test)]
pub(crate) use block_sync_driver::{
    abandoned_block_apply_finished_event, apply_block_sync_body, block_apply_class,
    block_sync_missing_body_window, block_sync_needed_blocks_from_state,
    coalesce_ready_needed_block_queries, coalesce_stale_needed_block_queries,
    commit_block_sync_body, query_block_sync_needed_blocks, BlockApplyClass,
    ZAKURA_BLOCK_SYNC_MISSING_BODY_WINDOW,
};
pub(crate) use frontier::{query_block_sync_frontiers, verified_block_tip_from_state};
pub(crate) use header_sync_driver::zakura_header_sync_driver_startup;
#[cfg(test)]
pub(crate) use header_sync_driver::{block_roots_cover_range, root_covered_query_best_header_tip};
pub(crate) use throughput_probe::{BlocksyncThroughputProbe, BlocksyncThroughputSummary};

pub(crate) const ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT: Duration = Duration::from_secs(30);
/// Maximum time for reconstructing the durable header chain from authenticated full state.
///
/// Unlike ordinary driver requests, this one-time startup operation scans the finalized header
/// history. Multi-million-block production databases can take longer than the request deadline.
pub(crate) const ZAKURA_HEADER_CHAIN_STARTUP_TIMEOUT: Duration = Duration::from_secs(15 * 60);
pub(crate) const ZAKURA_HEADER_SYNC_DRIVER_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) fn emit_commit_state(
    trace: &ZakuraTrace,
    event: &'static str,
    source: &'static str,
    build: impl FnOnce(&mut Map<String, Value>),
) {
    trace.emit_with(COMMIT_STATE_TABLE, |row| {
        row.insert(
            cs_trace::EVENT.to_string(),
            Value::String(event.to_string()),
        );
        insert_cs_str(row, cs_trace::SOURCE, source);
        build(row);
    });
}

pub(crate) fn insert_cs_height(
    row: &mut Map<String, Value>,
    key: &'static str,
    height: block::Height,
) {
    insert_cs_u64(row, key, u64::from(height.0));
}

pub(crate) fn insert_cs_hash(row: &mut Map<String, Value>, key: &'static str, hash: block::Hash) {
    row.insert(key.to_string(), Value::String(format!("{hash}")));
}

pub(crate) fn insert_cs_peer(row: &mut Map<String, Value>, key: &'static str, peer: &ZakuraPeerId) {
    row.insert(
        key.to_string(),
        Value::String(zakura_trace_peer_label(peer)),
    );
}

pub(crate) fn insert_cs_u64(row: &mut Map<String, Value>, key: &'static str, value: u64) {
    row.insert(key.to_string(), Value::Number(Number::from(value)));
}

pub(crate) fn insert_cs_str(row: &mut Map<String, Value>, key: &'static str, value: &str) {
    row.insert(key.to_string(), Value::String(value.to_string()));
}

pub(crate) fn block_apply_result_label(result: BlockApplyResult) -> &'static str {
    match result {
        BlockApplyResult::Committed => "committed",
        BlockApplyResult::Duplicate => "duplicate",
        BlockApplyResult::Rejected => "rejected",
        BlockApplyResult::Unavailable => "unavailable",
        BlockApplyResult::TimedOut => "timed_out",
    }
}

pub(crate) fn block_verify_error_class<Error>(
    error: &Error,
) -> zakura_header_chain::BodyVerificationClass
where
    Error: std::fmt::Debug + Send + Sync + 'static,
{
    use zakura_header_chain::{BodyVerificationClass, TransientBodyFailureKind};

    fn classify(error: &(dyn std::any::Any + Send + Sync)) -> Option<BodyVerificationClass> {
        error
            .downcast_ref::<zakura_consensus::RouterError>()
            .map(zakura_consensus::RouterError::body_verification_class)
            .or_else(|| {
                error
                    .downcast_ref::<zakura_consensus::VerifyBlockError>()
                    .map(zakura_consensus::VerifyBlockError::body_verification_class)
            })
            .or_else(|| {
                error
                    .downcast_ref::<zakura_consensus::VerifyCheckpointError>()
                    .map(zakura_consensus::VerifyCheckpointError::body_verification_class)
            })
    }

    fn classify_box(error: &zakura_consensus::BoxError) -> Option<BodyVerificationClass> {
        error
            .downcast_ref::<zakura_consensus::RouterError>()
            .map(zakura_consensus::RouterError::body_verification_class)
            .or_else(|| {
                error
                    .downcast_ref::<zakura_consensus::VerifyBlockError>()
                    .map(zakura_consensus::VerifyBlockError::body_verification_class)
            })
            .or_else(|| {
                error
                    .downcast_ref::<zakura_consensus::VerifyCheckpointError>()
                    .map(zakura_consensus::VerifyCheckpointError::body_verification_class)
            })
    }

    let error = error as &(dyn std::any::Any + Send + Sync);
    classify(error)
        .or_else(|| {
            error
                .downcast_ref::<zakura_consensus::BoxError>()
                .and_then(classify_box)
        })
        .unwrap_or(BodyVerificationClass::Retryable(
            TransientBodyFailureKind::VerifierUnavailable,
        ))
}
