use std::time::Duration;

use serde_json::{Map, Number, Value};
use zakura_chain::block;
use zakura_network::zakura::{
    commit_state_trace as cs_trace, zakura_trace_peer_label, BlockApplyResult, ZakuraPeerId,
    ZakuraTrace, COMMIT_STATE_TABLE,
};

pub(crate) mod block_sync_driver;
mod coordinator;
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
pub(crate) use coordinator::{
    BlockApplyOperation, BlockApplyTerminal, LegacyFallbackLease, SyncCoordinator,
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
