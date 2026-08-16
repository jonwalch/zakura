//! Hash-correlated diagnostics for near-tip block propagation.

use std::{path::PathBuf, time::Duration};

use serde::Serialize;
use zakura_chain::block;
use zakura_jsonl_trace::{
    saturating_millis, JsonlDisplay, JsonlEventEmitter, JsonlTraceEvent, JsonlTraceTable,
    JsonlTracer,
};
use zakura_network::{self as zn, PeerSocketAddr};

const TABLE: JsonlTraceTable = JsonlTraceTable::new("block_propagation", "block_propagation.jsonl");

/// Non-blocking block propagation event emitter.
#[derive(Clone, Debug)]
pub(crate) struct BlockPropagationTrace {
    emitter: JsonlEventEmitter,
    expose_peer_addresses: bool,
}

impl BlockPropagationTrace {
    /// Create a propagation trace writing into `trace_dir`, or a no-op trace when disabled.
    pub(crate) fn new(trace_dir: Option<PathBuf>, expose_peer_addresses: bool) -> Self {
        let tracer = trace_dir
            .map(JsonlTracer::spawn)
            .unwrap_or_else(JsonlTracer::noop);
        Self {
            emitter: JsonlEventEmitter::new(tracer, zakura_jsonl_trace::node_id()),
            expose_peer_addresses,
        }
    }

    /// Create a disabled propagation trace.
    pub(crate) fn noop() -> Self {
        Self {
            emitter: JsonlEventEmitter::noop(),
            expose_peer_addresses: false,
        }
    }

    /// Record the point where a locally submitted block starts network broadcast.
    pub(crate) fn mined_block_broadcast_started(&self, hash: block::Hash, height: block::Height) {
        self.emitter
            .emit_event(|| BlockPropagationEvent::MinedBlockBroadcastStarted {
                hash: JsonlDisplay(&hash),
                height: height.0,
            });
    }

    /// Record completion of the local peer-set broadcast request.
    pub(crate) fn mined_block_broadcast_finished(
        &self,
        hash: block::Hash,
        height: block::Height,
        result: &'static str,
        elapsed: Duration,
    ) {
        self.emitter
            .emit_event(|| BlockPropagationEvent::MinedBlockBroadcastFinished {
                hash: JsonlDisplay(&hash),
                height: height.0,
                result,
                elapsed_ms: saturating_millis(elapsed),
            });
    }

    /// Record a legacy-compatible block advertisement and its local admission result.
    pub(crate) fn block_announced(
        &self,
        hash: block::Hash,
        source: Option<&zn::PeerSource>,
        transport: &'static str,
        disposition: &'static str,
    ) {
        self.emitter
            .emit_event(|| BlockPropagationEvent::BlockAnnounced {
                hash: JsonlDisplay(&hash),
                transport,
                source: source.map(|source| self.source_label(source)),
                disposition,
            });
    }

    /// Record a complete legacy block body returned by the network service.
    pub(crate) fn legacy_block_downloaded(
        &self,
        hash: block::Hash,
        height: block::Height,
        peer: Option<PeerSocketAddr>,
        elapsed: Duration,
    ) {
        self.emitter
            .emit_event(|| BlockPropagationEvent::LegacyBlockDownloaded {
                hash: JsonlDisplay(&hash),
                height: height.0,
                source: peer.map(|peer| self.legacy_peer_label(peer)),
                elapsed_ms: saturating_millis(elapsed),
            });
    }

    /// Record the terminal result of legacy gossip download and verification.
    pub(crate) fn legacy_block_finished(
        &self,
        hash: block::Hash,
        height: Option<block::Height>,
        result: &'static str,
        reason: Option<&str>,
        elapsed: Duration,
    ) {
        self.emitter
            .emit_event(|| BlockPropagationEvent::LegacyBlockFinished {
                hash: JsonlDisplay(&hash),
                height: height.map(|height| height.0),
                result,
                reason: reason.map(bounded_reason),
                elapsed_ms: saturating_millis(elapsed),
            });
    }

    fn source_label(&self, source: &zn::PeerSource) -> String {
        match source {
            zn::PeerSource::LegacySocket(peer) => self.legacy_peer_label(*peer),
            zn::PeerSource::Zakura(peer) => {
                format!("native:{}", zn::zakura::zakura_trace_peer_label(peer))
            }
        }
    }

    fn legacy_peer_label(&self, peer: PeerSocketAddr) -> String {
        if self.expose_peer_addresses {
            format!("legacy:{}", peer.remove_socket_addr_privacy())
        } else {
            "legacy:redacted".to_string()
        }
    }
}

impl Default for BlockPropagationTrace {
    fn default() -> Self {
        Self::noop()
    }
}

#[derive(Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
enum BlockPropagationEvent<'a> {
    MinedBlockBroadcastStarted {
        hash: JsonlDisplay<'a, block::Hash>,
        height: u32,
    },
    MinedBlockBroadcastFinished {
        hash: JsonlDisplay<'a, block::Hash>,
        height: u32,
        result: &'static str,
        elapsed_ms: u64,
    },
    BlockAnnounced {
        hash: JsonlDisplay<'a, block::Hash>,
        transport: &'static str,
        #[serde(skip_serializing_if = "Option::is_none")]
        source: Option<String>,
        disposition: &'static str,
    },
    LegacyBlockDownloaded {
        hash: JsonlDisplay<'a, block::Hash>,
        height: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        source: Option<String>,
        elapsed_ms: u64,
    },
    LegacyBlockFinished {
        hash: JsonlDisplay<'a, block::Hash>,
        #[serde(skip_serializing_if = "Option::is_none")]
        height: Option<u32>,
        result: &'static str,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
        elapsed_ms: u64,
    },
}

impl JsonlTraceEvent for BlockPropagationEvent<'_> {
    const TABLE: JsonlTraceTable = TABLE;
}

fn bounded_reason(reason: &str) -> String {
    const MAX_REASON_CHARS: usize = 256;
    reason.chars().take(MAX_REASON_CHARS).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn propagation_events_have_hash_correlated_schema() {
        let hash = block::Hash([0x2a; 32]);
        let event = BlockPropagationEvent::BlockAnnounced {
            hash: JsonlDisplay(&hash),
            transport: "legacy",
            source: Some("legacy:peer:1234".to_string()),
            disposition: "queued",
        };

        let value = serde_json::to_value(event).expect("propagation event serializes");
        assert_eq!(value["event"], "block_announced");
        assert_eq!(value["hash"], hash.to_string());
        assert_eq!(value["transport"], "legacy");
        assert_eq!(value["source"], "legacy:peer:1234");
        assert_eq!(value["disposition"], "queued");
    }

    #[test]
    fn legacy_peer_labels_follow_address_privacy_policy() {
        let peer = PeerSocketAddr::from(([203, 0, 113, 7], 18233));
        let private_trace = BlockPropagationTrace::noop();
        let public_trace = BlockPropagationTrace {
            emitter: JsonlEventEmitter::noop(),
            expose_peer_addresses: true,
        };

        assert_eq!(private_trace.legacy_peer_label(peer), "legacy:redacted");
        assert_eq!(
            public_trace.legacy_peer_label(peer),
            "legacy:203.0.113.7:18233"
        );
    }

    #[test]
    fn failure_reasons_are_bounded() {
        let reason = "x".repeat(300);
        assert_eq!(bounded_reason(&reason).len(), 256);
    }
}
