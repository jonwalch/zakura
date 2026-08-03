//! Structured snapshots of the address book's dial candidates.
//!
//! The crawler asks the address book for one candidate at a time and gets an address or
//! nothing, so a node that has stopped making outbound connections looks the same as one
//! that is satisfied. This table records, on the peer disk cache interval, how many
//! addresses the book holds, how many the crawler could dial, and how many are held back
//! by each individual check, along with the first addresses in reconnection order.
//!
//! Peer addresses follow [`Config::expose_peer_addresses`](crate::config::Config), so
//! they are redacted to their port by default. The port is what distinguishes a listener
//! from the ephemeral source port of an inbound peer, which is the distinction these
//! snapshots exist to make.

use std::{path::PathBuf, sync::Arc, time::Instant};

use serde_json::{Map, Value};
use zakura_jsonl_trace::{JsonlTracer, JsonlWriteEvent};

use crate::address_book::{DialCandidateEntry, DialCandidateReport};

const TABLE: &str = "address_book";
const FILE_NAME: &str = "address_book.jsonl";

/// Non-blocking structured diagnostics for the address book's dial candidates.
#[derive(Clone, Debug)]
pub(crate) struct AddressBookTrace {
    tracer: JsonlTracer,
    node: Arc<str>,
    started: Instant,
    expose_peer_addresses: bool,
}

impl AddressBookTrace {
    /// Creates a trace writer, which does nothing unless `trace_dir` is configured.
    pub(crate) fn new(trace_dir: Option<PathBuf>, expose_peer_addresses: bool) -> Self {
        let tracer = trace_dir
            .map(JsonlTracer::spawn)
            .unwrap_or_else(JsonlTracer::noop);

        Self {
            tracer,
            node: zakura_jsonl_trace::node_id().into(),
            started: Instant::now(),
            expose_peer_addresses,
        }
    }

    /// Returns whether snapshots are being written.
    pub(crate) fn is_enabled(&self) -> bool {
        self.tracer.is_enabled()
    }

    /// Writes one summary row for `report`, then one row per reported address.
    pub(crate) fn snapshot(&self, report: &DialCandidateReport) {
        self.emit("summary", |row| {
            insert_count(row, "total", report.total);
            insert_count(row, "ready", report.ready);
            insert_count(row, "inbound", report.inbound);
            insert_count(row, "invalid_for_outbound", report.invalid_for_outbound);
            insert_count(row, "recently_updated", report.recently_updated);
            insert_count(row, "unreachable", report.unreachable);
            insert_count(row, "ip_busy", report.ip_busy);
            insert_count(row, "responded", report.responded);
            insert_count(row, "gossiped", report.never_attempted_gossiped);
            insert_count(row, "failed", report.failed);
            insert_count(row, "attempt_pending", report.attempt_pending);
            insert_count(row, "reported_entries", report.entries.len());
        });

        for entry in &report.entries {
            self.entry(entry);
        }
    }

    fn entry(&self, entry: &DialCandidateEntry) {
        self.emit("entry", |row| {
            row.insert(
                "peer".to_string(),
                Value::String(entry.addr.addr_label(self.expose_peer_addresses)),
            );
            row.insert("port".to_string(), Value::Number(entry.addr.port().into()));
            row.insert(
                "state".to_string(),
                Value::String(format!("{:?}", entry.state)),
            );
            row.insert("ready".to_string(), Value::Bool(entry.is_ready()));
            row.insert("inbound".to_string(), Value::Bool(entry.is_inbound));
            row.insert(
                "invalid_for_outbound".to_string(),
                Value::Bool(entry.invalid_for_outbound),
            );
            row.insert(
                "recently_updated".to_string(),
                Value::Bool(entry.recently_updated),
            );
            row.insert("unreachable".to_string(), Value::Bool(entry.unreachable));
            row.insert("ip_busy".to_string(), Value::Bool(entry.ip_busy));
        });
    }

    fn emit(&self, event: &'static str, build: impl FnOnce(&mut Map<String, Value>)) {
        let Ok(permit) = self.tracer.try_reserve() else {
            return;
        };

        let mut row = Map::new();
        row.insert(
            "ts".to_string(),
            Value::Number(
                u64::try_from(self.started.elapsed().as_micros())
                    .unwrap_or(u64::MAX)
                    .into(),
            ),
        );
        row.insert("node".to_string(), Value::String(self.node.to_string()));
        row.insert("event".to_string(), Value::String(event.to_string()));
        build(&mut row);

        if let Ok(line) = serde_json::to_vec(&Value::Object(row)) {
            permit.send(JsonlWriteEvent {
                table: TABLE,
                file_name: FILE_NAME,
                line,
            });
        }
    }
}

fn insert_count(row: &mut Map<String, Value>, key: &str, count: usize) {
    row.insert(
        key.to_string(),
        Value::Number(u64::try_from(count).unwrap_or(u64::MAX).into()),
    );
}
