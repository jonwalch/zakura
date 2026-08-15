use std::sync::{Arc, Mutex};

use tokio::sync::watch;
use zakura_header_chain::EngineSnapshot;

/// Publishes the latest snapshot only after its header-chain transition is durable.
#[derive(Clone, Debug)]
pub struct HeaderChainSnapshotPublisher {
    sender: watch::Sender<EngineSnapshot>,
    mirrors: Arc<Mutex<Vec<watch::Sender<Option<EngineSnapshot>>>>>,
}

impl HeaderChainSnapshotPublisher {
    pub(in crate::service::finalized_state::header_chain) fn new(snapshot: EngineSnapshot) -> Self {
        record_published_snapshot(&snapshot);
        let (sender, _) = watch::channel(snapshot);
        Self {
            sender,
            mirrors: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Return the latest durable snapshot.
    pub fn snapshot(&self) -> EngineSnapshot {
        self.sender.borrow().clone()
    }

    /// Subscribe to the latest durable snapshot without replay dependence.
    #[cfg(test)]
    pub fn subscribe(&self) -> watch::Receiver<EngineSnapshot> {
        self.sender.subscribe()
    }

    /// Mirror committed snapshots into a channel that can predate runtime attachment.
    pub(crate) fn mirror_to(&self, sender: watch::Sender<Option<EngineSnapshot>>) {
        sender.send_replace(Some(self.snapshot()));
        self.mirrors
            .lock()
            .expect("header-chain publisher mirror mutex is never poisoned")
            .push(sender);
    }

    pub(in crate::service::finalized_state::header_chain) fn publish(
        &self,
        snapshot: EngineSnapshot,
    ) {
        record_published_snapshot(&snapshot);
        self.sender.send_replace(snapshot.clone());
        self.mirrors
            .lock()
            .expect("header-chain publisher mirror mutex is never poisoned")
            .retain(|mirror| {
                if mirror.receiver_count() == 0 {
                    false
                } else {
                    mirror.send_replace(Some(snapshot.clone()));
                    true
                }
            });
    }
}

pub(in crate::service::finalized_state::header_chain) fn record_published_snapshot(
    snapshot: &EngineSnapshot,
) {
    metrics::gauge!("sync.header_chain.frontier.finalized_height")
        .set(f64::from(snapshot.frontiers.finalized.height.0));
    metrics::gauge!("sync.header_chain.frontier.header_best_height")
        .set(f64::from(snapshot.frontiers.header_best.height.0));
    metrics::gauge!("sync.header_chain.frontier.verified_best_height")
        .set(f64::from(snapshot.frontiers.verified_best.height.0));
    metrics::gauge!("sync.header_chain.frontier.divergence").set(f64::from(
        snapshot
            .frontiers
            .header_best
            .height
            .0
            .saturating_sub(snapshot.frontiers.verified_best.height.0),
    ));
    // Metrics expose approximate floating-point generations.
    // The durable counters retain the exact generation values.
    metrics::gauge!("sync.header_chain.generation.header")
        .set(snapshot.header_generation.get() as f64);
    metrics::gauge!("sync.header_chain.generation.verified")
        .set(snapshot.verified_generation.get() as f64);
    metrics::gauge!("sync.header_chain.alarm.resource_stalled").set(
        if snapshot.alarms.resource_stalled {
            1.0
        } else {
            0.0
        },
    );
    metrics::gauge!("sync.header_chain.alarm.migrated_pin_refuted").set(
        if snapshot.alarms.migrated_pin_refuted.is_some() {
            1.0
        } else {
            0.0
        },
    );

    tracing::debug!(
        mode = ?snapshot.mode,
        state_version = snapshot.state_version.get(),
        header_generation = snapshot.header_generation.get(),
        verified_generation = snapshot.verified_generation.get(),
        finalized_height = snapshot.frontiers.finalized.height.0,
        finalized_hash = ?snapshot.frontiers.finalized.hash,
        header_best_height = snapshot.frontiers.header_best.height.0,
        header_best_hash = ?snapshot.frontiers.header_best.hash,
        verified_best_height = snapshot.frontiers.verified_best.height.0,
        verified_best_hash = ?snapshot.frontiers.verified_best.hash,
        resource_stalled = snapshot.alarms.resource_stalled,
        body_unavailable = snapshot
            .alarms
            .header_best_body_unavailable
            .is_some_and(|alarm| alarm.alarmed),
        migrated_pin_refuted = ?snapshot.alarms.migrated_pin_refuted,
        "published committed Zakura header-chain snapshot"
    );
}
