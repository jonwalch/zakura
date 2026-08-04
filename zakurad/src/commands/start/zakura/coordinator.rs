use std::{
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use tokio::sync::{watch, Notify};
use zakura_node_services::sync_lifecycle::{
    ApplyPhase, ApplyTransition, LifecycleEpoch, LifecycleTransitionError,
};

/// Sole process-local owner of bulk block-apply lifecycle transitions.
#[derive(Debug)]
pub(crate) struct SyncCoordinator {
    phase: Mutex<ApplyPhase>,
    phase_tx: watch::Sender<ApplyPhase>,
    in_flight: std::sync::atomic::AtomicUsize,
    drained: Notify,
    phase_changed: Notify,
}

/// Tracks one in-flight native block apply.
#[derive(Debug)]
pub(crate) struct BlockApplyPermit(Arc<SyncCoordinator>);

/// Exclusive authorization for one fully drained legacy fallback round.
#[derive(Debug)]
pub(crate) struct LegacyFallbackLease {
    coordinator: Arc<SyncCoordinator>,
    epoch: LifecycleEpoch,
}

impl SyncCoordinator {
    /// Start with native Zakura sync authorized to apply bodies.
    pub(crate) fn new() -> Arc<Self> {
        Self::new_with_phase(ApplyPhase::Native {
            epoch: LifecycleEpoch::INITIAL,
        })
    }

    /// Start with legacy checkpoint bootstrap authorized until semantic handoff.
    pub(crate) fn new_legacy_bootstrap() -> Arc<Self> {
        Self::new_with_phase(ApplyPhase::LegacyBootstrap {
            epoch: LifecycleEpoch::INITIAL,
        })
    }

    fn new_with_phase(phase: ApplyPhase) -> Arc<Self> {
        let (phase_tx, _phase_rx) = watch::channel(phase);
        let coordinator = Arc::new(Self {
            phase: Mutex::new(phase),
            phase_tx,
            in_flight: std::sync::atomic::AtomicUsize::new(0),
            drained: Notify::new(),
            phase_changed: Notify::new(),
        });
        coordinator.publish_phase(phase);
        coordinator
    }

    /// Return the current authoritative apply phase.
    pub(crate) fn apply_phase(&self) -> ApplyPhase {
        *self.lock_phase()
    }

    /// Whether fallback is draining or has acquired exclusive legacy authorization.
    pub(crate) fn is_yielded_to_legacy(&self) -> bool {
        matches!(
            self.apply_phase(),
            ApplyPhase::FallbackDraining { .. } | ApplyPhase::LegacyFallback { .. }
        )
    }

    /// Whether native Zakura sync currently owns block applies.
    pub(crate) fn zakura_owns_applies(&self) -> bool {
        matches!(self.apply_phase(), ApplyPhase::Native { .. })
    }

    /// Transfers initial block-apply ownership to native Zakura exactly once.
    pub(crate) fn finish_legacy_bootstrap(&self) -> Result<(), LifecycleTransitionError> {
        self.transition(ApplyTransition::FinishBootstrap)
            .map(|_| ())
    }

    /// Wait until bootstrap completes or fallback owns/drains the apply pipeline.
    pub(crate) async fn wait_for_zakura_ownership(&self) {
        loop {
            let changed = self.phase_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if matches!(
                self.apply_phase(),
                ApplyPhase::Native { .. }
                    | ApplyPhase::FallbackDraining { .. }
                    | ApplyPhase::LegacyFallback { .. }
                    | ApplyPhase::Failed { .. }
            ) {
                return;
            }
            changed.await;
        }
    }

    /// Reserve one apply in the exact current native epoch.
    pub(crate) fn begin_apply(self: &Arc<Self>) -> Option<BlockApplyPermit> {
        let initial = self.apply_phase();
        if !matches!(initial, ApplyPhase::Native { .. }) {
            return None;
        }

        self.in_flight
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // Reserve before rechecking the complete phase+epoch. Concurrent fallback
        // either observes this permit in the drain count or makes this reservation
        // self-reject and release here.
        if self.apply_phase() != initial {
            self.release_apply();
            return None;
        }

        Some(BlockApplyPermit(self.clone()))
    }

    fn release_apply(&self) {
        if self
            .in_flight
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
            == 1
        {
            self.drained.notify_waiters();
        }
    }

    /// Stop native admission, drain the exact epoch, then authorize one legacy round.
    pub(crate) async fn acquire_legacy_fallback(
        self: &Arc<Self>,
        diagnostic_interval: Duration,
    ) -> Result<LegacyFallbackLease, LifecycleTransitionError> {
        let ApplyPhase::Native { epoch } = self.apply_phase() else {
            return Err(LifecycleTransitionError::IllegalPhase);
        };
        self.transition(ApplyTransition::BeginFallback {
            expected_epoch: epoch,
        })?;
        let lease = LegacyFallbackLease {
            coordinator: self.clone(),
            epoch,
        };
        self.wait_for_applies(diagnostic_interval).await;
        self.transition(ApplyTransition::ActivateFallback {
            expected_epoch: epoch,
        })?;
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
                    apply_epoch = self.apply_phase().epoch().get(),
                    elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                    "native block applies remain active while fallback waits for its exclusive lease"
                );
            }
        }
    }

    fn transition(
        &self,
        transition: ApplyTransition,
    ) -> Result<ApplyPhase, LifecycleTransitionError> {
        let mut phase = self.lock_phase();
        let previous = *phase;
        let next = match previous.transition(transition) {
            Ok(next) => next,
            Err(LifecycleTransitionError::EpochExhausted) => {
                let failed = ApplyPhase::Failed {
                    epoch: previous.epoch(),
                };
                *phase = failed;
                drop(phase);
                self.publish_phase(failed);
                return Err(LifecycleTransitionError::EpochExhausted);
            }
            Err(error) => return Err(error),
        };
        *phase = next;
        drop(phase);
        self.publish_phase(next);
        Ok(next)
    }

    fn lock_phase(&self) -> MutexGuard<'_, ApplyPhase> {
        self.phase.lock().unwrap_or_else(|poisoned| {
            let mut phase = poisoned.into_inner();
            *phase = ApplyPhase::Failed {
                epoch: phase.epoch(),
            };
            phase
        })
    }

    fn publish_phase(&self, phase: ApplyPhase) {
        self.phase_tx.send_replace(phase);
        self.phase_changed.notify_waiters();
        // This diagnostic gauge may round epochs above f64's exact integer range;
        // lifecycle authority always uses the original checked u64 value.
        metrics::gauge!("sync.zakura.apply.epoch").set(phase.epoch().get() as f64);
        metrics::gauge!("sync.zakura.apply.phase").set(match phase {
            ApplyPhase::LegacyBootstrap { .. } => 0.0,
            ApplyPhase::Native { .. } => 1.0,
            ApplyPhase::FallbackDraining { .. } => 2.0,
            ApplyPhase::LegacyFallback { .. } => 3.0,
            ApplyPhase::Failed { .. } => 4.0,
        });
        tracing::info!(
            apply_phase = phase.label(),
            apply_epoch = phase.epoch().get(),
            "sync apply lifecycle changed"
        );
    }
}

impl Drop for BlockApplyPermit {
    fn drop(&mut self) {
        self.0.release_apply();
    }
}

impl Drop for LegacyFallbackLease {
    fn drop(&mut self) {
        if let Err(error) = self.coordinator.transition(ApplyTransition::ResumeNative {
            expected_epoch: self.epoch,
        }) {
            tracing::error!(
                ?error,
                fallback_epoch = self.epoch.get(),
                current_phase = ?self.coordinator.apply_phase(),
                "legacy fallback lease could not restore native apply ownership"
            );
        }
        metrics::gauge!("sync.zakura.legacy_fallback.active").set(0.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stale_fallback_lease_cannot_change_a_new_epoch() {
        let coordinator = SyncCoordinator::new();
        let first = coordinator
            .acquire_legacy_fallback(Duration::from_millis(1))
            .await
            .expect("the initial native epoch drains");
        assert!(matches!(
            coordinator.apply_phase(),
            ApplyPhase::LegacyFallback {
                epoch: LifecycleEpoch::INITIAL
            }
        ));
        drop(first);
        let resumed = coordinator.apply_phase();
        assert!(matches!(resumed, ApplyPhase::Native { .. }));

        let stale = LegacyFallbackLease {
            coordinator: coordinator.clone(),
            epoch: LifecycleEpoch::INITIAL,
        };
        drop(stale);
        assert_eq!(coordinator.apply_phase(), resumed);
    }

    #[tokio::test]
    async fn phase_receiver_observes_bootstrap_and_fallback_epochs() {
        let coordinator = SyncCoordinator::new_legacy_bootstrap();
        let mut phases = coordinator.phase_tx.subscribe();
        coordinator
            .finish_legacy_bootstrap()
            .expect("bootstrap advances to native");
        phases
            .changed()
            .await
            .expect("coordinator publisher is live");
        let native = *phases.borrow_and_update();
        let ApplyPhase::Native { epoch } = native else {
            panic!("bootstrap publishes native ownership");
        };
        let lease = coordinator
            .acquire_legacy_fallback(Duration::from_millis(1))
            .await
            .expect("native ownership drains");
        assert!(matches!(
            *phases.borrow(),
            ApplyPhase::LegacyFallback { epoch: current } if current == epoch
        ));
        drop(lease);
        assert!(matches!(
            *phases.borrow(),
            ApplyPhase::Native { epoch: current } if current == epoch.checked_next().expect("test epoch advances")
        ));
    }
}
