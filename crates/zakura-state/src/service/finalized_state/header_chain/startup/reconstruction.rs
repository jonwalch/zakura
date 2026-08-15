use std::sync::Arc;

use sha2::{Digest, Sha256};
use zakura_chain::block;
use zakura_header_chain::{
    audit_store_for_trust_anchor_update, EngineConfig, EvidenceId, Frontier,
    FullStateEvidenceAuthority, FullStateFinalized, HeaderValidationFacts, RecoveryPlan,
    StoreAuditRead, SystemClock, TransitionContext, TransitionEvent, TransitionInput,
    ValidationLease, VerifiedChainChanged, VerifiedChangeCause, VerifiedHeaderRef,
};

use super::super::{
    load_transition_engine, DiskWriteBatch, HeaderChainRuntime, HeaderChainStore,
    HeaderChainStoreError, HeaderReconstructionPhaseDisk, HeaderReconstructionProgressDisk,
    StartupReport, HEADER_ENGINE_META, RECONSTRUCTION_PROGRESS_KEY,
};

impl HeaderChainStore {
    /// Audit and reconcile the exact restored full-state path before enabling publication.
    pub(in crate::service) fn startup_reconciled(
        self,
        config: &EngineConfig,
        full_state_finalized: Frontier,
        finalized_path: Vec<VerifiedHeaderRef>,
        restored_path: Vec<VerifiedHeaderRef>,
    ) -> Result<(HeaderChainRuntime, StartupReport), HeaderChainStoreError> {
        let writer_lock = Arc::clone(&self.writer);
        let writer = writer_lock
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?;
        let initial = audit_store_for_trust_anchor_update(&self, config)?;
        if let Some(pin) = initial.metadata.alarms.migrated_pin_refuted {
            return Err(HeaderChainStoreError::MigratedPinRefuted { pin });
        }
        if !initial.is_clean() {
            self.db.write(self.recovery_batch(&initial)?)?;
        }
        let RecoveryPlan {
            snapshot_before_repair: previous,
            repairs,
            ..
        } = initial;

        let max_nodes = config.limits.max_non_finalized_nodes.get();
        if finalized_path.len().saturating_add(restored_path.len()) > max_nodes {
            if restored_path.len() > max_nodes {
                return Err(HeaderChainStoreError::Incoherent(
                    "restored verified path exceeds the non-finalized node limit",
                ));
            }
            for chunk in finalized_path.chunks(max_nodes) {
                let chunk = chunk.to_vec();
                let chunk_tip = chunk
                    .last()
                    .map(|header| Frontier::new(header.height, header.hash))
                    .ok_or(HeaderChainStoreError::Incoherent(
                        "oversized reconciliation has an empty finalized chunk",
                    ))?;
                self.reconcile_verified_path(config, chunk)?;
                self.reconcile_finalized(config, chunk_tip)?;
            }
            self.reconcile_verified_path(config, restored_path)?;
        } else {
            let mut authoritative_path = finalized_path;
            authoritative_path.extend(restored_path);
            self.reconcile_verified_path(config, authoritative_path)?;
            self.reconcile_finalized(config, full_state_finalized)?;
        }

        let runtime = self.finalize_audited_runtime(config, previous, repairs, &writer)?;
        drop(writer);
        Ok(runtime)
    }

    /// Resume bounded canonical reconstruction without materializing finalized history.
    pub(in crate::service) fn startup_reconciled_streaming<F, P>(
        self,
        config: &EngineConfig,
        full_state_finalized: Frontier,
        restored_path: Vec<VerifiedHeaderRef>,
        mut canonical_header: F,
        mut report_progress: P,
    ) -> Result<(HeaderChainRuntime, StartupReport), HeaderChainStoreError>
    where
        F: FnMut(block::Height) -> Result<VerifiedHeaderRef, HeaderChainStoreError>,
        P: FnMut(zakura_node_services::sync_lifecycle::HeaderReconstructionProgress),
    {
        use zakura_node_services::sync_lifecycle::{
            HeaderReconstructionProgress, HeaderReconstructionStage,
        };

        let writer_lock = Arc::clone(&self.writer);
        let writer = writer_lock
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?;
        let initial = audit_store_for_trust_anchor_update(&self, config)?;
        if let Some(pin) = initial.metadata.alarms.migrated_pin_refuted {
            return Err(HeaderChainStoreError::MigratedPinRefuted { pin });
        }
        if !initial.is_clean() {
            self.db.write(self.recovery_batch(&initial)?)?;
        }
        let RecoveryPlan {
            snapshot_before_repair: previous,
            repairs,
            ..
        } = initial;

        let snapshot = self.snapshot()?;
        if snapshot.frontiers.finalized.height > full_state_finalized.height {
            return Err(HeaderChainStoreError::Incoherent(
                "header reconstruction target is below durable finality",
            ));
        }
        let base = snapshot.frontiers.finalized;
        let network = config.network.kind();
        let mut progress = match self.reconstruction_progress()? {
            Some(mut progress) => {
                if progress.network != network
                    || progress.last_committed != snapshot.frontiers.finalized
                    || progress.target.height > full_state_finalized.height
                    || progress.last_committed.height > progress.target.height
                    || (progress.target.height == full_state_finalized.height
                        && progress.target.hash != full_state_finalized.hash)
                {
                    return Err(HeaderChainStoreError::Incoherent(
                        "invalid durable header reconstruction progress",
                    ));
                }
                let expected_next = progress
                    .last_committed
                    .height
                    .next()
                    .unwrap_or(progress.last_committed.height);
                if progress.next_height != expected_next {
                    return Err(HeaderChainStoreError::Incoherent(
                        "header reconstruction progress has a discontinuous next height",
                    ));
                }
                if progress.target.height < full_state_finalized.height {
                    progress.phase = HeaderReconstructionPhaseDisk::FinalizedPath;
                } else {
                    match progress.phase {
                        HeaderReconstructionPhaseDisk::FinalizedPath => {}
                        HeaderReconstructionPhaseDisk::RestoredPath
                        | HeaderReconstructionPhaseDisk::FinalAudit
                            if progress.last_committed == progress.target => {}
                        HeaderReconstructionPhaseDisk::RestoredPath
                        | HeaderReconstructionPhaseDisk::FinalAudit => {
                            return Err(HeaderChainStoreError::Incoherent(
                                "terminal header reconstruction phase precedes its target",
                            ));
                        }
                    }
                }
                progress.target = full_state_finalized;
                progress
            }
            None => HeaderReconstructionProgressDisk {
                network,
                target: full_state_finalized,
                next_height: snapshot
                    .frontiers
                    .finalized
                    .height
                    .next()
                    .unwrap_or(snapshot.frontiers.finalized.height),
                phase: HeaderReconstructionPhaseDisk::FinalizedPath,
                last_committed: snapshot.frontiers.finalized,
            },
        };
        self.write_reconstruction_progress(&progress)?;

        let finalized_total =
            u64::from(full_state_finalized.height.0.saturating_sub(base.height.0));
        let restored_total = u64::try_from(restored_path.len()).unwrap_or(u64::MAX);
        let total = finalized_total.saturating_add(restored_total);
        report_progress(HeaderReconstructionProgress {
            stage: HeaderReconstructionStage::FullStateReconciliation,
            completed: 0,
            total: Some(total),
            target: Some(full_state_finalized),
            last_committed: Some(progress.last_committed),
        });

        if progress.phase == HeaderReconstructionPhaseDisk::FinalizedPath {
            let max_nodes = config.limits.max_non_finalized_nodes.get();
            while progress.last_committed.height < full_state_finalized.height {
                let remaining = full_state_finalized
                    .height
                    .0
                    .saturating_sub(progress.last_committed.height.0);
                let chunk_len = usize::try_from(remaining)
                    .unwrap_or(usize::MAX)
                    .min(max_nodes);
                let mut chunk = Vec::with_capacity(chunk_len);
                let mut expected_parent = progress.last_committed.hash;
                for offset in 0..chunk_len {
                    let offset = u32::try_from(offset).map_err(|_| {
                        HeaderChainStoreError::Incoherent("reconstruction chunk is too large")
                    })?;
                    let height = block::Height(
                        progress
                            .last_committed
                            .height
                            .0
                            .checked_add(offset.saturating_add(1))
                            .ok_or(HeaderChainStoreError::Incoherent(
                                "header reconstruction height overflow",
                            ))?,
                    );
                    let header = canonical_header(height)?;
                    if header.height != height
                        || header.header.previous_block_hash != expected_parent
                    {
                        return Err(HeaderChainStoreError::Incoherent(
                            "canonical reconstruction chunk is discontinuous",
                        ));
                    }
                    expected_parent = header.hash;
                    chunk.push(header);
                }
                let chunk_tip = chunk
                    .last()
                    .map(|header| Frontier::new(header.height, header.hash))
                    .ok_or(HeaderChainStoreError::Incoherent(
                        "header reconstruction produced an empty chunk",
                    ))?;
                self.reconcile_verified_path(config, chunk)?;
                progress.last_committed = chunk_tip;
                progress.next_height = chunk_tip.height.next().unwrap_or(chunk_tip.height);
                self.reconcile_finalized_with_progress(config, chunk_tip, Some(&progress))?;
                report_progress(HeaderReconstructionProgress {
                    stage: HeaderReconstructionStage::FullStateReconciliation,
                    completed: u64::from(chunk_tip.height.0.saturating_sub(base.height.0)),
                    total: Some(total),
                    target: Some(full_state_finalized),
                    last_committed: Some(chunk_tip),
                });
            }
        }

        progress.phase = HeaderReconstructionPhaseDisk::RestoredPath;
        self.write_reconstruction_progress(&progress)?;
        self.reconcile_verified_path(config, restored_path)?;
        progress.phase = HeaderReconstructionPhaseDisk::FinalAudit;
        self.write_reconstruction_progress(&progress)?;
        report_progress(HeaderReconstructionProgress {
            stage: HeaderReconstructionStage::FullStateReconciliation,
            completed: total,
            total: Some(total),
            target: Some(full_state_finalized),
            last_committed: Some(progress.last_committed),
        });

        if progress.last_committed != full_state_finalized
            || self.snapshot()?.frontiers.finalized != full_state_finalized
        {
            return Err(HeaderChainStoreError::Incoherent(
                "header reconstruction did not reach its full-state target",
            ));
        }
        self.clear_reconstruction_progress()?;
        let runtime = self.finalize_audited_runtime(config, previous, repairs, &writer)?;
        drop(writer);
        Ok(runtime)
    }

    pub(in crate::service::finalized_state::header_chain) fn reconcile_verified_path(
        &self,
        config: &EngineConfig,
        authoritative_path: Vec<VerifiedHeaderRef>,
    ) -> Result<(), HeaderChainStoreError> {
        struct Authority {
            event: zakura_header_chain::TransitionFingerprint,
            validation_context: [u8; 32],
        }

        impl FullStateEvidenceAuthority for Authority {
            fn authorizes_full_state(&self, event: &TransitionEvent) -> bool {
                event.fingerprint() == Some(self.event)
            }

            fn authorizes_validation_lease(&self, lease: &ValidationLease) -> bool {
                lease.context_digest() == self.validation_context
            }
        }

        let snapshot = self.snapshot()?;
        let mut expected_projection = vec![snapshot.frontiers.finalized];
        expected_projection.extend(
            authoritative_path
                .iter()
                .map(|header| Frontier::new(header.height, header.hash)),
        );
        if self.verified_projection()? == expected_projection {
            return Ok(());
        }
        let mut hasher = Sha256::new();
        hasher.update(b"zakura-header-chain-startup-reconciliation-v1");
        hasher.update(snapshot.state_version.get().to_be_bytes());
        hasher.update(snapshot.frontiers.verified_best.hash.0);
        for header in &authoritative_path {
            hasher.update(header.height.0.to_be_bytes());
            hasher.update(header.hash.0);
        }
        let evidence = EvidenceId::from_digest(hasher.finalize().into());
        let event = TransitionEvent::VerifiedChainChanged(VerifiedChainChanged {
            full_state_transition_id: evidence,
            old_tip: snapshot.frontiers.verified_best,
            new_path: authoritative_path,
            cause: VerifiedChangeCause::Reset,
        });
        let validation_context =
            self.validation_context(snapshot.frontiers.finalized.hash, &config.network)?;
        let authority = Authority {
            event: event
                .fingerprint()
                .expect("startup reconciliation carries stable evidence"),
            validation_context: validation_context.context_digest(),
        };
        let context = TransitionContext {
            config,
            clock: &SystemClock,
            full_state_authority: Some(&authority),
            retention_references: &[],
        };
        let engine = load_transition_engine(self)?;
        let TransitionEvent::VerifiedChainChanged(event) = event else {
            unreachable!("startup reconciliation constructs VerifiedChainChanged");
        };
        let transition = engine.plan_transition(
            TransitionInput::VerifiedChainChanged {
                expected_version: snapshot.state_version,
                event,
                facts: HeaderValidationFacts {
                    validation_leases: vec![validation_context],
                },
            },
            &context,
        )?;
        if !transition.is_no_change() {
            self.db.write(self.batch_for(transition.change_set())?)?;
        }
        Ok(())
    }

    pub(in crate::service::finalized_state::header_chain) fn reconcile_finalized(
        &self,
        config: &EngineConfig,
        full_state_finalized: Frontier,
    ) -> Result<(), HeaderChainStoreError> {
        self.reconcile_finalized_with_progress(config, full_state_finalized, None)
    }

    pub(in crate::service::finalized_state::header_chain) fn reconcile_finalized_with_progress(
        &self,
        config: &EngineConfig,
        full_state_finalized: Frontier,
        progress: Option<&HeaderReconstructionProgressDisk>,
    ) -> Result<(), HeaderChainStoreError> {
        struct Authority(zakura_header_chain::TransitionFingerprint);

        impl FullStateEvidenceAuthority for Authority {
            fn authorizes_full_state(&self, event: &TransitionEvent) -> bool {
                event.fingerprint() == Some(self.0)
            }
        }

        let snapshot = self.snapshot()?;
        if snapshot.frontiers.finalized == full_state_finalized {
            if let Some(progress) = progress {
                self.write_reconstruction_progress(progress)?;
            }
            return Ok(());
        }
        let proof = self
            .verified_projection()?
            .into_iter()
            .take_while(|frontier| frontier.height <= full_state_finalized.height)
            .map(|frontier| frontier.hash)
            .collect::<Vec<_>>();
        let mut hasher = Sha256::new();
        hasher.update(b"zakura-header-chain-startup-finalization-v1");
        hasher.update(snapshot.state_version.get().to_be_bytes());
        hasher.update(full_state_finalized.height.0.to_be_bytes());
        hasher.update(full_state_finalized.hash.0);
        for hash in &proof {
            hasher.update(hash.0);
        }
        let evidence = EvidenceId::from_digest(hasher.finalize().into());
        let event = TransitionEvent::FullStateFinalized(FullStateFinalized {
            full_state_transition_id: evidence,
            new_finalized: full_state_finalized,
            verified_path_proof: proof,
        });
        let authority = Authority(
            event
                .fingerprint()
                .expect("startup finalization carries stable evidence"),
        );
        let context = TransitionContext {
            config,
            clock: &SystemClock,
            full_state_authority: Some(&authority),
            retention_references: &[],
        };
        let engine = load_transition_engine(self)?;
        let TransitionEvent::FullStateFinalized(event) = event else {
            unreachable!("startup finalization constructs FullStateFinalized");
        };
        let transition = engine.plan_transition(
            TransitionInput::FullStateFinalized {
                expected_version: snapshot.state_version,
                event,
            },
            &context,
        )?;
        if !transition.is_no_change() {
            let mut batch = DiskWriteBatch::new();
            if let Some(progress) = progress {
                self.put_value(
                    &mut batch,
                    HEADER_ENGINE_META,
                    RECONSTRUCTION_PROGRESS_KEY,
                    progress,
                )?;
            }
            self.db
                .write(self.batch_for_combined(transition.change_set(), batch)?)?;
        } else if let Some(progress) = progress {
            self.write_reconstruction_progress(progress)?;
        }
        Ok(())
    }

    pub(in crate::service::finalized_state::header_chain) fn reconstruction_progress(
        &self,
    ) -> Result<Option<HeaderReconstructionProgressDisk>, HeaderChainStoreError> {
        self.get_value(HEADER_ENGINE_META, RECONSTRUCTION_PROGRESS_KEY)
    }

    pub(in crate::service::finalized_state::header_chain) fn write_reconstruction_progress(
        &self,
        progress: &HeaderReconstructionProgressDisk,
    ) -> Result<(), HeaderChainStoreError> {
        let mut batch = DiskWriteBatch::new();
        self.put_value(
            &mut batch,
            HEADER_ENGINE_META,
            RECONSTRUCTION_PROGRESS_KEY,
            progress,
        )?;
        self.db.write(batch)?;
        Ok(())
    }

    pub(in crate::service::finalized_state::header_chain) fn clear_reconstruction_progress(
        &self,
    ) -> Result<(), HeaderChainStoreError> {
        let mut batch = DiskWriteBatch::new();
        self.delete_raw(&mut batch, HEADER_ENGINE_META, RECONSTRUCTION_PROGRESS_KEY)?;
        self.db.write(batch)?;
        Ok(())
    }
}
