use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

use zakura_header_chain::{audit_store, EngineConfig, EngineSnapshot, RecoveryRepair};
#[cfg(any(test, feature = "header-fuzz"))]
use zakura_header_chain::{
    audit_store_for_trust_anchor_update, EngineMode, FinalityRecord, FinalitySource, Frontier,
    RecoveryPlan,
};

#[cfg(test)]
use super::FaultPoint;
use super::{
    load_transition_engine, HeaderChainRuntime, HeaderChainSnapshotPublisher, HeaderChainStore,
    HeaderChainStoreError, RetainedPathLeaseRegistry, StartupReport,
};
#[cfg(any(test, feature = "header-fuzz"))]
use super::{
    DiskWriteBatch, HeaderFinalityKey, IntoDisk, HEADER_ENGINE_META, HEADER_FINALITY_HISTORY,
    METADATA_KEY,
};

mod reconstruction;

impl HeaderChainStore {
    /// Exhaustively audit and repair reconstructible rows before creating any publisher.
    ///
    /// A successful return is the publication permission: every returned runtime was built only
    /// after the final exhaustive audit and all required repairs completed.
    #[cfg(any(test, feature = "header-fuzz"))]
    pub fn startup(
        self,
        config: &EngineConfig,
    ) -> Result<(HeaderChainRuntime, StartupReport), HeaderChainStoreError> {
        #[cfg(test)]
        {
            self.startup_inner(config, |_| Ok(()))
        }
        #[cfg(not(test))]
        {
            self.startup_inner(config)
        }
    }

    #[cfg(test)]
    pub(in crate::service::finalized_state::header_chain) fn startup_with_fault<F>(
        self,
        config: &EngineConfig,
        fault: F,
    ) -> Result<(HeaderChainRuntime, StartupReport), HeaderChainStoreError>
    where
        F: FnMut(FaultPoint) -> Result<(), HeaderChainStoreError>,
    {
        self.startup_inner(config, fault)
    }

    #[cfg(any(test, feature = "header-fuzz"))]
    pub(in crate::service::finalized_state::header_chain) fn startup_inner(
        self,
        config: &EngineConfig,
        #[cfg(test)] mut fault: impl FnMut(FaultPoint) -> Result<(), HeaderChainStoreError>,
    ) -> Result<(HeaderChainRuntime, StartupReport), HeaderChainStoreError> {
        let writer_lock = Arc::clone(&self.writer);
        let writer = writer_lock
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?;
        let plan = audit_store_for_trust_anchor_update(&self, config)?;
        if let Some(pin) = plan.metadata.alarms.migrated_pin_refuted {
            return Err(HeaderChainStoreError::MigratedPinRefuted { pin });
        }
        if !plan.is_clean() {
            #[cfg(test)]
            fault(FaultPoint::BeforeCommit)?;
            self.db.write(self.recovery_batch(&plan)?)?;
            #[cfg(test)]
            fault(FaultPoint::AfterCommit)?;
        }
        let RecoveryPlan {
            snapshot_before_repair: previous,
            repairs,
            ..
        } = plan;
        let runtime = self.finalize_audited_runtime(config, previous, repairs, &writer)?;
        #[cfg(test)]
        fault(FaultPoint::AfterPublish)?;
        drop(writer);
        Ok(runtime)
    }

    /// Explicitly preserve a headers-only store's pins while changing its durable mode.
    #[cfg(any(test, feature = "header-fuzz"))]
    pub fn migrate_headers_only_to_integrated(
        self,
        integrated_config: &EngineConfig,
        full_state_verified: Frontier,
    ) -> Result<(HeaderChainRuntime, StartupReport), HeaderChainStoreError> {
        if integrated_config.mode != EngineMode::Integrated {
            return Err(HeaderChainStoreError::Incoherent(
                "mode migration target is not integrated",
            ));
        }
        let mut headers_only_config = integrated_config.clone();
        headers_only_config.mode = EngineMode::HeadersOnly;
        let writer_lock = Arc::clone(&self.writer);
        let writer = writer_lock
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?;
        let source = audit_store_for_trust_anchor_update(&self, &headers_only_config)?;
        if let Some(pin) = source.metadata.alarms.migrated_pin_refuted {
            return Err(HeaderChainStoreError::MigratedPinRefuted { pin });
        }
        if source.metadata.frontiers.finalized != full_state_verified {
            return Err(HeaderChainStoreError::Incoherent(
                "integrated migration requires full-state verification through the preserved pin",
            ));
        }
        if !source.is_clean() {
            self.db.write(self.recovery_batch(&source)?)?;
        }
        let RecoveryPlan {
            snapshot_before_repair: previous,
            repairs,
            ..
        } = source;

        let history = self.finality_history()?;
        let mut metadata = self.metadata()?;
        metadata.mode = EngineMode::Integrated;
        metadata.headers_only_migration_epoch = Some(metadata.finality_epoch);
        metadata.state_version = metadata.state_version.checked_next()?;
        metadata.header_generation = metadata.header_generation.checked_next()?;
        metadata.verified_generation = metadata.verified_generation.checked_next()?;
        metadata.last_transition = None;

        let mut batch = DiskWriteBatch::new();
        for record in history.into_iter().map(preserve_headers_only_pin) {
            self.put_value(
                &mut batch,
                HEADER_FINALITY_HISTORY,
                HeaderFinalityKey(record.epoch).as_bytes(),
                &record,
            )?;
        }
        self.put_value(&mut batch, HEADER_ENGINE_META, METADATA_KEY, &metadata)?;
        self.db.write(batch)?;

        let runtime =
            self.finalize_audited_runtime(integrated_config, previous, repairs, &writer)?;
        drop(writer);
        Ok(runtime)
    }

    /// Finish startup only from a clean exhaustive audit while the writer boundary is held.
    fn finalize_audited_runtime(
        self,
        config: &EngineConfig,
        previous: EngineSnapshot,
        mut repairs: BTreeSet<RecoveryRepair>,
        _writer: &std::sync::MutexGuard<'_, ()>,
    ) -> Result<(HeaderChainRuntime, StartupReport), HeaderChainStoreError> {
        let mut final_audit = audit_store(&self, config)?;
        repairs.extend(final_audit.repairs.iter().copied());
        if !final_audit.is_clean() {
            self.db.write(self.recovery_batch(&final_audit)?)?;
            final_audit = audit_store(&self, config)?;
            if !final_audit.is_clean() {
                return Err(HeaderChainStoreError::Incoherent(
                    "startup repair did not produce a clean audited store",
                ));
            }
        }

        let transition_engine = Arc::new(Mutex::new(load_transition_engine(&self)?));
        let current = final_audit.metadata.snapshot();
        let report = StartupReport {
            previous,
            current: current.clone(),
            repairs,
        };
        let publisher = HeaderChainSnapshotPublisher::new(current);
        Ok((
            HeaderChainRuntime {
                store: self,
                config: config.clone(),
                publisher,
                leases: Arc::new(Mutex::new(RetainedPathLeaseRegistry::default())),
                transition_engine,
            },
            report,
        ))
    }
}

#[cfg(any(test, feature = "header-fuzz"))]
pub(in crate::service::finalized_state::header_chain) fn preserve_headers_only_pin(
    mut record: FinalityRecord,
) -> FinalityRecord {
    if matches!(record.source, FinalitySource::HeadersOnlyDepth { .. }) {
        record.source = FinalitySource::MigratedHeadersOnly;
    }
    record
}
