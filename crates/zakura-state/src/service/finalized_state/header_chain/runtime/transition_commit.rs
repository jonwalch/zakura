use std::{
    borrow::Cow,
    collections::{BTreeSet, HashMap, HashSet},
};

use tokio::time::Instant;
use zakura_chain::{block, parameters::Network};
use zakura_header_chain::{
    ApplyResult, AuxDelivery, CommittedStallReceipt, EngineSnapshot, Frontier,
    FullStateEvidenceAuthority, HeaderChainEngine, HeaderInsertionFacts, HeaderNode,
    HeaderSyncWorkOwner, HeaderValidationFacts, NoChangeReceipt, StaleReceipt, TransitionContext,
    TransitionEvent, TransitionFailure, TransitionInput, TransitionRequest, ValidationLease,
    VerifiedChangeCause, VerifiedHeaderRef,
};

#[cfg(test)]
use super::super::FaultPoint;
use super::super::{DiskWriteBatch, HeaderChainStoreError};
use super::{
    restore_transition_engine_after_staging_error, FullStateProjectionExpectation,
    HeaderChainRuntime,
};

struct DurableTransitionAuthority<'a> {
    inner: Option<&'a dyn FullStateEvidenceAuthority>,
    validation_leases: &'a [ValidationLease],
}

impl FullStateEvidenceAuthority for DurableTransitionAuthority<'_> {
    fn authorizes_full_state(&self, event: &TransitionEvent) -> bool {
        self.inner
            .is_some_and(|inner| inner.authorizes_full_state(event))
    }

    fn authorizes_scheduler_retry(&self, retry: &zakura_header_chain::OperatorBodyRetry) -> bool {
        self.inner
            .is_some_and(|inner| inner.authorizes_scheduler_retry(retry))
    }

    fn authorizes_header_completion(&self, insert: &zakura_header_chain::InsertHeaders) -> bool {
        self.inner
            .is_some_and(|inner| inner.authorizes_header_completion(insert))
    }

    fn authorizes_validation_lease(&self, lease: &ValidationLease) -> bool {
        self.validation_leases.contains(lease)
    }
}

pub(in crate::service::finalized_state::header_chain) fn combined_retention_references<'a>(
    context_references: &'a [block::Hash],
    active_lease_references: Option<&'a [block::Hash]>,
) -> Cow<'a, [block::Hash]> {
    let Some(active_lease_references) = active_lease_references else {
        return Cow::Borrowed(context_references);
    };
    if context_references.is_empty() {
        return Cow::Borrowed(active_lease_references);
    }

    let mut references = context_references.to_vec();
    references.extend(active_lease_references.iter().copied());
    references.sort_unstable_by_key(|hash| hash.0);
    references.dedup();
    Cow::Owned(references)
}

impl HeaderChainRuntime {
    /// Apply, commit, and publish one serialized transition.
    pub fn apply(
        &self,
        request: TransitionRequest,
        context: &TransitionContext<'_>,
    ) -> Result<ApplyResult, HeaderChainStoreError> {
        self.commit_with_full_state_batch(request, context, DiskWriteBatch::new(), || {})
    }

    pub(in crate::service) fn commit_with_full_state_batch<M>(
        &self,
        request: TransitionRequest,
        context: &TransitionContext<'_>,
        full_state_batch: DiskWriteBatch,
        memory_swap: M,
    ) -> Result<ApplyResult, HeaderChainStoreError>
    where
        M: FnOnce(),
    {
        #[cfg(test)]
        {
            self.commit_durable_fact_bound_transition(
                request,
                context,
                full_state_batch,
                memory_swap,
                FullStateProjectionExpectation::NONE,
                |_| Ok(()),
            )
        }
        #[cfg(not(test))]
        {
            self.commit_durable_fact_bound_transition(
                request,
                context,
                full_state_batch,
                memory_swap,
                FullStateProjectionExpectation::NONE,
            )
        }
    }

    pub(in crate::service) fn commit_expected_full_state_transition<M>(
        &self,
        request: TransitionRequest,
        context: &TransitionContext<'_>,
        full_state_batch: DiskWriteBatch,
        expected_verified: Frontier,
        expected_staged: &[VerifiedHeaderRef],
        memory_swap: M,
    ) -> Result<ApplyResult, HeaderChainStoreError>
    where
        M: FnOnce(),
    {
        #[cfg(test)]
        {
            self.commit_durable_fact_bound_transition(
                request,
                context,
                full_state_batch,
                memory_swap,
                FullStateProjectionExpectation {
                    verified: Some(expected_verified),
                    staged: expected_staged,
                },
                |_| Ok(()),
            )
        }
        #[cfg(not(test))]
        {
            self.commit_durable_fact_bound_transition(
                request,
                context,
                full_state_batch,
                memory_swap,
                FullStateProjectionExpectation {
                    verified: Some(expected_verified),
                    staged: expected_staged,
                },
            )
        }
    }

    /// Atomically apply auxiliary authentication followed by one checkpoint full-state advance.
    ///
    /// The planner creates the checkpoint transition against the projected auxiliary transition.
    /// One RocksDB write commits both header-chain changes and the full-state block batch.
    pub(in crate::service) fn commit_auxiliary_then_checkpoint<M>(
        &self,
        first_request: TransitionRequest,
        first_context: &TransitionContext<'_>,
        checkpoint_request: TransitionRequest,
        checkpoint_context: &TransitionContext<'_>,
        full_state_batch: DiskWriteBatch,
        memory_swap: M,
    ) -> Result<ApplyResult, HeaderChainStoreError>
    where
        M: FnOnce(),
    {
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?;
        let mut transition_engine = self
            .transition_engine
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?;
        let lease_references = self
            .leases
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?
            .active_references(Instant::now());

        let first_authority = DurableTransitionAuthority {
            inner: first_context.full_state_authority,
            validation_leases: &[],
        };
        let first_context = TransitionContext {
            config: first_context.config,
            clock: first_context.clock,
            full_state_authority: Some(&first_authority),
            retention_references: lease_references.as_ref(),
        };
        let checkpoint_parent = match &checkpoint_request.event {
            TransitionEvent::VerifiedChainChanged(event)
                if event.cause == VerifiedChangeCause::CheckpointFinalizedGrow =>
            {
                event.old_tip
            }
            _ => {
                return Err(HeaderChainStoreError::Incoherent(
                    "combined checkpoint transition has the wrong event kind",
                ));
            }
        };
        let checkpoint_headers_are_retained = match &checkpoint_request.event {
            TransitionEvent::VerifiedChainChanged(event) => event
                .new_path
                .iter()
                .all(|header| transition_engine.graph().header_node(header.hash).is_some()),
            _ => false,
        };
        // Header sync normally admits headers before native checkpoint growth promotes them.
        // Only a missing header needs contextual validation and a validation lease.
        let validation_leases = if checkpoint_headers_are_retained {
            Vec::new()
        } else {
            vec![self
                .store
                .validation_context(checkpoint_parent.hash, &self.config.network)?]
        };
        let checkpoint_authority = DurableTransitionAuthority {
            inner: checkpoint_context.full_state_authority,
            validation_leases: validation_leases.as_slice(),
        };
        let checkpoint_context = TransitionContext {
            config: checkpoint_context.config,
            clock: checkpoint_context.clock,
            full_state_authority: Some(&checkpoint_authority),
            retention_references: lease_references.as_ref(),
        };

        let TransitionEvent::AuxEvidence(first_event) = first_request.event else {
            return Err(HeaderChainStoreError::Incoherent(
                "combined auxiliary transition has the wrong event kind",
            ));
        };
        let first = transition_engine.plan_transition(
            TransitionInput::AuxEvidence { event: first_event },
            &first_context,
        )?;
        if first.effect().is_resource_stalled() {
            return Err(HeaderChainStoreError::Incoherent(
                "checkpoint auxiliary authentication exhausted header resources",
            ));
        }
        let batch = self
            .store
            .batch_for_combined(first.change_set(), full_state_batch)?;
        // The runtime uses the engine mutex as its coherent read boundary.
        // The publisher continues to expose the durable snapshot.
        // The writer can stage the first transition without exposing it.
        // The runtime reloads the unchanged durable engine after an error before the atomic write.
        if let Err(error) = transition_engine.install_committed_transition(first) {
            let error = restore_transition_engine_after_staging_error(
                &self.store,
                &mut transition_engine,
                error.into(),
            );
            return Err(error);
        }

        let expected_version = transition_engine.snapshot().state_version;
        let TransitionEvent::VerifiedChainChanged(checkpoint_event) = checkpoint_request.event
        else {
            return Err(HeaderChainStoreError::Incoherent(
                "combined checkpoint transition has the wrong event kind",
            ));
        };
        let checkpoint = match transition_engine.plan_transition(
            TransitionInput::VerifiedChainChanged {
                expected_version,
                event: checkpoint_event,
                facts: HeaderValidationFacts {
                    validation_leases: validation_leases.to_vec(),
                },
            },
            &checkpoint_context,
        ) {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                let error = restore_transition_engine_after_staging_error(
                    &self.store,
                    &mut transition_engine,
                    error.into(),
                );
                return Err(error);
            }
        };
        if checkpoint.effect().is_resource_stalled() {
            let error = restore_transition_engine_after_staging_error(
                &self.store,
                &mut transition_engine,
                HeaderChainStoreError::Incoherent(
                    "checkpoint full-state advance exhausted header resources",
                ),
            );
            return Err(error);
        }

        let current = checkpoint.snapshot_after_commit();
        let batch = match self
            .store
            .batch_for_combined(checkpoint.change_set(), batch)
        {
            Ok(batch) => batch,
            Err(error) => {
                let error = restore_transition_engine_after_staging_error(
                    &self.store,
                    &mut transition_engine,
                    error,
                );
                return Err(error);
            }
        };
        if let Err(error) = self.store.db.write(batch) {
            let error = restore_transition_engine_after_staging_error(
                &self.store,
                &mut transition_engine,
                error.into(),
            );
            return Err(error);
        }
        if let Err(error) = transition_engine.install_committed_transition(checkpoint) {
            let error = restore_transition_engine_after_staging_error(
                &self.store,
                &mut transition_engine,
                error.into(),
            );
            return Err(error);
        }
        memory_swap();
        self.publisher.publish(current);
        Ok(ApplyResult::Committed)
    }

    #[cfg(test)]
    pub(in crate::service::finalized_state::header_chain) fn commit_durable_fact_bound_transition_with_fault<
        M,
        F,
    >(
        &self,
        request: TransitionRequest,
        context: &TransitionContext<'_>,
        full_state_batch: DiskWriteBatch,
        memory_swap: M,
        fault: F,
    ) -> Result<ApplyResult, HeaderChainStoreError>
    where
        M: FnOnce(),
        F: FnMut(FaultPoint) -> Result<(), HeaderChainStoreError>,
    {
        self.commit_durable_fact_bound_transition(
            request,
            context,
            full_state_batch,
            memory_swap,
            FullStateProjectionExpectation::NONE,
            fault,
        )
    }

    /// Bind a state-service request to the exact durable facts its event may consume.
    pub(in crate::service::finalized_state::header_chain) fn bind_request_to_durable_facts(
        &self,
        request: TransitionRequest,
        before: &EngineSnapshot,
        network: &Network,
    ) -> Result<TransitionInput, HeaderChainStoreError> {
        let expected_version = request.expected_version;
        Ok(match request.event {
            TransitionEvent::InsertHeaders(event) => {
                let anchor_changed = event.owner.header_authority().branch.anchor_hash
                    != before.frontiers.finalized.hash;
                let mut validation_leases = Vec::new();
                if self.store.header_node(event.parent_hash)?.is_some() {
                    validation_leases
                        .push(self.store.validation_context(event.parent_hash, network)?);
                }
                if anchor_changed && event.parent_hash != before.frontiers.finalized.hash {
                    validation_leases.push(
                        self.store
                            .validation_context(before.frontiers.finalized.hash, network)?,
                    );
                }
                validation_leases.dedup_by_key(|lease| lease.parent());
                let finality_rebase_history = self.store.finality_rebase_history(
                    event.owner.header_authority().branch.anchor_hash,
                    before.frontiers.finalized,
                    before
                        .header_generation
                        .get()
                        .saturating_sub(event.owner.header_authority().header_generation.get()),
                )?;
                TransitionInput::InsertHeaders {
                    event,
                    facts: HeaderInsertionFacts {
                        validation: HeaderValidationFacts { validation_leases },
                        finality_rebase_history,
                    },
                }
            }
            TransitionEvent::VerifiedChainChanged(event) => {
                let parent = match event.cause {
                    VerifiedChangeCause::Grow | VerifiedChangeCause::CheckpointFinalizedGrow => {
                        event.old_tip
                    }
                    VerifiedChangeCause::Reset => before.frontiers.finalized,
                };
                TransitionInput::VerifiedChainChanged {
                    expected_version,
                    event,
                    facts: HeaderValidationFacts {
                        validation_leases: vec![self
                            .store
                            .validation_context(parent.hash, network)?],
                    },
                }
            }
            TransitionEvent::VerifiedBlockAccepted(event) => {
                TransitionInput::VerifiedBlockAccepted {
                    expected_version,
                    event,
                    facts: HeaderValidationFacts {
                        validation_leases: vec![self
                            .store
                            .validation_context(before.frontiers.finalized.hash, network)?],
                    },
                }
            }
            TransitionEvent::BodyEvidence(event) => TransitionInput::BodyEvidence {
                expected_version,
                event,
            },
            TransitionEvent::BodySupplierDiscovered(event) => {
                TransitionInput::BodySupplierDiscovered {
                    expected_version,
                    event,
                }
            }
            TransitionEvent::OperatorBodyRetry(event) => TransitionInput::OperatorBodyRetry {
                expected_version,
                event,
            },
            TransitionEvent::OperatorInvalidate(event) => TransitionInput::OperatorInvalidate {
                expected_version,
                event,
            },
            TransitionEvent::OperatorReconsider(event) => TransitionInput::OperatorReconsider {
                expected_version,
                event,
            },
            TransitionEvent::FullStateFinalized(event) => TransitionInput::FullStateFinalized {
                expected_version,
                event,
            },
            TransitionEvent::MigratedPinRefutation(event) => {
                let preserved_pin = self
                    .store
                    .is_migrated_finality_pin(event.pin)?
                    .then_some(event.pin);
                TransitionInput::MigratedPinRefutation {
                    expected_version,
                    event,
                    preserved_pin,
                }
            }
            TransitionEvent::AuxEvidence(event) => TransitionInput::AuxEvidence { event },
            TransitionEvent::ReevaluateDeferred => {
                TransitionInput::ReevaluateDeferred { expected_version }
            }
        })
    }

    /// Commit one request against facts read from the same serialized durable generation.
    ///
    /// The invariant is `plan -> batch -> durable write -> engine install -> memory swap ->
    /// publish`. No snapshot is published before its durable rows commit.
    fn commit_durable_fact_bound_transition<M>(
        &self,
        request: TransitionRequest,
        context: &TransitionContext<'_>,
        full_state_batch: DiskWriteBatch,
        memory_swap: M,
        expectation: FullStateProjectionExpectation<'_>,
        #[cfg(test)] mut fault: impl FnMut(FaultPoint) -> Result<(), HeaderChainStoreError>,
    ) -> Result<ApplyResult, HeaderChainStoreError>
    where
        M: FnOnce(),
    {
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?;
        let mut transition_engine = self
            .transition_engine
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?;
        let authoritative_full_state_fork_set = matches!(
            &request.event,
            TransitionEvent::VerifiedChainChanged(_) | TransitionEvent::VerifiedBlockAccepted(_)
        ) && context
            .full_state_authority
            .is_some_and(|authority| authority.authorizes_full_state(&request.event));
        let lease_references = if authoritative_full_state_fork_set {
            None
        } else {
            Some(
                self.leases
                    .lock()
                    .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?
                    .active_references(Instant::now()),
            )
        };
        let retention_references = combined_retention_references(
            context.retention_references,
            lease_references.as_deref(),
        );
        let base_context = TransitionContext {
            config: context.config,
            clock: context.clock,
            full_state_authority: context.full_state_authority,
            retention_references: retention_references.as_ref(),
        };
        let before = transition_engine.snapshot();
        if let Some(pin) = before.alarms.migrated_pin_refuted {
            return Err(HeaderChainStoreError::MigratedPinRefuted { pin });
        }
        let event = request.event.idempotency_key();
        let branch = request
            .event
            .header_sync_owner()
            .map(HeaderSyncWorkOwner::header_authority)
            .map(|authority| authority.branch)
            .or_else(|| request.event.body_owner().map(|owner| owner.branch));
        let input =
            self.bind_request_to_durable_facts(request, &before, &base_context.config.network)?;
        let validation_leases = input
            .header_validation_facts()
            .map(|facts| facts.validation_leases.clone())
            .unwrap_or_default();
        let state_authority = DurableTransitionAuthority {
            inner: base_context.full_state_authority,
            validation_leases: &validation_leases,
        };
        let transition_context = TransitionContext {
            config: base_context.config,
            clock: base_context.clock,
            full_state_authority: Some(&state_authority),
            retention_references: base_context.retention_references,
        };
        let transition = match transition_engine.plan_transition(input, &transition_context) {
            Ok(plan) => plan,
            Err(TransitionFailure::Stale { current }) => {
                return Ok(ApplyResult::Stale(StaleReceipt {
                    current_version: current,
                    branch,
                }));
            }
            Err(error) => return Err(error.into()),
        };
        let transition_effect = transition.effect();
        let resource_stalled = transition_effect.is_resource_stalled();
        let stall_receipt = resource_stalled.then(|| CommittedStallReceipt {
            state_version: transition.change_set().metadata.state_version,
            alarm_changed: transition.snapshot_before_commit().alarms.resource_stalled
                != transition.change_set().metadata.alarms.resource_stalled,
            attempted_branch: branch,
        });
        if transition_effect.is_header_work_rebased() {
            metrics::counter!("state.header.work.rebase.total", "outcome" => "rebased")
                .increment(1);
            metrics::counter!(
                "state.header.work.rebase.headers.total",
                "outcome" => "rebased"
            )
            .increment(u64::try_from(transition.change_set().put_nodes.len()).unwrap_or(u64::MAX));
        } else if transition_effect.is_header_work_already_applied() {
            metrics::counter!("state.header.work.rebase.total", "outcome" => "already_applied")
                .increment(1);
        }
        if resource_stalled {
            let receipt = stall_receipt.expect("resource-stalled transitions construct a receipt");
            if transition.is_no_change() {
                return Ok(ApplyResult::ResourceStalled(receipt));
            }
            let current = transition.snapshot_after_commit();
            let batch = self.store.batch_for(transition.change_set())?;
            #[cfg(test)]
            fault(FaultPoint::BeforeCommit)?;
            self.store.db.write(batch)?;
            transition_engine.install_committed_transition(transition)?;
            #[cfg(test)]
            fault(FaultPoint::AfterCommit)?;
            self.publisher.publish(current);
            #[cfg(test)]
            fault(FaultPoint::AfterPublish)?;
            return Ok(ApplyResult::ResourceStalled(receipt));
        }
        if !expectation.staged.is_empty() {
            let put_nodes: HashMap<_, _> = transition
                .change_set()
                .put_nodes
                .iter()
                .map(|node| (node.hash, node))
                .collect();
            let deleted: HashSet<_> = transition
                .change_set()
                .delete_nodes
                .iter()
                .copied()
                .collect();
            for expected in expectation.staged {
                let projected = if deleted.contains(&expected.hash) {
                    None
                } else if let Some(node) = put_nodes.get(&expected.hash) {
                    Some((*node).clone())
                } else {
                    self.store.header_node(expected.hash)?
                };
                let matches = projected.is_some_and(|node| {
                    node.height == expected.height
                        && node.hash == expected.hash
                        && node.parent_hash == expected.header.previous_block_hash
                });
                if !matches {
                    return Err(HeaderChainStoreError::StagedPathMismatch {
                        hash: expected.hash,
                    });
                }
            }
        }
        if let Some(expected) = expectation.verified {
            let actual = transition.change_set().metadata.frontiers.verified_best;
            if expected != actual {
                return Err(HeaderChainStoreError::VerifiedFrontierMismatch { expected, actual });
            }
        }
        if transition.is_no_change() {
            #[cfg(test)]
            fault(FaultPoint::BeforeCommit)?;
            self.store.db.write(full_state_batch)?;
            #[cfg(test)]
            fault(FaultPoint::AfterCommit)?;
            memory_swap();
            #[cfg(test)]
            fault(FaultPoint::AfterMemorySwap)?;
            return Ok(ApplyResult::NoChange(NoChangeReceipt {
                state_version: transition.snapshot_before_commit().state_version,
                idempotency_key: event,
            }));
        }

        let current = transition.snapshot_after_commit();
        let migrated_pin_refuted = transition.change_set().metadata.alarms.migrated_pin_refuted;
        let batch = self
            .store
            .batch_for_combined(transition.change_set(), full_state_batch)?;
        #[cfg(test)]
        fault(FaultPoint::BeforeCommit)?;
        self.store.db.write(batch)?;
        transition_engine.install_committed_transition(transition)?;
        #[cfg(test)]
        fault(FaultPoint::AfterCommit)?;
        if let Some(pin) = migrated_pin_refuted {
            return Err(HeaderChainStoreError::MigratedPinRefuted { pin });
        }
        memory_swap();
        #[cfg(test)]
        fault(FaultPoint::AfterMemorySwap)?;
        self.publisher.publish(current);
        #[cfg(test)]
        fault(FaultPoint::AfterPublish)?;
        Ok(ApplyResult::Committed)
    }
}

pub(in crate::service::finalized_state::header_chain) fn coherent_engine_aux_deliveries(
    engine: &HeaderChainEngine,
    node: &HeaderNode,
) -> Result<Vec<AuxDelivery>, HeaderChainStoreError> {
    let deliveries = engine.aux_deliveries(node.hash).to_vec();
    let indexed: BTreeSet<_> = node.aux_delivery_ids.iter().copied().collect();
    let stored: BTreeSet<_> = deliveries
        .iter()
        .map(|delivery| delivery.delivery_id)
        .collect();
    if indexed.len() != node.aux_delivery_ids.len()
        || stored.len() != deliveries.len()
        || indexed != stored
    {
        return Err(HeaderChainStoreError::Incoherent(
            "in-memory node and auxiliary delivery index disagree",
        ));
    }
    Ok(deliveries)
}
