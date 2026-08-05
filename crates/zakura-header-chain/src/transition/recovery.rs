//! Exhaustive startup audit and deterministic reconstruction planning.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use chrono::{DateTime, Duration, Utc};
use thiserror::Error;
use zakura_chain::block;

use crate::{
    AuxDelivery, BodyValidationState, CounterExhausted, EligibilityReason, EngineConfig,
    EngineMetadata, EngineMode, EngineSnapshot, FinalityRecord, FinalitySource, Frontier,
    HeaderNode, MemHeaderStore, StoreError,
};

/// One immutable predecessor record stored below the selectable finalized anchor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationContextRecord {
    /// Canonical context header, including its backward link.
    pub header: Arc<block::Header>,
    /// Authenticated context height.
    pub height: block::Height,
}

/// Complete exhaustive row/index view used only while publication is disabled.
pub trait StoreAuditRead {
    /// Return the atomic externally meaningful snapshot.
    fn snapshot(&self) -> Result<EngineSnapshot, StoreError>;
    /// Return complete singleton metadata from the same version.
    fn metadata(&self) -> Result<EngineMetadata, StoreError>;
    /// Every node row, including disconnected rows.
    fn all_nodes(&self) -> Result<Vec<HeaderNode>, StoreError>;
    /// Every persisted parent/child edge.
    fn child_edges(&self) -> Result<Vec<(block::Hash, block::Hash)>, StoreError>;
    /// Complete selected projection.
    fn selected_projection(&self) -> Result<Vec<Frontier>, StoreError>;
    /// Complete verified projection.
    fn verified_projection(&self) -> Result<Vec<Frontier>, StoreError>;
    /// Complete deferred-time index.
    fn deferred_entries(&self) -> Result<Vec<(DateTime<Utc>, block::Hash)>, StoreError>;
    /// Every authoritative direct-reason root.
    fn eligibility_roots(&self) -> Result<Vec<(block::Hash, EligibilityReason)>, StoreError>;
    /// Every auxiliary delivery, including dangling rows.
    fn all_aux_deliveries(&self) -> Result<Vec<AuxDelivery>, StoreError>;
    /// Every immutable below-finalized context row.
    fn validation_context_records(&self) -> Result<Vec<ValidationContextRecord>, StoreError>;
    /// Visit append-only finality provenance in ascending epoch order.
    fn visit_finality_history(
        &self,
        visitor: &mut dyn FnMut(FinalityRecord) -> Result<(), StoreError>,
    ) -> Result<(), StoreError>;
}

/// Stable exhaustive-audit violation categories.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AuditViolation {
    /// Canonical header and stored hash disagreed.
    NodeHash(block::Hash),
    /// A non-anchor node had no exact height-minus-one parent.
    Parent(block::Hash),
    /// Exact cumulative work did not equal parent plus block work.
    Work(block::Hash),
    /// Body invalidity and direct eligibility evidence disagreed.
    BodyEligibility(block::Hash),
    /// Header validation state contradicted deterministic header facts.
    HeaderValidation(block::Hash),
    /// A trust pin was absent or lacked its exact conflict reason.
    TrustPin(block::Height, block::Hash),
    /// Authoritative reason roots disagreed with node source rows.
    EligibilityRoot(block::Hash),
    /// Auxiliary provenance or a node foreign key was invalid.
    Auxiliary(block::Hash),
    /// Immutable validation context was malformed or discontinuous.
    ValidationContext(block::Hash),
    /// Finality history contradicted finalized metadata.
    Finality,
    /// Mode, network, manifest, schema, or snapshot contradicted configuration.
    Configuration,
    /// A protected source path was absent or discontinuous.
    ProtectedPath(block::Hash),
    /// Authoritative rows exceeded frozen limits without the permitted alarm.
    Limits,
}

/// Reconstructible categories replaced by one atomic recovery transaction.
#[derive(Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum RecoveryRepair {
    /// Parent/child adjacency differed from source nodes.
    ChildIndex,
    /// Future-time index differed from node states.
    DeferredIndex,
    /// Selected projection/frontier differed from recomputation.
    SelectedProjection,
    /// Verified projection differed from its authoritative frontier.
    VerifiedProjection,
    /// Cached inherited eligibility differed from ancestry.
    InheritedEligibility,
    /// Oldest-retained metadata differed from source nodes.
    RetentionMetadata,
    /// Selected-tip body-unavailability alarm differed from its durable node.
    BodyAvailabilityAlarm,
}

/// Exact source-derived state to install before startup publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryPlan {
    /// Snapshot observed before repair.
    pub before: EngineSnapshot,
    /// Corrected metadata with counters advanced exactly once when required.
    pub metadata: EngineMetadata,
    /// Nodes with reconstructed inherited eligibility caches.
    pub nodes: Vec<HeaderNode>,
    /// Complete expected adjacency index.
    pub child_edges: Vec<(block::Hash, block::Hash)>,
    /// Complete selected projection.
    pub selected_projection: Vec<Frontier>,
    /// Complete verified projection.
    pub verified_projection: Vec<Frontier>,
    /// Complete deferred index.
    pub deferred_entries: Vec<(DateTime<Utc>, block::Hash)>,
    /// Exact repairs, empty for a coherent store.
    pub repairs: BTreeSet<RecoveryRepair>,
}

impl RecoveryPlan {
    /// Return true when startup may publish without a repair transaction.
    pub fn is_clean(&self) -> bool {
        self.repairs.is_empty()
    }
}

/// Startup audit failed before publication became available.
#[derive(Clone, Debug, Error, PartialEq)]
pub enum RecoveryFailure {
    /// Exhaustive rows could not be read.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Authoritative source invariants failed.
    #[error("authoritative header-chain source rows failed startup audit")]
    Source {
        /// Deterministically ordered violations.
        violations: Vec<AuditViolation>,
    },
    /// A repair-required monotonic counter was exhausted.
    #[error(transparent)]
    Counter(#[from] CounterExhausted),
}

/// Audit authoritative rows and derive only reconstructible repairs.
pub fn audit_store<S: StoreAuditRead>(
    store: &S,
    config: &EngineConfig,
) -> Result<RecoveryPlan, RecoveryFailure> {
    let before = store.snapshot()?;
    let mut metadata = store.metadata()?;
    let mut violations = Vec::new();
    if before != metadata.snapshot()
        || metadata.disk_format.0 != 1
        || metadata.mode != config.mode
        || metadata.network_id != config.network.kind()
        || metadata.anchor_manifest_digest != config.trust_anchor_digest()
        || metadata.work_origin != config.bootstrap_anchor.frontier
    {
        violations.push(AuditViolation::Configuration);
    }

    let mut source_nodes = store.all_nodes()?;
    source_nodes.sort_unstable_by_key(|node| (node.height, node.hash.0));
    let mut unique = HashSet::new();
    for node in &source_nodes {
        if !unique.insert(node.hash) || node.header.hash() != node.hash {
            violations.push(AuditViolation::NodeHash(node.hash));
        }
    }
    let by_hash: HashMap<_, _> = source_nodes.iter().map(|node| (node.hash, node)).collect();
    let finalized = metadata.frontiers.finalized;
    if by_hash
        .get(&finalized.hash)
        .is_none_or(|node| node.height != finalized.height)
    {
        violations.push(AuditViolation::ProtectedPath(finalized.hash));
    }
    check_nodes(&source_nodes, &by_hash, &metadata, &mut violations);
    check_finalized_connectivity(&source_nodes, finalized, &mut violations);
    check_trust_pins(&source_nodes, config, &mut violations);
    check_authoritative_rows(store, &source_nodes, &metadata, config, &mut violations)?;
    if source_nodes.len().saturating_sub(1) > config.limits.max_non_finalized_nodes.get()
        && !metadata.alarms.resource_stalled
    {
        violations.push(AuditViolation::Limits);
    }
    violations.sort_by_key(violation_key);
    violations.dedup();
    if !violations.is_empty() {
        return Err(RecoveryFailure::Source { violations });
    }

    let mut graph = MemHeaderStore::from_nodes(finalized, source_nodes.clone()).map_err(|_| {
        RecoveryFailure::Source {
            violations: vec![AuditViolation::ProtectedPath(finalized.hash)],
        }
    })?;
    graph
        .recompute_all_eligibility()
        .map_err(|_| RecoveryFailure::Source {
            violations: vec![AuditViolation::ProtectedPath(finalized.hash)],
        })?;
    let mut nodes: Vec<_> = graph.nodes().cloned().collect();
    nodes.sort_unstable_by_key(|node| (node.height, node.hash.0));
    let node_map: HashMap<_, _> = nodes.iter().map(|node| (node.hash, node.clone())).collect();

    let mut child_edges: Vec<_> = nodes
        .iter()
        .filter(|node| node.hash != finalized.hash)
        .map(|node| (node.parent_hash, node.hash))
        .collect();
    child_edges.sort_unstable_by_key(|(parent, child)| (parent.0, child.0));
    let mut deferred_entries: Vec<_> = nodes
        .iter()
        .filter_map(|node| match node.validation {
            crate::HeaderValidationState::Valid => None,
            crate::HeaderValidationState::DeferredUntil(until) => Some((until, node.hash)),
        })
        .collect();
    deferred_entries.sort_unstable_by_key(|(until, hash)| (*until, hash.0));
    if graph.eligible_tips().len() > config.limits.max_candidate_tips.get() {
        return Err(source_failure(AuditViolation::Limits));
    }
    let (selected_tip, selected_score) = graph
        .select_header_best()
        .map_err(|_| source_failure(AuditViolation::ProtectedPath(finalized.hash)))?;
    let selected_projection = path_to(&node_map, finalized, selected_tip)?;
    let verified_projection = verified_path(&node_map, &metadata)?;

    let mut repairs = BTreeSet::new();
    compare_by_key(
        store.child_edges()?,
        &child_edges,
        |(parent, child)| (parent.0, child.0),
        RecoveryRepair::ChildIndex,
        &mut repairs,
    );
    compare_by_key(
        store.deferred_entries()?,
        &deferred_entries,
        |(until, hash)| (until.timestamp(), until.timestamp_subsec_nanos(), hash.0),
        RecoveryRepair::DeferredIndex,
        &mut repairs,
    );
    if store.selected_projection()? != selected_projection
        || metadata.frontiers.header_best != selected_tip
        || metadata.header_best_score != selected_score
    {
        repairs.insert(RecoveryRepair::SelectedProjection);
    }
    if store.verified_projection()? != verified_projection {
        repairs.insert(RecoveryRepair::VerifiedProjection);
    }
    if source_nodes != nodes {
        repairs.insert(RecoveryRepair::InheritedEligibility);
    }
    let oldest_retained_height = nodes
        .iter()
        .map(|node| node.height)
        .min()
        .unwrap_or(finalized.height);
    if metadata.oldest_retained_height != oldest_retained_height {
        repairs.insert(RecoveryRepair::RetentionMetadata);
    }
    let body_unavailable_alarm = match &node_map
        .get(&selected_tip.hash)
        .ok_or_else(|| source_failure(AuditViolation::ProtectedPath(selected_tip.hash)))?
        .body
    {
        crate::BodyValidationState::Unavailable(summary) if summary.alarmed => Some(*summary),
        _ => None,
    };
    if metadata.alarms.header_best_body_unavailable != body_unavailable_alarm {
        repairs.insert(RecoveryRepair::BodyAvailabilityAlarm);
    }

    if !repairs.is_empty() {
        metadata.state_version = metadata.state_version.checked_next()?;
        if repairs.contains(&RecoveryRepair::SelectedProjection)
            || repairs.contains(&RecoveryRepair::InheritedEligibility)
        {
            metadata.header_generation = metadata.header_generation.checked_next()?;
        }
        if repairs.contains(&RecoveryRepair::VerifiedProjection) {
            metadata.verified_generation = metadata.verified_generation.checked_next()?;
        }
        metadata.frontiers.header_best = selected_tip;
        metadata.header_best_score = selected_score;
        metadata.oldest_retained_height = oldest_retained_height;
        metadata.alarms.header_best_body_unavailable = body_unavailable_alarm;
    }

    Ok(RecoveryPlan {
        before,
        metadata,
        nodes,
        child_edges,
        selected_projection,
        verified_projection,
        deferred_entries,
        repairs,
    })
}

fn check_nodes(
    nodes: &[HeaderNode],
    by_hash: &HashMap<block::Hash, &HeaderNode>,
    metadata: &EngineMetadata,
    violations: &mut Vec<AuditViolation>,
) {
    for node in nodes {
        if node.work_coordinate().origin_hash() != metadata.work_origin.hash {
            violations.push(AuditViolation::Work(node.hash));
        }
        if node.hash == metadata.frontiers.finalized.hash {
            if node.eligibility.inherited_from.is_some() {
                violations.push(AuditViolation::Parent(node.hash));
            }
        } else if let Some(parent) = by_hash.get(&node.parent_hash) {
            if parent.height.next().ok() != Some(node.height)
                || node.header.previous_block_hash != parent.hash
            {
                violations.push(AuditViolation::Parent(node.hash));
            }
            if parent.work_coordinate().checked_add(node.block_work).ok()
                != Some(node.work_coordinate())
            {
                violations.push(AuditViolation::Work(node.hash));
            }
        } else {
            violations.push(AuditViolation::Parent(node.hash));
        }
        let body_reason = node
            .eligibility
            .direct_reasons
            .iter()
            .find(|reason| matches!(reason, EligibilityReason::ConsensusBodyInvalid { .. }));
        let matches = match (&node.body, body_reason) {
            (
                BodyValidationState::ConsensusInvalid {
                    evidence: left_evidence,
                    rule: left_rule,
                },
                Some(EligibilityReason::ConsensusBodyInvalid {
                    evidence: right_evidence,
                    rule: right_rule,
                }),
            ) => left_evidence == right_evidence && left_rule == right_rule,
            (BodyValidationState::ConsensusInvalid { .. }, _) => false,
            (_, None) => true,
            (_, Some(_)) => false,
        };
        if !matches {
            violations.push(AuditViolation::BodyEligibility(node.hash));
        }
        if let crate::HeaderValidationState::DeferredUntil(until) = node.validation {
            let expected = node.header.time.checked_sub_signed(Duration::hours(2));
            if expected != Some(until) {
                violations.push(AuditViolation::HeaderValidation(node.hash));
            }
        }
    }
}

fn check_finalized_connectivity(
    nodes: &[HeaderNode],
    finalized: Frontier,
    violations: &mut Vec<AuditViolation>,
) {
    let mut connected = HashSet::from([finalized.hash]);
    for node in nodes {
        if node.hash == finalized.hash {
            continue;
        }
        if connected.contains(&node.parent_hash) {
            connected.insert(node.hash);
        } else {
            violations.push(AuditViolation::ProtectedPath(node.hash));
        }
    }
}

fn check_trust_pins(
    nodes: &[HeaderNode],
    config: &EngineConfig,
    violations: &mut Vec<AuditViolation>,
) {
    let settled = config.settled_manifest.pin_for_network(&config.network);
    for node in nodes {
        let expected = if settled.is_some_and(|pin| pin.activation.height == node.height) {
            settled.map(|pin| (pin.activation.hash, true))
        } else {
            config
                .local_checkpoints
                .hash(node.height)
                .map(|hash| (hash, false))
        };
        let Some((expected, settled_reason)) = expected else {
            continue;
        };
        let reason = node
            .eligibility
            .direct_reasons
            .iter()
            .any(|reason| match reason {
                EligibilityReason::SettledUpgradeConflict {
                    height,
                    expected: hash,
                } if settled_reason => *height == node.height && *hash == expected,
                EligibilityReason::CheckpointConflict {
                    height,
                    expected: hash,
                } if !settled_reason => *height == node.height && *hash == expected,
                _ => false,
            });
        if (node.hash == expected && reason) || (node.hash != expected && !reason) {
            violations.push(AuditViolation::TrustPin(node.height, node.hash));
        }
    }
}

fn check_authoritative_rows<S: StoreAuditRead>(
    store: &S,
    nodes: &[HeaderNode],
    metadata: &EngineMetadata,
    config: &EngineConfig,
    violations: &mut Vec<AuditViolation>,
) -> Result<(), StoreError> {
    let mut expected: Vec<_> = nodes
        .iter()
        .flat_map(|node| {
            node.eligibility
                .direct_reasons
                .iter()
                .cloned()
                .map(move |reason| (node.hash, reason))
        })
        .collect();
    let mut actual = store.eligibility_roots()?;
    expected.sort_by_key(|(hash, reason)| (hash.0, reason.clone()));
    actual.sort_by_key(|(hash, reason)| (hash.0, reason.clone()));
    if expected != actual {
        let hash = expected
            .iter()
            .zip(&actual)
            .find(|(left, right)| left != right)
            .map(|(left, _)| left.0)
            .or_else(|| {
                expected
                    .get(actual.len())
                    .or_else(|| actual.get(expected.len()))
                    .map(|(hash, _)| *hash)
            })
            .unwrap_or(block::Hash([0; 32]));
        violations.push(AuditViolation::EligibilityRoot(hash));
    }

    let by_hash: HashMap<_, _> = nodes.iter().map(|node| (node.hash, node)).collect();
    let deliveries = store.all_aux_deliveries()?;
    let delivery_ids: HashSet<_> = deliveries.iter().map(|row| row.delivery_id).collect();
    if delivery_ids.len() != deliveries.len() {
        violations.push(AuditViolation::Auxiliary(block::Hash([0; 32])));
    }
    for delivery in &deliveries {
        if by_hash
            .get(&delivery.header_hash)
            .is_none_or(|node| !node.aux_delivery_ids.contains(&delivery.delivery_id))
        {
            violations.push(AuditViolation::Auxiliary(delivery.header_hash));
        }
    }
    for node in nodes {
        let node_ids: HashSet<_> = node.aux_delivery_ids.iter().copied().collect();
        if node_ids.len() != node.aux_delivery_ids.len()
            || node_ids.iter().any(|id| !delivery_ids.contains(id))
        {
            violations.push(AuditViolation::Auxiliary(node.hash));
        }
    }

    let mut contexts = store.validation_context_records()?;
    contexts.sort_unstable_by_key(|record| record.height);
    let predecessor_span = u32::try_from(crate::POW_PREDECESSOR_CONTEXT_SPAN)
        .map_err(|_| StoreError::Incoherent("validation context bound does not fit in u32"))?;
    let required_contexts = usize::try_from(
        metadata.frontiers.finalized.height.0.min(predecessor_span),
    )
    .map_err(|_| StoreError::Incoherent("validation context bound does not fit in usize"))?;
    if contexts.len() != required_contexts {
        violations.push(AuditViolation::ValidationContext(
            metadata.frontiers.finalized.hash,
        ));
    }
    for pair in contexts.windows(2) {
        if pair[0].height.next().ok() != Some(pair[1].height)
            || pair[1].header.previous_block_hash != pair[0].header.hash()
        {
            violations.push(AuditViolation::ValidationContext(pair[1].header.hash()));
        }
    }
    if let (Some(last), Some(finalized_node)) = (
        contexts.last(),
        by_hash.get(&metadata.frontiers.finalized.hash),
    ) {
        if last.height.next().ok() != Some(finalized_node.height)
            || finalized_node.header.previous_block_hash != last.header.hash()
        {
            violations.push(AuditViolation::ValidationContext(last.header.hash()));
        }
    }

    let mut previous = None;
    let mut last = None;
    let mut invalid_history = false;
    store.visit_finality_history(&mut |record| {
        if previous.is_some_and(|previous: FinalityRecord| {
            previous.current != record.previous
                || previous.epoch.get().checked_add(1) != Some(record.epoch.get())
        }) || !source_matches_mode(&record, metadata.mode, config)
        {
            invalid_history = true;
        }
        previous = Some(record);
        last = Some(record);
        Ok(())
    })?;
    if invalid_history
        || last.is_some_and(|record| {
            record.current != metadata.frontiers.finalized
                || record.epoch != metadata.finality_epoch
        })
        || last.is_none() && metadata.finality_epoch.get() != 0
    {
        violations.push(AuditViolation::Finality);
    }
    Ok(())
}

fn source_matches_mode(record: &FinalityRecord, mode: EngineMode, config: &EngineConfig) -> bool {
    match (mode, record.source) {
        (EngineMode::Integrated, FinalitySource::FullState { .. })
        | (_, FinalitySource::MigratedHeadersOnly) => true,
        (EngineMode::HeadersOnly, FinalitySource::HeadersOnlyDepth { selected_tip }) => {
            record.current.height > record.previous.height
                && selected_tip
                    .height
                    .0
                    .saturating_sub(record.current.height.0)
                    == config.limits.local_finality_depth.get()
        }
        _ => false,
    }
}

fn verified_path(
    nodes: &HashMap<block::Hash, HeaderNode>,
    metadata: &EngineMetadata,
) -> Result<Vec<Frontier>, RecoveryFailure> {
    if metadata.mode == EngineMode::HeadersOnly {
        if metadata.frontiers.verified_best != metadata.frontiers.finalized {
            return Err(source_failure(AuditViolation::ProtectedPath(
                metadata.frontiers.verified_best.hash,
            )));
        }
        return Ok(vec![metadata.frontiers.finalized]);
    }
    let path = path_to(
        nodes,
        metadata.frontiers.finalized,
        metadata.frontiers.verified_best,
    )?;
    if path.iter().skip(1).any(|frontier| {
        nodes
            .get(&frontier.hash)
            .is_none_or(|node| !matches!(node.body, BodyValidationState::Verified { .. }))
    }) {
        return Err(source_failure(AuditViolation::ProtectedPath(
            metadata.frontiers.verified_best.hash,
        )));
    }
    Ok(path)
}

fn path_to(
    nodes: &HashMap<block::Hash, HeaderNode>,
    finalized: Frontier,
    tip: Frontier,
) -> Result<Vec<Frontier>, RecoveryFailure> {
    let mut current = tip;
    let mut path = Vec::new();
    loop {
        let node = nodes
            .get(&current.hash)
            .filter(|node| node.height == current.height)
            .ok_or_else(|| source_failure(AuditViolation::ProtectedPath(current.hash)))?;
        path.push(current);
        if current == finalized {
            break;
        }
        current = Frontier::new(
            current
                .height
                .previous()
                .map_err(|_| source_failure(AuditViolation::ProtectedPath(current.hash)))?,
            node.parent_hash,
        );
    }
    path.reverse();
    Ok(path)
}

fn compare_by_key<T, K: Ord, F: FnMut(&T) -> K>(
    mut actual: Vec<T>,
    expected: &[T],
    mut key: F,
    repair: RecoveryRepair,
    repairs: &mut BTreeSet<RecoveryRepair>,
) where
    T: Clone + Eq,
{
    let mut expected = expected.to_vec();
    actual.sort_by_key(&mut key);
    expected.sort_by_key(key);
    if actual != expected {
        repairs.insert(repair);
    }
}

fn violation_key(violation: &AuditViolation) -> (u8, u32, [u8; 32]) {
    match violation {
        AuditViolation::NodeHash(hash) => (0, 0, hash.0),
        AuditViolation::Parent(hash) => (1, 0, hash.0),
        AuditViolation::Work(hash) => (2, 0, hash.0),
        AuditViolation::BodyEligibility(hash) => (3, 0, hash.0),
        AuditViolation::HeaderValidation(hash) => (4, 0, hash.0),
        AuditViolation::TrustPin(height, hash) => (5, height.0, hash.0),
        AuditViolation::EligibilityRoot(hash) => (6, 0, hash.0),
        AuditViolation::Auxiliary(hash) => (7, 0, hash.0),
        AuditViolation::ValidationContext(hash) => (8, 0, hash.0),
        AuditViolation::Finality => (9, 0, [0; 32]),
        AuditViolation::Configuration => (10, 0, [0; 32]),
        AuditViolation::ProtectedPath(hash) => (11, 0, hash.0),
        AuditViolation::Limits => (12, 0, [0; 32]),
    }
}

fn source_failure(violation: AuditViolation) -> RecoveryFailure {
    RecoveryFailure::Source {
        violations: vec![violation],
    }
}
