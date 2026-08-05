//! Sealed validation of complete observable-header batches.

use std::sync::Arc;

use chrono::Duration;
use sha2::{Digest, Sha256};
use thiserror::Error;
use zakura_chain::{block, parameters::Network};

use super::{
    infer_height, validate_commitment_structure, validate_compact_target,
    validate_contextual_difficulty_and_time, validate_encoding_version_hash, validate_hash_filter,
    AdjustedDifficulty, PowPolicy, PowPolicyError, POW_ADJUSTMENT_BLOCK_SPAN,
};
use crate::{
    Clock, EngineConfig, EvidenceId, HeaderContextFact, HeaderValidationState, PreparedHeader,
    PreparedHeaderBatch, RuleId, ValidationLease,
};

/// Ordered, nonempty canonical headers to validate against one exact parent lease.
#[derive(Copy, Clone, Debug)]
pub struct HeaderBatchInput<'a> {
    /// Headers in exact parent-first wire order.
    pub headers: &'a [Arc<block::Header>],
}

impl<'a> HeaderBatchInput<'a> {
    /// Construct an input over one complete target response assembled by the requester.
    pub const fn new(headers: &'a [Arc<block::Header>]) -> Self {
        Self { headers }
    }
}

/// Immutable authenticated rules used by the pure preparation pipeline.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderRules {
    network: Network,
    pow_policy: PowPolicy,
    trust_anchor_digest: [u8; 32],
}

impl HeaderRules {
    /// Derive rules only from the validated engine configuration.
    pub fn from_engine_config(config: &EngineConfig) -> Result<Self, PowPolicyError> {
        Ok(Self {
            network: config.network.clone(),
            pow_policy: PowPolicy::for_network(&config.network)?,
            trust_anchor_digest: config.trust_anchor_digest(),
        })
    }

    /// Bind authenticated network parameters to a state-issued validation lease. The state
    /// transition independently rechecks the lease's anchor digest before any mutation.
    pub fn for_validation_lease(
        network: Network,
        lease: &ValidationLease,
    ) -> Result<Self, PowPolicyError> {
        Ok(Self {
            pow_policy: PowPolicy::for_network(&network)?,
            network,
            trust_anchor_digest: lease.trust_anchor_digest,
        })
    }

    /// Return the authenticated network parameters bound into these rules.
    pub fn network(&self) -> &Network {
        &self.network
    }

    /// Return the authenticated trust-anchor identity sealed into preparation receipts.
    pub const fn trust_anchor_digest(&self) -> [u8; 32] {
        self.trust_anchor_digest
    }
}

/// Stable preparation stage used for peer attribution and conformance diagnostics.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum HeaderRule {
    /// Canonical signed version and full-header hash.
    EncodingVersionHash,
    /// Exact parent and internal linkage.
    ParentLink,
    /// Checked local height inference.
    InferredHeight,
    /// Height-dependent commitment interpretation.
    CommitmentStructure,
    /// Compact target domain and network limit.
    CompactTarget,
    /// Header hash at or below its target.
    HashToTarget,
    /// Network-bound Equihash shape and proof.
    Equihash,
    /// Branch-local target adjustment and median-time rules.
    ContextualDifficultyAndTime,
    /// Local-clock future-header classification.
    LocalFutureTime,
    /// Exact durable validation lease and trust-anchor identity.
    ValidationLease,
    /// Exact per-block work calculation.
    Work,
}

impl HeaderRule {
    /// Return every normative rule implemented by this validation stage.
    pub const fn rule_ids(self) -> &'static [RuleId] {
        const ENCODING_VERSION_HASH: &[RuleId] = &[RuleId::new("LC-VAL-02")];
        const PARENT_LINK: &[RuleId] = &[RuleId::new("LC-VAL-03")];
        const INFERRED_HEIGHT: &[RuleId] = &[RuleId::new("LC-HEIGHT-01")];
        const COMMITMENT_STRUCTURE: &[RuleId] =
            &[RuleId::new("LC-COMMIT-01"), RuleId::new("LC-COMMIT-02")];
        const TARGET: &[RuleId] = &[RuleId::new("LC-VAL-05")];
        const EQUIHASH: &[RuleId] = &[RuleId::new("LC-VAL-04")];
        const CONTEXTUAL_DIFFICULTY_AND_TIME: &[RuleId] = &[
            RuleId::new("LC-VAL-06"),
            RuleId::new("LC-VAL-07"),
            RuleId::new("LC-TIME-01"),
        ];
        const LOCAL_FUTURE_TIME: &[RuleId] = &[RuleId::new("LC-VAL-08")];
        const VALIDATION_LEASE: &[RuleId] =
            &[RuleId::new("LC-ANCHOR-03"), RuleId::new("LC-VAL-11")];
        const WORK: &[RuleId] = &[RuleId::new("LC-VAL-10")];

        match self {
            Self::EncodingVersionHash => ENCODING_VERSION_HASH,
            Self::ParentLink => PARENT_LINK,
            Self::InferredHeight => INFERRED_HEIGHT,
            Self::CommitmentStructure => COMMITMENT_STRUCTURE,
            Self::CompactTarget | Self::HashToTarget => TARGET,
            Self::Equihash => EQUIHASH,
            Self::ContextualDifficultyAndTime => CONTEXTUAL_DIFFICULTY_AND_TIME,
            Self::LocalFutureTime => LOCAL_FUTURE_TIME,
            Self::ValidationLease => VALIDATION_LEASE,
            Self::Work => WORK,
        }
    }
}

/// Failure to prepare a batch. Only local future time is represented in a successful batch.
#[derive(Debug, Error)]
pub enum HeaderFailure {
    /// The caller supplied no headers for an insertion event.
    #[error("header batch is empty")]
    Empty,
    /// Durable state supplied an incoherent or stale validation lease.
    #[error("validation lease is incoherent with the authenticated header rules")]
    InvalidLease,
    /// One deterministic observable-header rule failed.
    #[error("header at offset {offset} failed {rule:?}: {reason}")]
    Invalid {
        /// Zero-based header offset.
        offset: usize,
        /// Exact failed stage.
        rule: HeaderRule,
        /// Stable human-readable source description.
        reason: String,
    },
    /// A local time calculation exceeded the representable timestamp range.
    #[error("local future-time boundary is outside the representable timestamp range")]
    ClockRange,
}

fn invalid(offset: usize, rule: HeaderRule, error: impl std::fmt::Display) -> HeaderFailure {
    HeaderFailure::Invalid {
        offset,
        rule,
        reason: error.to_string(),
    }
}

/// Validate a complete batch without reading retained graph state.
pub fn prepare_context_free_headers(
    input: HeaderBatchInput<'_>,
    parent: crate::Frontier,
    rules: &HeaderRules,
    clock: &dyn Clock,
) -> Result<PreparedHeaderBatch, HeaderFailure> {
    prepare_headers_inner(input, parent, None, rules, clock)
}

/// Validate a complete batch without mutation and seal its results to `lease`.
///
/// This compatibility entry point retains contextual validation until production callers have
/// moved to [`prepare_context_free_headers`] and the engine performs the graph-dependent rules.
pub fn prepare_headers(
    input: HeaderBatchInput<'_>,
    lease: &ValidationLease,
    rules: &HeaderRules,
    clock: &dyn Clock,
) -> Result<PreparedHeaderBatch, HeaderFailure> {
    let required_predecessors = usize::try_from(lease.parent.height.0)
        .map_err(|_| HeaderFailure::InvalidLease)?
        .checked_add(1)
        .ok_or(HeaderFailure::InvalidLease)?
        .min(POW_ADJUSTMENT_BLOCK_SPAN);
    if lease.trust_anchor_digest != rules.trust_anchor_digest
        || lease.predecessors.first().map(|fact| fact.frontier) != Some(lease.parent)
        || lease.predecessors.len() != required_predecessors
    {
        return Err(HeaderFailure::InvalidLease);
    }
    prepare_headers_inner(input, lease.parent, Some(lease), rules, clock)
}

fn prepare_headers_inner(
    input: HeaderBatchInput<'_>,
    parent_frontier: crate::Frontier,
    contextual_lease: Option<&ValidationLease>,
    rules: &HeaderRules,
    clock: &dyn Clock,
) -> Result<PreparedHeaderBatch, HeaderFailure> {
    if input.headers.is_empty() {
        return Err(HeaderFailure::Empty);
    }

    let hashes: Vec<_> = input
        .headers
        .iter()
        .enumerate()
        .map(|(offset, header)| {
            validate_encoding_version_hash(header)
                .map_err(|error| invalid(offset, HeaderRule::EncodingVersionHash, error))
        })
        .collect::<Result<_, _>>()?;
    if contextual_lease.is_some() {
        let mut expected_parent = parent_frontier.hash;
        for (offset, (header, hash)) in input.headers.iter().zip(&hashes).enumerate() {
            if header.previous_block_hash != expected_parent {
                let error = super::HeaderLinkError {
                    offset,
                    expected: expected_parent,
                    actual: header.previous_block_hash,
                };
                return Err(invalid(offset, HeaderRule::ParentLink, error));
            }
            expected_parent = *hash;
        }
    }

    let now = clock.now();
    let future_limit = now
        .checked_add_signed(Duration::hours(2))
        .ok_or(HeaderFailure::ClockRange)?;
    let mut parent = parent_frontier;
    let mut context = contextual_lease.map(|lease| lease.predecessors.clone());
    let mut prepared = Vec::with_capacity(input.headers.len());

    for (offset, header) in input.headers.iter().enumerate() {
        let hash = hashes[offset];
        let height = infer_height(parent.height, None)
            .map_err(|error| invalid(offset, HeaderRule::InferredHeight, error))?;
        validate_commitment_structure(header, &rules.network, height)
            .map_err(|error| invalid(offset, HeaderRule::CommitmentStructure, error))?;
        let target = validate_compact_target(header, &rules.network)
            .map_err(|error| invalid(offset, HeaderRule::CompactTarget, error))?;
        if !rules.pow_policy.is_authenticated_custom_waiver() {
            validate_hash_filter(hash, target)
                .map_err(|error| invalid(offset, HeaderRule::HashToTarget, error))?;
        }
        rules
            .pow_policy
            .validate_solution(header)
            .map_err(|error| invalid(offset, HeaderRule::Equihash, error))?;

        if let Some(context) = context.as_ref() {
            let adjustment = AdjustedDifficulty::new_from_header_time(
                header.time,
                parent.height,
                &rules.network,
                context
                    .iter()
                    .map(|fact| (fact.difficulty_threshold, fact.time)),
            );
            validate_contextual_difficulty_and_time(header.difficulty_threshold, adjustment)
                .map_err(|error| invalid(offset, HeaderRule::ContextualDifficultyAndTime, error))?;
        }

        let validation = if header.time > future_limit {
            HeaderValidationState::DeferredUntil(
                header
                    .time
                    .checked_sub_signed(Duration::hours(2))
                    .ok_or(HeaderFailure::ClockRange)?,
            )
        } else {
            HeaderValidationState::Valid
        };
        let block_work = header
            .difficulty_threshold
            .to_work()
            .ok_or_else(|| invalid(offset, HeaderRule::Work, "invalid compact target"))?;

        prepared.push(PreparedHeader {
            header: header.clone(),
            hash,
            height,
            block_work,
            validation,
        });
        parent = crate::Frontier::new(height, hash);
        if let Some(context) = context.as_mut() {
            context.insert(
                0,
                HeaderContextFact {
                    frontier: parent,
                    difficulty_threshold: header.difficulty_threshold,
                    time: header.time,
                },
            );
            context.truncate(POW_ADJUSTMENT_BLOCK_SPAN);
        }
    }

    let mut hasher = Sha256::new();
    hasher.update(b"zakura-header-chain-context-free-batch-v1");
    hasher.update(parent_frontier.height.0.to_le_bytes());
    hasher.update(parent_frontier.hash.0);
    hasher.update(rules.trust_anchor_digest);
    for header in &prepared {
        hasher.update(header.height.0.to_le_bytes());
        hasher.update(header.hash.0);
    }
    PreparedHeaderBatch::new(
        prepared,
        parent_frontier,
        rules.trust_anchor_digest,
        EvidenceId::from_digest(hasher.finalize().into()),
    )
    .map_err(|error| invalid(0, HeaderRule::ValidationLease, error))
}
