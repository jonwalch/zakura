//! Settled and checkpoint trust-pin eligibility checks.

use sha2::Digest as _;

use crate::{BodyValidationState, EligibilityReason, EngineConfig, Frontier, HeaderNode};

use super::super::contracts::AuditViolation;

pub(super) fn check_trust_pins(
    nodes: &[HeaderNode],
    finalized: Frontier,
    config: &EngineConfig,
    violations: &mut Vec<AuditViolation>,
) {
    let settled = config.settled_manifest().pin_for_network(config.network());
    for node in nodes {
        for reason in &node.eligibility.direct_reasons {
            let valid = match reason {
                EligibilityReason::SettledUpgradeConflict { height, expected } => settled
                    .is_some_and(|pin| {
                        *height == node.height
                            && pin.activation == Frontier::new(*height, *expected)
                            && node.hash != *expected
                    }),
                EligibilityReason::CheckpointConflict { height, expected } => config
                    .local_checkpoints()
                    .hash(*height)
                    .is_some_and(|configured| {
                        configured == *expected && *height == node.height && node.hash != *expected
                    }),
                EligibilityReason::FinalityConflict {
                    finalized: reason_finalized,
                } => {
                    *reason_finalized == finalized
                        && node.height <= reason_finalized.height
                        && node.hash != reason_finalized.hash
                }
                EligibilityReason::ConsensusBodyInvalid { evidence, rule } => matches!(
                    &node.body_validation_state,
                    BodyValidationState::ConsensusInvalid {
                        evidence: body_evidence,
                        rule: body_rule,
                    } if body_evidence == evidence && body_rule == rule
                ),
                EligibilityReason::OperatorInvalid {
                    id, reason_digest, ..
                } => {
                    let mut hasher = sha2::Sha256::new();
                    hasher.update(b"zakura-operator-invalidation-v1");
                    hasher.update(node.hash.0);
                    hasher.update(id.bytes());
                    let digest: [u8; 32] = hasher.finalize().into();
                    *reason_digest == digest
                }
            };
            if !valid {
                violations.push(AuditViolation::EligibilityRoot(node.hash));
            }
        }
        // Settled pins and local checkpoints are independent trust sources. Check both
        // when they share a height so one source cannot mask the other's conflict reason.
        if let Some(expected) = settled
            .filter(|pin| pin.activation.height == node.height)
            .map(|pin| pin.activation.hash)
        {
            let reason = node.eligibility.direct_reasons.iter().any(|reason| {
                matches!(
                    reason,
                    EligibilityReason::SettledUpgradeConflict {
                        height,
                        expected: hash,
                    } if *height == node.height && *hash == expected
                )
            });
            // A matching hash must have no conflict reason; a mismatch must have one.
            if (node.hash == expected && reason) || (node.hash != expected && !reason) {
                violations.push(AuditViolation::TrustPin(node.height, node.hash));
            }
        }
        // Deliberately do not make this an `else`: the checkpoint can coincide with
        // the settled pin while requiring a distinct CheckpointConflict reason.
        if let Some(expected) = config.local_checkpoints().hash(node.height) {
            let reason = node.eligibility.direct_reasons.iter().any(|reason| {
                matches!(
                    reason,
                    EligibilityReason::CheckpointConflict {
                        height,
                        expected: hash,
                    } if *height == node.height && *hash == expected
                )
            });
            if (node.hash == expected && reason) || (node.hash != expected && !reason) {
                violations.push(AuditViolation::TrustPin(node.height, node.hash));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use zakura_chain::{
        block::{self, Block},
        parameters::Network,
        serialization::ZcashDeserialize,
    };

    use super::*;
    use crate::{CheckpointSet, EngineConfigError, EngineMode, TrustSource, TrustedAnchor};

    #[test]
    fn f225510_checked_policy_rejects_conflicting_trust_sources() {
        let genesis = Arc::<Block>::zcash_deserialize(
            zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES.as_slice(),
        )
        .expect("the mainnet genesis vector is canonical");
        let anchor = Frontier::new(block::Height(0), genesis.hash());
        let release = EngineConfig::new(
            EngineMode::Integrated,
            Network::Mainnet,
            TrustedAnchor {
                frontier: anchor,
                header: genesis.header.clone(),
            },
            CheckpointSet::default(),
        )
        .expect("the mainnet configuration has a settled pin");
        let settled = release
            .settled_manifest()
            .pin_for_network(release.network())
            .expect("mainnet has a settled pin")
            .activation;
        let checkpoint = Frontier::new(settled.height, block::Hash([0x5c; 32]));
        assert_eq!(
            EngineConfig::new(
                EngineMode::Integrated,
                Network::Mainnet,
                TrustedAnchor {
                    frontier: anchor,
                    header: genesis.header.clone(),
                },
                CheckpointSet::new([checkpoint]).expect("the checkpoint fixture is unique"),
            ),
            Err(EngineConfigError::ConflictingTrustEntry {
                height: settled.height,
                first: TrustSource::SettledUpgrade,
                second: TrustSource::LocalCheckpoint,
            }),
        );
    }
}
