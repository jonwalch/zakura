//! Versioned engine resource limits.

use std::{
    collections::BTreeMap,
    num::{NonZeroU32, NonZeroUsize},
    str::FromStr,
    sync::Arc,
};

use sha2::{Digest, Sha256};
use thiserror::Error;
use zakura_chain::{
    block,
    parameters::{
        constants::MAX_BLOCK_REORG_HEIGHT, Network, NetworkKind, NetworkUpgrade,
        MAX_NON_FINALIZED_CHAIN_FORKS,
    },
};

use crate::Frontier;

/// Exact v1 maximum number of retained non-finalized header nodes.
pub const MAX_NON_FINALIZED_NODES_V1: usize = 65_536;
/// Exact v1 maximum number of staged unknown targets across all peers.
pub const MAX_STAGED_TARGETS_V1: usize = 16;
/// Exact v1 maximum number of candidate tips.
pub const MAX_CANDIDATE_TIPS_V1: usize = MAX_NON_FINALIZED_CHAIN_FORKS;

/// Header-engine integration and finality mode.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum EngineMode {
    /// Full state is the only authority allowed to advance finality.
    Integrated,
    /// A selected header 1,000 blocks deep becomes a disclosed local trust pin.
    HeadersOnly,
}

/// Exact trusted bootstrap header and its hash-qualified height.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrustedAnchor {
    /// Exact configured frontier.
    pub frontier: Frontier,
    /// Canonical anchor header, still subject to observable validation.
    pub header: Arc<block::Header>,
}

/// Authenticated local checkpoint map; height-only or hash-only entries are impossible.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CheckpointSet(BTreeMap<block::Height, block::Hash>);

impl CheckpointSet {
    /// Construct a checkpoint set, rejecting conflicting duplicates.
    pub fn new(checkpoints: impl IntoIterator<Item = Frontier>) -> Result<Self, EngineConfigError> {
        let mut result = BTreeMap::new();
        for checkpoint in checkpoints {
            if result
                .insert(checkpoint.height, checkpoint.hash)
                .is_some_and(|old| old != checkpoint.hash)
            {
                return Err(EngineConfigError::ConflictingCheckpoint(checkpoint.height));
            }
        }
        Ok(Self(result))
    }

    /// Return the configured hash at `height`.
    pub fn hash(&self, height: block::Height) -> Option<block::Hash> {
        self.0.get(&height).copied()
    }

    /// Iterate checkpoints in ascending height order.
    pub fn iter(&self) -> impl Iterator<Item = Frontier> + '_ {
        self.0
            .iter()
            .map(|(height, hash)| Frontier::new(*height, *hash))
    }
}

/// One release-authenticated settled network-upgrade pin.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct SettledUpgradePin {
    /// Production network identity.
    pub network: NetworkKind,
    /// Settled upgrade identity.
    pub upgrade: NetworkUpgrade,
    /// Exact activation frontier.
    pub activation: Frontier,
}

/// Immutable settled pins compiled into this release.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SettledUpgradeManifest {
    pins: Vec<SettledUpgradePin>,
    digest: [u8; 32],
}

impl SettledUpgradeManifest {
    /// Construct and validate the exact specification-v1.3 release manifest.
    pub fn for_release() -> Result<Self, EngineConfigError> {
        let pins = vec![
            SettledUpgradePin {
                network: NetworkKind::Mainnet,
                upgrade: NetworkUpgrade::Nu6_2,
                activation: Frontier::new(
                    block::Height(3_364_600),
                    block::Hash::from_str(
                        "0000000000806344c408a4cfdf472f4132c632edbdc24cf2f3f672061da8b865",
                    )
                    .map_err(|_| EngineConfigError::MalformedSettledPin(NetworkKind::Mainnet))?,
                ),
            },
            SettledUpgradePin {
                network: NetworkKind::Testnet,
                upgrade: NetworkUpgrade::Nu6_2,
                activation: Frontier::new(
                    block::Height(4_052_000),
                    block::Hash::from_str(
                        "0010cb912b0188da5bc055ee67e3f77d30cd27611369d865974a5bf0b1ec2912",
                    )
                    .map_err(|_| EngineConfigError::MalformedSettledPin(NetworkKind::Testnet))?,
                ),
            },
        ];
        Self::new(pins)
    }

    fn new(mut pins: Vec<SettledUpgradePin>) -> Result<Self, EngineConfigError> {
        pins.sort_unstable_by_key(|pin| match pin.network {
            NetworkKind::Mainnet => 0_u8,
            NetworkKind::Testnet => 1_u8,
            NetworkKind::Regtest => 2_u8,
        });
        if pins.iter().any(|pin| pin.network == NetworkKind::Regtest) {
            return Err(EngineConfigError::InvalidSettledNetwork);
        }
        if pins
            .windows(2)
            .any(|pair| pair[0].network == pair[1].network)
        {
            return Err(EngineConfigError::DuplicateSettledPin);
        }
        let mut hasher = Sha256::new();
        hasher.update(b"zakura-settled-upgrade-manifest-v1");
        for pin in &pins {
            hasher.update(match pin.network {
                NetworkKind::Mainnet => b"mainnet".as_slice(),
                NetworkKind::Testnet => b"testnet".as_slice(),
                NetworkKind::Regtest => b"regtest".as_slice(),
            });
            hasher.update(b"nu6.2");
            hasher.update(pin.activation.height.0.to_le_bytes());
            hasher.update(pin.activation.hash.0);
        }
        Ok(Self {
            pins,
            digest: hasher.finalize().into(),
        })
    }

    /// Return the immutable manifest digest stored with engine metadata.
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }

    /// Return the mandatory pin for a production network; custom networks have none.
    pub fn pin_for_network(&self, network: &Network) -> Option<SettledUpgradePin> {
        let production_kind = match network {
            Network::Mainnet => Some(NetworkKind::Mainnet),
            Network::Testnet(_) if network.is_default_testnet() => Some(NetworkKind::Testnet),
            Network::Testnet(_) => None,
        }?;
        self.pins
            .iter()
            .find(|pin| pin.network == production_kind)
            .copied()
    }

    /// Iterate every release-authenticated production pin.
    pub fn iter(&self) -> impl Iterator<Item = SettledUpgradePin> + '_ {
        self.pins.iter().copied()
    }
}

/// Immutable pure-engine configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EngineConfig {
    /// Finality authority mode.
    pub mode: EngineMode,
    /// Authenticated network parameters.
    pub network: Network,
    /// Exact trusted bootstrap anchor.
    pub bootstrap_anchor: TrustedAnchor,
    /// Optional authenticated local checkpoints.
    pub local_checkpoints: CheckpointSet,
    /// Mandatory release-authenticated settled pins.
    pub settled_manifest: SettledUpgradeManifest,
    /// Frozen engine resource limits.
    pub limits: EngineLimits,
}

impl EngineConfig {
    /// Construct a configuration with the mandatory compiled settled manifest.
    pub fn new(
        mode: EngineMode,
        network: Network,
        bootstrap_anchor: TrustedAnchor,
        local_checkpoints: CheckpointSet,
    ) -> Result<Self, EngineConfigError> {
        let actual_anchor = crate::validation::validate_trusted_anchor_observables(
            &bootstrap_anchor.header,
            &network,
            bootstrap_anchor.frontier.height,
        )
        .map_err(EngineConfigError::InvalidTrustedAnchor)?;
        if actual_anchor != bootstrap_anchor.frontier.hash {
            return Err(EngineConfigError::AnchorHashMismatch {
                expected: bootstrap_anchor.frontier.hash,
                actual: actual_anchor,
            });
        }
        let settled_manifest = SettledUpgradeManifest::for_release()?;
        if matches!(network, Network::Mainnet) || network.is_default_testnet() {
            settled_manifest
                .pin_for_network(&network)
                .ok_or(EngineConfigError::MissingSettledPin(network.kind()))?;
        }
        Ok(Self {
            mode,
            network,
            bootstrap_anchor,
            local_checkpoints,
            settled_manifest,
            limits: EngineLimits::v1(),
        })
    }

    /// Digest binding every absolute trust anchor used by validation and startup.
    pub fn trust_anchor_digest(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(b"zakura-header-chain-trust-anchors-v1");
        hasher.update(self.settled_manifest.digest());
        hasher.update(self.bootstrap_anchor.frontier.height.0.to_le_bytes());
        hasher.update(self.bootstrap_anchor.frontier.hash.0);
        for checkpoint in self.local_checkpoints.iter() {
            hasher.update(checkpoint.height.0.to_le_bytes());
            hasher.update(checkpoint.hash.0);
        }
        hasher.finalize().into()
    }
}

/// Invalid immutable engine or trust-anchor configuration.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum EngineConfigError {
    /// A supplied trusted header failed a directly observable validation rule.
    #[error("trusted anchor failed {0}")]
    InvalidTrustedAnchor(&'static str),
    /// The canonical trusted header did not match its configured hash.
    #[error("trusted anchor header hashes to {actual:?}, expected {expected:?}")]
    AnchorHashMismatch {
        /// Configured hash.
        expected: block::Hash,
        /// Locally computed hash.
        actual: block::Hash,
    },
    /// Two local checkpoints name different hashes at one height.
    #[error("conflicting local checkpoint at {0:?}")]
    ConflictingCheckpoint(block::Height),
    /// A compiled settled hash failed canonical parsing.
    #[error("malformed compiled settled pin for {0:?}")]
    MalformedSettledPin(NetworkKind),
    /// A manifest contains more than one pin for a production identity.
    #[error("duplicate settled-upgrade production identity")]
    DuplicateSettledPin,
    /// Settled production pins cannot use the Regtest identity.
    #[error("settled-upgrade manifest cannot contain a Regtest pin")]
    InvalidSettledNetwork,
    /// A production configuration has no mandatory settled pin.
    #[error("missing mandatory settled pin for {0:?}")]
    MissingSettledPin(NetworkKind),
}

/// Immutable resource bounds for one header-chain engine version.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct EngineLimits {
    /// Irreversible local finality depth.
    pub local_finality_depth: NonZeroU32,
    /// Maximum retained eligible and ineligible candidate tips.
    pub max_candidate_tips: NonZeroUsize,
    /// Maximum retained non-finalized DAG nodes.
    pub max_non_finalized_nodes: NonZeroUsize,
}

impl EngineLimits {
    /// Return the exact limits frozen by specification version 1.3.
    pub fn v1() -> Self {
        Self {
            local_finality_depth: NonZeroU32::new(MAX_BLOCK_REORG_HEIGHT)
                .expect("the v1 local finality depth is nonzero"),
            max_candidate_tips: NonZeroUsize::new(MAX_CANDIDATE_TIPS_V1)
                .expect("the v1 candidate-tip limit is nonzero"),
            max_non_finalized_nodes: NonZeroUsize::new(MAX_NON_FINALIZED_NODES_V1)
                .expect("the v1 node limit is nonzero"),
        }
    }
}

impl Default for EngineLimits {
    fn default() -> Self {
        Self::v1()
    }
}

const _: () = assert!(MAX_BLOCK_REORG_HEIGHT == 1_000);
const _: () = assert!(MAX_CANDIDATE_TIPS_V1 == 10);
const _: () = assert!(MAX_NON_FINALIZED_NODES_V1 == 65_536);
const _: () = assert!(MAX_STAGED_TARGETS_V1 == 16);
