//! Versioned engine resource limits.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
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
    work::difficulty::ParameterDifficulty,
};

use crate::Frontier;

/// Maximum normalized trust entries accepted by one engine policy.
pub const MAX_TRUST_ENTRIES_V1: usize = 1_024;

/// Opaque identity of every consensus parameter that changes header validity.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct ConsensusPolicyId([u8; 32]);

impl ConsensusPolicyId {
    fn for_network(network: &Network) -> Self {
        let mut hasher = Sha256::new();
        append_consensus_policy(&mut hasher, network);
        Self(hasher.finalize().into())
    }

    /// Return the stable digest for durable encoding and equality checks.
    pub const fn digest(self) -> [u8; 32] {
        self.0
    }
}

/// Opaque identity of one normalized effective trust set.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct TrustSetId([u8; 32]);

impl TrustSetId {
    fn for_entries(entries: &BTreeMap<block::Height, TrustEntry>) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"zakura-header-chain-trust-set");
        hasher.update([1]);
        for (height, entry) in entries {
            hasher.update([1]);
            hasher.update(height.0.to_le_bytes());
            hasher.update([2]);
            hasher.update(entry.hash.0);
        }
        Self(hasher.finalize().into())
    }

    /// Return the stable digest for durable encoding and equality checks.
    pub const fn digest(self) -> [u8; 32] {
        self.0
    }
}

/// Authenticated source of one effective trust entry.
#[derive(Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum TrustSource {
    /// Exact bootstrap anchor supplied with the engine configuration.
    Bootstrap,
    /// Release-authenticated settled network-upgrade pin.
    SettledUpgrade,
    /// Operator-authenticated local checkpoint.
    LocalCheckpoint,
}

impl fmt::Display for TrustSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Bootstrap => "bootstrap",
            Self::SettledUpgrade => "settled-upgrade",
            Self::LocalCheckpoint => "local-checkpoint",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TrustEntry {
    hash: block::Hash,
    sources: BTreeSet<TrustSource>,
}

/// Checked, normalized effective trust policy.
#[derive(Clone, Debug, Eq, PartialEq)]
struct TrustSet {
    entries: BTreeMap<block::Height, TrustEntry>,
    id: TrustSetId,
}

/// One normalized durable trust entry with diagnostic source provenance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableTrustEntry {
    frontier: Frontier,
    sources: Arc<[TrustSource]>,
}

/// Durable record of one checked monotonic trust-set extension.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableTrustSetExtension {
    previous_trust_set_digest: [u8; 32],
    requested_trust_set_digest: [u8; 32],
    added: Arc<[DurableTrustEntry]>,
}

impl DurableTrustSetExtension {
    /// Validate an untrusted durable extension record against its current binding.
    pub fn from_untrusted_durable(
        previous_trust_set_digest: [u8; 32],
        requested_trust_set_digest: [u8; 32],
        added: impl IntoIterator<Item = DurableTrustEntry>,
        current: &EnginePolicyBinding,
    ) -> Option<Self> {
        let added: Arc<[DurableTrustEntry]> = added.into_iter().collect::<Vec<_>>().into();
        if added.is_empty()
            || previous_trust_set_digest == requested_trust_set_digest
            || requested_trust_set_digest != current.trust_set_digest
        {
            return None;
        }

        let added_by_height: BTreeMap<_, _> = added
            .iter()
            .map(|entry| (entry.frontier.height, entry))
            .collect();
        let expected_added: Vec<_> = current
            .trust_entries
            .iter()
            .filter(|entry| added_by_height.contains_key(&entry.frontier.height))
            .cloned()
            .collect();
        if added_by_height.len() != added.len()
            || added.as_ref() != expected_added.as_slice()
            || added_by_height
                .values()
                .any(|added| !current.trust_entries.iter().any(|entry| entry == *added))
        {
            return None;
        }

        let previous = TrustSet::new(
            current
                .trust_entries
                .iter()
                .filter(|entry| !added_by_height.contains_key(&entry.frontier.height))
                .flat_map(|entry| {
                    entry
                        .sources
                        .iter()
                        .copied()
                        .map(move |source| (entry.frontier, source))
                }),
        )
        .ok()?;
        (!previous.entries.is_empty() && previous.id().digest() == previous_trust_set_digest)
            .then_some(Self {
                previous_trust_set_digest,
                requested_trust_set_digest,
                added,
            })
    }

    /// Return the previous effective trust-set digest.
    pub const fn previous_trust_set_digest(&self) -> [u8; 32] {
        self.previous_trust_set_digest
    }

    /// Return the requested effective trust-set digest.
    pub const fn requested_trust_set_digest(&self) -> [u8; 32] {
        self.requested_trust_set_digest
    }

    /// Return each entry added by this extension.
    pub fn added(&self) -> &[DurableTrustEntry] {
        &self.added
    }
}

impl DurableTrustEntry {
    /// Construct an untrusted durable entry while checking its provenance shape.
    pub fn from_untrusted_durable(
        frontier: Frontier,
        sources: impl IntoIterator<Item = TrustSource>,
    ) -> Result<Self, EngineConfigError> {
        let sources: BTreeSet<_> = sources.into_iter().collect();
        if sources.is_empty() {
            return Err(EngineConfigError::MissingTrustProvenance(frontier.height));
        }
        Ok(Self {
            frontier,
            sources: sources.into_iter().collect::<Vec<_>>().into(),
        })
    }

    /// Return the exact trusted height and hash.
    pub const fn frontier(&self) -> Frontier {
        self.frontier
    }

    /// Return normalized source provenance.
    pub fn sources(&self) -> &[TrustSource] {
        &self.sources
    }
}

/// Immutable policy identity and normalized trust data stored with engine metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnginePolicyBinding {
    consensus_policy_digest: [u8; 32],
    trust_set_digest: [u8; 32],
    trust_entries: Arc<[DurableTrustEntry]>,
}

impl EnginePolicyBinding {
    /// Validate an untrusted durable policy binding before recovery uses it.
    pub fn from_untrusted_durable(
        consensus_policy_digest: [u8; 32],
        trust_set_digest: [u8; 32],
        trust_entries: impl IntoIterator<Item = DurableTrustEntry>,
    ) -> Result<Self, EngineConfigError> {
        let trust_entries: Vec<_> = trust_entries.into_iter().collect();
        let trust_set = TrustSet::new(trust_entries.iter().flat_map(|entry| {
            entry
                .sources
                .iter()
                .copied()
                .map(move |source| (entry.frontier, source))
        }))?;
        let normalized_entries = trust_set.durable_entries();
        if trust_set.id().digest() != trust_set_digest
            || trust_entries.as_slice() != normalized_entries.as_ref()
        {
            return Err(EngineConfigError::TrustSetIdentityMismatch);
        }
        Ok(Self {
            consensus_policy_digest,
            trust_set_digest,
            trust_entries: normalized_entries,
        })
    }

    /// Return the stored consensus-policy digest.
    pub const fn consensus_policy_digest(&self) -> [u8; 32] {
        self.consensus_policy_digest
    }

    /// Return the stored trust-set digest.
    pub const fn trust_set_digest(&self) -> [u8; 32] {
        self.trust_set_digest
    }

    /// Return the stored normalized trust entries.
    pub fn trust_entries(&self) -> &[DurableTrustEntry] {
        &self.trust_entries
    }

    /// Classify this stored binding against one checked requested policy.
    pub(crate) fn classify(
        &self,
        requested: &EnginePolicy,
    ) -> Result<PolicyBindingMatch, PolicyBindingMismatch> {
        if self.consensus_policy_digest != requested.consensus_policy_id().digest() {
            return Err(PolicyBindingMismatch::ConsensusPolicy);
        }
        let stored = TrustSet::new(self.trust_entries.iter().flat_map(|entry| {
            entry
                .sources
                .iter()
                .copied()
                .map(move |source| (entry.frontier, source))
        }))
        .map_err(|_| PolicyBindingMismatch::StoredTrustSet)?;
        if stored.id().digest() != self.trust_set_digest {
            return Err(PolicyBindingMismatch::StoredTrustSet);
        }
        if self.trust_set_digest == requested.trust_set_id().digest() {
            return Ok(
                if self.trust_entries.as_ref() == requested.durable_binding.trust_entries() {
                    PolicyBindingMatch::Exact
                } else {
                    PolicyBindingMatch::ProvenanceUpdate
                },
            );
        }
        let extension = stored.checked_extension(&requested.trust_set)?;
        Ok(PolicyBindingMatch::MonotonicExtension(extension))
    }
}

/// Checked relationship between a durable policy binding and the requested policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PolicyBindingMatch {
    /// Both effective identities match.
    Exact,
    /// Effective trust matches, but the authenticated source provenance changed.
    ProvenanceUpdate,
    /// The requested trust set only adds entries.
    MonotonicExtension(TrustSetExtension),
}

/// Reason that a durable policy binding cannot admit the requested policy.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum PolicyBindingMismatch {
    /// Consensus behavior changed.
    #[error("consensus policy changed")]
    ConsensusPolicy,
    /// Stored trust data failed checked reconstruction.
    #[error("stored trust set is invalid")]
    StoredTrustSet,
    /// The requested trust set removed or changed an effective entry.
    #[error("requested trust set is not a monotonic extension")]
    NonMonotonicTrustSet,
}

/// Checked monotonic addition to one effective trust set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TrustSetExtension {
    previous: TrustSetId,
    requested: TrustSetId,
    added: Arc<[DurableTrustEntry]>,
}

impl TrustSetExtension {
    /// Return the previous effective trust identity.
    #[cfg(test)]
    pub(crate) const fn previous(&self) -> TrustSetId {
        self.previous
    }

    /// Return the requested effective trust identity.
    #[cfg(test)]
    pub(crate) const fn requested(&self) -> TrustSetId {
        self.requested
    }

    /// Return each newly added effective trust entry.
    pub(crate) fn added(&self) -> &[DurableTrustEntry] {
        &self.added
    }

    pub(crate) fn durable_record(&self) -> DurableTrustSetExtension {
        DurableTrustSetExtension {
            previous_trust_set_digest: self.previous.digest(),
            requested_trust_set_digest: self.requested.digest(),
            added: self.added.clone(),
        }
    }
}

impl TrustSet {
    fn new(
        entries: impl IntoIterator<Item = (Frontier, TrustSource)>,
    ) -> Result<Self, EngineConfigError> {
        let mut normalized = BTreeMap::<block::Height, TrustEntry>::new();
        for (frontier, source) in entries {
            match normalized.entry(frontier.height) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(TrustEntry {
                        hash: frontier.hash,
                        sources: BTreeSet::from([source]),
                    });
                    if normalized.len() > MAX_TRUST_ENTRIES_V1 {
                        return Err(EngineConfigError::TooManyTrustEntries);
                    }
                }
                std::collections::btree_map::Entry::Occupied(mut entry)
                    if entry.get().hash == frontier.hash =>
                {
                    entry.get_mut().sources.insert(source);
                }
                std::collections::btree_map::Entry::Occupied(entry) => {
                    return Err(EngineConfigError::ConflictingTrustEntry {
                        height: frontier.height,
                        first: *entry
                            .get()
                            .sources
                            .iter()
                            .next()
                            .expect("every trust entry retains a source"),
                        second: source,
                    });
                }
            }
        }
        let id = TrustSetId::for_entries(&normalized);
        Ok(Self {
            entries: normalized,
            id,
        })
    }

    /// Iterate normalized effective entries in ascending height order.
    #[cfg(test)]
    fn iter(&self) -> impl Iterator<Item = Frontier> + '_ {
        self.entries
            .iter()
            .map(|(height, entry)| Frontier::new(*height, entry.hash))
    }

    /// Return the identity of this normalized effective trust set.
    pub const fn id(&self) -> TrustSetId {
        self.id
    }

    fn non_bootstrap_pins(&self) -> Arc<[Frontier]> {
        self.entries
            .iter()
            .filter(|(_, entry)| {
                entry.sources.iter().any(|source| {
                    matches!(
                        source,
                        TrustSource::SettledUpgrade | TrustSource::LocalCheckpoint
                    )
                })
            })
            .map(|(height, entry)| Frontier::new(*height, entry.hash))
            .collect::<Vec<_>>()
            .into()
    }

    fn durable_entries(&self) -> Arc<[DurableTrustEntry]> {
        self.entries
            .iter()
            .map(|(height, entry)| DurableTrustEntry {
                frontier: Frontier::new(*height, entry.hash),
                sources: entry.sources.iter().copied().collect::<Vec<_>>().into(),
            })
            .collect::<Vec<_>>()
            .into()
    }

    fn checked_extension(
        &self,
        requested: &Self,
    ) -> Result<TrustSetExtension, PolicyBindingMismatch> {
        for (height, stored) in &self.entries {
            if requested.entries.get(height).map(|entry| entry.hash) != Some(stored.hash) {
                return Err(PolicyBindingMismatch::NonMonotonicTrustSet);
            }
        }
        let added = requested
            .durable_entries()
            .iter()
            .filter(|entry| !self.entries.contains_key(&entry.frontier.height))
            .cloned()
            .collect::<Vec<_>>()
            .into();
        Ok(TrustSetExtension {
            previous: self.id,
            requested: requested.id,
            added,
        })
    }
}

fn append_consensus_policy(hasher: &mut Sha256, network: &Network) {
    hasher.update(b"zakura-header-chain-consensus-policy");
    hasher.update([1]);
    hasher.update([1]);
    hasher.update([match network.kind() {
        NetworkKind::Mainnet => 0,
        NetworkKind::Testnet => 1,
        NetworkKind::Regtest => 2,
    }]);
    hasher.update([2]);
    hasher.update(network.genesis_hash().0);
    hasher.update([3]);
    let target: zakura_chain::work::difficulty::U256 = network.target_difficulty_limit().into();
    hasher.update(target.to_big_endian());
    hasher.update([4, u8::from(network.disable_pow())]);
    hasher.update([5]);
    let max_time_height = match network {
        Network::Mainnet => block::Height::MIN,
        Network::Testnet(parameters) => parameters.max_block_time_start_height(),
    };
    hasher.update(max_time_height.0.to_le_bytes());
    for (height, upgrade) in network.activation_list() {
        hasher.update([6]);
        hasher.update(height.0.to_le_bytes());
        let (branch_tag, upgrade_code) = match upgrade.branch_id() {
            Some(branch) => (1_u8, u32::from(branch)),
            None => (
                0,
                match upgrade {
                    NetworkUpgrade::Genesis => 0,
                    NetworkUpgrade::BeforeOverwinter => 1,
                    NetworkUpgrade::Nu7 => 2,
                    _ => u32::MAX,
                },
            ),
        };
        hasher.update([7, branch_tag]);
        hasher.update(upgrade_code.to_le_bytes());
    }
}

/// Exact v1 maximum number of retained non-finalized header nodes.
pub const MAX_NON_FINALIZED_NODES_V1: usize = 65_536;
/// Exact v1 maximum number of staged unknown targets across all peers.
pub const MAX_STAGED_TARGETS_V1: usize = 16;
/// Exact v1 maximum prepared headers admitted by one transition.
pub const MAX_HEADERS_PER_TRANSITION_V1: usize = 4_000;
/// Exact v1 maximum auxiliary deliveries retained for one header.
pub const MAX_AUX_DELIVERIES_PER_HEADER_V1: usize = 16;
/// Exact v1 maximum auxiliary deliveries retained across the graph.
pub const MAX_AUX_DELIVERIES_TOTAL_V1: usize = MAX_NON_FINALIZED_NODES_V1;
/// Full-state fork policy sets the exact v1 candidate-tip cap.
pub const MAX_CANDIDATE_TIPS_V1: usize = MAX_NON_FINALIZED_CHAIN_FORKS;
/// Exact v1 maximum active retained-path references supplied to one transition.
pub const MAX_RETENTION_REFERENCES_V1: usize = MAX_STAGED_TARGETS_V1 + MAX_CANDIDATE_TIPS_V1;

/// Header-engine integration and finality mode.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum EngineMode {
    /// Only full state advances finality.
    Integrated,
    /// The engine turns a selected header 1,000 blocks deep into a disclosed local trust pin.
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

/// The local checkpoint map authenticates both height and hash.
/// The map rejects height-only and hash-only entries.
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

/// This release compiles immutable settled pins.
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

    /// Return a production network's mandatory pin or `None` for a custom network.
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

/// Checked consensus and trust policy for one engine instance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnginePolicy {
    network: Network,
    bootstrap_anchor: TrustedAnchor,
    local_checkpoints: CheckpointSet,
    settled_manifest: SettledUpgradeManifest,
    consensus_policy_id: ConsensusPolicyId,
    trust_set: TrustSet,
    durable_binding: EnginePolicyBinding,
    trust_pins: Arc<[Frontier]>,
}

impl EnginePolicy {
    /// Construct and validate the complete consensus and trust policy.
    fn new(
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
        let consensus_policy_id = ConsensusPolicyId::for_network(&network);
        let trust_set = checked_trust_set(
            &settled_manifest,
            &network,
            &bootstrap_anchor,
            &local_checkpoints,
        )?;
        let durable_binding = EnginePolicyBinding {
            consensus_policy_digest: consensus_policy_id.digest(),
            trust_set_digest: trust_set.id().digest(),
            trust_entries: trust_set.durable_entries(),
        };
        let trust_pins = trust_set.non_bootstrap_pins();
        Ok(Self {
            network,
            bootstrap_anchor,
            local_checkpoints,
            settled_manifest,
            consensus_policy_id,
            trust_set,
            durable_binding,
            trust_pins,
        })
    }

    /// Return the exact consensus parameters.
    pub(crate) const fn network(&self) -> &Network {
        &self.network
    }

    /// Return the exact trusted bootstrap anchor.
    const fn bootstrap_anchor(&self) -> &TrustedAnchor {
        &self.bootstrap_anchor
    }

    /// Return the authenticated local checkpoint set.
    const fn local_checkpoints(&self) -> &CheckpointSet {
        &self.local_checkpoints
    }

    /// Return the mandatory release-authenticated settled pins.
    const fn settled_manifest(&self) -> &SettledUpgradeManifest {
        &self.settled_manifest
    }

    /// Return the complete consensus-policy identity.
    pub(crate) const fn consensus_policy_id(&self) -> ConsensusPolicyId {
        self.consensus_policy_id
    }

    /// Return the checked effective trust-set identity.
    pub(crate) const fn trust_set_id(&self) -> TrustSetId {
        self.trust_set.id()
    }

    /// Build the immutable durable binding for this checked policy.
    pub(crate) fn durable_binding(&self) -> EnginePolicyBinding {
        self.durable_binding.clone()
    }

    /// This method returns the cached trust pins for transition verification.
    pub(crate) fn trust_pins(&self) -> Arc<[Frontier]> {
        self.trust_pins.clone()
    }

    #[cfg(test)]
    fn with_local_checkpoints(
        &self,
        local_checkpoints: CheckpointSet,
    ) -> Result<Self, EngineConfigError> {
        Self::new(
            self.network.clone(),
            self.bootstrap_anchor.clone(),
            local_checkpoints,
        )
    }
}

/// Immutable pure-engine configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EngineConfig {
    /// Finality authority mode.
    pub mode: EngineMode,
    policy: EnginePolicy,
    /// Frozen engine resource limits.
    pub limits: EngineLimits,
}

impl EngineConfig {
    /// Construct a configuration through the checked policy constructor.
    pub fn new(
        mode: EngineMode,
        network: Network,
        bootstrap_anchor: TrustedAnchor,
        local_checkpoints: CheckpointSet,
    ) -> Result<Self, EngineConfigError> {
        Ok(Self {
            mode,
            policy: EnginePolicy::new(network, bootstrap_anchor, local_checkpoints)?,
            limits: EngineLimits::v1(),
        })
    }

    /// Return the complete checked engine policy.
    pub const fn policy(&self) -> &EnginePolicy {
        &self.policy
    }

    /// Return the exact consensus parameters.
    pub const fn network(&self) -> &Network {
        self.policy.network()
    }

    /// Return the exact trusted bootstrap anchor.
    pub const fn bootstrap_anchor(&self) -> &TrustedAnchor {
        self.policy.bootstrap_anchor()
    }

    /// Return the authenticated local checkpoint set.
    pub const fn local_checkpoints(&self) -> &CheckpointSet {
        self.policy.local_checkpoints()
    }

    /// Return the mandatory release-authenticated settled pins.
    pub const fn settled_manifest(&self) -> &SettledUpgradeManifest {
        self.policy.settled_manifest()
    }

    /// Return the complete consensus-policy identity.
    pub const fn consensus_policy_id(&self) -> ConsensusPolicyId {
        self.policy.consensus_policy_id()
    }

    /// Return the checked effective trust-set identity.
    pub const fn trust_set_id(&self) -> TrustSetId {
        self.policy.trust_set_id()
    }

    /// Build the immutable durable binding for this checked policy.
    pub fn durable_policy_binding(&self) -> EnginePolicyBinding {
        self.policy.durable_binding()
    }

    pub(crate) fn trust_pins(&self) -> Arc<[Frontier]> {
        self.policy.trust_pins()
    }

    #[cfg(test)]
    pub(crate) fn replace_local_checkpoints(&mut self, local_checkpoints: CheckpointSet) {
        self.policy = self
            .policy
            .with_local_checkpoints(local_checkpoints)
            .expect("test checkpoint replacement must remain checked");
    }
}

fn checked_trust_set(
    settled_manifest: &SettledUpgradeManifest,
    network: &Network,
    bootstrap_anchor: &TrustedAnchor,
    local_checkpoints: &CheckpointSet,
) -> Result<TrustSet, EngineConfigError> {
    let settled = settled_manifest
        .pin_for_network(network)
        .into_iter()
        .map(|pin| (pin.activation, TrustSource::SettledUpgrade));
    let local = local_checkpoints
        .iter()
        .map(|checkpoint| (checkpoint, TrustSource::LocalCheckpoint));
    TrustSet::new(
        std::iter::once((bootstrap_anchor.frontier, TrustSource::Bootstrap))
            .chain(settled)
            .chain(local),
    )
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
    /// Two authenticated trust sources name different hashes at one height.
    #[error("conflicting trust entry at {height:?} from {first} and {second}")]
    ConflictingTrustEntry {
        /// Conflicting height.
        height: block::Height,
        /// Previously normalized source.
        first: TrustSource,
        /// Conflicting new source.
        second: TrustSource,
    },
    /// A durable trust entry has no authenticated source provenance.
    #[error("durable trust entry at {0:?} has no source provenance")]
    MissingTrustProvenance(block::Height),
    /// Durable trust entries do not produce the claimed trust-set identity.
    #[error("durable trust entries do not match the claimed trust-set identity")]
    TrustSetIdentityMismatch,
    /// The normalized trust set exceeds its fixed configuration bound.
    #[error("engine trust set exceeds its fixed entry bound")]
    TooManyTrustEntries,
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
    /// Maximum prepared headers accepted before any batch-proportional work.
    pub max_headers_per_transition: NonZeroUsize,
    /// Maximum fixed-size auxiliary records retained for one header.
    pub max_aux_deliveries_per_header: NonZeroUsize,
    /// Maximum fixed-size auxiliary records retained across the graph.
    pub max_aux_deliveries_total: NonZeroUsize,
    /// Maximum active retained-path references admitted by one transition.
    pub max_retention_references: NonZeroUsize,
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
            max_headers_per_transition: NonZeroUsize::new(MAX_HEADERS_PER_TRANSITION_V1)
                .expect("the v1 per-transition header limit is nonzero"),
            max_aux_deliveries_per_header: NonZeroUsize::new(MAX_AUX_DELIVERIES_PER_HEADER_V1)
                .expect("the v1 per-header auxiliary limit is nonzero"),
            max_aux_deliveries_total: NonZeroUsize::new(MAX_AUX_DELIVERIES_TOTAL_V1)
                .expect("the v1 aggregate auxiliary limit is nonzero"),
            max_retention_references: NonZeroUsize::new(MAX_RETENTION_REFERENCES_V1)
                .expect("the v1 retained-path reference limit is nonzero"),
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
const _: () = assert!(MAX_HEADERS_PER_TRANSITION_V1 == 4_000);
const _: () = assert!(MAX_AUX_DELIVERIES_PER_HEADER_V1 == 16);
const _: () = assert!(MAX_AUX_DELIVERIES_TOTAL_V1 == 65_536);
const _: () = assert!(MAX_RETENTION_REFERENCES_V1 == 26);

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use zakura_chain::{
        block::{genesis::regtest_genesis_block, Block},
        parameters::testnet::RegtestParameters,
        serialization::ZcashDeserialize,
    };

    #[test]
    fn engine_limits_v1_match_the_frozen_specification() {
        let limits = EngineLimits::v1();
        assert_eq!(limits.local_finality_depth.get(), 1_000);
        assert_eq!(limits.max_candidate_tips.get(), 10);
        assert_eq!(limits.max_non_finalized_nodes.get(), 65_536);
        assert_eq!(
            limits.max_retention_references.get(),
            MAX_STAGED_TARGETS_V1 + limits.max_candidate_tips.get(),
            "one atomic transition can retain every active header target and full-state fork tip"
        );
    }

    #[test]
    fn release_manifest_pins_exact_v1_3_production_tuples() {
        let manifest = SettledUpgradeManifest::for_release().expect("compiled pins are valid");
        let pins: Vec<_> = manifest.iter().collect();
        assert_eq!(pins.len(), 2);

        let mainnet = manifest
            .pin_for_network(&Network::Mainnet)
            .expect("mainnet has a mandatory pin");
        assert_eq!(mainnet.upgrade, NetworkUpgrade::Nu6_2);
        assert_eq!(mainnet.activation.height, block::Height(3_364_600));
        assert_eq!(mainnet.activation.hash.0[0], 0x65);
        assert_eq!(mainnet.activation.hash.0[31], 0x00);
        assert_eq!(
            mainnet.activation.hash.to_string(),
            "0000000000806344c408a4cfdf472f4132c632edbdc24cf2f3f672061da8b865"
        );

        let testnet = manifest
            .pin_for_network(&Network::new_default_testnet())
            .expect("default testnet has a mandatory pin");
        assert_eq!(testnet.upgrade, NetworkUpgrade::Nu6_2);
        assert_eq!(testnet.activation.height, block::Height(4_052_000));
        assert_eq!(testnet.activation.hash.0[0], 0x12);
        assert_eq!(testnet.activation.hash.0[31], 0x00);
        assert_eq!(
            testnet.activation.hash.to_string(),
            "0010cb912b0188da5bc055ee67e3f77d30cd27611369d865974a5bf0b1ec2912"
        );

        let regtest = Network::new_regtest(RegtestParameters::default());
        assert_eq!(manifest.pin_for_network(&regtest), None);
        assert_eq!(
            manifest.digest(),
            SettledUpgradeManifest::for_release()
                .expect("compiled pins are deterministic")
                .digest()
        );
    }

    #[test]
    fn production_config_always_installs_the_release_manifest() {
        for (network, bytes) in [
            (
                Network::Mainnet,
                zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES.as_slice(),
            ),
            (
                Network::new_default_testnet(),
                zakura_test::vectors::BLOCK_TESTNET_GENESIS_BYTES.as_slice(),
            ),
        ] {
            let block = Arc::<Block>::zcash_deserialize(bytes)
                .expect("the production genesis vector is canonical");
            let config = EngineConfig::new(
                EngineMode::Integrated,
                network.clone(),
                TrustedAnchor {
                    frontier: Frontier::new(block::Height(0), block.hash()),
                    header: block.header.clone(),
                },
                CheckpointSet::default(),
            )
            .expect("the production genesis anchor passes every direct check");
            assert!(config
                .settled_manifest()
                .pin_for_network(&network)
                .is_some());
        }
    }

    #[test]
    fn trusted_anchor_still_runs_every_directly_observable_check() {
        let sapling =
            Arc::<Block>::zcash_deserialize(zakura_test::vectors::MAINNET_BLOCKS[&419_200])
                .expect("the Mainnet Sapling activation vector is canonical");
        let make_config = |network: Network, height: block::Height, header: Arc<block::Header>| {
            let frontier_hash =
                crate::validate_encoding_version_hash(&header).unwrap_or(block::Hash([0; 32]));
            EngineConfig::new(
                EngineMode::Integrated,
                network,
                TrustedAnchor {
                    frontier: Frontier::new(height, frontier_hash),
                    header,
                },
                CheckpointSet::default(),
            )
        };
        make_config(
            Network::Mainnet,
            block::Height(419_200),
            sapling.header.clone(),
        )
        .expect("the real production activation anchor passes every direct check");

        let mut bad_version = *sapling.header;
        bad_version.version = 3;
        assert_eq!(
            make_config(
                Network::Mainnet,
                block::Height(419_200),
                Arc::new(bad_version)
            ),
            Err(EngineConfigError::InvalidTrustedAnchor(
                "canonical header version and hash"
            ))
        );

        let mut bad_commitment = *sapling.header;
        bad_commitment.commitment_bytes.0 = [0xff; 32];
        assert_eq!(
            make_config(
                Network::Mainnet,
                block::Height(419_200),
                Arc::new(bad_commitment)
            ),
            Err(EngineConfigError::InvalidTrustedAnchor(
                "height-dependent commitment structure"
            ))
        );

        let mut bad_target = *sapling.header;
        bad_target.difficulty_threshold =
            zakura_chain::work::difficulty::CompactDifficulty::from_le_bytes([0; 4]);
        assert_eq!(
            make_config(
                Network::Mainnet,
                block::Height(419_200),
                Arc::new(bad_target)
            ),
            Err(EngineConfigError::InvalidTrustedAnchor(
                "compact target and network limit"
            ))
        );

        let target = crate::validate_compact_target(&sapling.header, &Network::Mainnet)
            .expect("the vector target is valid");
        let mut bad_hash = *sapling.header;
        bad_hash.nonce.0[0] = bad_hash.nonce.0[0].wrapping_add(1);
        assert!(
            crate::validate_hash_filter(bad_hash.hash(), target).is_err(),
            "the deterministic nonce mutation no longer satisfies production work"
        );
        assert_eq!(
            make_config(Network::Mainnet, block::Height(419_200), Arc::new(bad_hash)),
            Err(EngineConfigError::InvalidTrustedAnchor(
                "header hash filter"
            ))
        );

        let regtest = Network::new_regtest(RegtestParameters::default());
        let mut wrong_solution_shape = *regtest_genesis_block().header;
        wrong_solution_shape.solution = zakura_chain::work::equihash::Solution::for_proposal();
        assert_eq!(
            make_config(regtest, block::Height(0), Arc::new(wrong_solution_shape)),
            Err(EngineConfigError::InvalidTrustedAnchor(
                "Equihash solution shape or proof"
            ))
        );
    }

    #[test]
    fn engine_config_binds_and_validates_every_trust_anchor() {
        let block = regtest_genesis_block();
        let network = Network::new_regtest(RegtestParameters::default());
        let anchor = TrustedAnchor {
            frontier: Frontier::new(block::Height(0), block.hash()),
            header: block.header.clone(),
        };
        let plain = EngineConfig::new(
            EngineMode::HeadersOnly,
            network.clone(),
            anchor.clone(),
            CheckpointSet::default(),
        )
        .expect("the fixture anchor is canonical");
        let checkpointed = EngineConfig::new(
            EngineMode::HeadersOnly,
            network.clone(),
            anchor.clone(),
            CheckpointSet::new([Frontier::new(block::Height(10), block::Hash([9; 32]))])
                .expect("the fixture checkpoint set has unique heights"),
        )
        .expect("the fixture checkpoint is hash-qualified");
        assert_ne!(plain.trust_set_id(), checkpointed.trust_set_id());

        let mut replaced = plain.clone();
        replaced.replace_local_checkpoints(checkpointed.local_checkpoints().clone());
        assert_eq!(
            replaced.trust_set_id(),
            checkpointed.trust_set_id(),
            "test-only checkpoint replacement must rebuild the complete policy",
        );
        assert_eq!(
            replaced.trust_pins().as_ref(),
            checkpointed.trust_pins().as_ref()
        );
        assert!(
            !Arc::ptr_eq(&plain.trust_pins(), &replaced.trust_pins()),
            "test-only checkpoint replacement must refresh the cached pins"
        );
        assert!(
            Arc::ptr_eq(&checkpointed.trust_pins(), &checkpointed.trust_pins()),
            "successive plans can share the immutable trust-pin allocation"
        );

        let mismatched = TrustedAnchor {
            frontier: Frontier::new(block::Height(0), block::Hash([1; 32])),
            ..anchor
        };
        assert!(matches!(
            EngineConfig::new(
                EngineMode::HeadersOnly,
                network,
                mismatched,
                CheckpointSet::default()
            ),
            Err(EngineConfigError::AnchorHashMismatch { .. })
        ));
    }

    #[test]
    fn f225509_consensus_policy_identity_distinguishes_custom_network_policy() {
        let default = Network::new_regtest(RegtestParameters::default());
        let later_max_time = Network::new_regtest(RegtestParameters {
            max_block_time_start_height: Some(block::Height(3)),
            ..RegtestParameters::default()
        });
        let later_nu7 = Network::new_regtest(RegtestParameters {
            activation_heights: zakura_chain::parameters::testnet::ConfiguredActivationHeights {
                nu7: Some(10),
                ..Default::default()
            },
            ..RegtestParameters::default()
        });
        let nu6_3_at_ten = zakura_chain::parameters::testnet::Parameters::build()
            .clear_funding_streams()
            .with_activation_heights(
                zakura_chain::parameters::testnet::ConfiguredActivationHeights {
                    nu6_3: Some(10),
                    ..Default::default()
                },
            )
            .expect("the NU6.3 activation policy is ordered")
            .to_network()
            .expect("the NU6.3 activation policy is valid");
        let nu7_at_ten = zakura_chain::parameters::testnet::Parameters::build()
            .clear_funding_streams()
            .with_activation_heights(
                zakura_chain::parameters::testnet::ConfiguredActivationHeights {
                    nu7: Some(10),
                    ..Default::default()
                },
            )
            .expect("the NU7 activation policy is ordered")
            .to_network()
            .expect("the NU7 activation policy is valid");

        assert_eq!(default.kind(), later_max_time.kind());
        assert_eq!(default.kind(), later_nu7.kind());
        let default_testnet = Network::new_default_testnet();
        let custom_testnet = zakura_chain::parameters::testnet::Parameters::build()
            .with_disable_pow(true)
            .to_network()
            .expect("the custom testnet policy is valid");
        let changed_genesis_hash = block::Hash([0x42; 32]);
        let changed_genesis = zakura_chain::parameters::testnet::Parameters::build()
            .with_genesis_hash(changed_genesis_hash)
            .expect("the custom genesis hash is canonical")
            .with_checkpoints(
                zakura_chain::parameters::testnet::ConfiguredCheckpoints::HeightsAndHashes(vec![
                    (block::Height(0), changed_genesis_hash),
                    (
                        default_testnet.mandatory_checkpoint_height(),
                        block::Hash([0x43; 32]),
                    ),
                ]),
            )
            .expect("the custom checkpoints follow the genesis hash")
            .to_network()
            .expect("the custom genesis policy is valid");
        assert_eq!(
            [
                ConsensusPolicyId::for_network(&Network::Mainnet).digest(),
                ConsensusPolicyId::for_network(&default_testnet).digest(),
                ConsensusPolicyId::for_network(&default).digest(),
                ConsensusPolicyId::for_network(&custom_testnet).digest(),
            ],
            [
                [
                    236, 43, 230, 97, 212, 183, 148, 254, 6, 102, 64, 193, 45, 117, 80, 86, 154,
                    70, 138, 48, 93, 18, 177, 16, 209, 95, 140, 245, 48, 11, 138, 99,
                ],
                [
                    152, 43, 91, 236, 252, 37, 96, 95, 106, 100, 165, 142, 47, 253, 40, 202, 118,
                    24, 117, 30, 224, 236, 194, 230, 84, 250, 141, 9, 119, 79, 165, 46,
                ],
                [
                    45, 32, 215, 71, 166, 97, 17, 19, 223, 223, 143, 91, 224, 80, 144, 90, 77, 73,
                    66, 209, 56, 155, 61, 46, 255, 199, 92, 47, 44, 70, 2, 33,
                ],
                [
                    96, 78, 57, 182, 178, 155, 153, 74, 208, 114, 230, 42, 104, 112, 129, 180, 254,
                    5, 92, 131, 244, 252, 11, 191, 72, 83, 85, 144, 64, 129, 63, 130,
                ],
            ],
            "the versioned encoder has stable production and configurable-policy vectors",
        );
        assert_ne!(
            ConsensusPolicyId::for_network(&default),
            ConsensusPolicyId::for_network(&later_max_time),
            "maximum-time policy must be part of consensus identity",
        );
        assert_ne!(
            ConsensusPolicyId::for_network(&default),
            ConsensusPolicyId::for_network(&later_nu7),
            "activation policy must be part of consensus identity",
        );
        assert_ne!(
            ConsensusPolicyId::for_network(&nu6_3_at_ten),
            ConsensusPolicyId::for_network(&nu7_at_ten),
            "upgrade and branch identity must be part of consensus identity",
        );
        assert_eq!(
            ConsensusPolicyId::for_network(&default),
            ConsensusPolicyId::for_network(&Network::new_regtest(RegtestParameters::default())),
            "equivalent policy construction must have stable identity",
        );

        let public_testnet = zakura_chain::parameters::testnet::Parameters::build()
            .to_network()
            .expect("the default configured testnet policy is valid");
        let pow_waived = zakura_chain::parameters::testnet::Parameters::build()
            .with_disable_pow(true)
            .to_network()
            .expect("the proof-of-work-waived policy is valid");
        let easier_target = zakura_chain::parameters::testnet::Parameters::build()
            .with_target_difficulty_limit(default.target_difficulty_limit())
            .expect("the regtest target is canonical")
            .to_network()
            .expect("the custom target policy is valid");
        assert_eq!(public_testnet.kind(), pow_waived.kind());
        assert_ne!(
            ConsensusPolicyId::for_network(&public_testnet),
            ConsensusPolicyId::for_network(&pow_waived),
            "proof-of-work policy must be part of consensus identity",
        );
        assert_ne!(
            ConsensusPolicyId::for_network(&public_testnet),
            ConsensusPolicyId::for_network(&easier_target),
            "target limit must be part of consensus identity",
        );
        assert_ne!(
            ConsensusPolicyId::for_network(&default_testnet),
            ConsensusPolicyId::for_network(&changed_genesis),
            "genesis hash must be part of consensus identity",
        );
    }

    #[test]
    fn f225510_trust_set_rejects_cross_source_conflicts() {
        let height = block::Height(42);
        let first = Frontier::new(height, block::Hash([1; 32]));
        let second = Frontier::new(height, block::Hash([2; 32]));

        assert_eq!(
            TrustSet::new([
                (first, TrustSource::Bootstrap),
                (second, TrustSource::LocalCheckpoint),
            ]),
            Err(EngineConfigError::ConflictingTrustEntry {
                height,
                first: TrustSource::Bootstrap,
                second: TrustSource::LocalCheckpoint,
            }),
        );
    }

    #[test]
    fn trust_set_identity_uses_normalized_effective_entries() {
        let first = Frontier::new(block::Height(10), block::Hash([1; 32]));
        let second = Frontier::new(block::Height(20), block::Hash([2; 32]));
        let ordered = TrustSet::new([
            (first, TrustSource::Bootstrap),
            (first, TrustSource::LocalCheckpoint),
            (second, TrustSource::SettledUpgrade),
        ])
        .expect("exact cross-source duplicates are valid");
        let reordered = TrustSet::new([
            (second, TrustSource::SettledUpgrade),
            (first, TrustSource::LocalCheckpoint),
            (first, TrustSource::Bootstrap),
        ])
        .expect("input order cannot change a valid trust set");

        assert_eq!(ordered.id(), reordered.id());
        assert_eq!(ordered.iter().collect::<Vec<_>>(), vec![first, second]);
        assert_ne!(
            TrustSet::new([])
                .expect("the identity function accepts an empty fixture")
                .id(),
            ordered.id(),
        );
        assert_ne!(
            ordered.id(),
            TrustSet::new([
                (
                    Frontier::new(block::Height(11), first.hash),
                    TrustSource::Bootstrap,
                ),
                (second, TrustSource::SettledUpgrade),
            ])
            .expect("changed-height fixture is valid")
            .id(),
        );
        assert_ne!(
            ordered.id(),
            TrustSet::new([
                (first, TrustSource::Bootstrap),
                (
                    Frontier::new(second.height, block::Hash([3; 32])),
                    TrustSource::SettledUpgrade,
                ),
            ])
            .expect("changed-hash fixture is valid")
            .id(),
        );

        let mut reversed = ordered.durable_entries().to_vec();
        reversed.reverse();
        assert_eq!(
            EnginePolicyBinding::from_untrusted_durable([7; 32], ordered.id().digest(), reversed),
            Err(EngineConfigError::TrustSetIdentityMismatch),
            "durable policy bindings must use the canonical entry order",
        );
    }

    #[test]
    fn checked_policy_classifies_provenance_updates_without_changing_trust_identity() {
        let block = regtest_genesis_block();
        let policy = EnginePolicy::new(
            Network::new_regtest(RegtestParameters::default()),
            TrustedAnchor {
                frontier: Frontier::new(block::Height(0), block.hash()),
                header: block.header.clone(),
            },
            CheckpointSet::default(),
        )
        .expect("the requested policy is valid");
        let stored = EnginePolicyBinding::from_untrusted_durable(
            policy.consensus_policy_id().digest(),
            policy.trust_set_id().digest(),
            [DurableTrustEntry::from_untrusted_durable(
                Frontier::new(block::Height(0), block.hash()),
                [TrustSource::LocalCheckpoint],
            )
            .expect("the alternate provenance has a valid durable shape")],
        )
        .expect("the alternate provenance has the same effective trust identity");

        assert_eq!(
            stored.classify(&policy),
            Ok(PolicyBindingMatch::ProvenanceUpdate),
        );
    }

    #[test]
    fn trust_set_extension_preserves_every_effective_entry() {
        let bootstrap = Frontier::new(block::Height(0), block::Hash([1; 32]));
        let checkpoint = Frontier::new(block::Height(10), block::Hash([2; 32]));
        let later_checkpoint = Frontier::new(block::Height(20), block::Hash([4; 32]));
        let stored = TrustSet::new([(bootstrap, TrustSource::Bootstrap)])
            .expect("the stored fixture is valid");
        let requested = TrustSet::new([
            (bootstrap, TrustSource::Bootstrap),
            (checkpoint, TrustSource::LocalCheckpoint),
            (later_checkpoint, TrustSource::SettledUpgrade),
        ])
        .expect("the requested fixture is valid");
        let extension = stored
            .checked_extension(&requested)
            .expect("adding one entry is monotonic");

        assert_eq!(extension.previous(), stored.id());
        assert_eq!(extension.requested(), requested.id());
        assert_eq!(extension.added()[0].frontier(), checkpoint);
        assert_eq!(extension.added()[1].frontier(), later_checkpoint);
        let binding = EnginePolicyBinding {
            consensus_policy_digest: [7; 32],
            trust_set_digest: requested.id().digest(),
            trust_entries: requested.durable_entries(),
        };
        let record = extension.durable_record();
        assert_eq!(
            DurableTrustSetExtension::from_untrusted_durable(
                record.previous_trust_set_digest(),
                record.requested_trust_set_digest(),
                record.added().iter().cloned(),
                &binding,
            ),
            Some(record.clone()),
        );
        assert_eq!(
            DurableTrustSetExtension::from_untrusted_durable(
                [0; 32],
                record.requested_trust_set_digest(),
                record.added().iter().cloned(),
                &binding,
            ),
            None,
            "the decoder must reconstruct and authenticate the complete prior trust set",
        );
        assert_eq!(
            DurableTrustSetExtension::from_untrusted_durable(
                record.previous_trust_set_digest(),
                record.requested_trust_set_digest(),
                [DurableTrustEntry::from_untrusted_durable(
                    checkpoint,
                    [TrustSource::Bootstrap],
                )
                .expect("the forged entry has a valid shape")],
                &binding,
            ),
            None,
            "the decoder must authenticate the added entry provenance",
        );
        assert_eq!(
            DurableTrustSetExtension::from_untrusted_durable(
                record.previous_trust_set_digest(),
                record.requested_trust_set_digest(),
                record.added().iter().rev().cloned(),
                &binding,
            ),
            None,
            "the decoder must reject a noncanonical extension order",
        );
        assert_eq!(
            requested.checked_extension(&stored),
            Err(PolicyBindingMismatch::NonMonotonicTrustSet),
        );

        let repinned = TrustSet::new([
            (bootstrap, TrustSource::Bootstrap),
            (
                Frontier::new(checkpoint.height, block::Hash([3; 32])),
                TrustSource::LocalCheckpoint,
            ),
        ])
        .expect("the repin fixture is internally coherent");
        assert_eq!(
            requested.checked_extension(&repinned),
            Err(PolicyBindingMismatch::NonMonotonicTrustSet),
        );
    }

    #[test]
    fn trust_set_stops_at_its_fixed_configuration_bound() {
        let entries = (0..=MAX_TRUST_ENTRIES_V1).map(|height| {
            (
                Frontier::new(
                    block::Height(u32::try_from(height).expect("the test height fits in u32")),
                    block::Hash([u8::try_from(height % 251).expect("the hash byte fits"); 32]),
                ),
                TrustSource::LocalCheckpoint,
            )
        });
        assert_eq!(
            TrustSet::new(entries),
            Err(EngineConfigError::TooManyTrustEntries),
        );
    }
}
