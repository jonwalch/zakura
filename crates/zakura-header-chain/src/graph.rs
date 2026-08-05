//! Pure in-memory header DAG queries, eligibility propagation, and selection.

use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    sync::Arc,
};

use thiserror::Error;
use zakura_chain::{
    block,
    work::difficulty::{Work, U256},
};

use crate::{
    BodyRuleId, BodyValidationState, ChainScore, EligibilityReason, EligibilityState, EvidenceId,
    Frontier, HeaderNode, HeaderValidationState, OperatorInvalidationId, WorkCoordinate,
    WorkCoordinateError,
};

mod overlay;
pub(crate) use overlay::{GraphDelta, GraphOverlay};

pub(crate) trait HeaderGraphView {
    fn view_finalized(&self) -> Frontier;
    fn view_node_count(&self) -> usize;
    fn view_node(&self, hash: block::Hash) -> Option<&HeaderNode>;
    fn view_nodes(&self) -> Vec<&HeaderNode>;
    fn view_retained_hashes(&self) -> Vec<block::Hash>;
    fn view_hashes_at_height(&self, height: block::Height) -> Vec<block::Hash>;
    fn view_children(&self, parent: block::Hash) -> Vec<block::Hash>;
    fn view_eligible_tips(&self) -> Vec<Frontier>;
    fn view_select_header_best(&self) -> Result<(Frontier, ChainScore), GraphError>;
    fn view_score(&self, hash: block::Hash) -> Result<ChainScore, GraphError>;
    fn view_ancestor(
        &self,
        descendant: block::Hash,
        height: block::Height,
    ) -> Result<Option<Frontier>, GraphError>;
}

pub(crate) trait HeaderGraphEdit: HeaderGraphView {
    fn edit_node_mut(&mut self, hash: block::Hash) -> Result<&mut HeaderNode, GraphError>;
    fn edit_insert(
        &mut self,
        header: Arc<block::Header>,
        block_work: Work,
        validation: HeaderValidationState,
        direct_reasons: Vec<EligibilityReason>,
        body: BodyValidationState,
    ) -> Result<InsertResult, GraphError>;
    fn edit_add_reason(
        &mut self,
        hash: block::Hash,
        reason: EligibilityReason,
    ) -> Result<bool, GraphError>;
    fn edit_remove_operator_invalidation(
        &mut self,
        hash: block::Hash,
        id: OperatorInvalidationId,
    ) -> Result<bool, GraphError>;
    fn edit_set_consensus_body_invalid(
        &mut self,
        hash: block::Hash,
        evidence: EvidenceId,
        rule: BodyRuleId,
    ) -> Result<bool, GraphError>;
    fn edit_set_body_state(
        &mut self,
        hash: block::Hash,
        body: BodyValidationState,
    ) -> Result<bool, GraphError>;
    fn edit_set_validation(
        &mut self,
        hash: block::Hash,
        validation: HeaderValidationState,
    ) -> Result<bool, GraphError>;
    fn edit_advance_finalized(
        &mut self,
        finalized: Frontier,
    ) -> Result<Vec<block::Hash>, GraphError>;
    fn edit_remove_leaf(&mut self, hash: block::Hash) -> Result<(), GraphError>;
}

/// Failure to construct or query a coherent in-memory header DAG.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum GraphError {
    /// The supplied trusted anchor header does not hash to its frontier.
    #[error("trusted anchor header hashes to {actual:?}, expected {expected:?}")]
    AnchorHashMismatch {
        /// Expected configured anchor hash.
        expected: block::Hash,
        /// Locally computed header hash.
        actual: block::Hash,
    },
    /// A durable insertion attempted to reference an unknown parent.
    #[error("header {header:?} has unknown parent {parent:?}")]
    UnknownParent {
        /// Candidate header hash.
        header: block::Hash,
        /// Missing parent hash.
        parent: block::Hash,
    },
    /// The inferred child height crossed the supported range.
    #[error("child of {parent:?} exceeds the supported height range")]
    HeightOverflow {
        /// Parent hash at maximum height.
        parent: block::Hash,
    },
    /// The exact header hash is already retained with different contents.
    #[error("conflicting duplicate header {0:?}")]
    ConflictingDuplicate(block::Hash),
    /// A requested retained node does not exist.
    #[error("unknown retained header {0:?}")]
    UnknownNode(block::Hash),
    /// Finality attempted to root the graph at an ineligible header.
    #[error("cannot finalize ineligible retained header {0:?}")]
    IneligibleFinalized(block::Hash),
    /// Retention attempted to remove a node that still has retained children.
    #[error("cannot remove non-leaf header {0:?}")]
    NodeHasChildren(block::Hash),
    /// Body-invalid state and its exact durable eligibility reason disagreed.
    #[error("body-invalid state and eligibility evidence disagree for {0:?}")]
    BodyEligibilityMismatch(block::Hash),
    /// A requested ancestor height is above its descendant.
    #[error("ancestor height {ancestor:?} exceeds descendant height {descendant:?}")]
    InvalidAncestorHeight {
        /// Requested ancestor height.
        ancestor: block::Height,
        /// Descendant height.
        descendant: block::Height,
    },
    /// Exact work accumulation or rebasing failed closed.
    #[error(transparent)]
    Work(#[from] WorkCoordinateError),
}

/// Result of an idempotent DAG insertion.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum InsertResult {
    /// A new node and all reconstructible indexes were inserted.
    Inserted(Frontier),
    /// The exact same node was already retained.
    AlreadyPresent(Frontier),
}

/// Pure hash-keyed in-memory implementation of the header DAG contract.
#[derive(Clone, Debug)]
pub struct MemHeaderStore {
    finalized: Frontier,
    nodes: HashMap<block::Hash, HeaderNode>,
    children: HashMap<block::Hash, HashSet<block::Hash>>,
    heights: HashMap<block::Height, HashSet<block::Hash>>,
    eligible_tips: HashSet<block::Hash>,
}

impl MemHeaderStore {
    /// Construct a store rooted at one trusted, already-validated work origin.
    pub fn new(
        finalized: Frontier,
        header: Arc<block::Header>,
        block_work: Work,
        cumulative_work: U256,
    ) -> Result<Self, GraphError> {
        let actual = header.hash();
        if actual != finalized.hash {
            return Err(GraphError::AnchorHashMismatch {
                expected: finalized.hash,
                actual,
            });
        }
        let anchor = HeaderNode {
            parent_hash: header.previous_block_hash,
            header,
            hash: finalized.hash,
            height: finalized.height,
            block_work,
            work_coordinate: WorkCoordinate::new(finalized.hash, cumulative_work),
            validation: HeaderValidationState::Valid,
            eligibility: EligibilityState::default(),
            body: BodyValidationState::Unknown,
            aux_delivery_ids: Vec::new(),
        };
        let mut nodes = HashMap::new();
        nodes.insert(finalized.hash, anchor);
        let mut heights = HashMap::new();
        heights.insert(finalized.height, HashSet::from([finalized.hash]));
        Ok(Self {
            finalized,
            nodes,
            children: HashMap::new(),
            heights,
            eligible_tips: HashSet::from([finalized.hash]),
        })
    }

    /// Return the immutable finalized root of every eligible path.
    pub const fn finalized(&self) -> Frontier {
        self.finalized
    }

    /// Return the number of retained nodes, including the finalized anchor.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Read one retained node by exact consensus hash.
    pub fn node(&self, hash: block::Hash) -> Option<&HeaderNode> {
        self.nodes.get(&hash)
    }

    /// Return every retained hash at a height, ordered by raw internal bytes.
    pub fn hashes_at_height(&self, height: block::Height) -> Vec<block::Hash> {
        let mut hashes: Vec<_> = self
            .heights
            .get(&height)
            .into_iter()
            .flatten()
            .copied()
            .collect();
        hashes.sort_unstable_by_key(|hash| hash.0);
        hashes
    }

    /// Return direct children ordered by raw internal bytes.
    pub fn children(&self, parent: block::Hash) -> Vec<block::Hash> {
        let mut children: Vec<_> = self
            .children
            .get(&parent)
            .into_iter()
            .flatten()
            .copied()
            .collect();
        children.sort_unstable_by_key(|hash| hash.0);
        children
    }

    /// Insert one admitted header after its exact parent is retained.
    pub(crate) fn insert(
        &mut self,
        header: Arc<block::Header>,
        block_work: Work,
        validation: HeaderValidationState,
        direct_reasons: impl IntoIterator<Item = EligibilityReason>,
        body: BodyValidationState,
    ) -> Result<InsertResult, GraphError> {
        let hash = header.hash();
        if let Some(existing) = self.nodes.get(&hash) {
            if existing.header == header {
                return Ok(InsertResult::AlreadyPresent(Frontier::new(
                    existing.height,
                    hash,
                )));
            }
            return Err(GraphError::ConflictingDuplicate(hash));
        }
        let parent_hash = header.previous_block_hash;
        let parent = self
            .nodes
            .get(&parent_hash)
            .ok_or(GraphError::UnknownParent {
                header: hash,
                parent: parent_hash,
            })?;
        let height = parent
            .height
            .next()
            .map_err(|_| GraphError::HeightOverflow {
                parent: parent_hash,
            })?;
        let inherited_from = (!parent.is_eligible()).then_some(parent_hash);
        let direct_reasons: BTreeSet<EligibilityReason> = direct_reasons.into_iter().collect();
        let body_reason = match &body {
            BodyValidationState::ConsensusInvalid { evidence, rule } => {
                Some(EligibilityReason::ConsensusBodyInvalid {
                    evidence: *evidence,
                    rule: rule.clone(),
                })
            }
            _ => None,
        };
        let recorded_body_reasons = direct_reasons
            .iter()
            .filter(|reason| matches!(reason, EligibilityReason::ConsensusBodyInvalid { .. }))
            .count();
        if body_reason
            .as_ref()
            .is_some_and(|reason| !direct_reasons.contains(reason))
            || (body_reason.is_none() && recorded_body_reasons != 0)
            || recorded_body_reasons > 1
        {
            return Err(GraphError::BodyEligibilityMismatch(hash));
        }
        let node = HeaderNode {
            header,
            hash,
            parent_hash,
            height,
            block_work,
            work_coordinate: parent.work_coordinate().checked_add(block_work)?,
            validation,
            eligibility: EligibilityState {
                direct_reasons,
                inherited_from,
            },
            body,
            aux_delivery_ids: Vec::new(),
        };
        self.nodes.insert(hash, node);
        self.children.entry(parent_hash).or_default().insert(hash);
        self.heights.entry(height).or_default().insert(hash);
        if self
            .nodes
            .get(&hash)
            .expect("the inserted node is present")
            .is_eligible()
        {
            self.eligible_tips.remove(&parent_hash);
            self.eligible_tips.insert(hash);
        }
        Ok(InsertResult::Inserted(Frontier::new(height, hash)))
    }

    /// Add one independent direct reason, then recompute the affected subtree cache.
    pub(crate) fn add_reason(
        &mut self,
        hash: block::Hash,
        reason: EligibilityReason,
    ) -> Result<bool, GraphError> {
        if matches!(reason, EligibilityReason::ConsensusBodyInvalid { .. }) {
            return Err(GraphError::BodyEligibilityMismatch(hash));
        }
        let changed = self
            .nodes
            .get_mut(&hash)
            .ok_or(GraphError::UnknownNode(hash))?
            .eligibility
            .direct_reasons
            .insert(reason);
        if changed {
            self.recompute_descendant_eligibility(hash)?;
        }
        Ok(changed)
    }

    /// Remove exactly one operator invalidation, preserving every unrelated reason.
    pub(crate) fn remove_operator_invalidation(
        &mut self,
        hash: block::Hash,
        id: OperatorInvalidationId,
    ) -> Result<bool, GraphError> {
        let reason = EligibilityReason::OperatorInvalid { id };
        let changed = self
            .nodes
            .get_mut(&hash)
            .ok_or(GraphError::UnknownNode(hash))?
            .eligibility
            .direct_reasons
            .remove(&reason);
        if changed {
            self.recompute_descendant_eligibility(hash)?;
        }
        Ok(changed)
    }

    /// Atomically record one commitment-matching deterministic body failure.
    pub(crate) fn set_consensus_body_invalid(
        &mut self,
        hash: block::Hash,
        evidence: EvidenceId,
        rule: BodyRuleId,
    ) -> Result<bool, GraphError> {
        let node = self
            .nodes
            .get_mut(&hash)
            .ok_or(GraphError::UnknownNode(hash))?;
        let body = BodyValidationState::ConsensusInvalid {
            evidence,
            rule: rule.clone(),
        };
        let reason = EligibilityReason::ConsensusBodyInvalid { evidence, rule };
        if node.eligibility.direct_reasons.iter().any(|existing| {
            matches!(existing, EligibilityReason::ConsensusBodyInvalid { .. })
                && *existing != reason
        }) || matches!(node.body, BodyValidationState::ConsensusInvalid { .. })
            && node.body != body
        {
            return Err(GraphError::BodyEligibilityMismatch(hash));
        }
        let changed = node.body != body || !node.eligibility.direct_reasons.contains(&reason);
        node.body = body;
        node.eligibility.direct_reasons.insert(reason);
        if changed {
            self.recompute_descendant_eligibility(hash)?;
        }
        Ok(changed)
    }

    /// Update body availability or verification without changing fork choice eligibility.
    pub(crate) fn set_body_state(
        &mut self,
        hash: block::Hash,
        body: BodyValidationState,
    ) -> Result<bool, GraphError> {
        if matches!(body, BodyValidationState::ConsensusInvalid { .. }) {
            return Err(GraphError::BodyEligibilityMismatch(hash));
        }
        let node = self
            .nodes
            .get_mut(&hash)
            .ok_or(GraphError::UnknownNode(hash))?;
        if node
            .eligibility
            .direct_reasons
            .iter()
            .any(|reason| matches!(reason, EligibilityReason::ConsensusBodyInvalid { .. }))
        {
            return Err(GraphError::BodyEligibilityMismatch(hash));
        }
        let changed = node.body != body;
        node.body = body;
        Ok(changed)
    }

    /// Update local-time validation state and recompute descendant eligibility.
    pub(crate) fn set_validation(
        &mut self,
        hash: block::Hash,
        validation: HeaderValidationState,
    ) -> Result<bool, GraphError> {
        let node = self
            .nodes
            .get_mut(&hash)
            .ok_or(GraphError::UnknownNode(hash))?;
        let changed = node.validation != validation;
        node.validation = validation;
        if changed {
            self.recompute_descendant_eligibility(hash)?;
        }
        Ok(changed)
    }

    /// Return the exact ancestor at `height`, if the retained path reaches it.
    pub fn ancestor(
        &self,
        descendant: block::Hash,
        height: block::Height,
    ) -> Result<Option<Frontier>, GraphError> {
        let mut node = self
            .nodes
            .get(&descendant)
            .ok_or(GraphError::UnknownNode(descendant))?;
        if height > node.height {
            return Err(GraphError::InvalidAncestorHeight {
                ancestor: height,
                descendant: node.height,
            });
        }
        while node.height > height {
            let Some(parent) = self.nodes.get(&node.parent_hash) else {
                return Ok(None);
            };
            node = parent;
        }
        Ok(Some(Frontier::new(node.height, node.hash)))
    }

    /// Return all currently maximal eligible nodes in deterministic hash order.
    pub fn eligible_tips(&self) -> Vec<Frontier> {
        let mut tips: Vec<_> = self
            .eligible_tips
            .iter()
            .filter_map(|hash| self.nodes.get(hash))
            .map(|node| Frontier::new(node.height, node.hash))
            .collect();
        tips.sort_unstable_by_key(|tip| tip.hash.0);
        tips
    }

    /// Select the deterministic greatest-work eligible tip after the finalized anchor.
    pub fn select_header_best(&self) -> Result<(Frontier, ChainScore), GraphError> {
        let anchor = self
            .nodes
            .get(&self.finalized.hash)
            .ok_or(GraphError::UnknownNode(self.finalized.hash))?;
        let mut best = None;
        for hash in &self.eligible_tips {
            let node = self
                .nodes
                .get(hash)
                .expect("eligible tips are derived from retained nodes");
            let tip = Frontier::new(node.height, node.hash);
            let score = ChainScore::new(
                node.work_coordinate()
                    .suffix_after(anchor.work_coordinate())?,
                tip.hash,
            );
            if best.is_none_or(|(_, best_score)| score > best_score) {
                best = Some((tip, score));
            }
        }
        best.ok_or(GraphError::UnknownNode(self.finalized.hash))
    }

    /// Return the selection score of one retained descendant of the finalized anchor.
    pub fn score(&self, hash: block::Hash) -> Result<ChainScore, GraphError> {
        let anchor = self
            .nodes
            .get(&self.finalized.hash)
            .ok_or(GraphError::UnknownNode(self.finalized.hash))?;
        let node = self.nodes.get(&hash).ok_or(GraphError::UnknownNode(hash))?;
        Ok(ChainScore::new(
            node.work_coordinate()
                .suffix_after(anchor.work_coordinate())?,
            hash,
        ))
    }

    pub(crate) fn from_nodes(
        finalized: Frontier,
        nodes: impl IntoIterator<Item = HeaderNode>,
    ) -> Result<Self, GraphError> {
        let mut node_map = HashMap::new();
        let mut children: HashMap<_, HashSet<_>> = HashMap::new();
        let mut heights: HashMap<_, HashSet<_>> = HashMap::new();
        for node in nodes {
            heights.entry(node.height).or_default().insert(node.hash);
            children
                .entry(node.parent_hash)
                .or_default()
                .insert(node.hash);
            node_map.insert(node.hash, node);
        }
        if !node_map.contains_key(&finalized.hash) {
            return Err(GraphError::UnknownNode(finalized.hash));
        }
        children.remove(
            &node_map
                .get(&finalized.hash)
                .expect("the finalized node was checked above")
                .parent_hash,
        );
        let mut store = Self {
            finalized,
            nodes: node_map,
            children,
            heights,
            eligible_tips: HashSet::new(),
        };
        store.rebuild_eligible_tips();
        Ok(store)
    }

    /// Rebuild all in-memory indexes from nodes returned by an exhaustive store audit.
    pub fn from_audited_nodes(
        finalized: Frontier,
        nodes: impl IntoIterator<Item = HeaderNode>,
    ) -> Result<Self, GraphError> {
        Self::from_nodes(finalized, nodes)
    }

    pub(crate) fn nodes(&self) -> impl Iterator<Item = &HeaderNode> {
        self.nodes.values()
    }

    pub(crate) fn node_mut(&mut self, hash: block::Hash) -> Result<&mut HeaderNode, GraphError> {
        self.nodes
            .get_mut(&hash)
            .ok_or(GraphError::UnknownNode(hash))
    }

    pub(crate) fn recompute_all_eligibility(&mut self) -> Result<(), GraphError> {
        let mut frontiers: Vec<_> = self
            .nodes
            .values()
            .map(|node| Frontier::new(node.height, node.hash))
            .collect();
        frontiers.sort_unstable_by_key(|frontier| (frontier.height, frontier.hash.0));
        for frontier in frontiers {
            if frontier == self.finalized {
                self.node_mut(frontier.hash)?.eligibility.inherited_from = None;
                continue;
            }
            let parent_hash = self
                .node(frontier.hash)
                .expect("frontier came from nodes")
                .parent_hash;
            let parent = self.node(parent_hash).ok_or(GraphError::UnknownParent {
                header: frontier.hash,
                parent: parent_hash,
            })?;
            let inherited_from = (!parent.is_eligible()).then_some(parent_hash);
            self.node_mut(frontier.hash)?.eligibility.inherited_from = inherited_from;
        }
        self.rebuild_eligible_tips();
        Ok(())
    }

    pub(crate) fn advance_finalized(
        &mut self,
        finalized: Frontier,
    ) -> Result<Vec<block::Hash>, GraphError> {
        let node = self
            .node(finalized.hash)
            .ok_or(GraphError::UnknownNode(finalized.hash))?;
        if node.height != finalized.height {
            return Err(GraphError::UnknownNode(finalized.hash));
        }
        if !node.is_eligible() {
            return Err(GraphError::IneligibleFinalized(finalized.hash));
        }
        let mut retained = HashSet::new();
        let mut pending = vec![finalized.hash];
        while let Some(hash) = pending.pop() {
            if retained.insert(hash) {
                pending.extend(self.children(hash));
            }
        }
        let mut deleted: Vec<_> = self
            .nodes
            .keys()
            .copied()
            .filter(|hash| !retained.contains(hash))
            .collect();
        deleted.sort_unstable_by_key(|hash| hash.0);
        for hash in &deleted {
            let node = self
                .nodes
                .remove(hash)
                .expect("the deletion set came from retained graph nodes");
            self.children.remove(hash);
            self.eligible_tips.remove(hash);
            if let Some(hashes) = self.heights.get_mut(&node.height) {
                hashes.remove(hash);
                if hashes.is_empty() {
                    self.heights.remove(&node.height);
                }
            }
        }
        self.finalized = finalized;
        self.nodes
            .get_mut(&finalized.hash)
            .expect("the new finalized root is retained")
            .eligibility
            .inherited_from = None;
        self.refresh_eligible_tip(finalized.hash);
        Ok(deleted)
    }

    pub(crate) fn retained_hashes(&self) -> impl Iterator<Item = block::Hash> + '_ {
        self.nodes.keys().copied()
    }

    pub(crate) fn remove_leaf(&mut self, hash: block::Hash) -> Result<(), GraphError> {
        let node = self.nodes.get(&hash).ok_or(GraphError::UnknownNode(hash))?;
        if self
            .children
            .get(&hash)
            .is_some_and(|children| !children.is_empty())
        {
            return Err(GraphError::NodeHasChildren(hash));
        }
        let parent_hash = node.parent_hash;
        let height = node.height;
        self.eligible_tips.remove(&hash);
        self.nodes.remove(&hash);
        self.children.remove(&hash);
        if let Some(children) = self.children.get_mut(&parent_hash) {
            children.remove(&hash);
            if children.is_empty() {
                self.children.remove(&parent_hash);
            }
        }
        if let Some(hashes) = self.heights.get_mut(&height) {
            hashes.remove(&hash);
            if hashes.is_empty() {
                self.heights.remove(&height);
            }
        }
        self.refresh_eligible_tip(parent_hash);
        Ok(())
    }

    fn recompute_descendant_eligibility(&mut self, root: block::Hash) -> Result<(), GraphError> {
        let mut affected = HashSet::from([root]);
        let mut queue = VecDeque::from(self.children(root));
        while let Some(hash) = queue.pop_front() {
            affected.insert(hash);
            let parent_hash = self
                .nodes
                .get(&hash)
                .ok_or(GraphError::UnknownNode(hash))?
                .parent_hash;
            let parent = self
                .nodes
                .get(&parent_hash)
                .ok_or(GraphError::UnknownNode(parent_hash))?;
            let inherited_from = (!parent.is_eligible()).then_some(parent_hash);
            self.nodes
                .get_mut(&hash)
                .expect("the queued child was read from the retained node map")
                .eligibility
                .inherited_from = inherited_from;
            queue.extend(self.children(hash));
        }
        let parents: Vec<_> = affected
            .iter()
            .filter_map(|hash| self.nodes.get(hash).map(|node| node.parent_hash))
            .collect();
        affected.extend(parents);
        for hash in affected {
            self.refresh_eligible_tip(hash);
        }
        Ok(())
    }

    fn has_eligible_child(&self, hash: block::Hash) -> bool {
        self.children.get(&hash).is_some_and(|children| {
            children
                .iter()
                .any(|child| self.nodes.get(child).is_some_and(HeaderNode::is_eligible))
        })
    }

    fn refresh_eligible_tip(&mut self, hash: block::Hash) {
        self.eligible_tips.remove(&hash);
        if self
            .nodes
            .get(&hash)
            .is_some_and(|node| node.is_eligible() && !self.has_eligible_child(hash))
        {
            self.eligible_tips.insert(hash);
        }
    }

    fn rebuild_eligible_tips(&mut self) {
        self.eligible_tips = self
            .nodes
            .values()
            .filter(|node| node.is_eligible() && !self.has_eligible_child(node.hash))
            .map(|node| node.hash)
            .collect();
    }

    pub(crate) fn apply_delta(&mut self, delta: &GraphDelta) -> Result<(), GraphError> {
        for hash in &delta.delete_nodes {
            if !self.nodes.contains_key(hash) {
                return Err(GraphError::UnknownNode(*hash));
            }
        }
        for node in &delta.put_nodes {
            if delta.delete_nodes.contains(&node.hash) {
                return Err(GraphError::ConflictingDuplicate(node.hash));
            }
        }

        for hash in &delta.delete_nodes {
            let node = self
                .nodes
                .remove(hash)
                .expect("delta deletions were validated before mutation");
            self.children.remove(hash);
            if let Some(hashes) = self.heights.get_mut(&node.height) {
                hashes.remove(hash);
                if hashes.is_empty() {
                    self.heights.remove(&node.height);
                }
            }
        }
        for node in &delta.put_nodes {
            let old = self.nodes.insert(node.hash, node.clone());
            if old.is_none() {
                self.heights
                    .entry(node.height)
                    .or_default()
                    .insert(node.hash);
            }
        }
        for (parent, child) in &delta.remove_children {
            if let Some(children) = self.children.get_mut(parent) {
                children.remove(child);
                if children.is_empty() {
                    self.children.remove(parent);
                }
            }
        }
        for (parent, child) in &delta.add_children {
            self.children.entry(*parent).or_default().insert(*child);
        }
        for hash in &delta.remove_eligible_tips {
            self.eligible_tips.remove(hash);
        }
        self.eligible_tips
            .extend(delta.add_eligible_tips.iter().copied());
        if let Some(finalized) = delta.finalized {
            self.finalized = finalized;
        }
        Ok(())
    }
}

impl HeaderGraphView for MemHeaderStore {
    fn view_finalized(&self) -> Frontier {
        self.finalized()
    }

    fn view_node_count(&self) -> usize {
        self.node_count()
    }

    fn view_node(&self, hash: block::Hash) -> Option<&HeaderNode> {
        self.node(hash)
    }

    fn view_nodes(&self) -> Vec<&HeaderNode> {
        self.nodes().collect()
    }

    fn view_retained_hashes(&self) -> Vec<block::Hash> {
        self.retained_hashes().collect()
    }

    fn view_hashes_at_height(&self, height: block::Height) -> Vec<block::Hash> {
        self.hashes_at_height(height)
    }

    fn view_children(&self, parent: block::Hash) -> Vec<block::Hash> {
        self.children(parent)
    }

    fn view_eligible_tips(&self) -> Vec<Frontier> {
        self.eligible_tips()
    }

    fn view_select_header_best(&self) -> Result<(Frontier, ChainScore), GraphError> {
        self.select_header_best()
    }

    fn view_score(&self, hash: block::Hash) -> Result<ChainScore, GraphError> {
        self.score(hash)
    }

    fn view_ancestor(
        &self,
        descendant: block::Hash,
        height: block::Height,
    ) -> Result<Option<Frontier>, GraphError> {
        self.ancestor(descendant, height)
    }
}

impl HeaderGraphEdit for MemHeaderStore {
    fn edit_node_mut(&mut self, hash: block::Hash) -> Result<&mut HeaderNode, GraphError> {
        self.node_mut(hash)
    }

    fn edit_insert(
        &mut self,
        header: Arc<block::Header>,
        block_work: Work,
        validation: HeaderValidationState,
        direct_reasons: Vec<EligibilityReason>,
        body: BodyValidationState,
    ) -> Result<InsertResult, GraphError> {
        self.insert(header, block_work, validation, direct_reasons, body)
    }

    fn edit_add_reason(
        &mut self,
        hash: block::Hash,
        reason: EligibilityReason,
    ) -> Result<bool, GraphError> {
        self.add_reason(hash, reason)
    }

    fn edit_remove_operator_invalidation(
        &mut self,
        hash: block::Hash,
        id: OperatorInvalidationId,
    ) -> Result<bool, GraphError> {
        self.remove_operator_invalidation(hash, id)
    }

    fn edit_set_consensus_body_invalid(
        &mut self,
        hash: block::Hash,
        evidence: EvidenceId,
        rule: BodyRuleId,
    ) -> Result<bool, GraphError> {
        self.set_consensus_body_invalid(hash, evidence, rule)
    }

    fn edit_set_body_state(
        &mut self,
        hash: block::Hash,
        body: BodyValidationState,
    ) -> Result<bool, GraphError> {
        self.set_body_state(hash, body)
    }

    fn edit_set_validation(
        &mut self,
        hash: block::Hash,
        validation: HeaderValidationState,
    ) -> Result<bool, GraphError> {
        self.set_validation(hash, validation)
    }

    fn edit_advance_finalized(
        &mut self,
        finalized: Frontier,
    ) -> Result<Vec<block::Hash>, GraphError> {
        self.advance_finalized(finalized)
    }

    fn edit_remove_leaf(&mut self, hash: block::Hash) -> Result<(), GraphError> {
        self.remove_leaf(hash)
    }
}

impl HeaderGraphView for GraphOverlay<'_> {
    fn view_finalized(&self) -> Frontier {
        self.finalized()
    }

    fn view_node_count(&self) -> usize {
        self.node_count()
    }

    fn view_node(&self, hash: block::Hash) -> Option<&HeaderNode> {
        self.node(hash)
    }

    fn view_nodes(&self) -> Vec<&HeaderNode> {
        self.nodes().collect()
    }

    fn view_retained_hashes(&self) -> Vec<block::Hash> {
        self.retained_hashes().collect()
    }

    fn view_hashes_at_height(&self, height: block::Height) -> Vec<block::Hash> {
        self.hashes_at_height(height)
    }

    fn view_children(&self, parent: block::Hash) -> Vec<block::Hash> {
        self.children(parent)
    }

    fn view_eligible_tips(&self) -> Vec<Frontier> {
        self.eligible_tips()
    }

    fn view_select_header_best(&self) -> Result<(Frontier, ChainScore), GraphError> {
        self.select_header_best()
    }

    fn view_score(&self, hash: block::Hash) -> Result<ChainScore, GraphError> {
        self.score(hash)
    }

    fn view_ancestor(
        &self,
        descendant: block::Hash,
        height: block::Height,
    ) -> Result<Option<Frontier>, GraphError> {
        self.ancestor(descendant, height)
    }
}

impl HeaderGraphEdit for GraphOverlay<'_> {
    fn edit_node_mut(&mut self, hash: block::Hash) -> Result<&mut HeaderNode, GraphError> {
        self.node_mut(hash)
    }

    fn edit_insert(
        &mut self,
        header: Arc<block::Header>,
        block_work: Work,
        validation: HeaderValidationState,
        direct_reasons: Vec<EligibilityReason>,
        body: BodyValidationState,
    ) -> Result<InsertResult, GraphError> {
        self.insert(header, block_work, validation, direct_reasons, body)
    }

    fn edit_add_reason(
        &mut self,
        hash: block::Hash,
        reason: EligibilityReason,
    ) -> Result<bool, GraphError> {
        self.add_reason(hash, reason)
    }

    fn edit_remove_operator_invalidation(
        &mut self,
        hash: block::Hash,
        id: OperatorInvalidationId,
    ) -> Result<bool, GraphError> {
        self.remove_operator_invalidation(hash, id)
    }

    fn edit_set_consensus_body_invalid(
        &mut self,
        hash: block::Hash,
        evidence: EvidenceId,
        rule: BodyRuleId,
    ) -> Result<bool, GraphError> {
        self.set_consensus_body_invalid(hash, evidence, rule)
    }

    fn edit_set_body_state(
        &mut self,
        hash: block::Hash,
        body: BodyValidationState,
    ) -> Result<bool, GraphError> {
        self.set_body_state(hash, body)
    }

    fn edit_set_validation(
        &mut self,
        hash: block::Hash,
        validation: HeaderValidationState,
    ) -> Result<bool, GraphError> {
        self.set_validation(hash, validation)
    }

    fn edit_advance_finalized(
        &mut self,
        finalized: Frontier,
    ) -> Result<Vec<block::Hash>, GraphError> {
        self.advance_finalized(finalized)
    }

    fn edit_remove_leaf(&mut self, hash: block::Hash) -> Result<(), GraphError> {
        self.remove_leaf(hash)
    }
}
