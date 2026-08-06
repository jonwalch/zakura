//! Authenticates the claimed coinbase height of a downloaded block against our chain tip.
//!
//! Shared by the syncer ([`crate::components::sync::downloads`]) and the gossip path
//! ([`crate::components::inbound::downloads`]), which both read a block's coinbase height
//! before verification and must not disagree about when that height can be trusted.
//!
//! # Security
//!
//! A V5+ coinbase `scriptSig` carries the block height as *authorizing* data. Authorizing data
//! is excluded from the mined transaction ID, so it is also excluded from the transaction
//! merkle root and from the block header's merkle root commitment — and therefore from the
//! block hash.
//!
//! A peer can take a canonical block, rewrite only its coinbase height, and return a body that
//! still matches the hash we requested or that it advertised. Full consensus validation rejects
//! that body via the header's authorizing-data commitment, but both block download paths read
//! the coinbase height *before* verification, to decide whether a block is worth verifying at
//! all. A rewritten height can steer those decisions: a height far behind the tip is discarded
//! as a benign old block, which satisfies the request without delivering a usable block.
//!
//! There is exactly one case where the real height is knowable without consulting the state: a
//! block whose parent is our own best tip must be at `tip + 1`. That is also the case that
//! matters most, because it is the block a miner is waiting for. This module is the single
//! implementation of that check, shared by the syncer and the gossip path so the two cannot
//! drift apart.

use zakura_chain::block::{self, Height};

/// Returns the height `block_height` should have been, if the block is a child of `best_tip`
/// and its claimed coinbase height contradicts that.
///
/// Returns `None` when the block is not a tip child — including when we have no tip — or when
/// its claimed height agrees with the tip. Those blocks are unaffected: their height cannot be
/// authenticated from the tip alone, so the caller's existing height policies still apply.
///
/// A `Some` result is proof of a poisoned body, not a heuristic. The caller should attribute it
/// to the supplying peer rather than dropping it as an ordinary out-of-range block.
pub(crate) fn tip_child_mismatch(
    previous_block_hash: block::Hash,
    block_height: Height,
    best_tip: Option<(Height, block::Hash)>,
) -> Option<Height> {
    let (tip_height, tip_hash) = best_tip?;

    if previous_block_hash != tip_hash {
        return None;
    }

    // The tip comes from our own committed state, so it is at most `Height::MAX`, which is
    // `u32::MAX / 2` — this addition cannot overflow. `saturating_add` is used rather than
    // `Height + 1` because the latter is fallible, and answering `None` on overflow would
    // silently skip the check instead of reporting a mismatch. This fails closed: a saturated
    // value could never equal a valid `block_height`, so it still reports a mismatch.
    let expected_height = Height(tip_height.0.saturating_add(1));

    (block_height != expected_height).then_some(expected_height)
}

/// Builds a copy of `canonical` with only its coinbase height rewritten, as a peer performing
/// this attack would.
///
/// For a V5+ coinbase the height lives in the transparent input's `scriptSig`, which is
/// authorizing data. It is therefore excluded from the mined transaction ID, the transaction
/// merkle root, and the block hash — so this mutation produces a body that still answers a
/// request for, or an advertisement of, the canonical hash.
#[cfg(test)]
pub(crate) fn poison_coinbase_height(
    canonical: &zakura_chain::block::Block,
    height: Height,
) -> std::sync::Arc<zakura_chain::block::Block> {
    use std::sync::Arc;
    use zakura_chain::transparent;

    let mut poisoned = canonical.clone();

    let coinbase = Arc::make_mut(
        poisoned
            .transactions
            .first_mut()
            .expect("test block has a coinbase transaction"),
    );

    match coinbase
        .inputs_mut()
        .first_mut()
        .expect("coinbase transaction has an input")
    {
        transparent::Input::Coinbase {
            height: coinbase_height,
            ..
        } => *coinbase_height = height,
        transparent::Input::PrevOut { .. } => panic!("the first input is a coinbase input"),
    }

    Arc::new(poisoned)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TIP_HASH: block::Hash = block::Hash([0xAA; 32]);
    const OTHER_HASH: block::Hash = block::Hash([0xBB; 32]);

    #[test]
    fn tip_child_with_the_expected_height_is_accepted() {
        assert_eq!(
            tip_child_mismatch(TIP_HASH, Height(101), Some((Height(100), TIP_HASH))),
            None,
        );
    }

    #[test]
    fn tip_child_with_a_rewritten_low_height_is_a_mismatch() {
        assert_eq!(
            tip_child_mismatch(TIP_HASH, Height(1), Some((Height(100), TIP_HASH))),
            Some(Height(101)),
            "a height rewritten far behind the tip must be reported, \
             not left to the behind-tip policy"
        );
    }

    #[test]
    fn tip_child_with_a_rewritten_high_height_is_a_mismatch() {
        assert_eq!(
            tip_child_mismatch(TIP_HASH, Height(500_000), Some((Height(100), TIP_HASH))),
            Some(Height(101)),
        );
    }

    #[test]
    fn a_block_that_is_not_a_tip_child_is_not_checked() {
        assert_eq!(
            tip_child_mismatch(OTHER_HASH, Height(1), Some((Height(100), TIP_HASH))),
            None,
            "without a known parent the height can't be authenticated from the tip alone",
        );
    }

    #[test]
    fn no_tip_means_no_check() {
        assert_eq!(tip_child_mismatch(TIP_HASH, Height(1), None), None);
    }
}
