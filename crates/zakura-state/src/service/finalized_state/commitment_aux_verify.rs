//! Read-only verification of supplied per-block note-commitment roots against the
//! checkpoint-committed block headers, via the ZIP-221 ChainHistory MMR.
//!
//! This is the "verify" component of the verified-commitment-trees design
//! (`docs/design/verified-commitment-trees.md`). Given a sequence of per-block
//! Sapling/Orchard/Ironwood roots (from a fixture today, an untrusted peer later), confirm
//! they reconstruct a history tree consistent with the header commitments.

use std::sync::Arc;

use zakura_chain::{
    block::{merkle::AuthDataRoot, Block, Header, Height},
    history_tree::HistoryTree,
    ironwood, orchard,
    parallel::commitment_aux_verify::{
        header_commitment_is_valid_for_chain_history, verify_supplied_ironwood_root_below_nu6_3,
        verify_supplied_orchard_root_below_nu5,
        verify_supplied_sapling_root_below_heartwood_from_header, SuppliedRootsError,
    },
    parameters::Network,
    sapling,
};

use crate::{service::check, ValidateContextError};

/// One block-sized step in supplied commitment-root verification.
#[derive(Clone, Debug)]
pub(crate) struct CommitmentRootVerification {
    pub(crate) block: Option<Arc<Block>>,
    pub(crate) header: Arc<Header>,
    pub(crate) height: Height,
    pub(crate) roots: Option<(
        sapling::tree::Root,
        orchard::tree::Root,
        ironwood::tree::Root,
    )>,
    pub(crate) precomputed_auth_data_root: Option<AuthDataRoot>,
    pub(crate) skip_parent_check: bool,
}

impl CommitmentRootVerification {
    /// Verify this block's parent-history commitment, then fold the supplied
    /// per-block roots into the running history tree for the next block.
    pub(crate) fn with_roots(
        block: Arc<Block>,
        sapling_root: sapling::tree::Root,
        orchard_root: orchard::tree::Root,
        ironwood_root: ironwood::tree::Root,
        precomputed_auth_data_root: Option<AuthDataRoot>,
        skip_parent_check: bool,
    ) -> Self {
        let height = block
            .coinbase_height()
            .expect("checkpoint-verified blocks have a coinbase height");
        CommitmentRootVerification {
            header: block.header.clone(),
            height,
            block: Some(block),
            roots: Some((sapling_root, orchard_root, ironwood_root)),
            precomputed_auth_data_root,
            skip_parent_check,
        }
    }

    /// Verify this block's parent-history commitment without folding in roots.
    ///
    /// This confirms the roots already accumulated in the running tree, which is useful
    /// for the final one-block lag: the roots at height `H` are checked by height `H + 1`.
    pub(crate) fn header_only(
        header: Arc<Header>,
        height: Height,
        precomputed_auth_data_root: Option<AuthDataRoot>,
    ) -> Self {
        CommitmentRootVerification {
            block: None,
            header,
            height,
            roots: None,
            precomputed_auth_data_root,
            skip_parent_check: false,
        }
    }
}

/// Identifies which exact VCT verification input failed.
#[derive(Debug)]
pub(crate) enum CommitmentRootVerificationError {
    /// The current block body does not match its parent-history commitment.
    CurrentBlock {
        #[cfg_attr(not(test), allow(dead_code))]
        height: Height,
        error: ValidateContextError,
    },
    /// The current block's supplied roots failed a direct check or history-tree fold.
    CurrentRoots {
        #[cfg_attr(not(test), allow(dead_code))]
        height: Height,
        error: ValidateContextError,
    },
    /// The successor header rejected the candidate tree containing the current roots.
    SuccessorBoundary {
        #[cfg_attr(not(test), allow(dead_code))]
        height: Height,
        error: ValidateContextError,
    },
}

impl CommitmentRootVerificationError {
    #[cfg(test)]
    fn failure_kind(&self) -> crate::error::VctCommitFailure {
        match self {
            Self::CurrentRoots { .. } => crate::error::VctCommitFailure::CurrentRoots,
            Self::SuccessorBoundary { .. } => crate::error::VctCommitFailure::SuccessorBoundary,
            Self::CurrentBlock { .. } => {
                panic!("current block failures are not auxiliary metadata failures")
            }
        }
    }

    #[cfg(test)]
    fn height(&self) -> Height {
        match self {
            Self::CurrentBlock { height, .. }
            | Self::CurrentRoots { height, .. }
            | Self::SuccessorBoundary { height, .. } => *height,
        }
    }

    #[cfg(test)]
    fn error(&self) -> &ValidateContextError {
        match self {
            Self::CurrentBlock { error, .. }
            | Self::CurrentRoots { error, .. }
            | Self::SuccessorBoundary { error, .. } => error,
        }
    }
}

/// Converts supplied-root verification failures into state validation failures.
fn supplied_roots_error_to_validate_context(error: SuppliedRootsError) -> ValidateContextError {
    match error {
        SuppliedRootsError::InvalidHeaderCommitment(error) => {
            ValidateContextError::InvalidBlockCommitment(error)
        }
        SuppliedRootsError::MissingHistoryTreeRoot => {
            ValidateContextError::HistoryTreeError(Arc::new(
                zakura_chain::history_tree::HistoryTreeError::InvalidCachedTree {
                    reason: "a header commitment requires a non-empty parent history tree",
                },
            ))
        }
        SuppliedRootsError::HistoryTree(error) => ValidateContextError::HistoryTreeError(error),
    }
}

/// Verifies that `items` (blocks in ascending height order, with supplied
/// Sapling/Orchard/Ironwood roots when they should be folded in) reconstruct a ZIP-221
/// history MMR consistent with the block header commitments, starting from `tree`
/// (the parent block's history tree).
///
/// Returns the final history tree on success, or `(height, error)` for the first
/// block whose header commitment rejects the roots folded in so far.
///
/// # Lag
///
/// A block's commitment commits to the history tree as of its *parent*, so the root
/// supplied for height `H` is only confirmed when height `H + 1` is processed. Over a
/// contiguous range `[start..=end]` this therefore confirms the roots at
/// `[start..=end - 1]`; pass the block at `end + 1` to confirm the root at `end`.
pub(crate) fn verify_commitment_roots<I>(
    network: &Network,
    mut history_tree: HistoryTree,
    blocks_to_verify: I,
) -> Result<HistoryTree, CommitmentRootVerificationError>
where
    I: IntoIterator<Item = CommitmentRootVerification>,
{
    for block_verify in blocks_to_verify {
        let CommitmentRootVerification {
            block,
            header,
            height,
            roots,
            precomputed_auth_data_root,
            skip_parent_check,
        } = block_verify;

        // Validate this block's header commitment against the current (parent) tree,
        // i.e. against every root already folded in.
        // We allow the caller to control skipping this check
        // in case the caller has already verified the parent tree
        // For example, a block execution loop is:
        // 1. Verify block X against block X - 1 history tree
        // 2. Wait for block X + 1 body to verify against block X history tree
        //    * This is so that we do not commit block X before we have verified its roots.
        // 3. Verify block X + 1 against block X history tree
        //
        // Note that, when we are processing block X + 1 step 1, we are ovrlapping
        // with step 3 of the prior iteration so verification can be skipped in that case
        // for perf reasons.
        if !skip_parent_check {
            if let Some(block) = &block {
                // This block + history tree up to and including the previous block.
                check::block_commitment_is_valid_for_chain_history(
                    block.clone(),
                    network,
                    &history_tree,
                    precomputed_auth_data_root,
                )
                .map_err(|error| CommitmentRootVerificationError::CurrentBlock { height, error })?;
            } else {
                let auth_data_root = precomputed_auth_data_root
                    .expect("header-only VCT witnesses have a stored precomputed auth-data root");
                header_commitment_is_valid_for_chain_history(
                    &header,
                    height,
                    network,
                    &history_tree,
                    auth_data_root,
                )
                .map_err(supplied_roots_error_to_validate_context)
                .map_err(|error| {
                    CommitmentRootVerificationError::SuccessorBoundary { height, error }
                })?;
            }
        }

        let Some((sapling_root, orchard_root, ironwood_root)) = roots else {
            continue;
        };

        let block = block.expect("verification items with supplied roots have a block body");
        verify_supplied_sapling_root_below_heartwood_from_header(
            network,
            &block.header,
            height,
            &sapling_root,
        )
        .map_err(supplied_roots_error_to_validate_context)
        .map_err(|error| CommitmentRootVerificationError::CurrentRoots { height, error })?;
        verify_supplied_orchard_root_below_nu5(network, height, &orchard_root)
            .map_err(supplied_roots_error_to_validate_context)
            .map_err(|error| CommitmentRootVerificationError::CurrentRoots { height, error })?;
        verify_supplied_ironwood_root_below_nu6_3(network, height, &ironwood_root)
            .map_err(supplied_roots_error_to_validate_context)
            .map_err(|error| CommitmentRootVerificationError::CurrentRoots { height, error })?;

        // Fold this block's supplied roots into the running MMR (builds the leaf
        // from the block body tx-counts + the roots).
        history_tree
            .push(network, block, &sapling_root, &orchard_root, &ironwood_root)
            .map_err(Arc::new)
            .map_err(ValidateContextError::from)
            .map_err(|error| CommitmentRootVerificationError::CurrentRoots { height, error })?;
    }

    Ok(history_tree)
}

#[cfg(test)]
mod tests {
    use super::*;

    use zakura_chain::{
        block::Block,
        parameters::{Network::Mainnet, NetworkUpgrade},
        serialization::ZcashDeserializeInto,
    };

    /// Build an empty [`HistoryTree`] (the genesis block is pre-Heartwood).
    fn empty_history_tree() -> HistoryTree {
        let genesis = Arc::new(
            zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
                .zcash_deserialize_into::<Block>()
                .expect("genesis deserializes"),
        );
        HistoryTree::from_block(
            &Mainnet,
            genesis,
            &Default::default(),
            &Default::default(),
            &Default::default(),
        )
        .expect("empty history tree for a pre-Heartwood block")
    }

    fn mainnet_block_at(height: u32) -> Arc<Block> {
        let (blocks, _) = Mainnet.block_sapling_roots_map();
        Arc::new(
            blocks
                .get(&height)
                .expect("test vector block exists")
                .zcash_deserialize_into::<Block>()
                .expect("block deserializes"),
        )
    }

    fn mainnet_sapling_root_at(height: u32) -> sapling::tree::Root {
        let (_, sapling_roots) = Mainnet.block_sapling_roots_map();
        sapling::tree::Root::try_from(**sapling_roots.get(&height).expect("root vector exists"))
            .expect("valid root")
    }

    fn empty_ironwood_root() -> ironwood::tree::Root {
        ironwood::tree::NoteCommitmentTree::default().root()
    }

    fn verification_item(
        block: Arc<Block>,
        sapling_root: sapling::tree::Root,
        orchard_root: orchard::tree::Root,
    ) -> CommitmentRootVerification {
        CommitmentRootVerification::with_roots(
            block,
            sapling_root,
            orchard_root,
            empty_ironwood_root(),
            None,
            false,
        )
    }

    #[test]
    fn commitment_root_verification_constructors_set_expected_fields() {
        let block = mainnet_block_at(1);
        let sapling_root = sapling::tree::NoteCommitmentTree::default().root();
        let orchard_root = orchard::tree::NoteCommitmentTree::default().root();
        let ironwood_root = empty_ironwood_root();

        let with_roots = CommitmentRootVerification::with_roots(
            block.clone(),
            sapling_root,
            orchard_root,
            ironwood_root,
            None,
            true,
        );
        assert!(Arc::ptr_eq(
            with_roots.block.as_ref().expect("roots item has a block"),
            &block
        ));
        assert!(Arc::ptr_eq(&with_roots.header, &block.header));
        assert_eq!(with_roots.height, block.coinbase_height().unwrap());
        assert_eq!(
            with_roots.roots,
            Some((sapling_root, orchard_root, ironwood_root))
        );
        assert_eq!(with_roots.precomputed_auth_data_root, None);
        assert!(with_roots.skip_parent_check);

        let height = block.coinbase_height().unwrap();
        let header_only = CommitmentRootVerification::header_only(
            block.header.clone(),
            height,
            Some(block.auth_data_root()),
        );
        assert!(header_only.block.is_none());
        assert!(Arc::ptr_eq(&header_only.header, &block.header));
        assert_eq!(header_only.height, height);
        assert_eq!(header_only.roots, None);
        assert_eq!(
            header_only.precomputed_auth_data_root,
            Some(block.auth_data_root())
        );
        assert!(!header_only.skip_parent_check);
    }

    /// The verifier confirms real Sapling roots over the Heartwood activation and its
    /// next block (the V1 `ChainHistoryRoot` path), and rejects a wrong root at the
    /// *next* block (the one-block lag).
    #[test]
    fn verifies_real_roots_with_header_only_successor_and_rejects_a_wrong_root() {
        let activation = NetworkUpgrade::Heartwood
            .activation_height(&Mainnet)
            .expect("mainnet has Heartwood")
            .0;

        let act_block = mainnet_block_at(activation);
        let next_block = mainnet_block_at(activation + 1);
        let act_root = mainnet_sapling_root_at(activation);
        let next_root = mainnet_sapling_root_at(activation + 1);
        let empty_orchard_root = orchard::tree::NoteCommitmentTree::default().root();

        // Positive: the real roots reconstruct a tree the next block's header commits to.
        let ok_items = vec![
            verification_item(act_block.clone(), act_root, empty_orchard_root),
            CommitmentRootVerification::header_only(
                next_block.header.clone(),
                Height(activation + 1),
                Some(next_block.auth_data_root()),
            ),
        ];
        verify_commitment_roots(&Mainnet, empty_history_tree(), ok_items)
            .expect("real roots verify against the headers");

        // Negative + lag: a wrong root at the activation height (here, the next
        // block's root, which is a valid but different root) is only caught when the
        // following block's commitment is checked.
        assert_ne!(act_root, next_root, "test needs two distinct roots");
        let bad_items = vec![
            verification_item(act_block, next_root, empty_orchard_root),
            CommitmentRootVerification::header_only(
                next_block.header.clone(),
                Height(activation + 1),
                Some(next_block.auth_data_root()),
            ),
        ];
        let failure = verify_commitment_roots(&Mainnet, empty_history_tree(), bad_items)
            .expect_err("a wrong root must be rejected");
        assert_eq!(
            failure.height().0,
            activation + 1,
            "a wrong root at H is detected at H+1 (the lag)"
        );
        assert_eq!(
            failure.failure_kind(),
            crate::error::VctCommitFailure::SuccessorBoundary,
            "the verifier preserves that the successor boundary detected the mismatch"
        );
    }

    /// Real NU5/V2-range verification over the POC range (1,707,211..=1,717,210),
    /// exercising the actual [`verify_commitment_roots`] on production data.
    ///
    /// Gated by env vars so it stays out of normal CI. Requires two read-only forks
    /// of the RUNBOOK 1.707M master snapshot:
    /// - `VCT_SEED_DB`: an *unsynced* `cp -al` fork (its tip history tree at height
    ///   1,707,210 is the seed — mid-NU5-epoch, so no activation boundary to handle).
    /// - `VCT_ARCHIVE_DB`: an archive fork synced to >= 1,717,211 (provides the blocks
    ///   and per-height roots).
    ///
    /// Run:
    /// ```text
    /// VCT_SEED_DB=<unsynced-fork> VCT_ARCHIVE_DB=<synced-fork> \
    ///   cargo test -p zakura-state --lib commitment_aux_verify -- --ignored --nocapture
    /// ```
    #[ignore]
    #[test]
    #[allow(clippy::print_stderr)] // intentional progress output for a manual run
    fn verifies_real_nu5_range_over_synced_forks() {
        use std::path::PathBuf;

        use crate::{
            constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
            service::finalized_state::{ZakuraDb, STATE_COLUMN_FAMILIES_IN_CODE},
            Config,
        };

        let (Some(seed_dir), Some(archive_dir)) = (
            std::env::var_os("VCT_SEED_DB"),
            std::env::var_os("VCT_ARCHIVE_DB"),
        ) else {
            eprintln!("skipping: set VCT_SEED_DB (unsynced fork) and VCT_ARCHIVE_DB (synced fork)");
            return;
        };

        let open = |dir: PathBuf| -> ZakuraDb {
            let config = Config {
                cache_dir: dir,
                ephemeral: false,
                ..Default::default()
            };
            ZakuraDb::new(
                &config,
                STATE_DATABASE_KIND,
                &state_database_format_version_in_code(),
                &Mainnet,
                true, // skip format upgrades
                STATE_COLUMN_FAMILIES_IN_CODE
                    .iter()
                    .map(ToString::to_string),
                true, // read-only
            )
            .expect("opening the finalized state database should succeed")
        };

        let seed_db = open(PathBuf::from(seed_dir));
        let archive_db = open(PathBuf::from(archive_dir));

        let start = 1_707_211u32;
        let end = 1_717_210u32;

        // Seed: the history tree at 1,707,210 (the unsynced fork's tip).
        let seed = (*seed_db.history_tree()).clone();
        assert_eq!(
            seed_db.finalized_tip_height().map(|h| h.0),
            Some(start - 1),
            "VCT_SEED_DB must be the unsynced 1,707,210 master fork"
        );
        assert!(
            archive_db.finalized_tip_height().map(|h| h.0).unwrap_or(0) > end,
            "VCT_ARCHIVE_DB must be synced to at least {}",
            end + 1
        );

        // Build (block, sapling_root, orchard_root) for [start..=end+1]; the +1 block
        // confirms the in-range root at `end` via the one-block lag.
        let item_at = |h: u32| -> CommitmentRootVerification {
            let block = archive_db
                .block(Height(h).into())
                .expect("archive fork has the block");
            let sapling_root = archive_db
                .sapling_tree_by_height(&Height(h))
                .expect("archive fork has the per-height Sapling tree")
                .root();
            let orchard_root = archive_db
                .orchard_tree_by_height(&Height(h))
                .expect("archive fork has the per-height Orchard tree")
                .root();
            verification_item(block, sapling_root, orchard_root)
        };
        let items: Vec<_> = (start..=end + 1).map(item_at).collect();

        // Positive: every supplied root in the range is confirmed by the V2 headers.
        verify_commitment_roots(&Mainnet, seed.clone(), items.clone())
            .expect("real NU5 roots verify against the headers");
        eprintln!("VCT NU5 positive: {} blocks verified", items.len());

        // Negative + lag: corrupt one root mid-range with a distinct valid root (the
        // range's first root, certainly different after thousands of sandblast blocks);
        // expect rejection at H+1.
        let bad_offset = 5_000usize;
        let bad_height = start + bad_offset as u32;
        let wrong_root = items[0].roots.expect("test verification item has roots").0;
        let mut bad_items = items;
        assert_ne!(
            bad_items[bad_offset]
                .roots
                .expect("test verification item has roots")
                .0,
            wrong_root,
            "need a distinct wrong root"
        );
        bad_items[bad_offset]
            .roots
            .as_mut()
            .expect("test verification item has roots")
            .0 = wrong_root;
        let failure = verify_commitment_roots(&Mainnet, seed, bad_items)
            .expect_err("a wrong NU5 root must be rejected");
        assert_eq!(
            failure.height().0,
            bad_height + 1,
            "a wrong root at H is detected at H+1 (the lag)"
        );
        eprintln!(
            "VCT NU5 negative: wrong root at {bad_height} rejected at {}",
            failure.height().0
        );
    }

    /// Validates the exact tree-aux records an archive database would serve for arbitrary ranges.
    ///
    /// This check needs only one read-only database and is intended for diagnosing a serving node.
    /// It verifies contiguous encoded heights and compares every served root, transaction count,
    /// and auth-data root with the database's block and per-height tree data.
    ///
    /// Run:
    /// ```text
    /// VCT_ARCHIVE_DB=<archive-db> \
    /// VCT_RANGES=187401-188400,200401-201400 \
    ///   cargo test -p zakura-state --lib validates_served_vct_ranges_from_read_only_db \
    ///   -- --ignored --nocapture
    /// ```
    #[ignore]
    #[test]
    #[allow(clippy::print_stderr)] // intentional progress output for a manual diagnostic
    fn validates_served_vct_ranges_from_read_only_db() {
        use std::path::PathBuf;

        use crate::{
            constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
            service::finalized_state::{
                commitment_aux::serve_block_roots, ZakuraDb, STATE_COLUMN_FAMILIES_IN_CODE,
            },
            Config,
        };

        let (Some(archive_dir), Ok(ranges)) = (
            std::env::var_os("VCT_ARCHIVE_DB"),
            std::env::var("VCT_RANGES"),
        ) else {
            eprintln!("skipping: set VCT_ARCHIVE_DB (archive state) and VCT_RANGES (START-END)");
            return;
        };

        let config = Config {
            cache_dir: PathBuf::from(archive_dir),
            ephemeral: false,
            ..Default::default()
        };
        let archive_db = ZakuraDb::new(
            &config,
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &Mainnet,
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            true,
        )
        .expect("opening the archive database read-only should succeed");

        let ranges: Vec<_> = ranges
            .split(',')
            .map(str::trim)
            .filter(|range| !range.is_empty())
            .map(|range| {
                let (start, end) = range
                    .split_once('-')
                    .unwrap_or_else(|| panic!("range {range:?} must use START-END syntax"));
                let start: u32 = start
                    .parse()
                    .unwrap_or_else(|_| panic!("invalid start height in range {range:?}"));
                let end: u32 = end
                    .parse()
                    .unwrap_or_else(|_| panic!("invalid end height in range {range:?}"));
                assert!(start <= end, "range start must not exceed end: {range:?}");
                (start, end)
            })
            .collect();
        assert!(!ranges.is_empty(), "VCT_RANGES did not contain any ranges");

        let root_at = |height: Height| {
            serve_block_roots(&archive_db, height..=height)
                .into_iter()
                .next()
                .unwrap_or_else(|| panic!("served root is missing at {height:?}"))
        };
        let verification_item_at = |height: Height| {
            let roots = root_at(height);
            assert_eq!(roots.height, height, "served root height at {height:?}");
            let block = archive_db
                .block(height.into())
                .unwrap_or_else(|| panic!("archive body is missing at {height:?}"));
            assert_eq!(
                roots.sapling_tx,
                block.sapling_transactions_count(),
                "Sapling transaction count at {height:?}"
            );
            assert_eq!(
                roots.orchard_tx,
                block.orchard_transactions_count(),
                "Orchard transaction count at {height:?}"
            );
            assert_eq!(
                roots.ironwood_tx,
                block.ironwood_transactions_count(),
                "Ironwood transaction count at {height:?}"
            );
            assert_eq!(
                roots.auth_data_root,
                block.auth_data_root(),
                "auth-data root at {height:?}"
            );
            CommitmentRootVerification::with_roots(
                block,
                roots.sapling_root,
                roots.orchard_root,
                roots.ironwood_root,
                Some(roots.auth_data_root),
                false,
            )
        };

        let mut checked = 0usize;
        for &(start, end) in &ranges {
            let range = format!("{start}-{end}");
            let roots = serve_block_roots(&archive_db, Height(start)..=Height(end));
            let expected_len = usize::try_from(end - start + 1)
                .expect("a u32 range length fits in usize on supported targets");
            assert_eq!(
                roots.len(),
                expected_len,
                "served roots must fully cover range {range:?}"
            );

            for (offset, roots) in roots.into_iter().enumerate() {
                let offset =
                    u32::try_from(offset).expect("the bounded diagnostic range offset fits in u32");
                let height = Height(
                    start
                        .checked_add(offset)
                        .expect("the validated range end fits in u32"),
                );
                assert_eq!(
                    roots.height, height,
                    "served root height is misaligned in range {range:?}"
                );

                let block = archive_db
                    .block(height.into())
                    .unwrap_or_else(|| panic!("archive body is missing at {height:?}"));
                let sapling = archive_db
                    .sapling_tree_by_height(&height)
                    .unwrap_or_else(|| panic!("Sapling tree is missing at {height:?}"));
                let orchard = archive_db
                    .orchard_tree_by_height(&height)
                    .unwrap_or_else(|| panic!("Orchard tree is missing at {height:?}"));
                let ironwood = archive_db
                    .ironwood_tree_by_height(&height)
                    .map(|tree| tree.root())
                    .unwrap_or_else(|| ironwood::tree::NoteCommitmentTree::default().root());

                assert_eq!(
                    roots.sapling_root,
                    sapling.root(),
                    "Sapling root at {height:?}"
                );
                assert_eq!(
                    roots.orchard_root,
                    orchard.root(),
                    "Orchard root at {height:?}"
                );
                assert_eq!(roots.ironwood_root, ironwood, "Ironwood root at {height:?}");
                assert_eq!(
                    roots.sapling_tx,
                    block.sapling_transactions_count(),
                    "Sapling transaction count at {height:?}"
                );
                assert_eq!(
                    roots.orchard_tx,
                    block.orchard_transactions_count(),
                    "Orchard transaction count at {height:?}"
                );
                assert_eq!(
                    roots.ironwood_tx,
                    block.ironwood_transactions_count(),
                    "Ironwood transaction count at {height:?}"
                );
                assert_eq!(
                    roots.auth_data_root,
                    block.auth_data_root(),
                    "auth-data root at {height:?}"
                );
                checked += 1;
            }

            eprintln!("validated served tree-aux records for {start}..={end}");
        }

        eprintln!("validated {checked} served tree-aux records");

        let heartwood = NetworkUpgrade::Heartwood
            .activation_height(&Mainnet)
            .expect("Heartwood has a mainnet activation height")
            .0;
        for &(start, end) in ranges.iter().filter(|(start, _)| *start < heartwood) {
            let items = (start..=end).map(|height| verification_item_at(Height(height)));
            verify_commitment_roots(&Mainnet, empty_history_tree(), items).unwrap_or_else(
                |failure| {
                    panic!(
                        "pre-Heartwood roots failed at {:?}: {}",
                        failure.height(),
                        failure.error()
                    )
                },
            );
            eprintln!("validated direct pre-Heartwood commitments for {start}..={end}");
        }

        let mut epoch_ends = std::collections::BTreeMap::new();
        for &(start, end) in ranges.iter().filter(|(start, _)| *start >= heartwood) {
            let upgrade = NetworkUpgrade::current(&Mainnet, Height(start));
            assert_eq!(
                NetworkUpgrade::current(&Mainnet, Height(end)),
                upgrade,
                "diagnostic range {start}..={end} must not cross a network upgrade"
            );
            let activation = upgrade
                .activation_height(&Mainnet)
                .expect("an active mainnet upgrade has an activation height")
                .0;
            epoch_ends
                .entry((activation, upgrade))
                .and_modify(|max_end: &mut u32| *max_end = (*max_end).max(end))
                .or_insert(end);
        }

        for ((activation, upgrade), end) in epoch_ends {
            let activation_item = verification_item_at(Height(activation));
            let activation_block = activation_item
                .block
                .expect("served root verification items contain block bodies");
            let (sapling_root, orchard_root, ironwood_root) = activation_item
                .roots
                .expect("served root verification items contain roots");
            let history_tree = HistoryTree::from_block(
                &Mainnet,
                activation_block,
                &sapling_root,
                &orchard_root,
                &ironwood_root,
            )
            .expect("network-upgrade activation starts a history-tree epoch");
            let confirm_end = end
                .checked_add(1)
                .expect("diagnostic range end has a successor");
            let items =
                (activation + 1..=confirm_end).map(|height| verification_item_at(Height(height)));

            verify_commitment_roots(&Mainnet, history_tree, items).unwrap_or_else(|failure| {
                panic!(
                    "{upgrade:?} MMR linkage failed at {:?} while validating through \
                     {confirm_end}: {}",
                    failure.height(),
                    failure.error()
                )
            });
            eprintln!(
                "validated {upgrade:?} MMR linkage for {activation}..={end} \
                 (confirmed by {confirm_end})"
            );
        }
    }
}
