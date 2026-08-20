use chrono::{DateTime, Duration};
use zakura_chain::{
    block,
    parameters::{testnet::Parameters, POW_AVERAGING_WINDOW},
    work::difficulty::{ExpandedDifficulty, ParameterDifficulty as _, U256},
};

use super::super::AdjustedDifficulty;

#[test]
fn a_target_limit_that_would_overflow_mean_target_is_rejected() {
    // `MeanTarget` sums `PoWAveragingWindow` expanded difficulties into a
    // `U256`, so the limit and the window are validated together when the
    // network is built rather than clamped when a block is verified.
    let error = Parameters::build()
        .with_target_difficulty_limit(U256::MAX)
        .expect("the maximum compact-representable target is valid on its own")
        .to_network()
        .expect_err("a limit that overflows the mean-target sum is rejected");

    assert_eq!(
        error.to_string(),
        "target difficulty limit is too large for the configured averaging window"
    );
}

#[test]
fn custom_target_scaling_clamps_at_the_largest_accepted_limit() {
    let candidate_time =
        DateTime::from_timestamp(2_000_000_000, 0).expect("test timestamp is in range");

    // The largest limit `validate_pow_parameters` accepts for the default window.
    let limit = ExpandedDifficulty::from(U256::MAX / U256::from(POW_AVERAGING_WINDOW));
    let network = Parameters::build()
        .with_target_difficulty_limit(limit)
        .expect("the largest accepted target is valid")
        .to_network()
        .expect("the custom network parameters are valid");

    let compact = network.target_difficulty_limit().to_compact();
    let mut context = vec![(compact, candidate_time - Duration::seconds(1)); 17];
    context.extend(vec![
        (compact, candidate_time - Duration::seconds(100_000));
        11
    ]);
    let adjustment = AdjustedDifficulty::new_from_header_time(
        candidate_time,
        block::Height(699_999),
        &network,
        context,
    )
    .expect("the complete context is accepted");

    assert_eq!(
        adjustment.expected_difficulty_threshold(),
        network.target_difficulty_limit().to_compact()
    );
}
