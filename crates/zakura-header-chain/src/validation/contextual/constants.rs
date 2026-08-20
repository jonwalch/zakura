use zakura_chain::parameters::{Network, MAX_POW_ADJUSTMENT_BLOCK_SPAN, POW_AVERAGING_WINDOW};

/// The median block span for time median calculations.
///
/// `PoWMedianBlockSpan` in the Zcash specification.
pub const POW_MEDIAN_BLOCK_SPAN: usize = 11;

/// The overall block span used for adjusting Zcash block difficulty.
///
/// `PoWAveragingWindow + PoWMedianBlockSpan` in the Zcash specification based on
/// > ActualTimespan(height : N) := MedianTime(height) − MedianTime(height − PoWAveragingWindow)
pub const POW_ADJUSTMENT_BLOCK_SPAN: usize = POW_AVERAGING_WINDOW + POW_MEDIAN_BLOCK_SPAN;

/// Durable predecessors needed below a separately retained parent frontier.
pub const POW_PREDECESSOR_CONTEXT_SPAN: usize = POW_ADJUSTMENT_BLOCK_SPAN - 1;

/// Returns the overall block span used for adjusting block difficulty on `network`.
///
/// `PoWAveragingWindow + PoWMedianBlockSpan` in the Zcash specification, based on
/// > ActualTimespan(height : N) := MedianTime(height) − MedianTime(height − PoWAveragingWindow)
///
/// This is [`POW_ADJUSTMENT_BLOCK_SPAN`] on Mainnet and the default Testnet. A
/// configured Testnet may raise either term, but the network builder rejects a
/// sum above [`MAX_POW_ADJUSTMENT_BLOCK_SPAN`], which is what keeps the
/// difficulty context vectors bounded.
pub fn pow_adjustment_block_span(network: &Network) -> usize {
    let span = network
        .pow_averaging_window()
        .saturating_add(network.pow_median_block_span());

    debug_assert!(
        span <= MAX_POW_ADJUSTMENT_BLOCK_SPAN,
        "configured networks with an oversized adjustment span are rejected when they are built"
    );

    span.min(MAX_POW_ADJUSTMENT_BLOCK_SPAN)
}

/// The maximum number of seconds between the `median-time-past` of a block,
/// and the block's `time` field.
///
/// Part of the block header consensus rules in the Zcash specification.
pub const BLOCK_MAX_TIME_SINCE_MEDIAN: u32 = 90 * 60;
