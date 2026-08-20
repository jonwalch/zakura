//! Shared block difficulty adjustment and median-time calculations.

pub(crate) use zakura_header_chain::{
    pow_adjustment_block_span, AdjustedDifficulty, BLOCK_MAX_TIME_SINCE_MEDIAN,
};
