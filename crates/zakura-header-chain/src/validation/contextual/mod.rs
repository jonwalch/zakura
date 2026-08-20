//! Block difficulty adjustment calculations for contextual validation.
//!
//! The contextual validator calculates the following consensus rules:
//!  * `ThresholdBits` from the Zcash Specification,
//!  * the Testnet minimum difficulty adjustment from ZIPs 205 and 208, and
//!  * `median-time-past`.

mod adjusted_difficulty;
mod constants;
mod validate;

pub use adjusted_difficulty::{AdjustedDifficulty, AdjustedDifficultyError};
pub use constants::{
    pow_adjustment_block_span, BLOCK_MAX_TIME_SINCE_MEDIAN, POW_ADJUSTMENT_BLOCK_SPAN,
    POW_MEDIAN_BLOCK_SPAN, POW_PREDECESSOR_CONTEXT_SPAN,
};
pub use validate::{validate_contextual_difficulty_and_time, ContextualValidationError};

#[cfg(test)]
mod tests;
