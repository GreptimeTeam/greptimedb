// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Anomaly detection window functions.
//!
//! This module provides statistical anomaly scoring functions that operate
//! as window UDFs (User Defined Window Functions):
//!
//! - `anomaly_score_mad(value)` — MAD-based scoring
//! - `anomaly_score_iqr(value, k)` — IQR-based scoring with configurable fence multiplier
//! - `anomaly_score_zscore(value)` — Z-Score-based scoring
//!
//! These functions return a floating-point anomaly score rather than a boolean,
//! allowing users to set their own threshold via `WHERE score > N`.
//!
//! ## Minimum Samples
//!
//! Each function has its own minimum based on statistical validity:
//!
//! | Function | min_samples | Rationale |
//! |---|---|---|
//! | `anomaly_score_zscore` | 2 | stddev requires n >= 2 (aligned with `STDDEV_SAMP`) |
//! | `anomaly_score_mad` | 3 | n <= 2 makes MAD almost always 0, yielding spurious +inf |
//! | `anomaly_score_iqr` | 3 | linear-interpolated Q1 != Q3 is possible at n >= 3 |
//!
//! ## Return Values
//!
//! | Condition | zscore | mad | iqr | Result |
//! |---|---|---|---|---|
//! | insufficient valid points | n < 2 | n < 3 | n < 3 | `NULL` |
//! | stddev / MAD / IQR = 0, value = center | distance = 0 | distance = 0 | on fence | `0.0` |
//! | stddev / MAD / IQR = 0, value ≠ center | distance > 0 | distance > 0 | outside fence | `+inf` |
//! | normal case | stddev > 0 | MAD > 0 | IQR > 0 | finite positive |
//!
//! ## Window Frame Semantics
//!
//! The functions score the **current row** in the partition, regardless of
//! window frame type. This works correctly for all frame specifications:
//!
//! ```sql
//! -- Trailing window
//! anomaly_score_mad(cpu) OVER (ORDER BY ts ROWS 100 PRECEDING)
//! -- Centered window
//! anomaly_score_mad(cpu) OVER (ORDER BY ts ROWS BETWEEN 50 PRECEDING AND 50 FOLLOWING)
//! ```
//!
//! Internally, a row counter tracks which partition row is being evaluated.
//! The `range` parameter determines only which rows participate in computing
//! the window statistics (median, MAD, IQR, mean, stddev).
//!
//! ## Performance Notes
//!
//! Current implementation uses per-row evaluation with O(N × W) complexity
//! where N is the partition size and W is the window size. This is acceptable
//! for typical window sizes (W ≤ a few thousand).
//!
//! Future optimizations could include:
//! - Incremental computation using order-statistic trees or two-heap median
//!   maintenance, reducing to O(N × log W)
//! - Batch `evaluate_all` for fixed-size windows

mod iqr;
mod mad;
pub(crate) mod utils;
mod zscore;

use datafusion_expr::WindowUDF;

use crate::function_registry::FunctionRegistry;

pub struct AnomalyFunction;

impl AnomalyFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register_window(WindowUDF::from(mad::AnomalyScoreMad::new()));
        registry.register_window(WindowUDF::from(iqr::AnomalyScoreIqr::new()));
        registry.register_window(WindowUDF::from(zscore::AnomalyScoreZscore::new()));
    }
}
