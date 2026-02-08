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

//! `anomaly_score_mad` window function — MAD-based anomaly scoring.
//!
//! Algorithm: `score = |x - median(window)| / (MAD * 1.4826)`
//! where `MAD = median(|xi - median(window)|)`
//!
//! When MAD = 0 (majority-constant window), returns 0.0 if value equals
//! median, or +inf otherwise.

use std::any::Any;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::type_coercion::aggregates::NUMERICS;
use datafusion_expr::{PartitionEvaluator, Signature, Volatility, WindowUDFImpl};
use datafusion_functions_window_common::field::WindowUDFFieldArgs;
use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;

use crate::scalars::anomaly::utils::{
    MIN_SAMPLES, anomaly_ratio, cast_to_f64, collect_window_values, median_f64,
};

/// MAD consistency constant for normal distribution: `1 / Φ⁻¹(3/4) ≈ 1.4826`
const MAD_CONSISTENCY_CONSTANT: f64 = 1.4826;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AnomalyScoreMad {
    signature: Signature,
}

impl AnomalyScoreMad {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(1, NUMERICS.to_vec(), Volatility::Immutable),
        }
    }
}

impl WindowUDFImpl for AnomalyScoreMad {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "anomaly_score_mad"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            field_args.name(),
            DataType::Float64,
            true, // nullable
        )))
    }

    fn partition_evaluator(
        &self,
        _partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(AnomalyScoreMadEvaluator { current_row: 0 }))
    }
}

#[derive(Debug)]
struct AnomalyScoreMadEvaluator {
    /// Tracks the current row index within the partition.
    /// DataFusion calls `evaluate()` sequentially for row 0, 1, 2, …
    /// and creates a fresh evaluator per partition.
    current_row: usize,
}

impl PartitionEvaluator for AnomalyScoreMadEvaluator {
    fn uses_window_frame(&self) -> bool {
        true
    }

    fn supports_bounded_execution(&self) -> bool {
        false
    }

    fn evaluate(&mut self, values: &[ArrayRef], range: &Range<usize>) -> Result<ScalarValue> {
        let values_f64 = cast_to_f64(&values[0])?;
        let array = values_f64
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Expected Float64Array, got: {:?}",
                    values_f64.data_type()
                ))
            })?;

        // Use the tracked current row index — correct for any window frame
        // (trailing, leading, centered, unbounded).
        let current_idx = self.current_row;
        self.current_row += 1;

        if current_idx >= array.len()
            || !array.is_valid(current_idx)
            || !array.value(current_idx).is_finite()
        {
            return Ok(ScalarValue::Float64(None));
        }
        let current_value = array.value(current_idx);

        let mut window_values = collect_window_values(array, range);
        if window_values.len() < MIN_SAMPLES {
            return Ok(ScalarValue::Float64(None));
        }

        // Compute median of window
        let median = match median_f64(&mut window_values) {
            Some(m) => m,
            None => return Ok(ScalarValue::Float64(None)),
        };

        // Compute MAD = median(|xi - median|)
        let mut abs_deviations: Vec<f64> =
            window_values.iter().map(|x| (x - median).abs()).collect();
        let mad = match median_f64(&mut abs_deviations) {
            Some(m) => m,
            None => return Ok(ScalarValue::Float64(None)),
        };

        let distance = (current_value - median).abs();
        let scale = mad * MAD_CONSISTENCY_CONSTANT;
        let score = anomaly_ratio(distance, scale);
        Ok(ScalarValue::Float64(Some(score)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Float64Array;
    use datafusion_expr::WindowUDF;

    use super::*;

    fn eval_mad(values: &[Option<f64>], range: Range<usize>) -> ScalarValue {
        let array = Arc::new(Float64Array::from(values.to_vec())) as ArrayRef;
        // current_row is the last index in the range — the row being scored
        let current_row = range.end.saturating_sub(1);
        let mut evaluator = AnomalyScoreMadEvaluator { current_row };
        evaluator.evaluate(&[array], &range).unwrap()
    }

    #[test]
    fn test_basic_outlier() {
        // Normal data with one outlier (100.0)
        let values: Vec<Option<f64>> = vec![
            Some(1.0),
            Some(2.0),
            Some(1.5),
            Some(2.5),
            Some(1.0),
            Some(100.0),
        ];
        let result = eval_mad(&values, 0..6);
        match result {
            ScalarValue::Float64(Some(score)) => assert!(score > 3.0, "score={score}"),
            other => panic!("expected Some(score), got {other:?}"),
        }
    }

    #[test]
    fn test_constant_sequence() {
        let values: Vec<Option<f64>> = vec![Some(5.0); 10];
        // All values are the same -> zero spread and zero distance => 0.0
        let result = eval_mad(&values, 0..10);
        match result {
            ScalarValue::Float64(Some(score)) => assert_eq!(score, 0.0),
            other => panic!("expected Some(0.0), got {other:?}"),
        }
    }

    #[test]
    fn test_mad_zero_non_median() {
        // More than 50% identical and current differs from median -> non-zero / 0 => +inf
        let values: Vec<Option<f64>> = vec![Some(1.0), Some(1.0), Some(1.0), Some(1.0), Some(5.0)];
        let result = eval_mad(&values, 0..5);
        match result {
            ScalarValue::Float64(Some(score)) => {
                assert!(score.is_infinite() && score.is_sign_positive())
            }
            other => panic!("expected Some(+inf), got {other:?}"),
        }
    }

    #[test]
    fn test_all_null() {
        let values: Vec<Option<f64>> = vec![None, None, None, None];
        let result = eval_mad(&values, 0..4);
        assert_eq!(result, ScalarValue::Float64(None));
    }

    #[test]
    fn test_insufficient_samples() {
        let values: Vec<Option<f64>> = vec![Some(1.0), Some(2.0)];
        let result = eval_mad(&values, 0..2);
        assert_eq!(result, ScalarValue::Float64(None));
    }

    #[test]
    fn test_nan_inf_skipped() {
        let values: Vec<Option<f64>> = vec![
            Some(1.0),
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(2.0),
            Some(3.0),
        ];
        let result = eval_mad(&values, 0..5);
        // Should compute using [1.0, 2.0, 3.0] only
        match result {
            ScalarValue::Float64(Some(_)) => {}
            other => panic!("expected Some(score), got {other:?}"),
        }
    }

    #[test]
    fn test_current_row_null() {
        let values: Vec<Option<f64>> = vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0), None];
        let result = eval_mad(&values, 0..5);
        assert_eq!(result, ScalarValue::Float64(None));
    }

    #[test]
    fn test_udwf_creation() {
        let udwf = WindowUDF::from(AnomalyScoreMad::new());
        assert_eq!(udwf.name(), "anomaly_score_mad");
    }
}
