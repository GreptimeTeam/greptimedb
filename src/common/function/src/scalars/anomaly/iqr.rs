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

//! `anomaly_score_iqr` window function — IQR-based anomaly scoring.
//!
//! Algorithm:
//! - Compute Q1 (25th percentile) and Q3 (75th percentile)
//! - IQR = Q3 - Q1
//! - Lower fence = Q1 - k * IQR, Upper fence = Q3 + k * IQR
//! - If value is outside fences, score = |distance to nearest fence| / IQR
//! - Otherwise, score = 0.0

use std::any::Any;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{PartitionEvaluator, Signature, TypeSignature, Volatility, WindowUDFImpl};
use datafusion_functions_window_common::field::WindowUDFFieldArgs;
use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;

use super::utils::{MIN_SAMPLES, collect_window_values, nearly_equal, percentile_sorted};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AnomalyScoreIqr {
    signature: Signature,
}

impl AnomalyScoreIqr {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![
                    DataType::Float64,
                    DataType::Float64,
                ])],
                Volatility::Immutable,
            ),
        }
    }
}

impl WindowUDFImpl for AnomalyScoreIqr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "anomaly_score_iqr"
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
        Ok(Box::new(AnomalyScoreIqrEvaluator { current_row: 0 }))
    }
}

#[derive(Debug)]
struct AnomalyScoreIqrEvaluator {
    /// Tracks the current row index within the partition.
    current_row: usize,
}

impl PartitionEvaluator for AnomalyScoreIqrEvaluator {
    fn uses_window_frame(&self) -> bool {
        true
    }

    fn supports_bounded_execution(&self) -> bool {
        false
    }

    fn evaluate(&mut self, values: &[ArrayRef], range: &Range<usize>) -> Result<ScalarValue> {
        let array = values[0]
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("anomaly_score_iqr expects Float64 as first input");

        // Extract k from the second argument (constant across the window)
        let k_array = values[1]
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("anomaly_score_iqr expects Float64 as second input (k)");

        // Use the tracked current row index — correct for any window frame.
        let current_idx = self.current_row;
        self.current_row += 1;

        // Check bounds and validity of the current row.
        if current_idx >= array.len()
            || !array.is_valid(current_idx)
            || !array.value(current_idx).is_finite()
        {
            return Ok(ScalarValue::Float64(None));
        }
        let current_value = array.value(current_idx);

        // Get k value (use current row's k)
        if current_idx >= k_array.len()
            || !k_array.is_valid(current_idx)
            || !k_array.value(current_idx).is_finite()
        {
            return Ok(ScalarValue::Float64(None));
        }
        let k = k_array.value(current_idx);
        if k < 0.0 {
            return Ok(ScalarValue::Float64(None));
        }

        let mut window_values = collect_window_values(array, range);
        if window_values.len() < MIN_SAMPLES {
            return Ok(ScalarValue::Float64(None));
        }

        // Sort for percentile computation
        window_values.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());

        let q1 = match percentile_sorted(&window_values, 0.25) {
            Some(v) => v,
            None => return Ok(ScalarValue::Float64(None)),
        };
        let q3 = match percentile_sorted(&window_values, 0.75) {
            Some(v) => v,
            None => return Ok(ScalarValue::Float64(None)),
        };
        let iqr = q3 - q1;

        // Handle degenerate case: IQR = 0
        if nearly_equal(iqr, 0.0) {
            return if current_value >= q1 && current_value <= q3 {
                Ok(ScalarValue::Float64(Some(0.0)))
            } else {
                Ok(ScalarValue::Float64(None))
            };
        }

        let lower_fence = q1 - k * iqr;
        let upper_fence = q3 + k * iqr;

        let score = if current_value < lower_fence {
            (lower_fence - current_value) / iqr
        } else if current_value > upper_fence {
            (current_value - upper_fence) / iqr
        } else {
            0.0
        };

        Ok(ScalarValue::Float64(Some(score)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Float64Array;
    use datafusion_expr::WindowUDF;

    use super::*;

    fn eval_iqr(values: &[Option<f64>], k: f64, range: Range<usize>) -> ScalarValue {
        let array = Arc::new(Float64Array::from(values.to_vec())) as ArrayRef;
        let k_array = Arc::new(Float64Array::from(vec![Some(k); values.len()])) as ArrayRef;
        let current_row = range.end.saturating_sub(1);
        let mut evaluator = AnomalyScoreIqrEvaluator { current_row };
        evaluator.evaluate(&[array, k_array], &range).unwrap()
    }

    #[test]
    fn test_basic_outlier() {
        // Normal range [1..10] with outlier at 100
        let mut values: Vec<Option<f64>> = (1..=10).map(|x| Some(x as f64)).collect();
        values.push(Some(100.0));
        let result = eval_iqr(&values, 1.5, 0..11);
        match result {
            ScalarValue::Float64(Some(score)) => assert!(score > 0.0, "score={score}"),
            other => panic!("expected Some(score), got {other:?}"),
        }
    }

    #[test]
    fn test_value_within_fences() {
        let values: Vec<Option<f64>> = (1..=10).map(|x| Some(x as f64)).collect();
        // Last value is 10, which is within fences for k=1.5
        let result = eval_iqr(&values, 1.5, 0..10);
        match result {
            ScalarValue::Float64(Some(score)) => {
                assert!(score >= 0.0, "score should be non-negative, got {score}");
            }
            other => panic!("expected Some(score), got {other:?}"),
        }
    }

    #[test]
    fn test_constant_sequence() {
        let values: Vec<Option<f64>> = vec![Some(5.0); 10];
        // IQR=0, current is in [Q1,Q3] → 0.0
        let result = eval_iqr(&values, 1.5, 0..10);
        assert_eq!(result, ScalarValue::Float64(Some(0.0)));
    }

    #[test]
    fn test_all_null() {
        let values: Vec<Option<f64>> = vec![None; 5];
        let result = eval_iqr(&values, 1.5, 0..5);
        assert_eq!(result, ScalarValue::Float64(None));
    }

    #[test]
    fn test_insufficient_samples() {
        let values: Vec<Option<f64>> = vec![Some(1.0), Some(2.0)];
        let result = eval_iqr(&values, 1.5, 0..2);
        assert_eq!(result, ScalarValue::Float64(None));
    }

    #[test]
    fn test_negative_k() {
        let values: Vec<Option<f64>> =
            vec![Some(48.0), Some(49.0), Some(50.0), Some(51.0), Some(52.0)];
        let result = eval_iqr(&values, -1.0, 0..5);
        assert_eq!(result, ScalarValue::Float64(None));
    }

    #[test]
    fn test_current_row_null() {
        let values: Vec<Option<f64>> = vec![Some(1.0), Some(2.0), Some(3.0), None];
        let result = eval_iqr(&values, 1.5, 0..4);
        assert_eq!(result, ScalarValue::Float64(None));
    }

    #[test]
    fn test_lower_outlier() {
        // Data mostly around 50, one low value at the end
        let values: Vec<Option<f64>> = vec![
            Some(48.0),
            Some(49.0),
            Some(50.0),
            Some(51.0),
            Some(52.0),
            Some(-100.0),
        ];
        let result = eval_iqr(&values, 1.5, 0..6);
        match result {
            ScalarValue::Float64(Some(score)) => assert!(score > 0.0, "score={score}"),
            other => panic!("expected Some(score), got {other:?}"),
        }

        // Test that a value in the middle gets 0
        let values: Vec<Option<f64>> = vec![
            Some(48.0),
            Some(49.0),
            Some(50.0),
            Some(51.0),
            Some(52.0),
            Some(-100.0),
            Some(50.0),
        ];
        let result = eval_iqr(&values, 1.5, 0..7);
        assert_eq!(result, ScalarValue::Float64(Some(0.0)));
    }

    #[test]
    fn test_udwf_creation() {
        let udwf = WindowUDF::from(AnomalyScoreIqr::new());
        assert_eq!(udwf.name(), "anomaly_score_iqr");
    }
}
