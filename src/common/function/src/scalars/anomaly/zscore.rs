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

//! `anomaly_score_zscore` window function — Z-Score-based anomaly scoring.
//!
//! Algorithm: `score = |x - mean(window)| / stddev(window)`

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

use super::utils::{MIN_SAMPLES, collect_window_values};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AnomalyScoreZscore {
    signature: Signature,
}

impl AnomalyScoreZscore {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::Float64])],
                Volatility::Immutable,
            ),
        }
    }
}

impl WindowUDFImpl for AnomalyScoreZscore {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "anomaly_score_zscore"
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
        Ok(Box::new(AnomalyScoreZscoreEvaluator { current_row: 0 }))
    }
}

#[derive(Debug)]
struct AnomalyScoreZscoreEvaluator {
    /// Tracks the current row index within the partition.
    current_row: usize,
}

impl PartitionEvaluator for AnomalyScoreZscoreEvaluator {
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
            .expect("anomaly_score_zscore expects Float64 input");

        // Use the tracked current row index — correct for any window frame.
        let current_idx = self.current_row;
        self.current_row += 1;

        if current_idx >= array.len()
            || !array.is_valid(current_idx)
            || !array.value(current_idx).is_finite()
        {
            return Ok(ScalarValue::Float64(None));
        }
        let current_value = array.value(current_idx);

        let window_values = collect_window_values(array, range);
        if window_values.len() < MIN_SAMPLES {
            return Ok(ScalarValue::Float64(None));
        }

        let n = window_values.len() as f64;
        let mean = window_values.iter().sum::<f64>() / n;

        let variance = window_values
            .iter()
            .map(|x| (x - mean).powi(2))
            .sum::<f64>()
            / n;
        let stddev = variance.sqrt();

        let score = (current_value - mean).abs() / stddev;
        Ok(ScalarValue::Float64(Some(score)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Float64Array;
    use datafusion_expr::WindowUDF;

    use super::*;

    fn eval_zscore(values: &[Option<f64>], range: Range<usize>) -> ScalarValue {
        let array = Arc::new(Float64Array::from(values.to_vec())) as ArrayRef;
        let current_row = range.end.saturating_sub(1);
        let mut evaluator = AnomalyScoreZscoreEvaluator { current_row };
        evaluator.evaluate(&[array], &range).unwrap()
    }

    #[test]
    fn test_basic_outlier() {
        // Use enough normal points so the outlier doesn't dominate stddev
        let values: Vec<Option<f64>> = vec![
            Some(1.0),
            Some(2.0),
            Some(1.5),
            Some(2.5),
            Some(1.0),
            Some(2.0),
            Some(1.5),
            Some(2.5),
            Some(1.0),
            Some(2.0),
            Some(100.0),
        ];
        let len = values.len();
        let result = eval_zscore(&values, 0..len);
        match result {
            ScalarValue::Float64(Some(score)) => assert!(score > 3.0, "score={score}"),
            other => panic!("expected Some(score), got {other:?}"),
        }
    }

    #[test]
    fn test_constant_sequence() {
        let values: Vec<Option<f64>> = vec![Some(5.0); 10];
        let result = eval_zscore(&values, 0..10);
        match result {
            ScalarValue::Float64(Some(score)) => assert!(score.is_nan()),
            other => panic!("expected Some(NaN), got {other:?}"),
        }
    }

    #[test]
    fn test_stddev_zero_non_mean() {
        // All same but current is different → NULL
        let values: Vec<Option<f64>> = vec![Some(1.0), Some(1.0), Some(1.0), Some(1.0), Some(5.0)];
        let result = eval_zscore(&values, 0..5);
        // stddev will be non-zero because 5.0 is in the window
        match result {
            ScalarValue::Float64(Some(score)) => assert!(score > 0.0),
            other => panic!("expected Some(score>0), got {other:?}"),
        }
    }

    #[test]
    fn test_all_null() {
        let values: Vec<Option<f64>> = vec![None, None, None, None];
        let result = eval_zscore(&values, 0..4);
        assert_eq!(result, ScalarValue::Float64(None));
    }

    #[test]
    fn test_insufficient_samples() {
        let values: Vec<Option<f64>> = vec![Some(1.0), Some(2.0)];
        let result = eval_zscore(&values, 0..2);
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
        let result = eval_zscore(&values, 0..5);
        match result {
            ScalarValue::Float64(Some(_)) => {}
            other => panic!("expected Some(score), got {other:?}"),
        }
    }

    #[test]
    fn test_current_row_null() {
        let values: Vec<Option<f64>> = vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0), None];
        let result = eval_zscore(&values, 0..5);
        assert_eq!(result, ScalarValue::Float64(None));
    }

    #[test]
    fn test_known_zscore() {
        // For data [0, 0, 0, 0, 10]:
        // mean = 2.0, variance = (4+4+4+4+64)/5 = 16, stddev = 4.0
        // zscore of 10 = |10-2|/4 = 2.0
        let values: Vec<Option<f64>> = vec![Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(10.0)];
        let result = eval_zscore(&values, 0..5);
        match result {
            ScalarValue::Float64(Some(score)) => {
                assert!((score - 2.0).abs() < 1e-10, "score={score}")
            }
            other => panic!("expected Some(2.0), got {other:?}"),
        }
    }

    #[test]
    fn test_udwf_creation() {
        let udwf = WindowUDF::from(AnomalyScoreZscore::new());
        assert_eq!(udwf.name(), "anomaly_score_zscore");
    }

    /// Verify the current_row counter increments correctly across sequential
    /// evaluate() calls, and that each call scores the right row.
    #[test]
    fn test_sequential_evaluate_calls() {
        // Data: [0, 0, 0, 0, 10]
        // Full window for all rows (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        // mean=2.0, stddev=4.0 for all rows
        let values: Vec<Option<f64>> = vec![Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(10.0)];
        let array = Arc::new(Float64Array::from(values)) as ArrayRef;
        let full_range = 0..5;
        let vals = std::slice::from_ref(&array);

        let mut evaluator = AnomalyScoreZscoreEvaluator { current_row: 0 };

        // Row 0: value=0.0, zscore = |0-2|/4 = 0.5
        let r0 = evaluator.evaluate(vals, &full_range).unwrap();
        assert_eq!(r0, ScalarValue::Float64(Some(0.5)));

        // Row 1: value=0.0, same score
        let r1 = evaluator.evaluate(vals, &full_range).unwrap();
        assert_eq!(r1, ScalarValue::Float64(Some(0.5)));

        // Row 4: skip ahead by consuming rows 2 and 3
        let _ = evaluator.evaluate(vals, &full_range).unwrap();
        let _ = evaluator.evaluate(vals, &full_range).unwrap();

        // Row 4: value=10.0, zscore = |10-2|/4 = 2.0
        let r4 = evaluator.evaluate(vals, &full_range).unwrap();
        assert_eq!(r4, ScalarValue::Float64(Some(2.0)));
    }

    /// Verify correct behavior with a centered window frame where the current
    /// row is NOT at range.end - 1. This is the regression test for the P1
    /// review finding about range.end - 1 assumption.
    #[test]
    fn test_centered_window_frame() {
        // Data: [0, 0, 10, 0, 0]
        // Centered window of size 3: ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
        let values: Vec<Option<f64>> = vec![Some(0.0), Some(0.0), Some(10.0), Some(0.0), Some(0.0)];
        let array = Arc::new(Float64Array::from(values)) as ArrayRef;

        // Score row 2 (value=10.0) with window [1..4) = {0.0, 10.0, 0.0}
        // mean = 10/3 ≈ 3.333, variance = ((10/3)^2 + (20/3)^2 + (10/3)^2)/3
        // = (100/9 + 400/9 + 100/9) / 3 = 600/27 ≈ 22.222
        // stddev ≈ 4.714, zscore = |10 - 3.333| / 4.714 ≈ 1.414
        let mut evaluator = AnomalyScoreZscoreEvaluator { current_row: 2 };
        let result = evaluator
            .evaluate(std::slice::from_ref(&array), &(1..4))
            .unwrap();
        match result {
            ScalarValue::Float64(Some(score)) => {
                // sqrt(2) ≈ 1.4142
                assert!(
                    (score - std::f64::consts::SQRT_2).abs() < 1e-10,
                    "expected ~1.414, got {score}"
                );
            }
            other => panic!("expected Some(score), got {other:?}"),
        }
    }
}
