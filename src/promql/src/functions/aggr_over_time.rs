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

use std::sync::Arc;

use common_macro::range_fn;
use datafusion::arrow::array::{Float64Array, TimestampMillisecondArray};
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{ScalarUDF, Volatility};
use datafusion::physical_plan::ColumnarValue;
use datatypes::arrow::array::Array;
use datatypes::arrow::compute;
use datatypes::arrow::datatypes::DataType;

use crate::functions::{compensated_sum_inc, extract_array};
use crate::range_array::RangeArray;

/// The average value of all points in the specified interval.
#[range_fn(
    name = AvgOverTime,
    ret = Float64Array,
    display_name = prom_avg_over_time
)]
pub fn avg_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    compute::sum(values).map(|result| result / values.len() as f64)
}

/// The minimum value of all points in the specified interval.
#[range_fn(
    name = MinOverTime,
    ret = Float64Array,
    display_name = prom_min_over_time
)]
pub fn min_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    let mut valid_values = values.iter().flatten();
    let mut min = valid_values.next()?;
    for value in valid_values {
        if value < min || min.is_nan() {
            min = value;
        }
    }
    Some(min)
}

/// The maximum value of all points in the specified interval.
#[range_fn(
    name = MaxOverTime,
    ret = Float64Array,
    display_name = prom_max_over_time
)]
pub fn max_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    let mut valid_values = values.iter().flatten();
    let mut max = valid_values.next()?;
    for value in valid_values {
        if value > max || max.is_nan() {
            max = value;
        }
    }
    Some(max)
}

/// The sum of all values in the specified interval.
#[range_fn(
    name = SumOverTime,
    ret = Float64Array,
    display_name = prom_sum_over_time
)]
pub fn sum_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    compute::sum(values)
}

/// The count of all values in the specified interval.
#[range_fn(
    name = CountOverTime,
    ret = Float64Array,
    display_name = prom_count_over_time
)]
pub fn count_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(values.len() as f64)
    }
}

/// The most recent point value in specified interval.
#[range_fn(
    name = LastOverTime,
    ret = Float64Array,
    display_name = prom_last_over_time
)]
pub fn last_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    values.values().last().copied()
}

/// absent_over_time returns an empty vector if the range vector passed to it has any
/// elements (floats or native histograms) and a 1-element vector with the value 1 if
/// the range vector passed to it has no elements.
#[range_fn(
    name = AbsentOverTime,
    ret = Float64Array,
    display_name = prom_absent_over_time
)]
pub fn absent_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    if values.is_empty() { Some(1.0) } else { None }
}

/// the value 1 for any series in the specified interval.
#[range_fn(
    name = PresentOverTime,
    ret = Float64Array,
    display_name = prom_present_over_time
)]
pub fn present_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    if values.is_empty() { None } else { Some(1.0) }
}

/// the population standard variance of the values in the specified interval.
/// DataFusion's implementation:
/// <https://github.com/apache/arrow-datafusion/blob/292eb954fc0bad3a1febc597233ba26cb60bda3e/datafusion/physical-expr/src/aggregate/variance.rs#L224-#L241>
#[range_fn(
    name = StdvarOverTime,
    ret = Float64Array,
    display_name = prom_stdvar_over_time
)]
pub fn stdvar_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        let mut count = 0;
        let mut mean: f64 = 0.0;
        let mut result: f64 = 0.0;
        for value in values {
            let value = value.unwrap();
            let new_count = count + 1;
            let delta1 = value - mean;
            let new_mean = delta1 / new_count as f64 + mean;
            let delta2 = value - new_mean;
            let new_result = result + delta1 * delta2;

            count += 1;
            mean = new_mean;
            result = new_result;
        }
        Some(result / count as f64)
    }
}

/// the population standard deviation of the values in the specified interval.
/// Prometheus's implementation: <https://github.com/prometheus/prometheus/blob/f55ab2217984770aa1eecd0f2d5f54580029b1c0/promql/functions.go#L556-L569>
#[range_fn(
    name = StddevOverTime,
    ret = Float64Array,
    display_name = prom_stddev_over_time
)]
pub fn stddev_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        let mut count = 0.0;
        let mut mean = 0.0;
        let mut comp_mean = 0.0;
        let mut deviations_sum_sq = 0.0;
        let mut comp_deviations_sum_sq = 0.0;
        for v in values {
            count += 1.0;
            let current_value = v.unwrap();
            let delta = current_value - (mean + comp_mean);
            let (new_mean, new_comp_mean) = compensated_sum_inc(delta / count, mean, comp_mean);
            mean = new_mean;
            comp_mean = new_comp_mean;
            let (new_deviations_sum_sq, new_comp_deviations_sum_sq) = compensated_sum_inc(
                delta * (current_value - (mean + comp_mean)),
                deviations_sum_sq,
                comp_deviations_sum_sq,
            );
            deviations_sum_sq = new_deviations_sum_sq;
            comp_deviations_sum_sq = new_comp_deviations_sum_sq;
        }
        Some(((deviations_sum_sq + comp_deviations_sum_sq) / count).sqrt())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::functions::test_util::{
        STALE_NAN, TinyPrng, invoke_range_udf, simple_range_udf_runner,
    };

    fn assert_over_time_value(actual: Option<f64>, expected: Option<f64>) {
        match (actual, expected) {
            (Some(actual), Some(expected)) if expected.is_nan() => assert!(actual.is_nan()),
            (Some(actual), Some(expected)) => assert_eq!(actual, expected),
            (None, None) => {}
            (actual, expected) => panic!("expected {expected:?}, got {actual:?}"),
        }
    }

    fn assert_min_max(
        values: Vec<Option<f64>>,
        expected_min: Option<f64>,
        expected_max: Option<f64>,
    ) {
        let timestamps = TimestampMillisecondArray::from(vec![0; values.len()]);
        let values = Float64Array::from(values);

        assert_over_time_value(min_over_time(&timestamps, &values), expected_min);
        assert_over_time_value(max_over_time(&timestamps, &values), expected_max);
    }

    #[test]
    fn min_max_over_time_ignore_ordinary_nan_when_finite_values_exist() {
        let ordinary_nan = f64::from_bits(0x7ff8_0000_0000_0000);

        assert_min_max(
            vec![Some(ordinary_nan), Some(3.0), Some(-2.0)],
            Some(-2.0),
            Some(3.0),
        );
        assert_min_max(
            vec![Some(3.0), Some(ordinary_nan), Some(-2.0)],
            Some(-2.0),
            Some(3.0),
        );
        assert_min_max(
            vec![Some(-2.0), Some(3.0), Some(ordinary_nan)],
            Some(-2.0),
            Some(3.0),
        );
        assert_min_max(
            vec![Some(ordinary_nan), Some(ordinary_nan)],
            Some(ordinary_nan),
            Some(ordinary_nan),
        );
        assert_min_max(
            vec![Some(3.0), Some(-2.0), Some(1.0)],
            Some(-2.0),
            Some(3.0),
        );
        assert_min_max(vec![], None, None);
        assert_min_max(vec![None, None], None, None);
    }

    // build timestamp range and value range arrays for test
    fn build_test_range_arrays() -> (RangeArray, RangeArray) {
        let ts_array = Arc::new(TimestampMillisecondArray::from_iter(
            [
                1000i64, 3000, 5000, 7000, 9000, 11000, 13000, 15000, 17000, 200000, 500000,
            ]
            .into_iter()
            .map(Some),
        ));
        let ranges = [
            (0, 2),
            (0, 5),
            (1, 1), // only 1 element
            (2, 0), // empty range
            (2, 0), // empty range
            (3, 3),
            (4, 3),
            (5, 3),
            (8, 1), // only 1 element
            (9, 0), // empty range
        ];

        let values_array = Arc::new(Float64Array::from_iter([
            12.345678, 87.654321, 31.415927, 27.182818, 70.710678, 41.421356, 57.735027, 69.314718,
            98.019802, 1.98019802, 61.803399,
        ]));

        let ts_range_array = RangeArray::from_ranges(ts_array, ranges).unwrap();
        let value_range_array = RangeArray::from_ranges(values_array, ranges).unwrap();

        (ts_range_array, value_range_array)
    }

    #[test]
    fn calculate_avg_over_time() {
        let (ts_array, value_array) = build_test_range_arrays();
        simple_range_udf_runner(
            AvgOverTime::scalar_udf(),
            ts_array,
            value_array,
            vec![],
            vec![
                Some(49.9999995),
                Some(45.8618844),
                Some(87.654321),
                None,
                None,
                Some(46.438284),
                Some(56.62235366666667),
                Some(56.15703366666667),
                Some(98.019802),
                None,
            ],
        );
    }

    #[test]
    fn calculate_min_over_time() {
        let (ts_array, value_array) = build_test_range_arrays();
        simple_range_udf_runner(
            MinOverTime::scalar_udf(),
            ts_array,
            value_array,
            vec![],
            vec![
                Some(12.345678),
                Some(12.345678),
                Some(87.654321),
                None,
                None,
                Some(27.182818),
                Some(41.421356),
                Some(41.421356),
                Some(98.019802),
                None,
            ],
        );
    }

    #[test]
    fn calculate_max_over_time() {
        let (ts_array, value_array) = build_test_range_arrays();
        simple_range_udf_runner(
            MaxOverTime::scalar_udf(),
            ts_array,
            value_array,
            vec![],
            vec![
                Some(87.654321),
                Some(87.654321),
                Some(87.654321),
                None,
                None,
                Some(70.710678),
                Some(70.710678),
                Some(69.314718),
                Some(98.019802),
                None,
            ],
        );
    }

    #[test]
    fn calculate_sum_over_time() {
        let (ts_array, value_array) = build_test_range_arrays();
        simple_range_udf_runner(
            SumOverTime::scalar_udf(),
            ts_array,
            value_array,
            vec![],
            vec![
                Some(99.999999),
                Some(229.309422),
                Some(87.654321),
                None,
                None,
                Some(139.314852),
                Some(169.867061),
                Some(168.471101),
                Some(98.019802),
                None,
            ],
        );
    }

    #[test]
    fn calculate_count_over_time() {
        let (ts_array, value_array) = build_test_range_arrays();
        simple_range_udf_runner(
            CountOverTime::scalar_udf(),
            ts_array,
            value_array,
            vec![],
            vec![
                Some(2.0),
                Some(5.0),
                Some(1.0),
                None,
                None,
                Some(3.0),
                Some(3.0),
                Some(3.0),
                Some(1.0),
                None,
            ],
        );
    }

    #[test]
    fn calculate_last_over_time() {
        let (ts_array, value_array) = build_test_range_arrays();
        simple_range_udf_runner(
            LastOverTime::scalar_udf(),
            ts_array,
            value_array,
            vec![],
            vec![
                Some(87.654321),
                Some(70.710678),
                Some(87.654321),
                None,
                None,
                Some(41.421356),
                Some(57.735027),
                Some(69.314718),
                Some(98.019802),
                None,
            ],
        );
    }

    #[test]
    fn calculate_absent_over_time() {
        let (ts_array, value_array) = build_test_range_arrays();
        simple_range_udf_runner(
            AbsentOverTime::scalar_udf(),
            ts_array,
            value_array,
            vec![],
            vec![
                None,
                None,
                None,
                Some(1.0),
                Some(1.0),
                None,
                None,
                None,
                None,
                Some(1.0),
            ],
        );
    }

    #[test]
    fn calculate_present_over_time() {
        let (ts_array, value_array) = build_test_range_arrays();
        simple_range_udf_runner(
            PresentOverTime::scalar_udf(),
            ts_array,
            value_array,
            vec![],
            vec![
                Some(1.0),
                Some(1.0),
                Some(1.0),
                None,
                None,
                Some(1.0),
                Some(1.0),
                Some(1.0),
                Some(1.0),
                None,
            ],
        );
    }

    #[test]
    fn calculate_stdvar_over_time() {
        let (ts_array, value_array) = build_test_range_arrays();
        simple_range_udf_runner(
            StdvarOverTime::scalar_udf(),
            ts_array,
            value_array,
            vec![],
            vec![
                Some(1417.8479276253622),
                Some(808.999919713209),
                Some(0.0),
                None,
                None,
                Some(328.3638826418587),
                Some(143.5964181766362),
                Some(130.91830542386285),
                Some(0.0),
                None,
            ],
        );

        // add more assertions
        let ts_array = Arc::new(TimestampMillisecondArray::from_iter(
            [1000i64, 3000, 5000, 7000, 9000, 11000, 13000, 15000]
                .into_iter()
                .map(Some),
        ));
        let values_array = Arc::new(Float64Array::from_iter([
            1.5990505637277868,
            1.5990505637277868,
            1.5990505637277868,
            0.0,
            8.0,
            8.0,
            2.0,
            3.0,
        ]));
        let ranges = [(0, 3), (3, 5)];
        simple_range_udf_runner(
            StdvarOverTime::scalar_udf(),
            RangeArray::from_ranges(ts_array, ranges).unwrap(),
            RangeArray::from_ranges(values_array, ranges).unwrap(),
            vec![],
            vec![Some(0.0), Some(10.559999999999999)],
        );
    }

    #[test]
    fn calculate_std_dev_over_time() {
        let (ts_array, value_array) = build_test_range_arrays();
        simple_range_udf_runner(
            StddevOverTime::scalar_udf(),
            ts_array,
            value_array,
            vec![],
            vec![
                Some(37.6543215),
                Some(28.442923895289123),
                Some(0.0),
                None,
                None,
                Some(18.12081352042062),
                Some(11.983172291869804),
                Some(11.441953741554055),
                Some(0.0),
                None,
            ],
        );

        // add more assertions
        let ts_array = Arc::new(TimestampMillisecondArray::from_iter(
            [1000i64, 3000, 5000, 7000, 9000, 11000, 13000, 15000]
                .into_iter()
                .map(Some),
        ));
        let values_array = Arc::new(Float64Array::from_iter([
            1.5990505637277868,
            1.5990505637277868,
            1.5990505637277868,
            0.0,
            8.0,
            8.0,
            2.0,
            3.0,
        ]));
        let ranges = [(0, 3), (3, 5)];
        simple_range_udf_runner(
            StddevOverTime::scalar_udf(),
            RangeArray::from_ranges(ts_array, ranges).unwrap(),
            RangeArray::from_ranges(values_array, ranges).unwrap(),
            vec![],
            vec![Some(0.0), Some(3.249615361854384)],
        );
    }

    // === Phase 2a: frozen extrema-selection semantics for min/max_over_time ===
    //
    // These tests pin down (without touching production code) the exact behavior
    // of `min_over_time`/`max_over_time`:
    // - left-to-right scan, null slots skipped; empty/all-null window -> NULL;
    // - seed = first non-null value; replace only on strict `<` / `>` (a NaN seed
    //   is healed by any later value via `current.is_nan()`);
    // - NaN payloads (ordinary + stale `0x7ff0000000000002`) are all "ordinary":
    //   an all-NaN window yields the *last* input NaN's bit pattern;
    // - `-0.0` and `0.0` compare equal -> first occurrence wins (sign bit kept);
    // - repeated extrema: strict comparison -> leftmost (smallest ordinal) wins.

    const NAN_PAYLOADS: [u64; 3] = [
        0x7ff8_0000_0000_0000, // ordinary quiet NaN
        0x7ff8_0000_0000_1234, // quiet NaN with payload
        STALE_NAN.to_bits(),   // stale NaN (Prometheus #4386)
    ];
    const SMALL_FINITE: [f64; 3] = [1.0, -1.0, 2.0];

    /// Bit-for-bit comparison of `Option<f64>` results (NaN payloads and ±0.0 matter).
    fn assert_option_bits_eq(actual: Option<f64>, expected: Option<f64>, what: &str) {
        match (actual, expected) {
            (Some(actual), Some(expected)) => assert_eq!(
                actual.to_bits(),
                expected.to_bits(),
                "{what}: expected {expected:?} (bits {expected_bits:#018x}), got {actual:?} (bits {actual_bits:#018x})",
                expected_bits = expected.to_bits(),
                actual_bits = actual.to_bits(),
            ),
            (None, None) => {}
            (actual, expected) => panic!("{what}: expected {expected:?}, got {actual:?}"),
        }
    }

    fn assert_min_bits(values: Vec<Option<f64>>, expected: Option<f64>) {
        let timestamps = TimestampMillisecondArray::from(vec![0; values.len()]);
        let values = Float64Array::from(values);
        assert_option_bits_eq(
            min_over_time(&timestamps, &values),
            expected,
            "min_over_time",
        );
    }

    fn assert_max_bits(values: Vec<Option<f64>>, expected: Option<f64>) {
        let timestamps = TimestampMillisecondArray::from(vec![0; values.len()]);
        let values = Float64Array::from(values);
        assert_option_bits_eq(
            max_over_time(&timestamps, &values),
            expected,
            "max_over_time",
        );
    }

    /// Run a range UDF and compare every output row bit-for-bit (NULL slots included).
    fn assert_range_udf_bits(
        udf: ScalarUDF,
        timestamps: RangeArray,
        values: RangeArray,
        expected: &[Option<f64>],
    ) {
        let result = invoke_range_udf(udf, timestamps, values).unwrap();
        let result_array_ref = extract_array(&result).unwrap();
        let result_array = result_array_ref
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(result_array.len(), expected.len());
        for (index, (actual, expected)) in result_array.iter().zip(expected).enumerate() {
            match (actual, *expected) {
                (Some(actual), Some(expected)) => assert_eq!(
                    actual.to_bits(),
                    expected.to_bits(),
                    "row {index}: expected {expected:?} (bits {expected_bits:#018x}), got {actual:?} (bits {actual_bits:#018x})",
                    expected_bits = expected.to_bits(),
                    actual_bits = actual.to_bits(),
                ),
                (None, None) => {}
                (actual, expected) => panic!("row {index}: expected {expected:?}, got {actual:?}"),
            }
        }
    }

    /// Frozen semantic baseline for `min_over_time`. Independent, literal
    /// transcription of the pinned semantics above.
    fn min_over_time_ref(values: &[Option<f64>]) -> Option<f64> {
        let mut min: Option<f64> = None;
        for value in values.iter().flatten().copied() {
            match min {
                None => min = Some(value),
                Some(current) if value < current || current.is_nan() => min = Some(value),
                _ => {}
            }
        }
        min
    }

    /// Frozen semantic baseline for `max_over_time` (see [`min_over_time_ref`]).
    fn max_over_time_ref(values: &[Option<f64>]) -> Option<f64> {
        let mut max: Option<f64> = None;
        for value in values.iter().flatten().copied() {
            match max {
                None => max = Some(value),
                Some(current) if value > current || current.is_nan() => max = Some(value),
                _ => {}
            }
        }
        max
    }

    /// Deterministic window generator mixing null / NaN payloads (incl. stale NaN) /
    /// ±0.0 / small finite (ties) / generic finite values. A few kinds are fully
    /// biased toward corner cases so every corpus covers all-NaN, all-null and
    /// all-±0.0 windows.
    fn random_window(prng: &mut TinyPrng) -> Vec<Option<f64>> {
        let len = (prng.next_u32() % 9) as usize;
        match prng.next_u32() % 8 {
            0 => vec![None; len], // all-null
            1 => (0..len)
                .map(|_| {
                    Some(f64::from_bits(
                        NAN_PAYLOADS[prng.next_index(NAN_PAYLOADS.len())],
                    ))
                })
                .collect(), // all-NaN, mixed payloads
            2 => (0..len)
                .map(|_| Some(if prng.next_u32() & 1 == 0 { 0.0 } else { -0.0 }))
                .collect(), // all-±0.0
            3 => (0..len)
                .map(|_| Some(SMALL_FINITE[prng.next_index(SMALL_FINITE.len())]))
                .collect(), // small finite -> repeated extrema
            _ => (0..len)
                .map(|_| match prng.next_u32() % 12 {
                    0..=1 => None,
                    2..=4 => Some(f64::from_bits(
                        NAN_PAYLOADS[prng.next_index(NAN_PAYLOADS.len())],
                    )),
                    5..=6 => Some(if prng.next_u32() & 1 == 0 { 0.0 } else { -0.0 }),
                    7..=8 => Some(SMALL_FINITE[prng.next_index(SMALL_FINITE.len())]),
                    _ => Some((prng.next_u32() as f64 / 100.0) - 50.0),
                })
                .collect(), // generic mixed corpus
        }
    }

    #[test]
    fn min_max_over_time_nan_at_head_middle_tail_with_finite_values() {
        let nan_a = f64::from_bits(NAN_PAYLOADS[0]);
        let nan_b = f64::from_bits(NAN_PAYLOADS[1]);

        // Ordinary NaN at head/middle/tail never displaces a finite extrema.
        for nan in [nan_a, nan_b] {
            // min: [NaN,1,2] -> 1, [1,NaN,2] -> 1, [1,2,NaN] -> 1
            assert_min_bits(vec![Some(nan), Some(1.0), Some(2.0)], Some(1.0));
            assert_min_bits(vec![Some(1.0), Some(nan), Some(2.0)], Some(1.0));
            assert_min_bits(vec![Some(1.0), Some(2.0), Some(nan)], Some(1.0));
            // max analog
            assert_max_bits(vec![Some(nan), Some(1.0), Some(2.0)], Some(2.0));
            assert_max_bits(vec![Some(1.0), Some(nan), Some(2.0)], Some(2.0));
            assert_max_bits(vec![Some(1.0), Some(2.0), Some(nan)], Some(2.0));
            // negative finite values
            assert_min_bits(vec![Some(nan), Some(-5.0), Some(-1.0)], Some(-5.0));
            assert_max_bits(vec![Some(nan), Some(-5.0), Some(-1.0)], Some(-1.0));
        }
    }

    #[test]
    fn min_max_over_time_all_nan_window_returns_last_nan_bits() {
        let nan_a = f64::from_bits(NAN_PAYLOADS[0]);
        let nan_b = f64::from_bits(NAN_PAYLOADS[1]);

        // All-NaN window: result is NaN and carries the *last* input NaN payload.
        assert_min_bits(vec![Some(nan_a), Some(nan_b)], Some(nan_b));
        assert_max_bits(vec![Some(nan_a), Some(nan_b)], Some(nan_b));
        assert_min_bits(vec![Some(nan_b), Some(nan_a)], Some(nan_a));
        assert_max_bits(vec![Some(nan_b), Some(nan_a)], Some(nan_a));
        assert_min_bits(vec![Some(nan_a), Some(nan_b), Some(nan_a)], Some(nan_a));
        assert_max_bits(vec![Some(nan_a), Some(nan_b), Some(nan_a)], Some(nan_a));
        // Single-NaN window yields that NaN itself.
        assert_min_bits(vec![Some(nan_b)], Some(nan_b));
        assert_max_bits(vec![Some(nan_b)], Some(nan_b));
    }

    #[test]
    fn min_max_over_time_stale_nan_is_ordinary_nan() {
        let nan_a = f64::from_bits(NAN_PAYLOADS[0]);
        let nan_b = f64::from_bits(NAN_PAYLOADS[1]);
        let stale = f64::from_bits(NAN_PAYLOADS[2]);
        assert_eq!(stale.to_bits(), 0x7ff0_0000_0000_0002);

        // Mixed NaN payloads, all-NaN window: last input bit pattern wins.
        assert_min_bits(vec![Some(nan_a), Some(stale), Some(nan_b)], Some(nan_b));
        assert_max_bits(vec![Some(nan_a), Some(stale), Some(nan_b)], Some(nan_b));
        assert_min_bits(vec![Some(stale), Some(nan_a)], Some(nan_a));
        assert_max_bits(vec![Some(stale), Some(nan_a)], Some(nan_a));

        // Stale NaN alongside finite values behaves like an ordinary NaN:
        // a leading stale NaN is healed, a trailing one is ignored.
        assert_min_bits(vec![Some(stale), Some(1.0)], Some(1.0));
        assert_max_bits(vec![Some(stale), Some(1.0)], Some(1.0));
        assert_min_bits(vec![Some(1.0), Some(stale), Some(2.0)], Some(1.0));
        assert_max_bits(vec![Some(1.0), Some(stale), Some(2.0)], Some(2.0));
        assert_min_bits(vec![Some(nan_a), Some(stale), Some(3.0)], Some(3.0));
        assert_max_bits(vec![Some(nan_a), Some(stale), Some(3.0)], Some(3.0));
    }

    #[test]
    fn min_max_over_time_negative_zero_first_wins() {
        let pos_zero = 0.0f64;
        let neg_zero = -0.0f64;
        assert_ne!(pos_zero.to_bits(), neg_zero.to_bits());

        // `-0.0 < 0.0` and `0.0 < -0.0` are both false -> first occurrence wins,
        // and its sign bit is preserved in the result (asserted via `to_bits`).
        assert_min_bits(vec![Some(pos_zero), Some(neg_zero)], Some(pos_zero));
        assert_max_bits(vec![Some(pos_zero), Some(neg_zero)], Some(pos_zero));
        assert_min_bits(vec![Some(neg_zero), Some(pos_zero)], Some(neg_zero));
        assert_max_bits(vec![Some(neg_zero), Some(pos_zero)], Some(neg_zero));
        // The sign bit survives even when non-displacing values are present.
        assert_min_bits(
            vec![Some(pos_zero), Some(neg_zero), Some(1.0)],
            Some(pos_zero),
        );
        assert_max_bits(
            vec![Some(pos_zero), Some(neg_zero), Some(-1.0)],
            Some(pos_zero),
        );
        assert_min_bits(
            vec![Some(neg_zero), Some(pos_zero), Some(1.0)],
            Some(neg_zero),
        );
        assert_max_bits(
            vec![Some(neg_zero), Some(pos_zero), Some(-1.0)],
            Some(neg_zero),
        );
    }

    #[test]
    fn min_max_over_time_repeated_extrema_leftmost_wins() {
        // Strict `<`/`>` replacement: a repeated extrema never replaces the
        // leftmost (smallest ordinal) occurrence.
        assert_min_bits(vec![Some(2.0), Some(1.0), Some(1.0)], Some(1.0));
        assert_min_bits(vec![Some(1.0), Some(1.0), Some(0.0)], Some(0.0));
        assert_max_bits(vec![Some(1.0), Some(2.0), Some(2.0)], Some(2.0));
        assert_max_bits(vec![Some(2.0), Some(2.0), Some(3.0)], Some(3.0));

        // Same value at different positions -> same stable result (compare-only,
        // no replacement; `to_bits` cannot distinguish the position, so the
        // stability is pinned by the value-level result).
        assert_min_bits(vec![Some(1.0), Some(2.0), Some(1.0)], Some(1.0));
        assert_min_bits(vec![Some(1.0), Some(1.0), Some(2.0)], Some(1.0));
        assert_max_bits(vec![Some(2.0), Some(1.0), Some(2.0)], Some(2.0));
        assert_max_bits(vec![Some(2.0), Some(2.0), Some(1.0)], Some(2.0));

        // A NaN can never displace a finite extrema, even when it trails a tie.
        let nan = f64::from_bits(NAN_PAYLOADS[0]);
        assert_min_bits(vec![Some(1.0), Some(1.0), Some(nan)], Some(1.0));
        assert_max_bits(vec![Some(1.0), Some(1.0), Some(nan)], Some(1.0));
    }

    #[test]
    fn min_max_over_time_null_mixed_windows() {
        // [null, 5, null, 3] -> 3 (min) / 5 (max)
        assert_min_bits(vec![None, Some(5.0), None, Some(3.0)], Some(3.0));
        assert_max_bits(vec![None, Some(5.0), None, Some(3.0)], Some(5.0));
        // all-null window -> NULL
        assert_min_bits(vec![None, None, None], None);
        assert_max_bits(vec![None, None, None], None);
        // empty window -> NULL
        assert_min_bits(vec![], None);
        assert_max_bits(vec![], None);
        // a single non-null value amid nulls -> that value
        assert_min_bits(vec![None, Some(-7.5), None], Some(-7.5));
        assert_max_bits(vec![None, Some(-7.5), None], Some(-7.5));
    }

    #[test]
    fn min_max_over_time_multi_window_including_single_and_empty() {
        let nan_a = f64::from_bits(NAN_PAYLOADS[0]);
        let nan_b = f64::from_bits(NAN_PAYLOADS[1]);
        let stale = f64::from_bits(NAN_PAYLOADS[2]);

        let ts_array = Arc::new(TimestampMillisecondArray::from_iter(
            [1000i64, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000]
                .into_iter()
                .map(Some),
        ));
        let values_array = Arc::new(Float64Array::from_iter([
            Some(1.0),   // 0
            Some(nan_a), // 1
            None,        // 2
            Some(2.0),   // 3
            Some(-0.0),  // 4
            Some(stale), // 5
            Some(3.0),   // 6
            None,        // 7
            Some(nan_b), // 8
        ]));
        // (0,1) [1.0]                  -> 1.0    | 1.0
        // (1,2) [nan_a, null]          -> nan_a  | nan_a
        // (2,0) []                     -> NULL   | NULL
        // (3,3) [2.0, -0.0, stale]     -> -0.0   | 2.0
        // (4,1) [-0.0]                 -> -0.0   | -0.0
        // (5,3) [stale, 3.0, null]     -> 3.0    | 3.0
        // (6,2) [3.0, null]            -> 3.0    | 3.0
        // (7,1) [null]                 -> NULL   | NULL
        // (8,0) []                     -> NULL   | NULL
        let ranges = [
            (0u32, 1u32),
            (1, 2),
            (2, 0),
            (3, 3),
            (4, 1),
            (5, 3),
            (6, 2),
            (7, 1),
            (8, 0),
        ];
        let min_expected = [
            Some(1.0),
            Some(nan_a),
            None,
            Some(-0.0),
            Some(-0.0),
            Some(3.0),
            Some(3.0),
            None,
            None,
        ];
        let max_expected = [
            Some(1.0),
            Some(nan_a),
            None,
            Some(2.0),
            Some(-0.0),
            Some(3.0),
            Some(3.0),
            None,
            None,
        ];

        assert_range_udf_bits(
            MinOverTime::scalar_udf(),
            RangeArray::from_ranges(ts_array.clone(), ranges).unwrap(),
            RangeArray::from_ranges(values_array.clone(), ranges).unwrap(),
            &min_expected,
        );
        assert_range_udf_bits(
            MaxOverTime::scalar_udf(),
            RangeArray::from_ranges(ts_array, ranges).unwrap(),
            RangeArray::from_ranges(values_array, ranges).unwrap(),
            &max_expected,
        );
    }

    #[test]
    fn min_max_over_time_differential_vs_reference() {
        let mut windows_with_nan = 0usize;
        let mut windows_with_zero = 0usize;
        let mut empty_windows = 0usize;
        let mut all_null_windows = 0usize;
        let mut all_nan_windows = 0usize;

        // 8 rounds, each with its own seed; 200 windows per round.
        for round in 0..8 {
            let mut prng = TinyPrng(0x5eed_1234_abcd_0001 ^ ((round as u64) << 32));
            let windows: Vec<Vec<Option<f64>>> =
                (0..200).map(|_| random_window(&mut prng)).collect();

            for window in &windows {
                let non_null = window.iter().flatten().copied().collect::<Vec<_>>();
                windows_with_nan += usize::from(non_null.iter().any(|v| v.is_nan()));
                windows_with_zero += usize::from(
                    non_null
                        .iter()
                        .any(|v| v.to_bits() & 0x7fff_ffff_ffff_ffff == 0),
                );
                empty_windows += usize::from(window.is_empty());
                all_null_windows +=
                    usize::from(!window.is_empty() && window.iter().all(Option::is_none));
                all_nan_windows +=
                    usize::from(!non_null.is_empty() && non_null.iter().all(|v| v.is_nan()));

                // Direct per-window comparison against the frozen reference.
                let timestamps = TimestampMillisecondArray::from(vec![0; window.len()]);
                let values = Float64Array::from(window.clone());
                let label = format!("round {round}");
                assert_option_bits_eq(
                    min_over_time(&timestamps, &values),
                    min_over_time_ref(window),
                    &format!("{label}: min"),
                );
                assert_option_bits_eq(
                    max_over_time(&timestamps, &values),
                    max_over_time_ref(window),
                    &format!("{label}: max"),
                );
            }

            // UDF-level multi-window comparison (all windows in one RangeArray).
            let mut concat = Vec::with_capacity(windows.iter().map(Vec::len).sum());
            let mut ranges: Vec<(u32, u32)> = Vec::with_capacity(windows.len());
            let mut offset = 0u32;
            for window in &windows {
                if window.is_empty() {
                    // An empty window can legally point anywhere; anchor it at 0.
                    ranges.push((0, 0));
                } else {
                    ranges.push((offset, window.len() as u32));
                    offset += window.len() as u32;
                }
                concat.extend(window.iter().copied());
            }
            let ts_array = Arc::new(TimestampMillisecondArray::from_iter(
                (0..concat.len() as i64).map(Some),
            ));
            let value_array = Arc::new(Float64Array::from_iter(concat));
            let min_expected: Vec<Option<f64>> =
                windows.iter().map(|w| min_over_time_ref(w)).collect();
            let max_expected: Vec<Option<f64>> =
                windows.iter().map(|w| max_over_time_ref(w)).collect();
            assert_range_udf_bits(
                MinOverTime::scalar_udf(),
                RangeArray::from_ranges(ts_array.clone(), ranges.clone()).unwrap(),
                RangeArray::from_ranges(value_array.clone(), ranges.clone()).unwrap(),
                &min_expected,
            );
            assert_range_udf_bits(
                MaxOverTime::scalar_udf(),
                RangeArray::from_ranges(ts_array, ranges.clone()).unwrap(),
                RangeArray::from_ranges(value_array, ranges).unwrap(),
                &max_expected,
            );
        }

        // Corpus coverage guards: the seeded generator must actually exercise the
        // tricky cases, otherwise the differential test would be vacuous.
        assert!(windows_with_nan > 0, "corpus must contain NaN windows");
        assert!(windows_with_zero > 0, "corpus must contain ±0.0 windows");
        assert!(empty_windows > 0, "corpus must contain empty windows");
        assert!(all_null_windows > 0, "corpus must contain all-null windows");
        assert!(all_nan_windows > 0, "corpus must contain all-NaN windows");
    }
}
