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

//! Shared statistical utilities for anomaly detection window functions.

use std::ops::Range;

use arrow::array::{Array, ArrayRef, Float64Array};
use arrow::compute;
use arrow::datatypes::DataType;
use datafusion_common::DataFusionError;

/// Cast an ArrayRef to Float64. Returns the array as-is if already Float64.
pub fn cast_to_f64(array: &ArrayRef) -> datafusion_common::Result<ArrayRef> {
    if array.data_type() == &DataType::Float64 {
        return Ok(array.clone());
    }
    compute::cast(array, &DataType::Float64)
        .map_err(|e| DataFusionError::Internal(format!("Failed to cast to Float64: {e}")))
}

/// Collect valid f64 values from a Float64Array within the given range,
/// skipping NULL, NaN, and ±Inf values.
pub fn collect_window_values(array: &Float64Array, range: &Range<usize>) -> Vec<f64> {
    let mut values = Vec::with_capacity(range.len());
    for i in range.clone() {
        if array.is_valid(i) {
            let v = array.value(i);
            if v.is_finite() {
                values.push(v);
            }
        }
    }
    values
}

/// Compute median of a mutable slice using O(n) selection algorithm.
///
/// The input slice will be partially reordered.
/// Returns `None` if the slice is empty.
pub fn median_f64(values: &mut [f64]) -> Option<f64> {
    let len = values.len();
    if len == 0 {
        return None;
    }
    let mid = len / 2;
    let (lower, median, _) = values.select_nth_unstable_by(mid, |a, b| a.total_cmp(b));
    if len % 2 == 1 {
        Some(*median)
    } else {
        let right = *median;
        // For even length, find the max of the left half (all elements before mid).
        let left = lower.iter().copied().max_by(|a, b| a.total_cmp(b)).unwrap();
        Some((left + right) / 2.0)
    }
}

/// Compute percentile on a sorted slice using linear interpolation.
///
/// `p` should be in [0.0, 1.0]. The slice must be sorted in ascending order.
/// Returns `None` if the slice is empty.
pub fn percentile_sorted(sorted: &[f64], p: f64) -> Option<f64> {
    let len = sorted.len();
    if len == 0 {
        return None;
    }
    if len == 1 {
        return Some(sorted[0]);
    }
    let idx = p * (len - 1) as f64;
    let lower = idx.floor() as usize;
    let upper = idx.ceil() as usize;
    if lower == upper {
        Some(sorted[lower])
    } else {
        let frac = idx - lower as f64;
        Some(sorted[lower] * (1.0 - frac) + sorted[upper] * frac)
    }
}

/// Compute a non-negative anomaly score ratio with stable zero-denominator semantics.
///
/// When `scale == 0.0`:
/// - returns `0.0` if `distance == 0.0` (on-center value),
/// - returns `+inf` if `distance > 0.0` (off-center value under zero spread).
#[inline]
pub fn anomaly_ratio(distance: f64, scale: f64) -> f64 {
    if scale == 0.0 {
        if distance == 0.0 { 0.0 } else { f64::INFINITY }
    } else {
        distance / scale
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_median_odd() {
        let mut v = vec![5.0, 1.0, 3.0, 2.0, 4.0];
        assert_eq!(median_f64(&mut v), Some(3.0));
    }

    #[test]
    fn test_median_even() {
        let mut v = vec![4.0, 1.0, 3.0, 2.0];
        assert_eq!(median_f64(&mut v), Some(2.5));
    }

    #[test]
    fn test_median_single() {
        let mut v = vec![42.0];
        assert_eq!(median_f64(&mut v), Some(42.0));
    }

    #[test]
    fn test_median_empty() {
        let mut v: Vec<f64> = vec![];
        assert_eq!(median_f64(&mut v), None);
    }

    #[test]
    fn test_percentile_sorted_quartiles() {
        let sorted = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        let q1 = percentile_sorted(&sorted, 0.25).unwrap();
        let q3 = percentile_sorted(&sorted, 0.75).unwrap();
        assert!((q1 - 3.25).abs() < 1e-10);
        assert!((q3 - 7.75).abs() < 1e-10);
    }

    #[test]
    fn test_percentile_sorted_empty() {
        assert_eq!(percentile_sorted(&[], 0.5), None);
    }

    #[test]
    fn test_collect_window_values_filters_invalid() {
        let array = Float64Array::from(vec![
            Some(1.0),
            None,
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(2.0),
            Some(3.0),
        ]);
        let values = collect_window_values(&array, &(0..7));
        assert_eq!(values, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_anomaly_ratio_zero_scale() {
        assert_eq!(anomaly_ratio(0.0, 0.0), 0.0);
        assert!(anomaly_ratio(1.0, 0.0).is_infinite());
    }
}
