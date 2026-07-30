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

//! Implementation of [`reset`](https://prometheus.io/docs/prometheus/latest/querying/functions/#resets) in PromQL. Refer to the [original
//! implementation](https://github.com/prometheus/prometheus/blob/90b2f7a540b8a70d8d81372e6692dcbb67ccbaaa/promql/functions.go#L1004-L1021).

use datafusion::logical_expr::ScalarUDF;

use crate::functions::edge_count::{self, EdgeKind};

/// used to count the number of times the time series starts over.
#[derive(Debug)]
pub struct Resets {}

impl Resets {
    pub const fn name() -> &'static str {
        "prom_resets"
    }

    pub fn scalar_udf() -> ScalarUDF {
        edge_count::scalar_udf(Self::name(), EdgeKind::Resets)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datafusion::arrow::array::{Float64Array, TimestampMillisecondArray};
    use datafusion::arrow::buffer::NullBuffer;
    use datatypes::arrow::array::Array;

    use super::*;
    use crate::functions::test_util::{
        self, STALE_NAN, TinyPrng, assert_execution_error, build_test_range_arrays,
        invoke_range_udf, simple_range_udf_runner,
    };
    use crate::range_array::RangeArray;

    fn resets_oracle(values: &[f64]) -> Option<f64> {
        let (first, rest) = values.split_first()?;
        let mut resets = 0;
        let mut previous = first;
        for current in rest {
            if current < previous {
                resets += 1;
            }
            previous = current;
        }
        Some(resets as f64)
    }

    #[test]
    fn calculate_resets() {
        let timestamps = vec![
            1000i64, 3000, 5000, 7000, 9000, 11000, 13000, 15000, 17000, 200000, 500000,
        ];
        let ranges = vec![
            (0, 1),
            (0, 4),
            (0, 6),
            (0, 10),
            (0, 0), // empty range
        ];

        // assertion 1
        let values_1 = vec![1.0, 2.0, 3.0, 0.0, 1.0, 0.0, 0.0, 1.0, 2.0, 0.0];
        let (ts_array_1, value_array_1) =
            build_test_range_arrays(timestamps.clone(), values_1, ranges.clone());
        simple_range_udf_runner(
            Resets::scalar_udf(),
            ts_array_1,
            value_array_1,
            vec![],
            vec![Some(0.0), Some(1.0), Some(2.0), Some(3.0), None],
        );

        // assertion 2
        let values_2 = vec![1.0, 2.0, 3.0, 4.0, 5.0, 1.0, 2.0, 3.0, 4.0, 5.0];
        let (ts_array_2, value_array_2) =
            build_test_range_arrays(timestamps.clone(), values_2, ranges.clone());
        simple_range_udf_runner(
            Resets::scalar_udf(),
            ts_array_2,
            value_array_2,
            vec![],
            vec![Some(0.0), Some(0.0), Some(1.0), Some(1.0), None],
        );

        // assertion 3
        let values_3 = vec![0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0];
        let (ts_array_3, value_array_3) = build_test_range_arrays(timestamps, values_3, ranges);
        simple_range_udf_runner(
            Resets::scalar_udf(),
            ts_array_3,
            value_array_3,
            vec![],
            vec![Some(0.0), Some(0.0), Some(0.0), Some(0.0), None],
        );
    }

    #[test]
    fn resets_range_array_oracle_edge_cases() {
        let values = vec![
            Some(-0.0),
            Some(0.0),
            Some(f64::INFINITY),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(f64::NAN),
            Some(STALE_NAN),
            Some(f64::NAN),
            Some(3.0),
            Some(1.0),
            Some(2.0),
            Some(3.0),
            Some(1.0),
            Some(3.0),
            Some(0.0),
        ];
        let raw_values = values.iter().map(|value| value.unwrap()).collect();
        let expected = test_util::run_oracle_ranges(
            values,
            raw_values,
            vec![(5, 0), (2, 1), (7, 5), (0, 5), (11, 5), (16, 4)],
            vec![(0, 0), (0, 1), (4, 5), (0, 5), (8, 5), (1, 4)],
            resets_oracle,
            Resets::scalar_udf(),
        );
        assert_eq!(
            expected,
            vec![None, Some(0.0), Some(0.0), Some(1.0), Some(2.0), Some(1.0)]
        );

        let expected = test_util::run_oracle_ranges(
            vec![Some(2.0), None, Some(2.0)],
            vec![2.0, 0.0, 2.0],
            vec![(9, 3), (1, 1)],
            vec![(0, 3), (1, 1)],
            resets_oracle,
            Resets::scalar_udf(),
        );
        assert_eq!(expected, vec![Some(1.0), Some(0.0)]);
    }

    #[test]
    fn resets_range_array_seeded_differential() {
        let mut prng = TinyPrng(0x2f6e_2b1d_834a_90c5);
        let raw_values = (0..48)
            .map(|_| match prng.next_index(12) {
                0 => -0.0,
                1 => 0.0,
                2 => -2.0,
                3 => -1.0,
                4 => 1.0,
                5 => 2.0,
                6 => f64::INFINITY,
                7 => f64::NEG_INFINITY,
                8 | 9 => f64::NAN,
                _ => STALE_NAN,
            })
            .collect::<Vec<_>>();
        let values = raw_values.iter().copied().map(Some).collect();
        let mut timestamp_ranges = Vec::new();
        let mut value_ranges = Vec::new();
        for _ in 0..32 {
            let length = prng.next_index(13) as u32;
            timestamp_ranges.push((prng.next_index(65 - length as usize) as u32, length));
            value_ranges.push((prng.next_index(49 - length as usize) as u32, length));
        }

        test_util::run_oracle_ranges(
            values,
            raw_values,
            timestamp_ranges,
            value_ranges,
            resets_oracle,
            Resets::scalar_udf(),
        );
    }

    #[test]
    fn resets_range_array_mismatch_errors() {
        let timestamps = Arc::new(TimestampMillisecondArray::from_iter([
            Some(0),
            Some(1),
            Some(2),
        ]));
        let values = Arc::new(Float64Array::from_iter([1.0, 2.0, 3.0]));
        let error = invoke_range_udf(
            Resets::scalar_udf(),
            RangeArray::from_ranges(timestamps.clone(), [(0, 1), (1, 1)]).unwrap(),
            RangeArray::from_ranges(values.clone(), [(0, 1)]).unwrap(),
        )
        .unwrap_err();
        assert_execution_error(
            error,
            "RangeArray have different lengths in PromQL function prom_resets: array1=2, array2=1",
        );

        let error = invoke_range_udf(
            Resets::scalar_udf(),
            RangeArray::from_ranges(timestamps, [(0, 1), (1, 2)]).unwrap(),
            RangeArray::from_ranges(values, [(0, 1), (1, 1)]).unwrap(),
        )
        .unwrap_err();
        assert_execution_error(
            error,
            "RangeArray's element 1 have different lengths in PromQL function prom_resets: array1=2, array2=1",
        );
    }

    #[test]
    fn resets_range_array_boundaries_and_raw_nulls() {
        // The 3.0 -> 1.0 edge enters the range; both identical windows contain no resets.
        let (timestamps, values) = build_test_range_arrays(
            vec![0, 1, 2, 3],
            vec![3.0, 1.0, 1.0, 1.0],
            vec![(1, 3), (1, 3)],
        );
        simple_range_udf_runner(
            Resets::scalar_udf(),
            timestamps,
            values,
            vec![],
            vec![Some(0.0), Some(0.0)],
        );

        let (timestamps, values) = build_test_range_arrays(
            vec![0, 1, 2, 3],
            vec![1.0, 2.0, 3.0, 4.0],
            vec![(0, 0), (2, 0), (4, 0)],
        );
        simple_range_udf_runner(
            Resets::scalar_udf(),
            timestamps,
            values,
            vec![],
            vec![None, None, None],
        );

        let (timestamps, values) = build_test_range_arrays(
            vec![0, 1, 2, 3],
            vec![1.0, 2.0, 3.0, 4.0],
            vec![(0, 1), (2, 1), (3, 1)],
        );
        simple_range_udf_runner(
            Resets::scalar_udf(),
            timestamps,
            values,
            vec![],
            vec![Some(0.0), Some(0.0), Some(0.0)],
        );

        let values = Arc::new(Float64Array::new(
            vec![10.0, 7.0, 10.0].into(),
            Some(NullBuffer::from(vec![true, false, true])),
        ));
        assert!(!values.is_valid(1));
        assert_eq!(values.value(1), 7.0);
        let timestamps = Arc::new(TimestampMillisecondArray::from_iter([
            Some(0),
            Some(1),
            Some(2),
        ]));
        simple_range_udf_runner(
            Resets::scalar_udf(),
            RangeArray::from_ranges(timestamps, [(0, 3)]).unwrap(),
            RangeArray::from_ranges(values, [(0, 3)]).unwrap(),
            vec![],
            vec![Some(1.0)],
        );
    }
}
