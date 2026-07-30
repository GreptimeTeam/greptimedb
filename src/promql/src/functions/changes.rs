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

//! Implementation of [`changes`](https://prometheus.io/docs/prometheus/latest/querying/functions/#changes) in PromQL. Refer to the [original
//! implementation](https://github.com/prometheus/prometheus/blob/main/promql/functions.go#L1023-L1040).

use datafusion::logical_expr::ScalarUDF;

use crate::functions::edge_count::{self, EdgeKind};

/// used to count the number of value changes that occur within a specific time range
#[derive(Debug)]
pub struct Changes {}

impl Changes {
    pub const fn name() -> &'static str {
        "prom_changes"
    }

    pub fn scalar_udf() -> ScalarUDF {
        edge_count::scalar_udf(Self::name(), EdgeKind::Changes)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, Float64Array, TimestampMillisecondArray};
    use datafusion::arrow::buffer::NullBuffer;
    use datafusion::common::DataFusionError;
    use datafusion::physical_plan::ColumnarValue;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::ScalarFunctionArgs;
    use datatypes::arrow::datatypes::{DataType, Field};

    use super::*;
    use crate::functions::test_util::simple_range_udf_runner;
    use crate::range_array::RangeArray;

    // build timestamp range and value range arrays for test
    fn build_test_range_arrays(
        timestamps: Vec<i64>,
        values: Vec<f64>,
        ranges: Vec<(u32, u32)>,
    ) -> (RangeArray, RangeArray) {
        let ts_array = Arc::new(TimestampMillisecondArray::from_iter(
            timestamps.into_iter().map(Some),
        ));
        let values_array = Arc::new(Float64Array::from_iter(values));

        let ts_range_array = RangeArray::from_ranges(ts_array, ranges.clone()).unwrap();
        let value_range_array = RangeArray::from_ranges(values_array, ranges).unwrap();

        (ts_range_array, value_range_array)
    }

    fn invoke_range_udf(
        udf: ScalarUDF,
        timestamps: RangeArray,
        values: RangeArray,
    ) -> Result<ColumnarValue, DataFusionError> {
        let number_rows = timestamps.len();
        let args = vec![
            ColumnarValue::Array(Arc::new(timestamps.into_dict())),
            ColumnarValue::Array(Arc::new(values.into_dict())),
        ];
        let arg_fields = vec![
            Arc::new(Field::new("timestamps", args[0].data_type(), false)),
            Arc::new(Field::new("values", args[1].data_type(), false)),
        ];
        udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field: Arc::new(Field::new("result", DataType::Float64, false)),
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    fn assert_execution_error(error: DataFusionError, expected: &str) {
        match error {
            DataFusionError::Execution(message) => assert_eq!(message, expected),
            other => panic!("expected execution error, got {other:?}"),
        }
    }

    #[test]
    fn calculate_changes() {
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
            Changes::scalar_udf(),
            ts_array_1,
            value_array_1,
            vec![],
            vec![Some(0.0), Some(3.0), Some(5.0), Some(8.0), None],
        );

        // assertion 2
        let values_2 = vec![1.0, 2.0, 3.0, 4.0, 5.0, 1.0, 2.0, 3.0, 4.0, 5.0];
        let (ts_array_2, value_array_2) =
            build_test_range_arrays(timestamps.clone(), values_2, ranges.clone());
        simple_range_udf_runner(
            Changes::scalar_udf(),
            ts_array_2,
            value_array_2,
            vec![],
            vec![Some(0.0), Some(3.0), Some(5.0), Some(9.0), None],
        );

        // assertion 3
        let values_3 = vec![0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0];
        let (ts_array_3, value_array_3) = build_test_range_arrays(timestamps, values_3, ranges);
        simple_range_udf_runner(
            Changes::scalar_udf(),
            ts_array_3,
            value_array_3,
            vec![],
            vec![Some(0.0), Some(0.0), Some(1.0), Some(1.0), None],
        );
    }

    const STALE_NAN: f64 = f64::from_bits(0x7ff0_0000_0000_0002);

    struct TinyPrng(u64);

    impl TinyPrng {
        fn next_u32(&mut self) -> u32 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 as u32
        }

        fn next_index(&mut self, bound: usize) -> usize {
            (self.next_u32() as usize) % bound
        }
    }

    fn changes_oracle(values: &[f64]) -> Option<f64> {
        let (first, rest) = values.split_first()?;
        let mut changes = 0;
        let mut previous = first;
        for current in rest {
            if current != previous && !(current.is_nan() && previous.is_nan()) {
                changes += 1;
            }
            previous = current;
        }
        Some(changes as f64)
    }

    fn run_oracle_ranges(
        values: Vec<Option<f64>>,
        raw_values: Vec<f64>,
        timestamp_ranges: Vec<(u32, u32)>,
        value_ranges: Vec<(u32, u32)>,
    ) -> Vec<Option<f64>> {
        assert_eq!(timestamp_ranges.len(), value_ranges.len());
        assert!(
            timestamp_ranges
                .iter()
                .zip(&value_ranges)
                .all(|((_, timestamp_length), (_, value_length))| timestamp_length == value_length)
        );
        assert_eq!(values.len(), raw_values.len());
        let nulls = values.iter().map(Option::is_none).collect::<Vec<_>>();

        let expected = value_ranges
            .iter()
            .map(|(offset, length)| {
                changes_oracle(&raw_values[*offset as usize..(*offset + *length) as usize])
            })
            .collect::<Vec<_>>();

        let timestamp_values = (0..64)
            .map(|value| Some(i64::from(value) * 1_000))
            .collect::<Vec<_>>();
        let timestamp_array = Arc::new(TimestampMillisecondArray::from_iter(timestamp_values));
        let value_array = Arc::new(Float64Array::from_iter(values));
        for (index, ((actual, expected), is_null)) in value_array
            .values()
            .iter()
            .zip(&raw_values)
            .zip(nulls)
            .enumerate()
        {
            assert_eq!(actual.to_bits(), expected.to_bits());
            assert_eq!(value_array.is_null(index), is_null);
        }

        let timestamp_ranges = RangeArray::from_ranges(timestamp_array, timestamp_ranges).unwrap();
        let value_ranges = RangeArray::from_ranges(value_array, value_ranges).unwrap();
        simple_range_udf_runner(
            Changes::scalar_udf(),
            timestamp_ranges,
            value_ranges,
            vec![],
            expected.clone(),
        );
        expected
    }

    #[test]
    fn changes_range_array_oracle_edge_cases() {
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
        let expected = run_oracle_ranges(
            values,
            raw_values,
            vec![(5, 0), (2, 1), (7, 5), (0, 5), (11, 5), (16, 4)],
            vec![(0, 0), (0, 1), (4, 5), (0, 5), (8, 5), (1, 4)],
        );
        assert_eq!(
            expected,
            vec![None, Some(0.0), Some(2.0), Some(2.0), Some(4.0), Some(2.0)]
        );

        let expected = run_oracle_ranges(
            vec![Some(2.0), None, Some(2.0)],
            vec![2.0, 0.0, 2.0],
            vec![(9, 3), (1, 1)],
            vec![(0, 3), (1, 1)],
        );
        assert_eq!(expected, vec![Some(2.0), Some(0.0)]);
    }

    #[test]
    fn changes_range_array_seeded_differential() {
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

        run_oracle_ranges(values, raw_values, timestamp_ranges, value_ranges);
    }

    #[test]
    fn changes_range_array_mismatch_errors() {
        let timestamps = Arc::new(TimestampMillisecondArray::from_iter([
            Some(0),
            Some(1),
            Some(2),
        ]));
        let values = Arc::new(Float64Array::from_iter([1.0, 2.0, 3.0]));
        let error = invoke_range_udf(
            Changes::scalar_udf(),
            RangeArray::from_ranges(timestamps.clone(), [(0, 1), (1, 1)]).unwrap(),
            RangeArray::from_ranges(values.clone(), [(0, 1)]).unwrap(),
        )
        .unwrap_err();
        assert_execution_error(
            error,
            "RangeArray have different lengths in PromQL function prom_changes: array1=2, array2=1",
        );

        let error = invoke_range_udf(
            Changes::scalar_udf(),
            RangeArray::from_ranges(timestamps, [(0, 1), (1, 2)]).unwrap(),
            RangeArray::from_ranges(values, [(0, 1), (1, 1)]).unwrap(),
        )
        .unwrap_err();
        assert_execution_error(
            error,
            "RangeArray's element 1 have different lengths in PromQL function prom_changes: array1=2, array2=1",
        );
    }

    #[test]
    fn changes_range_array_boundaries_and_raw_nulls() {
        // The 0.0 -> 1.0 edge enters the range; both identical windows contain no changes.
        let (timestamps, values) = build_test_range_arrays(
            vec![0, 1, 2, 3],
            vec![0.0, 1.0, 1.0, 1.0],
            vec![(1, 3), (1, 3)],
        );
        simple_range_udf_runner(
            Changes::scalar_udf(),
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
            Changes::scalar_udf(),
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
            Changes::scalar_udf(),
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
            Changes::scalar_udf(),
            RangeArray::from_ranges(timestamps, [(0, 3)]).unwrap(),
            RangeArray::from_ranges(values, [(0, 3)]).unwrap(),
            vec![],
            vec![Some(2.0)],
        );
    }
}
