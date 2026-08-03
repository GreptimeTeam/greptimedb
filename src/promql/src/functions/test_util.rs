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

use datafusion::arrow::array::{Float64Array, TimestampMillisecondArray};
use datafusion::common::DataFusionError;
use datafusion::logical_expr::ScalarUDF;
use datafusion::physical_plan::ColumnarValue;
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::ScalarFunctionArgs;
use datatypes::arrow::array::Array;
use datatypes::arrow::datatypes::{DataType, Field};

use crate::functions::extract_array;
use crate::range_array::RangeArray;

/// Runner to run range UDFs that only requires ts range and value range.
pub fn simple_range_udf_runner(
    range_fn: ScalarUDF,
    input_ts: RangeArray,
    input_value: RangeArray,
    other_args: Vec<ScalarValue>,
    expected: Vec<Option<f64>>,
) {
    let num_rows = input_ts.len();
    let input = [
        ColumnarValue::Array(Arc::new(input_ts.into_dict())),
        ColumnarValue::Array(Arc::new(input_value.into_dict())),
    ]
    .into_iter()
    .chain(other_args.into_iter().map(ColumnarValue::Scalar))
    .collect::<Vec<_>>();
    let arg_fields = vec![
        Arc::new(Field::new("a", input[0].data_type(), false)),
        Arc::new(Field::new("b", input[1].data_type(), false)),
    ];
    let return_field = Arc::new(Field::new("c", DataType::Float64, false));
    let args = ScalarFunctionArgs {
        args: input,
        arg_fields,
        number_rows: num_rows,
        return_field,
        config_options: Arc::new(ConfigOptions::default()),
    };
    let value = range_fn.invoke_with_args(args).unwrap();
    let eval_result: Vec<Option<f64>> = extract_array(&value)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .iter()
        .collect();
    assert_eq!(eval_result.len(), expected.len());
    assert!(
        eval_result
            .iter()
            .zip(expected.iter())
            .all(|(x, y)| match (*x, *y) {
                (Some(x), Some(y)) => (x - y).abs() < 0.0001,
                (None, None) => true,
                _ => false,
            })
    );
}

/// Build timestamp range and value range arrays for test.
pub fn build_test_range_arrays(
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

/// Invoke a range UDF and return the raw `ColumnarValue`.
pub fn invoke_range_udf(
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

/// Assert that a `DataFusionError` is an `Execution` variant with the expected message.
pub fn assert_execution_error(error: DataFusionError, expected: &str) {
    match error {
        DataFusionError::Execution(message) => assert_eq!(message, expected),
        other => panic!("expected execution error, got {other:?}"),
    }
}

/// Stale NaN as used by Prometheus.
pub const STALE_NAN: f64 = f64::from_bits(0x7ff0_0000_0000_0002);

/// A tiny PRNG for generating deterministic test data.
pub struct TinyPrng(pub u64);

impl TinyPrng {
    pub fn next_u32(&mut self) -> u32 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 as u32
    }

    pub fn next_index(&mut self, bound: usize) -> usize {
        (self.next_u32() as usize) % bound
    }
}

/// Run the oracle-based differential test: build range arrays, verify raw bits and
/// null validity, invoke the UDF via [`simple_range_udf_runner`], and return the
/// expected values for further assertions.
///
/// `oracle` is the behavior-specific function (e.g. `changes_oracle` or `resets_oracle`)
/// that computes the expected count for a slice of raw f64 values.
pub fn run_oracle_ranges(
    values: Vec<Option<f64>>,
    raw_values: Vec<f64>,
    timestamp_ranges: Vec<(u32, u32)>,
    value_ranges: Vec<(u32, u32)>,
    oracle: fn(&[f64]) -> Option<f64>,
    udf: ScalarUDF,
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
        .map(|(offset, length)| oracle(&raw_values[*offset as usize..(*offset + *length) as usize]))
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
        udf,
        timestamp_ranges,
        value_ranges,
        vec![],
        expected.clone(),
    );
    expected
}
