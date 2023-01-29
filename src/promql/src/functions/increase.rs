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

use std::fmt::Display;
use std::sync::Arc;

use datafusion::arrow::array::Float64Array;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{ScalarUDF, Signature, TypeSignature, Volatility};
use datafusion::physical_plan::ColumnarValue;
use datatypes::arrow::array::{Array, PrimitiveArray};
use datatypes::arrow::datatypes::DataType;

use crate::functions::extract_array;
use crate::range_array::RangeArray;

/// Part of the `extrapolatedRate` in Promql,
/// from https://github.com/prometheus/prometheus/blob/6bdecf377cea8e856509914f35234e948c4fcb80/promql/functions.go#L66
#[derive(Debug)]
pub struct Increase {}

impl Increase {
    pub fn name() -> &'static str {
        "prom_increase"
    }

    fn input_type() -> DataType {
        RangeArray::convert_data_type(DataType::Float64)
    }

    fn return_type() -> DataType {
        DataType::Float64
    }

    fn calc(input: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        // construct matrix from input
        assert_eq!(input.len(), 1);
        let input_array = extract_array(input.first().unwrap())?;
        let array_data = input_array.data().clone();
        let range_array: RangeArray = RangeArray::try_new(array_data.into())?;

        // calculation
        let mut result_array = Vec::with_capacity(range_array.len());
        for index in 0..range_array.len() {
            let range = range_array.get(index).unwrap();
            let range = range
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values();

            if range.len() < 2 {
                result_array.push(0.0);
                continue;
            }

            // refer to functions.go L83-L110
            let mut result_value = range.last().unwrap() - range.first().unwrap();
            for window in range.windows(2) {
                let prev = window[0];
                let curr = window[1];
                if curr < prev {
                    result_value += prev
                }
            }

            result_array.push(result_value);
        }

        let result = ColumnarValue::Array(Arc::new(PrimitiveArray::from_iter(result_array)));
        Ok(result)
    }

    pub fn scalar_udf() -> ScalarUDF {
        ScalarUDF {
            name: Self::name().to_string(),
            signature: Signature::new(
                TypeSignature::Exact(vec![Self::input_type()]),
                Volatility::Immutable,
            ),
            return_type: Arc::new(|_| Ok(Arc::new(Self::return_type()))),
            fun: Arc::new(Self::calc),
        }
    }
}

impl Display for Increase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PromQL Increase Function")
    }
}

#[cfg(test)]
mod test {

    use super::*;

    fn increase_runner(input: RangeArray, expected: Vec<f64>) {
        let input = vec![ColumnarValue::Array(Arc::new(input.into_dict()))];
        let output = extract_array(&Increase::calc(&input).unwrap())
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(output, expected);
    }

    #[test]
    fn abnormal_input() {
        let values_array = Arc::new(Float64Array::from_iter([
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0,
        ]));
        let ranges = [(0, 2), (0, 5), (1, 1), (3, 3), (8, 1), (9, 0)];
        let range_array = RangeArray::from_ranges(values_array, ranges).unwrap();
        increase_runner(range_array, vec![1.0, 4.0, 0.0, 2.0, 0.0, 0.0]);
    }

    #[test]
    fn normal_input() {
        let values_array = Arc::new(Float64Array::from_iter([
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0,
        ]));
        let ranges = [
            (0, 2),
            (1, 2),
            (2, 2),
            (3, 2),
            (4, 2),
            (5, 2),
            (6, 2),
            (7, 2),
        ];
        let range_array = RangeArray::from_ranges(values_array, ranges).unwrap();
        increase_runner(range_array, vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]);
    }

    #[test]
    fn short_input() {
        let values_array = Arc::new(Float64Array::from_iter([
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0,
        ]));
        let ranges = [
            (0, 1),
            (1, 0),
            (2, 1),
            (3, 0),
            (4, 3),
            (5, 1),
            (6, 0),
            (7, 2),
        ];
        let range_array = RangeArray::from_ranges(values_array, ranges).unwrap();
        increase_runner(range_array, vec![0.0, 0.0, 0.0, 0.0, 2.0, 0.0, 0.0, 1.0]);
    }

    #[test]
    fn counter_reset() {
        // this series should be treated [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
        let values_array = Arc::new(Float64Array::from_iter([
            1.0, 2.0, 3.0, 4.0, 1.0, 2.0, 3.0, 4.0, 5.0,
        ]));
        let ranges = [
            (0, 2),
            (1, 2),
            (2, 2),
            (3, 2),
            (4, 2),
            (5, 2),
            (6, 2),
            (7, 2),
        ];
        let range_array = RangeArray::from_ranges(values_array, ranges).unwrap();
        increase_runner(range_array, vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]);
    }
}
