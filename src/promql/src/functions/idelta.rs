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

use datafusion::arrow::array::{Float64Array, TimestampMillisecondArray};
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{ScalarUDF, Signature, TypeSignature, Volatility};
use datafusion::physical_plan::ColumnarValue;
use datatypes::arrow::array::Array;
use datatypes::arrow::datatypes::DataType;

use crate::error;
use crate::functions::extract_array;
use crate::range_array::RangeArray;

/// The `funcIdelta` in Promql,
/// from <https://github.com/prometheus/prometheus/blob/6bdecf377cea8e856509914f35234e948c4fcb80/promql/functions.go#L235>
#[derive(Debug)]
pub struct IDelta<const IS_RATE: bool> {}

impl<const IS_RATE: bool> IDelta<IS_RATE> {
    pub const fn name() -> &'static str {
        if IS_RATE {
            "prom_irate"
        } else {
            "prom_idelta"
        }
    }

    pub fn scalar_udf() -> ScalarUDF {
        ScalarUDF {
            name: Self::name().to_string(),
            signature: Signature::new(
                TypeSignature::Exact(Self::input_type()),
                Volatility::Immutable,
            ),
            return_type: Arc::new(|_| Ok(Arc::new(Self::return_type()))),
            fun: Arc::new(Self::calc),
        }
    }

    // time index column and value column
    fn input_type() -> Vec<DataType> {
        vec![
            RangeArray::convert_data_type(DataType::Timestamp(TimeUnit::Millisecond, None)),
            RangeArray::convert_data_type(DataType::Float64),
        ]
    }

    fn return_type() -> DataType {
        DataType::Float64
    }

    fn calc(input: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        // construct matrix from input
        assert_eq!(input.len(), 2);
        let ts_array = extract_array(&input[0])?;
        let value_array = extract_array(&input[1])?;

        let ts_range: RangeArray = RangeArray::try_new(ts_array.to_data().into())?;
        let value_range: RangeArray = RangeArray::try_new(value_array.to_data().into())?;
        error::ensure(
            ts_range.len() == value_range.len(),
            DataFusionError::Execution(format!(
                "{}: input arrays should have the same length, found {} and {}",
                Self::name(),
                ts_range.len(),
                value_range.len()
            )),
        )?;
        error::ensure(
            ts_range.value_type() == DataType::Timestamp(TimeUnit::Millisecond, None),
            DataFusionError::Execution(format!(
                "{}: expect TimestampMillisecond as time index array's type, found {}",
                Self::name(),
                ts_range.value_type()
            )),
        )?;
        error::ensure(
            value_range.value_type() == DataType::Float64,
            DataFusionError::Execution(format!(
                "{}: expect Float64 as value array's type, found {}",
                Self::name(),
                value_range.value_type()
            )),
        )?;

        // calculation
        let mut result_array = Vec::with_capacity(ts_range.len());

        for index in 0..ts_range.len() {
            let timestamps = ts_range.get(index).unwrap();
            let timestamps = timestamps
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .values();

            let values = value_range.get(index).unwrap();
            let values = values
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values();
            error::ensure(
                timestamps.len() == values.len(),
                DataFusionError::Execution(format!(
                    "{}: input arrays should have the same length, found {} and {}",
                    Self::name(),
                    timestamps.len(),
                    values.len()
                )),
            )?;

            let len = timestamps.len();
            if len < 2 {
                result_array.push(None);
                continue;
            }

            // if is delta
            if !IS_RATE {
                result_array.push(Some(values[len - 1] - values[len - 2]));
                continue;
            }

            // else is rate
            // TODO(ruihang): "divide 1000" converts the timestamp from millisecond to second.
            //     it should consider other percisions.
            let sampled_interval = (timestamps[len - 1] - timestamps[len - 2]) / 1000;
            let last_value = values[len - 1];
            let prev_value = values[len - 2];
            let result_value = if last_value < prev_value {
                // counter reset
                last_value
            } else {
                last_value - prev_value
            };

            result_array.push(Some(result_value / sampled_interval as f64));
        }

        let result = ColumnarValue::Array(Arc::new(Float64Array::from_iter(result_array)));
        Ok(result)
    }
}

impl<const IS_RATE: bool> Display for IDelta<IS_RATE> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PromQL Idelta Function (is_rate: {IS_RATE})",)
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::functions::test_util::simple_range_udf_runner;

    #[test]
    fn basic_idelta_and_irate() {
        let ts_array = Arc::new(TimestampMillisecondArray::from_iter(
            [1000i64, 3000, 5000, 7000, 9000, 11000, 13000, 15000, 17000]
                .into_iter()
                .map(Some),
        ));
        let ts_ranges = [(0, 2), (0, 5), (1, 1), (3, 3), (8, 1), (9, 0)];

        let values_array = Arc::new(Float64Array::from_iter([
            1.0, 2.0, 3.0, 5.0, 0.0, 6.0, 7.0, 8.0, 9.0,
        ]));
        let values_ranges = [(0, 2), (0, 5), (1, 1), (3, 3), (8, 1), (9, 0)];

        // test idelta
        let ts_range_array = RangeArray::from_ranges(ts_array.clone(), ts_ranges).unwrap();
        let value_range_array =
            RangeArray::from_ranges(values_array.clone(), values_ranges).unwrap();
        simple_range_udf_runner(
            IDelta::<false>::scalar_udf(),
            ts_range_array,
            value_range_array,
            vec![Some(1.0), Some(-5.0), None, Some(6.0), None, None],
        );

        // test irate
        let ts_range_array = RangeArray::from_ranges(ts_array, ts_ranges).unwrap();
        let value_range_array = RangeArray::from_ranges(values_array, values_ranges).unwrap();
        simple_range_udf_runner(
            IDelta::<true>::scalar_udf(),
            ts_range_array,
            value_range_array,
            // the second point represent counter reset
            vec![Some(0.5), Some(0.0), None, Some(3.0), None, None],
        );
    }
}
