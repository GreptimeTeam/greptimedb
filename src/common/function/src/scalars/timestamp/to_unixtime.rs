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

use std::fmt;
use std::sync::Arc;

use common_query::error::Result;
use common_time::{Date, Timestamp};
use datafusion_common::DataFusionError;
use datafusion_common::arrow::array::{
    Array, ArrayRef, AsArray, Date32Array, Int64Array, Int64Builder,
};
use datafusion_common::arrow::compute;
use datafusion_common::arrow::datatypes::{ArrowTimestampType, DataType, Date32Type, TimeUnit};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};

use crate::function::{Function, FunctionContext, extract_args, find_function_context};
use crate::helper::with_match_timestamp_types;

/// A function to convert the column into the unix timestamp in seconds.
#[derive(Clone, Debug, Default)]
pub struct ToUnixtimeFunction;

const NAME: &str = "to_unixtime";

fn convert_to_seconds(arg: &str, func_ctx: &FunctionContext) -> Option<i64> {
    let timezone = &func_ctx.query_ctx.timezone();
    if let Ok(ts) = Timestamp::from_str(arg, Some(timezone)) {
        return Some(ts.split().0);
    }

    if let Ok(date) = Date::from_str(arg, Some(timezone)) {
        return Some(date.to_secs());
    }

    None
}

fn convert_timestamps_to_seconds(array: &ArrayRef) -> datafusion_common::Result<Vec<Option<i64>>> {
    with_match_timestamp_types!(array.data_type(), |$S| {
        let array = array.as_primitive::<$S>();
        array
            .iter()
            .map(|x| x.map(|i| Timestamp::new(i, $S::UNIT.into()).split().0))
            .collect::<Vec<_>>()
    })
}

fn convert_dates_to_seconds(vector: &Date32Array) -> Vec<Option<i64>> {
    (0..vector.len())
        .map(|i| {
            vector
                .is_valid(i)
                .then(|| Date::from(vector.value(i)).to_secs())
        })
        .collect::<Vec<Option<i64>>>()
}

impl Function for ToUnixtimeFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn signature(&self) -> Signature {
        Signature::uniform(
            1,
            vec![
                DataType::Utf8,
                DataType::Int32,
                DataType::Int64,
                DataType::Date32,
                DataType::Timestamp(TimeUnit::Second, None),
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ],
            Volatility::Immutable,
        )
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let ctx = find_function_context(&args)?;
        let [arg0] = extract_args(self.name(), &args)?;
        let result: ArrayRef = match arg0.data_type() {
            DataType::Utf8 => {
                let arg0 = arg0.as_string::<i32>();
                let mut builder = Int64Builder::with_capacity(arg0.len());
                for i in 0..arg0.len() {
                    builder.append_option(
                        arg0.is_valid(i)
                            .then(|| convert_to_seconds(arg0.value(i), ctx))
                            .flatten(),
                    );
                }
                Arc::new(builder.finish())
            }
            DataType::Int64 | DataType::Int32 => compute::cast(&arg0, &DataType::Int64)?,
            DataType::Date32 => {
                let vector = arg0.as_primitive::<Date32Type>();
                let seconds = convert_dates_to_seconds(vector);
                Arc::new(Int64Array::from(seconds))
            }
            DataType::Timestamp(_, _) => {
                let seconds = convert_timestamps_to_seconds(&arg0)?;
                Arc::new(Int64Array::from(seconds))
            }
            x => {
                return Err(DataFusionError::Execution(format!(
                    "{}: unsupported input data type {x}",
                    self.name()
                )));
            }
        };
        Ok(ColumnarValue::Array(result))
    }
}

impl fmt::Display for ToUnixtimeFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TO_UNIXTIME")
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::Field;
    use datafusion_common::arrow::array::{
        StringArray, TimestampMillisecondArray, TimestampSecondArray,
    };
    use datafusion_common::arrow::datatypes::Int64Type;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::TypeSignature;

    use super::{ToUnixtimeFunction, *};

    fn test_invoke(arg0: ArrayRef, expects: &[Option<i64>]) {
        let mut config_options = ConfigOptions::default();
        config_options.extensions.insert(FunctionContext::default());
        let config_options = Arc::new(config_options);

        let number_rows = arg0.len();
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(arg0)],
            arg_fields: vec![],
            number_rows,
            return_field: Arc::new(Field::new("", DataType::Int64, true)),
            config_options,
        };
        let result = ToUnixtimeFunction
            .invoke_with_args(args)
            .and_then(|x| x.to_array(number_rows))
            .unwrap();
        let result = result.as_primitive::<Int64Type>();

        assert_eq!(result.len(), expects.len());
        for (actual, expect) in result.iter().zip(expects) {
            assert_eq!(&actual, expect);
        }
    }

    #[test]
    fn test_string_to_unixtime() {
        let f = ToUnixtimeFunction;
        assert_eq!("to_unixtime", f.name());
        assert_eq!(DataType::Int64, f.return_type(&[]).unwrap());

        assert!(matches!(f.signature(),
                         Signature {
                             type_signature: TypeSignature::Uniform(1, valid_types),
                             volatility: Volatility::Immutable
                         } if  valid_types == vec![
                             DataType::Utf8,
                             DataType::Int32,
                             DataType::Int64,
                             DataType::Date32,
                             DataType::Timestamp(TimeUnit::Second, None),
                             DataType::Timestamp(TimeUnit::Millisecond, None),
                             DataType::Timestamp(TimeUnit::Microsecond, None),
                             DataType::Timestamp(TimeUnit::Nanosecond, None),
                         ]
        ));

        let times = vec![
            Some("2023-03-01T06:35:02Z"),
            None,
            Some("2022-06-30T23:59:60Z"),
            Some("invalid_time_stamp"),
        ];
        let results = [Some(1677652502), None, Some(1656633600), None];
        let arg0 = Arc::new(StringArray::from(times));
        test_invoke(arg0, &results);
    }

    #[test]
    fn test_int_to_unixtime() {
        let times = vec![Some(3_i64), None, Some(5_i64), None];
        let results = [Some(3), None, Some(5), None];
        let arg0 = Arc::new(Int64Array::from(times));
        test_invoke(arg0, &results);
    }

    #[test]
    fn test_date_to_unixtime() {
        let times = vec![Some(123), None, Some(42), None];
        let results = [Some(10627200), None, Some(3628800), None];
        let arg0 = Arc::new(Date32Array::from(times));
        test_invoke(arg0, &results);
    }

    #[test]
    fn test_timestamp_to_unixtime() {
        let times = vec![Some(123), None, Some(42), None];
        let results = [Some(123), None, Some(42), None];
        let arg0 = Arc::new(TimestampSecondArray::from(times));
        test_invoke(arg0, &results);

        let times = vec![Some(123000), None, Some(42000), None];
        let results = [Some(123), None, Some(42), None];
        let arg0 = Arc::new(TimestampMillisecondArray::from(times));
        test_invoke(arg0, &results);
    }
}
