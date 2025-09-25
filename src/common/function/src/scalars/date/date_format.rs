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

use common_error::ext::BoxedError;
use common_query::error::{self, Result};
use common_time::{Date, Timestamp};
use datafusion_common::DataFusionError;
use datafusion_common::arrow::array::{Array, AsArray, StringViewBuilder};
use datafusion_common::arrow::datatypes::{ArrowTimestampType, DataType, Date32Type, TimeUnit};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature};
use snafu::ResultExt;

use crate::function::{Function, extract_args, find_function_context};
use crate::helper;
use crate::helper::with_match_timestamp_types;

/// A function that formats timestamp/date/datetime into string by the format
#[derive(Clone, Debug, Default)]
pub struct DateFormatFunction;

const NAME: &str = "date_format";

impl Function for DateFormatFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn signature(&self) -> Signature {
        helper::one_of_sigs2(
            vec![
                DataType::Date32,
                DataType::Timestamp(TimeUnit::Second, None),
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ],
            vec![DataType::Utf8],
        )
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let ctx = find_function_context(&args)?;
        let timezone = &ctx.query_ctx.timezone();

        let [left, arg1] = extract_args(self.name(), &args)?;
        let formats = arg1.as_string::<i32>();

        let size = left.len();
        let left_datatype = left.data_type();
        let mut builder = StringViewBuilder::with_capacity(size);

        match left_datatype {
            DataType::Timestamp(_, _) => {
                with_match_timestamp_types!(left_datatype, |$S| {
                    let array = left.as_primitive::<$S>();
                    for (date, format) in array.iter().zip(formats.iter()) {
                        let result = match (date, format) {
                            (Some(date), Some(format)) => {
                                let ts = Timestamp::new(date, $S::UNIT.into());
                                let x = ts.as_formatted_string(&format, Some(timezone))
                                    .map_err(|e| DataFusionError::Execution(format!(
                                        "cannot format {ts:?} as '{format}': {e}"
                                    )))?;
                                Some(x)
                            }
                            _ => None
                        };
                        builder.append_option(result.as_deref());
                    }
                })?;
            }
            DataType::Date32 => {
                let left = left.as_primitive::<Date32Type>();
                for i in 0..size {
                    let date = left.is_valid(i).then(|| Date::from(left.value(i)));
                    let format = formats.is_valid(i).then(|| formats.value(i));

                    let result = match (date, format) {
                        (Some(date), Some(fmt)) => date
                            .as_formatted_string(fmt, Some(timezone))
                            .map_err(BoxedError::new)
                            .context(error::ExecuteSnafu)?,
                        _ => None,
                    };

                    builder.append_option(result.as_deref());
                }
            }
            x => {
                return Err(DataFusionError::Execution(format!(
                    "unsupported input data type {x}"
                )));
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

impl fmt::Display for DateFormatFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DATE_FORMAT")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use datafusion_common::arrow::array::{Date32Array, StringArray, TimestampSecondArray};
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::{TypeSignature, Volatility};

    use super::{DateFormatFunction, *};
    use crate::function::FunctionContext;

    #[test]
    fn test_date_format_misc() {
        let f = DateFormatFunction;
        assert_eq!("date_format", f.name());
        assert_eq!(
            DataType::Utf8View,
            f.return_type(&[DataType::Timestamp(TimeUnit::Microsecond, None)])
                .unwrap()
        );
        assert_eq!(
            DataType::Utf8View,
            f.return_type(&[DataType::Timestamp(TimeUnit::Second, None)])
                .unwrap()
        );
        assert_eq!(
            DataType::Utf8View,
            f.return_type(&[DataType::Date32]).unwrap()
        );
        assert!(matches!(f.signature(),
                         Signature {
                             type_signature: TypeSignature::OneOf(sigs),
                             volatility: Volatility::Immutable
                         } if  sigs.len() == 5));
    }

    #[test]
    fn test_timestamp_date_format() {
        let f = DateFormatFunction;

        let times = vec![Some(123), None, Some(42), None];
        let formats = vec![
            "%Y-%m-%d %T.%3f",
            "%Y-%m-%d %T.%3f",
            "%Y-%m-%d %T.%3f",
            "%Y-%m-%d %T.%3f",
        ];
        let results = [
            Some("1970-01-01 00:02:03.000"),
            None,
            Some("1970-01-01 00:00:42.000"),
            None,
        ];

        let mut config_options = ConfigOptions::default();
        config_options.extensions.insert(FunctionContext::default());
        let config_options = Arc::new(config_options);

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(TimestampSecondArray::from(times))),
                ColumnarValue::Array(Arc::new(StringArray::from_iter_values(formats))),
            ],
            arg_fields: vec![],
            number_rows: 4,
            return_field: Arc::new(Field::new("x", DataType::Utf8View, false)),
            config_options,
        };
        let result = f
            .invoke_with_args(args)
            .and_then(|x| x.to_array(4))
            .unwrap();
        let vector = result.as_string_view();

        assert_eq!(4, vector.len());
        for (actual, expect) in vector.iter().zip(results) {
            assert_eq!(actual, expect);
        }
    }

    #[test]
    fn test_date_date_format() {
        let f = DateFormatFunction;

        let dates = vec![Some(123), None, Some(42), None];
        let formats = vec![
            "%Y-%m-%d %T.%3f",
            "%Y-%m-%d %T.%3f",
            "%Y-%m-%d %T.%3f",
            "%Y-%m-%d %T.%3f",
        ];
        let results = [
            Some("1970-05-04 00:00:00.000"),
            None,
            Some("1970-02-12 00:00:00.000"),
            None,
        ];

        let mut config_options = ConfigOptions::default();
        config_options.extensions.insert(FunctionContext::default());
        let config_options = Arc::new(config_options);

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Date32Array::from(dates))),
                ColumnarValue::Array(Arc::new(StringArray::from_iter_values(formats))),
            ],
            arg_fields: vec![],
            number_rows: 4,
            return_field: Arc::new(Field::new("x", DataType::Utf8View, false)),
            config_options,
        };
        let result = f
            .invoke_with_args(args)
            .and_then(|x| x.to_array(4))
            .unwrap();
        let vector = result.as_string_view();

        assert_eq!(4, vector.len());
        for (actual, expect) in vector.iter().zip(results) {
            assert_eq!(actual, expect);
        }
    }
}
