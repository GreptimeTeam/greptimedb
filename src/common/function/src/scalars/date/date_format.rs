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
use common_query::error;
use common_time::{Date, Timestamp, Timezone};
use datafusion_common::DataFusionError;
use datafusion_common::arrow::array::{Array, AsArray, StringViewBuilder};
use datafusion_common::arrow::datatypes::{
    ArrowTimestampType, DataType, Date32Type, Date64Type, TimeUnit,
};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature};
use snafu::ResultExt;

use crate::function::{Function, extract_args, find_function_context};
use crate::helper;
use crate::helper::with_match_timestamp_types;

/// A function that formats timestamp/date/datetime into string by the format
#[derive(Clone, Debug)]
pub(crate) struct DateFormatFunction {
    signature: Signature,
}

impl Default for DateFormatFunction {
    fn default() -> Self {
        Self {
            signature: helper::one_of_sigs2(
                vec![
                    DataType::Date32,
                    DataType::Date64,
                    DataType::Timestamp(TimeUnit::Second, None),
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                ],
                vec![DataType::Utf8],
            ),
        }
    }
}

impl Function for DateFormatFunction {
    fn name(&self) -> &str {
        "date_format"
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn signature(&self) -> &Signature {
        &self.signature
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
                let utc_timezone = Timezone::from_tz_string("UTC")
                    .map_err(BoxedError::new)
                    .context(error::ExecuteSnafu)?;
                let left = left.as_primitive::<Date32Type>();
                for i in 0..size {
                    let date = left.is_valid(i).then(|| Date::from(left.value(i)));
                    let format = formats.is_valid(i).then(|| formats.value(i));

                    let result = match (date, format) {
                        (Some(date), Some(fmt)) => date
                            .as_formatted_string(fmt, Some(&utc_timezone))
                            .map_err(BoxedError::new)
                            .context(error::ExecuteSnafu)?,
                        _ => None,
                    };

                    builder.append_option(result.as_deref());
                }
            }
            DataType::Date64 => {
                let left = left.as_primitive::<Date64Type>();
                for i in 0..size {
                    let date = left.is_valid(i).then(|| {
                        let ms = left.value(i);
                        Timestamp::new_millisecond(ms)
                    });
                    let format = formats.is_valid(i).then(|| formats.value(i));

                    let result = match (date, format) {
                        (Some(ts), Some(fmt)) => {
                            Some(ts.as_formatted_string(fmt, Some(timezone)).map_err(|e| {
                                DataFusionError::Execution(format!(
                                    "cannot format {ts:?} as '{fmt}': {e}"
                                ))
                            })?)
                        }
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
    use std::process::Command;
    use std::sync::Arc;

    use arrow_schema::Field;
    use common_time::Timezone;
    use datafusion_common::arrow::array::{
        Date32Array, Date64Array, StringArray, TimestampSecondArray,
    };
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::{TypeSignature, Volatility};
    use session::context::QueryContextBuilder;

    use super::{DateFormatFunction, *};
    use crate::function::FunctionContext;

    const DEFAULT_TIMEZONE_CHILD_ENV: &str = "GREPTIME_DATE_FORMAT_DEFAULT_TIMEZONE_CHILD";

    #[test]
    fn test_date_format_misc() {
        let f = DateFormatFunction::default();
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
                             volatility: Volatility::Immutable,
                             ..
                         } if  sigs.len() == 6));
    }

    #[test]
    fn test_timestamp_date_format() {
        let f = DateFormatFunction::default();

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
    fn test_date64_date_format() {
        let f = DateFormatFunction::default();

        let dates = vec![Some(123000), None, Some(42000), None];
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
                ColumnarValue::Array(Arc::new(Date64Array::from(dates))),
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
        let f = DateFormatFunction::default();

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

    #[test]
    fn test_date32_date_format_ignores_session_timezone() {
        let output = Command::new(std::env::current_exe().unwrap())
            .arg(
                "scalars::date::date_format::tests::test_date32_date_format_ignores_default_timezone_child",
            )
            .arg("--exact")
            .arg("--ignored")
            .env(DEFAULT_TIMEZONE_CHILD_ENV, "1")
            .output()
            .unwrap();

        assert!(
            output.status.success(),
            "child Date32 timezone test failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.contains("1 passed"),
            "child Date32 timezone test did not run\nstdout:\n{stdout}"
        );
    }

    #[test]
    #[ignore = "spawned by test_date32_date_format_ignores_session_timezone"]
    fn test_date32_date_format_ignores_default_timezone_child() {
        assert_eq!(
            Some("1"),
            std::env::var(DEFAULT_TIMEZONE_CHILD_ENV).ok().as_deref()
        );
        common_time::timezone::set_default_timezone(Some("-08:00")).unwrap();

        fn format_with_timezone(
            f: &DateFormatFunction,
            value: ColumnarValue,
            timezone: &str,
            format: &str,
            number_rows: usize,
        ) -> Vec<Option<String>> {
            let query_ctx = QueryContextBuilder::default()
                .timezone(Timezone::from_tz_string(timezone).unwrap())
                .build()
                .into();
            let mut config_options = ConfigOptions::default();
            config_options.extensions.insert(FunctionContext {
                query_ctx,
                ..Default::default()
            });

            let args = ScalarFunctionArgs {
                args: vec![
                    value,
                    ColumnarValue::Array(Arc::new(StringArray::from_iter_values(
                        std::iter::repeat_n(format, number_rows),
                    ))),
                ],
                arg_fields: vec![],
                number_rows,
                return_field: Arc::new(Field::new("x", DataType::Utf8View, false)),
                config_options: Arc::new(config_options),
            };
            let result = f
                .invoke_with_args(args)
                .and_then(|value| value.to_array(number_rows))
                .unwrap();

            result
                .as_string_view()
                .iter()
                .map(|value| value.map(str::to_owned))
                .collect()
        }

        let f = DateFormatFunction::default();

        assert_eq!(
            vec![
                Some("1969-12-31 00:00:00 +0000 UTC".to_string()),
                Some("1970-01-01 00:00:00 +0000 UTC".to_string()),
                Some("1970-01-02 00:00:00 +0000 UTC".to_string()),
            ],
            format_with_timezone(
                &f,
                ColumnarValue::Array(Arc::new(Date32Array::from(vec![
                    Some(-1),
                    Some(0),
                    Some(1)
                ]))),
                "UTC",
                "%Y-%m-%d %T %z %Z",
                3,
            )
        );
        assert_eq!(
            vec![
                Some("1969-12-31 00:00:00 +0000 UTC".to_string()),
                Some("1970-01-01 00:00:00 +0000 UTC".to_string()),
                Some("1970-01-02 00:00:00 +0000 UTC".to_string()),
            ],
            format_with_timezone(
                &f,
                ColumnarValue::Array(Arc::new(Date32Array::from(vec![
                    Some(-1),
                    Some(0),
                    Some(1)
                ]))),
                "-08:00",
                "%Y-%m-%d %T %z %Z",
                3,
            )
        );
        assert_eq!(
            vec![Some("1970-01-01".to_string())],
            format_with_timezone(
                &f,
                ColumnarValue::Array(Arc::new(TimestampSecondArray::from(vec![Some(0)]))),
                "UTC",
                "%Y-%m-%d",
                1,
            )
        );
        assert_eq!(
            vec![Some("1969-12-31".to_string())],
            format_with_timezone(
                &f,
                ColumnarValue::Array(Arc::new(TimestampSecondArray::from(vec![Some(0)]))),
                "-08:00",
                "%Y-%m-%d",
                1,
            )
        );
    }
}
