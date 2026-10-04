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

use std::str::FromStr;

use api::v1::region::{StrictWindow, compact_request};
use arrow::datatypes::DataType as ArrowDataType;
use common_error::ext::BoxedError;
use common_macro::admin_fn;
use common_query::error::{
    InvalidFuncArgsSnafu, MissingTableMutationHandlerSnafu, Result, TableMutationSnafu,
    UnsupportedInputDataTypeSnafu,
};
use common_telemetry::info;
use common_time::range::TimestampRange;
use common_time::{Timestamp, Timezone};
use datafusion_expr::{Signature, Volatility};
use datatypes::prelude::*;
use session::context::QueryContextRef;
use session::table_name::table_name_to_full_name;
use snafu::{ResultExt, ensure};
use table::requests::{CompactTableRequest, FlushTableRequest};

use crate::handlers::TableMutationHandlerRef;

/// Compact type: strict window.
const COMPACT_TYPE_STRICT_WINDOW: &str = "strict_window";
/// Compact type: strict window (short name).
const COMPACT_TYPE_STRICT_WINDOW_SHORT: &str = "swcs";

const DEFAULT_COMPACTION_PARALLELISM: u32 = 1;

#[admin_fn(
    name = FlushTableFunction,
    display_name = flush_table,
    sig_fn = flush_signature,
    ret = uint64
)]
pub(crate) async fn flush_table(
    table_mutation_handler: &TableMutationHandlerRef,
    query_ctx: &QueryContextRef,
    params: &[ValueRef<'_>],
) -> Result<Value> {
    ensure!(
        params.len() == 1,
        InvalidFuncArgsSnafu {
            err_msg: format!(
                "The length of the args is not correct, expect 1, have: {}",
                params.len()
            ),
        }
    );

    let ValueRef::String(table_name) = params[0] else {
        return UnsupportedInputDataTypeSnafu {
            function: "flush_table",
            datatypes: params.iter().map(|v| v.data_type()).collect::<Vec<_>>(),
        }
        .fail();
    };

    let (catalog_name, schema_name, table_name) = table_name_to_full_name(table_name, query_ctx)
        .map_err(BoxedError::new)
        .context(TableMutationSnafu)?;

    let affected_rows = table_mutation_handler
        .flush(
            FlushTableRequest {
                catalog_name,
                schema_name,
                table_name,
            },
            query_ctx.clone(),
        )
        .await?;

    Ok(Value::from(affected_rows as u64))
}

#[admin_fn(
    name = CompactTableFunction,
    display_name = compact_table,
    sig_fn = compact_signature,
    ret = uint64
)]
pub(crate) async fn compact_table(
    table_mutation_handler: &TableMutationHandlerRef,
    query_ctx: &QueryContextRef,
    params: &[ValueRef<'_>],
) -> Result<Value> {
    let request = parse_compact_request(params, query_ctx)?;
    info!("Compact table request: {:?}", request);

    let affected_rows = table_mutation_handler
        .compact(request, query_ctx.clone())
        .await?;

    Ok(Value::from(affected_rows as u64))
}

fn flush_signature() -> Signature {
    Signature::uniform(1, vec![ArrowDataType::Utf8], Volatility::Immutable)
}

fn compact_signature() -> Signature {
    Signature::variadic(vec![ArrowDataType::Utf8], Volatility::Immutable)
}

/// Parses `compact_table` UDF parameters. This function accepts following combinations:
/// - `[<table_name>]`: only tables name provided, using default compaction type: regular
/// - `[<table_name>, <type>]`: specify table name and compaction type. The compaction options will be default.
/// - `[<table_name>, <type>, <options>]`: provides both type and type-specific options.
///   - For `twcs`, it accepts `parallelism=[N]` where N is an unsigned 32 bits number
///   - For `swcs`, it accepts two numeric parameter: `parallelism` and `window`.
///   - Both types accept `start_time` and `end_time` to constrain compaction windows.
fn parse_compact_request(
    params: &[ValueRef<'_>],
    query_ctx: &QueryContextRef,
) -> Result<CompactTableRequest> {
    ensure!(
        !params.is_empty() && params.len() <= 3,
        InvalidFuncArgsSnafu {
            err_msg: format!(
                "The length of the args is not correct, expect 1-3, have: {}",
                params.len()
            ),
        }
    );

    let timezone = query_ctx.timezone();
    let (table_name, compact_type, parallelism, time_range) = match params {
        // 1. Only table name, strategy defaults to twcs and default parallelism.
        [ValueRef::String(table_name)] => (
            table_name,
            compact_request::Options::Regular(Default::default()),
            DEFAULT_COMPACTION_PARALLELISM,
            None,
        ),
        // 2. Both table name and strategy are provided.
        [
            ValueRef::String(table_name),
            ValueRef::String(compact_ty_str),
        ] => {
            let (compact_type, parallelism, time_range) =
                parse_compact_options(compact_ty_str, None, &timezone)?;
            (table_name, compact_type, parallelism, time_range)
        }
        // 3. Table name, strategy and strategy specific options
        [
            ValueRef::String(table_name),
            ValueRef::String(compact_ty_str),
            ValueRef::String(options_str),
        ] => {
            let (compact_type, parallelism, time_range) =
                parse_compact_options(compact_ty_str, Some(options_str), &timezone)?;
            (table_name, compact_type, parallelism, time_range)
        }
        _ => {
            return UnsupportedInputDataTypeSnafu {
                function: "compact_table",
                datatypes: params.iter().map(|v| v.data_type()).collect::<Vec<_>>(),
            }
            .fail();
        }
    };

    let (catalog_name, schema_name, table_name) = table_name_to_full_name(table_name, query_ctx)
        .map_err(BoxedError::new)
        .context(TableMutationSnafu)?;

    Ok(CompactTableRequest {
        catalog_name,
        schema_name,
        table_name,
        compact_options: compact_type,
        parallelism,
        time_range,
    })
}

/// Parses compaction strategy type. For `strict_window` or `swcs` strict window compaction is chosen,
/// otherwise choose regular (TWCS) compaction.
fn parse_compact_options(
    type_str: &str,
    option: Option<&str>,
    timezone: &Timezone,
) -> Result<(compact_request::Options, u32, Option<TimestampRange>)> {
    let strict_window = type_str.eq_ignore_ascii_case(COMPACT_TYPE_STRICT_WINDOW)
        || type_str.eq_ignore_ascii_case(COMPACT_TYPE_STRICT_WINDOW_SHORT);
    let Some(option_str) = option else {
        let options = if strict_window {
            compact_request::Options::StrictWindow(StrictWindow { window_seconds: 0 })
        } else {
            compact_request::Options::Regular(Default::default())
        };
        return Ok((options, DEFAULT_COMPACTION_PARALLELISM, None));
    };

    // For compatibility, strict-window compaction accepts a single number as window size.
    if strict_window && let Ok(window_seconds) = i64::from_str(option_str) {
        return Ok((
            compact_request::Options::StrictWindow(StrictWindow { window_seconds }),
            DEFAULT_COMPACTION_PARALLELISM,
            None,
        ));
    }

    let mut window_seconds = 0i64;
    let mut parallelism = DEFAULT_COMPACTION_PARALLELISM;
    let mut start_time = None;
    let mut end_time = None;

    for pair in option_str.split(',') {
        let Some((key, value)) = pair.trim().split_once('=') else {
            return InvalidFuncArgsSnafu {
                err_msg: format!("Invalid key-value pair: {}", pair.trim()),
            }
            .fail();
        };
        let key = key.trim();
        let value = value.trim();

        match key {
            "window" | "window_seconds" if strict_window => {
                window_seconds = i64::from_str(value).map_err(|_| {
                    InvalidFuncArgsSnafu {
                        err_msg: format!("Invalid value for window: {}", value),
                    }
                    .build()
                })?;
            }
            "parallelism" => {
                parallelism = value.parse::<u32>().map_err(|_| {
                    InvalidFuncArgsSnafu {
                        err_msg: format!("Invalid value for parallelism: {}", value),
                    }
                    .build()
                })?;
            }
            "start_time" => {
                start_time = Some(Timestamp::from_str(value, Some(timezone)).map_err(|_| {
                    InvalidFuncArgsSnafu {
                        err_msg: format!("Invalid value for start_time: {}", value),
                    }
                    .build()
                })?);
            }
            "end_time" => {
                end_time = Some(Timestamp::from_str(value, Some(timezone)).map_err(|_| {
                    InvalidFuncArgsSnafu {
                        err_msg: format!("Invalid value for end_time: {}", value),
                    }
                    .build()
                })?);
            }
            _ => {
                return InvalidFuncArgsSnafu {
                    err_msg: format!("Unknown parameter: {}", key),
                }
                .fail();
            }
        }
    }

    let time_range = match (start_time, end_time) {
        (None, None) => None,
        (Some(start), Some(end)) if start < end => {
            Some(TimestampRange::new(start, end).ok_or_else(|| {
                InvalidFuncArgsSnafu {
                    err_msg: "invalid compaction time range".to_string(),
                }
                .build()
            })?)
        }
        (Some(_), Some(_)) => {
            return InvalidFuncArgsSnafu {
                err_msg: "start_time must be earlier than end_time".to_string(),
            }
            .fail();
        }
        _ => {
            return InvalidFuncArgsSnafu {
                err_msg: "start_time and end_time must be specified together".to_string(),
            }
            .fail();
        }
    };

    let options = if strict_window {
        compact_request::Options::StrictWindow(StrictWindow { window_seconds })
    } else {
        compact_request::Options::Regular(Default::default())
    };
    Ok((options, parallelism, time_range))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::region::compact_request::Options;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_time::Timestamp;
    use common_time::range::TimestampRange;
    use datafusion_expr::ColumnarValue;
    use session::context::QueryContext;

    use super::*;
    use crate::function::FunctionContext;
    use crate::function_factory::ScalarFunctionFactory;

    macro_rules! define_table_function_test {
        ($name: ident, $func: ident) => {
            paste::paste!{
                #[test]
                fn [<test_ $name _misc>]() {
                    let factory: ScalarFunctionFactory = $func::factory().into();
                    let f = factory.provide(FunctionContext::mock());
                    assert_eq!(stringify!($name), f.name());
                    assert_eq!(
                        DataType::UInt64,
                        f.return_type(&[]).unwrap()
                    );
                    assert!(matches!(f.signature(),
                                     datafusion_expr::Signature {
                                         type_signature: datafusion_expr::TypeSignature::Uniform(1, valid_types),
                                         volatility: datafusion_expr::Volatility::Immutable,
                                         ..
                                     } if valid_types == &vec![ArrowDataType::Utf8]));
                }

                #[tokio::test]
                async fn [<test_ $name _missing_table_mutation>]() {
                    let factory: ScalarFunctionFactory = $func::factory().into();
                    let provider = factory.provide(FunctionContext::default());
                    let f = provider.as_async().unwrap();

                    let func_args = datafusion::logical_expr::ScalarFunctionArgs {
                        args: vec![
                            ColumnarValue::Array(Arc::new(StringArray::from(vec!["test"]))),
                        ],
                        arg_fields: vec![
                            Arc::new(Field::new("arg_0", DataType::Utf8, false)),
                        ],
                        return_field: Arc::new(Field::new("result", DataType::UInt64, true)),
                        number_rows: 1,
                        config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
                    };
                    let result = f.invoke_async_with_args(func_args).await.unwrap_err();
                    assert_eq!(
                        "Execution error: Missing TableMutationHandler, not expected",
                        result.to_string()
                    );
                }

                #[tokio::test]
                async fn [<test_ $name>]() {
                    let factory: ScalarFunctionFactory = $func::factory().into();
                    let provider = factory.provide(FunctionContext::mock());
                    let f = provider.as_async().unwrap();

                    let func_args = datafusion::logical_expr::ScalarFunctionArgs {
                        args: vec![
                            ColumnarValue::Array(Arc::new(StringArray::from(vec!["test"]))),
                        ],
                        arg_fields: vec![
                            Arc::new(Field::new("arg_0", DataType::Utf8, false)),
                        ],
                        return_field: Arc::new(Field::new("result", DataType::UInt64, true)),
                        number_rows: 1,
                        config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
                    };
                    let result = f.invoke_async_with_args(func_args).await.unwrap();

                    match result {
                        ColumnarValue::Array(array) => {
                            let result_array = array.as_any().downcast_ref::<arrow::array::UInt64Array>().unwrap();
                            assert_eq!(result_array.value(0), 42u64);
                        }
                        ColumnarValue::Scalar(scalar) => {
                            assert_eq!(scalar, datafusion_common::ScalarValue::UInt64(Some(42)));
                        }
                    }
                }
            }
        }
    }

    define_table_function_test!(flush_table, FlushTableFunction);

    fn check_parse_compact_params(cases: &[(&[&str], CompactTableRequest)]) {
        for (params, expected) in cases {
            let params = params
                .iter()
                .map(|s| ValueRef::String(s))
                .collect::<Vec<_>>();

            assert_eq!(
                expected,
                &parse_compact_request(&params, &QueryContext::arc()).unwrap()
            );
        }
    }

    #[test]
    fn test_parse_compact_time_range() {
        let params = [
            "table",
            "regular",
            "start_time=2026-01-01T00:00:00Z,end_time=2026-02-01T00:00:00Z",
        ]
        .into_iter()
        .map(ValueRef::String)
        .collect::<Vec<_>>();

        let request = parse_compact_request(&params, &QueryContext::arc()).unwrap();
        assert_eq!(
            Some(
                TimestampRange::new(
                    Timestamp::from_str_utc("2026-01-01T00:00:00Z").unwrap(),
                    Timestamp::from_str_utc("2026-02-01T00:00:00Z").unwrap(),
                )
                .unwrap()
            ),
            request.time_range
        );

        let query_ctx = QueryContext::arc();
        query_ctx.set_timezone(Timezone::from_tz_string("Asia/Shanghai").unwrap());
        let params = [
            "table",
            "regular",
            "start_time=2026-01-01T00:00:00,end_time=2026-02-01T00:00:00",
        ]
        .into_iter()
        .map(ValueRef::String)
        .collect::<Vec<_>>();
        let request = parse_compact_request(&params, &query_ctx).unwrap();
        assert_eq!(
            Some(
                TimestampRange::new(
                    Timestamp::from_str_utc("2025-12-31T16:00:00Z").unwrap(),
                    Timestamp::from_str_utc("2026-01-31T16:00:00Z").unwrap(),
                )
                .unwrap()
            ),
            request.time_range
        );
    }

    #[test]
    fn test_parse_strict_window_compact_time_range() {
        let params = [
            "table",
            "strict_window",
            "window=3600,parallelism=2,start_time=2026-01-01T00:00:00Z,end_time=2026-02-01T00:00:00Z",
        ]
        .into_iter()
        .map(ValueRef::String)
        .collect::<Vec<_>>();

        let request = parse_compact_request(&params, &QueryContext::arc()).unwrap();
        assert_eq!(
            Options::StrictWindow(StrictWindow {
                window_seconds: 3600,
            }),
            request.compact_options
        );
        assert_eq!(2, request.parallelism);
        assert!(request.time_range.is_some());
    }

    #[test]
    fn test_parse_compact_time_range_requires_valid_bounds() {
        for options in [
            "start_time=2026-01-01T00:00:00Z",
            "end_time=2026-02-01T00:00:00Z",
            "start_time=2026-02-01T00:00:00Z,end_time=2026-01-01T00:00:00Z",
            "start_time=2026-01-01T00:00:00Z,end_time=2026-01-01T00:00:00Z",
        ] {
            let params = ["table", "regular", options]
                .into_iter()
                .map(ValueRef::String)
                .collect::<Vec<_>>();
            assert!(parse_compact_request(&params, &QueryContext::arc()).is_err());
        }
    }

    #[test]
    fn test_parse_compact_params() {
        check_parse_compact_params(&[
            (
                &["table"],
                CompactTableRequest {
                    catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                    schema_name: DEFAULT_SCHEMA_NAME.to_string(),
                    table_name: "table".to_string(),
                    compact_options: Options::Regular(Default::default()),
                    parallelism: 1,
                    time_range: None,
                },
            ),
            (
                &[&format!("{}.table", DEFAULT_SCHEMA_NAME)],
                CompactTableRequest {
                    catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                    schema_name: DEFAULT_SCHEMA_NAME.to_string(),
                    table_name: "table".to_string(),
                    compact_options: Options::Regular(Default::default()),
                    parallelism: 1,
                    time_range: None,
                },
            ),
            (
                &[&format!(
                    "{}.{}.table",
                    DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME
                )],
                CompactTableRequest {
                    catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                    schema_name: DEFAULT_SCHEMA_NAME.to_string(),
                    table_name: "table".to_string(),
                    compact_options: Options::Regular(Default::default()),
                    parallelism: 1,
                    time_range: None,
                },
            ),
            (
                &["table", "regular"],
                CompactTableRequest {
                    catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                    schema_name: DEFAULT_SCHEMA_NAME.to_string(),
                    table_name: "table".to_string(),
                    compact_options: Options::Regular(Default::default()),
                    parallelism: 1,
                    time_range: None,
                },
            ),
            (
                &["table", "strict_window"],
                CompactTableRequest {
                    catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                    schema_name: DEFAULT_SCHEMA_NAME.to_string(),
                    table_name: "table".to_string(),
                    compact_options: Options::StrictWindow(StrictWindow { window_seconds: 0 }),
                    parallelism: 1,
                    time_range: None,
                },
            ),
            (
                &["table", "strict_window", "3600"],
                CompactTableRequest {
                    catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                    schema_name: DEFAULT_SCHEMA_NAME.to_string(),
                    table_name: "table".to_string(),
                    compact_options: Options::StrictWindow(StrictWindow {
                        window_seconds: 3600,
                    }),
                    parallelism: 1,
                    time_range: None,
                },
            ),
            (
                &["table", "swcs", "120"],
                CompactTableRequest {
                    catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                    schema_name: DEFAULT_SCHEMA_NAME.to_string(),
                    table_name: "table".to_string(),
                    compact_options: Options::StrictWindow(StrictWindow {
                        window_seconds: 120,
                    }),
                    parallelism: 1,
                    time_range: None,
                },
            ),
            // Test with parallelism parameter
            (
                &["table", "regular", "parallelism=4"],
                CompactTableRequest {
                    catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                    schema_name: DEFAULT_SCHEMA_NAME.to_string(),
                    table_name: "table".to_string(),
                    compact_options: Options::Regular(Default::default()),
                    parallelism: 4,
                    time_range: None,
                },
            ),
            (
                &["table", "strict_window", "window=3600,parallelism=2"],
                CompactTableRequest {
                    catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                    schema_name: DEFAULT_SCHEMA_NAME.to_string(),
                    table_name: "table".to_string(),
                    compact_options: Options::StrictWindow(StrictWindow {
                        window_seconds: 3600,
                    }),
                    parallelism: 2,
                    time_range: None,
                },
            ),
            (
                &["table", "strict_window", "window=3600"],
                CompactTableRequest {
                    catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                    schema_name: DEFAULT_SCHEMA_NAME.to_string(),
                    table_name: "table".to_string(),
                    compact_options: Options::StrictWindow(StrictWindow {
                        window_seconds: 3600,
                    }),
                    parallelism: 1,
                    time_range: None,
                },
            ),
            (
                &["table", "strict_window", "window_seconds=7200"],
                CompactTableRequest {
                    catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                    schema_name: DEFAULT_SCHEMA_NAME.to_string(),
                    table_name: "table".to_string(),
                    compact_options: Options::StrictWindow(StrictWindow {
                        window_seconds: 7200,
                    }),
                    parallelism: 1,
                    time_range: None,
                },
            ),
            (
                &["table", "strict_window", "window=1800"],
                CompactTableRequest {
                    catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                    schema_name: DEFAULT_SCHEMA_NAME.to_string(),
                    table_name: "table".to_string(),
                    compact_options: Options::StrictWindow(StrictWindow {
                        window_seconds: 1800,
                    }),
                    parallelism: 1,
                    time_range: None,
                },
            ),
            (
                &["table", "regular", "parallelism=8"],
                CompactTableRequest {
                    catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                    schema_name: DEFAULT_SCHEMA_NAME.to_string(),
                    table_name: "table".to_string(),
                    compact_options: Options::Regular(Default::default()),
                    parallelism: 8,
                    time_range: None,
                },
            ),
        ]);

        assert!(
            parse_compact_request(
                &["table", "strict_window", "abc"]
                    .into_iter()
                    .map(ValueRef::String)
                    .collect::<Vec<_>>(),
                &QueryContext::arc(),
            )
            .is_err()
        );

        assert!(
            parse_compact_request(
                &["a.b.table", "strict_window", "abc"]
                    .into_iter()
                    .map(ValueRef::String)
                    .collect::<Vec<_>>(),
                &QueryContext::arc(),
            )
            .is_err()
        );

        // Test invalid parallelism
        assert!(
            parse_compact_request(
                &["table", "regular", "options", "invalid"]
                    .into_iter()
                    .map(ValueRef::String)
                    .collect::<Vec<_>>(),
                &QueryContext::arc(),
            )
            .is_err()
        );

        // Test too many parameters
        assert!(
            parse_compact_request(
                &["table", "regular", "options", "4", "extra"]
                    .into_iter()
                    .map(ValueRef::String)
                    .collect::<Vec<_>>(),
                &QueryContext::arc(),
            )
            .is_err()
        );

        // Test invalid keyword argument format
        assert!(
            parse_compact_request(
                &["table", "strict_window", "window"]
                    .into_iter()
                    .map(ValueRef::String)
                    .collect::<Vec<_>>(),
                &QueryContext::arc(),
            )
            .is_err()
        );

        // Test invalid keyword
        assert!(
            parse_compact_request(
                &["table", "strict_window", "invalid_key=123"]
                    .into_iter()
                    .map(ValueRef::String)
                    .collect::<Vec<_>>(),
                &QueryContext::arc(),
            )
            .is_err()
        );

        assert!(
            parse_compact_request(
                &["table", "regular", "abcd"]
                    .into_iter()
                    .map(ValueRef::String)
                    .collect::<Vec<_>>(),
                &QueryContext::arc(),
            )
            .is_err()
        );

        // Test invalid window value
        assert!(
            parse_compact_request(
                &["table", "strict_window", "window=abc"]
                    .into_iter()
                    .map(ValueRef::String)
                    .collect::<Vec<_>>(),
                &QueryContext::arc(),
            )
            .is_err()
        );

        // Test invalid parallelism in options string
        assert!(
            parse_compact_request(
                &["table", "strict_window", "parallelism=abc"]
                    .into_iter()
                    .map(ValueRef::String)
                    .collect::<Vec<_>>(),
                &QueryContext::arc(),
            )
            .is_err()
        );
    }
}
