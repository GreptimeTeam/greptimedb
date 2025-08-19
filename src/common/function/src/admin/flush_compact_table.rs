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

use api::v1::region::{compact_request, StrictWindow};
use common_error::ext::BoxedError;
use common_macro::admin_fn;
use common_query::error::{
    InvalidFuncArgsSnafu, MissingTableMutationHandlerSnafu, Result, TableMutationSnafu,
    UnsupportedInputDataTypeSnafu,
};
use common_telemetry::info;
use datafusion_expr::{Signature, Volatility};
use datatypes::data_type::DataType;
use datatypes::prelude::*;
use session::context::QueryContextRef;
use session::table_name::table_name_to_full_name;
use snafu::{ensure, ResultExt};
use table::requests::{CompactTableRequest, FlushTableRequest};

use crate::handlers::TableMutationHandlerRef;

/// Compact type: strict window.
const COMPACT_TYPE_STRICT_WINDOW: &str = "strict_window";
/// Compact type: strict window (short name).
const COMPACT_TYPE_STRICT_WINDOW_SHORT: &str = "swcs";

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
    let request = parse_compact_params(params, query_ctx)?;
    info!("Compact table request: {:?}", request);

    let affected_rows = table_mutation_handler
        .compact(request, query_ctx.clone())
        .await?;

    Ok(Value::from(affected_rows as u64))
}

fn flush_signature() -> Signature {
    Signature::uniform(
        1,
        vec![ConcreteDataType::string_datatype().as_arrow_type()],
        Volatility::Immutable,
    )
}

fn compact_signature() -> Signature {
    Signature::variadic(
        vec![ConcreteDataType::string_datatype().as_arrow_type()],
        Volatility::Immutable,
    )
}

/// Parses `compact_table` UDF parameters. This function accepts following combinations:
/// - `[<table_name>]`: only tables name provided, using default compaction type: regular
/// - `[<table_name>, <type>]`: specify table name and compaction type. The compaction options will be default.
/// - `[<table_name>, <type>, <options>]`: provides both type and type-specific options.
fn parse_compact_params(
    params: &[ValueRef<'_>],
    query_ctx: &QueryContextRef,
) -> Result<CompactTableRequest> {
    ensure!(
        !params.is_empty(),
        InvalidFuncArgsSnafu {
            err_msg: "Args cannot be empty",
        }
    );

    let (table_name, compact_type) = match params {
        [ValueRef::String(table_name)] => (
            table_name,
            compact_request::Options::Regular(Default::default()),
        ),
        [ValueRef::String(table_name), ValueRef::String(compact_ty_str)] => {
            let compact_type = parse_compact_type(compact_ty_str, None)?;
            (table_name, compact_type)
        }

        [ValueRef::String(table_name), ValueRef::String(compact_ty_str), ValueRef::String(options_str)] =>
        {
            let compact_type = parse_compact_type(compact_ty_str, Some(options_str))?;
            (table_name, compact_type)
        }
        _ => {
            return UnsupportedInputDataTypeSnafu {
                function: "compact_table",
                datatypes: params.iter().map(|v| v.data_type()).collect::<Vec<_>>(),
            }
            .fail()
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
    })
}

/// Parses compaction strategy type. For `strict_window` or `swcs` strict window compaction is chose,
/// otherwise choose regular (TWCS) compaction.
fn parse_compact_type(type_str: &str, option: Option<&str>) -> Result<compact_request::Options> {
    if type_str.eq_ignore_ascii_case(COMPACT_TYPE_STRICT_WINDOW)
        | type_str.eq_ignore_ascii_case(COMPACT_TYPE_STRICT_WINDOW_SHORT)
    {
        let window_seconds = option
            .map(|v| {
                i64::from_str(v).map_err(|_| {
                    InvalidFuncArgsSnafu {
                        err_msg: format!(
                            "Compact window is expected to be a valid number, provided: {}",
                            v
                        ),
                    }
                    .build()
                })
            })
            .transpose()?
            .unwrap_or(0);

        Ok(compact_request::Options::StrictWindow(StrictWindow {
            window_seconds,
        }))
    } else {
        Ok(compact_request::Options::Regular(Default::default()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::region::compact_request::Options;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
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
                                         volatility: datafusion_expr::Volatility::Immutable
                                     } if valid_types == &vec![{ use datatypes::data_type::DataType; ConcreteDataType::string_datatype().as_arrow_type() }]));
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
                        "Execution error: Handler error: Missing TableMutationHandler, not expected",
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
                &parse_compact_params(&params, &QueryContext::arc()).unwrap()
            );
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
                },
            ),
            (
                &[&format!("{}.table", DEFAULT_SCHEMA_NAME)],
                CompactTableRequest {
                    catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                    schema_name: DEFAULT_SCHEMA_NAME.to_string(),
                    table_name: "table".to_string(),
                    compact_options: Options::Regular(Default::default()),
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
                },
            ),
            (
                &["table", "regular"],
                CompactTableRequest {
                    catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                    schema_name: DEFAULT_SCHEMA_NAME.to_string(),
                    table_name: "table".to_string(),
                    compact_options: Options::Regular(Default::default()),
                },
            ),
            (
                &["table", "strict_window"],
                CompactTableRequest {
                    catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                    schema_name: DEFAULT_SCHEMA_NAME.to_string(),
                    table_name: "table".to_string(),
                    compact_options: Options::StrictWindow(StrictWindow { window_seconds: 0 }),
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
                },
            ),
            (
                &["table", "regular", "abcd"],
                CompactTableRequest {
                    catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                    schema_name: DEFAULT_SCHEMA_NAME.to_string(),
                    table_name: "table".to_string(),
                    compact_options: Options::Regular(Default::default()),
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
                },
            ),
        ]);

        assert!(parse_compact_params(
            &["table", "strict_window", "abc"]
                .into_iter()
                .map(ValueRef::String)
                .collect::<Vec<_>>(),
            &QueryContext::arc(),
        )
        .is_err());

        assert!(parse_compact_params(
            &["a.b.table", "strict_window", "abc"]
                .into_iter()
                .map(ValueRef::String)
                .collect::<Vec<_>>(),
            &QueryContext::arc(),
        )
        .is_err());
    }
}
