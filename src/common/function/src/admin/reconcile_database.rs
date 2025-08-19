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

use api::v1::meta::reconcile_request::Target;
use api::v1::meta::{ReconcileDatabase, ReconcileRequest};
use arrow::datatypes::DataType as ArrowDataType;
use common_macro::admin_fn;
use common_query::error::{
    InvalidFuncArgsSnafu, MissingProcedureServiceHandlerSnafu, Result,
    UnsupportedInputDataTypeSnafu,
};
use common_telemetry::info;
use datafusion_expr::{Signature, TypeSignature, Volatility};
use datatypes::data_type::DataType;
use datatypes::prelude::*;
use session::context::QueryContextRef;

use crate::handlers::ProcedureServiceHandlerRef;
use crate::helper::{
    cast_u32, default_parallelism, default_resolve_strategy, get_string_from_params,
    parse_resolve_strategy,
};

const FN_NAME: &str = "reconcile_database";

/// A function to reconcile a database.
/// Returns the procedure id if success.
///
/// - `reconcile_database(database_name)`.
/// - `reconcile_database(database_name, resolve_strategy)`.
/// - `reconcile_database(database_name, resolve_strategy, parallelism)`.
///
/// The parameters:
/// - `database_name`:  the database name
#[admin_fn(
    name = ReconcileDatabaseFunction,
    display_name = reconcile_database,
    sig_fn = signature,
    ret = string
)]
pub(crate) async fn reconcile_database(
    procedure_service_handler: &ProcedureServiceHandlerRef,
    query_ctx: &QueryContextRef,
    params: &[ValueRef<'_>],
) -> Result<Value> {
    let (database_name, resolve_strategy, parallelism) = match params.len() {
        1 => (
            get_string_from_params(params, 0, FN_NAME)?,
            default_resolve_strategy(),
            default_parallelism(),
        ),
        2 => (
            get_string_from_params(params, 0, FN_NAME)?,
            parse_resolve_strategy(get_string_from_params(params, 1, FN_NAME)?)?,
            default_parallelism(),
        ),
        3 => {
            let Some(parallelism) = cast_u32(&params[2])? else {
                return UnsupportedInputDataTypeSnafu {
                    function: FN_NAME,
                    datatypes: params.iter().map(|v| v.data_type()).collect::<Vec<_>>(),
                }
                .fail();
            };
            (
                get_string_from_params(params, 0, FN_NAME)?,
                parse_resolve_strategy(get_string_from_params(params, 1, FN_NAME)?)?,
                parallelism,
            )
        }
        size => {
            return InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The length of the args is not correct, expect 1, 2 or 3, have: {}",
                    size
                ),
            }
            .fail();
        }
    };
    info!(
        "Reconciling database: {}, resolve_strategy: {:?}, parallelism: {}",
        database_name, resolve_strategy, parallelism
    );
    let pid = procedure_service_handler
        .reconcile(ReconcileRequest {
            target: Some(Target::ReconcileDatabase(ReconcileDatabase {
                catalog_name: query_ctx.current_catalog().to_string(),
                database_name: database_name.to_string(),
                parallelism,
                resolve_strategy: resolve_strategy as i32,
            })),
            ..Default::default()
        })
        .await?;
    match pid {
        Some(pid) => Ok(Value::from(pid)),
        None => Ok(Value::Null),
    }
}

fn signature() -> Signature {
    let nums = ConcreteDataType::numerics();
    let mut signs = Vec::with_capacity(2 + nums.len());
    signs.extend([
        // reconcile_database(datanode_name)
        TypeSignature::Exact(vec![ArrowDataType::Utf8]),
        // reconcile_database(database_name, resolve_strategy)
        TypeSignature::Exact(vec![ArrowDataType::Utf8, ArrowDataType::Utf8]),
    ]);
    for sign in nums {
        // reconcile_database(database_name, resolve_strategy, parallelism)
        signs.push(TypeSignature::Exact(vec![
            ArrowDataType::Utf8,
            ArrowDataType::Utf8,
            sign.as_arrow_type(),
        ]));
    }
    Signature::one_of(signs, Volatility::Immutable)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{StringArray, UInt32Array};
    use arrow::datatypes::{DataType, Field};
    use datafusion_expr::ColumnarValue;

    use crate::admin::reconcile_database::ReconcileDatabaseFunction;
    use crate::function::FunctionContext;
    use crate::function_factory::ScalarFunctionFactory;

    #[tokio::test]
    async fn test_reconcile_catalog() {
        common_telemetry::init_default_ut_logging();

        // reconcile_database(database_name)
        let factory: ScalarFunctionFactory = ReconcileDatabaseFunction::factory().into();
        let provider = factory.provide(FunctionContext::mock());
        let f = provider.as_async().unwrap();

        let func_args = datafusion::logical_expr::ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(StringArray::from(vec![
                "test",
            ])))],
            arg_fields: vec![Arc::new(Field::new("arg_0", DataType::Utf8, false))],
            return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
            number_rows: 1,
            config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
        };
        let result = f.invoke_async_with_args(func_args).await.unwrap();
        match result {
            ColumnarValue::Array(array) => {
                let result_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(result_array.value(0), "test_pid");
            }
            ColumnarValue::Scalar(scalar) => {
                assert_eq!(
                    scalar,
                    datafusion_common::ScalarValue::Utf8(Some("test_pid".to_string()))
                );
            }
        }

        // reconcile_database(database_name, resolve_strategy)
        let factory: ScalarFunctionFactory = ReconcileDatabaseFunction::factory().into();
        let provider = factory.provide(FunctionContext::mock());
        let f = provider.as_async().unwrap();

        let func_args = datafusion::logical_expr::ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(StringArray::from(vec!["test"]))),
                ColumnarValue::Array(Arc::new(StringArray::from(vec!["UseLatest"]))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("arg_0", DataType::Utf8, false)),
                Arc::new(Field::new("arg_1", DataType::Utf8, false)),
            ],
            return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
            number_rows: 1,
            config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
        };
        let result = f.invoke_async_with_args(func_args).await.unwrap();
        match result {
            ColumnarValue::Array(array) => {
                let result_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(result_array.value(0), "test_pid");
            }
            ColumnarValue::Scalar(scalar) => {
                assert_eq!(
                    scalar,
                    datafusion_common::ScalarValue::Utf8(Some("test_pid".to_string()))
                );
            }
        }

        // reconcile_database(database_name, resolve_strategy, parallelism)
        let factory: ScalarFunctionFactory = ReconcileDatabaseFunction::factory().into();
        let provider = factory.provide(FunctionContext::mock());
        let f = provider.as_async().unwrap();

        let func_args = datafusion::logical_expr::ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(StringArray::from(vec!["test"]))),
                ColumnarValue::Array(Arc::new(StringArray::from(vec!["UseLatest"]))),
                ColumnarValue::Array(Arc::new(UInt32Array::from(vec![10]))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("arg_0", DataType::Utf8, false)),
                Arc::new(Field::new("arg_1", DataType::Utf8, false)),
                Arc::new(Field::new("arg_2", DataType::UInt32, false)),
            ],
            return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
            number_rows: 1,
            config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
        };
        let result = f.invoke_async_with_args(func_args).await.unwrap();
        match result {
            ColumnarValue::Array(array) => {
                let result_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(result_array.value(0), "test_pid");
            }
            ColumnarValue::Scalar(scalar) => {
                assert_eq!(
                    scalar,
                    datafusion_common::ScalarValue::Utf8(Some("test_pid".to_string()))
                );
            }
        }

        // invalid function args
        let factory: ScalarFunctionFactory = ReconcileDatabaseFunction::factory().into();
        let provider = factory.provide(FunctionContext::mock());
        let f = provider.as_async().unwrap();

        let func_args = datafusion::logical_expr::ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(StringArray::from(vec!["UseLatest"]))),
                ColumnarValue::Array(Arc::new(UInt32Array::from(vec![10]))),
                ColumnarValue::Array(Arc::new(StringArray::from(vec!["v1"]))),
                ColumnarValue::Array(Arc::new(StringArray::from(vec!["v2"]))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("arg_0", DataType::Utf8, false)),
                Arc::new(Field::new("arg_1", DataType::UInt32, false)),
                Arc::new(Field::new("arg_2", DataType::Utf8, false)),
                Arc::new(Field::new("arg_3", DataType::Utf8, false)),
            ],
            return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
            number_rows: 1,
            config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
        };
        let _err = f.invoke_async_with_args(func_args).await.unwrap_err();
        // Note: Error type is DataFusionError at this level, not common_query::Error

        // unsupported input data type
        let factory: ScalarFunctionFactory = ReconcileDatabaseFunction::factory().into();
        let provider = factory.provide(FunctionContext::mock());
        let f = provider.as_async().unwrap();

        let func_args = datafusion::logical_expr::ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(StringArray::from(vec!["UseLatest"]))),
                ColumnarValue::Array(Arc::new(UInt32Array::from(vec![10]))),
                ColumnarValue::Array(Arc::new(StringArray::from(vec!["v1"]))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("arg_0", DataType::Utf8, false)),
                Arc::new(Field::new("arg_1", DataType::UInt32, false)),
                Arc::new(Field::new("arg_2", DataType::Utf8, false)),
            ],
            return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
            number_rows: 1,
            config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
        };
        let _err = f.invoke_async_with_args(func_args).await.unwrap_err();
        // Note: Error type is DataFusionError at this level, not common_query::Error
    }
}
