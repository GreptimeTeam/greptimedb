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
use api::v1::meta::{ReconcileCatalog, ReconcileRequest};
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

const FN_NAME: &str = "reconcile_catalog";

/// A function to reconcile a catalog.
/// Returns the procedure id if success.
///
/// - `reconcile_catalog(resolve_strategy)`.
/// - `reconcile_catalog(resolve_strategy, parallelism)`.
///
/// - `reconcile_catalog()`.
#[admin_fn(
    name = ReconcileCatalogFunction,
    display_name = reconcile_catalog,
    sig_fn = signature,
    ret = string
)]
pub(crate) async fn reconcile_catalog(
    procedure_service_handler: &ProcedureServiceHandlerRef,
    query_ctx: &QueryContextRef,
    params: &[ValueRef<'_>],
) -> Result<Value> {
    let (resolve_strategy, parallelism) = match params.len() {
        0 => (default_resolve_strategy(), default_parallelism()),
        1 => (
            parse_resolve_strategy(get_string_from_params(params, 0, FN_NAME)?)?,
            default_parallelism(),
        ),
        2 => {
            let Some(parallelism) = cast_u32(&params[1])? else {
                return UnsupportedInputDataTypeSnafu {
                    function: FN_NAME,
                    datatypes: params.iter().map(|v| v.data_type()).collect::<Vec<_>>(),
                }
                .fail();
            };
            (
                parse_resolve_strategy(get_string_from_params(params, 0, FN_NAME)?)?,
                parallelism,
            )
        }
        size => {
            return InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The length of the args is not correct, expect 0, 1 or 2, have: {}",
                    size
                ),
            }
            .fail();
        }
    };
    info!(
        "Reconciling catalog with resolve_strategy: {:?}, parallelism: {}",
        resolve_strategy, parallelism
    );
    let pid = procedure_service_handler
        .reconcile(ReconcileRequest {
            target: Some(Target::ReconcileCatalog(ReconcileCatalog {
                catalog_name: query_ctx.current_catalog().to_string(),
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
        // reconcile_catalog()
        TypeSignature::Nullary,
        // reconcile_catalog(resolve_strategy)
        TypeSignature::Exact(vec![ConcreteDataType::string_datatype().as_arrow_type()]),
    ]);
    for sign in nums {
        // reconcile_catalog(resolve_strategy, parallelism)
        signs.push(TypeSignature::Exact(vec![
            ConcreteDataType::string_datatype().as_arrow_type(),
            sign.as_arrow_type(),
        ]));
    }
    Signature::one_of(signs, Volatility::Immutable)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field};
    use datafusion_expr::ColumnarValue;

    use crate::admin::reconcile_catalog::ReconcileCatalogFunction;
    use crate::function::FunctionContext;
    use crate::function_factory::ScalarFunctionFactory;

    #[tokio::test]
    async fn test_reconcile_catalog() {
        common_telemetry::init_default_ut_logging();

        // reconcile_catalog()
        let factory: ScalarFunctionFactory = ReconcileCatalogFunction::factory().into();
        let provider = factory.provide(FunctionContext::mock());
        let f = provider.as_async().unwrap();

        let func_args = datafusion::logical_expr::ScalarFunctionArgs {
            args: vec![],
            arg_fields: vec![],
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

        // reconcile_catalog(resolve_strategy)
        let factory: ScalarFunctionFactory = ReconcileCatalogFunction::factory().into();
        let provider = factory.provide(FunctionContext::mock());
        let f = provider.as_async().unwrap();

        let func_args = datafusion::logical_expr::ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(StringArray::from(vec![
                "UseMetasrv",
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

        // reconcile_catalog(resolve_strategy, parallelism)
        let factory: ScalarFunctionFactory = ReconcileCatalogFunction::factory().into();
        let provider = factory.provide(FunctionContext::mock());
        let f = provider.as_async().unwrap();

        let func_args = datafusion::logical_expr::ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(StringArray::from(vec!["UseLatest"]))),
                ColumnarValue::Array(Arc::new(UInt64Array::from(vec![10]))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("arg_0", DataType::Utf8, false)),
                Arc::new(Field::new("arg_1", DataType::UInt64, false)),
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

        // unsupported input data type
        let factory: ScalarFunctionFactory = ReconcileCatalogFunction::factory().into();
        let provider = factory.provide(FunctionContext::mock());
        let f = provider.as_async().unwrap();

        let func_args = datafusion::logical_expr::ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(StringArray::from(vec!["UseLatest"]))),
                ColumnarValue::Array(Arc::new(StringArray::from(vec!["test"]))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("arg_0", DataType::Utf8, false)),
                Arc::new(Field::new("arg_1", DataType::Utf8, false)),
            ],
            return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
            number_rows: 1,
            config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
        };
        let _err = f.invoke_async_with_args(func_args).await.unwrap_err();
        // Note: Error type is DataFusionError at this level, not common_query::Error

        // invalid function args
        let factory: ScalarFunctionFactory = ReconcileCatalogFunction::factory().into();
        let provider = factory.provide(FunctionContext::mock());
        let f = provider.as_async().unwrap();

        let func_args = datafusion::logical_expr::ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(StringArray::from(vec!["UseLatest"]))),
                ColumnarValue::Array(Arc::new(UInt64Array::from(vec![10]))),
                ColumnarValue::Array(Arc::new(StringArray::from(vec!["10"]))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("arg_0", DataType::Utf8, false)),
                Arc::new(Field::new("arg_1", DataType::UInt64, false)),
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
