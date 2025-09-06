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
use api::v1::meta::{ReconcileRequest, ReconcileTable, ResolveStrategy};
use arrow::datatypes::DataType as ArrowDataType;
use common_catalog::format_full_table_name;
use common_error::ext::BoxedError;
use common_macro::admin_fn;
use common_query::error::{
    MissingProcedureServiceHandlerSnafu, Result, TableMutationSnafu, UnsupportedInputDataTypeSnafu,
};
use common_telemetry::info;
use datafusion_expr::{Signature, TypeSignature, Volatility};
use datatypes::prelude::*;
use session::context::QueryContextRef;
use session::table_name::table_name_to_full_name;
use snafu::ResultExt;

use crate::handlers::ProcedureServiceHandlerRef;
use crate::helper::parse_resolve_strategy;

const FN_NAME: &str = "reconcile_table";

/// A function to reconcile a table.
/// Returns the procedure id if success.
///
/// - `reconcile_table(table_name)`.
/// - `reconcile_table(table_name, resolve_strategy)`.
///
/// The parameters:
/// - `table_name`:  the table name
#[admin_fn(
    name = ReconcileTableFunction,
    display_name = reconcile_table,
    sig_fn = signature,
    ret = string
)]
pub(crate) async fn reconcile_table(
    procedure_service_handler: &ProcedureServiceHandlerRef,
    query_ctx: &QueryContextRef,
    params: &[ValueRef<'_>],
) -> Result<Value> {
    let (table_name, resolve_strategy) = match params {
        [ValueRef::String(table_name)] => (table_name, ResolveStrategy::UseLatest),
        [
            ValueRef::String(table_name),
            ValueRef::String(resolve_strategy),
        ] => (table_name, parse_resolve_strategy(resolve_strategy)?),
        _ => {
            return UnsupportedInputDataTypeSnafu {
                function: FN_NAME,
                datatypes: params.iter().map(|v| v.data_type()).collect::<Vec<_>>(),
            }
            .fail();
        }
    };
    let (catalog_name, schema_name, table_name) = table_name_to_full_name(table_name, query_ctx)
        .map_err(BoxedError::new)
        .context(TableMutationSnafu)?;
    info!(
        "Reconciling table: {} with resolve_strategy: {:?}",
        format_full_table_name(&catalog_name, &schema_name, &table_name),
        resolve_strategy
    );
    let pid = procedure_service_handler
        .reconcile(ReconcileRequest {
            target: Some(Target::ReconcileTable(ReconcileTable {
                catalog_name,
                schema_name,
                table_name,
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
    Signature::one_of(
        vec![
            // reconcile_table(table_name)
            TypeSignature::Exact(vec![ArrowDataType::Utf8]),
            // reconcile_table(table_name, resolve_strategy)
            TypeSignature::Exact(vec![ArrowDataType::Utf8, ArrowDataType::Utf8]),
        ],
        Volatility::Immutable,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field};
    use datafusion_expr::ColumnarValue;

    use crate::admin::reconcile_table::ReconcileTableFunction;
    use crate::function::FunctionContext;
    use crate::function_factory::ScalarFunctionFactory;

    #[tokio::test]
    async fn test_reconcile_table() {
        common_telemetry::init_default_ut_logging();

        // reconcile_table(table_name)
        let factory: ScalarFunctionFactory = ReconcileTableFunction::factory().into();
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

        // reconcile_table(table_name, resolve_strategy)
        let factory: ScalarFunctionFactory = ReconcileTableFunction::factory().into();
        let provider = factory.provide(FunctionContext::mock());
        let f = provider.as_async().unwrap();

        let func_args = datafusion::logical_expr::ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(StringArray::from(vec!["test"]))),
                ColumnarValue::Array(Arc::new(StringArray::from(vec!["UseMetasrv"]))),
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

        // unsupported input data type
        let factory: ScalarFunctionFactory = ReconcileTableFunction::factory().into();
        let provider = factory.provide(FunctionContext::mock());
        let f = provider.as_async().unwrap();

        let func_args = datafusion::logical_expr::ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(StringArray::from(vec!["test"]))),
                ColumnarValue::Array(Arc::new(StringArray::from(vec!["UseMetasrv"]))),
                ColumnarValue::Array(Arc::new(StringArray::from(vec!["10"]))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("arg_0", DataType::Utf8, false)),
                Arc::new(Field::new("arg_1", DataType::Utf8, false)),
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
