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

mod event;
mod layer;

use std::sync::Arc;

use common_function::function::FunctionContext;
use common_function::function_registry::{FUNCTION_REGISTRY, get_admin_function};
use common_query::Output;
use common_recordbatch::{RecordBatch, RecordBatches};
use common_sql::convert::sql_value_to_value;
use common_telemetry::tracing;
use common_time::Timezone;
use datafusion_expr::TypeSignature;
use datatypes::arrow::datatypes::DataType as ArrowDataType;
use datatypes::data_type::DataType;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::value::Value;
use datatypes::vectors::VectorRef;
pub use layer::{
    AdminEventRecorderHandle, AdminFunctionLayer, AdminFunctionLayerRef,
    AdminFunctionRecordingLayer,
};
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt, ensure};
use sql::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Value as SqlValue};
use sql::statements::admin::Admin;

use crate::error::{self, CastSnafu, ExecuteAdminFunctionSnafu, Result};
use crate::statement::StatementExecutor;

const DUMMY_COLUMN: &str = "<dummy>";

/// A request to execute one ADMIN function statement.
#[derive(Clone)]
pub struct AdminFunctionRequest {
    /// The parsed ADMIN statement to execute.
    pub statement: Admin,
    /// The query context of the request.
    pub query_ctx: QueryContextRef,
}

/// The client output and immediate typed result of one ADMIN function execution.
pub struct AdminFunctionResponse {
    /// The output returned to the client.
    pub output: Output,
    /// The typed immediate result exposed to outer layers.
    pub immediate_result: Value,
}

/// Executes an ADMIN function request.
#[async_trait::async_trait]
pub trait AdminFunctionService: Send + Sync {
    /// Executes an ADMIN function request.
    async fn call(&self, request: AdminFunctionRequest) -> Result<AdminFunctionResponse>;
}

/// A shared ADMIN function service.
pub type AdminFunctionServiceRef = Arc<dyn AdminFunctionService>;

#[derive(Clone)]
struct CoreAdminFunctionService {
    query_engine: query::QueryEngineRef,
}

impl CoreAdminFunctionService {
    fn new(query_engine: query::QueryEngineRef) -> Self {
        Self { query_engine }
    }

    async fn execute(&self, request: AdminFunctionRequest) -> Result<AdminFunctionResponse> {
        let AdminFunctionRequest {
            statement: stmt,
            query_ctx,
        } = request;

        let Admin::Func(func) = &stmt;
        // the function name should be in lower case.
        let func_name = func.name.to_string().to_lowercase();
        let factory = get_admin_function(&func_name)
            .or_else(|| FUNCTION_REGISTRY.get_function(&func_name))
            .context(error::AdminFunctionNotFoundSnafu {
                name: func_name.clone(),
            })?;

        let func_ctx = FunctionContext {
            query_ctx: query_ctx.clone(),
            state: self.query_engine.engine_state().function_state(),
        };

        let admin_udf = factory.provide(func_ctx);
        let admin_async_fn = admin_udf
            .as_async()
            .context(error::AdminFunctionNotFoundSnafu { name: func_name })?;

        let fn_name = admin_udf.name();
        let signature = admin_udf.signature();

        // Parse function arguments
        let FunctionArguments::List(args) = &func.args else {
            return error::BuildAdminFunctionArgsSnafu {
                msg: format!("unsupported function args {} for {}", func.args, fn_name),
            }
            .fail();
        };
        let arg_values = args
            .args
            .iter()
            .map(|arg| {
                let FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(value))) = arg else {
                    return error::BuildAdminFunctionArgsSnafu {
                        msg: format!("unsupported function arg {arg} for {}", fn_name),
                    }
                    .fail();
                };
                Ok(&value.value)
            })
            .collect::<Result<Vec<_>>>()?;

        let args = args_to_vector(&signature.type_signature, &arg_values, &query_ctx)?;
        let arg_types = args
            .iter()
            .map(|arg| arg.data_type().as_arrow_type())
            .collect::<Vec<_>>();
        let ret_type = admin_udf.return_type(&arg_types).map_err(|e| {
            error::Error::BuildAdminFunctionArgs {
                msg: format!(
                    "Failed to get return type of admin function {}: {}",
                    fn_name, e
                ),
            }
        })?;

        // Convert arguments to DataFusion ColumnarValue format
        let columnar_args: Vec<datafusion_expr::ColumnarValue> = args
            .iter()
            .map(|vector| datafusion_expr::ColumnarValue::Array(vector.to_arrow_array()))
            .collect();

        // Create ScalarFunctionArgs following the same pattern as udf.rs
        let func_args = datafusion::logical_expr::ScalarFunctionArgs {
            args: columnar_args,
            arg_fields: args
                .iter()
                .enumerate()
                .map(|(i, vector)| {
                    Arc::new(arrow::datatypes::Field::new(
                        format!("arg_{}", i),
                        arg_types[i].clone(),
                        vector.null_count() > 0,
                    ))
                })
                .collect(),
            return_field: Arc::new(arrow::datatypes::Field::new("result", ret_type, true)),
            number_rows: if args.is_empty() { 1 } else { args[0].len() },
            config_options: Arc::new(query_ctx.create_config_options()),
        };

        // Execute the async UDF
        let result_columnar = admin_async_fn
            .invoke_async_with_args(func_args)
            .await
            .with_context(|_| ExecuteAdminFunctionSnafu {
                msg: fn_name.to_string(),
            })?;

        // Convert result back to VectorRef
        let result_columnar: common_query::prelude::ColumnarValue =
            (&result_columnar).try_into().context(CastSnafu)?;

        let result_vector: VectorRef = result_columnar.try_into_vector(1).context(CastSnafu)?;
        let immediate_result = immediate_result(&result_vector)?;

        let column_schemas = vec![ColumnSchema::new(
            // Use statement as the result column name
            stmt.to_string(),
            result_vector.data_type(),
            false,
        )];
        let schema = Arc::new(Schema::new(column_schemas));
        let batch = RecordBatch::new(schema.clone(), vec![result_vector])
            .context(error::BuildRecordBatchSnafu)?;
        let batches =
            RecordBatches::try_new(schema, vec![batch]).context(error::BuildRecordBatchSnafu)?;

        Ok(AdminFunctionResponse {
            output: Output::new_with_record_batches(batches),
            immediate_result,
        })
    }
}

fn immediate_result(result_vector: &VectorRef) -> Result<Value> {
    ensure!(
        !result_vector.is_empty(),
        error::EmptyAdminFunctionResultSnafu
    );
    Ok(result_vector.get(0))
}

#[async_trait::async_trait]
impl AdminFunctionService for CoreAdminFunctionService {
    async fn call(&self, request: AdminFunctionRequest) -> Result<AdminFunctionResponse> {
        self.execute(request).await
    }
}

/// Creates the core ADMIN function service.
pub(crate) fn new_admin_function_service(
    query_engine: query::QueryEngineRef,
) -> AdminFunctionServiceRef {
    Arc::new(CoreAdminFunctionService::new(query_engine))
}

impl StatementExecutor {
    /// Executes the [`Admin`] statement and returns the output.
    #[tracing::instrument(skip_all)]
    pub(crate) async fn execute_admin_command(
        &self,
        stmt: Admin,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        self.admin_function_service
            .call(AdminFunctionRequest {
                statement: stmt,
                query_ctx,
            })
            .await
            .map(|response| response.output)
    }
}

/// Try to cast the arguments to vectors by function's signature.
fn args_to_vector(
    type_signature: &TypeSignature,
    args: &Vec<&SqlValue>,
    query_ctx: &QueryContextRef,
) -> Result<Vec<VectorRef>> {
    let tz = query_ctx.timezone();

    match type_signature {
        TypeSignature::Variadic(valid_types) => {
            values_to_vectors_by_valid_types(valid_types, args, Some(&tz))
        }

        TypeSignature::Uniform(arity, valid_types) => {
            ensure!(
                *arity == args.len(),
                error::FunctionArityMismatchSnafu {
                    actual: args.len(),
                    expected: *arity,
                }
            );

            values_to_vectors_by_valid_types(valid_types, args, Some(&tz))
        }

        TypeSignature::Exact(data_types) => {
            values_to_vectors_by_exact_types(data_types, args, Some(&tz))
        }

        TypeSignature::VariadicAny => {
            let data_types = args
                .iter()
                .map(|value| try_get_data_type_for_sql_value(value))
                .collect::<Result<Vec<_>>>()?;

            values_to_vectors_by_exact_types(&data_types, args, Some(&tz))
        }

        TypeSignature::Any(arity) => {
            ensure!(
                *arity == args.len(),
                error::FunctionArityMismatchSnafu {
                    actual: args.len(),
                    expected: *arity,
                }
            );

            let data_types = args
                .iter()
                .map(|value| try_get_data_type_for_sql_value(value))
                .collect::<Result<Vec<_>>>()?;

            values_to_vectors_by_exact_types(&data_types, args, Some(&tz))
        }

        TypeSignature::OneOf(type_sigs) => {
            for type_sig in type_sigs {
                if let Ok(vectors) = args_to_vector(type_sig, args, query_ctx) {
                    return Ok(vectors);
                }
            }

            error::BuildAdminFunctionArgsSnafu {
                msg: "function signature not match",
            }
            .fail()
        }

        _ => error::BuildAdminFunctionArgsSnafu {
            msg: format!("unknown function type signature: {type_signature:?}"),
        }
        .fail(),
    }
}

/// Try to cast sql values to vectors by exact data types.
fn values_to_vectors_by_exact_types(
    exact_types: &[ArrowDataType],
    args: &[&SqlValue],
    tz: Option<&Timezone>,
) -> Result<Vec<VectorRef>> {
    args.iter()
        .zip(exact_types.iter())
        .map(|(value, data_type)| {
            let schema = ColumnSchema::new(
                DUMMY_COLUMN,
                ConcreteDataType::from_arrow_type(data_type),
                true,
            );
            let value = sql_value_to_value(&schema, value, tz, None, false)
                .context(error::SqlCommonSnafu)?;

            Ok(value_to_vector(value))
        })
        .collect()
}

/// Try to cast sql values to vectors by valid data types.
fn values_to_vectors_by_valid_types(
    valid_types: &[ArrowDataType],
    args: &[&SqlValue],
    tz: Option<&Timezone>,
) -> Result<Vec<VectorRef>> {
    args.iter()
        .map(|value| {
            for data_type in valid_types {
                let schema = ColumnSchema::new(
                    DUMMY_COLUMN,
                    ConcreteDataType::from_arrow_type(data_type),
                    true,
                );
                if let Ok(value) = sql_value_to_value(&schema, value, tz, None, false) {
                    return Ok(value_to_vector(value));
                }
            }

            error::BuildAdminFunctionArgsSnafu {
                msg: format!("failed to cast {value}"),
            }
            .fail()
        })
        .collect::<Result<Vec<_>>>()
}

/// Build a [`VectorRef`] from [`Value`]
fn value_to_vector(value: Value) -> VectorRef {
    let data_type = value.data_type();
    let mut mutable_vector = data_type.create_mutable_vector(1);
    mutable_vector.push_value_ref(&value.as_value_ref());

    mutable_vector.to_vector()
}

/// Try to infer the data type from sql value.
fn try_get_data_type_for_sql_value(value: &SqlValue) -> Result<ArrowDataType> {
    match value {
        SqlValue::Number(_, _) => Ok(ArrowDataType::Float64),
        SqlValue::Null => Ok(ArrowDataType::Null),
        SqlValue::Boolean(_) => Ok(ArrowDataType::Boolean),
        SqlValue::HexStringLiteral(_)
        | SqlValue::DoubleQuotedString(_)
        | SqlValue::SingleQuotedString(_) => Ok(ArrowDataType::Utf8),
        _ => error::BuildAdminFunctionArgsSnafu {
            msg: format!("unsupported sql value: {value}"),
        }
        .fail(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::vectors::{Int32Vector, VectorRef};

    use crate::error::Error;
    use crate::statement::admin::immediate_result;

    #[test]
    fn rejects_empty_admin_function_result() {
        let result_vector: VectorRef = Arc::new(Int32Vector::from(vec![]));

        assert!(matches!(
            immediate_result(&result_vector),
            Err(Error::EmptyAdminFunctionResult)
        ));
    }
}
