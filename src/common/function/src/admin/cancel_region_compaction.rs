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

use arrow::datatypes::DataType as ArrowDataType;
use common_macro::admin_fn;
use common_query::error::{
    InvalidFuncArgsSnafu, MissingTableMutationHandlerSnafu, Result, UnsupportedInputDataTypeSnafu,
};
use datafusion_expr::Signature;
use datatypes::data_type::DataType;
use datatypes::prelude::{ConcreteDataType, Value, ValueRef};
use session::context::QueryContextRef;
use snafu::ensure;
use store_api::storage::RegionId;

use crate::handlers::TableMutationHandlerRef;
use crate::helper;

/// Cancels the region compaction identified by `(region_id, task_id)`.
/// Returns one when cancellation is issued or already pending, and zero when absent.
#[admin_fn(
    name = CancelRegionCompactionFunction,
    display_name = cancel_region_compaction,
    sig_fn = signature,
    ret = uint64
)]
pub(crate) async fn cancel_region_compaction(
    table_mutation_handler: &TableMutationHandlerRef,
    query_ctx: &QueryContextRef,
    params: &[ValueRef<'_>],
) -> Result<Value> {
    ensure!(
        params.len() == 2,
        InvalidFuncArgsSnafu {
            err_msg: format!(
                "cancel_region_compaction expects 2 arguments, received {}",
                params.len()
            ),
        }
    );
    let Some(region_id) = helper::cast_u64(&params[0])? else {
        return UnsupportedInputDataTypeSnafu {
            function: "cancel_region_compaction",
            datatypes: params
                .iter()
                .map(|value| value.data_type())
                .collect::<Vec<_>>(),
        }
        .fail();
    };
    let ValueRef::String(task_id) = params[1] else {
        return InvalidFuncArgsSnafu {
            err_msg: "task_id must be a non-empty string",
        }
        .fail();
    };
    ensure!(
        !task_id.trim().is_empty(),
        InvalidFuncArgsSnafu {
            err_msg: "task_id must be a non-empty string"
        }
    );
    let affected_rows = table_mutation_handler
        .cancel_region_compaction(
            RegionId::from_u64(region_id),
            task_id.to_string(),
            query_ctx.clone(),
        )
        .await?;
    Ok(Value::from(affected_rows as u64))
}

fn signature() -> Signature {
    // Matches COMPACT_REGION: primitive numerics without Decimal or Float16 overloads.
    helper::one_of_sigs2(
        ConcreteDataType::numerics()
            .into_iter()
            .map(|data_type| data_type.as_arrow_type())
            .collect(),
        vec![ArrowDataType::Utf8],
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::Field;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};

    use super::*;
    use crate::function::FunctionContext;
    use crate::function_registry;

    #[tokio::test]
    async fn test_cancel_region_compaction_arguments() {
        let ctx = FunctionContext::mock();
        let handler = ctx.state.table_mutation_handler.as_ref().unwrap();
        for params in [
            vec![],
            vec![ValueRef::UInt64(42)],
            vec![
                ValueRef::UInt64(42),
                ValueRef::String("task"),
                ValueRef::Null,
            ],
            vec![ValueRef::Null, ValueRef::String("task")],
            vec![ValueRef::UInt64(42), ValueRef::Null],
            vec![ValueRef::UInt64(42), ValueRef::UInt64(1)],
            vec![ValueRef::UInt64(42), ValueRef::String(" \t")],
        ] {
            assert!(
                cancel_region_compaction(handler, &ctx.query_ctx, &params)
                    .await
                    .is_err(),
                "{params:?}"
            );
        }
        let error = cancel_region_compaction(
            handler,
            &ctx.query_ctx,
            &[ValueRef::Null, ValueRef::String("cycle-id")],
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            common_query::error::Error::UnsupportedInputDataType { .. }
        ));
        assert_eq!(
            cancel_region_compaction(
                handler,
                &ctx.query_ctx,
                &[ValueRef::UInt64(u64::MAX), ValueRef::String("cycle-id")]
            )
            .await
            .unwrap(),
            Value::UInt64(42)
        );
    }

    #[tokio::test]
    async fn test_cancel_region_compaction_async_function() {
        let factory = function_registry::FUNCTION_REGISTRY
            .get_function("cancel_region_compaction")
            .unwrap();
        for (ctx, has_handler) in [
            (FunctionContext::mock(), true),
            (FunctionContext::default(), false),
        ] {
            let function = factory.provide(ctx);
            let result = function
                .as_async()
                .unwrap()
                .invoke_async_with_args(ScalarFunctionArgs {
                    args: vec![
                        ColumnarValue::Scalar(ScalarValue::UInt64(Some(u64::MAX))),
                        ColumnarValue::Scalar(ScalarValue::Utf8(Some("cycle-id".into()))),
                    ],
                    arg_fields: vec![
                        Arc::new(Field::new("region_id", ArrowDataType::UInt64, false)),
                        Arc::new(Field::new("task_id", ArrowDataType::Utf8, false)),
                    ],
                    return_field: Arc::new(Field::new("result", ArrowDataType::UInt64, false)),
                    number_rows: 1,
                    config_options: Default::default(),
                })
                .await;
            if has_handler {
                let ColumnarValue::Array(array) = result.unwrap() else {
                    panic!("expected array output");
                };
                assert_eq!(
                    ScalarValue::try_from_array(&array, 0).unwrap(),
                    ScalarValue::UInt64(Some(42))
                );
            } else {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("Missing TableMutationHandler")
                );
            }
        }
    }

    #[test]
    fn test_cancel_region_compaction_registration_and_signature() {
        let factory = function_registry::FUNCTION_REGISTRY
            .get_function("cancel_region_compaction")
            .unwrap();
        let function = factory.provide(FunctionContext::mock());
        assert_eq!(function.name(), "cancel_region_compaction");
        assert_eq!(function.return_type(&[]).unwrap(), ArrowDataType::UInt64);
        assert_eq!(function.signature(), &signature());
    }
}
