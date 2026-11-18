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
use common_error::ext::BoxedError;
use common_macro::admin_fn;
use common_query::error::{
    InvalidFuncArgsSnafu, MissingTableMutationHandlerSnafu, Result, TableMutationSnafu,
    UnsupportedInputDataTypeSnafu,
};
use datafusion_expr::Signature;
use datatypes::data_type::DataType;
use datatypes::prelude::*;
use session::context::QueryContextRef;
use session::table_name::table_name_to_full_name;
use snafu::{ResultExt, ensure};
use store_api::storage::RegionId;
use table::table_name::TableName;

use crate::handlers::TableMutationHandlerRef;
use crate::helper::{cast_u64, one_of_sigs2};

/// Discards the pending WAL tails that a datanode wrote from a region or from all regions
/// of a table, without replaying them. Returns the number of discarded tails.
///
/// A region's flushed entry id is raised to the largest bound of the tails it discards, so
/// the region reopens after a crash only if its datanode's WAL durably holds history past that
/// bound. The function does not check this: whoever records the tails ensures it.
///
/// - `region_id` or `table_name`: the region or the table.
/// - `node_id`: the datanode that wrote the tails.
#[admin_fn(
    name = DiscardPendingTailsFunction,
    display_name = discard_pending_tails,
    sig_fn = signature,
    ret = uint64,
    single_row
)]
pub(crate) async fn discard_pending_tails(
    table_mutation_handler: &TableMutationHandlerRef,
    query_ctx: &QueryContextRef,
    params: &[ValueRef<'_>],
) -> Result<Value> {
    ensure!(
        params.len() == 2,
        InvalidFuncArgsSnafu {
            err_msg: format!(
                "The length of the args is not correct, expect 2, have: {}",
                params.len()
            ),
        }
    );
    let Some(node_id) = cast_u64(&params[1])? else {
        return UnsupportedInputDataTypeSnafu {
            function: "discard_pending_tails",
            datatypes: params
                .iter()
                .map(|value| value.data_type())
                .collect::<Vec<_>>(),
        }
        .fail();
    };

    let discarded = match params[0] {
        ValueRef::String(table_name) => {
            let (catalog_name, schema_name, table_name) =
                table_name_to_full_name(table_name, query_ctx)
                    .map_err(BoxedError::new)
                    .context(TableMutationSnafu)?;
            table_mutation_handler
                .discard_pending_tails_by_table(
                    TableName::new(catalog_name, schema_name, table_name),
                    node_id,
                    query_ctx.clone(),
                )
                .await?
        }
        _ => {
            let Some(region_id) = cast_u64(&params[0])? else {
                return UnsupportedInputDataTypeSnafu {
                    function: "discard_pending_tails",
                    datatypes: params
                        .iter()
                        .map(|value| value.data_type())
                        .collect::<Vec<_>>(),
                }
                .fail();
            };
            table_mutation_handler
                .discard_pending_tails(RegionId::from_u64(region_id), node_id, query_ctx.clone())
                .await?
        }
    };

    Ok(Value::from(discarded as u64))
}

fn signature() -> Signature {
    let numerics = ConcreteDataType::numerics()
        .into_iter()
        .map(|data_type| data_type.as_arrow_type())
        .collect::<Vec<_>>();
    let mut targets = numerics.clone();
    targets.push(ArrowDataType::Utf8);
    one_of_sigs2(targets, numerics)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field};
    use datafusion_expr::ColumnarValue;

    use super::*;
    use crate::function::FunctionContext;
    use crate::function_factory::ScalarFunctionFactory;
    use crate::function_registry::{FUNCTION_REGISTRY, get_admin_function};

    #[test]
    fn test_discard_pending_tails_is_admin_only() {
        assert!(get_admin_function("discard_pending_tails").is_some());
        assert!(
            FUNCTION_REGISTRY
                .get_function("discard_pending_tails")
                .is_none()
        );
    }

    /// Calls the function with `args`; the mock handler returns the node id for a region and
    /// 42 plus the node id for a table.
    async fn call(args: Vec<(ColumnarValue, DataType)>) -> datafusion_common::Result<u64> {
        let factory: ScalarFunctionFactory = DiscardPendingTailsFunction::factory().into();
        let function = factory.provide(FunctionContext::mock());
        let (args, arg_fields): (Vec<_>, Vec<_>) = args
            .into_iter()
            .enumerate()
            .map(|(i, (arg, data_type))| {
                (
                    arg,
                    Arc::new(Field::new(format!("arg_{i}"), data_type, false)),
                )
            })
            .unzip();
        let args = datafusion::logical_expr::ScalarFunctionArgs {
            args,
            arg_fields,
            return_field: Arc::new(Field::new("result", DataType::UInt64, false)),
            number_rows: 1,
            config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
        };
        let ColumnarValue::Array(array) = function
            .as_async()
            .unwrap()
            .invoke_async_with_args(args)
            .await?
        else {
            panic!("expected array output");
        };
        Ok(array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0))
    }

    fn uint64(value: u64) -> (ColumnarValue, DataType) {
        let array = Arc::new(UInt64Array::from(vec![value]));
        (ColumnarValue::Array(array), DataType::UInt64)
    }

    fn string(value: &str) -> (ColumnarValue, DataType) {
        let array = Arc::new(StringArray::from(vec![value]));
        (ColumnarValue::Array(array), DataType::Utf8)
    }

    #[tokio::test]
    async fn test_discard_pending_tails_of_region_or_table() {
        assert_eq!(7, call(vec![uint64(99), uint64(7)]).await.unwrap());
        assert_eq!(49, call(vec![string("my_table"), uint64(7)]).await.unwrap());
    }

    #[tokio::test]
    async fn test_discard_pending_tails_requires_two_arguments() {
        let error = call(vec![uint64(99)]).await.unwrap_err();
        assert!(error.to_string().contains("expect 2, have: 1"), "{error}");
    }
}
