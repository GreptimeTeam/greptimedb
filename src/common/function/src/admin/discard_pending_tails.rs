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
}
