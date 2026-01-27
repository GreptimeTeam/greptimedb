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

use itertools::Itertools;
use snafu::OptionExt;
use store_api::storage::TableId;
use table::metadata::TableInfo;
use table::table_reference::TableReference;

use crate::error::{Result, TableInfoNotFoundSnafu};
use crate::key::table_info::{TableInfoManager, TableInfoValue};
use crate::key::table_route::{TableRouteManager, TableRouteValue};
use crate::key::{DeserializedValueWithBytes, TableMetadataManager};

/// Get all table info values by table ids.
///
/// Returns an error if any table does not exist.
pub(crate) async fn get_all_table_info_values_by_table_ids<'a>(
    table_info_manager: &TableInfoManager,
    table_ids: &[TableId],
    table_names: &[TableReference<'a>],
) -> Result<Vec<DeserializedValueWithBytes<TableInfoValue>>> {
    let mut table_info_map = table_info_manager.batch_get_raw(table_ids).await?;
    let mut table_info_values = Vec::with_capacity(table_ids.len());
    for (table_id, table_name) in table_ids.iter().zip(table_names) {
        let table_info_value =
            table_info_map
                .remove(table_id)
                .with_context(|| TableInfoNotFoundSnafu {
                    table: table_name.to_string(),
                })?;
        table_info_values.push(table_info_value);
    }

    Ok(table_info_values)
}

/// Checks if all the logical table routes have the same physical table id.
pub(crate) async fn all_logical_table_routes_have_same_physical_id(
    table_route_manager: &TableRouteManager,
    table_ids: &[TableId],
    physical_table_id: TableId,
) -> Result<bool> {
    let table_routes = table_route_manager
        .table_route_storage()
        .batch_get(table_ids)
        .await?;

    let is_same_physical_table = table_routes.iter().all(|r| {
        if let Some(TableRouteValue::Logical(r)) = r {
            r.physical_table_id() == physical_table_id
        } else {
            false
        }
    });

    Ok(is_same_physical_table)
}

/// Batch updates the table info values.
///
/// The table info values are grouped into chunks, and each chunk is updated in a single transaction.
///
/// Returns an error if any table info value fails to update.
pub(crate) async fn batch_update_table_info_values(
    table_metadata_manager: &TableMetadataManager,
    table_info_values: Vec<(DeserializedValueWithBytes<TableInfoValue>, TableInfo)>,
) -> Result<()> {
    let chunk_size = table_metadata_manager.batch_update_table_info_value_chunk_size();
    if table_info_values.len() > chunk_size {
        let chunks = table_info_values
            .into_iter()
            .chunks(chunk_size)
            .into_iter()
            .map(|check| check.collect::<Vec<_>>())
            .collect::<Vec<_>>();
        for chunk in chunks {
            table_metadata_manager
                .batch_update_table_info_values(chunk)
                .await?;
        }
    } else {
        table_metadata_manager
            .batch_update_table_info_values(table_info_values)
            .await?;
    }

    Ok(())
}
