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

use snafu::OptionExt;
use store_api::storage::TableId;
use table::table_reference::TableReference;

use crate::error::{Result, TableInfoNotFoundSnafu};
use crate::key::table_info::{TableInfoManager, TableInfoValue};
use crate::key::DeserializedValueWithBytes;

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
