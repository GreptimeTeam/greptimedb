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

use crate::error::{Result, TableNotFoundSnafu};
use crate::key::table_name::{TableNameKey, TableNameManager};

/// Get all the table ids from the table names.
///
/// Returns an error if any table does not exist.
pub(crate) async fn get_all_table_ids_by_names<'a>(
    table_name_manager: &TableNameManager,
    table_names: &[TableReference<'a>],
) -> Result<Vec<TableId>> {
    let table_name_keys = table_names
        .iter()
        .map(TableNameKey::from)
        .collect::<Vec<_>>();
    let table_name_values = table_name_manager.batch_get(table_name_keys).await?;
    let mut table_ids = Vec::with_capacity(table_name_values.len());
    for (value, table_name) in table_name_values.into_iter().zip(table_names) {
        let value = value
            .with_context(|| TableNotFoundSnafu {
                table_name: table_name.to_string(),
            })?
            .table_id();

        table_ids.push(value);
    }

    Ok(table_ids)
}
