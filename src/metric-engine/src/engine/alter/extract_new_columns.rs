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

use std::collections::{HashMap, HashSet};

use store_api::metadata::ColumnMetadata;
use store_api::region_request::{AlterKind, RegionAlterRequest};
use store_api::storage::{ColumnId, RegionId};

use crate::error::Result;

/// Extract new columns from the create requests.
///
/// # Panics
///
/// This function will panic if the alter kind is not `AddColumns`.
pub fn extract_new_columns<'a>(
    requests: &'a [(RegionId, RegionAlterRequest)],
    physical_columns: &HashMap<String, ColumnId>,
    new_column_names: &mut HashSet<&'a str>,
    new_columns: &mut Vec<ColumnMetadata>,
) -> Result<()> {
    for (_, request) in requests {
        let AlterKind::AddColumns { columns } = &request.kind else {
            unreachable!()
        };
        for col in columns {
            let column_name = col.column_metadata.column_schema.name.as_str();
            if !physical_columns.contains_key(column_name)
                && !new_column_names.contains(column_name)
            {
                new_column_names.insert(column_name);
                // TODO(weny): avoid clone
                new_columns.push(col.column_metadata.clone());
            }
        }
    }

    Ok(())
}
