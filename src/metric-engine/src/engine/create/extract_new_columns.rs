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

use api::v1::SemanticType;
use snafu::ensure;
use store_api::metadata::ColumnMetadata;
use store_api::region_request::RegionCreateRequest;
use store_api::storage::{ColumnId, RegionId};

use crate::error::{AddingFieldColumnSnafu, Result};

/// Extract new columns from the create requests.
pub fn extract_new_columns<'a>(
    requests: &'a [(RegionId, RegionCreateRequest)],
    physical_columns: &HashMap<String, ColumnId>,
    new_column_names: &mut HashSet<&'a str>,
    new_columns: &mut Vec<ColumnMetadata>,
) -> Result<()> {
    for (_, request) in requests {
        for col in &request.column_metadatas {
            if !physical_columns.contains_key(&col.column_schema.name)
                && !new_column_names.contains(&col.column_schema.name.as_str())
            {
                ensure!(
                    col.semantic_type != SemanticType::Field,
                    AddingFieldColumnSnafu {
                        name: col.column_schema.name.to_string(),
                    }
                );
                new_column_names.insert(&col.column_schema.name);
                // TODO(weny): avoid clone
                new_columns.push(col.clone());
            }
        }
    }

    Ok(())
}
