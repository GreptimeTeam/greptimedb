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
use store_api::region_request::{AlterKind, RegionAlterRequest};
use store_api::storage::RegionId;

use crate::engine::state::PhysicalColumnInfo;
use crate::error::{AddingFieldColumnSnafu, Result};

/// Extract new columns from the create requests.
///
/// # Panics
///
/// This function will panic if the alter kind is not `AddColumns`.
pub fn extract_new_columns<'a>(
    requests: &'a [(RegionId, RegionAlterRequest)],
    physical_columns: &HashMap<String, PhysicalColumnInfo>,
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
                ensure!(
                    col.column_metadata.semantic_type != SemanticType::Field,
                    AddingFieldColumnSnafu {
                        name: column_name.to_string(),
                    }
                );
                new_column_names.insert(column_name);
                // TODO(weny): avoid clone
                new_columns.push(col.column_metadata.clone());
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use api::v1::SemanticType;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::ColumnMetadata;
    use store_api::region_request::{AddColumn, AlterKind, RegionAlterRequest};
    use store_api::storage::RegionId;

    use super::*;
    use crate::error::Error;

    #[test]
    fn test_extract_new_columns_with_field_type() {
        let requests = vec![(
            RegionId::new(1, 1),
            RegionAlterRequest {
                kind: AlterKind::AddColumns {
                    columns: vec![AddColumn {
                        column_metadata: ColumnMetadata {
                            column_schema: ColumnSchema::new(
                                "new_column".to_string(),
                                ConcreteDataType::string_datatype(),
                                false,
                            ),
                            semantic_type: SemanticType::Field,
                            column_id: 0,
                        },
                        location: None,
                    }],
                },
            },
        )];

        let physical_columns = HashMap::new();
        let mut new_column_names = HashSet::new();
        let mut new_columns = Vec::new();

        let err = extract_new_columns(
            &requests,
            &physical_columns,
            &mut new_column_names,
            &mut new_columns,
        )
        .unwrap_err();

        assert!(matches!(err, Error::AddingFieldColumn { .. }));
    }
}
