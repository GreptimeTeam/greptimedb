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

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::collections::{HashMap, HashSet};

    use api::v1::SemanticType;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::ColumnMetadata;
    use store_api::region_request::RegionCreateRequest;
    use store_api::storage::RegionId;

    use super::*;
    use crate::error::Error;

    #[test]
    fn test_extract_new_columns() {
        let requests = vec![
            (
                RegionId::new(1, 1),
                RegionCreateRequest {
                    column_metadatas: vec![
                        ColumnMetadata {
                            column_schema: ColumnSchema::new(
                                "existing_column".to_string(),
                                ConcreteDataType::string_datatype(),
                                false,
                            ),
                            semantic_type: SemanticType::Tag,
                            column_id: 0,
                        },
                        ColumnMetadata {
                            column_schema: ColumnSchema::new(
                                "new_column".to_string(),
                                ConcreteDataType::string_datatype(),
                                false,
                            ),
                            semantic_type: SemanticType::Tag,
                            column_id: 0,
                        },
                    ],
                    engine: "test".to_string(),
                    primary_key: vec![],
                    options: HashMap::new(),
                    region_dir: "test".to_string(),
                },
            ),
            (
                RegionId::new(1, 2),
                RegionCreateRequest {
                    column_metadatas: vec![ColumnMetadata {
                        // Duplicate column name
                        column_schema: ColumnSchema::new(
                            "new_column".to_string(),
                            ConcreteDataType::string_datatype(),
                            false,
                        ),
                        semantic_type: SemanticType::Tag,
                        column_id: 0,
                    }],
                    engine: "test".to_string(),
                    primary_key: vec![],
                    options: HashMap::new(),
                    region_dir: "test".to_string(),
                },
            ),
        ];

        let mut physical_columns = HashMap::new();
        physical_columns.insert("existing_column".to_string(), 0);
        let mut new_column_names = HashSet::new();
        let mut new_columns = Vec::new();

        let result = extract_new_columns(
            &requests,
            &physical_columns,
            &mut new_column_names,
            &mut new_columns,
        );

        assert!(result.is_ok());
        assert!(new_column_names.contains("new_column"));
        assert_eq!(new_columns.len(), 1);
        assert_eq!(new_columns[0].column_schema.name, "new_column");
    }

    #[test]
    fn test_extract_new_columns_with_field_type() {
        let requests = vec![(
            RegionId::new(1, 1),
            RegionCreateRequest {
                column_metadatas: vec![ColumnMetadata {
                    column_schema: ColumnSchema::new(
                        "new_column".to_string(),
                        ConcreteDataType::string_datatype(),
                        false,
                    ),
                    semantic_type: SemanticType::Field,
                    column_id: 0,
                }],
                engine: "test".to_string(),
                primary_key: vec![],
                options: HashMap::new(),
                region_dir: "test".to_string(),
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

        assert_matches!(err, Error::AddingFieldColumn { .. });
    }
}
