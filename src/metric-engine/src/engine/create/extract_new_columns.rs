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

use std::collections::HashMap;

use api::v1::SemanticType;
use common_query::native_histogram::is_native_histogram_value_type;
use snafu::ensure;
use store_api::metadata::ColumnMetadata;
use store_api::region_request::RegionCreateRequest;
use store_api::storage::RegionId;

use crate::error::{AddingFieldColumnSnafu, Result};

/// Extract new columns from the create requests.
pub fn extract_new_columns<'a>(
    requests: &'a [(RegionId, RegionCreateRequest)],
    physical_columns: &HashMap<String, ColumnMetadata>,
    new_column_indices: &mut HashMap<&'a str, usize>,
    new_columns: &mut Vec<ColumnMetadata>,
) -> Result<()> {
    for (region_id, request) in requests {
        for col in &request.column_metadatas {
            if let Some(existing) = physical_columns.get(&col.column_schema.name).or_else(|| {
                new_column_indices
                    .get(col.column_schema.name.as_str())
                    .map(|&index| &new_columns[index])
            }) {
                super::super::validate_shared_column(*region_id, existing, col)?;
            } else {
                ensure!(
                    col.semantic_type != SemanticType::Field
                        || is_native_histogram_value_type(&col.column_schema.data_type),
                    AddingFieldColumnSnafu {
                        name: col.column_schema.name.clone(),
                    }
                );
                new_column_indices.insert(&col.column_schema.name, new_columns.len());
                // TODO(weny): avoid clone
                new_columns.push(col.clone());
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::assert_matches;
    use std::collections::HashMap;

    use api::v1::SemanticType;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::ColumnMetadata;
    use store_api::region_request::{PathType, RegionCreateRequest};
    use store_api::storage::RegionId;

    use super::*;
    use crate::error::Error;

    #[test]
    fn test_conflicting_shared_definitions_rejected() {
        let column = ColumnMetadata {
            column_schema: ColumnSchema::new("shared", ConcreteDataType::string_datatype(), true),
            semantic_type: SemanticType::Tag,
            column_id: 0,
        };
        let mut incompatible = column.clone();
        incompatible.semantic_type = SemanticType::Field;
        let region = RegionId::new(1, 1);
        assert!(
            super::super::super::validate_shared_column(region, &column, &incompatible).is_err()
        );
        incompatible.semantic_type = SemanticType::Tag;
        incompatible.column_schema =
            ColumnSchema::new("shared", ConcreteDataType::int64_datatype(), true);
        assert!(
            super::super::super::validate_shared_column(region, &column, &incompatible).is_err()
        );
        assert!(super::super::super::validate_shared_column(region, &column, &column).is_ok());
        for existing in [false, true] {
            let physical = if existing {
                HashMap::from([("shared".into(), column.clone())])
            } else {
                HashMap::new()
            };
            let requests = [(
                region,
                RegionCreateRequest {
                    column_metadatas: vec![column.clone(), incompatible.clone()],
                    engine: "metric".into(),
                    primary_key: vec![0],
                    options: HashMap::new(),
                    table_dir: "test".into(),
                    path_type: PathType::Bare,
                    partition_expr_json: None,
                    requirements: Default::default(),
                },
            )];
            let error =
                extract_new_columns(&requests, &physical, &mut HashMap::new(), &mut Vec::new())
                    .unwrap_err();
            assert!(matches!(error, Error::InvalidRequest { .. }));
        }
    }

    #[test]
    fn test_extract_new_columns() {
        let mut requests = vec![
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
                    table_dir: "test".to_string(),
                    path_type: PathType::Bare,
                    partition_expr_json: Some("".to_string()),
                    requirements: Default::default(),
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
                    table_dir: "test".to_string(),
                    path_type: PathType::Bare,
                    partition_expr_json: Some("".to_string()),
                    requirements: Default::default(),
                },
            ),
        ];

        let mut another = requests[0].1.column_metadatas[1].clone();
        another.column_schema.name = "another_column".into();
        requests[0].1.column_metadatas.push(another.clone());
        requests[1].1.column_metadatas.push(another);

        let mut physical_columns = HashMap::new();
        physical_columns.insert(
            "existing_column".to_string(),
            ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "existing_column".to_string(),
                    ConcreteDataType::string_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 0,
            },
        );
        let mut new_column_indices = HashMap::new();
        let mut new_columns = Vec::new();

        let result = extract_new_columns(
            &requests,
            &physical_columns,
            &mut new_column_indices,
            &mut new_columns,
        );

        assert!(result.is_ok());
        assert_eq!(new_column_indices["new_column"], 0);
        assert_eq!(new_column_indices["another_column"], 1);
        assert_eq!(new_columns.len(), 2);
        assert_eq!(new_columns[0].column_schema.name, "new_column");
        assert_eq!(new_columns[1].column_schema.name, "another_column");
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
                table_dir: "test".to_string(),
                path_type: PathType::Bare,
                partition_expr_json: Some("".to_string()),
                requirements: Default::default(),
            },
        )];

        let physical_columns = HashMap::new();
        let mut new_column_indices = HashMap::new();
        let mut new_columns = Vec::new();

        let err = extract_new_columns(
            &requests,
            &physical_columns,
            &mut new_column_indices,
            &mut new_columns,
        )
        .unwrap_err();

        assert_matches!(err, Error::AddingFieldColumn { .. });
    }
}
