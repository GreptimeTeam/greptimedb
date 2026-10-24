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
use store_api::region_request::{AlterKind, RegionAlterRequest};
use store_api::storage::RegionId;

use crate::error::{AddingFieldColumnSnafu, Result};

/// Extract new columns from the create requests.
///
/// # Panics
///
/// This function will panic if the alter kind is not `AddColumns`.
pub fn extract_new_columns<'a>(
    requests: &'a [(RegionId, RegionAlterRequest)],
    physical_columns: &HashMap<String, ColumnMetadata>,
    new_column_indices: &mut HashMap<&'a str, usize>,
    new_columns: &mut Vec<ColumnMetadata>,
) -> Result<()> {
    for (region_id, request) in requests {
        let AlterKind::AddColumns { columns } = &request.kind else {
            unreachable!()
        };
        for col in columns {
            let column_name = col.column_metadata.column_schema.name.as_str();
            if let Some(existing) = physical_columns.get(column_name).or_else(|| {
                new_column_indices
                    .get(column_name)
                    .map(|&index| &new_columns[index])
            }) {
                crate::engine::validate_shared_column(*region_id, existing, &col.column_metadata)?;
            } else {
                ensure!(
                    col.column_metadata.semantic_type != SemanticType::Field
                        || is_native_histogram_value_type(
                            &col.column_metadata.column_schema.data_type
                        ),
                    AddingFieldColumnSnafu {
                        name: column_name.to_string(),
                    }
                );
                new_column_indices.insert(column_name, new_columns.len());
                // TODO(weny): avoid clone
                new_columns.push(col.column_metadata.clone());
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use api::v1::SemanticType;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::ColumnMetadata;
    use store_api::region_request::{AddColumn, AlterKind, RegionAlterRequest};
    use store_api::storage::RegionId;

    use super::*;
    use crate::error::Error;

    #[test]
    fn test_alter_rejects_conflicting_shared_definitions() {
        let column = ColumnMetadata {
            column_schema: ColumnSchema::new("shared", ConcreteDataType::string_datatype(), true),
            semantic_type: SemanticType::Tag,
            column_id: 0,
        };
        let mut incompatible = column.clone();
        incompatible.column_schema =
            ColumnSchema::new("shared", ConcreteDataType::int64_datatype(), true);
        for existing in [false, true] {
            let physical = if existing {
                HashMap::from([("shared".into(), column.clone())])
            } else {
                HashMap::new()
            };
            let requests = [(
                RegionId::new(1, 1),
                RegionAlterRequest {
                    kind: AlterKind::AddColumns {
                        columns: [column.clone(), incompatible.clone()]
                            .into_iter()
                            .map(|column_metadata| AddColumn {
                                column_metadata,
                                location: None,
                            })
                            .collect(),
                    },
                },
            )];
            let error =
                extract_new_columns(&requests, &physical, &mut HashMap::new(), &mut Vec::new())
                    .unwrap_err();
            assert!(matches!(error, Error::InvalidRequest { .. }));
        }
    }

    #[test]
    fn test_extract_new_columns_preserves_first_seen_order() {
        let requests = [["b", "a"], ["a", "b"]].map(|names| {
            (
                RegionId::new(1, 1),
                RegionAlterRequest {
                    kind: AlterKind::AddColumns {
                        columns: names
                            .into_iter()
                            .map(|name| AddColumn {
                                column_metadata: ColumnMetadata {
                                    column_schema: ColumnSchema::new(
                                        name,
                                        ConcreteDataType::string_datatype(),
                                        true,
                                    ),
                                    semantic_type: SemanticType::Tag,
                                    column_id: 0,
                                },
                                location: None,
                            })
                            .collect(),
                    },
                },
            )
        });
        let mut indices = HashMap::new();
        let mut columns = Vec::new();
        extract_new_columns(&requests, &HashMap::new(), &mut indices, &mut columns).unwrap();
        assert_eq!(indices, HashMap::from([("b", 0), ("a", 1)]));
        assert_eq!(
            columns
                .iter()
                .map(|c| c.column_schema.name.as_str())
                .collect::<Vec<_>>(),
            ["b", "a"]
        );
    }

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
        let mut new_column_indices = HashMap::new();
        let mut new_columns = Vec::new();

        let err = extract_new_columns(
            &requests,
            &physical_columns,
            &mut new_column_indices,
            &mut new_columns,
        )
        .unwrap_err();

        assert!(matches!(err, Error::AddingFieldColumn { .. }));
    }
}
