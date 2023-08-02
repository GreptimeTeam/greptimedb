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

//! Metadata of mito regions.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datatypes::prelude::DataType;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::{ColumnId, RegionId};

use crate::error::{InvalidMetaSnafu, InvalidSchemaSnafu, Result, SerdeJsonSnafu};
use crate::region::VersionNumber;

/// Initial version number of a new region.
pub(crate) const INIT_REGION_VERSION: VersionNumber = 0;

#[cfg_attr(doc, aquamarine::aquamarine)]
/// Static metadata of a region.
///
/// This struct implements [Serialize] and [Deserialize] traits.
/// To build a [RegionMetadata] object, use [RegionMetadataBuilder].
///
/// ```mermaid
/// class RegionMetadata {
///     +RegionId region_id
///     +VersionNumber version
///     +SchemaRef schema
///     +Vec&lt;ColumnMetadata&gt; column_metadatas
///     +Vec&lt;ColumnId&gt; primary_key
/// }
/// class Schema
/// class ColumnMetadata {
///     +ColumnSchema column_schema
///     +SemanticTyle semantic_type
///     +ColumnId column_id
/// }
/// class SemanticType
/// RegionMetadata o-- Schema
/// RegionMetadata o-- ColumnMetadata
/// ColumnMetadata o-- SemanticType
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RegionMetadata {
    /// Latest schema constructed from [column_metadatas](RegionMetadata::column_metadatas).
    #[serde(skip)]
    pub schema: SchemaRef,

    // We don't pub `time_index` and `id_to_index` and always construct them via [SkippedFields]
    // so we can assumes they are valid.
    /// Id of the time index column.
    #[serde(skip)]
    time_index: ColumnId,
    /// Map column id to column's index in [column_metadatas](RegionMetadata::column_metadatas).
    #[serde(skip)]
    id_to_index: HashMap<ColumnId, usize>,

    /// Columns in the region. Has the same order as columns
    /// in [schema](RegionMetadata::schema).
    pub column_metadatas: Vec<ColumnMetadata>,
    /// Version of metadata.
    pub version: VersionNumber,
    /// Maintains an ordered list of primary keys
    pub primary_key: Vec<ColumnId>,

    /// Immutable and unique id of a region.
    pub region_id: RegionId,
}

pub type RegionMetadataRef = Arc<RegionMetadata>;

impl<'de> Deserialize<'de> for RegionMetadata {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // helper internal struct for deserialization
        #[derive(Deserialize)]
        struct RegionMetadataWithoutSchema {
            column_metadatas: Vec<ColumnMetadata>,
            version: VersionNumber,
            primary_key: Vec<ColumnId>,
            region_id: RegionId,
        }

        let without_schema = RegionMetadataWithoutSchema::deserialize(deserializer)?;
        let skipped =
            SkippedFields::new(&without_schema.column_metadatas).map_err(D::Error::custom)?;

        Ok(Self {
            schema: skipped.schema,
            time_index: skipped.time_index,
            id_to_index: skipped.id_to_index,
            column_metadatas: without_schema.column_metadatas,
            version: without_schema.version,
            primary_key: without_schema.primary_key,
            region_id: without_schema.region_id,
        })
    }
}

impl RegionMetadata {
    /// Encode the metadata to a JSON string.
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(&self).context(SerdeJsonSnafu)
    }

    /// Find column by id.
    pub(crate) fn column_by_id(&self, column_id: ColumnId) -> Option<&ColumnMetadata> {
        self.id_to_index
            .get(&column_id)
            .map(|index| &self.column_metadatas[*index])
    }

    /// Returns the time index column
    ///
    /// # Panics
    /// Panics if the time index column id is invalid.
    pub(crate) fn time_index_column(&self) -> &ColumnMetadata {
        let index = self.id_to_index[&self.time_index];
        &self.column_metadatas[index]
    }

    /// Checks whether the metadata is valid.
    fn validate(&self) -> Result<()> {
        // Id to name.
        let mut id_names = HashMap::with_capacity(self.column_metadatas.len());
        for col in &self.column_metadatas {
            // Validate each column.
            col.validate()?;

            // Check whether column id is duplicated. We already check column name
            // is unique in `Schema` so we only check column id here.
            ensure!(
                !id_names.contains_key(&col.column_id),
                InvalidMetaSnafu {
                    reason: format!(
                        "column {} and {} have the same column id",
                        id_names[&col.column_id], col.column_schema.name
                    ),
                }
            );
            id_names.insert(col.column_id, &col.column_schema.name);
        }

        // Checks there is only one time index.
        let num_time_index = self
            .column_metadatas
            .iter()
            .filter(|col| col.semantic_type == SemanticType::Timestamp)
            .count();
        ensure!(
            num_time_index == 1,
            InvalidMetaSnafu {
                reason: format!("expect only one time index, found {}", num_time_index),
            }
        );

        // Checks the time index column is not nullable.
        ensure!(
            !self.time_index_column().column_schema.is_nullable(),
            InvalidMetaSnafu {
                reason: format!(
                    "time index column {} must be NOT NULL",
                    self.time_index_column().column_schema.name
                ),
            }
        );

        if !self.primary_key.is_empty() {
            let mut pk_ids = HashSet::with_capacity(self.primary_key.len());
            // Checks column ids in the primary key is valid.
            for column_id in &self.primary_key {
                // Checks whether the column id exists.
                ensure!(
                    id_names.contains_key(column_id),
                    InvalidMetaSnafu {
                        reason: format!("unknown column id {}", column_id),
                    }
                );

                // Checks duplicate.
                ensure!(
                    !pk_ids.contains(&column_id),
                    InvalidMetaSnafu {
                        reason: format!("duplicate column {} in primary key", id_names[column_id]),
                    }
                );

                // Checks this is not a time index column.
                ensure!(
                    *column_id != self.time_index,
                    InvalidMetaSnafu {
                        reason: format!(
                            "column {} is already a time index column",
                            id_names[column_id]
                        ),
                    }
                );

                pk_ids.insert(column_id);
            }
        }

        Ok(())
    }
}

/// Fields skipped in serialization.
struct SkippedFields {
    /// Last schema.
    schema: SchemaRef,
    /// Id of the time index column.
    time_index: ColumnId,
    /// Map column id to column's index in [column_metadatas](RegionMetadata::column_metadatas).
    id_to_index: HashMap<ColumnId, usize>,
}

impl SkippedFields {
    /// Constructs skipped fields from `column_metadatas`.
    fn new(column_metadatas: &[ColumnMetadata]) -> Result<SkippedFields> {
        let column_schemas = column_metadatas
            .iter()
            .map(|column_metadata| column_metadata.column_schema.clone())
            .collect();
        let schema = Arc::new(Schema::try_new(column_schemas).context(InvalidSchemaSnafu)?);
        let time_index = column_metadatas
            .iter()
            .find_map(|col| {
                if col.semantic_type == SemanticType::Timestamp {
                    Some(col.column_id)
                } else {
                    None
                }
            })
            .context(InvalidMetaSnafu {
                reason: "time index not found",
            })?;
        let id_to_index = column_metadatas
            .iter()
            .enumerate()
            .map(|(idx, col)| (col.column_id, idx))
            .collect();

        Ok(SkippedFields {
            schema,
            time_index,
            id_to_index,
        })
    }
}

/// Builder to build [RegionMetadata].
pub struct RegionMetadataBuilder {
    region_id: RegionId,
    version: VersionNumber,
    column_metadatas: Vec<ColumnMetadata>,
    primary_key: Vec<ColumnId>,
}

impl RegionMetadataBuilder {
    /// Returns a new builder.
    pub fn new(id: RegionId, version: VersionNumber) -> Self {
        Self {
            region_id: id,
            version,
            column_metadatas: vec![],
            primary_key: vec![],
        }
    }

    /// Create a builder from existing [RegionMetadata].
    pub fn from_existing(existing: RegionMetadata, new_version: VersionNumber) -> Self {
        Self {
            column_metadatas: existing.column_metadatas,
            version: new_version,
            primary_key: existing.primary_key,
            region_id: existing.region_id,
        }
    }

    /// Push a new column metadata to this region's metadata.
    pub fn push_column_metadata(&mut self, column_metadata: ColumnMetadata) -> &mut Self {
        self.column_metadatas.push(column_metadata);
        self
    }

    /// Set the primary key of the region.
    pub fn primary_key(&mut self, key: Vec<ColumnId>) -> &mut Self {
        self.primary_key = key;
        self
    }

    /// Consume the builder and build a [RegionMetadata].
    pub fn build(self) -> Result<RegionMetadata> {
        let skipped = SkippedFields::new(&self.column_metadatas)?;

        let meta = RegionMetadata {
            schema: skipped.schema,
            time_index: skipped.time_index,
            id_to_index: skipped.id_to_index,
            column_metadatas: self.column_metadatas,
            version: self.version,
            primary_key: self.primary_key,
            region_id: self.region_id,
        };

        meta.validate()?;

        Ok(meta)
    }
}

/// Metadata of a column.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnMetadata {
    /// Schema of this column. Is the same as `column_schema` in [SchemaRef].
    pub column_schema: ColumnSchema,
    /// Semantic type of this column (e.g. tag or timestamp).
    pub semantic_type: SemanticType,
    /// Immutable and unique id of a region.
    pub column_id: ColumnId,
}

impl ColumnMetadata {
    /// Checks whether it is a valid column.
    pub fn validate(&self) -> Result<()> {
        if self.semantic_type == SemanticType::Timestamp {
            ensure!(
                self.column_schema.data_type.is_timestamp_compatible(),
                InvalidMetaSnafu {
                    reason: format!("{} is not timestamp compatible", self.column_schema.name),
                }
            );
        }

        Ok(())
    }
}

/// The semantic type of one column
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SemanticType {
    /// Tag column, also is a part of primary key.
    Tag = 0,
    /// A column that isn't a time index or part of primary key.
    Field = 1,
    /// Time index column.
    Timestamp = 2,
}

/// Fields skipped in serialization.
struct SkippedFields {
    /// Last schema.
    schema: SchemaRef,
    /// Id of the time index column.
    time_index: ColumnId,
    /// Map column id to column's index in [column_metadatas](RegionMetadata::column_metadatas).
    id_to_index: HashMap<ColumnId, usize>,
}

impl SkippedFields {
    /// Constructs skipped fields from `column_metadatas`.
    fn new(column_metadatas: &[ColumnMetadata]) -> Result<SkippedFields> {
        let column_schemas = column_metadatas
            .iter()
            .map(|column_metadata| column_metadata.column_schema.clone())
            .collect();
        let schema = Arc::new(Schema::try_new(column_schemas).context(InvalidSchemaSnafu)?);
        let time_index = column_metadatas
            .iter()
            .find_map(|col| {
                if col.semantic_type == SemanticType::Timestamp {
                    Some(col.column_id)
                } else {
                    None
                }
            })
            .context(InvalidMetaSnafu {
                reason: "time index not found",
            })?;
        let id_to_index = column_metadatas
            .iter()
            .enumerate()
            .map(|(idx, col)| (col.column_id, idx))
            .collect();

        Ok(SkippedFields {
            schema,
            time_index,
            id_to_index,
        })
    }
}

#[cfg(test)]
mod test {
    use datatypes::prelude::ConcreteDataType;

    use super::*;

    fn create_builder() -> RegionMetadataBuilder {
        RegionMetadataBuilder::new(RegionId::new(1234, 5678), 9)
    }

    fn build_test_region_metadata() -> RegionMetadata {
        let mut builder = create_builder();
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("a", ConcreteDataType::int64_datatype(), false),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("b", ConcreteDataType::float64_datatype(), false),
                semantic_type: SemanticType::Field,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "c",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 3,
            })
            .primary_key(vec![1]);
        builder.build().unwrap()
    }

    #[test]
    fn test_region_metadata() {
        let region_metadata = build_test_region_metadata();
        assert_eq!("c", region_metadata.time_index_column().column_schema.name);
        assert_eq!(
            "a",
            region_metadata.column_by_id(1).unwrap().column_schema.name
        );
        assert_eq!(None, region_metadata.column_by_id(10));
    }

    #[test]
    fn test_region_metadata_serde() {
        let region_metadata = build_test_region_metadata();
        let serialized = serde_json::to_string(&region_metadata).unwrap();
        let deserialized: RegionMetadata = serde_json::from_str(&serialized).unwrap();
        assert_eq!(region_metadata, deserialized);
    }

    #[test]
    fn test_column_metadata_validate() {
        let mut builder = create_builder();
        let col = ColumnMetadata {
            column_schema: ColumnSchema::new("ts", ConcreteDataType::string_datatype(), false),
            semantic_type: SemanticType::Timestamp,
            column_id: 1,
        };
        col.validate().unwrap_err();

        builder.push_column_metadata(col);
        let err = builder.build().unwrap_err();
        assert!(
            err.to_string().contains("ts is not timestamp compatible"),
            "unexpected err: {err}",
        );
    }

    #[test]
    fn test_empty_region_metadata() {
        let builder = create_builder();
        let err = builder.build().unwrap_err();
        // A region must have a time index.
        assert!(
            err.to_string().contains("time index not found"),
            "unexpected err: {err}",
        );
    }

    #[test]
    fn test_same_column_id() {
        let mut builder = create_builder();
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("a", ConcreteDataType::int64_datatype(), false),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "b",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 1,
            });
        let err = builder.build().unwrap_err();
        assert!(
            err.to_string()
                .contains("column a and b have the same column id"),
            "unexpected err: {err}",
        );
    }

    #[test]
    fn test_duplicate_time_index() {
        let mut builder = create_builder();
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "a",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "b",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 2,
            });
        let err = builder.build().unwrap_err();
        assert!(
            err.to_string().contains("expect only one time index"),
            "unexpected err: {err}",
        );
    }

    #[test]
    fn test_unknown_primary_key() {
        let mut builder = create_builder();
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("a", ConcreteDataType::string_datatype(), false),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "b",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 2,
            })
            .primary_key(vec![3]);
        let err = builder.build().unwrap_err();
        assert!(
            err.to_string().contains("unknown column id 3"),
            "unexpected err: {err}",
        );
    }

    #[test]
    fn test_same_primary_key() {
        let mut builder = create_builder();
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("a", ConcreteDataType::string_datatype(), false),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "b",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 2,
            })
            .primary_key(vec![1, 1]);
        let err = builder.build().unwrap_err();
        assert!(
            err.to_string()
                .contains("duplicate column a in primary key"),
            "unexpected err: {err}",
        );
    }

    #[test]
    fn test_in_time_index() {
        let mut builder = create_builder();
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 1,
            })
            .primary_key(vec![1]);
        let err = builder.build().unwrap_err();
        assert!(
            err.to_string()
                .contains("column ts is already a time index column"),
            "unexpected err: {err}",
        );
    }

    #[test]
    fn test_nullable_time_index() {
        let mut builder = create_builder();
        builder.push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                true,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 1,
        });
        let err = builder.build().unwrap_err();
        assert!(
            err.to_string()
                .contains("time index column ts must be NOT NULL"),
            "unexpected err: {err}",
        );
    }
}
