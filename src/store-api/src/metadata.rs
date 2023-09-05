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

//! Metadata of region and column.
//!
//! This mod has its own error type [MetadataError] for validation and codec exceptions.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use api::helper::ColumnDataTypeWrapper;
use api::v1::region::RegionColumnDef;
use api::v1::SemanticType;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use datatypes::arrow::datatypes::FieldRef;
use datatypes::prelude::DataType;
use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema, Schema, SchemaRef};
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize};
use snafu::{ensure, Location, OptionExt, ResultExt, Snafu};

use crate::storage::{ColumnId, RegionId};

pub type Result<T> = std::result::Result<T, MetadataError>;

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
    /// Construct `Self` from protobuf struct [ColumnDef]
    pub fn try_from_column_def(column_def: RegionColumnDef) -> Result<Self> {
        let column_id = column_def.column_id;

        let column_def = column_def
            .column_def
            .context(InvalidRawRegionRequestSnafu {
                err: "column_def is absent",
            })?;

        let semantic_type = column_def.semantic_type();

        let default_constrain = if column_def.default_constraint.is_empty() {
            None
        } else {
            Some(
                ColumnDefaultConstraint::try_from(column_def.default_constraint.as_slice())
                    .context(ConvertDatatypesSnafu)?,
            )
        };
        let data_type = ColumnDataTypeWrapper::new(column_def.data_type()).into();
        let column_schema = ColumnSchema::new(column_def.name, data_type, column_def.is_nullable)
            .with_default_constraint(default_constrain)
            .context(ConvertDatatypesSnafu)?;
        Ok(Self {
            column_schema,
            semantic_type,
            column_id,
        })
    }
}

#[cfg_attr(doc, aquamarine::aquamarine)]
/// General static metadata of a region.
///
/// This struct implements [Serialize] and [Deserialize] traits.
/// To build a [RegionMetadata] object, use [RegionMetadataBuilder].
///
/// ```mermaid
/// class RegionMetadata {
///     +RegionId region_id
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
            primary_key: without_schema.primary_key,
            region_id: without_schema.region_id,
        })
    }
}

impl RegionMetadata {
    /// Decode the metadata from a JSON str.
    pub fn from_json(s: &str) -> Result<Self> {
        serde_json::from_str(s).context(SerdeJsonSnafu)
    }

    /// Encode the metadata to a JSON string.
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(&self).context(SerdeJsonSnafu)
    }

    /// Find column by id.
    pub fn column_by_id(&self, column_id: ColumnId) -> Option<&ColumnMetadata> {
        self.id_to_index
            .get(&column_id)
            .map(|index| &self.column_metadatas[*index])
    }

    /// Find column index by id.
    pub fn column_index_by_id(&self, column_id: ColumnId) -> Option<usize> {
        self.id_to_index.get(&column_id).copied()
    }

    /// Returns the time index column
    ///
    /// # Panics
    /// Panics if the time index column id is invalid.
    pub fn time_index_column(&self) -> &ColumnMetadata {
        let index = self.id_to_index[&self.time_index];
        &self.column_metadatas[index]
    }

    /// Returns the arrow field of the time index column.
    pub fn time_index_field(&self) -> FieldRef {
        let index = self.id_to_index[&self.time_index];
        self.schema.arrow_schema().fields[index].clone()
    }

    /// Finds a column by name.
    pub fn column_by_name(&self, name: &str) -> Option<&ColumnMetadata> {
        self.schema
            .column_index_by_name(name)
            .map(|index| &self.column_metadatas[index])
    }

    /// Returns all primary key columns.
    pub fn primary_key_columns(&self) -> impl Iterator<Item = &ColumnMetadata> {
        // safety: RegionMetadata::validate ensures every primary key exists.
        self.primary_key
            .iter()
            .map(|id| self.column_by_id(*id).unwrap())
    }

    /// Returns all field columns.
    pub fn field_columns(&self) -> impl Iterator<Item = &ColumnMetadata> {
        self.column_metadatas
            .iter()
            .filter(|column| column.semantic_type == SemanticType::Field)
    }

    /// Returns a column's index in primary key if it is a primary key column.
    ///
    /// This does a linear search.
    pub fn primary_key_index(&self, column_id: ColumnId) -> Option<usize> {
        self.primary_key.iter().position(|id| *id == column_id)
    }

    /// Returns a column's index in fields if it is a field column.
    ///
    /// This does a linear search.
    pub fn field_index(&self, column_id: ColumnId) -> Option<usize> {
        self.field_columns()
            .position(|column| column.column_id == column_id)
    }

    /// Checks whether the metadata is valid.
    fn validate(&self) -> Result<()> {
        // Id to name.
        let mut id_names = HashMap::with_capacity(self.column_metadatas.len());
        for col in &self.column_metadatas {
            // Validate each column.
            Self::validate_column_metadata(col)?;

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

                // Safety: Column with specific id must exist.
                let column = self.column_by_id(*column_id).unwrap();
                // Checks duplicate.
                ensure!(
                    !pk_ids.contains(&column_id),
                    InvalidMetaSnafu {
                        reason: format!(
                            "duplicate column {} in primary key",
                            column.column_schema.name
                        ),
                    }
                );

                // Checks this is not a time index column.
                ensure!(
                    *column_id != self.time_index,
                    InvalidMetaSnafu {
                        reason: format!(
                            "column {} is already a time index column",
                            column.column_schema.name,
                        ),
                    }
                );

                // Checks semantic type.
                ensure!(
                    column.semantic_type == SemanticType::Tag,
                    InvalidMetaSnafu {
                        reason: format!(
                            "semantic type of column {} should be Tag, not {:?}",
                            column.column_schema.name, column.semantic_type
                        ),
                    }
                );

                pk_ids.insert(column_id);
            }
        }

        // Checks tag semantic type.
        let num_tag = self
            .column_metadatas
            .iter()
            .filter(|col| col.semantic_type == SemanticType::Tag)
            .count();
        ensure!(
            num_tag == self.primary_key.len(),
            InvalidMetaSnafu {
                reason: format!(
                    "number of primary key columns {} not equal to tag columns {}",
                    self.primary_key.len(),
                    num_tag
                ),
            }
        );

        Ok(())
    }

    /// Checks whether it is a valid column.
    fn validate_column_metadata(column_metadata: &ColumnMetadata) -> Result<()> {
        // TODO(yingwen): Ensure column name is not internal columns.
        if column_metadata.semantic_type == SemanticType::Timestamp {
            ensure!(
                column_metadata
                    .column_schema
                    .data_type
                    .is_timestamp_compatible(),
                InvalidMetaSnafu {
                    reason: format!(
                        "{} is not timestamp compatible",
                        column_metadata.column_schema.name
                    ),
                }
            );
        }

        Ok(())
    }
}

/// Builder to build [RegionMetadata].
pub struct RegionMetadataBuilder {
    region_id: RegionId,
    column_metadatas: Vec<ColumnMetadata>,
    primary_key: Vec<ColumnId>,
}

impl RegionMetadataBuilder {
    /// Returns a new builder.
    pub fn new(id: RegionId) -> Self {
        Self {
            region_id: id,
            column_metadatas: vec![],
            primary_key: vec![],
        }
    }

    /// Create a builder from existing [RegionMetadata].
    pub fn from_existing(existing: RegionMetadata) -> Self {
        Self {
            column_metadatas: existing.column_metadatas,
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
            primary_key: self.primary_key,
            region_id: self.region_id,
        };

        meta.validate()?;

        Ok(meta)
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

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum MetadataError {
    #[snafu(display("Invalid schema, source: {}, location: {}", source, location))]
    InvalidSchema {
        source: datatypes::error::Error,
        location: Location,
    },

    #[snafu(display("Invalid metadata, {}, location: {}", reason, location))]
    InvalidMeta { reason: String, location: Location },

    #[snafu(display(
        "Failed to ser/de json object. Location: {}, source: {}",
        location,
        source
    ))]
    SerdeJson {
        location: Location,
        source: serde_json::Error,
    },

    #[snafu(display(
        "Failed to convert with struct from datatypes, location: {}, source: {}",
        location,
        source
    ))]
    ConvertDatatypes {
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Invalid raw region request: {err}, at {location}"))]
    InvalidRawRegionRequest { err: String, location: Location },
}

impl ErrorExt for MetadataError {
    fn status_code(&self) -> StatusCode {
        StatusCode::InvalidArguments
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod test {
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;

    use super::*;

    fn create_builder() -> RegionMetadataBuilder {
        RegionMetadataBuilder::new(RegionId::new(1234, 5678))
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

    #[test]
    fn test_primary_key_semantic_type() {
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
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("a", ConcreteDataType::float64_datatype(), true),
                semantic_type: SemanticType::Field,
                column_id: 2,
            })
            .primary_key(vec![2]);
        let err = builder.build().unwrap_err();
        assert!(
            err.to_string()
                .contains("semantic type of column a should be Tag, not Field"),
            "unexpected err: {err}",
        );
    }

    #[test]
    fn test_primary_key_tag_num() {
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
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("a", ConcreteDataType::string_datatype(), true),
                semantic_type: SemanticType::Tag,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("b", ConcreteDataType::string_datatype(), true),
                semantic_type: SemanticType::Tag,
                column_id: 3,
            })
            .primary_key(vec![2]);
        let err = builder.build().unwrap_err();
        assert!(
            err.to_string()
                .contains("number of primary key columns 1 not equal to tag columns 2"),
            "unexpected err: {err}",
        );
    }
}
