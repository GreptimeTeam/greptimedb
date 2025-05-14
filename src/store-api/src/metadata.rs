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
use std::fmt;
use std::sync::Arc;

use api::v1::column_def::try_as_column_schema;
use api::v1::region::RegionColumnDef;
use api::v1::SemanticType;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use datatypes::arrow;
use datatypes::arrow::datatypes::FieldRef;
use datatypes::schema::{ColumnSchema, FulltextOptions, Schema, SchemaRef, SkippingIndexOptions};
use datatypes::types::TimestampType;
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize};
use snafu::{ensure, Location, OptionExt, ResultExt, Snafu};

use crate::codec::PrimaryKeyEncoding;
use crate::region_request::{
    AddColumn, AddColumnLocation, AlterKind, ApiSetIndexOptions, ApiUnsetIndexOptions,
    ModifyColumnType,
};
use crate::storage::consts::is_internal_column;
use crate::storage::{ColumnId, RegionId};

pub type Result<T> = std::result::Result<T, MetadataError>;

/// Metadata of a column.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnMetadata {
    /// Schema of this column. Is the same as `column_schema` in [SchemaRef].
    pub column_schema: ColumnSchema,
    /// Semantic type of this column (e.g. tag or timestamp).
    pub semantic_type: SemanticType,
    /// Immutable and unique id of a region.
    pub column_id: ColumnId,
}

impl fmt::Debug for ColumnMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{:?} {:?} {:?}]",
            self.column_schema, self.semantic_type, self.column_id,
        )
    }
}

impl ColumnMetadata {
    /// Construct `Self` from protobuf struct [RegionColumnDef]
    pub fn try_from_column_def(column_def: RegionColumnDef) -> Result<Self> {
        let column_id = column_def.column_id;
        let column_def = column_def
            .column_def
            .context(InvalidRawRegionRequestSnafu {
                err: "column_def is absent",
            })?;
        let semantic_type = column_def.semantic_type();
        let column_schema = try_as_column_schema(&column_def).context(ConvertColumnSchemaSnafu)?;

        Ok(Self {
            column_schema,
            semantic_type,
            column_id,
        })
    }

    /// Encodes a vector of `ColumnMetadata` into a JSON byte vector.
    pub fn encode_list(columns: &[Self]) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(columns)
    }

    /// Decodes a JSON byte vector into a vector of `ColumnMetadata`.
    pub fn decode_list(bytes: &[u8]) -> serde_json::Result<Vec<Self>> {
        serde_json::from_slice(bytes)
    }

    pub fn is_same_datatype(&self, other: &Self) -> bool {
        self.column_schema.data_type == other.column_schema.data_type
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
#[derive(Clone, PartialEq, Eq, Serialize)]
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
    /// Current version of the region schema.
    ///
    /// The version starts from 0. Altering the schema bumps the version.
    pub schema_version: u64,

    /// Primary key encoding mode.
    pub primary_key_encoding: PrimaryKeyEncoding,
}

impl fmt::Debug for RegionMetadata {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("RegionMetadata")
            .field("column_metadatas", &self.column_metadatas)
            .field("time_index", &self.time_index)
            .field("primary_key", &self.primary_key)
            .field("region_id", &self.region_id)
            .field("schema_version", &self.schema_version)
            .finish()
    }
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
            schema_version: u64,
            #[serde(default)]
            primary_key_encoding: PrimaryKeyEncoding,
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
            schema_version: without_schema.schema_version,
            primary_key_encoding: without_schema.primary_key_encoding,
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

    /// Find column index by name.
    pub fn column_index_by_name(&self, column_name: &str) -> Option<usize> {
        self.column_metadatas
            .iter()
            .position(|col| col.column_schema.name == column_name)
    }

    /// Returns the time index column
    ///
    /// # Panics
    /// Panics if the time index column id is invalid.
    pub fn time_index_column(&self) -> &ColumnMetadata {
        let index = self.id_to_index[&self.time_index];
        &self.column_metadatas[index]
    }

    /// Returns timestamp type of time index column
    ///
    /// # Panics
    /// Panics if the time index column id is invalid.
    pub fn time_index_type(&self) -> TimestampType {
        let index = self.id_to_index[&self.time_index];
        self.column_metadatas[index]
            .column_schema
            .data_type
            .as_timestamp()
            .unwrap()
    }

    /// Returns the position of the time index.
    pub fn time_index_column_pos(&self) -> usize {
        self.id_to_index[&self.time_index]
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

    /// Returns all field columns before projection.
    ///
    /// **Use with caution**. On read path where might have projection, this method
    /// can return columns that not present in data batch.
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

    /// Project the metadata to a new one using specified column ids.
    ///
    /// [RegionId] and schema version are preserved.
    pub fn project(&self, projection: &[ColumnId]) -> Result<RegionMetadata> {
        // check time index
        ensure!(
            projection.contains(&self.time_index),
            TimeIndexNotFoundSnafu
        );

        // prepare new indices
        let indices_to_preserve = projection
            .iter()
            .map(|id| {
                self.column_index_by_id(*id)
                    .with_context(|| InvalidRegionRequestSnafu {
                        region_id: self.region_id,
                        err: format!("column id {} not found", id),
                    })
            })
            .collect::<Result<Vec<_>>>()?;

        // project schema
        let projected_schema =
            self.schema
                .try_project(&indices_to_preserve)
                .with_context(|_| SchemaProjectSnafu {
                    origin_schema: self.schema.clone(),
                    projection: projection.to_vec(),
                })?;

        // project columns, generate projected primary key and new id_to_index
        let mut projected_column_metadatas = Vec::with_capacity(indices_to_preserve.len());
        let mut projected_primary_key = vec![];
        let mut projected_id_to_index = HashMap::with_capacity(indices_to_preserve.len());
        for index in indices_to_preserve {
            let col = self.column_metadatas[index].clone();
            if col.semantic_type == SemanticType::Tag {
                projected_primary_key.push(col.column_id);
            }
            projected_id_to_index.insert(col.column_id, projected_column_metadatas.len());
            projected_column_metadatas.push(col);
        }

        Ok(RegionMetadata {
            schema: Arc::new(projected_schema),
            time_index: self.time_index,
            id_to_index: projected_id_to_index,
            column_metadatas: projected_column_metadatas,
            primary_key: projected_primary_key,
            region_id: self.region_id,
            schema_version: self.schema_version,
            primary_key_encoding: self.primary_key_encoding,
        })
    }

    /// Gets the column ids to be indexed by inverted index.
    pub fn inverted_indexed_column_ids<'a>(
        &self,
        ignore_column_ids: impl Iterator<Item = &'a ColumnId>,
    ) -> HashSet<ColumnId> {
        let mut inverted_index = self
            .column_metadatas
            .iter()
            .filter(|column| column.column_schema.is_inverted_indexed())
            .map(|column| column.column_id)
            .collect::<HashSet<_>>();

        for ignored in ignore_column_ids {
            inverted_index.remove(ignored);
        }

        inverted_index
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
                        "column {} and {} have the same column id {}",
                        id_names[&col.column_id], col.column_schema.name, col.column_id,
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
        if column_metadata.semantic_type == SemanticType::Timestamp {
            ensure!(
                column_metadata.column_schema.data_type.is_timestamp(),
                InvalidMetaSnafu {
                    reason: format!(
                        "column `{}` is not timestamp type",
                        column_metadata.column_schema.name
                    ),
                }
            );
        }

        ensure!(
            !is_internal_column(&column_metadata.column_schema.name),
            InvalidMetaSnafu {
                reason: format!(
                    "{} is internal column name that can not be used",
                    column_metadata.column_schema.name
                ),
            }
        );

        Ok(())
    }
}

/// Builder to build [RegionMetadata].
pub struct RegionMetadataBuilder {
    region_id: RegionId,
    column_metadatas: Vec<ColumnMetadata>,
    primary_key: Vec<ColumnId>,
    schema_version: u64,
    primary_key_encoding: PrimaryKeyEncoding,
}

impl RegionMetadataBuilder {
    /// Returns a new builder.
    pub fn new(id: RegionId) -> Self {
        Self {
            region_id: id,
            column_metadatas: vec![],
            primary_key: vec![],
            schema_version: 0,
            primary_key_encoding: PrimaryKeyEncoding::Dense,
        }
    }

    /// Creates a builder from existing [RegionMetadata].
    pub fn from_existing(existing: RegionMetadata) -> Self {
        Self {
            column_metadatas: existing.column_metadatas,
            primary_key: existing.primary_key,
            region_id: existing.region_id,
            schema_version: existing.schema_version,
            primary_key_encoding: existing.primary_key_encoding,
        }
    }

    /// Sets the primary key encoding mode.
    pub fn primary_key_encoding(&mut self, encoding: PrimaryKeyEncoding) -> &mut Self {
        self.primary_key_encoding = encoding;
        self
    }

    /// Pushes a new column metadata to this region's metadata.
    pub fn push_column_metadata(&mut self, column_metadata: ColumnMetadata) -> &mut Self {
        self.column_metadatas.push(column_metadata);
        self
    }

    /// Sets the primary key of the region.
    pub fn primary_key(&mut self, key: Vec<ColumnId>) -> &mut Self {
        self.primary_key = key;
        self
    }

    /// Increases the schema version by 1.
    pub fn bump_version(&mut self) -> &mut Self {
        self.schema_version += 1;
        self
    }

    /// Applies the alter `kind` to the builder.
    ///
    /// The `kind` should be valid.
    pub fn alter(&mut self, kind: AlterKind) -> Result<&mut Self> {
        match kind {
            AlterKind::AddColumns { columns } => self.add_columns(columns)?,
            AlterKind::DropColumns { names } => self.drop_columns(&names),
            AlterKind::ModifyColumnTypes { columns } => self.modify_column_types(columns)?,
            AlterKind::SetIndex { options } => match options {
                ApiSetIndexOptions::Fulltext {
                    column_name,
                    options,
                } => self.change_column_fulltext_options(column_name, true, Some(options))?,
                ApiSetIndexOptions::Inverted { column_name } => {
                    self.change_column_inverted_index_options(column_name, true)?
                }
                ApiSetIndexOptions::Skipping {
                    column_name,
                    options,
                } => self.change_column_skipping_index_options(column_name, Some(options))?,
            },
            AlterKind::UnsetIndex { options } => match options {
                ApiUnsetIndexOptions::Fulltext { column_name } => {
                    self.change_column_fulltext_options(column_name, false, None)?
                }
                ApiUnsetIndexOptions::Inverted { column_name } => {
                    self.change_column_inverted_index_options(column_name, false)?
                }
                ApiUnsetIndexOptions::Skipping { column_name } => {
                    self.change_column_skipping_index_options(column_name, None)?
                }
            },
            AlterKind::SetRegionOptions { options: _ } => {
                // nothing to be done with RegionMetadata
            }
            AlterKind::UnsetRegionOptions { keys: _ } => {
                // nothing to be done with RegionMetadata
            }
        }
        Ok(self)
    }

    /// Consumes the builder and build a [RegionMetadata].
    pub fn build(self) -> Result<RegionMetadata> {
        let skipped = SkippedFields::new(&self.column_metadatas)?;

        let meta = RegionMetadata {
            schema: skipped.schema,
            time_index: skipped.time_index,
            id_to_index: skipped.id_to_index,
            column_metadatas: self.column_metadatas,
            primary_key: self.primary_key,
            region_id: self.region_id,
            schema_version: self.schema_version,
            primary_key_encoding: self.primary_key_encoding,
        };

        meta.validate()?;

        Ok(meta)
    }

    /// Adds columns to the metadata if not exist.
    fn add_columns(&mut self, columns: Vec<AddColumn>) -> Result<()> {
        let mut names: HashSet<_> = self
            .column_metadatas
            .iter()
            .map(|col| col.column_schema.name.clone())
            .collect();

        for add_column in columns {
            if names.contains(&add_column.column_metadata.column_schema.name) {
                // Column already exists.
                continue;
            }

            let column_id = add_column.column_metadata.column_id;
            let semantic_type = add_column.column_metadata.semantic_type;
            let column_name = add_column.column_metadata.column_schema.name.clone();
            match add_column.location {
                None => {
                    self.column_metadatas.push(add_column.column_metadata);
                }
                Some(AddColumnLocation::First) => {
                    self.column_metadatas.insert(0, add_column.column_metadata);
                }
                Some(AddColumnLocation::After { column_name }) => {
                    let pos = self
                        .column_metadatas
                        .iter()
                        .position(|col| col.column_schema.name == column_name)
                        .context(InvalidRegionRequestSnafu {
                            region_id: self.region_id,
                            err: format!(
                                "column {} not found, failed to add column {} after it",
                                column_name, add_column.column_metadata.column_schema.name
                            ),
                        })?;
                    // Insert after pos.
                    self.column_metadatas
                        .insert(pos + 1, add_column.column_metadata);
                }
            }
            names.insert(column_name);
            if semantic_type == SemanticType::Tag {
                // For a new tag, we extend the primary key.
                self.primary_key.push(column_id);
            }
        }

        Ok(())
    }

    /// Drops columns from the metadata if exist.
    fn drop_columns(&mut self, names: &[String]) {
        let name_set: HashSet<_> = names.iter().collect();
        self.column_metadatas
            .retain(|col| !name_set.contains(&col.column_schema.name));
    }

    /// Changes columns type to the metadata if exist.
    fn modify_column_types(&mut self, columns: Vec<ModifyColumnType>) -> Result<()> {
        let mut change_type_map: HashMap<_, _> = columns
            .into_iter()
            .map(
                |ModifyColumnType {
                     column_name,
                     target_type,
                 }| (column_name, target_type),
            )
            .collect();

        for column_meta in self.column_metadatas.iter_mut() {
            if let Some(target_type) = change_type_map.remove(&column_meta.column_schema.name) {
                column_meta.column_schema.data_type = target_type.clone();
                // also cast default value to target_type if default value exist
                let new_default =
                    if let Some(default_value) = column_meta.column_schema.default_constraint() {
                        Some(
                            default_value
                                .cast_to_datatype(&target_type)
                                .with_context(|_| CastDefaultValueSnafu {
                                    reason: format!(
                                        "Failed to cast default value from {:?} to type {:?}",
                                        default_value, target_type
                                    ),
                                })?,
                        )
                    } else {
                        None
                    };
                column_meta.column_schema = column_meta
                    .column_schema
                    .clone()
                    .with_default_constraint(new_default.clone())
                    .with_context(|_| CastDefaultValueSnafu {
                        reason: format!("Failed to set new default: {:?}", new_default),
                    })?;
            }
        }

        Ok(())
    }

    fn change_column_inverted_index_options(
        &mut self,
        column_name: String,
        value: bool,
    ) -> Result<()> {
        for column_meta in self.column_metadatas.iter_mut() {
            if column_meta.column_schema.name == column_name {
                column_meta.column_schema.set_inverted_index(value)
            }
        }
        Ok(())
    }

    fn change_column_fulltext_options(
        &mut self,
        column_name: String,
        enable: bool,
        options: Option<FulltextOptions>,
    ) -> Result<()> {
        for column_meta in self.column_metadatas.iter_mut() {
            if column_meta.column_schema.name == column_name {
                ensure!(
                    column_meta.column_schema.data_type.is_string(),
                    InvalidColumnOptionSnafu {
                        column_name,
                        msg: "FULLTEXT index only supports string type".to_string(),
                    }
                );

                let current_fulltext_options = column_meta
                    .column_schema
                    .fulltext_options()
                    .context(SetFulltextOptionsSnafu {
                        column_name: column_name.clone(),
                    })?;

                if enable {
                    ensure!(
                        options.is_some(),
                        InvalidColumnOptionSnafu {
                            column_name,
                            msg: "FULLTEXT index options must be provided",
                        }
                    );
                    set_column_fulltext_options(
                        column_meta,
                        column_name,
                        options.unwrap(),
                        current_fulltext_options,
                    )?;
                } else {
                    unset_column_fulltext_options(
                        column_meta,
                        column_name,
                        current_fulltext_options,
                    )?;
                }
                break;
            }
        }
        Ok(())
    }

    fn change_column_skipping_index_options(
        &mut self,
        column_name: String,
        options: Option<SkippingIndexOptions>,
    ) -> Result<()> {
        for column_meta in self.column_metadatas.iter_mut() {
            if column_meta.column_schema.name == column_name {
                if let Some(options) = &options {
                    column_meta
                        .column_schema
                        .set_skipping_options(options)
                        .context(UnsetSkippingIndexOptionsSnafu {
                            column_name: column_name.clone(),
                        })?;
                } else {
                    column_meta.column_schema.unset_skipping_options().context(
                        UnsetSkippingIndexOptionsSnafu {
                            column_name: column_name.clone(),
                        },
                    )?;
                }
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

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum MetadataError {
    #[snafu(display("Invalid schema"))]
    InvalidSchema {
        source: datatypes::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid metadata, {}", reason))]
    InvalidMeta {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to ser/de json object"))]
    SerdeJson {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: serde_json::Error,
    },

    #[snafu(display("Invalid raw region request, err: {}", err))]
    InvalidRawRegionRequest {
        err: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid region request, region_id: {}, err: {}", region_id, err))]
    InvalidRegionRequest {
        region_id: RegionId,
        err: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unexpected schema error during project"))]
    SchemaProject {
        origin_schema: SchemaRef,
        projection: Vec<ColumnId>,
        #[snafu(implicit)]
        location: Location,
        source: datatypes::Error,
    },

    #[snafu(display("Time index column not found"))]
    TimeIndexNotFound {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Change column {} not exists in region: {}", column_name, region_id))]
    ChangeColumnNotFound {
        column_name: String,
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to convert column schema"))]
    ConvertColumnSchema {
        source: api::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid set region option request, key: {}, value: {}", key, value))]
    InvalidSetRegionOptionRequest {
        key: String,
        value: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid set region option request, key: {}", key))]
    InvalidUnsetRegionOptionRequest {
        key: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to decode protobuf"))]
    DecodeProto {
        #[snafu(source)]
        error: prost::UnknownEnumValue,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid column option, column name: {}, error: {}", column_name, msg))]
    InvalidColumnOption {
        column_name: String,
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to set fulltext options for column {}", column_name))]
    SetFulltextOptions {
        column_name: String,
        source: datatypes::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to set skipping index options for column {}", column_name))]
    SetSkippingIndexOptions {
        column_name: String,
        source: datatypes::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to unset skipping index options for column {}", column_name))]
    UnsetSkippingIndexOptions {
        column_name: String,
        source: datatypes::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to decode arrow ipc record batches"))]
    DecodeArrowIpc {
        #[snafu(source)]
        error: arrow::error::ArrowError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to cast default value, reason: {}", reason))]
    CastDefaultValue {
        reason: String,
        source: datatypes::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unexpected: {}", reason))]
    Unexpected {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to encode/decode flight message"))]
    FlightCodec {
        source: common_grpc::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to decode prost message"))]
    Prost {
        #[snafu(source)]
        error: prost::DecodeError,
        #[snafu(implicit)]
        location: Location,
    },
}

impl ErrorExt for MetadataError {
    fn status_code(&self) -> StatusCode {
        StatusCode::InvalidArguments
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Set column fulltext options if it passed the validation.
///
/// Options allowed to modify:
/// * backend
///
/// Options not allowed to modify:
/// * analyzer
/// * case_sensitive
fn set_column_fulltext_options(
    column_meta: &mut ColumnMetadata,
    column_name: String,
    options: FulltextOptions,
    current_options: Option<FulltextOptions>,
) -> Result<()> {
    if let Some(current_options) = current_options {
        ensure!(
            current_options.analyzer == options.analyzer
                && current_options.case_sensitive == options.case_sensitive,
            InvalidColumnOptionSnafu {
                column_name,
                msg: format!("Cannot change analyzer or case_sensitive if FULLTEXT index is set before. Previous analyzer: {}, previous case_sensitive: {}",
                current_options.analyzer, current_options.case_sensitive),
            }
        );
    }

    column_meta
        .column_schema
        .set_fulltext_options(&options)
        .context(SetFulltextOptionsSnafu { column_name })?;

    Ok(())
}

fn unset_column_fulltext_options(
    column_meta: &mut ColumnMetadata,
    column_name: String,
    current_options: Option<FulltextOptions>,
) -> Result<()> {
    if let Some(mut current_options) = current_options
        && current_options.enable
    {
        current_options.enable = false;
        column_meta
            .column_schema
            .set_fulltext_options(&current_options)
            .context(SetFulltextOptionsSnafu { column_name })?;
    } else {
        return InvalidColumnOptionSnafu {
            column_name,
            msg: "FULLTEXT index already disabled",
        }
        .fail();
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, FulltextAnalyzer, FulltextBackend};

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
            err.to_string()
                .contains("column `ts` is not timestamp type"),
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

    #[test]
    fn test_bump_version() {
        let mut region_metadata = build_test_region_metadata();
        let mut builder = RegionMetadataBuilder::from_existing(region_metadata.clone());
        builder.bump_version();
        let new_meta = builder.build().unwrap();
        region_metadata.schema_version += 1;
        assert_eq!(region_metadata, new_meta);
    }

    fn new_column_metadata(name: &str, is_tag: bool, column_id: ColumnId) -> ColumnMetadata {
        let semantic_type = if is_tag {
            SemanticType::Tag
        } else {
            SemanticType::Field
        };
        ColumnMetadata {
            column_schema: ColumnSchema::new(name, ConcreteDataType::string_datatype(), true),
            semantic_type,
            column_id,
        }
    }

    fn check_columns(metadata: &RegionMetadata, names: &[&str]) {
        let actual: Vec<_> = metadata
            .column_metadatas
            .iter()
            .map(|col| &col.column_schema.name)
            .collect();
        assert_eq!(names, actual);
    }

    #[test]
    fn test_alter() {
        // a (tag), b (field), c (ts)
        let metadata = build_test_region_metadata();
        let mut builder = RegionMetadataBuilder::from_existing(metadata);
        // tag d
        builder
            .alter(AlterKind::AddColumns {
                columns: vec![AddColumn {
                    column_metadata: new_column_metadata("d", true, 4),
                    location: None,
                }],
            })
            .unwrap();
        let metadata = builder.build().unwrap();
        check_columns(&metadata, &["a", "b", "c", "d"]);
        assert_eq!([1, 4], &metadata.primary_key[..]);

        let mut builder = RegionMetadataBuilder::from_existing(metadata);
        builder
            .alter(AlterKind::AddColumns {
                columns: vec![AddColumn {
                    column_metadata: new_column_metadata("e", false, 5),
                    location: Some(AddColumnLocation::First),
                }],
            })
            .unwrap();
        let metadata = builder.build().unwrap();
        check_columns(&metadata, &["e", "a", "b", "c", "d"]);

        let mut builder = RegionMetadataBuilder::from_existing(metadata);
        builder
            .alter(AlterKind::AddColumns {
                columns: vec![AddColumn {
                    column_metadata: new_column_metadata("f", false, 6),
                    location: Some(AddColumnLocation::After {
                        column_name: "b".to_string(),
                    }),
                }],
            })
            .unwrap();
        let metadata = builder.build().unwrap();
        check_columns(&metadata, &["e", "a", "b", "f", "c", "d"]);

        let mut builder = RegionMetadataBuilder::from_existing(metadata);
        builder
            .alter(AlterKind::AddColumns {
                columns: vec![AddColumn {
                    column_metadata: new_column_metadata("g", false, 7),
                    location: Some(AddColumnLocation::After {
                        column_name: "d".to_string(),
                    }),
                }],
            })
            .unwrap();
        let metadata = builder.build().unwrap();
        check_columns(&metadata, &["e", "a", "b", "f", "c", "d", "g"]);

        let mut builder = RegionMetadataBuilder::from_existing(metadata);
        builder
            .alter(AlterKind::DropColumns {
                names: vec!["g".to_string(), "e".to_string()],
            })
            .unwrap();
        let metadata = builder.build().unwrap();
        check_columns(&metadata, &["a", "b", "f", "c", "d"]);

        let mut builder = RegionMetadataBuilder::from_existing(metadata.clone());
        builder
            .alter(AlterKind::DropColumns {
                names: vec!["a".to_string()],
            })
            .unwrap();
        // Build returns error as the primary key contains a.
        let err = builder.build().unwrap_err();
        assert_eq!(StatusCode::InvalidArguments, err.status_code());

        let mut builder = RegionMetadataBuilder::from_existing(metadata);
        builder
            .alter(AlterKind::ModifyColumnTypes {
                columns: vec![ModifyColumnType {
                    column_name: "b".to_string(),
                    target_type: ConcreteDataType::string_datatype(),
                }],
            })
            .unwrap();
        let metadata = builder.build().unwrap();
        check_columns(&metadata, &["a", "b", "f", "c", "d"]);
        let b_type = &metadata
            .column_by_name("b")
            .unwrap()
            .column_schema
            .data_type;
        assert_eq!(ConcreteDataType::string_datatype(), *b_type);

        let mut builder = RegionMetadataBuilder::from_existing(metadata);
        builder
            .alter(AlterKind::SetIndex {
                options: ApiSetIndexOptions::Fulltext {
                    column_name: "b".to_string(),
                    options: FulltextOptions {
                        enable: true,
                        analyzer: FulltextAnalyzer::Chinese,
                        case_sensitive: true,
                        backend: FulltextBackend::Bloom,
                    },
                },
            })
            .unwrap();
        let metadata = builder.build().unwrap();
        let a_fulltext_options = metadata
            .column_by_name("b")
            .unwrap()
            .column_schema
            .fulltext_options()
            .unwrap()
            .unwrap();
        assert!(a_fulltext_options.enable);
        assert_eq!(
            datatypes::schema::FulltextAnalyzer::Chinese,
            a_fulltext_options.analyzer
        );
        assert!(a_fulltext_options.case_sensitive);

        let mut builder = RegionMetadataBuilder::from_existing(metadata);
        builder
            .alter(AlterKind::UnsetIndex {
                options: ApiUnsetIndexOptions::Fulltext {
                    column_name: "b".to_string(),
                },
            })
            .unwrap();
        let metadata = builder.build().unwrap();
        let a_fulltext_options = metadata
            .column_by_name("b")
            .unwrap()
            .column_schema
            .fulltext_options()
            .unwrap()
            .unwrap();
        assert!(!a_fulltext_options.enable);
        assert_eq!(
            datatypes::schema::FulltextAnalyzer::Chinese,
            a_fulltext_options.analyzer
        );
        assert!(a_fulltext_options.case_sensitive);
    }

    #[test]
    fn test_add_if_not_exists() {
        // a (tag), b (field), c (ts)
        let metadata = build_test_region_metadata();
        let mut builder = RegionMetadataBuilder::from_existing(metadata);
        // tag d
        builder
            .alter(AlterKind::AddColumns {
                columns: vec![
                    AddColumn {
                        column_metadata: new_column_metadata("d", true, 4),
                        location: None,
                    },
                    AddColumn {
                        column_metadata: new_column_metadata("d", true, 4),
                        location: None,
                    },
                ],
            })
            .unwrap();
        let metadata = builder.build().unwrap();
        check_columns(&metadata, &["a", "b", "c", "d"]);
        assert_eq!([1, 4], &metadata.primary_key[..]);

        let mut builder = RegionMetadataBuilder::from_existing(metadata);
        // field b.
        builder
            .alter(AlterKind::AddColumns {
                columns: vec![AddColumn {
                    column_metadata: new_column_metadata("b", false, 2),
                    location: None,
                }],
            })
            .unwrap();
        let metadata = builder.build().unwrap();
        check_columns(&metadata, &["a", "b", "c", "d"]);
    }

    #[test]
    fn test_add_column_with_inverted_index() {
        // only set inverted index to true explicitly will this column be inverted indexed

        // a (tag), b (field), c (ts)
        let metadata = build_test_region_metadata();
        let mut builder = RegionMetadataBuilder::from_existing(metadata);
        // tag d, e
        let mut col = new_column_metadata("d", true, 4);
        col.column_schema.set_inverted_index(true);
        builder
            .alter(AlterKind::AddColumns {
                columns: vec![
                    AddColumn {
                        column_metadata: col,
                        location: None,
                    },
                    AddColumn {
                        column_metadata: new_column_metadata("e", true, 5),
                        location: None,
                    },
                ],
            })
            .unwrap();
        let metadata = builder.build().unwrap();
        check_columns(&metadata, &["a", "b", "c", "d", "e"]);
        assert_eq!([1, 4, 5], &metadata.primary_key[..]);
        let column_metadata = metadata.column_by_name("a").unwrap();
        assert!(!column_metadata.column_schema.is_inverted_indexed());
        let column_metadata = metadata.column_by_name("b").unwrap();
        assert!(!column_metadata.column_schema.is_inverted_indexed());
        let column_metadata = metadata.column_by_name("c").unwrap();
        assert!(!column_metadata.column_schema.is_inverted_indexed());
        let column_metadata = metadata.column_by_name("d").unwrap();
        assert!(column_metadata.column_schema.is_inverted_indexed());
        let column_metadata = metadata.column_by_name("e").unwrap();
        assert!(!column_metadata.column_schema.is_inverted_indexed());
    }

    #[test]
    fn test_drop_if_exists() {
        // a (tag), b (field), c (ts)
        let metadata = build_test_region_metadata();
        let mut builder = RegionMetadataBuilder::from_existing(metadata);
        // field d, e
        builder
            .alter(AlterKind::AddColumns {
                columns: vec![
                    AddColumn {
                        column_metadata: new_column_metadata("d", false, 4),
                        location: None,
                    },
                    AddColumn {
                        column_metadata: new_column_metadata("e", false, 5),
                        location: None,
                    },
                ],
            })
            .unwrap();
        let metadata = builder.build().unwrap();
        check_columns(&metadata, &["a", "b", "c", "d", "e"]);

        let mut builder = RegionMetadataBuilder::from_existing(metadata);
        builder
            .alter(AlterKind::DropColumns {
                names: vec!["b".to_string(), "b".to_string()],
            })
            .unwrap();
        let metadata = builder.build().unwrap();
        check_columns(&metadata, &["a", "c", "d", "e"]);

        let mut builder = RegionMetadataBuilder::from_existing(metadata);
        builder
            .alter(AlterKind::DropColumns {
                names: vec!["b".to_string(), "e".to_string()],
            })
            .unwrap();
        let metadata = builder.build().unwrap();
        check_columns(&metadata, &["a", "c", "d"]);
    }

    #[test]
    fn test_invalid_column_name() {
        let mut builder = create_builder();
        builder.push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "__sequence",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 1,
        });
        let err = builder.build().unwrap_err();
        assert!(
            err.to_string()
                .contains("internal column name that can not be used"),
            "unexpected err: {err}",
        );
    }

    #[test]
    fn test_debug_for_column_metadata() {
        let region_metadata = build_test_region_metadata();
        let formatted = format!("{:?}", region_metadata);
        assert_eq!(formatted, "RegionMetadata { column_metadatas: [[a Int64 not null Tag 1], [b Float64 not null Field 2], [c TimestampMillisecond not null Timestamp 3]], time_index: 3, primary_key: [1], region_id: 5299989648942(1234, 5678), schema_version: 0 }");
    }

    #[test]
    fn test_region_metadata_deserialize_default_primary_key_encoding() {
        let serialize = r#"{"column_metadatas":[{"column_schema":{"name":"a","data_type":{"Int64":{}},"is_nullable":false,"is_time_index":false,"default_constraint":null,"metadata":{}},"semantic_type":"Tag","column_id":1},{"column_schema":{"name":"b","data_type":{"Float64":{}},"is_nullable":false,"is_time_index":false,"default_constraint":null,"metadata":{}},"semantic_type":"Field","column_id":2},{"column_schema":{"name":"c","data_type":{"Timestamp":{"Millisecond":null}},"is_nullable":false,"is_time_index":false,"default_constraint":null,"metadata":{}},"semantic_type":"Timestamp","column_id":3}],"primary_key":[1],"region_id":5299989648942,"schema_version":0}"#;
        let deserialized: RegionMetadata = serde_json::from_str(serialize).unwrap();
        assert_eq!(deserialized.primary_key_encoding, PrimaryKeyEncoding::Dense);

        let serialize = r#"{"column_metadatas":[{"column_schema":{"name":"a","data_type":{"Int64":{}},"is_nullable":false,"is_time_index":false,"default_constraint":null,"metadata":{}},"semantic_type":"Tag","column_id":1},{"column_schema":{"name":"b","data_type":{"Float64":{}},"is_nullable":false,"is_time_index":false,"default_constraint":null,"metadata":{}},"semantic_type":"Field","column_id":2},{"column_schema":{"name":"c","data_type":{"Timestamp":{"Millisecond":null}},"is_nullable":false,"is_time_index":false,"default_constraint":null,"metadata":{}},"semantic_type":"Timestamp","column_id":3}],"primary_key":[1],"region_id":5299989648942,"schema_version":0,"primary_key_encoding":"sparse"}"#;
        let deserialized: RegionMetadata = serde_json::from_str(serialize).unwrap();
        assert_eq!(
            deserialized.primary_key_encoding,
            PrimaryKeyEncoding::Sparse
        );
    }
}
