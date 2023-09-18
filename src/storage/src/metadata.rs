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
use std::num::ParseIntError;
use std::str::FromStr;
use std::sync::Arc;

use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Metadata, COMMENT_KEY};
use serde::{Deserialize, Serialize};
use snafu::{ensure, Location, OptionExt, ResultExt, Snafu};
use store_api::storage::consts::{self, ReservedColumnId};
use store_api::storage::{
    AddColumn, AlterOperation, AlterRequest, ColumnDescriptor, ColumnDescriptorBuilder,
    ColumnDescriptorBuilderError, ColumnFamilyDescriptor, ColumnFamilyDescriptorBuilder,
    ColumnFamilyId, ColumnId, RegionDescriptor, RegionDescriptorBuilder, RegionId, RegionMeta,
    RowKeyDescriptor, RowKeyDescriptorBuilder, Schema, SchemaRef,
};

use crate::manifest::action::{RawColumnFamiliesMetadata, RawColumnsMetadata, RawRegionMetadata};
use crate::schema::{RegionSchema, RegionSchemaRef};

/// Error for handling metadata.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Column name {} already exists", name))]
    ColNameExists { name: String, location: Location },

    #[snafu(display("Column family name {} already exists", name))]
    CfNameExists { name: String, location: Location },

    #[snafu(display("Column family id {} already exists", id))]
    CfIdExists { id: ColumnId, location: Location },

    #[snafu(display("Column id {} already exists", id))]
    ColIdExists { id: ColumnId, location: Location },

    #[snafu(display("Failed to build schema"))]
    InvalidSchema {
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Column name {} is reserved by the system", name))]
    ReservedColumn { name: String, location: Location },

    #[snafu(display("Missing timestamp key column"))]
    MissingTimestamp { location: Location },

    // Variants for validating `AlterRequest`, which won't have a backtrace.
    #[snafu(display("Expect altering metadata with version {}, given {}", expect, given))]
    InvalidAlterVersion {
        expect: VersionNumber,
        given: VersionNumber,
    },

    #[snafu(display("Failed to add column as there is already a column named {}", name))]
    AddExistColumn { name: String },

    #[snafu(display("Failed to add a non null column {}", name))]
    AddNonNullColumn { name: String },

    #[snafu(display("Failed to drop column as there is no column named {}", name))]
    DropAbsentColumn { name: String },

    #[snafu(display("Failed to drop column {} as it is part of key", name))]
    DropKeyColumn { name: String },

    #[snafu(display("Failed to drop column {} as it is an internal column", name))]
    DropInternalColumn { name: String },

    // End of variants for validating `AlterRequest`.
    #[snafu(display("Failed to convert to column schema"))]
    ToColumnSchema {
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to parse metadata to int, key_value: {}", key_value))]
    ParseMetaInt {
        // Store key and value in one string to reduce the enum size.
        key_value: String,
        source: std::num::ParseIntError,
        location: Location,
    },

    #[snafu(display("Metadata of {} not found", key))]
    MetaNotFound { key: String, location: Location },

    #[snafu(display("Failed to build column descriptor"))]
    BuildColumnDescriptor {
        source: ColumnDescriptorBuilderError,
        location: Location,
    },

    #[snafu(display("Failed to convert from arrow schema"))]
    ConvertArrowSchema {
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Invalid internal column index in arrow schema"))]
    InvalidIndex { location: Location },

    #[snafu(display("Failed to convert arrow chunk to batch, name: {}", name))]
    ConvertChunk {
        name: String,
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to convert schema"))]
    ConvertSchema {
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Invalid projection, {}", msg))]
    InvalidProjection { msg: String, location: Location },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        StatusCode::InvalidArguments
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Implementation of [RegionMeta].
///
/// Holds a snapshot of region metadata.
#[derive(Clone, Debug)]
pub struct RegionMetaImpl {
    metadata: RegionMetadataRef,
}

impl RegionMetaImpl {
    pub fn new(metadata: RegionMetadataRef) -> RegionMetaImpl {
        RegionMetaImpl { metadata }
    }
}

impl RegionMeta for RegionMetaImpl {
    fn schema(&self) -> &SchemaRef {
        self.metadata.user_schema()
    }

    fn version(&self) -> u32 {
        self.metadata.version
    }
}

pub type VersionNumber = u32;

// TODO(yingwen): We may need to hold a list of history schema.

/// In memory metadata of region.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegionMetadata {
    // The following fields are immutable.
    id: RegionId,
    name: String,

    // The following fields are mutable.
    /// Latest schema of the region.
    schema: RegionSchemaRef,
    pub columns: ColumnsMetadataRef,
    column_families: ColumnFamiliesMetadata,
    version: VersionNumber,
}

impl RegionMetadata {
    #[inline]
    pub fn id(&self) -> RegionId {
        self.id
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    pub fn schema(&self) -> &RegionSchemaRef {
        &self.schema
    }

    #[inline]
    pub fn user_schema(&self) -> &SchemaRef {
        self.schema.user_schema()
    }

    #[inline]
    pub fn version(&self) -> u32 {
        self.schema.version()
    }

    /// Checks whether the `req` is valid, returns `Err` if it is invalid.
    pub fn validate_alter(&self, req: &AlterRequest) -> Result<()> {
        ensure!(
            req.version == self.version,
            InvalidAlterVersionSnafu {
                expect: req.version,
                given: self.version,
            }
        );

        match &req.operation {
            AlterOperation::AddColumns { columns } => {
                for col in columns {
                    self.validate_add_column(col)?;
                }
            }
            AlterOperation::DropColumns { names } => {
                for name in names {
                    self.validate_drop_column(name)?;
                }
            }
        }

        Ok(())
    }

    /// Returns a new [RegionMetadata] after alteration, leave `self` unchanged.
    ///
    /// Caller should use [RegionMetadata::validate_alter] to validate the `req` and
    /// ensure the version of the `req` is equal to the version of the metadata.
    ///
    /// # Panics
    /// Panics if `req.version != self.version`.
    pub fn alter(&self, req: &AlterRequest) -> Result<RegionMetadata> {
        // The req should have been validated before.
        assert_eq!(req.version, self.version);

        let mut desc = self.to_descriptor();
        // Apply the alter operation to the descriptor.
        req.operation.apply(&mut desc);

        RegionMetadataBuilder::try_from(desc)?
            .version(self.version + 1) // Bump the metadata version.
            .build()
    }

    fn validate_add_column(&self, add_column: &AddColumn) -> Result<()> {
        // We don't check the case that the column is not nullable but default constraint is null. The
        // caller should guarantee this.
        ensure!(
            add_column.desc.is_nullable() || add_column.desc.default_constraint().is_some(),
            AddNonNullColumnSnafu {
                name: &add_column.desc.name,
            }
        );

        // Use the store schema to check the column as it contains all internal columns.
        let store_schema = self.schema.store_schema();
        ensure!(
            !store_schema.contains_column(&add_column.desc.name),
            AddExistColumnSnafu {
                name: &add_column.desc.name,
            }
        );

        Ok(())
    }

    fn validate_drop_column(&self, name: &str) -> Result<()> {
        let store_schema = self.schema.store_schema();
        ensure!(
            store_schema.contains_column(name),
            DropAbsentColumnSnafu { name }
        );
        ensure!(
            !store_schema.is_key_column(name),
            DropKeyColumnSnafu { name }
        );
        ensure!(
            store_schema.is_user_column(name),
            DropInternalColumnSnafu { name }
        );

        Ok(())
    }

    fn to_descriptor(&self) -> RegionDescriptor {
        let row_key = self.columns.to_row_key_descriptor();
        let mut builder = RegionDescriptorBuilder::default()
            .id(self.id)
            .name(&self.name)
            .row_key(row_key);

        for (cf_id, cf) in &self.column_families.id_to_cfs {
            let mut cf_builder = ColumnFamilyDescriptorBuilder::default()
                .cf_id(*cf_id)
                .name(&cf.name);
            for column in &self.columns.columns[cf.column_index_start..cf.column_index_end] {
                cf_builder = cf_builder.push_column(column.desc.clone());
            }
            // It should always be able to build the descriptor back.
            let desc = cf_builder.build().unwrap();
            if *cf_id == consts::DEFAULT_CF_ID {
                builder = builder.default_cf(desc);
            } else {
                builder = builder.push_extra_column_family(desc);
            }
        }

        // We could ensure all fields are set here.
        builder.build().unwrap()
    }
}

pub type RegionMetadataRef = Arc<RegionMetadata>;

impl From<&RegionMetadata> for RawRegionMetadata {
    fn from(data: &RegionMetadata) -> RawRegionMetadata {
        RawRegionMetadata {
            id: data.id,
            name: data.name.clone(),
            columns: RawColumnsMetadata::from(&*data.columns),
            column_families: RawColumnFamiliesMetadata::from(&data.column_families),
            version: data.version,
        }
    }
}

impl TryFrom<RawRegionMetadata> for RegionMetadata {
    type Error = Error;

    fn try_from(raw: RawRegionMetadata) -> Result<RegionMetadata> {
        let columns = Arc::new(ColumnsMetadata::from(raw.columns));
        let schema = Arc::new(RegionSchema::new(columns.clone(), raw.version)?);

        Ok(RegionMetadata {
            id: raw.id,
            name: raw.name,
            schema,
            columns,
            column_families: raw.column_families.into(),
            version: raw.version,
        })
    }
}

const METADATA_CF_ID_KEY: &str = "greptime:storage:cf_id";
const METADATA_COLUMN_ID_KEY: &str = "greptime:storage:column_id";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnMetadata {
    pub cf_id: ColumnFamilyId,
    pub desc: ColumnDescriptor,
}

impl ColumnMetadata {
    #[inline]
    pub fn id(&self) -> ColumnId {
        self.desc.id
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.desc.name
    }

    /// Convert `self` to [`ColumnSchema`] for building a [`StoreSchema`](crate::schema::StoreSchema). This
    /// would store additional metadatas to the ColumnSchema.
    pub fn to_column_schema(&self) -> Result<ColumnSchema> {
        let desc = &self.desc;

        ColumnSchema::new(&desc.name, desc.data_type.clone(), desc.is_nullable())
            .with_metadata(self.to_metadata())
            .with_time_index(self.desc.is_time_index())
            .with_default_constraint(desc.default_constraint().cloned())
            .context(ToColumnSchemaSnafu)
    }

    /// Convert [`ColumnSchema`] in [`StoreSchema`](crate::schema::StoreSchema) to [`ColumnMetadata`].
    pub fn from_column_schema(column_schema: &ColumnSchema) -> Result<ColumnMetadata> {
        let metadata = column_schema.metadata();
        let cf_id = try_parse_int(metadata, METADATA_CF_ID_KEY, Some(consts::DEFAULT_CF_ID))?;
        let column_id = try_parse_int(metadata, METADATA_COLUMN_ID_KEY, None)?;
        let comment = metadata.get(COMMENT_KEY).cloned().unwrap_or_default();

        let desc = ColumnDescriptorBuilder::new(
            column_id,
            &column_schema.name,
            column_schema.data_type.clone(),
        )
        .is_nullable(column_schema.is_nullable())
        .is_time_index(column_schema.is_time_index())
        .default_constraint(column_schema.default_constraint().cloned())
        .comment(comment)
        .build()
        .context(BuildColumnDescriptorSnafu)?;

        Ok(ColumnMetadata { cf_id, desc })
    }

    fn to_metadata(&self) -> Metadata {
        let mut metadata = Metadata::new();
        if self.cf_id != consts::DEFAULT_CF_ID {
            let _ = metadata.insert(METADATA_CF_ID_KEY.to_string(), self.cf_id.to_string());
        }
        let _ = metadata.insert(METADATA_COLUMN_ID_KEY.to_string(), self.desc.id.to_string());
        if !self.desc.comment.is_empty() {
            let _ = metadata.insert(COMMENT_KEY.to_string(), self.desc.comment.clone());
        }

        metadata
    }
}

fn try_parse_int<T>(metadata: &Metadata, key: &str, default_value: Option<T>) -> Result<T>
where
    T: FromStr<Err = ParseIntError>,
{
    if let Some(value) = metadata.get(key) {
        return value.parse().with_context(|_| ParseMetaIntSnafu {
            key_value: format!("{key}={value}"),
        });
    }
    // No such key in metadata.

    default_value.context(MetaNotFoundSnafu { key })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnsMetadata {
    /// All columns.
    ///
    /// Columns are organized in the following order:
    /// ```text
    /// key columns, timestamp, [version,] value columns, internal columns
    /// ```
    ///
    /// The key columns, timestamp and version forms the row key.
    columns: Vec<ColumnMetadata>,
    /// Maps column name to index of columns, used to fast lookup column by name.
    name_to_col_index: HashMap<String, usize>,
    /// Exclusive end index of row key columns.
    row_key_end: usize,
    /// Index of timestamp key column.
    timestamp_key_index: usize,
    /// Exclusive end index of user columns.
    ///
    /// Columns in `[user_column_end..)` are internal columns.
    user_column_end: usize,
}

impl ColumnsMetadata {
    /// Returns an iterator to all row key columns.
    ///
    /// Row key columns includes all key columns, the timestamp column and the
    /// optional version column.
    pub fn iter_row_key_columns(&self) -> impl Iterator<Item = &ColumnMetadata> {
        self.columns.iter().take(self.row_key_end)
    }

    /// Returns an iterator to all value columns (internal columns are excluded).
    pub fn iter_field_columns(&self) -> impl Iterator<Item = &ColumnMetadata> {
        self.columns[self.row_key_end..self.user_column_end].iter()
    }

    pub fn iter_user_columns(&self) -> impl Iterator<Item = &ColumnMetadata> {
        self.columns.iter().take(self.user_column_end)
    }

    #[inline]
    pub fn columns(&self) -> &[ColumnMetadata] {
        &self.columns
    }

    #[inline]
    pub fn num_row_key_columns(&self) -> usize {
        self.row_key_end
    }

    #[inline]
    pub fn num_field_columns(&self) -> usize {
        self.user_column_end - self.row_key_end
    }

    #[inline]
    pub fn timestamp_key_index(&self) -> usize {
        self.timestamp_key_index
    }

    #[inline]
    pub fn row_key_end(&self) -> usize {
        self.row_key_end
    }

    #[inline]
    pub fn user_column_end(&self) -> usize {
        self.user_column_end
    }

    #[inline]
    pub fn column_metadata(&self, idx: usize) -> &ColumnMetadata {
        &self.columns[idx]
    }

    fn to_row_key_descriptor(&self) -> RowKeyDescriptor {
        let mut builder = RowKeyDescriptorBuilder::default();
        for (idx, column) in self.iter_row_key_columns().enumerate() {
            // Not a timestamp column.
            if idx != self.timestamp_key_index {
                builder = builder.push_column(column.desc.clone());
            }
        }
        builder = builder.timestamp(self.column_metadata(self.timestamp_key_index).desc.clone());
        // Since the metadata is built from descriptor, so it should always be able to build the descriptor back.
        builder.build().unwrap()
    }
}

pub type ColumnsMetadataRef = Arc<ColumnsMetadata>;

impl From<&ColumnsMetadata> for RawColumnsMetadata {
    fn from(data: &ColumnsMetadata) -> RawColumnsMetadata {
        RawColumnsMetadata {
            columns: data.columns.clone(),
            row_key_end: data.row_key_end,
            timestamp_key_index: data.timestamp_key_index,
            user_column_end: data.user_column_end,
        }
    }
}

impl From<RawColumnsMetadata> for ColumnsMetadata {
    fn from(raw: RawColumnsMetadata) -> ColumnsMetadata {
        let name_to_col_index = raw
            .columns
            .iter()
            .enumerate()
            .map(|(i, col)| (col.desc.name.clone(), i))
            .collect();

        ColumnsMetadata {
            columns: raw.columns,
            name_to_col_index,
            row_key_end: raw.row_key_end,
            timestamp_key_index: raw.timestamp_key_index,
            user_column_end: raw.user_column_end,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnFamiliesMetadata {
    /// Map column family id to column family metadata.
    id_to_cfs: HashMap<ColumnFamilyId, ColumnFamilyMetadata>,
}

impl ColumnFamiliesMetadata {
    pub fn cf_by_id(&self, cf_id: ColumnFamilyId) -> Option<&ColumnFamilyMetadata> {
        self.id_to_cfs.get(&cf_id)
    }
}

impl From<&ColumnFamiliesMetadata> for RawColumnFamiliesMetadata {
    fn from(data: &ColumnFamiliesMetadata) -> RawColumnFamiliesMetadata {
        let column_families = data.id_to_cfs.values().cloned().collect();
        RawColumnFamiliesMetadata { column_families }
    }
}

impl From<RawColumnFamiliesMetadata> for ColumnFamiliesMetadata {
    fn from(raw: RawColumnFamiliesMetadata) -> ColumnFamiliesMetadata {
        let id_to_cfs = raw
            .column_families
            .into_iter()
            .map(|cf| (cf.cf_id, cf))
            .collect();
        ColumnFamiliesMetadata { id_to_cfs }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnFamilyMetadata {
    /// Column family name.
    pub name: String,
    pub cf_id: ColumnFamilyId,
    /// Inclusive start index of columns in the column family.
    pub column_index_start: usize,
    /// Exclusive end index of columns in the column family.
    pub column_index_end: usize,
}

impl TryFrom<RegionDescriptor> for RegionMetadataBuilder {
    type Error = Error;

    fn try_from(desc: RegionDescriptor) -> Result<RegionMetadataBuilder> {
        let mut builder = RegionMetadataBuilder::new()
            .name(desc.name)
            .id(desc.id)
            .row_key(desc.row_key)?
            .add_column_family(desc.default_cf)?;
        for cf in desc.extra_cfs {
            builder = builder.add_column_family(cf)?;
        }

        Ok(builder)
    }
}

impl TryFrom<RegionDescriptor> for RegionMetadata {
    type Error = Error;

    fn try_from(desc: RegionDescriptor) -> Result<RegionMetadata> {
        // Doesn't set version explicitly here, because this is a new region meta
        // created from descriptor, using initial version is reasonable.
        let builder = RegionMetadataBuilder::try_from(desc)?;

        builder.build()
    }
}

#[derive(Default)]
struct ColumnsMetadataBuilder {
    columns: Vec<ColumnMetadata>,
    name_to_col_index: HashMap<String, usize>,
    /// Column id set, used to validate column id uniqueness.
    column_ids: HashSet<ColumnId>,

    // Row key metadata:
    row_key_end: usize,
    timestamp_key_index: Option<usize>,
}

impl ColumnsMetadataBuilder {
    fn row_key(&mut self, key: RowKeyDescriptor) -> Result<&mut Self> {
        for col in key.columns {
            let _ = self.push_row_key_column(col)?;
        }

        // TODO(yingwen): Validate this is a timestamp column.
        self.timestamp_key_index = Some(self.columns.len());
        let _ = self.push_row_key_column(key.timestamp)?;
        self.row_key_end = self.columns.len();

        Ok(self)
    }

    fn push_row_key_column(&mut self, desc: ColumnDescriptor) -> Result<&mut Self> {
        self.push_field_column(consts::KEY_CF_ID, desc)
    }

    fn push_field_column(
        &mut self,
        cf_id: ColumnFamilyId,
        desc: ColumnDescriptor,
    ) -> Result<&mut Self> {
        ensure!(
            !is_internal_field_column(&desc.name),
            ReservedColumnSnafu { name: &desc.name }
        );

        self.push_new_column(cf_id, desc)
    }

    fn push_new_column(
        &mut self,
        cf_id: ColumnFamilyId,
        desc: ColumnDescriptor,
    ) -> Result<&mut Self> {
        ensure!(
            !self.name_to_col_index.contains_key(&desc.name),
            ColNameExistsSnafu { name: &desc.name }
        );
        ensure!(
            !self.column_ids.contains(&desc.id),
            ColIdExistsSnafu { id: desc.id }
        );

        let column_name = desc.name.clone();
        let column_id = desc.id;
        let meta = ColumnMetadata { cf_id, desc };

        let column_index = self.columns.len();
        self.columns.push(meta);
        let _ = self.name_to_col_index.insert(column_name, column_index);
        let _ = self.column_ids.insert(column_id);

        Ok(self)
    }

    fn build(mut self) -> Result<ColumnsMetadata> {
        let timestamp_key_index = self.timestamp_key_index.context(MissingTimestampSnafu)?;

        let user_column_end = self.columns.len();
        // Setup internal columns.
        for internal_desc in internal_column_descs() {
            let _ = self.push_new_column(consts::DEFAULT_CF_ID, internal_desc)?;
        }

        Ok(ColumnsMetadata {
            columns: self.columns,
            name_to_col_index: self.name_to_col_index,
            row_key_end: self.row_key_end,
            timestamp_key_index,
            user_column_end,
        })
    }
}

#[derive(Default)]
struct ColumnFamiliesMetadataBuilder {
    id_to_cfs: HashMap<ColumnFamilyId, ColumnFamilyMetadata>,
    cf_names: HashSet<String>,
}

impl ColumnFamiliesMetadataBuilder {
    fn add_column_family(&mut self, cf: ColumnFamilyMetadata) -> Result<&mut Self> {
        ensure!(
            !self.id_to_cfs.contains_key(&cf.cf_id),
            CfIdExistsSnafu { id: cf.cf_id }
        );

        ensure!(
            !self.cf_names.contains(&cf.name),
            CfNameExistsSnafu { name: &cf.name }
        );

        let _ = self.cf_names.insert(cf.name.clone());
        let _ = self.id_to_cfs.insert(cf.cf_id, cf);

        Ok(self)
    }

    fn build(self) -> ColumnFamiliesMetadata {
        ColumnFamiliesMetadata {
            id_to_cfs: self.id_to_cfs,
        }
    }
}

struct RegionMetadataBuilder {
    id: RegionId,
    name: String,
    columns_meta_builder: ColumnsMetadataBuilder,
    cfs_meta_builder: ColumnFamiliesMetadataBuilder,
    version: VersionNumber,
}

impl Default for RegionMetadataBuilder {
    fn default() -> RegionMetadataBuilder {
        RegionMetadataBuilder::new()
    }
}

impl RegionMetadataBuilder {
    fn new() -> RegionMetadataBuilder {
        RegionMetadataBuilder {
            id: 0.into(),
            name: String::new(),
            columns_meta_builder: ColumnsMetadataBuilder::default(),
            cfs_meta_builder: ColumnFamiliesMetadataBuilder::default(),
            version: Schema::INITIAL_VERSION,
        }
    }

    fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    fn id(mut self, id: RegionId) -> Self {
        self.id = id;
        self
    }

    fn version(mut self, version: VersionNumber) -> Self {
        self.version = version;
        self
    }

    fn row_key(mut self, key: RowKeyDescriptor) -> Result<Self> {
        let _ = self.columns_meta_builder.row_key(key)?;

        Ok(self)
    }

    fn add_column_family(mut self, cf: ColumnFamilyDescriptor) -> Result<Self> {
        let column_index_start = self.columns_meta_builder.columns.len();
        let column_index_end = column_index_start + cf.columns.len();
        let cf_meta = ColumnFamilyMetadata {
            name: cf.name.clone(),
            cf_id: cf.cf_id,
            column_index_start,
            column_index_end,
        };

        let _ = self.cfs_meta_builder.add_column_family(cf_meta)?;

        for col in cf.columns {
            let _ = self.columns_meta_builder.push_field_column(cf.cf_id, col)?;
        }

        Ok(self)
    }

    fn build(self) -> Result<RegionMetadata> {
        let columns = Arc::new(self.columns_meta_builder.build()?);
        let schema = Arc::new(RegionSchema::new(columns.clone(), self.version)?);

        Ok(RegionMetadata {
            id: self.id,
            name: self.name,
            schema,
            columns,
            column_families: self.cfs_meta_builder.build(),
            version: self.version,
        })
    }
}

fn internal_column_descs() -> [ColumnDescriptor; 2] {
    [
        ColumnDescriptorBuilder::new(
            ReservedColumnId::sequence(),
            consts::SEQUENCE_COLUMN_NAME.to_string(),
            ConcreteDataType::uint64_datatype(),
        )
        .is_nullable(false)
        .build()
        .unwrap(),
        ColumnDescriptorBuilder::new(
            ReservedColumnId::op_type(),
            consts::OP_TYPE_COLUMN_NAME.to_string(),
            ConcreteDataType::uint8_datatype(),
        )
        .is_nullable(false)
        .build()
        .unwrap(),
    ]
}

/// Returns true if this is an internal column for value column.
#[inline]
fn is_internal_field_column(column_name: &str) -> bool {
    matches!(
        column_name,
        consts::SEQUENCE_COLUMN_NAME | consts::OP_TYPE_COLUMN_NAME
    )
}

#[cfg(test)]
mod tests {
    use datatypes::schema::ColumnDefaultConstraint;
    use datatypes::type_id::LogicalTypeId;
    use datatypes::value::Value;
    use store_api::storage::{
        AddColumn, AlterOperation, ColumnDescriptorBuilder, ColumnFamilyDescriptorBuilder,
        RowKeyDescriptorBuilder,
    };

    use super::*;
    use crate::test_util::descriptor_util::RegionDescBuilder;
    use crate::test_util::schema_util;

    const TEST_REGION: &str = "test-region";

    #[test]
    fn test_descriptor_to_region_metadata() {
        let region_name = "region-0";
        let desc = RegionDescBuilder::new(region_name)
            .timestamp(("ts", LogicalTypeId::TimestampMillisecond, false))
            .push_key_column(("k1", LogicalTypeId::Int32, false))
            .push_field_column(("v1", LogicalTypeId::Float32, true))
            .build();

        let expect_schema = schema_util::new_schema_ref(
            &[
                ("k1", LogicalTypeId::Int32, false),
                ("ts", LogicalTypeId::TimestampMillisecond, false),
                ("v1", LogicalTypeId::Float32, true),
            ],
            Some(1),
        );

        let metadata = RegionMetadata::try_from(desc).unwrap();
        assert_eq!(region_name, metadata.name);
        assert_eq!(expect_schema, *metadata.user_schema());
        assert_eq!(2, metadata.columns.num_row_key_columns());
        assert_eq!(1, metadata.columns.num_field_columns());
    }

    #[test]
    fn test_build_empty_region_metadata() {
        let err = RegionMetadataBuilder::default().build().err().unwrap();
        assert!(matches!(err, Error::MissingTimestamp { .. }));
    }

    #[test]
    fn test_build_metadata_duplicate_name() {
        let cf = ColumnFamilyDescriptorBuilder::default()
            .push_column(
                ColumnDescriptorBuilder::new(4, "v1", ConcreteDataType::int64_datatype())
                    .build()
                    .unwrap(),
            )
            .push_column(
                ColumnDescriptorBuilder::new(5, "v1", ConcreteDataType::int64_datatype())
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        let err = RegionMetadataBuilder::new()
            .add_column_family(cf)
            .err()
            .unwrap();
        assert!(matches!(err, Error::ColNameExists { .. }));
    }

    #[test]
    fn test_build_metadata_internal_name() {
        let names = [consts::SEQUENCE_COLUMN_NAME, consts::OP_TYPE_COLUMN_NAME];
        for name in names {
            let cf = ColumnFamilyDescriptorBuilder::default()
                .push_column(
                    ColumnDescriptorBuilder::new(5, name, ConcreteDataType::int64_datatype())
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap();
            let err = RegionMetadataBuilder::new()
                .add_column_family(cf)
                .err()
                .unwrap();
            assert!(matches!(err, Error::ReservedColumn { .. }));
        }
    }

    #[test]
    fn test_build_metadata_duplicate_id() {
        let cf = ColumnFamilyDescriptorBuilder::default()
            .push_column(
                ColumnDescriptorBuilder::new(4, "v1", ConcreteDataType::int64_datatype())
                    .build()
                    .unwrap(),
            )
            .push_column(
                ColumnDescriptorBuilder::new(4, "v2", ConcreteDataType::int64_datatype())
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        let err = RegionMetadataBuilder::new()
            .add_column_family(cf)
            .err()
            .unwrap();
        assert!(matches!(err, Error::ColIdExists { .. }));

        let timestamp = ColumnDescriptorBuilder::new(2, "ts", ConcreteDataType::int64_datatype())
            .is_nullable(false)
            .is_time_index(true)
            .build()
            .unwrap();
        let row_key = RowKeyDescriptorBuilder::new(timestamp)
            .push_column(
                ColumnDescriptorBuilder::new(2, "k1", ConcreteDataType::int64_datatype())
                    .is_nullable(false)
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        let err = RegionMetadataBuilder::new().row_key(row_key).err().unwrap();
        assert!(matches!(err, Error::ColIdExists { .. }));
    }

    fn new_metadata() -> RegionMetadata {
        let timestamp = ColumnDescriptorBuilder::new(
            2,
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
        )
        .is_nullable(false)
        .is_time_index(true)
        .build()
        .unwrap();
        let row_key = RowKeyDescriptorBuilder::new(timestamp)
            .push_column(
                ColumnDescriptorBuilder::new(3, "k1", ConcreteDataType::int64_datatype())
                    .is_nullable(false)
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        let cf = ColumnFamilyDescriptorBuilder::default()
            .push_column(
                ColumnDescriptorBuilder::new(4, "v1", ConcreteDataType::int64_datatype())
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        RegionMetadataBuilder::new()
            .name(TEST_REGION)
            .row_key(row_key)
            .unwrap()
            .add_column_family(cf)
            .unwrap()
            .build()
            .unwrap()
    }

    #[test]
    fn test_build_metedata_disable_version() {
        let metadata = new_metadata();
        assert_eq!(TEST_REGION, metadata.name);

        let expect_schema = schema_util::new_schema_ref(
            &[
                ("k1", LogicalTypeId::Int64, false),
                ("ts", LogicalTypeId::TimestampMillisecond, false),
                ("v1", LogicalTypeId::Int64, true),
            ],
            Some(1),
        );

        assert_eq!(expect_schema, *metadata.user_schema());

        // 3 user columns and 2 internal columns
        assert_eq!(5, metadata.columns.columns.len());
        // 2 row key columns
        assert_eq!(2, metadata.columns.num_row_key_columns());
        let row_key_names: Vec<_> = metadata
            .columns
            .iter_row_key_columns()
            .map(|column| &column.desc.name)
            .collect();
        assert_eq!(["k1", "ts"], &row_key_names[..]);
        // 1 value column
        assert_eq!(1, metadata.columns.num_field_columns());
        let value_names: Vec<_> = metadata
            .columns
            .iter_field_columns()
            .map(|column| &column.desc.name)
            .collect();
        assert_eq!(["v1"], &value_names[..]);
        // Check timestamp index.
        assert_eq!(1, metadata.columns.timestamp_key_index);

        assert!(metadata
            .column_families
            .cf_by_id(consts::DEFAULT_CF_ID)
            .is_some());

        assert_eq!(0, metadata.version);
    }

    #[test]
    fn test_convert_between_raw() {
        let metadata = new_metadata();
        let raw = RawRegionMetadata::from(&metadata);

        let converted = RegionMetadata::try_from(raw).unwrap();
        assert_eq!(metadata, converted);
    }

    #[test]
    fn test_alter_metadata_add_columns() {
        let region_name = "region-0";
        let builder = RegionDescBuilder::new(region_name)
            .push_key_column(("k1", LogicalTypeId::Int32, false))
            .push_field_column(("v1", LogicalTypeId::Float32, true));
        let last_column_id = builder.last_column_id();
        let metadata: RegionMetadata = builder.build().try_into().unwrap();

        let req = AlterRequest {
            operation: AlterOperation::AddColumns {
                columns: vec![
                    AddColumn {
                        desc: ColumnDescriptorBuilder::new(
                            last_column_id + 1,
                            "k2",
                            ConcreteDataType::int32_datatype(),
                        )
                        .build()
                        .unwrap(),
                        is_key: true,
                    },
                    AddColumn {
                        desc: ColumnDescriptorBuilder::new(
                            last_column_id + 2,
                            "v2",
                            ConcreteDataType::float32_datatype(),
                        )
                        .build()
                        .unwrap(),
                        is_key: false,
                    },
                ],
            },
            version: 0,
        };
        metadata.validate_alter(&req).unwrap();
        let metadata = metadata.alter(&req).unwrap();

        let builder: RegionMetadataBuilder = RegionDescBuilder::new(region_name)
            .push_key_column(("k1", LogicalTypeId::Int32, false))
            .push_field_column(("v1", LogicalTypeId::Float32, true))
            .push_key_column(("k2", LogicalTypeId::Int32, true))
            .push_field_column(("v2", LogicalTypeId::Float32, true))
            .build()
            .try_into()
            .unwrap();
        let expect = builder.version(1).build().unwrap();
        assert_eq!(expect, metadata);
    }

    #[test]
    fn test_alter_metadata_drop_columns() {
        let region_name = "region-0";
        let metadata: RegionMetadata = RegionDescBuilder::new(region_name)
            .push_key_column(("k1", LogicalTypeId::Int32, false))
            .push_key_column(("k2", LogicalTypeId::Int32, false))
            .push_field_column(("v1", LogicalTypeId::Float32, true))
            .push_field_column(("v2", LogicalTypeId::Float32, true))
            .build()
            .try_into()
            .unwrap();

        let req = AlterRequest {
            operation: AlterOperation::DropColumns {
                names: vec![
                    String::from("k1"), // k1 would be ignored.
                    String::from("v1"),
                ],
            },
            version: 0,
        };
        let metadata = metadata.alter(&req).unwrap();

        let builder = RegionDescBuilder::new(region_name)
            .push_key_column(("k1", LogicalTypeId::Int32, false))
            .push_key_column(("k2", LogicalTypeId::Int32, false));
        let last_column_id = builder.last_column_id() + 1;
        let builder: RegionMetadataBuilder = builder
            .set_last_column_id(last_column_id) // This id is reserved for v1
            .push_field_column(("v2", LogicalTypeId::Float32, true))
            .build()
            .try_into()
            .unwrap();
        let expect = builder.version(1).build().unwrap();
        assert_eq!(expect, metadata);
    }

    #[test]
    fn test_validate_alter_request() {
        let builder = RegionDescBuilder::new("region-alter")
            .timestamp(("ts", LogicalTypeId::TimestampMillisecond, false))
            .push_key_column(("k0", LogicalTypeId::Int32, false))
            .push_field_column(("v0", LogicalTypeId::Float32, true))
            .push_field_column(("v1", LogicalTypeId::Float32, true));
        let last_column_id = builder.last_column_id();
        let metadata: RegionMetadata = builder.build().try_into().unwrap();

        // Test request with different version.
        let mut req = AlterRequest {
            operation: AlterOperation::AddColumns {
                columns: vec![AddColumn {
                    desc: ColumnDescriptorBuilder::new(
                        last_column_id + 1,
                        "k2",
                        ConcreteDataType::int32_datatype(),
                    )
                    .build()
                    .unwrap(),
                    is_key: true,
                }],
            },
            version: 1,
        };
        assert!(matches!(
            metadata.validate_alter(&req).err().unwrap(),
            Error::InvalidAlterVersion { .. }
        ));
        req.version = 0;

        // Add existing column.
        req.operation = AlterOperation::AddColumns {
            columns: vec![AddColumn {
                desc: ColumnDescriptorBuilder::new(
                    last_column_id + 1,
                    "ts",
                    ConcreteDataType::int32_datatype(),
                )
                .build()
                .unwrap(),
                is_key: false,
            }],
        };
        assert!(matches!(
            metadata.validate_alter(&req).err().unwrap(),
            Error::AddExistColumn { .. }
        ));

        // Add non null column.
        req.operation = AlterOperation::AddColumns {
            columns: vec![AddColumn {
                desc: ColumnDescriptorBuilder::new(
                    last_column_id + 1,
                    "v2",
                    ConcreteDataType::int32_datatype(),
                )
                .is_nullable(false)
                .build()
                .unwrap(),
                is_key: false,
            }],
        };
        assert!(matches!(
            metadata.validate_alter(&req).err().unwrap(),
            Error::AddNonNullColumn { .. }
        ));

        // Drop absent column.
        let mut req = AlterRequest {
            operation: AlterOperation::DropColumns {
                names: vec![String::from("v2")],
            },
            version: 0,
        };
        assert!(matches!(
            metadata.validate_alter(&req).err().unwrap(),
            Error::DropAbsentColumn { .. }
        ));

        // Drop key column.
        req.operation = AlterOperation::DropColumns {
            names: vec![String::from("ts")],
        };
        assert!(matches!(
            metadata.validate_alter(&req).err().unwrap(),
            Error::DropKeyColumn { .. }
        ));
        req.operation = AlterOperation::DropColumns {
            names: vec![String::from("k0")],
        };
        assert!(matches!(
            metadata.validate_alter(&req).err().unwrap(),
            Error::DropKeyColumn { .. }
        ));

        // Drop internal column.
        req.operation = AlterOperation::DropColumns {
            names: vec![String::from(consts::SEQUENCE_COLUMN_NAME)],
        };
        assert!(matches!(
            metadata.validate_alter(&req).err().unwrap(),
            Error::DropInternalColumn { .. }
        ));

        // Valid request
        req.operation = AlterOperation::DropColumns {
            names: vec![String::from("v0")],
        };
        metadata.validate_alter(&req).unwrap();
    }

    #[test]
    fn test_column_metadata_conversion() {
        let desc = ColumnDescriptorBuilder::new(123, "test", ConcreteDataType::int32_datatype())
            .is_nullable(false)
            .default_constraint(Some(ColumnDefaultConstraint::Value(Value::Int32(321))))
            .comment("hello")
            .build()
            .unwrap();

        let meta = ColumnMetadata {
            cf_id: consts::DEFAULT_CF_ID,
            desc: desc.clone(),
        };
        let column_schema = meta.to_column_schema().unwrap();
        let new_meta = ColumnMetadata::from_column_schema(&column_schema).unwrap();
        assert_eq!(meta, new_meta);

        let meta = ColumnMetadata { cf_id: 567, desc };
        let column_schema = meta.to_column_schema().unwrap();
        let new_meta = ColumnMetadata::from_column_schema(&column_schema).unwrap();
        assert_eq!(meta, new_meta);
    }
}
