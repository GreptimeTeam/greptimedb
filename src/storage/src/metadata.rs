use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use common_error::prelude::*;
use datatypes::data_type::ConcreteDataType;
use snafu::ensure;
use store_api::storage::{
    consts, ColumnDescriptor, ColumnDescriptorBuilder, ColumnFamilyDescriptor, ColumnFamilyId,
    ColumnId, ColumnSchema, RegionDescriptor, RegionMeta, RowKeyDescriptor, Schema, SchemaRef,
};

/// Error for handling metadata.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Column name already exists, name: {}", name))]
    ColNameExists { name: String, backtrace: Backtrace },

    #[snafu(display("Column family name already exists, name: {}", name))]
    CfNameExists { name: String, backtrace: Backtrace },

    #[snafu(display("Column family id already exists, id: {}", id))]
    CfIdExists { id: ColumnId, backtrace: Backtrace },
}

pub type Result<T> = std::result::Result<T, Error>;

/// Implementation of [RegionMeta].
///
/// Holds a snapshot of region metadata.
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
        &self.metadata.schema
    }
}

pub type VersionNumber = u32;

// TODO(yingwen): Make some fields of metadata private.

/// In memory metadata of region.
#[derive(Clone)]
pub struct RegionMetadata {
    /// Schema of the region.
    ///
    /// Holding a [SchemaRef] to allow converting into `SchemaRef`/`arrow::SchemaRef`
    /// conveniently. The fields order in `SchemaRef` **must** be consistent with
    /// columns order in [ColumnsMetadata] to ensure the projection index of a field
    /// is correct.
    pub schema: SchemaRef,
    pub columns_row_key: ColumnsRowKeyMetadataRef,
    pub column_families: ColumnFamiliesMetadata,
    /// Version of the metadata. Version is set to zero initially and bumped once the
    /// metadata have been altered.
    pub version: VersionNumber,
}

pub type RegionMetadataRef = Arc<RegionMetadata>;

#[derive(Clone)]
pub struct ColumnMetadata {
    /// Id of column family that this column belongs to.
    pub cf_id: ColumnFamilyId,
    pub desc: ColumnDescriptor,
}

#[derive(Clone)]
pub struct ColumnsMetadata {
    /// All columns, in `(key columns, timestamp, [version,] value columns)` order.
    ///
    /// Columns order should be consistent with fields order in [SchemaRef].
    pub columns: Vec<ColumnMetadata>,
    /// Maps column name to index of columns, used to fast lookup column by name.
    pub name_to_col_index: HashMap<String, usize>,
}

#[derive(Default, Clone)]
pub struct RowKeyMetadata {
    /// Exclusive end index of row key columns.
    row_key_end: usize,
    /// Index of timestamp key column.
    pub timestamp_key_index: usize,
    /// If version column is enabled, then the last column of key columns is a
    /// version column.
    pub enable_version_column: bool,
}

#[derive(Clone)]
pub struct ColumnsRowKeyMetadata {
    columns: ColumnsMetadata,
    row_key: RowKeyMetadata,
}

impl ColumnsRowKeyMetadata {
    pub fn iter_row_key_columns(&self) -> impl Iterator<Item = &ColumnMetadata> {
        self.columns.columns.iter().take(self.row_key.row_key_end)
    }

    pub fn iter_value_columns(&self) -> impl Iterator<Item = &ColumnMetadata> {
        self.columns.columns.iter().skip(self.row_key.row_key_end)
    }

    #[inline]
    pub fn num_row_key_columns(&self) -> usize {
        self.row_key.row_key_end
    }

    #[inline]
    pub fn num_value_columns(&self) -> usize {
        self.columns.columns.len() - self.row_key.row_key_end
    }
}

pub type ColumnsRowKeyMetadataRef = Arc<ColumnsRowKeyMetadata>;

// TODO(yingwen): id to cfs metadata, name to id.
#[derive(Clone)]
pub struct ColumnFamiliesMetadata {
    /// Map column family id to column family metadata.
    pub id_to_cfs: HashMap<ColumnFamilyId, ColumnFamilyMetadata>,
}

#[derive(Clone)]
pub struct ColumnFamilyMetadata {
    /// Column family name.
    pub name: String,
    pub cf_id: ColumnFamilyId,
    /// Inclusive start index of columns in the column family.
    pub column_index_start: usize,
    /// Exclusive end index of columns in the column family.
    pub column_index_end: usize,
}

impl TryFrom<RegionDescriptor> for RegionMetadata {
    type Error = Error;

    fn try_from(desc: RegionDescriptor) -> Result<RegionMetadata> {
        // Doesn't set version explicitly here, because this is a new region meta
        // created from descriptor, using initial version is reasonable.
        let mut builder = RegionMetadataBuilder::new()
            .row_key(desc.row_key)?
            .add_column_family(desc.default_cf)?;
        for cf in desc.extra_cfs {
            builder = builder.add_column_family(cf)?;
        }

        Ok(builder.build())
    }
}

#[derive(Default)]
struct RegionMetadataBuilder {
    columns: Vec<ColumnMetadata>,
    column_schemas: Vec<ColumnSchema>,
    name_to_col_index: HashMap<String, usize>,

    row_key: RowKeyMetadata,

    id_to_cfs: HashMap<ColumnFamilyId, ColumnFamilyMetadata>,
    cf_names: HashSet<String>,
}

impl RegionMetadataBuilder {
    fn new() -> RegionMetadataBuilder {
        RegionMetadataBuilder::default()
    }

    fn row_key(mut self, key: RowKeyDescriptor) -> Result<Self> {
        for col in key.columns {
            self.push_row_key_column(col)?;
        }

        // TODO(yingwen): Validate this is a timestamp column.
        let timestamp_key_index = self.columns.len();
        self.push_row_key_column(key.timestamp)?;

        if key.enable_version_column {
            // TODO(yingwen): Validate that version column must be uint64 column.
            let version_col = version_column_desc();
            self.push_row_key_column(version_col)?;
        }

        let row_key_end = self.columns.len();

        self.row_key = RowKeyMetadata {
            row_key_end,
            timestamp_key_index,
            enable_version_column: key.enable_version_column,
        };

        Ok(self)
    }

    fn add_column_family(mut self, cf: ColumnFamilyDescriptor) -> Result<Self> {
        ensure!(
            !self.id_to_cfs.contains_key(&cf.cf_id),
            CfIdExistsSnafu { id: cf.cf_id }
        );

        ensure!(
            !self.cf_names.contains(&cf.name),
            CfNameExistsSnafu { name: &cf.name }
        );

        let column_index_start = self.columns.len();
        let column_index_end = column_index_start + cf.columns.len();
        for col in cf.columns {
            self.push_value_column(cf.cf_id, col)?;
        }

        let cf_meta = ColumnFamilyMetadata {
            name: cf.name.clone(),
            cf_id: cf.cf_id,
            column_index_start,
            column_index_end,
        };

        self.id_to_cfs.insert(cf.cf_id, cf_meta);
        self.cf_names.insert(cf.name);

        Ok(self)
    }

    fn build(self) -> RegionMetadata {
        let schema = Arc::new(Schema::new(self.column_schemas));
        let columns = ColumnsMetadata {
            columns: self.columns,
            name_to_col_index: self.name_to_col_index,
        };
        let columns_row_key = Arc::new(ColumnsRowKeyMetadata {
            columns,
            row_key: self.row_key,
        });

        RegionMetadata {
            schema,
            columns_row_key,
            column_families: ColumnFamiliesMetadata {
                id_to_cfs: self.id_to_cfs,
            },
            version: 0,
        }
    }

    // Helper methods:

    fn push_row_key_column(&mut self, desc: ColumnDescriptor) -> Result<()> {
        self.push_value_column(consts::KEY_CF_ID, desc)
    }

    fn push_value_column(&mut self, cf_id: ColumnFamilyId, desc: ColumnDescriptor) -> Result<()> {
        ensure!(
            !self.name_to_col_index.contains_key(&desc.name),
            ColNameExistsSnafu { name: &desc.name }
        );

        let column_schema = ColumnSchema::from(&desc);

        let column_name = desc.name.clone();
        let meta = ColumnMetadata { cf_id, desc };

        // TODO(yingwen): Store cf_id to metadata in field.

        let column_index = self.columns.len();
        self.columns.push(meta);
        self.column_schemas.push(column_schema);
        self.name_to_col_index.insert(column_name, column_index);

        Ok(())
    }
}

fn version_column_desc() -> ColumnDescriptor {
    ColumnDescriptorBuilder::new(
        consts::VERSION_COLUMN_ID,
        consts::VERSION_COLUMN_NAME.to_string(),
        ConcreteDataType::uint64_datatype(),
    )
    .is_nullable(false)
    .build()
}
