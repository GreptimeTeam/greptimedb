use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use common_error::prelude::*;
use datatypes::arrow::datatypes::Field;
use datatypes::arrow::datatypes::Schema as ArrowSchema;
use datatypes::data_type::{ConcreteDataType, DataType};
use snafu::ensure;

use crate::storage::consts;
use crate::storage::descriptors::{
    ColumnDescriptor, ColumnDescriptorBuilder, ColumnFamilyDescriptor, ColumnFamilyId, ColumnId,
    RegionDescriptor, RowKeyDescriptor,
};
use crate::storage::{Schema, SchemaRef};

pub type RegionMetadataRef = Arc<RegionMetadata>;

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

/// In memory metadata of region.
pub struct RegionMetadata {
    /// Schema of the region.
    ///
    /// Holding a [SchemaRef] to allow converting into `SchemaRef`/`arrow::SchemaRef`
    /// conveniently. The fields order in `SchemaRef` **must** be consistent with
    /// columns order in [ColumnsMetadata] to ensure the projection index of a field
    /// is correct.
    schema: SchemaRef,
    columns: ColumnsMetadata,
    row_key: RowKeyMetadata,
    column_families: ColumnFamiliesMetadata,
}

struct ColumnMetadata {
    /// Id of column family that this column belongs to.
    cf_id: ColumnFamilyId,
    desc: ColumnDescriptor,
}

struct ColumnsMetadata {
    /// All columns, in `(key columns, timestamp, [version,] value columns)` order.
    ///
    /// Columns order should be consistent with fields order in [SchemaRef].
    columns: Vec<ColumnMetadata>,
    /// Maps column name to index of columns, used to fast lookup column by name.
    name_to_col_index: HashMap<String, usize>,
}

#[derive(Default)]
struct RowKeyMetadata {
    /// Exclusive end index of row key columns.
    row_key_end: usize,
    /// Index of timestamp key column.
    timestamp_key_index: usize,
    /// If version column is enabled, then the last column of key columns is a
    /// version column.
    enable_version_column: bool,
}

struct ColumnFamiliesMetadata {
    column_families: Vec<ColumnFamilyMetadata>,
    /// Map column family id to index of column family.
    id_to_cf_index: HashMap<ColumnFamilyId, usize>,
    /// Map column family name to index of column family.
    name_to_cf_index: HashMap<String, usize>,
}

struct ColumnFamilyMetadata {
    /// Column family name.
    name: String,
    cf_id: ColumnFamilyId,
    /// Inclusive start index of columns in the column family.
    column_index_start: usize,
    /// Exclusive end index of columns in the column family.
    column_index_end: usize,
}

impl TryFrom<RegionDescriptor> for RegionMetadata {
    type Error = Error;

    fn try_from(desc: RegionDescriptor) -> Result<RegionMetadata> {
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
    fields: Vec<Field>,
    name_to_col_index: HashMap<String, usize>,

    row_key: RowKeyMetadata,

    column_families: Vec<ColumnFamilyMetadata>,
    id_to_cf_index: HashMap<ColumnFamilyId, usize>,
    name_to_cf_index: HashMap<String, usize>,
}

impl RegionMetadataBuilder {
    fn new() -> RegionMetadataBuilder {
        RegionMetadataBuilder::default()
    }

    fn row_key(mut self, key: RowKeyDescriptor) -> Result<Self> {
        for col in key.columns {
            self.push_row_key_column(col)?;
        }

        let timestamp_key_index = self.columns.len();
        self.push_row_key_column(key.timestamp)?;

        if key.enable_version_column {
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
            !self.name_to_cf_index.contains_key(&cf.name),
            CfNameExistsSnafu { name: &cf.name }
        );

        ensure!(
            !self.id_to_cf_index.contains_key(&cf.cf_id),
            CfIdExistsSnafu { id: cf.cf_id }
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

        let cf_index = self.column_families.len();
        self.column_families.push(cf_meta);
        self.id_to_cf_index.insert(cf.cf_id, cf_index);
        self.name_to_cf_index.insert(cf.name, cf_index);

        Ok(self)
    }

    fn build(self) -> RegionMetadata {
        let arrow_schema = Arc::new(ArrowSchema {
            fields: self.fields,
            metadata: BTreeMap::new(),
        });

        RegionMetadata {
            schema: Arc::new(Schema::new(arrow_schema)),
            columns: ColumnsMetadata {
                columns: self.columns,
                name_to_col_index: self.name_to_col_index,
            },
            row_key: self.row_key,
            column_families: ColumnFamiliesMetadata {
                column_families: self.column_families,
                id_to_cf_index: self.id_to_cf_index,
                name_to_cf_index: self.name_to_cf_index,
            },
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

        let field = column_desc_to_field(&desc);

        let column_name = desc.name.clone();
        let meta = ColumnMetadata { cf_id, desc };

        // TODO(yingwen): Store cf_id to metadata in field.

        let column_index = self.columns.len();
        self.columns.push(meta);
        self.fields.push(field);
        self.name_to_col_index.insert(column_name, column_index);

        Ok(())
    }
}

fn column_desc_to_field(desc: &ColumnDescriptor) -> Field {
    Field::new(&desc.name, desc.data_type.as_arrow_type(), desc.is_nullable)
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
