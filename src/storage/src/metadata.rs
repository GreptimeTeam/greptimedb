use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use common_error::prelude::*;
use datatypes::data_type::ConcreteDataType;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt};
use store_api::storage::{
    consts::{self, ReservedColumnId},
    ColumnDescriptor, ColumnDescriptorBuilder, ColumnFamilyDescriptor, ColumnFamilyId, ColumnId,
    ColumnSchema, RegionDescriptor, RegionId, RegionMeta, RowKeyDescriptor, Schema, SchemaRef,
};

use crate::manifest::action::{RawColumnFamiliesMetadata, RawColumnsMetadata, RawRegionMetadata};
use crate::schema::{RegionSchema, RegionSchemaRef};

/// Error for handling metadata.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Column name {} already exists", name))]
    ColNameExists { name: String, backtrace: Backtrace },

    #[snafu(display("Column family name {} already exists", name))]
    CfNameExists { name: String, backtrace: Backtrace },

    #[snafu(display("Column family id {} already exists", id))]
    CfIdExists { id: ColumnId, backtrace: Backtrace },

    #[snafu(display("Column id {} already exists", id))]
    ColIdExists { id: ColumnId, backtrace: Backtrace },

    #[snafu(display("Failed to build schema, source: {}", source))]
    InvalidSchema {
        #[snafu(backtrace)]
        source: crate::schema::Error,
    },

    #[snafu(display("Column name {} is reserved by the system", name))]
    ReservedColumn { name: String, backtrace: Backtrace },

    #[snafu(display("Missing timestamp key column"))]
    MissingTimestamp { backtrace: Backtrace },
}

pub type Result<T> = std::result::Result<T, Error>;

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
}

pub type VersionNumber = u32;

// TODO(yingwen): We may need to hold a list of history schema.

/// In memory metadata of region.
#[derive(Clone, Debug, PartialEq)]
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
}

pub type RegionMetadataRef = Arc<RegionMetadata>;

impl From<&RegionMetadata> for RawRegionMetadata {
    fn from(data: &RegionMetadata) -> RawRegionMetadata {
        RawRegionMetadata {
            id: data.id,
            name: data.name.clone(),
            columns: (&*data.columns).into(),
            column_families: (&data.column_families).into(),
            version: data.version,
        }
    }
}

impl TryFrom<RawRegionMetadata> for RegionMetadata {
    type Error = Error;

    fn try_from(raw: RawRegionMetadata) -> Result<RegionMetadata> {
        let columns = Arc::new(ColumnsMetadata::from(raw.columns));
        let schema =
            Arc::new(RegionSchema::new(columns.clone(), raw.version).context(InvalidSchemaSnafu)?);

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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ColumnMetadata {
    pub cf_id: ColumnFamilyId,
    pub desc: ColumnDescriptor,
}

#[derive(Clone, Debug, PartialEq)]
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
    /// If version column is enabled, then the last column of key columns is a
    /// version column.
    enable_version_column: bool,
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
    pub fn iter_value_columns(&self) -> impl Iterator<Item = &ColumnMetadata> {
        self.columns[self.row_key_end..self.user_column_end].iter()
    }

    pub fn iter_user_columns(&self) -> impl Iterator<Item = &ColumnMetadata> {
        self.columns.iter().take(self.user_column_end)
    }

    pub fn iter_all_columns(&self) -> impl Iterator<Item = &ColumnMetadata> {
        self.columns.iter()
    }

    #[inline]
    pub fn num_row_key_columns(&self) -> usize {
        self.row_key_end
    }

    #[inline]
    pub fn num_value_columns(&self) -> usize {
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
}

pub type ColumnsMetadataRef = Arc<ColumnsMetadata>;

impl From<&ColumnsMetadata> for RawColumnsMetadata {
    fn from(data: &ColumnsMetadata) -> RawColumnsMetadata {
        RawColumnsMetadata {
            columns: data.columns.clone(),
            row_key_end: data.row_key_end,
            timestamp_key_index: data.timestamp_key_index,
            enable_version_column: data.enable_version_column,
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
            enable_version_column: raw.enable_version_column,
            user_column_end: raw.user_column_end,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
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

impl TryFrom<RegionDescriptor> for RegionMetadata {
    type Error = Error;

    fn try_from(desc: RegionDescriptor) -> Result<RegionMetadata> {
        // Doesn't set version explicitly here, because this is a new region meta
        // created from descriptor, using initial version is reasonable.
        let mut builder = RegionMetadataBuilder::new()
            .name(desc.name)
            .id(desc.id)
            .row_key(desc.row_key)?
            .add_column_family(desc.default_cf)?;
        for cf in desc.extra_cfs {
            builder = builder.add_column_family(cf)?;
        }

        builder.build()
    }
}

// TODO(yingwen): Add a builder to build ColumnMetadata and refactor this builder.
#[derive(Default)]
struct RegionMetadataBuilder {
    id: RegionId,
    name: String,
    columns: Vec<ColumnMetadata>,
    column_schemas: Vec<ColumnSchema>,
    name_to_col_index: HashMap<String, usize>,
    /// Column id set, used to validate column id uniqueness.
    column_ids: HashSet<ColumnId>,

    // Row key metadata:
    row_key_end: usize,
    timestamp_key_index: Option<usize>,
    enable_version_column: bool,

    id_to_cfs: HashMap<ColumnFamilyId, ColumnFamilyMetadata>,
    cf_names: HashSet<String>,
}

impl RegionMetadataBuilder {
    fn new() -> RegionMetadataBuilder {
        RegionMetadataBuilder::default()
    }

    fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    fn id(mut self, id: RegionId) -> Self {
        self.id = id;
        self
    }

    fn row_key(mut self, key: RowKeyDescriptor) -> Result<Self> {
        for col in key.columns {
            self.push_row_key_column(col)?;
        }

        // TODO(yingwen): Validate this is a timestamp column.
        self.timestamp_key_index = Some(self.columns.len());
        self.push_row_key_column(key.timestamp)?;

        if key.enable_version_column {
            // TODO(yingwen): Validate that version column must be uint64 column.
            let version_col = version_column_desc();
            self.push_row_key_column(version_col)?;
        }

        self.row_key_end = self.columns.len();
        self.enable_version_column = key.enable_version_column;

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

    fn build(mut self) -> Result<RegionMetadata> {
        let timestamp_key_index = self.timestamp_key_index.context(MissingTimestampSnafu)?;

        let user_column_end = self.columns.len();
        // Setup internal columns.
        for internal_desc in internal_column_descs() {
            self.push_new_column(consts::DEFAULT_CF_ID, internal_desc)?;
        }

        let columns = Arc::new(ColumnsMetadata {
            columns: self.columns,
            name_to_col_index: self.name_to_col_index,
            row_key_end: self.row_key_end,
            timestamp_key_index,
            enable_version_column: self.enable_version_column,
            user_column_end,
        });
        let schema = Arc::new(
            RegionSchema::new(columns.clone(), Schema::INITIAL_VERSION)
                .context(InvalidSchemaSnafu)?,
        );

        Ok(RegionMetadata {
            id: self.id,
            name: self.name,
            schema,
            columns,
            column_families: ColumnFamiliesMetadata {
                id_to_cfs: self.id_to_cfs,
            },
            version: 0,
        })
    }

    // Helper methods:

    fn push_row_key_column(&mut self, desc: ColumnDescriptor) -> Result<()> {
        self.push_value_column(consts::KEY_CF_ID, desc)
    }

    fn push_value_column(&mut self, cf_id: ColumnFamilyId, desc: ColumnDescriptor) -> Result<()> {
        ensure!(
            !is_internal_value_column(&desc.name),
            ReservedColumnSnafu { name: &desc.name }
        );

        self.push_new_column(cf_id, desc)
    }

    fn push_new_column(&mut self, cf_id: ColumnFamilyId, desc: ColumnDescriptor) -> Result<()> {
        ensure!(
            !self.name_to_col_index.contains_key(&desc.name),
            ColNameExistsSnafu { name: &desc.name }
        );
        ensure!(
            !self.column_ids.contains(&desc.id),
            ColIdExistsSnafu { id: desc.id }
        );

        let column_schema = ColumnSchema::from(&desc);

        let column_name = desc.name.clone();
        let column_id = desc.id;
        let meta = ColumnMetadata { cf_id, desc };

        let column_index = self.columns.len();
        self.columns.push(meta);
        self.column_schemas.push(column_schema);
        self.name_to_col_index.insert(column_name, column_index);
        self.column_ids.insert(column_id);

        Ok(())
    }
}

fn version_column_desc() -> ColumnDescriptor {
    ColumnDescriptorBuilder::new(
        ReservedColumnId::version(),
        consts::VERSION_COLUMN_NAME.to_string(),
        ConcreteDataType::uint64_datatype(),
    )
    .is_nullable(false)
    .build()
    .unwrap()
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
            ReservedColumnId::value_type(),
            consts::VALUE_TYPE_COLUMN_NAME.to_string(),
            ConcreteDataType::uint8_datatype(),
        )
        .is_nullable(false)
        .build()
        .unwrap(),
    ]
}

/// Returns true if this is an internal column for value column.
#[inline]
fn is_internal_value_column(column_name: &str) -> bool {
    matches!(
        column_name,
        consts::SEQUENCE_COLUMN_NAME | consts::VALUE_TYPE_COLUMN_NAME
    )
}

// TODO(yingwen): Add tests for using invalid row_key/cf to build metadata.
#[cfg(test)]
mod tests {
    use datatypes::type_id::LogicalTypeId;
    use store_api::storage::{
        ColumnDescriptorBuilder, ColumnFamilyDescriptorBuilder, RowKeyDescriptorBuilder,
    };

    use super::*;
    use crate::test_util::descriptor_util::RegionDescBuilder;
    use crate::test_util::schema_util;

    const TEST_REGION: &str = "test-region";

    #[test]
    fn test_descriptor_to_region_metadata() {
        let region_name = "region-0";
        let desc = RegionDescBuilder::new(region_name)
            .timestamp(("ts", LogicalTypeId::Int64, false))
            .enable_version_column(false)
            .push_key_column(("k1", LogicalTypeId::Int32, false))
            .push_value_column(("v1", LogicalTypeId::Float32, true))
            .build();

        let expect_schema = schema_util::new_schema_ref(
            &[
                ("k1", LogicalTypeId::Int32, false),
                ("ts", LogicalTypeId::Int64, false),
                ("v1", LogicalTypeId::Float32, true),
            ],
            Some(1),
        );

        let metadata = RegionMetadata::try_from(desc).unwrap();
        assert_eq!(region_name, metadata.name);
        assert_eq!(expect_schema, *metadata.user_schema());
        assert_eq!(2, metadata.columns.num_row_key_columns());
        assert_eq!(1, metadata.columns.num_value_columns());
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
        let names = [consts::SEQUENCE_COLUMN_NAME, consts::VALUE_TYPE_COLUMN_NAME];
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

    fn new_metadata(enable_version_column: bool) -> RegionMetadata {
        let timestamp = ColumnDescriptorBuilder::new(2, "ts", ConcreteDataType::int64_datatype())
            .is_nullable(false)
            .build()
            .unwrap();
        let row_key = RowKeyDescriptorBuilder::new(timestamp)
            .push_column(
                ColumnDescriptorBuilder::new(3, "k1", ConcreteDataType::int64_datatype())
                    .is_nullable(false)
                    .build()
                    .unwrap(),
            )
            .enable_version_column(enable_version_column)
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
        let metadata = new_metadata(false);
        assert_eq!(TEST_REGION, metadata.name);

        let expect_schema = schema_util::new_schema_ref(
            &[
                ("k1", LogicalTypeId::Int64, false),
                ("ts", LogicalTypeId::Int64, false),
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
        assert_eq!(1, metadata.columns.num_value_columns());
        let value_names: Vec<_> = metadata
            .columns
            .iter_value_columns()
            .map(|column| &column.desc.name)
            .collect();
        assert_eq!(["v1"], &value_names[..]);
        // Check timestamp index.
        assert_eq!(1, metadata.columns.timestamp_key_index);
        // Check version column.
        assert!(!metadata.columns.enable_version_column);

        assert!(metadata
            .column_families
            .cf_by_id(consts::DEFAULT_CF_ID)
            .is_some());

        assert_eq!(0, metadata.version);
    }

    #[test]
    fn test_build_metedata_enable_version() {
        let metadata = new_metadata(true);
        assert_eq!(TEST_REGION, metadata.name);

        let expect_schema = schema_util::new_schema_ref(
            &[
                ("k1", LogicalTypeId::Int64, false),
                ("ts", LogicalTypeId::Int64, false),
                (consts::VERSION_COLUMN_NAME, LogicalTypeId::UInt64, false),
                ("v1", LogicalTypeId::Int64, true),
            ],
            Some(1),
        );

        assert_eq!(expect_schema, *metadata.user_schema());

        // 4 user columns and 2 internal columns.
        assert_eq!(6, metadata.columns.columns.len());
        // 3 row key columns
        assert_eq!(3, metadata.columns.num_row_key_columns());
        let row_key_names: Vec<_> = metadata
            .columns
            .iter_row_key_columns()
            .map(|column| &column.desc.name)
            .collect();
        assert_eq!(
            ["k1", "ts", consts::VERSION_COLUMN_NAME],
            &row_key_names[..]
        );
        // 1 value column
        assert_eq!(1, metadata.columns.num_value_columns());
        let value_names: Vec<_> = metadata
            .columns
            .iter_value_columns()
            .map(|column| &column.desc.name)
            .collect();
        assert_eq!(["v1"], &value_names[..]);
        // Check timestamp index.
        assert_eq!(1, metadata.columns.timestamp_key_index);
        // Check version column.
        assert!(metadata.columns.enable_version_column);
    }

    #[test]
    fn test_convert_between_raw() {
        let metadata = new_metadata(true);
        let raw = RawRegionMetadata::from(&metadata);

        let converted = RegionMetadata::try_from(raw).unwrap();
        assert_eq!(metadata, converted);
    }
}
