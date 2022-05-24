use crate::storage::ConcreteDataType;

/// A [ColumnDescriptor] contains information about a column.
#[derive(Debug)]
pub struct ColumnDescriptor {
    pub name: String,
    pub data_type: ConcreteDataType,
    pub is_nullable: bool,
}

/// A [KeyDescriptor] contains information about a row key.
#[derive(Debug)]
pub struct KeyDescriptor {
    pub columns: Vec<ColumnDescriptor>,
    pub timestamp: ColumnDescriptor,
    /// Enable version column in row key if this field is true.
    ///
    /// The default value is true.
    pub enable_version_column: bool,
}

/// A [ColumnFamilyDescriptor] contains information about a column family.
#[derive(Debug)]
pub struct ColumnFamilyDescriptor {
    pub name: String,
    /// Descriptors of columns in this column family.
    pub columns: Vec<ColumnDescriptor>,
}

/// A [RegionDescriptor] contains information about a region.
#[derive(Debug)]
pub struct RegionDescriptor {
    /// Row key descriptor of this region.
    pub key: KeyDescriptor,
    /// Default column family.
    pub default_cf: ColumnFamilyDescriptor,
    /// Extra column families defined by user.
    pub extra_cfs: Vec<ColumnFamilyDescriptor>,
}
