use datatypes::value::Value;
use serde::{Deserialize, Serialize};

use crate::manifest::MetadataId;
use crate::storage::{consts, ColumnSchema, ConcreteDataType};

/// Id of column, unique in each region.
pub type ColumnId = u32;
/// Id of column family, unique in each region.
pub type ColumnFamilyId = u32;
pub type RegionId = u32;
impl MetadataId for RegionId {}
/// Default region name prefix
pub const REGION_PREFIX: &str = "r_";

#[inline]
pub fn gen_region_name(id: RegionId) -> String {
    format!("{}{}", REGION_PREFIX, id)
}

// TODO(yingwen): Validate default value has same type with column, and name is a valid column name.
/// A [ColumnDescriptor] contains information to create a column.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Builder)]
#[builder(pattern = "owned")]
pub struct ColumnDescriptor {
    pub id: ColumnId,
    #[builder(setter(into))]
    pub name: String,
    pub data_type: ConcreteDataType,
    /// Is column nullable, default is true.
    #[builder(default = "true")]
    pub is_nullable: bool,
    /// Default value of column, default is None, which means no default value
    /// for this column, and user must provide value for a not-null column.
    #[builder(default)]
    pub default_value: Option<Value>,
    #[builder(default, setter(into))]
    pub comment: String,
}

impl ColumnDescriptorBuilder {
    pub fn new<S: Into<String>>(id: ColumnId, name: S, data_type: ConcreteDataType) -> Self {
        Self {
            id: Some(id),
            name: Some(name.into()),
            data_type: Some(data_type),
            ..Default::default()
        }
    }
}

impl From<&ColumnDescriptor> for ColumnSchema {
    fn from(desc: &ColumnDescriptor) -> ColumnSchema {
        ColumnSchema::new(&desc.name, desc.data_type.clone(), desc.is_nullable)
    }
}

/// A [RowKeyDescriptor] contains information about row key.
#[derive(Debug, Clone, PartialEq, Builder)]
#[builder(pattern = "owned")]
pub struct RowKeyDescriptor {
    #[builder(default)]
    pub columns: Vec<ColumnDescriptor>,
    /// Timestamp key column.
    pub timestamp: ColumnDescriptor,
    /// Enable version column in row key if this field is true.
    ///
    /// The default value is true.
    // FIXME(yingwen): Change default value to true (Disable version column by
    // default).
    #[builder(default = "false")]
    pub enable_version_column: bool,
}

/// A [ColumnFamilyDescriptor] contains information to create a column family.
#[derive(Debug, Clone, PartialEq, Builder)]
pub struct ColumnFamilyDescriptor {
    #[builder(default = "consts::DEFAULT_CF_ID")]
    pub cf_id: ColumnFamilyId,
    #[builder(default = "consts::DEFAULT_CF_NAME.to_string()", setter(into))]
    pub name: String,
    /// Descriptors of columns in this column family.
    #[builder(default)]
    pub columns: Vec<ColumnDescriptor>,
}

/// A [RegionDescriptor] contains information to create a region.
#[derive(Debug, Clone, PartialEq)]
pub struct RegionDescriptor {
    pub id: RegionId,
    /// Region name.
    pub name: String,
    /// Row key descriptor of this region.
    pub row_key: RowKeyDescriptor,
    /// Default column family.
    pub default_cf: ColumnFamilyDescriptor,
    /// Extra column families defined by user.
    pub extra_cfs: Vec<ColumnFamilyDescriptor>,
}

impl RowKeyDescriptorBuilder {
    pub fn new(timestamp: ColumnDescriptor) -> Self {
        Self {
            timestamp: Some(timestamp),
            ..Default::default()
        }
    }

    pub fn columns_capacity(mut self, capacity: usize) -> Self {
        self.columns = Some(Vec::with_capacity(capacity));
        self
    }

    pub fn push_column(mut self, column: ColumnDescriptor) -> Self {
        if self.columns.is_none() {
            self.columns = Some(Vec::new());
        }

        self.columns.as_mut().unwrap().push(column);
        self
    }
}

impl ColumnFamilyDescriptorBuilder {
    pub fn columns_capacity(mut self, capacity: usize) -> Self {
        self.columns = Some(Vec::with_capacity(capacity));
        self
    }

    pub fn push_column(mut self, column: ColumnDescriptor) -> Self {
        if self.columns.is_none() {
            self.columns = Some(Vec::new());
        }

        self.columns.as_mut().unwrap().push(column);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[inline]
    fn new_column_desc_builder() -> ColumnDescriptorBuilder {
        ColumnDescriptorBuilder::new(3, "test", ConcreteDataType::int32_datatype())
    }

    #[test]
    fn test_column_descriptor_builder() {
        let desc = new_column_desc_builder().build().unwrap();
        assert_eq!(3, desc.id);
        assert_eq!("test", desc.name);
        assert_eq!(ConcreteDataType::int32_datatype(), desc.data_type);
        assert!(desc.is_nullable);
        assert!(desc.default_value.is_none());
        assert!(desc.comment.is_empty());

        let desc = new_column_desc_builder()
            .is_nullable(false)
            .build()
            .unwrap();
        assert!(!desc.is_nullable);

        let desc = new_column_desc_builder()
            .default_value(Some(Value::Null))
            .build()
            .unwrap();
        assert_eq!(Value::Null, desc.default_value.unwrap());

        let desc = new_column_desc_builder()
            .default_value(Some(Value::Int32(123)))
            .build()
            .unwrap();
        assert_eq!(Value::Int32(123), desc.default_value.unwrap());

        let desc = new_column_desc_builder()
            .comment("A test column")
            .build()
            .unwrap();
        assert_eq!("A test column", desc.comment);
    }

    fn new_timestamp_desc() -> ColumnDescriptor {
        ColumnDescriptorBuilder::new(5, "timestamp", ConcreteDataType::int64_datatype())
            .build()
            .unwrap()
    }

    #[test]
    fn test_row_key_descriptor_builder() {
        let timestamp = new_timestamp_desc();

        let desc = RowKeyDescriptorBuilder::new(timestamp.clone())
            .build()
            .unwrap();
        assert!(desc.columns.is_empty());
        assert!(!desc.enable_version_column);

        let desc = RowKeyDescriptorBuilder::new(timestamp.clone())
            .columns_capacity(1)
            .push_column(
                ColumnDescriptorBuilder::new(6, "c1", ConcreteDataType::int32_datatype())
                    .build()
                    .unwrap(),
            )
            .push_column(
                ColumnDescriptorBuilder::new(7, "c2", ConcreteDataType::int32_datatype())
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        assert_eq!(2, desc.columns.len());
        assert!(!desc.enable_version_column);

        let desc = RowKeyDescriptorBuilder::new(timestamp)
            .enable_version_column(false)
            .build()
            .unwrap();
        assert!(desc.columns.is_empty());
        assert!(!desc.enable_version_column);
    }

    #[test]
    fn test_cf_descriptor_builder() {
        let desc = ColumnFamilyDescriptorBuilder::default().build().unwrap();
        assert_eq!(consts::DEFAULT_CF_ID, desc.cf_id);
        assert_eq!(consts::DEFAULT_CF_NAME, desc.name);
        assert!(desc.columns.is_empty());

        let desc = ColumnFamilyDescriptorBuilder::default()
            .cf_id(32)
            .name("cf1")
            .build()
            .unwrap();
        assert_eq!(32, desc.cf_id);
        assert_eq!("cf1", desc.name);

        let desc = ColumnFamilyDescriptorBuilder::default()
            .push_column(
                ColumnDescriptorBuilder::default()
                    .id(6)
                    .name("c1")
                    .data_type(ConcreteDataType::int32_datatype())
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        assert_eq!(1, desc.columns.len());
    }

    #[test]
    fn test_gen_region_name() {
        assert_eq!("r_0", gen_region_name(0));
        assert_eq!("r_99", gen_region_name(99));
    }
}
