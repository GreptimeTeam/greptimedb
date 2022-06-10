use datatypes::value::Value;

use crate::storage::{consts, ColumnSchema, ConcreteDataType};

/// Id of column, unique in each region.
pub type ColumnId = u32;
/// Id of column family, unique in each region.
pub type ColumnFamilyId = u32;

// TODO(yingwen): Validate default value has same type with column, and name is a valid column name.
/// A [ColumnDescriptor] contains information to create a column.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDescriptor {
    pub id: ColumnId,
    pub name: String,
    pub data_type: ConcreteDataType,
    /// Is column nullable, default is true.
    pub is_nullable: bool,
    /// Default value of column, default is None, which means no default value
    /// for this column, and user must provide value for a not-null column.
    pub default_value: Option<Value>,
    pub comment: String,
}

impl From<&ColumnDescriptor> for ColumnSchema {
    fn from(desc: &ColumnDescriptor) -> ColumnSchema {
        ColumnSchema::new(&desc.name, desc.data_type.clone(), desc.is_nullable)
    }
}

/// A [RowKeyDescriptor] contains information about row key.
#[derive(Debug, Clone, PartialEq)]
pub struct RowKeyDescriptor {
    pub columns: Vec<ColumnDescriptor>,
    /// Timestamp key column.
    pub timestamp: ColumnDescriptor,
    /// Enable version column in row key if this field is true.
    ///
    /// The default value is true.
    pub enable_version_column: bool,
}

/// A [ColumnFamilyDescriptor] contains information to create a column family.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnFamilyDescriptor {
    pub cf_id: ColumnFamilyId,
    pub name: String,
    /// Descriptors of columns in this column family.
    pub columns: Vec<ColumnDescriptor>,
}

/// A [RegionDescriptor] contains information to create a region.
#[derive(Debug, Clone, PartialEq)]
pub struct RegionDescriptor {
    /// Region name.
    pub name: String,
    /// Row key descriptor of this region.
    pub row_key: RowKeyDescriptor,
    /// Default column family.
    pub default_cf: ColumnFamilyDescriptor,
    /// Extra column families defined by user.
    pub extra_cfs: Vec<ColumnFamilyDescriptor>,
}

pub struct ColumnDescriptorBuilder {
    id: ColumnId,
    name: String,
    data_type: ConcreteDataType,
    is_nullable: bool,
    default_value: Option<Value>,
    comment: String,
}

impl ColumnDescriptorBuilder {
    pub fn new<T: Into<String>>(id: ColumnId, name: T, data_type: ConcreteDataType) -> Self {
        Self {
            id,
            name: name.into(),
            data_type,
            is_nullable: true,
            default_value: None,
            comment: "".to_string(),
        }
    }

    pub fn is_nullable(mut self, is_nullable: bool) -> Self {
        self.is_nullable = is_nullable;
        self
    }

    pub fn default_value(mut self, value: Option<Value>) -> Self {
        self.default_value = value;
        self
    }

    pub fn comment<T: Into<String>>(mut self, comment: T) -> Self {
        self.comment = comment.into();
        self
    }

    pub fn build(self) -> ColumnDescriptor {
        ColumnDescriptor {
            id: self.id,
            name: self.name,
            data_type: self.data_type,
            is_nullable: self.is_nullable,
            default_value: self.default_value,
            comment: self.comment,
        }
    }
}

pub struct RowKeyDescriptorBuilder {
    columns: Vec<ColumnDescriptor>,
    timestamp: ColumnDescriptor,
    enable_version_column: bool,
}

impl RowKeyDescriptorBuilder {
    pub fn new(timestamp: ColumnDescriptor) -> Self {
        Self {
            columns: Vec::new(),
            timestamp,
            enable_version_column: true,
        }
    }

    pub fn columns_capacity(mut self, capacity: usize) -> Self {
        self.columns.reserve(capacity);
        self
    }

    pub fn push_column(mut self, column: ColumnDescriptor) -> Self {
        self.columns.push(column);
        self
    }

    pub fn enable_version_column(mut self, enable: bool) -> Self {
        self.enable_version_column = enable;
        self
    }

    pub fn build(self) -> RowKeyDescriptor {
        RowKeyDescriptor {
            columns: self.columns,
            timestamp: self.timestamp,
            enable_version_column: self.enable_version_column,
        }
    }
}

pub struct ColumnFamilyDescriptorBuilder {
    cf_id: ColumnFamilyId,
    name: String,
    columns: Vec<ColumnDescriptor>,
}

impl ColumnFamilyDescriptorBuilder {
    pub fn new() -> Self {
        Self {
            cf_id: consts::DEFAULT_CF_ID,
            name: consts::DEFAULT_CF_NAME.to_string(),
            columns: Vec::new(),
        }
    }

    pub fn cf_id(mut self, cf_id: ColumnFamilyId) -> Self {
        self.cf_id = cf_id;
        self
    }

    pub fn name<T: Into<String>>(mut self, name: T) -> Self {
        self.name = name.into();
        self
    }

    pub fn columns_capacity(mut self, capacity: usize) -> Self {
        self.columns.reserve(capacity);
        self
    }

    pub fn push_column(mut self, column: ColumnDescriptor) -> Self {
        self.columns.push(column);
        self
    }

    pub fn build(self) -> ColumnFamilyDescriptor {
        ColumnFamilyDescriptor {
            cf_id: self.cf_id,
            name: self.name,
            columns: self.columns,
        }
    }
}

impl Default for ColumnFamilyDescriptorBuilder {
    fn default() -> ColumnFamilyDescriptorBuilder {
        ColumnFamilyDescriptorBuilder::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_column_desc_builder() -> ColumnDescriptorBuilder {
        ColumnDescriptorBuilder::new(3, "test", ConcreteDataType::int32_datatype())
    }

    #[test]
    fn test_column_descriptor_builder() {
        let desc = new_column_desc_builder().build();
        assert_eq!(3, desc.id);
        assert_eq!("test", desc.name);
        assert_eq!(ConcreteDataType::int32_datatype(), desc.data_type);
        assert!(desc.is_nullable);
        assert!(desc.default_value.is_none());
        assert!(desc.comment.is_empty());

        let desc = new_column_desc_builder().is_nullable(false).build();
        assert!(!desc.is_nullable);

        let desc = new_column_desc_builder()
            .default_value(Some(Value::Null))
            .build();
        assert_eq!(Value::Null, desc.default_value.unwrap());

        let desc = new_column_desc_builder()
            .default_value(Some(Value::Int32(123)))
            .build();
        assert_eq!(Value::Int32(123), desc.default_value.unwrap());

        let desc = new_column_desc_builder().comment("A test column").build();
        assert_eq!("A test column", desc.comment);
    }

    fn new_timestamp_desc() -> ColumnDescriptor {
        ColumnDescriptorBuilder::new(5, "timestamp", ConcreteDataType::int64_datatype()).build()
    }

    #[test]
    fn test_row_key_descriptor_builder() {
        let timestamp = new_timestamp_desc();

        let desc = RowKeyDescriptorBuilder::new(timestamp.clone()).build();
        assert!(desc.columns.is_empty());
        assert!(desc.enable_version_column);

        let desc = RowKeyDescriptorBuilder::new(timestamp.clone())
            .columns_capacity(1)
            .push_column(
                ColumnDescriptorBuilder::new(6, "c1", ConcreteDataType::int32_datatype()).build(),
            )
            .push_column(
                ColumnDescriptorBuilder::new(7, "c2", ConcreteDataType::int32_datatype()).build(),
            )
            .build();
        assert_eq!(2, desc.columns.len());
        assert!(desc.enable_version_column);

        let desc = RowKeyDescriptorBuilder::new(timestamp)
            .enable_version_column(false)
            .build();
        assert!(desc.columns.is_empty());
        assert!(!desc.enable_version_column);
    }

    #[test]
    fn test_cf_descriptor_builder() {
        let desc = ColumnFamilyDescriptorBuilder::default().build();
        assert_eq!(consts::DEFAULT_CF_ID, desc.cf_id);
        assert_eq!(consts::DEFAULT_CF_NAME, desc.name);
        assert!(desc.columns.is_empty());

        let desc = ColumnFamilyDescriptorBuilder::new()
            .cf_id(32)
            .name("cf1")
            .build();
        assert_eq!(32, desc.cf_id);
        assert_eq!("cf1", desc.name);

        let desc = ColumnFamilyDescriptorBuilder::new()
            .push_column(
                ColumnDescriptorBuilder::new(6, "c1", ConcreteDataType::int32_datatype()).build(),
            )
            .build();
        assert_eq!(1, desc.columns.len());
    }
}
