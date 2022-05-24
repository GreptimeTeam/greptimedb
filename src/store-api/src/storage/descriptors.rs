use datatypes::value::Value;

use crate::storage::ConcreteDataType;

/// Id of column, unique in each region.
pub type ColumnId = u32;
/// Id of column family, unique in each region.
pub type ColumnFamilyId = u32;

// TODO(yingwen): Validate default value has same type with column, and name is a valid column name.
/// A [ColumnDescriptor] contains information to create a column.
#[derive(Debug)]
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

/// A [RowKeyDescriptor] contains information to about row key.
#[derive(Debug)]
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
#[derive(Debug)]
pub struct ColumnFamilyDescriptor {
    pub cf_id: ColumnFamilyId,
    pub name: String,
    /// Descriptors of columns in this column family.
    pub columns: Vec<ColumnDescriptor>,
}

/// A [RegionDescriptor] contains information to create a region.
#[derive(Debug)]
pub struct RegionDescriptor {
    /// Region name.
    name: String,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn new_column_descriptor_builder() -> ColumnDescriptorBuilder {
        ColumnDescriptorBuilder::new(3, "test", ConcreteDataType::float64_datatype())
    }

    #[test]
    fn test_column_descriptor_builder() {
        let desc = new_column_descriptor_builder().build();
        assert_eq!(3, desc.id);
        assert_eq!("test", desc.name);
        assert_eq!(ConcreteDataType::float64_datatype(), desc.data_type);
        assert!(desc.is_nullable);
        assert!(desc.default_value.is_none());
        assert!(desc.comment.is_empty());

        let desc = new_column_descriptor_builder().is_nullable(false).build();
        assert!(!desc.is_nullable);

        let desc = new_column_descriptor_builder()
            .default_value(Some(Value::Null))
            .build();
        assert_eq!(Value::Null, desc.default_value.unwrap());

        let desc = new_column_descriptor_builder()
            .default_value(Some(Value::Float64(1.0)))
            .build();
        assert_eq!(Value::Float64(1.0), desc.default_value.unwrap());

        let desc = new_column_descriptor_builder()
            .comment("A test column")
            .build();
        assert_eq!("A test column", desc.comment);
    }
}
