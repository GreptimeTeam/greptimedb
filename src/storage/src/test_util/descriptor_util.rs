use datatypes::prelude::ConcreteDataType;
use store_api::storage::{
    ColumnDescriptor, ColumnDescriptorBuilder, ColumnFamilyDescriptorBuilder, ColumnId,
    RegionDescriptor, RowKeyDescriptorBuilder,
};

use crate::test_util::schema_util::{self, ColumnDef};

/// A RegionDescriptor builder for test.
pub struct RegionDescBuilder {
    name: String,
    last_column_id: ColumnId,
    key_builder: RowKeyDescriptorBuilder,
    default_cf_builder: ColumnFamilyDescriptorBuilder,
}

impl RegionDescBuilder {
    pub fn new<T: Into<String>>(name: T) -> Self {
        let key_builder = RowKeyDescriptorBuilder::new(
            ColumnDescriptorBuilder::new(2, "timestamp", ConcreteDataType::uint64_datatype())
                .build(),
        );

        Self {
            name: name.into(),
            last_column_id: 2,
            key_builder,
            default_cf_builder: ColumnFamilyDescriptorBuilder::new(),
        }
    }

    // This will reset the row key builder, so should be called by before `push_key_column()`
    // and `enable_version_column()`, or just call after `new()`.
    pub fn timestamp(mut self, column_def: ColumnDef) -> Self {
        let builder = RowKeyDescriptorBuilder::new(self.new_column(column_def));
        self.key_builder = builder;
        self
    }

    pub fn enable_version_column(mut self, enable: bool) -> Self {
        self.key_builder = self.key_builder.enable_version_column(enable);
        self
    }

    pub fn push_key_column(mut self, column_def: ColumnDef) -> Self {
        let column = self.new_column(column_def);
        self.key_builder = self.key_builder.push_column(column);
        self
    }

    pub fn push_value_column(mut self, column_def: ColumnDef) -> Self {
        let column = self.new_column(column_def);
        self.default_cf_builder = self.default_cf_builder.push_column(column);
        self
    }

    pub fn build(self) -> RegionDescriptor {
        RegionDescriptor {
            name: self.name,
            row_key: self.key_builder.build(),
            default_cf: self.default_cf_builder.build(),
            extra_cfs: Vec::new(),
        }
    }

    fn alloc_column_id(&mut self) -> ColumnId {
        self.last_column_id += 1;
        self.last_column_id
    }

    fn new_column(&mut self, column_def: ColumnDef) -> ColumnDescriptor {
        let datatype = schema_util::logical_type_id_to_concrete_type(column_def.1);
        ColumnDescriptorBuilder::new(self.alloc_column_id(), column_def.0, datatype)
            .is_nullable(column_def.2)
            .build()
    }
}
