use datatypes::prelude::ConcreteDataType;
use datatypes::type_id::LogicalTypeId;
use store_api::storage::{
    ColumnDescriptor, ColumnDescriptorBuilder, ColumnFamilyDescriptorBuilder, ColumnId,
    RegionDescriptor, RegionId, RowKeyDescriptorBuilder,
};

use crate::test_util::{self, schema_util::ColumnDef};

/// A RegionDescriptor builder for test.
pub struct RegionDescBuilder {
    id: RegionId,
    name: String,
    last_column_id: ColumnId,
    key_builder: RowKeyDescriptorBuilder,
    default_cf_builder: ColumnFamilyDescriptorBuilder,
}

impl RegionDescBuilder {
    pub fn new<T: Into<String>>(name: T) -> Self {
        let key_builder = RowKeyDescriptorBuilder::new(
            ColumnDescriptorBuilder::new(
                2,
                test_util::TIMESTAMP_NAME,
                ConcreteDataType::int64_datatype(),
            )
            .is_nullable(false)
            .build()
            .unwrap(),
        );

        Self {
            id: 0,
            name: name.into(),
            last_column_id: 2,
            key_builder,
            default_cf_builder: ColumnFamilyDescriptorBuilder::default(),
        }
    }

    pub fn id(mut self, id: RegionId) -> Self {
        self.id = id;
        self
    }

    // This will reset the row key builder, so should be called before `push_key_column()`
    // and `enable_version_column()`, or just call after `new()`.
    //
    // NOTE(ning): it's now possible to change this function to:
    //
    // ```
    // self.key_builder.timestamp(self.new_column(column_def))
    // ```
    // to resolve the constraint above

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
            id: self.id,
            name: self.name,
            row_key: self.key_builder.build().unwrap(),
            default_cf: self.default_cf_builder.build().unwrap(),
            extra_cfs: Vec::new(),
        }
    }

    fn alloc_column_id(&mut self) -> ColumnId {
        self.last_column_id += 1;
        self.last_column_id
    }

    fn new_column(&mut self, column_def: ColumnDef) -> ColumnDescriptor {
        let datatype = column_def.1.data_type();
        ColumnDescriptorBuilder::new(self.alloc_column_id(), column_def.0, datatype)
            .is_nullable(column_def.2)
            .build()
            .unwrap()
    }
}

/// Create desc with schema (k0, timestamp, v0, ... vn-1)
pub fn desc_with_value_columns(region_name: &str, num_value_columns: usize) -> RegionDescriptor {
    let mut builder =
        RegionDescBuilder::new(region_name).push_key_column(("k0", LogicalTypeId::Int64, false));
    for i in 0..num_value_columns {
        let name = format!("v{}", i);
        builder = builder.push_value_column((&name, LogicalTypeId::Int64, true));
    }
    builder.build()
}
