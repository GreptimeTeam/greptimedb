use datatypes::prelude::ConcreteDataType;
use store_api::storage::{
    ColumnDescriptor, ColumnDescriptorBuilder, ColumnFamilyDescriptorBuilder, ColumnId,
    RegionDescriptor, RowKeyDescriptorBuilder,
};

use super::{schema_util::ColumnDef, TIMESTAMP_NAME};
pub struct RegionDescBuilder {
    name: String,
    last_column_id: ColumnId,
    key_builder: RowKeyDescriptorBuilder,
    default_cf_builder: ColumnFamilyDescriptorBuilder,
}

impl RegionDescBuilder {
    pub fn new<T: Into<String>>(name: T) -> Self {
        let key_builder = RowKeyDescriptorBuilder::new(
            ColumnDescriptorBuilder::new(2, TIMESTAMP_NAME, ConcreteDataType::int64_datatype())
                .is_nullable(false)
                .build()
                .unwrap(),
        );

        Self {
            name: name.into(),
            last_column_id: 2,
            key_builder,
            default_cf_builder: ColumnFamilyDescriptorBuilder::default(),
        }
    }

    pub fn push_value_column(mut self, column_def: ColumnDef) -> Self {
        let column = self.new_column(column_def);
        self.default_cf_builder = self.default_cf_builder.push_column(column);
        self
    }

    pub fn build(self) -> RegionDescriptor {
        RegionDescriptor {
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
