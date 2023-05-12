// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use datatypes::prelude::ConcreteDataType;
use store_api::storage::{
    ColumnDescriptor, ColumnDescriptorBuilder, ColumnFamilyDescriptorBuilder, ColumnId,
    RegionDescriptor, RowKeyDescriptorBuilder,
};

use super::schema_util::ColumnDef;
use super::TIMESTAMP_NAME;

pub struct RegionDescBuilder {
    name: String,
    last_column_id: ColumnId,
    key_builder: RowKeyDescriptorBuilder,
    default_cf_builder: ColumnFamilyDescriptorBuilder,
}

impl RegionDescBuilder {
    pub fn new<T: Into<String>>(name: T) -> Self {
        let key_builder = RowKeyDescriptorBuilder::new(
            ColumnDescriptorBuilder::new(
                1,
                TIMESTAMP_NAME,
                ConcreteDataType::timestamp_millisecond_datatype(),
            )
            .is_nullable(false)
            .build()
            .unwrap(),
        );

        Self {
            name: name.into(),
            last_column_id: 1,
            key_builder,
            default_cf_builder: ColumnFamilyDescriptorBuilder::default(),
        }
    }

    pub fn push_field_column(mut self, column_def: ColumnDef) -> Self {
        let column = self.new_column(column_def);
        self.default_cf_builder = self.default_cf_builder.push_column(column);
        self
    }

    pub fn build(self) -> RegionDescriptor {
        RegionDescriptor {
            id: 0,
            name: self.name,
            row_key: self.key_builder.build().unwrap(),
            default_cf: self.default_cf_builder.build().unwrap(),
            extra_cfs: Vec::new(),
            compaction_time_window: None,
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
