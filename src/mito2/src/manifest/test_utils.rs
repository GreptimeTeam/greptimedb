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
use datatypes::type_id::LogicalTypeId;
use storage::sst::{FileId, FileMeta};
use store_api::storage::{
    ColumnDescriptor, ColumnDescriptorBuilder, ColumnFamilyDescriptorBuilder, ColumnId,
    RegionDescriptor, RegionId, RowKeyDescriptorBuilder, SequenceNumber,
};

use crate::manifest::action::*;
use crate::region::metadata::RegionMetadata;

pub const DEFAULT_TEST_FILE_SIZE: u64 = 1024;

/// Column definition: (name, datatype, is_nullable)
pub type ColumnDef<'a> = (&'a str, LogicalTypeId, bool);

pub const TIMESTAMP_NAME: &str = "timestamp";

pub fn build_region_meta() -> RegionMetadata {
    let region_name = "region-0";
    let desc = RegionDescBuilder::new(region_name)
        .id(0)
        .push_key_column(("k1", LogicalTypeId::Int32, false))
        .push_field_column(("v1", LogicalTypeId::Float32, true))
        .build();
    desc.try_into().unwrap()
}

pub fn build_altered_region_meta() -> RegionMetadata {
    let region_name = "region-0";
    let desc = RegionDescBuilder::new(region_name)
        .id(0)
        .push_key_column(("k1", LogicalTypeId::Int32, false))
        .push_field_column(("v1", LogicalTypeId::Float32, true))
        .push_field_column(("v2", LogicalTypeId::Float32, true))
        .build();
    desc.try_into().unwrap()
}

pub fn build_region_edit(
    sequence: SequenceNumber,
    files_to_add: &[FileId],
    files_to_remove: &[FileId],
) -> RegionEdit {
    RegionEdit {
        region_version: 0,
        flushed_sequence: Some(sequence),
        files_to_add: files_to_add
            .iter()
            .map(|f| FileMeta {
                region_id: 0.into(),
                file_id: *f,
                time_range: None,
                level: 0,
                file_size: DEFAULT_TEST_FILE_SIZE,
            })
            .collect(),
        files_to_remove: files_to_remove
            .iter()
            .map(|f| FileMeta {
                region_id: 0.into(),
                file_id: *f,
                time_range: None,
                level: 0,
                file_size: DEFAULT_TEST_FILE_SIZE,
            })
            .collect(),
        compaction_time_window: None,
    }
}

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
                1,
                TIMESTAMP_NAME,
                ConcreteDataType::timestamp_millisecond_datatype(),
            )
            .is_nullable(false)
            .is_time_index(true)
            .build()
            .unwrap(),
        );

        Self {
            id: 0.into(),
            name: name.into(),
            last_column_id: 1,
            key_builder,
            default_cf_builder: ColumnFamilyDescriptorBuilder::default(),
        }
    }

    pub fn id(mut self, id: impl Into<RegionId>) -> Self {
        self.id = id.into();
        self
    }

    pub fn timestamp(mut self, column_def: ColumnDef) -> Self {
        let column = self.new_ts_column(column_def);
        self.key_builder = self.key_builder.timestamp(column);
        self
    }

    pub fn push_key_column(mut self, column_def: ColumnDef) -> Self {
        let column = self.new_column(column_def);
        self.key_builder = self.key_builder.push_column(column);
        self
    }

    pub fn push_field_column(mut self, column_def: ColumnDef) -> Self {
        let column = self.new_column(column_def);
        self.default_cf_builder = self.default_cf_builder.push_column(column);
        self
    }

    pub fn set_last_column_id(mut self, column_id: ColumnId) -> Self {
        self.last_column_id = column_id;
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

    pub fn last_column_id(&self) -> ColumnId {
        self.last_column_id
    }

    fn alloc_column_id(&mut self) -> ColumnId {
        self.last_column_id += 1;
        self.last_column_id
    }

    fn new_ts_column(&mut self, column_def: ColumnDef) -> ColumnDescriptor {
        let datatype = column_def.1.data_type();
        ColumnDescriptorBuilder::new(self.alloc_column_id(), column_def.0, datatype)
            .is_nullable(column_def.2)
            .is_time_index(true)
            .build()
            .unwrap()
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
pub fn desc_with_field_columns(region_name: &str, num_field_columns: usize) -> RegionDescriptor {
    let mut builder =
        RegionDescBuilder::new(region_name).push_key_column(("k0", LogicalTypeId::Int64, false));
    for i in 0..num_field_columns {
        let name = format!("v{i}");
        builder = builder.push_field_column((&name, LogicalTypeId::Int64, true));
    }
    builder.build()
}
