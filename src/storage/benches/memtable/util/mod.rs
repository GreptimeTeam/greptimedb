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

pub mod bench_context;
pub mod regiondesc_util;
pub mod schema_util;

use datatypes::type_id::LogicalTypeId;
use storage::memtable::{DefaultMemtableBuilder, MemtableBuilder, MemtableRef};
use storage::metadata::RegionMetadata;
use storage::schema::RegionSchemaRef;

use crate::memtable::util::regiondesc_util::RegionDescBuilder;

pub const TIMESTAMP_NAME: &str = "timestamp";

pub fn schema_for_test() -> RegionSchemaRef {
    let desc = RegionDescBuilder::new("bench")
        .enable_version_column(true)
        .push_field_column(("v1", LogicalTypeId::UInt64, true))
        .push_field_column(("v2", LogicalTypeId::String, true))
        .build();
    let metadata: RegionMetadata = desc.try_into().unwrap();

    metadata.schema().clone()
}

pub fn new_memtable() -> MemtableRef {
    DefaultMemtableBuilder::default().build(schema_for_test())
}
