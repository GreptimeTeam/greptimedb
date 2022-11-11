pub mod bench_context;
pub mod regiondesc_util;
pub mod schema_util;

use datatypes::type_id::LogicalTypeId;
use storage::{
    memtable::{DefaultMemtableBuilder, MemtableBuilder, MemtableRef},
    metadata::RegionMetadata,
    schema::RegionSchemaRef,
};

use crate::memtable::util::regiondesc_util::RegionDescBuilder;

pub const TIMESTAMP_NAME: &str = "timestamp";

pub fn schema_for_test() -> RegionSchemaRef {
    let desc = RegionDescBuilder::new("bench")
        .enable_version_column(true)
        .push_value_column(("v1", LogicalTypeId::UInt64, true))
        .push_value_column(("v2", LogicalTypeId::String, true))
        .build();
    let metadata: RegionMetadata = desc.try_into().unwrap();

    metadata.schema().clone()
}

pub fn new_memtable() -> MemtableRef {
    DefaultMemtableBuilder::default().build(schema_for_test())
}
