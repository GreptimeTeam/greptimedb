pub mod bench_context;
pub mod regiondesc_util;
pub mod schema_util;

use datatypes::type_id::LogicalTypeId;
use storage::{
    memtable::{DefaultMemtableBuilder, MemtableBuilder, MemtableRef, MemtableSchema},
    metadata::RegionMetadata,
};

use crate::memtable::util::regiondesc_util::RegionDescBuilder;

pub const TIMESTAMP_NAME: &str = "timestamp";

pub fn schema_for_test() -> MemtableSchema {
    let desc = RegionDescBuilder::new("bench")
        .push_value_column(("v1", LogicalTypeId::UInt64, true))
        .push_value_column(("v2", LogicalTypeId::String, true))
        .build();
    let metadata: RegionMetadata = desc.try_into().unwrap();
    MemtableSchema::new(metadata.columns_row_key)
}

pub fn new_memtable() -> MemtableRef {
    DefaultMemtableBuilder {}.build(1, schema_for_test())
}
