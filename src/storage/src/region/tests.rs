//! Region tests.

mod read_write;

use datatypes::type_id::LogicalTypeId;
use log_store::fs::noop::NoopLogStore;
use store_api::storage::consts;

use super::*;
use crate::test_util::{self, descriptor_util::RegionDescBuilder, schema_util};

#[test]
fn test_new_region() {
    let region_name = "region-0";
    let desc = RegionDescBuilder::new(region_name)
        .push_key_column(("k1", LogicalTypeId::Int32, false))
        .push_value_column(("v1", LogicalTypeId::Float32, true))
        .build();
    let metadata = desc.try_into().unwrap();

    let wal = Wal::new(region_name, Arc::new(NoopLogStore::default()));
    let region = RegionImpl::new(region_name.to_string(), metadata, wal);

    let expect_schema = schema_util::new_schema_ref(&[
        ("k1", LogicalTypeId::Int32, false),
        (test_util::TIMESTAMP_NAME, LogicalTypeId::Int64, false),
        (consts::VERSION_COLUMN_NAME, LogicalTypeId::UInt64, false),
        ("v1", LogicalTypeId::Float32, true),
    ]);

    assert_eq!(region_name, region.name());
    assert_eq!(expect_schema, *region.in_memory_metadata().schema());
}
