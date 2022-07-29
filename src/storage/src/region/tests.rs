//! Region tests.

mod flush;
mod read_write;

use datatypes::type_id::LogicalTypeId;
use store_api::storage::consts;
use tempdir::TempDir;

use super::*;
use crate::test_util::{self, config_util, descriptor_util::RegionDescBuilder, schema_util};

#[tokio::test]
async fn test_new_region() {
    let region_name = "region-0";
    let desc = RegionDescBuilder::new(region_name)
        .enable_version_column(true)
        .push_key_column(("k1", LogicalTypeId::Int32, false))
        .push_value_column(("v1", LogicalTypeId::Float32, true))
        .build();
    let metadata = desc.try_into().unwrap();

    let store_dir = TempDir::new("test_new_region")
        .unwrap()
        .path()
        .to_string_lossy()
        .to_string();

    let store_config = config_util::new_store_config(&store_dir, region_name).await;

    let region = RegionImpl::new(0, region_name.to_string(), metadata, store_config);

    let expect_schema = schema_util::new_schema_ref(
        &[
            ("k1", LogicalTypeId::Int32, false),
            (test_util::TIMESTAMP_NAME, LogicalTypeId::Int64, false),
            (consts::VERSION_COLUMN_NAME, LogicalTypeId::UInt64, false),
            ("v1", LogicalTypeId::Float32, true),
        ],
        Some(1),
    );

    assert_eq!(region_name, region.name());
    assert_eq!(expect_schema, *region.in_memory_metadata().schema());
}
