//! Region tests.

mod read_write;

use datatypes::type_id::LogicalTypeId;
use log_store::fs::noop::NoopLogStore;
use object_store::{backend::fs::Backend, ObjectStore};
use store_api::manifest::Manifest;
use store_api::storage::consts;
use tempdir::TempDir;

use super::*;
use crate::manifest::region::RegionManifest;
use crate::sst::FsAccessLayer;
use crate::test_util::{self, descriptor_util::RegionDescBuilder, schema_util};

#[tokio::test]
async fn test_new_region() {
    let region_id = 0;
    let region_name = "region-0";
    let desc = RegionDescBuilder::new(region_name)
        .enable_version_column(true)
        .push_key_column(("k1", LogicalTypeId::Int32, false))
        .push_value_column(("v1", LogicalTypeId::Float32, true))
        .build();
    let metadata = desc.try_into().unwrap();

    let wal = Wal::new(region_id, region_name, Arc::new(NoopLogStore::default()));
    let store_dir = TempDir::new("test_new_region")
        .unwrap()
        .path()
        .to_string_lossy()
        .to_string();

    let accessor = Backend::build().root(&store_dir).finish().await.unwrap();
    let object_store = ObjectStore::new(accessor);
    let sst_layer = Arc::new(FsAccessLayer::new("/", object_store.clone()));
    let manifest = RegionManifest::new(region_id, "/manifest/", object_store);

    let region = RegionImpl::new(
        region_id,
        region_name.to_string(),
        metadata,
        wal,
        sst_layer,
        manifest,
    );

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
