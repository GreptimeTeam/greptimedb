//! Region tests.

mod flush;
mod read_write;

use datatypes::type_id::LogicalTypeId;
use log_store::fs::noop::NoopLogStore;
use object_store::{backend::fs, ObjectStore};
use store_api::storage::consts;
use tempdir::TempDir;

use super::*;
use crate::manifest::action::RegionChange;
use crate::manifest::action::RegionMetaActionList;
use crate::manifest::test_utils::*;
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

#[tokio::test]
async fn test_recover_region_manifets() {
    let tmp_dir = TempDir::new("test_new_region").unwrap();

    let object_store = ObjectStore::new(
        fs::Backend::build()
            .root(&tmp_dir.path().to_string_lossy())
            .finish()
            .await
            .unwrap(),
    );

    let manifest = RegionManifest::new("/manifest/", object_store);
    let region_name = "region-0";
    let region_meta = Arc::new(build_region_meta());

    // Recover from empty
    assert!(
        RegionImpl::<NoopLogStore>::recover_from_manifest(region_name, &manifest)
            .await
            .is_err()
    );

    {
        // save some actions into region_meta
        manifest
            .update(RegionMetaActionList::with_action(RegionMetaAction::Change(
                RegionChange {
                    metadata: region_meta.clone(),
                },
            )))
            .await
            .unwrap();

        manifest
            .update(RegionMetaActionList::new(vec![
                RegionMetaAction::Edit(build_region_edit(1, &["f1"], &[])),
                RegionMetaAction::Edit(build_region_edit(2, &["f2", "f3"], &[])),
            ]))
            .await
            .unwrap();
    }

    // try to recover
    let version = RegionImpl::<NoopLogStore>::recover_from_manifest(region_name, &manifest)
        .await
        .unwrap();
    assert_eq!(*version.metadata(), region_meta);
    assert_eq!(version.flushed_sequence(), 2);
    assert_eq!(version.manifest_version(), 1);
    let ssts = version.ssts();
    let files = ssts.levels()[0].files();
    assert_eq!(3, files.len());
    for (i, file) in files.iter().enumerate() {
        assert_eq!(format!("f{}", i + 1), file.file_name());
    }
    assert!(version.mutable_memtables().is_empty());

    // check manifest state
    assert_eq!(2, manifest.last_version());
}
