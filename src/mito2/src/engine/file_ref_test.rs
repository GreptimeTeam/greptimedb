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

use std::collections::{HashMap, HashSet};

use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use store_api::storage::RegionId;

use crate::config::MitoConfig;
use crate::test_util::TestEnv;

#[tokio::test]
async fn get_snapshot_of_file_refs_errors_on_missing_related_region() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let source_region = RegionId::new(1, 1);
    let missing_target_region = RegionId::new(1, 2);
    let related_regions = HashMap::from([(source_region, HashSet::from([missing_target_region]))]);

    let err = engine
        .get_snapshot_of_file_refs([], related_regions)
        .await
        .expect_err("missing related target region should fail file ref snapshot");

    assert_eq!(StatusCode::RegionNotFound, err.status_code());
}

#[tokio::test]
async fn get_snapshot_of_file_refs_skips_missing_file_handle_region() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let missing_region = RegionId::new(1, 1);
    let manifest = engine
        .get_snapshot_of_file_refs([missing_region], HashMap::new())
        .await
        .expect("missing file handle region should be skipped");

    assert!(manifest.file_refs.is_empty());
    assert!(manifest.manifest_version.is_empty());
    assert!(manifest.cross_region_refs.is_empty());
}

#[tokio::test]
async fn get_snapshot_preserves_related_region_old_index_readers() {
    use store_api::region_engine::{RegionEngine, RegionRole};
    use store_api::region_request::RegionRequest;
    use store_api::storage::{FileId, FileRef};

    use crate::manifest::action::{RegionEdit, RegionMetaAction, RegionMetaActionList};
    use crate::region::RegionLeaderState;
    use crate::sst::file::{FileHandle, FileMeta, IndexType};
    use crate::test_util::CreateRequestBuilder;

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            gc: crate::gc::GcConfig {
                enable: true,
                ..Default::default()
            },
            ..Default::default()
        })
        .await;
    let source_region = RegionId::new(1, 1);
    let target_region = RegionId::new(1, 2);
    engine
        .handle_request(
            target_region,
            RegionRequest::Create(CreateRequestBuilder::new().build()),
        )
        .await
        .unwrap();
    engine
        .set_region_role(target_region, RegionRole::Leader)
        .unwrap();
    let meta = FileMeta {
        region_id: source_region,
        file_id: FileId::random(),
        index_version: 2,
        available_indexes: smallvec::smallvec![IndexType::InvertedIndex],
        ..Default::default()
    };
    let target = engine.get_region(target_region).unwrap();
    let old_reader = FileHandle::new(
        FileMeta {
            index_version: 0,
            ..meta.clone()
        },
        target.file_purger.clone(),
    );
    target
        .manifest_ctx
        .update_manifest(
            RegionLeaderState::Writable,
            RegionMetaActionList::with_action(RegionMetaAction::Edit(RegionEdit {
                files_to_add: vec![meta.clone()],
                files_to_remove: vec![],
                timestamp_ms: None,
                compaction_time_window: None,
                flushed_entry_id: None,
                flushed_sequence: None,
                committed_sequence: None,
            })),
            false,
        )
        .await
        .unwrap();

    // The source is not open here, but a query on the destination still uses its v0 index.
    let related_regions = HashMap::from([(source_region, HashSet::from([target_region]))]);
    let snapshot = engine
        .get_snapshot_of_file_refs([], related_regions.clone())
        .await
        .unwrap();
    assert_eq!(
        snapshot.file_refs[&source_region],
        [0, 2]
            .into_iter()
            .map(|version| FileRef::new(source_region, meta.file_id, Some(version)))
            .collect()
    );
    assert_eq!(
        snapshot.cross_region_refs[&source_region],
        HashSet::from([target_region])
    );
    drop(old_reader);
    let snapshot = engine
        .get_snapshot_of_file_refs([], related_regions)
        .await
        .unwrap();
    assert_eq!(
        snapshot.file_refs[&source_region],
        HashSet::from([FileRef::new(source_region, meta.file_id, Some(2))])
    );
}

#[rstest::rstest]
#[tokio::test]
async fn get_snapshot_preserves_old_index_readers_until_release(
    #[values(false, true)] old_has_index: bool,
) {
    use store_api::region_engine::{RegionEngine, RegionRole};
    use store_api::region_request::RegionRequest;
    use store_api::storage::{FileId, FileRef};

    use crate::manifest::action::{RegionEdit, RegionMetaAction, RegionMetaActionList};
    use crate::region::RegionLeaderState;
    use crate::sst::file::{FileMeta, IndexType};
    use crate::sst::version::SstVersion;
    use crate::test_util::CreateRequestBuilder;

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            gc: crate::gc::GcConfig {
                enable: true,
                ..Default::default()
            },
            ..Default::default()
        })
        .await;
    let region_id = RegionId::new(1, 1);
    engine
        .handle_request(
            region_id,
            RegionRequest::Create(CreateRequestBuilder::new().build()),
        )
        .await
        .unwrap();
    engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap();
    let region = engine.get_region(region_id).unwrap();
    let meta = FileMeta {
        region_id,
        file_id: FileId::random(),
        available_indexes: if old_has_index {
            smallvec::smallvec![IndexType::InvertedIndex]
        } else {
            Default::default()
        },
        ..Default::default()
    };
    let mut version = SstVersion::new(crate::test_util::memtable_util::metadata_for_test());
    version.add_files(region.file_purger.clone(), [meta.clone()].into_iter());
    let old_reader = version.levels()[0].files[&meta.file_id].clone();
    let current_meta = FileMeta {
        index_version: 1,
        available_indexes: smallvec::smallvec![IndexType::InvertedIndex],
        ..meta.clone()
    };
    version.add_files(
        region.file_purger.clone(),
        [current_meta.clone()].into_iter(),
    );
    region
        .manifest_ctx
        .update_manifest(
            RegionLeaderState::Writable,
            RegionMetaActionList::with_action(RegionMetaAction::Edit(RegionEdit {
                files_to_add: vec![current_meta],
                files_to_remove: vec![],
                timestamp_ms: None,
                compaction_time_window: None,
                flushed_entry_id: None,
                flushed_sequence: None,
                committed_sequence: None,
            })),
            false,
        )
        .await
        .unwrap();
    let snapshot = engine
        .get_snapshot_of_file_refs([region_id], HashMap::new())
        .await
        .unwrap();
    // Only v1 is covered by the manifest; keep the older view's exact reference.
    let expected = HashSet::from([FileRef::new(
        region_id,
        meta.file_id,
        old_has_index.then_some(0),
    )]);
    assert_eq!(snapshot.file_refs[&region_id], expected);
    // Closing the current view without retirement must keep the old reader protected.
    drop(version);
    let snapshot = engine
        .get_snapshot_of_file_refs([region_id], HashMap::new())
        .await
        .unwrap();
    assert_eq!(snapshot.file_refs[&region_id], expected);
    drop(old_reader);
    let snapshot = engine
        .get_snapshot_of_file_refs([region_id], HashMap::new())
        .await
        .unwrap();
    assert!(
        snapshot
            .file_refs
            .get(&region_id)
            .is_none_or(HashSet::is_empty)
    );
}
