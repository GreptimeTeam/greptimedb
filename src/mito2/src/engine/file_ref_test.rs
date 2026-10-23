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
async fn get_snapshot_preserves_old_index_readers_until_release() {
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
        available_indexes: smallvec::smallvec![IndexType::InvertedIndex],
        ..Default::default()
    };
    let mut version = SstVersion::new(crate::test_util::memtable_util::metadata_for_test());
    version.add_files(region.file_purger.clone(), [meta.clone()].into_iter());
    let old_reader = version.levels()[0].files[&meta.file_id].clone();
    let current_meta = FileMeta {
        index_version: 1,
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
    assert_eq!(
        snapshot.file_refs[&region_id],
        HashSet::from([FileRef::new(region_id, meta.file_id, Some(0))])
    );
    drop(old_reader);
    let snapshot = engine
        .get_snapshot_of_file_refs([region_id], HashMap::new())
        .await
        .unwrap();
    assert!(snapshot.file_refs[&region_id].is_empty());
    drop(version);
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
