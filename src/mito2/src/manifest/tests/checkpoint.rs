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

use std::assert_matches::assert_matches;
use std::sync::Arc;
use std::time::Duration;

use common_datasource::compression::CompressionType;
use store_api::storage::RegionId;
use strum::IntoEnumIterator;

use crate::error::Error::ChecksumMismatch;
use crate::manifest::action::{
    RegionCheckpoint, RegionEdit, RegionMetaAction, RegionMetaActionList,
};
use crate::manifest::manager::RegionManifestManager;
use crate::manifest::tests::utils::basic_region_metadata;
use crate::sst::file::{FileId, FileMeta};
use crate::test_util::TestEnv;

async fn build_manager(
    checkpoint_distance: u64,
    compress_type: CompressionType,
) -> (TestEnv, RegionManifestManager) {
    let metadata = Arc::new(basic_region_metadata());
    let env = TestEnv::new();
    let manager = env
        .create_manifest_manager(compress_type, checkpoint_distance, Some(metadata.clone()))
        .await
        .unwrap()
        .unwrap();

    (env, manager)
}

async fn reopen_manager(
    env: &TestEnv,
    checkpoint_distance: u64,
    compress_type: CompressionType,
) -> RegionManifestManager {
    env.create_manifest_manager(compress_type, checkpoint_distance, None)
        .await
        .unwrap()
        .unwrap()
}

fn nop_action() -> RegionMetaActionList {
    RegionMetaActionList::new(vec![RegionMetaAction::Edit(RegionEdit {
        files_to_add: vec![],
        files_to_remove: vec![],
        compaction_time_window: None,
        flushed_entry_id: None,
        flushed_sequence: None,
    })])
}

#[tokio::test]
async fn manager_without_checkpoint() {
    let (_env, mut manager) = build_manager(0, CompressionType::Uncompressed).await;

    // apply 10 actions
    for _ in 0..10 {
        manager.update(nop_action()).await.unwrap();
    }

    // no checkpoint
    assert!(manager
        .store()
        .load_last_checkpoint()
        .await
        .unwrap()
        .is_none());

    // check files
    let mut expected = vec![
        "00000000000000000010.json",
        "00000000000000000009.json",
        "00000000000000000008.json",
        "00000000000000000007.json",
        "00000000000000000006.json",
        "00000000000000000005.json",
        "00000000000000000004.json",
        "00000000000000000003.json",
        "00000000000000000002.json",
        "00000000000000000001.json",
        "00000000000000000000.json",
    ];
    expected.sort_unstable();
    let mut paths = manager
        .store()
        .get_paths(|e| Some(e.name().to_string()))
        .await
        .unwrap();
    paths.sort_unstable();
    assert_eq!(expected, paths);
}

#[tokio::test]
async fn manager_with_checkpoint_distance_1() {
    common_telemetry::init_default_ut_logging();
    let (env, mut manager) = build_manager(1, CompressionType::Uncompressed).await;

    // apply 10 actions
    for _ in 0..10 {
        manager.update(nop_action()).await.unwrap();

        while manager.checkpointer().is_doing_checkpoint() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    // has checkpoint
    assert!(manager
        .store()
        .load_last_checkpoint()
        .await
        .unwrap()
        .is_some());

    // check files
    let mut expected = vec![
        "00000000000000000009.checkpoint",
        "00000000000000000010.checkpoint",
        "00000000000000000010.json",
        "_last_checkpoint",
    ];
    expected.sort_unstable();
    let mut paths = manager
        .store()
        .get_paths(|e| Some(e.name().to_string()))
        .await
        .unwrap();
    paths.sort_unstable();
    assert_eq!(expected, paths);

    // check content in `_last_checkpoint`
    let raw_bytes = manager
        .store()
        .read_file(&manager.store().last_checkpoint_path())
        .await
        .unwrap();
    let raw_json = std::str::from_utf8(&raw_bytes).unwrap();
    let expected_json =
        "{\"size\":848,\"version\":10,\"checksum\":4186457347,\"extend_metadata\":{}}";
    assert_eq!(expected_json, raw_json);

    // reopen the manager
    manager.stop().await;
    let manager = reopen_manager(&env, 1, CompressionType::Uncompressed).await;
    assert_eq!(10, manager.manifest().manifest_version);
}

#[tokio::test]
async fn test_corrupted_data_causing_checksum_error() {
    // Initialize manager
    common_telemetry::init_default_ut_logging();
    let (_env, mut manager) = build_manager(1, CompressionType::Uncompressed).await;

    // Apply actions
    for _ in 0..10 {
        manager.update(nop_action()).await.unwrap();
    }

    // Wait for the checkpoint to finish.
    while manager.checkpointer().is_doing_checkpoint() {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Check if there is a checkpoint
    assert!(manager
        .store()
        .load_last_checkpoint()
        .await
        .unwrap()
        .is_some());

    // Corrupt the last checkpoint data
    let mut corrupted_bytes = manager
        .store()
        .read_file(&manager.store().last_checkpoint_path())
        .await
        .unwrap();
    corrupted_bytes[0] ^= 1;

    // Overwrite the latest checkpoint data
    manager
        .store()
        .write_last_checkpoint(9, &corrupted_bytes)
        .await
        .unwrap();

    // Attempt to load the corrupted checkpoint
    let load_corrupted_result = manager.store().load_last_checkpoint().await;

    // Check if the result is an error and if it's of type VerifyChecksum
    assert_matches!(load_corrupted_result, Err(ChecksumMismatch { .. }));
}

#[tokio::test]
async fn checkpoint_with_different_compression_types() {
    common_telemetry::init_default_ut_logging();

    let mut actions = vec![];
    for _ in 0..10 {
        let file_meta = FileMeta {
            region_id: RegionId::new(123, 456),
            file_id: FileId::random(),
            time_range: (0.into(), 10000000.into()),
            level: 0,
            file_size: 1024000,
            available_indexes: Default::default(),
            index_file_size: 0,
            num_rows: 0,
            num_row_groups: 0,
        };
        let action = RegionMetaActionList::new(vec![RegionMetaAction::Edit(RegionEdit {
            files_to_add: vec![file_meta],
            files_to_remove: vec![],
            compaction_time_window: None,
            flushed_entry_id: None,
            flushed_sequence: None,
        })]);
        actions.push(action);
    }

    // collect and check all compression types
    let mut checkpoints = vec![];
    for compress_type in CompressionType::iter() {
        checkpoints
            .push(generate_checkpoint_with_compression_types(compress_type, actions.clone()).await);
    }
    let last = checkpoints.last().unwrap().clone();
    assert!(checkpoints.into_iter().all(|ckpt| last.eq(&ckpt)));
}

async fn generate_checkpoint_with_compression_types(
    compress_type: CompressionType,
    actions: Vec<RegionMetaActionList>,
) -> RegionCheckpoint {
    let (_env, mut manager) = build_manager(1, compress_type).await;

    for action in actions {
        manager.update(action).await.unwrap();

        while manager.checkpointer().is_doing_checkpoint() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    RegionManifestManager::last_checkpoint(&mut manager.store())
        .await
        .unwrap()
        .unwrap()
}
