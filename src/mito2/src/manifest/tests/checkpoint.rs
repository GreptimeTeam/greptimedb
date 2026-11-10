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

use std::assert_matches;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common_datasource::compression::CompressionType;
use common_error::ext::{ErrorExt, RetryHint};
use common_error::status_code::StatusCode;
use object_store::layers::mock::{
    Error as MockError, ErrorKind, MockLayerBuilder, OpDelete, Result as MockResult, oio,
};
use store_api::storage::{FileId, RegionId};
use strum::IntoEnumIterator;

use crate::error::Error::{ChecksumMismatch, ManifestDeltaNotFound};
use crate::manifest::action::{
    RegionCheckpoint, RegionEdit, RegionMetaAction, RegionMetaActionList,
};
use crate::manifest::manager::RegionManifestManager;
use crate::manifest::storage::checkpoint::CheckpointMetadata;
use crate::manifest::storage::{
    checkpoint_file, delta_file, file_version, gen_path, is_delta_file,
};
use crate::manifest::tests::utils::{
    basic_region_metadata, fail_manifest_reads_layer, manifest_dir_usage, manifest_recovery_usage,
};
use crate::sst::file::FileMeta;
use crate::test_util::{CheckpointTaskBlocker, TestEnv, nop_action};

async fn build_manager(
    checkpoint_distance: u64,
    compress_type: CompressionType,
) -> (TestEnv, RegionManifestManager) {
    let metadata = Arc::new(basic_region_metadata());
    let env = TestEnv::new().await;
    let manager = env
        .create_manifest_manager(compress_type, checkpoint_distance, Some(metadata.clone()))
        .await
        .unwrap()
        .unwrap();

    (env, manager)
}

async fn build_manager_with_initial_metadata(
    env: &TestEnv,
    checkpoint_distance: u64,
    compress_type: CompressionType,
) -> RegionManifestManager {
    let metadata = Arc::new(basic_region_metadata());
    env.create_manifest_manager(compress_type, checkpoint_distance, Some(metadata.clone()))
        .await
        .unwrap()
        .unwrap()
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

#[tokio::test]
async fn manager_without_checkpoint() {
    let (_env, mut manager) = build_manager(0, CompressionType::Uncompressed).await;

    // apply 10 actions
    for _ in 0..10 {
        manager.update(nop_action(), false).await.unwrap();
    }

    // no checkpoint
    assert!(
        manager
            .store()
            .load_last_checkpoint()
            .await
            .unwrap()
            .is_none()
    );

    // check files
    let mut expected = vec![
        "/",
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
        .delta_storage()
        .get_paths(None, |e| Some(e.name().to_string()))
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
        manager.update(nop_action(), false).await.unwrap();

        while manager.checkpointer().is_doing_checkpoint() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    // has checkpoint
    assert!(
        manager
            .store()
            .load_last_checkpoint()
            .await
            .unwrap()
            .is_some()
    );

    // check files
    let mut expected = vec![
        "/",
        "00000000000000000009.checkpoint",
        "00000000000000000010.checkpoint",
        "00000000000000000010.json",
        "_last_checkpoint",
    ];
    expected.sort_unstable();
    let mut paths = manager
        .store()
        .delta_storage()
        .get_paths(None, |e| Some(e.name().to_string()))
        .await
        .unwrap();
    paths.sort_unstable();
    assert_eq!(expected, paths);

    // check content in `_last_checkpoint`
    let raw_bytes = manager
        .store()
        .read_file(&manager.store().checkpoint_storage().last_checkpoint_path())
        .await
        .unwrap();
    let raw_json = std::str::from_utf8(&raw_bytes).unwrap();
    let checkpoint_metadata: CheckpointMetadata =
        serde_json::from_str(raw_json).expect("Failed to parse checkpoint metadata");

    // size and checksum vary due to different machine time, so we only check version
    assert_eq!(checkpoint_metadata.version, 10);

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
        manager.update(nop_action(), false).await.unwrap();
    }

    // Wait for the checkpoint to finish.
    while manager.checkpointer().is_doing_checkpoint() {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Check if there is a checkpoint
    assert!(
        manager
            .store()
            .load_last_checkpoint()
            .await
            .unwrap()
            .is_some()
    );

    // Corrupt the last checkpoint data
    let mut corrupted_bytes = manager
        .store()
        .read_file(&manager.store().checkpoint_storage().last_checkpoint_path())
        .await
        .unwrap();
    corrupted_bytes[0] ^= 1;

    // Overwrite the latest checkpoint data
    manager
        .store()
        .checkpoint_storage()
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
            max_row_group_uncompressed_size: 1024000,
            available_indexes: Default::default(),
            indexes: Default::default(),
            index_file_size: 0,
            index_version: 0,
            num_rows: 0,
            num_row_groups: 0,
            sequence: None,
            partition_expr: None,
            num_series: 0,
            ..Default::default()
        };
        let action = RegionMetaActionList::new(vec![RegionMetaAction::Edit(RegionEdit {
            files_to_add: vec![file_meta],
            files_to_remove: vec![],
            timestamp_ms: None,
            compaction_time_window: None,
            flushed_entry_id: None,
            flushed_sequence: None,
            committed_sequence: None,
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
        manager.update(action, false).await.unwrap();

        while manager.checkpointer().is_doing_checkpoint() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    RegionManifestManager::last_checkpoint(&mut manager.store())
        .await
        .unwrap()
        .unwrap()
}

fn generate_action_lists(num: usize) -> (Vec<FileId>, Vec<RegionMetaActionList>) {
    let mut files = vec![];
    let mut actions = vec![];
    for _ in 0..num {
        let file_id = FileId::random();
        files.push(file_id);
        let file_meta = FileMeta {
            region_id: RegionId::new(123, 456),
            file_id,
            time_range: (0.into(), 10000000.into()),
            level: 0,
            file_size: 1024000,
            max_row_group_uncompressed_size: 1024000,
            available_indexes: Default::default(),
            indexes: Default::default(),
            index_file_size: 0,
            index_version: 0,
            num_rows: 0,
            num_row_groups: 0,
            sequence: None,
            partition_expr: None,
            num_series: 0,
            ..Default::default()
        };
        let action = RegionMetaActionList::new(vec![RegionMetaAction::Edit(RegionEdit {
            files_to_add: vec![file_meta],
            files_to_remove: vec![],
            timestamp_ms: None,
            compaction_time_window: None,
            flushed_entry_id: None,
            flushed_sequence: None,
            committed_sequence: None,
        })]);
        actions.push(action);
    }
    (files, actions)
}

#[tokio::test]
async fn manifest_install_manifest_to() {
    common_telemetry::init_default_ut_logging();
    let (env, mut manager) = build_manager(0, CompressionType::Uncompressed).await;
    let (files, actions) = generate_action_lists(10);
    for action in actions {
        manager.update(action, false).await.unwrap();
    }

    // Nothing to install
    let target_version = manager.manifest().manifest_version;
    let installed_version = manager.install_manifest_to(target_version).await.unwrap();
    assert_eq!(target_version, installed_version);

    let mut another_manager =
        build_manager_with_initial_metadata(&env, 0, CompressionType::Uncompressed).await;

    // install manifest changes
    let target_version = manager.manifest().manifest_version;
    let installed_version = another_manager
        .install_manifest_to(target_version - 1)
        .await
        .unwrap();
    assert_eq!(target_version - 1, installed_version);
    for file_id in files[0..9].iter() {
        assert!(another_manager.manifest().files.contains_key(file_id));
    }

    let installed_version = another_manager
        .install_manifest_to(target_version)
        .await
        .unwrap();
    assert_eq!(target_version, installed_version);
    for file_id in files.iter() {
        assert!(another_manager.manifest().files.contains_key(file_id));
    }
}

#[tokio::test]
async fn manifest_install_manifest_to_with_checkpoint() {
    common_telemetry::init_default_ut_logging();
    let (env, mut manager) = build_manager(3, CompressionType::Uncompressed).await;
    let (files, actions) = generate_action_lists(10);
    for action in actions {
        manager.update(action, false).await.unwrap();

        while manager.checkpointer().is_doing_checkpoint() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    // has checkpoint
    assert!(
        manager
            .store()
            .load_last_checkpoint()
            .await
            .unwrap()
            .is_some()
    );

    // check files
    let mut expected = vec![
        "/",
        "00000000000000000006.checkpoint",
        "00000000000000000007.json",
        "00000000000000000008.json",
        "00000000000000000009.checkpoint",
        "00000000000000000009.json",
        "00000000000000000010.json",
        "_last_checkpoint",
    ];
    expected.sort_unstable();
    let mut paths = manager
        .store()
        .delta_storage()
        .get_paths(None, |e| Some(e.name().to_string()))
        .await
        .unwrap();

    paths.sort_unstable();
    assert_eq!(expected, paths);

    let mut another_manager =
        build_manager_with_initial_metadata(&env, 0, CompressionType::Uncompressed).await;

    // Install 9 manifests
    let target_version = manager.manifest().manifest_version;
    let installed_version = another_manager
        .install_manifest_to(target_version - 1)
        .await
        .unwrap();
    assert_eq!(target_version - 1, installed_version);
    for file_id in files[0..9].iter() {
        assert!(another_manager.manifest().files.contains_key(file_id));
    }

    // Install all manifests
    let target_version = manager.manifest().manifest_version;
    let installed_version = another_manager
        .install_manifest_to(target_version)
        .await
        .unwrap();
    assert_eq!(target_version, installed_version);
    for file_id in files.iter() {
        assert!(another_manager.manifest().files.contains_key(file_id));
    }
    assert_eq!(
        another_manager.manifest_usage(),
        manifest_recovery_usage(env.data_home().join("manifest")).await
    );
}

#[tokio::test]
async fn test_checkpoint_bypass_in_staging_mode() {
    common_telemetry::init_default_ut_logging();
    let (_env, mut manager) = build_manager(1, CompressionType::Uncompressed).await;

    // Apply actions in staging mode - checkpoint should be bypassed
    for _ in 0..15 {
        manager.update(nop_action(), true).await.unwrap();
    }
    assert!(!manager.checkpointer().is_doing_checkpoint());

    // Verify no checkpoint was created in staging mode
    assert!(
        manager
            .store()
            .load_last_checkpoint()
            .await
            .unwrap()
            .is_none()
    );

    // Now switch to normal mode and apply one more action
    manager.update(nop_action(), false).await.unwrap();

    // Wait for potential checkpoint
    while manager.checkpointer().is_doing_checkpoint() {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    // Now checkpoint should exist because we switched to writable mode
    let checkpoint_result = manager.store().load_last_checkpoint().await.unwrap();

    assert!(
        checkpoint_result.is_some(),
        "Checkpoint should exist after switching to writable mode"
    );
    let (last_version, _checkpoint_data) = checkpoint_result.unwrap();

    // Checkpoint should include all 16 actions (15 from staging + 1 from writable)
    assert_eq!(last_version, 16);
}

/// Simulates both a rejected delete and a close failure after eager deletion.
struct FailingDeleter {
    inner: oio::Deleter,
    failure_calls: Arc<AtomicUsize>,
    fail_delete: bool,
}

impl oio::Delete for FailingDeleter {
    async fn delete(&mut self, path: &str, args: OpDelete) -> MockResult<()> {
        if self.fail_delete {
            self.failure_calls.fetch_add(1, Ordering::Relaxed);
            return Err(MockError::new(
                ErrorKind::Unexpected,
                "mock rejected delete",
            ));
        }
        self.inner.delete(path, args).await
    }

    async fn close(&mut self) -> MockResult<()> {
        self.failure_calls.fetch_add(1, Ordering::Relaxed);
        Err(MockError::new(
            ErrorKind::Unexpected,
            "mock manifest delete close failure",
        ))
    }
}

#[rstest::rstest]
#[case::close_failure(false)]
#[case::delete_failure(true)]
#[tokio::test]
async fn manifest_size_ignores_leftover_files_after_failed_delete(#[case] fail_delete: bool) {
    common_telemetry::init_default_ut_logging();

    let failure_calls = Arc::new(AtomicUsize::new(0));
    let factory_failure_calls = failure_calls.clone();
    let mock_layer = MockLayerBuilder::default()
        .deleter_factory(Arc::new(move |inner| {
            Box::new(FailingDeleter {
                inner,
                failure_calls: factory_failure_calls.clone(),
                fail_delete,
            })
        }))
        .build()
        .unwrap();

    let env = TestEnv::new().await.with_mock_layer(mock_layer);
    let metadata = Arc::new(basic_region_metadata());
    let mut manager = env
        .create_manifest_manager(CompressionType::Uncompressed, 1, Some(metadata))
        .await
        .unwrap()
        .unwrap();

    for _ in 0..10 {
        manager.update(nop_action(), false).await.unwrap();
        while manager.checkpointer().is_doing_checkpoint() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    // The checkpointer must have attempted to delete stale files at least once.
    assert!(failure_calls.load(Ordering::Relaxed) > 0);

    // Despite delete failures, the in-memory checkpoint marker advances so
    // subsequent `maybe_do_checkpoint` calls compute correct ranges.
    assert_eq!(manager.checkpointer().last_checkpoint_version(), 10);

    // And the durable `_last_checkpoint` metadata reflects the latest version.
    let (last_version, _) = manager
        .store()
        .load_last_checkpoint()
        .await
        .unwrap()
        .expect("checkpoint should be durable");
    assert_eq!(last_version, 10);

    // A rejected delete preserves stale files; a close failure after eager FS
    // deletion does not. Exclude the current delta from the stale-file count.
    let file_names: Vec<String> = manager
        .store()
        .delta_storage()
        .get_paths(None, |e| Some(e.name().to_string()))
        .await
        .unwrap();
    let stale_delta_count = file_names
        .iter()
        .filter(|name| is_delta_file(name) && file_version(name) < 10)
        .count();
    assert_eq!(stale_delta_count > 0, fail_delete, "{file_names:?}");
    assert_eq!(
        manager.manifest_usage(),
        manifest_recovery_usage(env.data_home().join("manifest")).await
    );

    // Recovery must succeed in both failure modes.
    manager.stop().await;
    let reopened = env
        .create_manifest_manager(CompressionType::Uncompressed, 1, None)
        .await
        .unwrap()
        .expect("manifest should be recoverable");
    assert_eq!(reopened.manifest().manifest_version, 10);
    assert_eq!(
        manager.manifest_usage(),
        manifest_recovery_usage(env.data_home().join("manifest")).await
    );
    assert_eq!(
        reopened.manifest_usage(),
        manifest_recovery_usage(env.data_home().join("manifest")).await
    );
    assert!(manifest_dir_usage(env.data_home().join("manifest")).await > reopened.manifest_usage());
}

#[tokio::test]
async fn open_preserves_listed_delta_not_found_retry_hint() {
    let env = TestEnv::new()
        .await
        .with_mock_layer(fail_manifest_reads_layer(is_delta_file));
    let metadata = Arc::new(basic_region_metadata());
    let mut manager = env
        .create_manifest_manager(CompressionType::Uncompressed, 0, Some(metadata.clone()))
        .await
        .unwrap()
        .unwrap();
    manager.stop().await;

    let error = env
        .create_manifest_manager(CompressionType::Uncompressed, 0, None)
        .await
        .expect_err("reopen must fail on the mocked delta read");
    assert_matches!(
        &error,
        ManifestDeltaNotFound {
            version: 0,
            path,
            error,
            ..
        } if path.ends_with("00000000000000000000.json")
            && error.kind() == object_store::ErrorKind::NotFound
    );
    assert_eq!(StatusCode::StorageUnavailable, error.status_code());
    assert_eq!(RetryHint::Retryable, error.retry_hint());
}

#[tokio::test]
async fn install_preserves_listed_delta_not_found_retry_hint() {
    let env = TestEnv::new()
        .await
        .with_mock_layer(fail_manifest_reads_layer(is_delta_file));
    let metadata = Arc::new(basic_region_metadata());
    let mut manager = env
        .create_manifest_manager(CompressionType::Uncompressed, 0, Some(metadata))
        .await
        .unwrap()
        .unwrap();
    let mut store = manager.store();
    store
        .save(1, &nop_action().encode().unwrap(), false)
        .await
        .unwrap();

    let error = manager.install_manifest_to(1).await.unwrap_err();
    assert_matches!(
        &error,
        ManifestDeltaNotFound {
            version: 1,
            path,
            ..
        } if path.ends_with("00000000000000000001.json")
    );
    assert_eq!(RetryHint::Retryable, error.retry_hint());
}

#[tokio::test]
async fn cancelled_waiter_keeps_pending_checkpoint_handle() {
    let (blocker, mock_layer) = CheckpointTaskBlocker::block_cleanup();
    let env = TestEnv::new().await.with_mock_layer(mock_layer);
    let metadata = Arc::new(basic_region_metadata());
    let mut manager = env
        .create_manifest_manager(CompressionType::Uncompressed, 1, Some(metadata))
        .await
        .unwrap()
        .unwrap();

    manager.update(nop_action(), false).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), blocker.wait_until_blocked())
        .await
        .expect("checkpoint cleanup did not start");

    let wait_started = manager.checkpointer().pending_checkpoint_wait_started();
    let wait_started = wait_started.notified();
    let mut first_wait = Box::pin(manager.wait_for_pending_checkpoint());
    tokio::select! {
        _ = wait_started => {}
        _ = &mut first_wait => panic!("checkpoint waiter returned before cleanup finished"),
    }
    drop(first_wait);
    assert!(manager.checkpointer().is_doing_checkpoint());

    blocker.release();
    manager.wait_for_pending_checkpoint().await;
    assert!(!manager.checkpointer().is_doing_checkpoint());

    let (version, _) = manager
        .store()
        .load_last_checkpoint()
        .await
        .unwrap()
        .expect("checkpoint must be published");
    assert_eq!(1, version);
}

#[tokio::test]
async fn running_checkpoint_prevents_scheduling_another_checkpoint() {
    let (blocker, mock_layer) = CheckpointTaskBlocker::block_cleanup();
    let env = TestEnv::new().await.with_mock_layer(mock_layer);
    let metadata = Arc::new(basic_region_metadata());
    let mut manager = env
        .create_manifest_manager(CompressionType::Uncompressed, 1, Some(metadata))
        .await
        .unwrap()
        .unwrap();

    manager.update(nop_action(), false).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), blocker.wait_until_blocked())
        .await
        .expect("checkpoint cleanup did not start");

    // The second update is eligible for checkpointing, but the first task still
    // owns the pending handle and must prevent another task from being scheduled.
    manager.update(nop_action(), false).await.unwrap();
    blocker.release();
    manager.wait_for_pending_checkpoint().await;

    assert_eq!(2, manager.manifest().manifest_version);
    assert_eq!(1, manager.checkpointer().last_checkpoint_version());
}

#[tokio::test]
async fn completed_checkpoint_is_reaped_before_scheduling_the_next_one() {
    let (blocker, mock_layer) = CheckpointTaskBlocker::block_cleanup();
    let env = TestEnv::new().await.with_mock_layer(mock_layer);
    let metadata = Arc::new(basic_region_metadata());
    let mut manager = env
        .create_manifest_manager(CompressionType::Uncompressed, 1, Some(metadata))
        .await
        .unwrap()
        .unwrap();

    manager.update(nop_action(), false).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), blocker.wait_until_blocked())
        .await
        .expect("first checkpoint cleanup did not start");
    blocker.release();

    tokio::time::timeout(Duration::from_secs(5), async {
        while manager.checkpointer().is_doing_checkpoint() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first checkpoint did not finish");
    assert_eq!(1, manager.checkpointer().last_checkpoint_version());

    // Do not explicitly wait/reap the completed handle. The next eligible
    // update must reap it before scheduling another checkpoint.
    blocker.arm_next_close();
    manager.update(nop_action(), false).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), blocker.wait_until_blocked())
        .await
        .expect("second checkpoint cleanup did not start");
    blocker.release();
    manager.wait_for_pending_checkpoint().await;

    assert_eq!(2, manager.checkpointer().last_checkpoint_version());
}

/// Retention runs inside the checkpoint task, so waiting for task completion also
/// makes logical size accounting observable to the assertions below.
async fn wait_for_manifest_checkpoint(manager: &RegionManifestManager) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while manager.checkpointer().is_doing_checkpoint() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn manifest_size_matches_recovery_bytes_after_open() {
    for compress_type in [
        CompressionType::Uncompressed,
        CompressionType::Gzip,
        CompressionType::Zstd,
    ] {
        for checkpoint_distance in [0, 3] {
            let (env, mut manager) = build_manager(checkpoint_distance, compress_type).await;
            for _ in 0..7 {
                manager.update(nop_action(), false).await.unwrap();
                wait_for_manifest_checkpoint(&manager).await;
            }
            let expected = manifest_recovery_usage(env.data_home().join("manifest")).await;
            assert_eq!(manager.manifest_usage(), expected);
            manager.stop().await;
            let reopened = reopen_manager(&env, checkpoint_distance, compress_type).await;
            assert_eq!(reopened.manifest_usage(), expected);
        }
    }
}

#[rstest::rstest]
#[case::without_checkpoints(0)]
#[case::with_checkpoints(3)]
#[tokio::test]
async fn manifest_size_follower_converges_after_sync(#[case] distance: u64) {
    for compression in [
        CompressionType::Uncompressed,
        CompressionType::Gzip,
        CompressionType::Zstd,
    ] {
        let (env, mut leader) = build_manager(distance, compression).await;
        let mut follower = reopen_manager(&env, distance, compression).await;
        for _ in 0..12 {
            leader.update(nop_action(), false).await.unwrap();
            leader.wait_for_pending_checkpoint().await;
            let expected = manifest_recovery_usage(env.data_home().join("manifest")).await;
            assert_eq!(leader.manifest_usage(), expected);
            follower
                .install_manifest_to(leader.last_version())
                .await
                .unwrap();
            assert_eq!(follower.manifest_usage(), expected);
        }
        // Promotion requires no accounting hook. New leader writes and checkpoints
        // continue from the same logical recovery set.
        leader.stop().await;
        for _ in 0..4 {
            follower.update(nop_action(), false).await.unwrap();
            follower.wait_for_pending_checkpoint().await;
            assert_eq!(
                follower.manifest_usage(),
                manifest_recovery_usage(env.data_home().join("manifest")).await
            );
        }
    }
}

#[tokio::test]
async fn noop_sync_accounts_for_new_checkpoint_without_listing() {
    let lists = Arc::new(AtomicUsize::new(0));
    let counter = lists.clone();
    let layer = MockLayerBuilder::default()
        .lister_factory(Arc::new(move |_, _, inner| {
            counter.fetch_add(1, Ordering::Relaxed);
            inner
        }))
        .build()
        .unwrap();
    let env = TestEnv::new().await.with_mock_layer(layer);
    let mut leader = build_manager_with_initial_metadata(&env, 0, CompressionType::Gzip).await;
    for _ in 0..3 {
        leader.update(nop_action(), false).await.unwrap();
    }
    let mut follower = reopen_manager(&env, 0, CompressionType::Gzip).await;
    let version = leader.last_version();
    let checkpoint = RegionCheckpoint {
        last_version: version,
        compacted_actions: 4,
        checkpoint: Some(leader.manifest().as_ref().clone()),
    };
    leader
        .store()
        .save_checkpoint(version, &checkpoint.encode().unwrap())
        .await
        .unwrap();
    let expected = manifest_recovery_usage(env.data_home().join("manifest")).await;
    assert_ne!(follower.manifest_usage(), expected);
    let before = lists.load(Ordering::Relaxed);
    // Pointer decoding and checkpoint stat failures are diagnostic only. The
    // next request must retry the missing accounting rather than prune blindly.
    let previous = follower.manifest_usage();
    let directory = env.data_home().join("manifest");
    let pointer = directory.join("_last_checkpoint");
    let bytes = tokio::fs::read(&pointer).await.unwrap();
    tokio::fs::write(&pointer, b"invalid checkpoint metadata")
        .await
        .unwrap();
    follower.install_manifest_to(version).await.unwrap();
    assert_eq!(follower.manifest_usage(), previous);
    tokio::fs::write(&pointer, bytes).await.unwrap();
    let checkpoint_path = directory.join(gen_path(
        "",
        &checkpoint_file(version),
        CompressionType::Gzip,
    ));
    let hidden = env.data_home().join("temporarily-hidden-checkpoint");
    tokio::fs::rename(&checkpoint_path, &hidden).await.unwrap();
    follower.install_manifest_to(version).await.unwrap();
    assert_eq!(follower.manifest_usage(), previous);
    tokio::fs::rename(&hidden, &checkpoint_path).await.unwrap();
    for _ in 0..3 {
        follower.install_manifest_to(version).await.unwrap();
        assert_eq!(follower.manifest_usage(), expected);
    }
    assert_eq!(lists.load(Ordering::Relaxed), before);
}

#[rstest::rstest]
#[case::listed_sizes(false, false, false)]
#[case::unknown_sizes(true, false, false)]
#[case::fallback_cache(false, true, false)]
#[case::missing_checkpoint(false, false, true)]
#[tokio::test]
async fn manifest_size_warm_cache_matches_recovery_bytes(
    #[case] unknown_sizes: bool,
    #[case] fallback: bool,
    #[case] missing_checkpoint: bool,
) {
    use common_base::readable_size::ReadableSize;
    use object_store::ObjectStore;
    use object_store::services::Fs;

    use crate::cache::manifest_cache::ManifestCache;
    use crate::manifest::manager::RegionManifestOptions;

    for compress_type in [
        CompressionType::Uncompressed,
        CompressionType::Gzip,
        CompressionType::Zstd,
    ] {
        let stored_compression = if fallback {
            CompressionType::Uncompressed
        } else {
            compress_type
        };
        let (env, mut leader) = build_manager(3, stored_compression).await;
        for _ in 0..7 {
            leader.update(nop_action(), false).await.unwrap();
            wait_for_manifest_checkpoint(&leader).await;
        }
        let object_store = ObjectStore::new(
            Fs::default().root(env.data_home().join("manifest").to_str().unwrap()),
        )
        .unwrap();
        let object_store = if unknown_sizes {
            object_store.layer(
                MockLayerBuilder::default()
                    .lister_factory(Arc::new(|_, _, inner| Box::new(ZeroSizeLister(inner))))
                    .build()
                    .unwrap(),
            )
        } else {
            object_store
        };
        let local_store =
            ObjectStore::new(Fs::default().root(env.data_home().join("cache").to_str().unwrap()))
                .unwrap();
        let cache = ManifestCache::new(local_store, ReadableSize::mb(10), None, true).await;
        let options = RegionManifestOptions {
            manifest_dir: "/".to_string(),
            object_store,
            compress_type,
            checkpoint_distance: 3,
            remove_file_options: Default::default(),
            manifest_cache: Some(cache.clone()),
            conditional_publication: false,
        };
        let expected = manifest_recovery_usage(env.data_home().join("manifest")).await;
        // First open fills the cache; the second reuses the cached checkpoint and deltas.
        for iteration in 0..2 {
            if iteration == 1 {
                let checkpoint_version = leader.checkpointer().last_checkpoint_version();
                let checkpoint_key = gen_path(
                    "/",
                    &checkpoint_file(checkpoint_version),
                    stored_compression,
                );
                assert!(
                    cache.get(&checkpoint_key).await.is_some(),
                    "{checkpoint_key}"
                );
                for version in checkpoint_version + 1..=leader.manifest().manifest_version {
                    let delta_key = gen_path("/", &delta_file(version), stored_compression);
                    assert!(
                        cache.get(delta_key.trim_start_matches('/')).await.is_some(),
                        "{delta_key}"
                    );
                }
            }
            let checkpoint_path = env.data_home().join("manifest").join(gen_path(
                "",
                &checkpoint_file(leader.checkpointer().last_checkpoint_version()),
                stored_compression,
            ));
            let hidden = env.data_home().join("temporarily-hidden-checkpoint");
            let missing_size = if missing_checkpoint && iteration == 1 {
                let size = tokio::fs::metadata(&checkpoint_path).await.unwrap().len();
                tokio::fs::rename(&checkpoint_path, &hidden).await.unwrap();
                size
            } else {
                0
            };
            let mut reopened = RegionManifestManager::open(options.clone(), &Default::default())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(reopened.manifest_usage(), expected - missing_size);
            if missing_size != 0 {
                tokio::fs::rename(&hidden, &checkpoint_path).await.unwrap();
                reopened
                    .install_manifest_to(reopened.last_version())
                    .await
                    .unwrap();
                assert_eq!(reopened.manifest_usage(), expected);
            }
            reopened.install_last_checkpoint().await.unwrap();
            assert_eq!(reopened.manifest_usage(), expected);
        }
    }
}

/// Exercises cache-hit accounting on backends whose LIST omits object sizes.
struct ZeroSizeLister(oio::Lister);

impl oio::List for ZeroSizeLister {
    async fn next(&mut self) -> MockResult<Option<oio::Entry>> {
        let Some(entry) = self.0.next().await? else {
            return Ok(None);
        };
        let (path, metadata) = entry.into_parts();
        if !metadata.is_file() {
            return Ok(Some(oio::Entry::new(&path, metadata)));
        }
        let mut builder = metadata.into_builder();
        builder.set_file(0);
        Ok(Some(oio::Entry::new(&path, builder.build())))
    }
}

#[rstest::rstest]
#[case(CompressionType::Uncompressed)]
#[case(CompressionType::Gzip)]
#[case(CompressionType::Zstd)]
#[tokio::test]
async fn manifest_cache_write_through_survives_reopen(#[case] compression: CompressionType) {
    use common_base::readable_size::ReadableSize;
    use common_test_util::temp_dir::create_temp_dir;
    use object_store::ObjectStore;
    use object_store::services::Fs;

    use crate::cache::manifest_cache::ManifestCache;
    use crate::manifest::manager::RegionManifestOptions;
    use crate::sst::FormatType;

    let remote_dir = create_temp_dir("manifest-manager-remote");
    let cache_dir = create_temp_dir("manifest-manager-cache");
    let remote = ObjectStore::new(Fs::default().root(remote_dir.path().to_str().unwrap())).unwrap();
    let local = ObjectStore::new(Fs::default().root(cache_dir.path().to_str().unwrap())).unwrap();
    let cache = ManifestCache::new(local.clone(), ReadableSize::mb(32), None, true).await;
    // Use a relative prefix, matching production keys and the recovered index.
    let mut options = RegionManifestOptions {
        manifest_dir: "region/".to_string(),
        object_store: remote,
        compress_type: compression,
        checkpoint_distance: 3,
        remove_file_options: Default::default(),
        manifest_cache: Some(cache.clone()),
        conditional_publication: false,
    };
    let mut writer = RegionManifestManager::new(
        Arc::new(basic_region_metadata()),
        0,
        options.clone(),
        FormatType::PrimaryKey,
        false,
        &Default::default(),
    )
    .await
    .unwrap();
    for _ in 0..7 {
        writer.update(nop_action(), false).await.unwrap();
        wait_for_manifest_checkpoint(&writer).await;
    }
    let key = gen_path(
        "region/",
        &checkpoint_file(writer.checkpointer().last_checkpoint_version()),
        compression,
    );
    assert!(cache.get(&key).await.is_some());
    let version = writer.manifest().manifest_version;
    let usage = writer.manifest_usage();
    drop(writer);
    drop(cache);
    // Release the final old cache handle before recovering a new index.
    drop(options.manifest_cache.take());
    let cache = ManifestCache::new(local, ReadableSize::mb(32), None, true).await;
    assert!(
        cache.get(&key).await.is_some(),
        "cache recovery missed {key}"
    );
    options.manifest_cache = Some(cache);
    let reopened = RegionManifestManager::open(options, &Default::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(version, reopened.manifest().manifest_version);
    assert_eq!(usage, reopened.manifest_usage());
}
