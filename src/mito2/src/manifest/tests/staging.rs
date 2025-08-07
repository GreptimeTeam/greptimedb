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

//! Tests for staging state functionality in manifest management.

use std::sync::Arc;
use std::time::Duration;

use common_datasource::compression::CompressionType;

use crate::manifest::action::{RegionEdit, RegionMetaAction, RegionMetaActionList};
use crate::manifest::manager::RegionManifestManager;
use crate::manifest::tests::utils::basic_region_metadata;
use crate::region::{RegionLeaderState, RegionRoleState};
use crate::test_util::TestEnv;

async fn build_manager_with_checkpoint_distance(
    checkpoint_distance: u64,
) -> (TestEnv, RegionManifestManager) {
    let metadata = Arc::new(basic_region_metadata());
    let env = TestEnv::new().await;
    let manager = env
        .create_manifest_manager(
            CompressionType::Uncompressed,
            checkpoint_distance,
            Some(metadata.clone()),
        )
        .await
        .unwrap()
        .unwrap();

    (env, manager)
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
async fn test_manifest_checkpoint_bypass_in_staging() {
    common_telemetry::init_default_ut_logging();
    let (_env, mut manager) = build_manager_with_checkpoint_distance(1).await;

    // Apply actions in staging mode - checkpoint should be bypassed
    for _ in 0..15 {
        manager
            .update(
                nop_action(),
                RegionRoleState::Leader(RegionLeaderState::Staging),
            )
            .await
            .unwrap();
    }

    // Wait a bit to ensure no checkpoint is created
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify no checkpoint was created in staging mode
    assert!(manager
        .store()
        .load_last_checkpoint()
        .await
        .unwrap()
        .is_none());

    // Now switch to normal mode and apply one more action
    manager
        .update(
            nop_action(),
            RegionRoleState::Leader(RegionLeaderState::Writable),
        )
        .await
        .unwrap();

    // Wait for potential checkpoint
    while manager.checkpointer().is_doing_checkpoint() {
        tokio::time::sleep(Duration::from_millis(10)).await;
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

#[tokio::test]
async fn test_manifest_checkpoint_normal_mode() {
    common_telemetry::init_default_ut_logging();
    let (_env, mut manager) = build_manager_with_checkpoint_distance(1).await;

    // Apply actions in normal writable mode
    for _ in 0..5 {
        manager
            .update(
                nop_action(),
                RegionRoleState::Leader(RegionLeaderState::Writable),
            )
            .await
            .unwrap();

        while manager.checkpointer().is_doing_checkpoint() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    // Verify checkpoint was created in normal mode
    let checkpoint_result = manager.store().load_last_checkpoint().await.unwrap();
    assert!(
        checkpoint_result.is_some(),
        "Checkpoint should exist in writable mode"
    );
}

#[tokio::test]
async fn test_manifest_mixed_staging_and_writable_modes() {
    common_telemetry::init_default_ut_logging();
    let (_env, mut manager) = build_manager_with_checkpoint_distance(3).await;

    // Apply some actions in writable mode
    for _ in 0..2 {
        manager
            .update(
                nop_action(),
                RegionRoleState::Leader(RegionLeaderState::Writable),
            )
            .await
            .unwrap();
    }

    // Switch to staging mode and apply more actions
    for _ in 0..5 {
        manager
            .update(
                nop_action(),
                RegionRoleState::Leader(RegionLeaderState::Staging),
            )
            .await
            .unwrap();
    }

    // Wait to ensure no checkpoint during staging
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Switch back to writable and trigger checkpoint
    manager
        .update(
            nop_action(),
            RegionRoleState::Leader(RegionLeaderState::Writable),
        )
        .await
        .unwrap();

    while manager.checkpointer().is_doing_checkpoint() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Verify checkpoint was eventually created
    let checkpoint_result = manager.store().load_last_checkpoint().await.unwrap();
    assert!(checkpoint_result.is_some(), "Checkpoint should exist");

    let (last_version, _) = checkpoint_result.unwrap();
    // Should have checkpointed after accumulating enough actions
    assert!(
        last_version >= 3,
        "Should checkpoint after reaching distance"
    );
}

#[tokio::test]
async fn test_manifest_staging_state_context_integration() {
    use crate::manifest::manager::RegionManifestOptions;
    use crate::region::ManifestContext;
    use crate::test_util::scheduler_util::SchedulerEnv;
    use crate::test_util::version_util::VersionControlBuilder;

    let env = SchedulerEnv::new().await;
    let builder = VersionControlBuilder::new();
    let version_control = Arc::new(builder.build());
    let metadata = version_control.current().version.metadata.clone();

    let manager = RegionManifestManager::new(
        metadata.clone(),
        RegionManifestOptions {
            manifest_dir: "".to_string(),
            object_store: env.access_layer.object_store().clone(),
            compress_type: CompressionType::Uncompressed,
            checkpoint_distance: 10,
        },
        Default::default(),
        Default::default(),
    )
    .await
    .unwrap();

    let manifest_ctx = Arc::new(ManifestContext::new(
        manager,
        RegionRoleState::Leader(RegionLeaderState::Writable),
    ));

    // Verify initial state
    assert_eq!(
        manifest_ctx.current_state(),
        RegionRoleState::Leader(RegionLeaderState::Writable)
    );

    // Test that the manifest context properly supports staging state management
    // (State transitions are tested at the region level)
}
