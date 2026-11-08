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

use store_api::region_engine::RegionEngine;
use store_api::region_request::RegionRequest;
use store_api::storage::{FileId, RegionId};

use crate::config::MitoConfig;
use crate::manifest::action::{RegionEdit, RegionMetaAction, RegionMetaActionList};
use crate::sst::file::{FileMeta, LEVEL_SHARED};
use crate::test_util::{CreateRequestBuilder, TestEnv};

#[tokio::test]
async fn test_shared_import_preserves_visible_sequence_and_rejects_overwrite() {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            schedule_compaction_after_edit: false,
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
    let region = engine.get_region(region_id).unwrap();
    let source = FileMeta {
        region_id: RegionId::new(1, 0),
        file_id: FileId::random(),
        level: LEVEL_SHARED,
        ..Default::default()
    };
    let import = RegionEdit {
        files_to_add: vec![source.clone()],
        ..Default::default()
    };
    let first = engine.edit_region(region_id, import.clone()).await;
    if !cfg!(feature = "enterprise") {
        assert!(first.is_err());
        assert!(
            region.version().ssts.levels()[LEVEL_SHARED as usize]
                .files
                .is_empty()
        );
        return;
    }
    first.unwrap();
    let admitted = region.version().ssts.levels()[LEVEL_SHARED as usize].files[&source.file_id]
        .meta_ref()
        .clone();
    assert_eq!(source.file_id(), admitted.file_id());
    assert_eq!(Some(1), admitted.sequence.map(|s| s.get()));
    engine.edit_region(region_id, import).await.unwrap();
    assert_eq!(1, region.version_control.current().last_sequence);
    assert_eq!(
        &admitted,
        region.version().ssts.levels()[LEVEL_SHARED as usize].files[&source.file_id].meta_ref()
    );

    let manifest_version = region.manifest_ctx.manifest_version().await;
    let changed = FileMeta {
        num_rows: 1,
        ..admitted.clone()
    };
    assert!(
        engine
            .edit_region(
                region_id,
                RegionEdit {
                    files_to_add: vec![changed],
                    ..Default::default()
                }
            )
            .await
            .is_err()
    );
    assert!(
        engine
            .edit_region(
                region_id,
                RegionEdit {
                    files_to_remove: vec![admitted.clone()],
                    ..Default::default()
                }
            )
            .await
            .is_err()
    );
    // A plan captured before a file became shared must also fail at publication,
    // even if it bypasses the import queue and still calls the ordinary L0 path.
    let stale = FileMeta {
        level: 0,
        ..admitted
    };
    assert!(
        region
            .manifest_ctx
            .update_manifest_for_compaction(RegionMetaActionList::with_action(
                RegionMetaAction::Edit(RegionEdit {
                    files_to_remove: vec![stale],
                    ..Default::default()
                })
            ))
            .await
            .is_err()
    );
    assert_eq!(
        manifest_version,
        region.manifest_ctx.manifest_version().await
    );
    assert_eq!(1, region.version_control.current().last_sequence);
}
