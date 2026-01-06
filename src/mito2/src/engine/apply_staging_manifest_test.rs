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
use std::fs;

use api::v1::Rows;
use datatypes::value::Value;
use partition::expr::{PartitionExpr, col};
use store_api::region_engine::{
    RegionEngine, RegionRole, RemapManifestsRequest, SettableRegionRoleState,
};
use store_api::region_request::{
    ApplyStagingManifestRequest, EnterStagingRequest, RegionFlushRequest, RegionRequest,
};
use store_api::storage::{FileId, RegionId};

use crate::config::MitoConfig;
use crate::error::Error;
use crate::manifest::action::RegionManifest;
use crate::sst::file::FileMeta;
use crate::test_util::{CreateRequestBuilder, TestEnv, build_rows, put_rows, rows_schema};

fn range_expr(col_name: &str, start: i64, end: i64) -> PartitionExpr {
    col(col_name)
        .gt_eq(Value::Int64(start))
        .and(col(col_name).lt(Value::Int64(end)))
}

#[tokio::test]
async fn test_apply_staging_manifest_invalid_region_state() {
    common_telemetry::init_default_ut_logging();
    test_apply_staging_manifest_invalid_region_state_with_format(false).await;
    test_apply_staging_manifest_invalid_region_state_with_format(true).await;
}

async fn test_apply_staging_manifest_invalid_region_state_with_format(flat_format: bool) {
    let mut env = TestEnv::with_prefix("invalid-region-state").await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .partition_expr_json(Some(range_expr("x", 0, 50).as_json_str().unwrap()))
        .build();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Region is in leader state, apply staging manifest request should fail.
    let err = engine
        .handle_request(
            region_id,
            RegionRequest::ApplyStagingManifest(ApplyStagingManifestRequest {
                partition_expr: range_expr("x", 0, 100).as_json_str().unwrap(),
                central_region_id: RegionId::new(1, 0),
                manifest_path: "manifest.json".to_string(),
            }),
        )
        .await
        .unwrap_err();
    assert_matches!(
        err.into_inner().as_any().downcast_ref::<Error>().unwrap(),
        Error::RegionState { .. }
    );

    // Region is in leader state, apply staging manifest request should fail.
    engine
        .set_region_role(region_id, RegionRole::Follower)
        .unwrap();
    let err = engine
        .handle_request(
            region_id,
            RegionRequest::ApplyStagingManifest(ApplyStagingManifestRequest {
                partition_expr: range_expr("x", 0, 100).as_json_str().unwrap(),
                central_region_id: RegionId::new(1, 0),
                manifest_path: "manifest.json".to_string(),
            }),
        )
        .await
        .unwrap_err();
    assert_matches!(
        err.into_inner().as_any().downcast_ref::<Error>().unwrap(),
        Error::RegionState { .. }
    );
}

#[tokio::test]
async fn test_apply_staging_manifest_mismatched_partition_expr() {
    common_telemetry::init_default_ut_logging();
    test_apply_staging_manifest_mismatched_partition_expr_with_format(false).await;
    test_apply_staging_manifest_mismatched_partition_expr_with_format(true).await;
}

async fn test_apply_staging_manifest_mismatched_partition_expr_with_format(flat_format: bool) {
    let mut env = TestEnv::with_prefix("mismatched-partition-expr").await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_expr: range_expr("x", 0, 50).as_json_str().unwrap(),
            }),
        )
        .await
        .unwrap();

    let err = engine
        .handle_request(
            region_id,
            RegionRequest::ApplyStagingManifest(ApplyStagingManifestRequest {
                partition_expr: range_expr("x", 0, 100).as_json_str().unwrap(),
                central_region_id: RegionId::new(1, 0),
                manifest_path: "dummy".to_string(),
            }),
        )
        .await
        .unwrap_err();
    assert_matches!(
        err.into_inner().as_any().downcast_ref::<Error>().unwrap(),
        Error::StagingPartitionExprMismatch { .. }
    );

    // If staging manifest's partition expr is different from the request.
    let result = engine
        .remap_manifests(RemapManifestsRequest {
            region_id,
            input_regions: vec![region_id],
            region_mapping: [(region_id, vec![region_id])].into_iter().collect(),
            new_partition_exprs: [(region_id, range_expr("x", 0, 49).as_json_str().unwrap())]
                .into_iter()
                .collect(),
        })
        .await
        .unwrap();

    let err = engine
        .handle_request(
            region_id,
            RegionRequest::ApplyStagingManifest(ApplyStagingManifestRequest {
                partition_expr: range_expr("x", 0, 50).as_json_str().unwrap(),
                central_region_id: region_id,
                manifest_path: result.manifest_paths[&region_id].clone(),
            }),
        )
        .await
        .unwrap_err();
    assert_matches!(
        err.into_inner().as_any().downcast_ref::<Error>().unwrap(),
        Error::StagingPartitionExprMismatch { .. }
    );
}

#[tokio::test]
async fn test_apply_staging_manifest_success() {
    common_telemetry::init_default_ut_logging();
    test_apply_staging_manifest_success_with_format(false).await;
    test_apply_staging_manifest_success_with_format(true).await;
}

async fn test_apply_staging_manifest_success_with_format(flat_format: bool) {
    let mut env = TestEnv::with_prefix("success").await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .partition_expr_json(Some(range_expr("tag_0", 0, 100).as_json_str().unwrap()))
        .build();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    let new_region_id_1 = RegionId::new(1, 2);
    let new_region_id_2 = RegionId::new(1, 3);
    // Generate some data
    for i in 0..3 {
        let rows_data = Rows {
            schema: column_schemas.clone(),
            rows: build_rows(i * 10, (i + 1) * 10),
        };
        put_rows(&engine, region_id, rows_data).await;
        engine
            .handle_request(
                region_id,
                RegionRequest::Flush(RegionFlushRequest {
                    row_group_size: None,
                }),
            )
            .await
            .unwrap();
    }
    engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::StagingLeader)
        .await
        .unwrap();
    let result = engine
        .remap_manifests(RemapManifestsRequest {
            region_id,
            input_regions: vec![region_id],
            region_mapping: [(region_id, vec![new_region_id_1, new_region_id_2])]
                .into_iter()
                .collect(),
            new_partition_exprs: [
                (
                    new_region_id_1,
                    range_expr("tag_0", 0, 50).as_json_str().unwrap(),
                ),
                (
                    new_region_id_2,
                    range_expr("tag_0", 50, 100).as_json_str().unwrap(),
                ),
            ]
            .into_iter()
            .collect(),
        })
        .await
        .unwrap();
    let region = engine.get_region(region_id).unwrap();
    let manager = region.manifest_ctx.manifest_manager.write().await;
    let manifest_storage = manager.store();
    let blob_store = manifest_storage.staging_storage().blob_storage();

    assert_eq!(result.manifest_paths.len(), 2);
    common_telemetry::debug!("manifest paths: {:?}", result.manifest_paths);
    let new_manifest_1 = blob_store
        .get(&result.manifest_paths[&new_region_id_1])
        .await
        .unwrap();
    let new_manifest_2 = blob_store
        .get(&result.manifest_paths[&new_region_id_2])
        .await
        .unwrap();
    let new_manifest_1 = serde_json::from_slice::<RegionManifest>(&new_manifest_1).unwrap();
    let new_manifest_2 = serde_json::from_slice::<RegionManifest>(&new_manifest_2).unwrap();
    assert_eq!(new_manifest_1.files.len(), 3);
    assert_eq!(new_manifest_2.files.len(), 3);
    drop(manager);

    let request = CreateRequestBuilder::new().build();
    engine
        .handle_request(new_region_id_1, RegionRequest::Create(request))
        .await
        .unwrap();
    engine
        .handle_request(
            new_region_id_1,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_expr: range_expr("tag_0", 0, 50).as_json_str().unwrap(),
            }),
        )
        .await
        .unwrap();
    // Before apply staging manifest, the files should be empty
    let region = engine.get_region(new_region_id_1).unwrap();
    let manifest = region.manifest_ctx.manifest().await;
    assert_eq!(manifest.files.len(), 0);
    let staging_manifest = region.manifest_ctx.staging_manifest().await.unwrap();
    assert_eq!(staging_manifest.files.len(), 0);

    engine
        .handle_request(
            new_region_id_1,
            RegionRequest::ApplyStagingManifest(ApplyStagingManifestRequest {
                partition_expr: range_expr("tag_0", 0, 50).as_json_str().unwrap(),
                central_region_id: region_id,
                manifest_path: result.manifest_paths[&new_region_id_1].clone(),
            }),
        )
        .await
        .unwrap();
    // After apply staging manifest, the files should be the same as the new manifest
    let region = engine.get_region(new_region_id_1).unwrap();
    let manifest = region.manifest_ctx.manifest().await;
    assert_eq!(manifest.files.len(), 3);
    assert!(region.is_writable());
    assert!(!region.is_staging());
    // The manifest partition expr should be the same as the request.
    assert_eq!(
        manifest.metadata.partition_expr.as_ref().unwrap(),
        &range_expr("tag_0", 0, 50).as_json_str().unwrap()
    );
    // The staging manifest should be cleared.
    let staging_manifest = region.manifest_ctx.staging_manifest().await;
    assert!(staging_manifest.is_none());
    // The staging partition expr should be cleared.
    assert!(region.staging_partition_expr.lock().unwrap().is_none());
    // The staging manifest directory should be empty.
    let data_home = env.data_home();
    let region_dir = format!("{}/data/test/1_0000000001", data_home.display());
    let staging_manifest_dir = format!("{}/staging/manifest", region_dir);
    let staging_files = fs::read_dir(&staging_manifest_dir)
        .map(|entries| {
            entries
                .filter(|e| e.as_ref().unwrap().metadata().unwrap().is_file())
                .collect::<Result<Vec<_>, _>>()
                .unwrap_or_default()
        })
        .unwrap_or_default();
    assert_eq!(staging_files.len(), 0, "staging_files: {:?}", staging_files);

    let region = engine.get_region(region_id).unwrap();
    let manager = region.manifest_ctx.manifest_manager.write().await;
    let manifest_storage = manager.store();
    let blob_store = manifest_storage.staging_storage().blob_storage();

    let new_manifest_1 = blob_store
        .get(&result.manifest_paths[&new_region_id_1])
        .await
        .unwrap();
    let mut new_manifest_1 = serde_json::from_slice::<RegionManifest>(&new_manifest_1).unwrap();

    // Try to modify the file sequence.
    let file_id = FileId::random();
    new_manifest_1.files.insert(
        file_id,
        FileMeta {
            region_id,
            file_id,
            ..Default::default()
        },
    );
    blob_store
        .put(
            &result.manifest_paths[&new_region_id_1],
            serde_json::to_vec(&new_manifest_1).unwrap(),
        )
        .await
        .unwrap();
    drop(manager);
    // This request will be ignored.
    engine
        .handle_request(
            new_region_id_1,
            RegionRequest::ApplyStagingManifest(ApplyStagingManifestRequest {
                partition_expr: range_expr("tag_0", 0, 50).as_json_str().unwrap(),
                central_region_id: region_id,
                manifest_path: result.manifest_paths[&new_region_id_1].clone(),
            }),
        )
        .await
        .unwrap();
    // The files number should not change.
    let region = engine.get_region(new_region_id_1).unwrap();
    let manifest = region.manifest_ctx.manifest().await;
    assert_eq!(manifest.files.len(), 3);
}

#[tokio::test]
async fn test_apply_staging_manifest_invalid_files_to_add() {
    common_telemetry::init_default_ut_logging();
    test_apply_staging_manifest_invalid_files_to_add_with_format(false).await;
    test_apply_staging_manifest_invalid_files_to_add_with_format(true).await;
}

async fn test_apply_staging_manifest_invalid_files_to_add_with_format(flat_format: bool) {
    let mut env = TestEnv::with_prefix("invalid-files-to-add").await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_expr: range_expr("tag_0", 0, 50).as_json_str().unwrap(),
            }),
        )
        .await
        .unwrap();
    // Apply staging manifest with not exists manifest path.
    let err = engine
        .handle_request(
            region_id,
            RegionRequest::ApplyStagingManifest(ApplyStagingManifestRequest {
                partition_expr: range_expr("tag_0", 0, 50).as_json_str().unwrap(),
                central_region_id: RegionId::new(1, 0),
                manifest_path: "dummy".to_string(),
            }),
        )
        .await
        .unwrap_err();
    assert_matches!(
        err.into_inner().as_any().downcast_ref::<Error>().unwrap(),
        Error::OpenDal { .. }
    );

    // Apply staging manifest with invalid bytes.
    let region = engine.get_region(region_id).unwrap();
    let manager = region.manifest_ctx.manifest_manager.write().await;
    let manifest_storage = manager.store();
    let blob_store = manifest_storage.staging_storage().blob_storage();
    blob_store
        .put("invalid_bytes", b"invalid_bytes".to_vec())
        .await
        .unwrap();
    drop(manager);
    let err = engine
        .handle_request(
            region_id,
            RegionRequest::ApplyStagingManifest(ApplyStagingManifestRequest {
                partition_expr: range_expr("tag_0", 0, 50).as_json_str().unwrap(),
                central_region_id: region_id,
                manifest_path: "invalid_bytes".to_string(),
            }),
        )
        .await
        .unwrap_err();
    assert_matches!(
        err.into_inner().as_any().downcast_ref::<Error>().unwrap(),
        Error::SerdeJson { .. }
    );
}
