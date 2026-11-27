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

use api::v1::Rows;
use datatypes::value::Value;
use partition::expr::{PartitionExpr, col};
use store_api::region_engine::{RegionEngine, RemapManifestsRequest, SettableRegionRoleState};
use store_api::region_request::{RegionFlushRequest, RegionRequest};
use store_api::storage::RegionId;

use crate::config::MitoConfig;
use crate::error::Error;
use crate::manifest::action::RegionManifest;
use crate::test_util::{CreateRequestBuilder, TestEnv, build_rows, put_rows, rows_schema};

#[tokio::test]
async fn test_remap_manifests_invalid_partition_expr() {
    common_telemetry::init_default_ut_logging();
    test_remap_manifests_invalid_partition_expr_with_format(false).await;
    test_remap_manifests_invalid_partition_expr_with_format(true).await;
}

async fn test_remap_manifests_invalid_partition_expr_with_format(flat_format: bool) {
    let mut env = TestEnv::with_prefix("invalid-partition-expr").await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let err = engine
        .remap_manifests(RemapManifestsRequest {
            validate_only: false,
            region_id,
            input_regions: vec![region_id],
            region_mapping: [(region_id, vec![region_id])].into_iter().collect(),
            new_partition_exprs: [(region_id, "invalid expr".to_string())]
                .into_iter()
                .collect(),
        })
        .await
        .unwrap_err();
    assert_matches!(
        err.into_inner().as_any().downcast_ref::<Error>().unwrap(),
        Error::InvalidPartitionExpr { .. }
    )
}

#[tokio::test]
async fn test_remap_manifests_invalid_region_state() {
    common_telemetry::init_default_ut_logging();
    test_remap_manifests_invalid_region_state_with_format(false).await;
    test_remap_manifests_invalid_region_state_with_format(true).await;
}

fn range_expr(col_name: &str, start: i64, end: i64) -> PartitionExpr {
    col(col_name)
        .gt_eq(Value::Int64(start))
        .and(col(col_name).lt(Value::Int64(end)))
}

async fn test_remap_manifests_invalid_region_state_with_format(flat_format: bool) {
    let mut env = TestEnv::with_prefix("invalid-region-state").await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let err = engine
        .remap_manifests(RemapManifestsRequest {
            validate_only: false,
            region_id,
            input_regions: vec![region_id],
            region_mapping: [(region_id, vec![region_id])].into_iter().collect(),
            new_partition_exprs: [(region_id, range_expr("x", 0, 100).as_json_str().unwrap())]
                .into_iter()
                .collect(),
        })
        .await
        .unwrap_err();
    assert_matches!(
        err.into_inner().as_any().downcast_ref::<Error>().unwrap(),
        Error::RegionState { .. }
    )
}

#[tokio::test]
async fn test_remap_manifests_invalid_input_regions() {
    common_telemetry::init_default_ut_logging();
    test_remap_manifests_invalid_input_regions_with_format(false).await;
    test_remap_manifests_invalid_input_regions_with_format(true).await;
}

async fn test_remap_manifests_invalid_input_regions_with_format(flat_format: bool) {
    let mut env = TestEnv::with_prefix("invalid-input-regions").await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
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
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::StagingLeader)
        .await
        .unwrap();
    let err = engine
        .remap_manifests(RemapManifestsRequest {
            validate_only: false,
            region_id,
            input_regions: vec![region_id, RegionId::new(2, 1)],
            region_mapping: [(region_id, vec![region_id])].into_iter().collect(),
            new_partition_exprs: [(region_id, range_expr("x", 0, 100).as_json_str().unwrap())]
                .into_iter()
                .collect(),
        })
        .await
        .unwrap_err();
    assert_matches!(
        err.into_inner().as_any().downcast_ref::<Error>().unwrap(),
        Error::InvalidRequest { .. }
    )
}

#[tokio::test]
async fn test_validate_only_checks_without_staging() {
    common_telemetry::init_default_ut_logging();
    test_validate_only_checks_without_staging_with_format(false).await;
    test_validate_only_checks_without_staging_with_format(true).await;
}

async fn test_validate_only_checks_without_staging_with_format(flat_format: bool) {
    // The repartition procedure asks the merge sources before it publishes the new routes, so the
    // region is not in staging yet. The instruction must still be answered: it inspects the
    // ordinary manifest, stages nothing, and returns no manifest paths.
    let mut env = TestEnv::with_prefix("validate-only").await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let expr_json = range_expr("tag_0", 0, 100).as_json_str().unwrap();
    let request = CreateRequestBuilder::new()
        .partition_expr_json(Some(expr_json.clone()))
        .build();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas,
            rows: build_rows(0, 10),
        },
    )
    .await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest::default()),
        )
        .await
        .unwrap();

    let validate_request = || RemapManifestsRequest {
        validate_only: true,
        region_id,
        input_regions: vec![region_id],
        region_mapping: [(region_id, vec![RegionId::new(1, 2)])]
            .into_iter()
            .collect(),
        new_partition_exprs: [(RegionId::new(1, 2), expr_json.clone())]
            .into_iter()
            .collect(),
    };
    let result = engine.remap_manifests(validate_request()).await.unwrap();
    assert!(
        result.manifest_paths.is_empty(),
        "{:?}",
        result.manifest_paths
    );

    // Nothing was staged: the same request without the flag is refused, which is the very error
    // a datanode that ignores the flag would hit.
    let err = engine
        .remap_manifests(RemapManifestsRequest {
            validate_only: false,
            ..validate_request()
        })
        .await
        .unwrap_err();
    assert_matches!(
        err.into_inner().as_any().downcast_ref::<Error>().unwrap(),
        Error::RegionState { .. }
    );
}

#[tokio::test]
async fn test_remap_manifests_success() {
    common_telemetry::init_default_ut_logging();
    test_remap_manifests_success_with_format(false).await;
    test_remap_manifests_success_with_format(true).await;
}

async fn test_remap_manifests_success_with_format(flat_format: bool) {
    let mut env = TestEnv::with_prefix("engine-stop").await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
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
                RegionRequest::Flush(RegionFlushRequest::default()),
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
            validate_only: false,
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
}
