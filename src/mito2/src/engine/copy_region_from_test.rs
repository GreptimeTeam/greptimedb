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
use std::sync::Arc;

use api::v1::Rows;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use object_store::layers::mock::{Error as MockError, ErrorKind, MockLayerBuilder};
use store_api::region_engine::{CopyRegionFromRequest, RegionEngine, RegionRole};
use store_api::region_request::{RegionFlushRequest, RegionRequest};
use store_api::storage::RegionId;

use crate::config::MitoConfig;
use crate::error::Error;
use crate::test_util::{CreateRequestBuilder, TestEnv, build_rows, put_rows, rows_schema};

#[tokio::test]
async fn test_engine_copy_region_from() {
    common_telemetry::init_default_ut_logging();

    test_engine_copy_region_from_with_format(true, true).await;
    test_engine_copy_region_from_with_format(true, false).await;
    test_engine_copy_region_from_with_format(false, true).await;
    test_engine_copy_region_from_with_format(false, false).await;
}

async fn test_engine_copy_region_from_with_format(flat_format: bool, with_index: bool) {
    let mut env = TestEnv::with_prefix("copy-region-from").await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;
    // Creates a source region and adds some data
    let source_region_id = RegionId::new(1, 1);
    let mut request = CreateRequestBuilder::new().build();
    if with_index {
        request
            .column_metadatas
            .iter_mut()
            .find(|c| c.column_schema.name == "tag_0")
            .unwrap()
            .column_schema
            .set_inverted_index(true);
    }

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(source_region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 42),
    };
    put_rows(&engine, source_region_id, rows).await;
    engine
        .handle_request(
            source_region_id,
            RegionRequest::Flush(RegionFlushRequest {
                row_group_size: None,
            }),
        )
        .await
        .unwrap();

    // Creates a target region and enters staging mode
    let target_region_id = RegionId::new(1, 2);
    engine
        .handle_request(target_region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    common_telemetry::debug!("copy region from");
    let resp = engine
        .copy_region_from(
            target_region_id,
            CopyRegionFromRequest {
                source_region_id,
                parallelism: 1,
            },
        )
        .await
        .unwrap();

    let manifest = engine
        .get_region(target_region_id)
        .unwrap()
        .manifest_ctx
        .manifest()
        .await;
    assert!(!manifest.files.is_empty());
    for meta in manifest.files.values() {
        assert_eq!(meta.region_id, target_region_id);
        assert_eq!(meta.exists_index(), with_index);
    }

    let source_region_dir = format!("{}/data/test/1_0000000001", env.data_home().display());
    assert_file_num_in_dir(&source_region_dir, 1);
    if with_index {
        assert_file_num_in_dir(&format!("{}/index", source_region_dir), 1);
    }

    let target_region_dir = format!("{}/data/test/1_0000000002", env.data_home().display());
    assert_file_num_in_dir(&target_region_dir, 1);
    if with_index {
        assert_file_num_in_dir(&format!("{}/index", target_region_dir), 1);
    }
    common_telemetry::debug!("copy region from again");
    let resp2 = engine
        .copy_region_from(
            target_region_id,
            CopyRegionFromRequest {
                source_region_id,
                parallelism: 1,
            },
        )
        .await
        .unwrap();
    assert_eq!(resp.copied_file_ids, resp2.copied_file_ids);
}

#[tokio::test]
async fn test_engine_copy_region_failure() {
    common_telemetry::init_default_ut_logging();
    test_engine_copy_region_failure_with_format(false).await;
    test_engine_copy_region_failure_with_format(true).await;
}

async fn test_engine_copy_region_failure_with_format(flat_format: bool) {
    let mock_layer = MockLayerBuilder::default()
        .copy_interceptor(Arc::new(|from, _, _args| {
            if from.contains(".puffin") {
                Some(Err(MockError::new(ErrorKind::Unexpected, "mock err")))
            } else {
                None
            }
        }))
        .build()
        .unwrap();
    let mut env = TestEnv::new().await.with_mock_layer(mock_layer);
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;
    // Creates a source region and adds some data
    let source_region_id = RegionId::new(1, 1);
    let mut request = CreateRequestBuilder::new().build();
    request
        .column_metadatas
        .iter_mut()
        .find(|c| c.column_schema.name == "tag_0")
        .unwrap()
        .column_schema
        .set_inverted_index(true);

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(source_region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 42),
    };
    put_rows(&engine, source_region_id, rows).await;
    engine
        .handle_request(
            source_region_id,
            RegionRequest::Flush(RegionFlushRequest {
                row_group_size: None,
            }),
        )
        .await
        .unwrap();
    let source_region_dir = format!("{}/data/test/1_0000000001", env.data_home().display());
    assert_file_num_in_dir(&source_region_dir, 1);
    assert_file_num_in_dir(&format!("{}/index", source_region_dir), 1);

    // Creates a target region and enters staging mode
    let target_region_id = RegionId::new(1, 2);
    engine
        .handle_request(target_region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    let err = engine
        .copy_region_from(
            target_region_id,
            CopyRegionFromRequest {
                source_region_id,
                parallelism: 1,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.status_code(), StatusCode::StorageUnavailable);

    // Check target region directory is empty
    let target_region_dir = format!("{}/data/test/1_0000000002", env.data_home().display());
    assert_file_num_in_dir(&target_region_dir, 0);
    assert!(!fs::exists(format!("{}/index", target_region_dir)).unwrap());

    // Check source region directory is not affected
    let source_region_dir = format!("{}/data/test/1_0000000001", env.data_home().display());
    assert_file_num_in_dir(&source_region_dir, 1);
    assert_file_num_in_dir(&format!("{}/index", source_region_dir), 1);
}

fn assert_file_num_in_dir(dir: &str, expected_num: usize) {
    let files = fs::read_dir(dir)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .into_iter()
        .filter(|f| f.metadata().unwrap().is_file())
        .collect::<Vec<_>>();
    assert_eq!(
        files.len(),
        expected_num,
        "The number of files in the directory should be {}, got: {:?}",
        expected_num,
        files
    );
}

#[tokio::test]
async fn test_engine_copy_region_invalid_args() {
    common_telemetry::init_default_ut_logging();
    test_engine_copy_region_invalid_args_with_format(false).await;
    test_engine_copy_region_invalid_args_with_format(true).await;
}

async fn test_engine_copy_region_invalid_args_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();
    let err = engine
        .copy_region_from(
            region_id,
            CopyRegionFromRequest {
                source_region_id: RegionId::new(2, 1),
                parallelism: 1,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.status_code(), StatusCode::InvalidArguments);
    let err = engine
        .copy_region_from(
            region_id,
            CopyRegionFromRequest {
                source_region_id: RegionId::new(1, 1),
                parallelism: 1,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.status_code(), StatusCode::InvalidArguments);
}

#[tokio::test]
async fn test_engine_copy_region_unexpected_state() {
    common_telemetry::init_default_ut_logging();
    test_engine_copy_region_unexpected_state_with_format(false).await;
    test_engine_copy_region_unexpected_state_with_format(true).await;
}

async fn test_engine_copy_region_unexpected_state_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();
    engine
        .set_region_role(region_id, RegionRole::Follower)
        .unwrap();

    let err = engine
        .copy_region_from(
            region_id,
            CopyRegionFromRequest {
                source_region_id: RegionId::new(1, 2),
                parallelism: 1,
            },
        )
        .await
        .unwrap_err();
    assert_matches!(
        err.as_any().downcast_ref::<Error>().unwrap(),
        Error::RegionState { .. }
    )
}
