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

use std::sync::Arc;
use std::{assert_matches, fs};

use api::v1::Rows;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_recordbatch::RecordBatches;
use object_store::layers::mock::{Error as MockError, ErrorKind, MockLayerBuilder};
use store_api::region_engine::{MitoCopyRegionFromRequest, RegionEngine, RegionRole};
use store_api::region_request::{RegionFlushRequest, RegionRequest};
use store_api::storage::{RegionId, ScanRequest};

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
            default_flat_format: flat_format,
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
            RegionRequest::Flush(RegionFlushRequest::default()),
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
            MitoCopyRegionFromRequest {
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
    let source_region_files = collect_filename_in_dir(&source_region_dir);
    let target_region_dir = format!("{}/data/test/1_0000000002", env.data_home().display());
    let target_region_files = collect_filename_in_dir(&target_region_dir);
    assert_eq!(source_region_files, target_region_files);

    if with_index {
        let source_region_index_files =
            collect_filename_in_dir(&format!("{}/index", source_region_dir));
        let target_region_index_files =
            collect_filename_in_dir(&format!("{}/index", target_region_dir));
        assert_eq!(source_region_index_files, target_region_index_files);
    }
    common_telemetry::debug!("copy region from again");
    let resp2 = engine
        .copy_region_from(
            target_region_id,
            MitoCopyRegionFromRequest {
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
            default_flat_format: flat_format,
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
            RegionRequest::Flush(RegionFlushRequest::default()),
        )
        .await
        .unwrap();
    let source_region_dir = format!("{}/data/test/1_0000000001", env.data_home().display());
    assert_file_num_in_dir(&source_region_dir, 1);
    assert_file_num_in_dir(&format!("{}/index", source_region_dir), 1);
    let source_region_files = collect_filename_in_dir(&source_region_dir);
    let source_region_index_files =
        collect_filename_in_dir(&format!("{}/index", source_region_dir));

    // Creates a target region and enters staging mode
    let target_region_id = RegionId::new(1, 2);
    engine
        .handle_request(target_region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    let err = engine
        .copy_region_from(
            target_region_id,
            MitoCopyRegionFromRequest {
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
    assert_eq!(
        source_region_files,
        collect_filename_in_dir(&source_region_dir)
    );
    assert_eq!(
        source_region_index_files,
        collect_filename_in_dir(&format!("{}/index", source_region_dir))
    );
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

fn collect_filename_in_dir(dir: &str) -> Vec<String> {
    let mut files = fs::read_dir(dir)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .into_iter()
        .filter(|f| f.metadata().unwrap().is_file())
        .map(|f| {
            f.path()
                .to_string_lossy()
                .rsplit("/")
                .last()
                .unwrap()
                .to_string()
        })
        .collect::<Vec<_>>();
    files.sort_unstable();

    files
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
            default_flat_format: flat_format,
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
            MitoCopyRegionFromRequest {
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
            MitoCopyRegionFromRequest {
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
            default_flat_format: flat_format,
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
            MitoCopyRegionFromRequest {
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

/// Regression for #8865: `copy_region_from` must not carry the source region's
/// `FileMeta::preserve_row_sequence` trust marker into the target region, whose
/// sequence domain is independent. The copied file's physical per-row sequences
/// belong to the source region only; trusting them in the target would let an
/// exact sequence-range request replay source rows as if they were target
/// sequences. The marker must be cleared so the target fails closed with
/// `SequenceRangeUnsupported` instead.
#[tokio::test]
async fn test_copy_region_from_clears_preserve_row_sequence_marker() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::with_prefix("copy-region-from-preserve-sequence").await;
    let engine = env.create_engine(MitoConfig::default()).await;

    // Source and target regions both preserve per-row sequences.
    let source_region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("append_mode", "true")
        .insert_option("preserve_row_sequence", "true")
        .build();
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
            RegionRequest::Flush(RegionFlushRequest::default()),
        )
        .await
        .unwrap();

    // The flushed source file keeps the trust marker: the source region's own
    // sequence domain is intact.
    let source_manifest = engine
        .get_region(source_region_id)
        .unwrap()
        .manifest_ctx
        .manifest()
        .await;
    assert_eq!(1, source_manifest.files.len());
    assert!(
        source_manifest
            .files
            .values()
            .all(|meta| meta.preserve_row_sequence),
        "source files must keep the preserve_row_sequence marker"
    );

    // Create a preserve-enabled target and copy the source files into it.
    let target_region_id = RegionId::new(1, 2);
    engine
        .handle_request(target_region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    engine
        .copy_region_from(
            target_region_id,
            MitoCopyRegionFromRequest {
                source_region_id,
                parallelism: 1,
            },
        )
        .await
        .unwrap();

    // The copied file in the target must NOT be trusted: its physical per-row
    // sequences belong to the source region's independent sequence domain.
    // Both the `preserve_row_sequence` marker and the source-domain max
    // `sequence` hint must be cleared, so exact reads fail closed instead of
    // silently skipping the file as "proven disjoint".
    let target_manifest = engine
        .get_region(target_region_id)
        .unwrap()
        .manifest_ctx
        .manifest()
        .await;
    assert_eq!(1, target_manifest.files.len());
    assert!(
        target_manifest
            .files
            .values()
            .all(|meta| !meta.preserve_row_sequence),
        "copied files must have the preserve_row_sequence marker cleared"
    );
    assert!(
        target_manifest
            .files
            .values()
            .all(|meta| meta.sequence.is_none()),
        "copied files must have their source-domain sequence hint cleared"
    );

    // An exact sequence-range request intersecting the copied rows' sequences
    // (rows 3..=7 in the source domain) must fail closed with
    // `SequenceRangeUnsupported` instead of replaying source-domain rows.
    let err = engine
        .scanner(
            target_region_id,
            ScanRequest {
                memtable_min_sequence: Some(2),
                memtable_max_sequence: Some(7),
                exact_sequence_range: true,
                ..Default::default()
            },
        )
        .await
        .err()
        .expect("expected sequence-range unsupported error");
    assert!(
        matches!(err, Error::SequenceRangeUnsupported { .. }),
        "unexpected err: {err}"
    );
    assert_eq!(err.status_code(), StatusCode::Unsupported);

    // Sanity: the source region keeps the marker and still serves the same
    // exact range with its own row-level sequences (3..=7, 5 rows).
    let stream = engine
        .scan_to_stream(
            source_region_id,
            ScanRequest {
                memtable_min_sequence: Some(2),
                memtable_max_sequence: Some(7),
                exact_sequence_range: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        5, row_count,
        "source exact range should return rows with sequence 3..=7"
    );
}
