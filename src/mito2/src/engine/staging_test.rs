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

//! Integration tests for staging state functionality.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use std::{assert_matches, fs};

use api::v1::{ArrowIpc, ColumnSchema, Rows};
use async_trait::async_trait;
use common_error::ext::{ErrorExt, RetryHint};
use common_error::status_code::StatusCode;
use common_function::utils::partition_expr_version;
use common_grpc::flight;
use common_recordbatch::RecordBatches;
use common_time::util::current_time_millis;
use datatypes::arrow::array::{ArrayRef, Float64Array, StringArray, TimestampMillisecondArray};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::value::Value;
use object_store::Buffer;
use object_store::layers::mock::{
    Delete, Deleter, Entry, Error as MockError, ErrorKind, List, Lister, Metadata,
    MockLayerBuilder, OpDelete, Result as MockResult, Write, Writer,
};
use partition::expr::{PartitionExpr, col};
use store_api::mito_engine_options::WRITE_BUFFER_SIZE_KEY;
use store_api::region_engine::{
    RegionEngine, RegionRole, RemapManifestsRequest, SetRegionRoleStateResponse,
    SettableRegionRoleState,
};
use store_api::region_request::{
    ApplyStagingManifestRequest, EnterStagingRequest, RegionAlterRequest, RegionBulkInsertsRequest,
    RegionCloseRequest, RegionFlushReason, RegionFlushRequest, RegionPutRequest, RegionRequest,
    RegionTruncateRequest, StagingPartitionDirective,
};
use store_api::storage::{FileId, RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::engine::basic_test::build_bulk_insert_request;
use crate::engine::listener::{
    CountingStallListener, EventListener, FlushListener, NotifyEnterStagingResultListener,
};
use crate::error::Error;
use crate::flush::WriteBufferManager;
use crate::manifest::action::{
    RegionChange, RegionEdit, RegionMetaAction, RegionMetaActionList, RegionPartitionExprChange,
};
use crate::region::{MitoRegionRef, RegionLeaderState, RegionRoleState};
use crate::request::{BackgroundNotify, RegionEditRequest, WorkerRequest};
use crate::sst::FormatType;
use crate::sst::file::FileMeta;
use crate::test_util::{
    CheckpointTaskBlocker, CreateRequestBuilder, DeltaFaults, MockWriteBufferManager, TestEnv,
    build_delete_rows, build_rows, delete_rows, delete_rows_schema, flush_region, put_rows,
    reopen_region, rows_schema,
};
use crate::time_provider::mock::MockTimeProvider;
use crate::worker::MAX_INITIAL_CHECK_DELAY_SECS;

fn range_expr(col_name: &str, start: i64, end: i64) -> PartitionExpr {
    col(col_name)
        .gt_eq(Value::Int64(start))
        .and(col(col_name).lt(Value::Int64(end)))
}

fn float_range_expr(col_name: &str, start: f64, end: f64) -> PartitionExpr {
    col(col_name)
        .gt_eq(Value::Float64(start.into()))
        .and(col(col_name).lt(Value::Float64(end.into())))
}

#[tokio::test]
async fn test_staging_state_integration() {
    test_staging_state_integration_with_format(false).await;
    test_staging_state_integration_with_format(true).await;
}

async fn test_staging_state_integration_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    // Create region
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Test external API patterns work correctly
    use store_api::region_engine::SettableRegionRoleState;

    let (role_req, _receiver) = WorkerRequest::new_set_readonly_gracefully(
        region_id,
        SettableRegionRoleState::StagingLeader,
    );
    match role_req {
        WorkerRequest::SetRegionRoleStateGracefully {
            region_id: req_region_id,
            region_role_state,
            ..
        } => {
            assert_eq!(req_region_id, region_id);
            assert_eq!(region_role_state, SettableRegionRoleState::StagingLeader);
        }
        _ => panic!("Expected SetRegionRoleStateGracefully request"),
    }

    let (role_req, _receiver) =
        WorkerRequest::new_set_readonly_gracefully(region_id, SettableRegionRoleState::Leader);
    match role_req {
        WorkerRequest::SetRegionRoleStateGracefully {
            region_id: req_region_id,
            region_role_state,
            ..
        } => {
            assert_eq!(req_region_id, region_id);
            assert_eq!(region_role_state, SettableRegionRoleState::Leader);
        }
        _ => panic!("Expected SetRegionRoleStateGracefully request"),
    }
}

#[tokio::test]
async fn test_staging_blocks_alter_operations() {
    test_staging_blocks_alter_operations_with_format(false).await;
    test_staging_blocks_alter_operations_with_format(true).await;
}

async fn test_staging_blocks_alter_operations_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    // Create region
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Note: In the current implementation, we can't directly test staging mode
    // through the engine interface since staging transitions are handled
    // through worker requests. This test demonstrates the pattern that would
    // be used once external control interfaces are implemented.

    // Test that ALTER operations would be blocked in staging mode
    let alter_request = RegionAlterRequest {
        kind: store_api::region_request::AlterKind::AddColumns { columns: vec![] },
    };

    // This currently succeeds since we're not in staging mode
    let result = engine
        .handle_request(region_id, RegionRequest::Alter(alter_request))
        .await;
    assert!(result.is_ok(), "ALTER should succeed in normal mode");
}

#[tokio::test]
async fn test_staging_blocks_truncate_operations() {
    test_staging_blocks_truncate_operations_with_format(false).await;
    test_staging_blocks_truncate_operations_with_format(true).await;
}

async fn test_staging_blocks_truncate_operations_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    // Create region
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Test that TRUNCATE operations would be blocked in staging mode
    let truncate_request = RegionTruncateRequest::All;

    // This currently succeeds since we're not in staging mode
    let result = engine
        .handle_request(region_id, RegionRequest::Truncate(truncate_request))
        .await;
    assert!(result.is_ok(), "TRUNCATE should succeed in normal mode");
}

#[tokio::test]
async fn test_staging_state_validation_patterns() {
    // Test the state validation patterns used throughout the codebase
    let staging_state = RegionRoleState::Leader(RegionLeaderState::Staging);
    let writable_state = RegionRoleState::Leader(RegionLeaderState::Writable);

    // Test staging detection
    let is_staging = staging_state == RegionRoleState::Leader(RegionLeaderState::Staging);
    assert!(is_staging, "Should correctly identify staging state");

    let is_not_staging = writable_state == RegionRoleState::Leader(RegionLeaderState::Staging);
    assert!(
        !is_not_staging,
        "Should correctly identify non-staging state"
    );

    // Test writable state check
    let staging_is_writable = matches!(
        staging_state,
        RegionRoleState::Leader(RegionLeaderState::Writable)
            | RegionRoleState::Leader(RegionLeaderState::Staging)
    );
    assert!(staging_is_writable, "Staging regions should be writable");

    let writable_is_writable = matches!(
        writable_state,
        RegionRoleState::Leader(RegionLeaderState::Writable)
            | RegionRoleState::Leader(RegionLeaderState::Staging)
    );
    assert!(writable_is_writable, "Writable regions should be writable");

    // Test flushable state check
    let staging_is_flushable = matches!(
        staging_state,
        RegionRoleState::Leader(RegionLeaderState::Writable)
            | RegionRoleState::Leader(RegionLeaderState::Staging)
            | RegionRoleState::Leader(RegionLeaderState::Downgrading)
    );
    assert!(staging_is_flushable, "Staging regions should be flushable");

    let writable_is_flushable = matches!(
        writable_state,
        RegionRoleState::Leader(RegionLeaderState::Writable)
            | RegionRoleState::Leader(RegionLeaderState::Staging)
            | RegionRoleState::Leader(RegionLeaderState::Downgrading)
    );
    assert!(
        writable_is_flushable,
        "Writable regions should be flushable"
    );
}

fn default_partition_expr() -> String {
    range_expr("a", 0, 100).as_json_str().unwrap()
}

#[tokio::test]
async fn test_staging_reject_all_writes_rejects_put_and_bulk() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(2048, 0);
    let request = CreateRequestBuilder::new().build();
    let column_schemas = rows_schema(&request);

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::RejectAllWrites,
            }),
        )
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 2),
    };
    let payload = RecordBatch::try_from_iter([
        (
            "tag_0",
            Arc::new(StringArray::from(vec!["0", "1"])) as ArrayRef,
        ),
        (
            "field_0",
            Arc::new(Float64Array::from(vec![0.0, 1.0])) as ArrayRef,
        ),
        (
            "ts",
            Arc::new(TimestampMillisecondArray::from(vec![0, 1000])) as ArrayRef,
        ),
    ])
    .unwrap();
    let (schema, data_header, data) = flight::record_batch_to_ipc(payload.clone()).unwrap();
    for request in [
        RegionRequest::Put(RegionPutRequest {
            skip_wal: false,
            rows,
            hint: None,
            partition_expr_version: None,
        }),
        RegionRequest::BulkInserts(RegionBulkInsertsRequest {
            skip_wal: false,
            region_id,
            payload,
            raw_data: ArrowIpc {
                schema,
                data_header,
                payload: data,
            },
            partition_expr_version: None,
            aligned_schema_version: None,
        }),
    ] {
        let err = engine.handle_request(region_id, request).await.unwrap_err();
        assert_eq!(err.status_code(), StatusCode::StorageUnavailable);
        assert_eq!(err.retry_hint(), RetryHint::RetryAfterRouteRefresh);
        assert_matches!(
            err.into_inner().as_any().downcast_ref::<Error>().unwrap(),
            Error::RejectWriteInStaging { .. }
        );
    }
}

#[tokio::test]
async fn test_staging_write_partition_expr_version() {
    test_staging_write_partition_expr_version_with_format(false).await;
    test_staging_write_partition_expr_version_with_format(true).await;
}

async fn test_staging_write_partition_expr_version_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1024, 0);
    let origin_partition_expr = range_expr("a", 0, 50).as_json_str().unwrap();
    let request = CreateRequestBuilder::new()
        .partition_expr_json(Some(origin_partition_expr.clone()))
        .build();
    let column_schemas = rows_schema(&request);

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let partition_expr = default_partition_expr();
    engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::UpdatePartitionExpr(
                    partition_expr.clone(),
                ),
            }),
        )
        .await
        .unwrap();

    let expected_version = partition_expr_version(Some(&partition_expr));
    let origin_version = partition_expr_version(Some(&origin_partition_expr));
    common_telemetry::info!(
        "expected_version: {}, origin_version: {}",
        expected_version,
        origin_version
    );
    let bad_rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    let err = engine
        .handle_request(
            region_id,
            RegionRequest::Put(RegionPutRequest {
                skip_wal: false,
                rows: bad_rows,
                hint: None,
                partition_expr_version: Some(origin_version),
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(err.status_code(), StatusCode::InvalidArguments);
    assert_matches!(
        err.into_inner().as_any().downcast_ref::<Error>().unwrap(),
        Error::PartitionExprVersionMismatch { .. }
    );

    let compat_rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(3, 6),
    };
    let result = engine
        .handle_request(
            region_id,
            RegionRequest::Put(RegionPutRequest {
                skip_wal: false,
                rows: compat_rows,
                hint: None,
                partition_expr_version: None,
            }),
        )
        .await
        .unwrap();
    assert_eq!(result.affected_rows, 3);

    let ok_rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(6, 9),
    };
    let result = engine
        .handle_request(
            region_id,
            RegionRequest::Put(RegionPutRequest {
                skip_wal: false,
                rows: ok_rows,
                hint: None,
                partition_expr_version: Some(expected_version),
            }),
        )
        .await
        .unwrap();
    assert_eq!(result.affected_rows, 3);

    let result = engine
        .remap_manifests(RemapManifestsRequest {
            region_id,
            input_regions: vec![region_id],
            region_mapping: [(region_id, vec![region_id])].into_iter().collect(),
            new_partition_exprs: [(region_id, default_partition_expr())]
                .into_iter()
                .collect(),
        })
        .await
        .unwrap();
    engine
        .handle_request(
            region_id,
            RegionRequest::ApplyStagingManifest(ApplyStagingManifestRequest {
                partition_expr: default_partition_expr(),
                central_region_id: region_id,
                manifest_path: result.manifest_paths[&region_id].clone(),
            }),
        )
        .await
        .unwrap();

    let exit_rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(9, 12),
    };
    let err = engine
        .handle_request(
            region_id,
            RegionRequest::Put(RegionPutRequest {
                skip_wal: false,
                rows: exit_rows,
                hint: None,
                partition_expr_version: Some(origin_version),
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(err.status_code(), StatusCode::InvalidArguments);

    let compat_rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(12, 15),
    };
    let result = engine
        .handle_request(
            region_id,
            RegionRequest::Put(RegionPutRequest {
                skip_wal: false,
                rows: compat_rows,
                hint: None,
                partition_expr_version: None,
            }),
        )
        .await
        .unwrap();
    assert_eq!(result.affected_rows, 3);

    let committed_version = engine
        .get_region(region_id)
        .unwrap()
        .version()
        .metadata
        .partition_expr_version;
    assert_ne!(0, committed_version);
    assert_eq!(committed_version, expected_version,);

    let commit_rows = Rows {
        schema: column_schemas,
        rows: build_rows(15, 18),
    };
    let result = engine
        .handle_request(
            region_id,
            RegionRequest::Put(RegionPutRequest {
                skip_wal: false,
                rows: commit_rows,
                hint: None,
                partition_expr_version: Some(expected_version),
            }),
        )
        .await
        .unwrap();
    assert_eq!(result.affected_rows, 3);
}

#[tokio::test]
async fn test_staging_manifest_directory() {
    test_staging_manifest_directory_with_format(false).await;
    test_staging_manifest_directory_with_format(true).await;
}

async fn test_staging_manifest_directory_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let partition_expr = default_partition_expr();
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1024, 0);
    let request = CreateRequestBuilder::new().build();

    // Check manifest files after region creation (before staging mode)
    // Create region
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Check that manifest files exist after region creation
    let data_home = env.data_home();

    let region_dir = format!("{}/data/test/1024_0000000000", data_home.display());
    let normal_manifest_dir = format!("{}/manifest", region_dir);
    assert!(
        fs::metadata(&normal_manifest_dir).is_ok(),
        "Normal manifest directory should exist"
    );
    let normal_files_before = fs::read_dir(&normal_manifest_dir).unwrap().count();

    // Now test staging mode manifest creation
    // Set region to staging mode using the engine API
    engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::UpdatePartitionExpr(
                    partition_expr.clone(),
                ),
            }),
        )
        .await
        .unwrap();
    let region = engine.get_region(region_id).unwrap();
    let staging_partition_info = region.manifest_ctx.staging_partition_info();
    assert_eq!(
        staging_partition_info
            .unwrap()
            .partition_expr()
            .unwrap()
            .to_string(),
        partition_expr,
    );
    {
        let manager = region.manifest_ctx.manifest_manager.read().await;
        assert_eq!(
            manager
                .staging_manifest()
                .unwrap()
                .metadata
                .partition_expr
                .as_deref()
                .unwrap(),
            &partition_expr,
        );
        assert!(manager.manifest().metadata.partition_expr.is_none());
    }

    // Should be ok to enter staging mode again with the same partition expr
    engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::UpdatePartitionExpr(
                    partition_expr.clone(),
                ),
            }),
        )
        .await
        .unwrap();

    // Should throw error if try to enter staging mode again with a different partition expr
    let err = engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::UpdatePartitionExpr("".to_string()),
            }),
        )
        .await
        .unwrap_err();
    assert_matches!(
        err.into_inner().as_any().downcast_ref::<Error>().unwrap(),
        Error::StagingPartitionExprMismatch { .. }
    );

    // Staging keeps its actions in memory: it writes no manifest file.
    let staging_manifest_dir = format!("{}/staging/manifest", region_dir);
    assert_eq!(
        fs::read_dir(&staging_manifest_dir)
            .map(|entries| entries.count())
            .unwrap_or_default(),
        0
    );
    assert_eq!(
        fs::read_dir(&normal_manifest_dir).unwrap().count(),
        normal_files_before
    );
}

#[tokio::test]
async fn test_staging_exit_success_with_manifests() {
    test_staging_exit_success_with_manifests_with_format(false).await;
    test_staging_exit_success_with_manifests_with_format(true).await;
}

async fn test_staging_exit_success_with_manifests_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let partition_expr = float_range_expr("field_0", 0., 100.).as_json_str().unwrap();
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1024, 0);
    let request = CreateRequestBuilder::new().build();
    let column_schemas = rows_schema(&request);

    // Create region
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // This data is flushed when entering staging mode.
    let rows_data = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows_data).await;

    // Enter staging mode
    engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::UpdatePartitionExpr(
                    partition_expr.clone(),
                ),
            }),
        )
        .await
        .unwrap();

    // Rows written during staging stay in the memtable.
    let rows_data = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(3, 10),
    };
    put_rows(&engine, region_id, rows_data).await;

    // A source region provides the files that staging imports.
    let source_region_id = RegionId::new(1024, 1);
    engine
        .handle_request(
            source_region_id,
            RegionRequest::Create(CreateRequestBuilder::new().build()),
        )
        .await
        .unwrap();
    for (start, end) in [(10, 13), (13, 15)] {
        let rows_data = Rows {
            schema: column_schemas.clone(),
            rows: build_rows(start, end),
        };
        put_rows(&engine, source_region_id, rows_data).await;
        flush_region(&engine, source_region_id, None).await;
    }
    let source_files = engine
        .get_region(source_region_id)
        .unwrap()
        .manifest_ctx
        .manifest()
        .await
        .files
        .values()
        .cloned()
        .collect::<Vec<_>>();
    engine
        .handle_request(
            source_region_id,
            RegionRequest::Close(RegionCloseRequest::default()),
        )
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    for (file, expected) in source_files.into_iter().zip([11, 12]) {
        engine
            .edit_region(
                region_id,
                RegionEdit {
                    files_to_add: vec![file],
                    files_to_remove: vec![],
                    timestamp_ms: None,
                    compaction_time_window: None,
                    flushed_entry_id: None,
                    flushed_sequence: None,
                    committed_sequence: None,
                    wal_recovery: Default::default(),
                    last_invalid_seq: None,
                },
            )
            .await
            .unwrap();
        let current = region.version_control.current();
        assert_eq!(current.last_sequence, expected);
        assert_eq!(current.committed_sequence, 10);
    }

    // Staging keeps its actions in memory and takes no version.
    let data_home = env.data_home();
    let region_dir = format!("{}/data/test/1024_0000000000", data_home.display());
    let version_before = region.manifest_ctx.manifest_version().await;
    {
        let manager = region.manifest_ctx.manifest_manager.read().await;
        // Entering staging and two edits.
        assert_eq!(manager.staged_actions().unwrap().actions.len(), 3);
        let staging_manifest = manager.staging_manifest().unwrap();
        assert_eq!(staging_manifest.files.len(), 3);
        assert_eq!(
            staging_manifest.metadata.partition_expr.as_ref().unwrap(),
            &partition_expr
        );
    }

    // Count normal manifest files before exit
    let normal_manifest_dir = format!("{}/manifest", region_dir);
    let normal_files_before = fs::read_dir(&normal_manifest_dir)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let normal_count_before = normal_files_before.len();
    assert_eq!(
        // One file for table creation
        // One file for flush operation
        normal_count_before,
        2,
        "Normal manifest directory should initially contain 2 files"
    );

    // Try read data before exiting staging, imported SST files should be invisible
    let request = ScanRequest::default();
    let scanner = engine.scanner(region_id, request).await.unwrap();
    assert_eq!(
        scanner.num_files(),
        1,
        "1 SST files should be scanned before exit"
    );
    assert_eq!(
        scanner.num_memtables(),
        1,
        "Memtables should be kept in staging before exit"
    );
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let total_rows: usize = batches.iter().map(|rb| rb.num_rows()).sum();
    assert_eq!(
        total_rows, 10,
        "10 rows should be readable before exit staging mode"
    );

    // Inspect SSTs from manifest
    let sst_entries = engine.all_ssts_from_manifest().await;
    assert_eq!(
        sst_entries.len(),
        3,
        "sst entries should be 3, got: {:?}",
        sst_entries
    );
    assert_eq!(sst_entries.iter().filter(|e| e.visible).count(), 1);
    assert_eq!(sst_entries.iter().filter(|e| !e.visible).count(), 2);

    let ingested_parquet_bytes = region.region_statistic().ingested_parquet_bytes;
    assert!(ingested_parquet_bytes > 0);

    // Streaming inspection must retain staging entries and their visibility.
    {
        use futures::StreamExt;

        let mut batches = engine.scan_ssts_from_manifest(
            &[datafusion_expr::col("region_id").eq(datafusion_expr::lit(region_id.as_u64()))],
            1,
        );
        let mut actual = Vec::new();
        while let Some(batch) = batches.next().await {
            assert_eq!(batch.len(), 1);
            actual.extend(batch);
        }
        actual.sort_unstable_by(|a, b| a.file_id.cmp(&b.file_id));
        let mut expected = sst_entries;
        expected.sort_unstable_by(|a, b| a.file_id.cmp(&b.file_id));
        assert_eq!(actual, expected);
    }

    // Staging metadata overrides the same file in the normal manifest. Filter
    // evaluation must use that winning metadata, even if it is not projected.
    {
        use futures::StreamExt;

        let region = engine.get_region(region_id).unwrap();
        let original = region
            .manifest_ctx
            .manifest_manager
            .read()
            .await
            .manifest()
            .files
            .values()
            .next()
            .unwrap()
            .clone();
        let mut overridden = original.clone();
        overridden.file_size = u64::MAX - 1;
        for metadata in [overridden, original.clone()] {
            region
                .manifest_ctx
                .manifest_manager
                .write()
                .await
                .update(
                    RegionMetaActionList::new(vec![RegionMetaAction::Edit(RegionEdit {
                        files_to_add: vec![metadata.clone()],
                        files_to_remove: vec![],
                        timestamp_ms: None,
                        compaction_time_window: None,
                        flushed_entry_id: None,
                        flushed_sequence: None,
                        committed_sequence: None,
                        wal_recovery: Default::default(),
                        last_invalid_seq: None,
                    })]),
                    true,
                )
                .await
                .unwrap();
            let entries = engine.all_ssts_from_manifest().await;
            assert_eq!(entries.len(), 3);
            let matches = entries
                .iter()
                .filter(|entry| entry.file_id == original.file_id.to_string())
                .collect::<Vec<_>>();
            assert_eq!(matches.len(), 1);
            assert_eq!(matches[0].file_size, metadata.file_size);
            assert!(matches[0].visible);

            let schema = store_api::sst_entry::ManifestSstEntry::schema();
            let projection = ["file_id", "visible"]
                .map(|name| {
                    schema
                        .column_schemas()
                        .iter()
                        .position(|c| c.name == name)
                        .unwrap()
                })
                .to_vec();
            let mut stream = engine.scan_ssts_from_manifest_projected(
                &[datafusion_expr::col("file_size").eq(datafusion_expr::lit(metadata.file_size))],
                1,
                Some(&projection),
            );
            let mut filtered = Vec::new();
            while let Some(batch) = stream.next().await {
                filtered.extend(batch);
            }
            assert!(
                filtered
                    .iter()
                    .any(|entry| entry.file_id == original.file_id.to_string())
            );
            if metadata.file_size == u64::MAX - 1 {
                assert_eq!(filtered.len(), 1);
            }
        }
    }

    // Exit staging mode successfully
    engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Leader)
        .await
        .unwrap();

    // Verify we're back in normal mode
    let workers = &engine.inner.workers;
    let region = workers.get_region(region_id).unwrap();
    let current = region.version_control.current();
    // The exit publishes its own invalid-seq boundary `B = 13` (last_sequence 12 + 1) and
    // raises the commit watermark to it in the same publish. It holds the publish window
    // (the `Editing` state) while it allocates, persists and publishes, so no write can be
    // attributed a sequence at or below `B`: every write admitted afterwards obtains
    // `B + 1` (14) or later, and `last_sequence` never runs ahead of a failed publish.
    assert_eq!(
        (current.last_sequence, current.committed_sequence),
        (13, 13)
    );
    assert_eq!(13, current.version.last_invalid_seq());
    // Every staged file must carry the published boundary: the persisted action list
    // applies the final merged edit exactly once, so no original edit can overwrite
    // the barrier on replay.
    let manifest = region.manifest_ctx.manifest().await;
    assert!(!manifest.files.is_empty());
    assert!(
        manifest
            .files
            .values()
            .all(|meta| meta.sequence.map(|sequence| sequence.get()) == Some(13)),
        "all staged files must carry the published boundary, got: {:?}",
        manifest
            .files
            .values()
            .map(|meta| meta.sequence.map(|sequence| sequence.get()))
            .collect::<Vec<_>>()
    );
    assert_eq!(Some(13), manifest.last_invalid_seq);
    assert!(
        !region.is_staging(),
        "Region should no longer be in staging mode"
    );

    assert_eq!(
        region.region_statistic().ingested_parquet_bytes,
        ingested_parquet_bytes
    );

    // The end of staging commits the staged actions as the next version.
    assert_eq!(
        region.manifest_ctx.manifest_version().await,
        version_before + 1
    );
    let normal_files_after = fs::read_dir(&normal_manifest_dir)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(normal_files_after.len(), normal_count_before + 1);

    // Validate in-memory version reflects merged manifests (files visible in levels)
    let version = region.version();
    let levels = version.ssts.levels();
    assert!(
        !levels.is_empty() && !levels[0].files.is_empty(),
        "SST levels should have files after exiting staging"
    );

    // Also ensure scanner behavior reflects 2 SSTs
    let request = ScanRequest::default();
    let scanner = engine.scanner(region_id, request).await.unwrap();
    assert_eq!(
        scanner.num_files(),
        3,
        "SST files should be scanned after exit"
    );

    // Try reading data via scanner to ensure previous staged data is actually readable
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let total_rows: usize = batches.iter().map(|rb| rb.num_rows()).sum();
    assert_eq!(total_rows, 15, "Expected to read all staged rows");

    // Inspect SSTs from manifest
    let sst_entries = engine.all_ssts_from_manifest().await;
    assert_eq!(sst_entries.len(), 3);
    assert!(sst_entries.iter().all(|e| e.visible));
}

#[tokio::test]
async fn test_enter_staging_writes_partition_expr_change_action() {
    test_enter_staging_writes_partition_expr_change_action_with_format(false).await;
    test_enter_staging_writes_partition_expr_change_action_with_format(true).await;
}

async fn test_enter_staging_writes_partition_expr_change_action_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(2000, 1);
    let request = CreateRequestBuilder::new().build();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let partition_expr = default_partition_expr();
    engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::UpdatePartitionExpr(
                    partition_expr.clone(),
                ),
            }),
        )
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    let manager = region.manifest_ctx.manifest_manager.read().await;
    let staged_actions = manager.staged_actions().unwrap();

    let mut found_partition_expr_change = false;
    let mut found_change = false;
    for action in &staged_actions.actions {
        match action {
            RegionMetaAction::PartitionExprChange(change) => {
                found_partition_expr_change = true;
                assert_eq!(change.partition_expr, Some(partition_expr.clone()));
            }
            RegionMetaAction::Change(_) => {
                found_change = true;
            }
            _ => {}
        }
    }

    assert!(found_partition_expr_change);
    assert!(!found_change);
}

#[tokio::test]
async fn test_staging_exit_conflict_partition_expr_change_and_change() {
    test_staging_exit_conflict_partition_expr_change_and_change_with_format(false).await;
    test_staging_exit_conflict_partition_expr_change_and_change_with_format(true).await;
}

async fn test_staging_exit_conflict_partition_expr_change_and_change_with_format(
    flat_format: bool,
) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(2000, 2);
    let request = CreateRequestBuilder::new().build();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let partition_expr = default_partition_expr();
    engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::UpdatePartitionExpr(
                    partition_expr.clone(),
                ),
            }),
        )
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    let mut changed_metadata = region.version().metadata.as_ref().clone();
    changed_metadata.set_partition_expr(Some(partition_expr.clone()));

    let mut manager = region.manifest_ctx.manifest_manager.write().await;
    manager
        .update(
            RegionMetaActionList::new(vec![
                RegionMetaAction::PartitionExprChange(RegionPartitionExprChange {
                    partition_expr: Some(partition_expr),
                }),
                RegionMetaAction::Change(RegionChange {
                    metadata: Arc::new(changed_metadata),
                    sst_format: FormatType::PrimaryKey,
                    append_mode: None,
                }),
                RegionMetaAction::Edit(RegionEdit {
                    files_to_add: Vec::new(),
                    files_to_remove: Vec::new(),
                    timestamp_ms: None,
                    compaction_time_window: None,
                    flushed_entry_id: None,
                    flushed_sequence: None,
                    committed_sequence: None,
                    wal_recovery: Default::default(),
                    last_invalid_seq: None,
                }),
            ]),
            true,
        )
        .await
        .unwrap();
    drop(manager);

    let response = engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Leader)
        .await
        .unwrap();
    match response {
        SetRegionRoleStateResponse::InvalidTransition(err) => {
            assert_matches!(
                err.as_any().downcast_ref::<Error>().unwrap(),
                Error::Unexpected { .. }
            );
        }
        _ => panic!("Expected InvalidTransition response, got: {response:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_write_stall_on_enter_staging() {
    test_write_stall_on_enter_staging_with_format(false).await;
    test_write_stall_on_enter_staging_with_format(true).await;
}

async fn test_write_stall_on_enter_staging_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let partition_expr = default_partition_expr();
    let listener = Arc::new(NotifyEnterStagingResultListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig {
                default_flat_format: flat_format,
                ..Default::default()
            },
            None,
            Some(listener.clone()),
            None,
        )
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let engine_cloned = engine.clone();
    let alter_job = tokio::spawn(async move {
        engine_cloned
            .handle_request(
                region_id,
                RegionRequest::EnterStaging(EnterStagingRequest {
                    partition_directive: StagingPartitionDirective::UpdatePartitionExpr(
                        partition_expr.clone(),
                    ),
                }),
            )
            .await
            .unwrap();
    });
    // Make sure the loop is handling the alter request.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let column_schemas_cloned = column_schemas.clone();
    let engine_cloned = engine.clone();
    let put_job = tokio::spawn(async move {
        let rows = Rows {
            schema: column_schemas_cloned,
            rows: build_rows(0, 3),
        };
        put_rows(&engine_cloned, region_id, rows).await;
    });
    // Make sure the loop is handling the put request.
    tokio::time::sleep(Duration::from_millis(100)).await;

    listener.wake_notify();
    alter_job.await.unwrap();
    put_job.await.unwrap();

    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    let request = ScanRequest::default();
    let scanner = engine.scanner(region_id, request).await.unwrap();
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_enter_staging_waits_for_pending_checkpoint() {
    for block_last_checkpoint_write in [false, true] {
        let partition_directive =
            StagingPartitionDirective::UpdatePartitionExpr(default_partition_expr());
        let (blocker, mock_layer) = if block_last_checkpoint_write {
            CheckpointTaskBlocker::block_last_checkpoint_write()
        } else {
            CheckpointTaskBlocker::block_cleanup()
        };
        let mut env = TestEnv::new().await.with_mock_layer(mock_layer);
        let engine = env
            .create_engine(MitoConfig {
                manifest_checkpoint_distance: 1,
                ..Default::default()
            })
            .await;
        let region_id = RegionId::new(1, 1);
        let request = CreateRequestBuilder::new().build();
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
                rows: build_rows(0, 1),
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
        tokio::time::timeout(Duration::from_secs(5), blocker.wait_until_blocked())
            .await
            .expect("checkpoint task did not reach the configured block point");

        let region = engine.get_region(region_id).unwrap();
        let wait_started = region
            .manifest_ctx
            .manifest_manager
            .read()
            .await
            .checkpointer()
            .pending_checkpoint_wait_started();
        // Register before starting the transition so the notification cannot be lost.
        let wait_started = wait_started.notified();
        let cloned_engine = engine.clone();
        let enter_staging = tokio::spawn(async move {
            cloned_engine
                .handle_request(
                    region_id,
                    RegionRequest::EnterStaging(EnterStagingRequest {
                        partition_directive,
                    }),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), wait_started)
            .await
            .expect("EnterStaging did not start waiting for the checkpoint");
        assert_eq!(
            RegionRoleState::Leader(RegionLeaderState::EnteringStaging),
            region.state()
        );
        assert!(
            !enter_staging.is_finished(),
            "EnterStaging returned before the checkpoint task finished"
        );

        blocker.release();
        enter_staging.await.unwrap().unwrap();
        assert_eq!(
            RegionRoleState::Leader(RegionLeaderState::Staging),
            region.state()
        );
        assert!(
            !region
                .manifest_ctx
                .manifest_manager
                .read()
                .await
                .checkpointer()
                .is_doing_checkpoint()
        );
    }
}

#[tokio::test]
async fn test_enter_staging_clean_staging_manifest_error() {
    common_telemetry::init_default_ut_logging();
    test_enter_staging_clean_staging_manifest_error_with_format(false).await;
    test_enter_staging_clean_staging_manifest_error_with_format(true).await;
}

struct MockLister {
    path: String,
    inner: Lister,
}

impl List for MockLister {
    async fn next(&mut self) -> MockResult<Option<Entry>> {
        if self.path.contains("staging") {
            return Err(MockError::new(ErrorKind::Unexpected, "mock error"));
        }
        self.inner.next().await
    }
}

/// Fails closing Parquet files while `fail_parquet` is set.
struct ParquetFailWriter {
    path: String,
    inner: Writer,
    fail_parquet: Arc<AtomicBool>,
}

impl Write for ParquetFailWriter {
    async fn write(&mut self, bs: Buffer) -> MockResult<()> {
        self.inner.write(bs).await
    }

    async fn close(&mut self) -> MockResult<Metadata> {
        if self.fail_parquet.load(Ordering::Relaxed) && self.path.ends_with(".parquet") {
            return Err(MockError::new(ErrorKind::Unexpected, "mock error"));
        }
        self.inner.close().await
    }

    async fn abort(&mut self) -> MockResult<()> {
        self.inner.abort().await
    }
}

struct MockDeleter {
    inner: Deleter,
}

impl Delete for MockDeleter {
    async fn delete(&mut self, path: &str, args: OpDelete) -> MockResult<()> {
        if path.contains("staging") {
            return Err(MockError::new(ErrorKind::Unexpected, "mock error"));
        }
        self.inner.delete(path, args).await
    }

    async fn close(&mut self) -> MockResult<()> {
        self.inner.close().await
    }
}

async fn test_enter_staging_error(env: &mut TestEnv, flat_format: bool) {
    let partition_expr = default_partition_expr();
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;
    let region_id = RegionId::new(1024, 0);
    let request = CreateRequestBuilder::new().build();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let err = engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::UpdatePartitionExpr(
                    partition_expr.clone(),
                ),
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(err.status_code(), StatusCode::StorageUnavailable);
    let region = engine.get_region(region_id).unwrap();
    assert!(
        region
            .manifest_ctx
            .manifest_manager
            .read()
            .await
            .staging_manifest()
            .is_none()
    );
    let state = region.state();
    assert_eq!(state, RegionRoleState::Leader(RegionLeaderState::Writable));
}

async fn test_enter_staging_clean_staging_manifest_error_with_format(flat_format: bool) {
    let mock_layer = MockLayerBuilder::default()
        .lister_factory(Arc::new(|path, _args, lister| {
            Box::new(MockLister {
                path: path.to_string(),
                inner: lister,
            })
        }))
        .deleter_factory(Arc::new(|deleter| Box::new(MockDeleter { inner: deleter })))
        .build()
        .unwrap();
    let mut env = TestEnv::new().await.with_mock_layer(mock_layer);
    test_enter_staging_error(&mut env, flat_format).await;
}

async fn enter_staging(engine: &MitoEngine, region_id: RegionId) {
    engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::UpdatePartitionExpr(
                    float_range_expr("field_0", 0., 100.).as_json_str().unwrap(),
                ),
            }),
        )
        .await
        .unwrap();
}

/// Creates a region and enters staging, returning its schema and table dir.
async fn create_staging_region(
    engine: &MitoEngine,
    region_id: RegionId,
) -> (Vec<ColumnSchema>, String) {
    let request = CreateRequestBuilder::new().build();
    let schema = rows_schema(&request);
    let table_dir = request.table_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    enter_staging(engine, region_id).await;
    (schema, table_dir)
}

async fn create_writable_region(engine: &MitoEngine, region_id: RegionId) {
    engine
        .handle_request(
            region_id,
            RegionRequest::Create(CreateRequestBuilder::new().build()),
        )
        .await
        .unwrap();
}

async fn put_range(
    engine: &MitoEngine,
    region_id: RegionId,
    schema: &[ColumnSchema],
    range: (usize, usize),
) {
    let rows = Rows {
        schema: schema.to_vec(),
        rows: build_rows(range.0, range.1),
    };
    put_rows(engine, region_id, rows).await;
}

/// Sends a put that the engine is expected to refuse with a retryable error.
async fn assert_put_rejected(engine: &MitoEngine, region_id: RegionId, schema: &[ColumnSchema]) {
    let err = engine
        .handle_request(
            region_id,
            RegionRequest::Put(RegionPutRequest {
                rows: Rows {
                    schema: schema.to_vec(),
                    rows: build_rows(50, 51),
                },
                hint: None,
                partition_expr_version: None,
                skip_wal: false,
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(RetryHint::Retryable, err.retry_hint());
}

async fn put_range_skip_wal(
    engine: &MitoEngine,
    region_id: RegionId,
    schema: &[ColumnSchema],
    range: (usize, usize),
) {
    engine
        .handle_request(
            region_id,
            RegionRequest::Put(RegionPutRequest {
                rows: Rows {
                    schema: schema.to_vec(),
                    rows: build_rows(range.0, range.1),
                },
                hint: None,
                partition_expr_version: None,
                skip_wal: true,
            }),
        )
        .await
        .unwrap();
}

/// Returns one of the files in the region's manifest.
async fn first_file(region: &MitoRegionRef) -> FileMeta {
    let manifest = region.manifest_ctx.manifest().await;
    manifest.files.values().next().unwrap().clone()
}

fn edit_adding(file: FileMeta) -> RegionEdit {
    RegionEdit {
        files_to_add: vec![file],
        files_to_remove: vec![],
        timestamp_ms: None,
        compaction_time_window: None,
        flushed_entry_id: None,
        flushed_sequence: None,
        committed_sequence: None,
        wal_recovery: Default::default(),
        last_invalid_seq: None,
    }
}

/// Spawns an edit importing `file`.
fn spawn_edit(
    engine: &MitoEngine,
    region_id: RegionId,
    file: FileMeta,
) -> tokio::task::JoinHandle<crate::error::Result<()>> {
    let edit_engine = engine.clone();
    tokio::spawn(async move { edit_engine.edit_region(region_id, edit_adding(file)).await })
}

/// Starts an edit importing `file` and waits until the region is `Editing`.
/// The caller holds the manifest write lock so that the edit cannot finish.
async fn start_staged_edit(
    engine: &MitoEngine,
    region: &MitoRegionRef,
    file: FileMeta,
) -> tokio::task::JoinHandle<crate::error::Result<()>> {
    let edit = spawn_edit(engine, region.region_id, file);
    tokio::time::timeout(Duration::from_secs(5), async {
        while region.state() != RegionRoleState::Leader(RegionLeaderState::Editing) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    edit
}

async fn count_rows(engine: &MitoEngine, region_id: RegionId) -> usize {
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(scanner.scan().await.unwrap())
        .await
        .unwrap();
    batches.iter().map(|batch| batch.num_rows()).sum()
}

/// Returns the numbers of SST files and memtables that a scan reads.
async fn scan_inputs(engine: &MitoEngine, region_id: RegionId) -> (usize, usize) {
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    (scanner.num_files(), scanner.num_memtables())
}

/// Asserts that no flush has been scheduled for the rows in the region's memtable.
async fn assert_unflushed(engine: &MitoEngine, region_id: RegionId) {
    let version = engine.get_region(region_id).unwrap().version();
    assert!(version.memtables.immutables().is_empty());
    assert_eq!((0, 1), scan_inputs(engine, region_id).await);
}

#[tokio::test]
async fn test_memory_pressure_keeps_staging_rows_after_rollback() {
    let mut write_buffer_manager = MockWriteBufferManager::default();
    write_buffer_manager.set_flush_limit(1 << 20);
    write_buffer_manager.reserve_mem(1 << 25);
    let write_buffer_manager = Arc::new(write_buffer_manager);
    let listener = Arc::new(FlushListener::default());
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine_with(
            MitoConfig {
                num_workers: 1,
                ..Default::default()
            },
            Some(write_buffer_manager.clone()),
            Some(listener.clone()),
            None,
        )
        .await;
    let staging_region_id = RegionId::new(1024, 0);
    let writable_region_id = RegionId::new(1024, 1);
    let (schema, table_dir) = create_staging_region(&engine, staging_region_id).await;
    create_writable_region(&engine, writable_region_id).await;
    put_range(&engine, writable_region_id, &schema, (0, 3)).await;
    put_range(&engine, staging_region_id, &schema, (0, 3)).await;

    write_buffer_manager.set_should_flush(true);
    put_range(&engine, staging_region_id, &schema, (3, 5)).await;
    listener.wait().await;
    write_buffer_manager.set_should_flush(false);

    assert_eq!((1, 0), scan_inputs(&engine, writable_region_id).await);
    assert_unflushed(&engine, staging_region_id).await;

    engine
        .set_region_role(staging_region_id, RegionRole::Leader)
        .unwrap();
    assert_eq!(5, count_rows(&engine, staging_region_id).await);
    reopen_region(&engine, staging_region_id, table_dir, true, HashMap::new()).await;
    assert_eq!(5, count_rows(&engine, staging_region_id).await);
}

#[tokio::test]
async fn test_periodic_flush_skips_staging_region() {
    let listener = Arc::new(FlushListener::default());
    let now = current_time_millis();
    let time_provider = Arc::new(MockTimeProvider::new(now));
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine_with_time(
            MitoConfig {
                auto_flush_interval: Duration::from_secs(300),
                num_workers: 1,
                ..Default::default()
            },
            None,
            Some(listener.clone()),
            time_provider.clone(),
        )
        .await;
    let staging_region_id = RegionId::new(1024, 0);
    let writable_region_id = RegionId::new(1024, 1);
    let (schema, _) = create_staging_region(&engine, staging_region_id).await;
    create_writable_region(&engine, writable_region_id).await;
    put_range(&engine, staging_region_id, &schema, (0, 3)).await;
    put_range(&engine, writable_region_id, &schema, (0, 3)).await;

    time_provider.set_now(now + 600_000);
    time_provider.set_elapsed((MAX_INITIAL_CHECK_DELAY_SECS as i64 + 1) * 1000);
    tokio::time::timeout(Duration::from_secs(3), listener.wait())
        .await
        .unwrap();

    assert_eq!((1, 0), scan_inputs(&engine, writable_region_id).await);
    assert_unflushed(&engine, staging_region_id).await;
}

#[tokio::test]
async fn test_region_write_buffer_limit_rejects_staging_writes_without_flush() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1024, 0);
    let request = CreateRequestBuilder::new()
        .insert_option(WRITE_BUFFER_SIZE_KEY, "1B")
        .build();
    let schema = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    enter_staging(&engine, region_id).await;

    put_range(&engine, region_id, &schema, (0, 3)).await;
    assert_put_rejected(&engine, region_id, &schema).await;

    assert_unflushed(&engine, region_id).await;
}

#[tokio::test]
async fn test_region_write_buffer_limit_does_not_queue_staging_writes() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let probe_region_id = RegionId::new(1024, 0);
    let (schema, _) = create_staging_region(&engine, probe_region_id).await;
    put_range(&engine, probe_region_id, &schema, (0, 3)).await;
    let usage = engine
        .get_region(probe_region_id)
        .unwrap()
        .region_statistic()
        .memtable_size;

    let region_id = RegionId::new(1024, 1);
    let request = CreateRequestBuilder::new()
        .insert_option(WRITE_BUFFER_SIZE_KEY, &format!("{usage}B"))
        .build();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    enter_staging(&engine, region_id).await;
    put_range(&engine, region_id, &schema, (0, 3)).await;

    tokio::time::timeout(
        Duration::from_secs(5),
        put_range(&engine, region_id, &schema, (3, 6)),
    )
    .await
    .unwrap();
    assert_unflushed(&engine, region_id).await;
    assert_eq!(6, count_rows(&engine, region_id).await);
}

#[tokio::test]
async fn test_staging_memtable_cap_rejects_only_staging_writes() {
    let mut write_buffer_manager = MockWriteBufferManager::default();
    // Staging writes are rejected once the node's staging memtables hold one byte.
    write_buffer_manager.set_flush_limit(2);
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine_with(
            MitoConfig {
                num_workers: 2,
                ..Default::default()
            },
            Some(Arc::new(write_buffer_manager)),
            None,
            None,
        )
        .await;
    // The two staging regions belong to different workers.
    let first_region_id = RegionId::new(1024, 0);
    let second_region_id = RegionId::new(1024, 1);
    let writable_region_id = RegionId::new(1024, 2);
    let (schema, _) = create_staging_region(&engine, first_region_id).await;
    create_staging_region(&engine, second_region_id).await;
    create_writable_region(&engine, writable_region_id).await;

    put_range(&engine, first_region_id, &schema, (0, 3)).await;
    assert_put_rejected(&engine, second_region_id, &schema).await;
    put_range(&engine, writable_region_id, &schema, (0, 3)).await;

    engine
        .set_region_role(first_region_id, RegionRole::Leader)
        .unwrap();
    put_range(&engine, second_region_id, &schema, (0, 3)).await;
}

#[tokio::test]
async fn test_staging_memtable_cap_counts_region_during_staged_edit() {
    let mut write_buffer_manager = MockWriteBufferManager::default();
    write_buffer_manager.set_flush_limit(2);
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine_with(
            MitoConfig {
                num_workers: 2,
                ..Default::default()
            },
            Some(Arc::new(write_buffer_manager)),
            None,
            None,
        )
        .await;
    let edited_region_id = RegionId::new(1024, 0);
    let other_region_id = RegionId::new(1024, 1);
    let request = CreateRequestBuilder::new().build();
    let schema = rows_schema(&request);
    engine
        .handle_request(edited_region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    put_range(&engine, edited_region_id, &schema, (0, 3)).await;
    flush_region(&engine, edited_region_id, None).await;
    enter_staging(&engine, edited_region_id).await;
    put_range(&engine, edited_region_id, &schema, (3, 5)).await;
    create_staging_region(&engine, other_region_id).await;

    let region = engine.get_region(edited_region_id).unwrap();
    let file = first_file(&region).await;
    let manager = region.manifest_ctx.manifest_manager.write().await;
    let edit = start_staged_edit(&engine, &region, file).await;
    assert_put_rejected(&engine, other_region_id, &schema).await;

    drop(manager);
    edit.await.unwrap().unwrap();
    assert!(region.is_staging());
    assert_put_rejected(&engine, other_region_id, &schema).await;
}

#[tokio::test]
async fn test_flush_request_on_staging_region_with_data_is_retryable() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1024, 0);
    let (schema, _) = create_staging_region(&engine, region_id).await;
    flush_region(&engine, region_id, None).await;
    put_range(&engine, region_id, &schema, (0, 3)).await;

    for reason in [None, Some(RegionFlushReason::Repartition)] {
        let err = engine
            .handle_request(
                region_id,
                RegionRequest::Flush(RegionFlushRequest {
                    reason,
                    ..Default::default()
                }),
            )
            .await
            .unwrap_err();
        assert_eq!(RetryHint::Retryable, err.retry_hint());
        assert_matches!(
            err.into_inner().as_any().downcast_ref::<Error>().unwrap(),
            Error::RegionBusy { .. }
        );
    }

    assert_unflushed(&engine, region_id).await;
}

#[tokio::test]
async fn test_migration_and_wal_prune_flushes_leave_staging_region_unflushed() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1024, 0);
    let (schema, _) = create_staging_region(&engine, region_id).await;
    put_range(&engine, region_id, &schema, (0, 3)).await;

    for reason in [
        RegionFlushReason::RegionMigration,
        RegionFlushReason::RemoteWalPrune,
    ] {
        engine
            .handle_request(
                region_id,
                RegionRequest::Flush(RegionFlushRequest {
                    reason: Some(reason),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
    }

    assert_unflushed(&engine, region_id).await;
}

#[tokio::test]
async fn test_successful_staging_end_keeps_rows_of_rejected_flush() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1024, 0);
    let request = CreateRequestBuilder::new().build();
    let schema = rows_schema(&request);
    let table_dir = request.table_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    put_range(&engine, region_id, &schema, (0, 3)).await;
    enter_staging(&engine, region_id).await;
    put_range(&engine, region_id, &schema, (3, 5)).await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest::default()),
        )
        .await
        .unwrap_err();

    let imported_file = first_file(&engine.get_region(region_id).unwrap()).await;
    engine
        .edit_region(
            region_id,
            RegionEdit {
                files_to_add: vec![imported_file],
                files_to_remove: vec![],
                timestamp_ms: None,
                compaction_time_window: None,
                flushed_entry_id: None,
                flushed_sequence: None,
                committed_sequence: None,
                wal_recovery: Default::default(),
                last_invalid_seq: None,
            },
        )
        .await
        .unwrap();
    engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Leader)
        .await
        .unwrap();

    assert_eq!(5, count_rows(&engine, region_id).await);
    reopen_region(&engine, region_id, table_dir, true, HashMap::new()).await;
    assert_eq!(5, count_rows(&engine, region_id).await);
}

#[tokio::test]
async fn test_downgrade_flushes_staging_rows_to_normal_manifest() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1024, 0);
    let (schema, table_dir) = create_staging_region(&engine, region_id).await;
    put_range(&engine, region_id, &schema, (0, 3)).await;

    engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::DowngradingLeader)
        .await
        .unwrap();
    flush_region(&engine, region_id, None).await;

    assert_eq!((1, 0), scan_inputs(&engine, region_id).await);
    reopen_region(&engine, region_id, table_dir, true, HashMap::new()).await;
    assert_eq!(3, count_rows(&engine, region_id).await);
}

#[tokio::test]
async fn test_staging_rows_survive_restart() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1024, 0);
    let (schema, table_dir) = create_staging_region(&engine, region_id).await;
    put_range(&engine, region_id, &schema, (0, 3)).await;

    let engine = env.reopen_engine(engine, MitoConfig::default()).await;
    reopen_region(&engine, region_id, table_dir, true, HashMap::new()).await;

    assert_eq!(3, count_rows(&engine, region_id).await);
}

#[tokio::test]
async fn test_close_flushes_skip_wal_rows_of_staging_region() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1024, 0);
    let (schema, table_dir) = create_staging_region(&engine, region_id).await;
    put_range_skip_wal(&engine, region_id, &schema, (0, 3)).await;

    reopen_region(&engine, region_id, table_dir, true, HashMap::new()).await;

    assert_eq!((1, 0), scan_inputs(&engine, region_id).await);
    assert_eq!(3, count_rows(&engine, region_id).await);
}

#[derive(Default)]
struct RequestCountListener {
    count: AtomicUsize,
    /// A flush waits here before it starts while a test holds the write side.
    flush_gate: tokio::sync::RwLock<()>,
}

impl RequestCountListener {
    /// Waits until a worker has received a request batch after the count reached `received`.
    async fn wait_received_after(&self, received: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.count.load(Ordering::Relaxed) == received {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
    }
}

#[async_trait]
impl EventListener for RequestCountListener {
    fn on_recv_requests(&self, request_num: usize) {
        self.count.fetch_add(request_num, Ordering::Relaxed);
    }

    async fn on_flush_begin(&self, _region_id: RegionId) {
        let _open = self.flush_gate.read().await;
    }
}

#[tokio::test]
async fn test_edit_returns_to_staging_after_staging_leader_role() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1024, 0);
    let request = CreateRequestBuilder::new().build();
    let schema = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    put_range(&engine, region_id, &schema, (0, 3)).await;
    flush_region(&engine, region_id, None).await;
    engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::StagingLeader)
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    let file = first_file(&region).await;
    spawn_edit(&engine, region_id, file).await.unwrap().unwrap();

    assert!(region.is_staging());
}

#[tokio::test]
async fn test_staged_files_not_listed_after_leaving_staging() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1024, 0);
    let request = CreateRequestBuilder::new().build();
    let schema = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    put_range(&engine, region_id, &schema, (0, 3)).await;
    flush_region(&engine, region_id, None).await;
    enter_staging(&engine, region_id).await;
    let region = engine.get_region(region_id).unwrap();
    let file = FileMeta {
        file_id: FileId::random(),
        ..first_file(&region).await
    };
    spawn_edit(&engine, region_id, file).await.unwrap().unwrap();
    assert_eq!(2, region.manifest_sst_entries().await.len());
    assert_eq!(2, region.all_manifest_files().await.0.len());

    // Leaving staging without a commit keeps the staged file out of a later Leader's listing.
    region.set_role(RegionRole::Follower);
    region.set_role(RegionRole::Leader);
    assert_eq!(1, region.manifest_sst_entries().await.len());
    assert_eq!(1, region.all_manifest_files().await.0.len());
}

#[tokio::test]
async fn test_close_time_flush_rejects_queued_and_late_edits() {
    let mut env = TestEnv::new().await;
    let listener = Arc::new(RequestCountListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig {
                num_workers: 1,
                ..Default::default()
            },
            None,
            Some(listener.clone()),
            None,
        )
        .await;
    let region_id = RegionId::new(1024, 0);
    let request = CreateRequestBuilder::new().build();
    let schema = rows_schema(&request);
    let table_dir = request.table_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    put_range(&engine, region_id, &schema, (0, 3)).await;
    flush_region(&engine, region_id, None).await;
    enter_staging(&engine, region_id).await;
    put_range_skip_wal(&engine, region_id, &schema, (3, 5)).await;

    let region = engine.get_region(region_id).unwrap();
    let file = first_file(&region).await;
    let queued_file = FileMeta {
        file_id: FileId::random(),
        ..file.clone()
    };
    let late_file = FileMeta {
        file_id: FileId::random(),
        ..file.clone()
    };
    let flush_gate = listener.flush_gate.write().await;
    let manager = region.manifest_ctx.manifest_manager.write().await;
    let edit = start_staged_edit(&engine, &region, file).await;

    let received = listener.count.load(Ordering::Relaxed);
    let queued = spawn_edit(&engine, region_id, queued_file.clone());
    listener.wait_received_after(received).await;

    let received = listener.count.load(Ordering::Relaxed);
    let close_engine = engine.clone();
    let close = tokio::spawn(async move {
        close_engine
            .handle_request(
                region_id,
                RegionRequest::Close(RegionCloseRequest::default()),
            )
            .await
    });
    listener.wait_received_after(received).await;
    drop(manager);

    // The close-time flush is held back, so both edits arrive while the region waits for it.
    let _ = edit.await;
    let err = queued.await.unwrap().unwrap_err();
    assert_eq!(StatusCode::RegionNotFound, err.status_code());
    let err = spawn_edit(&engine, region_id, late_file.clone())
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(StatusCode::RegionBusy, err.status_code());

    drop(flush_gate);
    close.await.unwrap().unwrap();

    reopen_region(&engine, region_id, table_dir, true, HashMap::new()).await;
    let region = engine.get_region(region_id).unwrap();
    let manifest = region.manifest_ctx.manifest_manager.read().await.manifest();
    assert!(!manifest.files.contains_key(&queued_file.file_id));
    assert!(!manifest.files.contains_key(&late_file.file_id));
    assert_eq!(5, count_rows(&engine, region_id).await);
}

#[tokio::test]
async fn test_stale_staging_edit_rejected_after_close_time_flush_fails() {
    let fail_parquet = Arc::new(AtomicBool::new(false));
    let writer_flag = fail_parquet.clone();
    let mock_layer = MockLayerBuilder::default()
        .writer_factory(Arc::new(move |path, _args, inner| {
            Box::new(ParquetFailWriter {
                path: path.to_string(),
                inner,
                fail_parquet: writer_flag.clone(),
            })
        }))
        .build()
        .unwrap();
    let mut env = TestEnv::new().await.with_mock_layer(mock_layer);
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1024, 0);
    let request = CreateRequestBuilder::new().build();
    let schema = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    put_range(&engine, region_id, &schema, (0, 3)).await;
    flush_region(&engine, region_id, None).await;
    enter_staging(&engine, region_id).await;
    put_range_skip_wal(&engine, region_id, &schema, (3, 5)).await;

    fail_parquet.store(true, Ordering::Relaxed);
    engine
        .handle_request(
            region_id,
            RegionRequest::Close(RegionCloseRequest::default()),
        )
        .await
        .unwrap_err();

    // The close failed, so the region is open, writable and no longer in staging.
    let region = engine.get_region(region_id).unwrap();
    assert_eq!(
        RegionRoleState::Leader(RegionLeaderState::Writable),
        region.state()
    );
    let file = FileMeta {
        file_id: FileId::random(),
        ..first_file(&region).await
    };
    let file_id = file.file_id;
    let stale_expr = float_range_expr("field_0", 0., 100.).as_json_str().unwrap();
    let result = submit_staging_edit(&engine, region_id, file.clone(), stale_expr.clone()).await;
    assert_matches!(result.unwrap_err(), Error::RegionState { .. });

    // A later staging for another partition expression does not take the stale edit either.
    fail_parquet.store(false, Ordering::Relaxed);
    engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::UpdatePartitionExpr(
                    float_range_expr("field_0", 0., 50.).as_json_str().unwrap(),
                ),
            }),
        )
        .await
        .unwrap();
    let result = submit_staging_edit(&engine, region_id, file, stale_expr).await;
    assert_matches!(result.unwrap_err(), Error::RegionState { .. });

    let manager = region.manifest_ctx.manifest_manager.read().await;
    assert!(!manager.manifest().files.contains_key(&file_id));
    assert!(
        manager
            .staging_manifest()
            .is_none_or(|staging| !staging.files.contains_key(&file_id))
    );
}

async fn submit_staging_edit(
    engine: &MitoEngine,
    region_id: RegionId,
    file: FileMeta,
    partition_expr: String,
) -> crate::error::Result<()> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    engine
        .inner
        .workers
        .submit_to_worker(
            region_id,
            WorkerRequest::EditRegion(
                RegionEditRequest::new(region_id, edit_adding(file), false, tx)
                    .for_staging(partition_expr),
            ),
        )
        .await
        .unwrap();
    rx.await.unwrap()
}

#[tokio::test]
async fn test_close_during_staged_edit_flushes_skip_wal_rows() {
    let mut env = TestEnv::new().await;
    let listener = Arc::new(RequestCountListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig {
                num_workers: 1,
                ..Default::default()
            },
            None,
            Some(listener.clone()),
            None,
        )
        .await;
    let region_id = RegionId::new(1024, 0);
    let request = CreateRequestBuilder::new().build();
    let schema = rows_schema(&request);
    let table_dir = request.table_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    put_range(&engine, region_id, &schema, (0, 3)).await;
    flush_region(&engine, region_id, None).await;
    enter_staging(&engine, region_id).await;
    put_range_skip_wal(&engine, region_id, &schema, (3, 5)).await;

    let region = engine.get_region(region_id).unwrap();
    let file = first_file(&region).await;
    let manager = region.manifest_ctx.manifest_manager.write().await;
    let edit = start_staged_edit(&engine, &region, file).await;
    let received = listener.count.load(Ordering::Relaxed);
    let close_engine = engine.clone();
    let close = tokio::spawn(async move {
        close_engine
            .handle_request(
                region_id,
                RegionRequest::Close(RegionCloseRequest::default()),
            )
            .await
    });
    // The worker has the close request before the edit, still waiting for the lock, can finish.
    listener.wait_received_after(received).await;
    drop(manager);
    close.await.unwrap().unwrap();
    let _ = edit.await;

    reopen_region(&engine, region_id, table_dir, true, HashMap::new()).await;
    assert_eq!(5, count_rows(&engine, region_id).await);
}

/// Waits until a writer is queued on the region's manifest lock while the caller holds it shared.
async fn wait_for_queued_manifest_writer(region: &MitoRegionRef) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while region.manifest_ctx.manifest_manager.try_read().is_ok() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_close_waits_for_manifest_lock_before_leaving_staging() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1024, 0);
    create_staging_region(&engine, region_id).await;
    let region = engine.get_region(region_id).unwrap();

    let manager = region.manifest_ctx.manifest_manager.read().await;
    let close_engine = engine.clone();
    let close = tokio::spawn(async move {
        close_engine
            .handle_request(
                region_id,
                RegionRequest::Close(RegionCloseRequest::default()),
            )
            .await
    });
    wait_for_queued_manifest_writer(&region).await;
    assert!(region.is_staging());

    drop(manager);
    close.await.unwrap().unwrap();
    assert!(!region.is_staging());
}

#[tokio::test]
async fn test_close_after_end_of_staging_is_queued_keeps_committed_result() {
    let mut env = TestEnv::new().await;
    let listener = Arc::new(RequestCountListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig {
                num_workers: 1,
                ..Default::default()
            },
            None,
            Some(listener.clone()),
            None,
        )
        .await;
    let region_id = RegionId::new(1024, 0);
    let request = CreateRequestBuilder::new().build();
    let schema = rows_schema(&request);
    let table_dir = request.table_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    put_range(&engine, region_id, &schema, (0, 3)).await;
    enter_staging(&engine, region_id).await;
    put_range(&engine, region_id, &schema, (3, 5)).await;
    let region = engine.get_region(region_id).unwrap();
    let file = first_file(&region).await;
    engine
        .edit_region(
            region_id,
            RegionEdit {
                files_to_add: vec![file],
                files_to_remove: vec![],
                timestamp_ms: None,
                compaction_time_window: None,
                flushed_entry_id: None,
                flushed_sequence: None,
                committed_sequence: None,
                wal_recovery: Default::default(),
                last_invalid_seq: None,
            },
        )
        .await
        .unwrap();

    let manager = region.manifest_ctx.manifest_manager.read().await;
    let end_engine = engine.clone();
    let end = tokio::spawn(async move {
        end_engine
            .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Leader)
            .await
    });
    wait_for_queued_manifest_writer(&region).await;
    let received = listener.count.load(Ordering::Relaxed);
    let close_engine = engine.clone();
    let close = tokio::spawn(async move {
        close_engine
            .handle_request(
                region_id,
                RegionRequest::Close(RegionCloseRequest::default()),
            )
            .await
    });
    listener.wait_received_after(received).await;
    drop(manager);

    end.await.unwrap().unwrap();
    close.await.unwrap().unwrap();
    reopen_region(&engine, region_id, table_dir, true, HashMap::new()).await;
    let reopened = engine.get_region(region_id).unwrap();
    assert_eq!(
        Some(float_range_expr("field_0", 0., 100.).as_json_str().unwrap()),
        reopened.metadata().partition_expr
    );
    assert_eq!(5, count_rows(&engine, region_id).await);
}

#[rstest::rstest]
#[case::committed(false)]
#[case::downgraded(true)]
#[tokio::test]
async fn test_writes_wait_while_staging_ends(#[case] downgrade: bool) {
    let mut env = TestEnv::new().await;
    let listener = Arc::new(CountingStallListener::default());
    let engine = env
        .create_engine_with(MitoConfig::default(), None, Some(listener.clone()), None)
        .await;
    let region_id = RegionId::new(1024, 0);
    let (schema, _) = create_staging_region(&engine, region_id).await;
    let region = engine.get_region(region_id).unwrap();
    let started = region.manifest_ctx.begin_ending_staging(region_id).unwrap();

    let put_engine = engine.clone();
    let put = tokio::spawn(async move {
        put_engine
            .handle_request(
                region_id,
                RegionRequest::Put(RegionPutRequest {
                    rows: Rows {
                        schema,
                        rows: build_rows(0, 3),
                    },
                    hint: None,
                    partition_expr_version: None,
                    skip_wal: false,
                }),
            )
            .await
    });
    wait_for_stalls(&listener, 1).await;
    assert!(region.version().memtables.is_empty());

    if downgrade {
        region.manifest_ctx.begin_downgrade(region_id);
    }
    region
        .manifest_ctx
        .leave_ending_staging(region_id, started, &Ok::<(), Error>(()));
    send_staging_ended(&engine, region_id).await;

    // The drained write sees the state that the end of staging left: a downgrade refuses it.
    let result = put.await.unwrap();
    assert_eq!(downgrade, result.is_err());
    assert_eq!(downgrade, region.version().memtables.is_empty());
}

#[tokio::test]
async fn test_writes_after_staging_ends_wait_for_stalled_writes() {
    let mut env = TestEnv::new().await;
    let listener = Arc::new(CountingStallListener::default());
    let engine = env
        .create_engine_with(MitoConfig::default(), None, Some(listener.clone()), None)
        .await;
    let region_id = RegionId::new(1024, 0);
    let (schema, _) = create_staging_region(&engine, region_id).await;
    let region = engine.get_region(region_id).unwrap();
    let started = region.manifest_ctx.begin_ending_staging(region_id).unwrap();
    let put_engine = engine.clone();
    let rows = Rows {
        schema,
        rows: build_rows(0, 3),
    };
    let put = tokio::spawn(async move { put_rows(&put_engine, region_id, rows).await });
    wait_for_stalls(&listener, 1).await;

    // A delete of the same rows that arrives after staging ended, before the worker drains the
    // stalled put, waits behind it.
    region
        .manifest_ctx
        .leave_ending_staging(region_id, started, &Ok::<(), Error>(()));
    let delete_engine = engine.clone();
    let rows = Rows {
        schema: delete_rows_schema(&CreateRequestBuilder::new().build()),
        rows: build_delete_rows(0, 3),
    };
    let delete = tokio::spawn(async move { delete_rows(&delete_engine, region_id, rows).await });
    wait_for_stalls(&listener, 2).await;
    send_staging_ended(&engine, region_id).await;
    put.await.unwrap();
    delete.await.unwrap();

    assert_eq!(0, count_rows(&engine, region_id).await);
}

#[tokio::test]
async fn test_bulk_writes_after_staging_ends_wait_for_stalled_writes() {
    let mut env = TestEnv::new().await;
    let listener = Arc::new(CountingStallListener::default());
    let engine = env
        .create_engine_with(MitoConfig::default(), None, Some(listener.clone()), None)
        .await;
    let region_id = RegionId::new(1024, 0);
    create_staging_region(&engine, region_id).await;
    let region = engine.get_region(region_id).unwrap();
    let started = region.manifest_ctx.begin_ending_staging(region_id).unwrap();
    let bulk_insert = |num_rows| {
        let engine = engine.clone();
        let request = RegionRequest::BulkInserts(build_bulk_insert_request(region_id, num_rows));
        tokio::spawn(async move { engine.handle_request(region_id, request).await })
    };
    let first = bulk_insert(3);
    wait_for_stalls(&listener, 1).await;

    // A bulk write that arrives after staging ended, before the worker drains the stalled one,
    // waits behind it.
    region
        .manifest_ctx
        .leave_ending_staging(region_id, started, &Ok::<(), Error>(()));
    let second = bulk_insert(5);
    wait_for_stalls(&listener, 2).await;
    assert!(region.version().memtables.is_empty());
    send_staging_ended(&engine, region_id).await;
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();

    assert_eq!(5, count_rows(&engine, region_id).await);
}

/// Waits until the engine of `listener` has stalled writes `n` times.
async fn wait_for_stalls(listener: &CountingStallListener, n: usize) {
    tokio::time::timeout(Duration::from_secs(5), listener.wait_for_count(n))
        .await
        .expect("the writes were not stalled");
}

/// Has the worker handle the writes it stalled while the region was ending staging.
async fn send_staging_ended(engine: &MitoEngine, region_id: RegionId) {
    let (drained, wait) = tokio::sync::oneshot::channel();
    engine
        .inner
        .workers
        .submit_to_worker(
            region_id,
            WorkerRequest::Background {
                region_id,
                notify: BackgroundNotify::StagingEnded(drained),
            },
        )
        .await
        .unwrap();
    wait.await.unwrap();
}

#[tokio::test]
async fn test_migration_flush_retries_while_staging_ends() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1024, 0);
    let (schema, _) = create_staging_region(&engine, region_id).await;
    put_range(&engine, region_id, &schema, (0, 3)).await;
    let region = engine.get_region(region_id).unwrap();
    let migration_flush = || {
        engine.handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest {
                reason: Some(RegionFlushReason::RegionMigration),
                ..Default::default()
            }),
        )
    };

    let started = region.manifest_ctx.begin_ending_staging(region_id).unwrap();
    assert_eq!(Some(RegionRole::StagingLeader), engine.role(region_id));
    let error = migration_flush().await.unwrap_err();
    assert_eq!(RetryHint::Retryable, error.retry_hint());

    region
        .manifest_ctx
        .leave_ending_staging(region_id, started, &Ok::<(), Error>(()));
    assert_eq!(0, migration_flush().await.unwrap().affected_rows);
}

/// Waits until the region reaches `state`.
async fn wait_until_region_state(region: &MitoRegionRef, state: RegionLeaderState) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while region.state() != RegionRoleState::Leader(state) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("region did not reach the expected state in time");
}

/// Returns the admission barrier of `file_id` in the region's current SST version.
fn file_sequence(region: &MitoRegionRef, file_id: FileId) -> Option<u64> {
    region
        .version()
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files.iter())
        .find(|(id, _)| **id == file_id)
        .and_then(|(_, file)| file.meta_ref().sequence)
        .map(|sequence| sequence.get())
}

/// The staging exit holds the worker-coordinated publish window while it allocates the
/// boundary, persists the merged layout and makes it visible. Ordinary writes that arrive
/// during the window are queued by the worker, so none of them can be attributed a sequence
/// at or below the published boundary: a row at the boundary (or above it) written before
/// the publish was visible would let a Flow cursor reach that sequence and then prune the
/// published files as already consumed.
#[tokio::test(flavor = "multi_thread")]
async fn test_staging_exit_publish_window_queues_writes() {
    common_telemetry::init_default_ut_logging();
    let partition_expr = float_range_expr("field_0", 0., 100.).as_json_str().unwrap();
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1024, 0);
    let request = CreateRequestBuilder::new().build();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // One visible flush; rows written during staging stay in the staging memtable because a
    // staging region keeps its WAL and flushes nothing, so the exit is what publishes them.
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows(0, 3),
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
    engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::UpdatePartitionExpr(partition_expr),
            }),
        )
        .await
        .unwrap();
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows(3, 5),
        },
    )
    .await;
    // A staging region publishes nothing on its own, so the exit needs an edit to merge and
    // re-mark; import one file from a source region to give it that edit.
    let source_region_id = RegionId::new(1024, 1);
    engine
        .handle_request(
            source_region_id,
            RegionRequest::Create(CreateRequestBuilder::new().build()),
        )
        .await
        .unwrap();
    put_rows(
        &engine,
        source_region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows(0, 2),
        },
    )
    .await;
    flush_region(&engine, source_region_id, None).await;
    let source_file = first_file(&engine.get_region(source_region_id).unwrap()).await;
    engine
        .handle_request(
            source_region_id,
            RegionRequest::Close(RegionCloseRequest::default()),
        )
        .await
        .unwrap();
    engine
        .edit_region(
            region_id,
            RegionEdit {
                files_to_add: vec![source_file],
                files_to_remove: vec![],
                timestamp_ms: None,
                compaction_time_window: None,
                flushed_entry_id: None,
                flushed_sequence: None,
                committed_sequence: None,
                last_invalid_seq: None,
                wal_recovery: Default::default(),
            },
        )
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    let last_sequence_before_exit = region.version_control.current().last_sequence;

    // Holding the manifest write lock keeps the exit inside its publish window: the worker
    // loop opens the window before the background publish runs, so the region stays in the
    // `Editing` state and the worker keeps queuing writes.
    let manifest_guard = region.manifest_ctx.manifest_manager.write().await;
    let exit_engine = engine.clone();
    let exit_task = tokio::spawn(async move {
        exit_engine
            .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Leader)
            .await
    });
    wait_until_region_state(&region, RegionLeaderState::Editing).await;

    // A write that arrives while the window is open must be queued, not admitted.
    let write_engine = engine.clone();
    let write_rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(5, 7),
    };
    let write_task =
        tokio::spawn(async move { put_rows(&write_engine, region_id, write_rows).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !write_task.is_finished(),
        "a write must be queued while the publish window of the staging exit is open"
    );
    assert_eq!(
        last_sequence_before_exit,
        region.version_control.current().last_sequence,
        "a queued write must not allocate a sequence during the publish"
    );

    // Releasing the lock lets the publish become visible, which closes the window: only then
    // is the queued write admitted.
    drop(manifest_guard);
    exit_task.await.unwrap().unwrap();
    write_task.await.unwrap();

    let boundary = region.version().last_invalid_seq();
    assert_eq!(
        last_sequence_before_exit + 1,
        boundary,
        "the staging exit must publish at last_sequence + 1"
    );
    assert_eq!(
        RegionRoleState::Leader(RegionLeaderState::Writable),
        region.state(),
        "the publish window must be released after the publish"
    );
    assert!(
        region.version_control.current().last_sequence > boundary,
        "the queued write must be attributed a sequence above the published boundary, \
         last_sequence: {}, boundary: {}",
        region.version_control.current().last_sequence,
        boundary
    );

    // The invariant the window exists for: a Flow cursor that reaches the boundary after the
    // publish cannot be handed a file it never consumed, because file-level pruning skips
    // files at or below the cursor. Every file this region produces after the publish
    // therefore carries a sequence strictly above the boundary.
    let files_before_flush = region
        .version()
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files.keys().copied())
        .collect::<std::collections::HashSet<_>>();
    engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest::default()),
        )
        .await
        .unwrap();
    let post_publish_version = region.version();
    let post_publish_files = post_publish_version
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files.values())
        .filter(|file| !files_before_flush.contains(&file.meta_ref().file_id))
        .collect::<Vec<_>>();
    assert!(
        !post_publish_files.is_empty(),
        "the queued write must have been flushed"
    );
    for file in post_publish_files {
        let sequence = file
            .meta_ref()
            .sequence
            .map(|sequence| sequence.get())
            .expect("a flushed file carries an admission barrier");
        assert!(
            sequence > boundary,
            "a post-publish file must stay above the published boundary (cursor), \
             file sequence: {sequence}, boundary: {boundary}"
        );
    }
}

/// An end of staging whose commit may have landed freezes its payload — and with it the
/// boundary `B` it publishes — and returns the region to `Staging`. Ordinary and bulk writes
/// must be refused until the retry completes: an admitted write could be attributed a sequence
/// at or below `B`, read back as a commit watermark `C >= B` before the published files are
/// visible, and a later reader would prune the files the retry publishes as already consumed.
#[tokio::test(flavor = "multi_thread")]
async fn test_staging_exit_rejects_writes_while_end_commit_pending() {
    common_telemetry::init_default_ut_logging();
    let faults = DeltaFaults::default();
    let mut env = TestEnv::with_prefix("staging-exit-end-commit-pending")
        .await
        .with_mock_layer(faults.layer());
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1024, 0);
    let request = CreateRequestBuilder::new().build();
    let schema = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // One visible flush, then rows written during staging that stay in the memtable: the exit
    // publishes at `last_sequence + 1` and keeps them.
    put_range(&engine, region_id, &schema, (0, 3)).await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest::default()),
        )
        .await
        .unwrap();
    enter_staging(&engine, region_id).await;
    put_range(&engine, region_id, &schema, (3, 5)).await;

    // A file imported from a source region gives the exit an edit to publish. Its rows use
    // their own key range so the row count of the scan below is unambiguous.
    let source_region_id = RegionId::new(1024, 1);
    engine
        .handle_request(
            source_region_id,
            RegionRequest::Create(CreateRequestBuilder::new().build()),
        )
        .await
        .unwrap();
    put_range(&engine, source_region_id, &schema, (20, 22)).await;
    flush_region(&engine, source_region_id, None).await;
    let imported = first_file(&engine.get_region(source_region_id).unwrap()).await;
    engine
        .handle_request(
            source_region_id,
            RegionRequest::Close(RegionCloseRequest::default()),
        )
        .await
        .unwrap();
    engine
        .edit_region(region_id, edit_adding(imported.clone()))
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    let last_sequence_before_exit = region.version_control.current().last_sequence;

    // The commit lands but its response is lost, so the region keeps the payload to retry with
    // and goes back to `Staging`.
    faults.lose_next_response();
    let response = engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Leader)
        .await
        .unwrap();
    assert!(response.is_invalid_transition(), "{response:?}");
    assert!(
        region.is_staging(),
        "a failed exit must keep the region staging"
    );
    assert!(
        region.staging_end_pending(),
        "a commit that may have landed must leave the retry pending"
    );
    assert_eq!(
        last_sequence_before_exit,
        region.version_control.current().last_sequence,
        "a failed exit must not publish its boundary in memory"
    );

    // The write is refused with the error of the pending commit. It is not queued: the region
    // is `Staging` again, so nothing would release such a queue.
    let err = engine
        .handle_request(
            region_id,
            RegionRequest::Put(RegionPutRequest {
                rows: Rows {
                    schema: schema.clone(),
                    rows: build_rows(5, 6),
                },
                hint: None,
                partition_expr_version: None,
                skip_wal: false,
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(StatusCode::RegionBusy, err.status_code());
    assert_eq!(RetryHint::Retryable, err.retry_hint());
    assert!(
        err.to_string().contains("waits for a retry"),
        "the write must be refused by the pending end commit, got: {err}"
    );
    assert_eq!(
        last_sequence_before_exit,
        region.version_control.current().last_sequence,
        "the refused write must not be attributed a sequence at or below the frozen boundary"
    );

    // The retry re-sends the frozen payload, which publishes the imported file at `B`.
    engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Leader)
        .await
        .unwrap();
    assert!(
        !region.staging_end_pending(),
        "a committed retry must drop the pending end commit"
    );
    assert_eq!(
        RegionRoleState::Leader(RegionLeaderState::Writable),
        region.state(),
        "the retry must release the region to its writable state"
    );
    let boundary = region.version().last_invalid_seq();
    assert_eq!(
        last_sequence_before_exit + 1,
        boundary,
        "the retry publishes the boundary it froze, not a new one"
    );
    assert_eq!(
        boundary,
        region.version_control.current().committed_sequence
    );
    assert!(
        region
            .version()
            .ssts
            .levels()
            .iter()
            .flat_map(|level| level.files.keys())
            .any(|file_id| *file_id == imported.file_id),
        "the file the retry published must be visible"
    );
    // 3 flushed rows + 2 staged rows + 2 imported rows.
    assert_eq!(
        7,
        count_rows(&engine, region_id).await,
        "the staged file and rows of the retried publish must be counted"
    );

    // Writes are admitted again, strictly above the published boundary.
    put_range(&engine, region_id, &schema, (5, 7)).await;
    assert!(
        region.version_control.current().last_sequence > boundary,
        "a write after the publish must be attributed a sequence above the boundary, \
         last_sequence: {}, boundary: {}",
        region.version_control.current().last_sequence,
        boundary
    );
}

/// Every staged edit must be published at the exit boundary, even when several staged edits
/// added files at different barriers. The persisted action list keeps the metadata /
/// partition actions in order and applies the final merged edit exactly once, so a reopen
/// replays the exit boundary for all staged files instead of restoring a stale sequence from
/// an original edit (which would leave a file at or below a cursor that never consumed it).
#[tokio::test]
async fn test_staging_exit_reopen_applies_boundary_to_every_staged_edit() {
    common_telemetry::init_default_ut_logging();
    let partition_expr = float_range_expr("field_0", 0., 100.).as_json_str().unwrap();
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1024, 0);
    let request = CreateRequestBuilder::new().build();
    let column_schemas = rows_schema(&request);
    let table_dir = request.table_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows(0, 3),
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
    engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::UpdatePartitionExpr(partition_expr),
            }),
        )
        .await
        .unwrap();
    // Rows written during staging stay in the staging memtable; staged files are imported
    // one edit at a time, so the staging layout holds several distinct files.
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows(3, 5),
        },
    )
    .await;
    // A source region provides the files that staging imports.
    let source_region_id = RegionId::new(1024, 1);
    engine
        .handle_request(
            source_region_id,
            RegionRequest::Create(CreateRequestBuilder::new().build()),
        )
        .await
        .unwrap();
    for (start, end) in [(10, 13), (13, 15)] {
        put_rows(
            &engine,
            source_region_id,
            Rows {
                schema: column_schemas.clone(),
                rows: build_rows(start, end),
            },
        )
        .await;
        flush_region(&engine, source_region_id, None).await;
    }
    let source_files = engine
        .get_region(source_region_id)
        .unwrap()
        .manifest_ctx
        .manifest()
        .await
        .files
        .values()
        .cloned()
        .collect::<Vec<_>>();
    engine
        .handle_request(
            source_region_id,
            RegionRequest::Close(RegionCloseRequest::default()),
        )
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    // Each imported file is added by its own staged edit, so the staging layout holds files
    // that were admitted at different barriers.
    for file in source_files {
        engine
            .edit_region(
                region_id,
                RegionEdit {
                    files_to_add: vec![file],
                    files_to_remove: vec![],
                    timestamp_ms: None,
                    compaction_time_window: None,
                    flushed_entry_id: None,
                    flushed_sequence: None,
                    committed_sequence: None,
                    last_invalid_seq: None,
                    wal_recovery: Default::default(),
                },
            )
            .await
            .unwrap();
    }
    // Two distinct staged files, each added by its own staged edit at its own barrier.
    let staged_files = {
        let manager = region.manifest_ctx.manifest_manager.read().await;
        let mut seen = std::collections::HashSet::new();
        manager
            .staging_manifest()
            .unwrap()
            .files
            .values()
            .filter(|file| seen.insert(file.file_id))
            .take(2)
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(2, staged_files.len(), "need two distinct staged files");
    for file in &staged_files {
        engine
            .edit_region(
                region_id,
                RegionEdit {
                    files_to_add: vec![file.clone()],
                    files_to_remove: vec![],
                    timestamp_ms: None,
                    compaction_time_window: None,
                    flushed_entry_id: None,
                    flushed_sequence: None,
                    committed_sequence: None,
                    last_invalid_seq: None,
                    wal_recovery: Default::default(),
                },
            )
            .await
            .unwrap();
    }
    let last_sequence_before_exit = region.version_control.current().last_sequence;
    let boundary = last_sequence_before_exit + 1;

    engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Leader)
        .await
        .unwrap();
    assert_eq!(
        boundary,
        region.version().last_invalid_seq(),
        "the exit must publish its own boundary"
    );
    for file in &staged_files {
        assert_eq!(
            Some(boundary),
            file_sequence(&region, file.file_id),
            "the in-memory publish must re-mark every staged file at the boundary"
        );
    }

    // Replaying the persisted action list must agree with the in-memory publish: the merged
    // edit is applied exactly once and after every metadata action, so no original edit can
    // overwrite the boundary with its own stale sequence.
    reopen_region(&engine, region_id, table_dir, true, Default::default()).await;
    let reopened = engine.get_region(region_id).unwrap();
    assert_eq!(
        boundary,
        reopened.version().last_invalid_seq(),
        "the exit boundary must survive a reopen"
    );
    for file in &staged_files {
        assert_eq!(
            Some(boundary),
            file_sequence(&reopened, file.file_id),
            "every staged edit must carry the exit boundary after a reopen"
        );
    }
}
