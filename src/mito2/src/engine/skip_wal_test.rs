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
use std::time::Duration;

use api::v1::Rows;
use common_wal::options::{WAL_OPTIONS_KEY, WalOptions};
use store_api::logstore::provider::Provider;
use store_api::mito_engine_options::SKIP_WAL_KEY;
use store_api::region_engine::{RegionEngine, RegionRole};
use store_api::region_request::{
    AlterKind, RegionAlterRequest, RegionCloseRequest, RegionOpenRequest, RegionPutRequest,
    RegionRequest, RegionTruncateRequest, SetRegionOption,
};
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::engine::listener::AlterFlushListener;
use crate::test_util::{
    CreateRequestBuilder, TestEnv, build_rows, flush_region, put_rows, rows_schema,
};

#[tokio::test]
async fn test_close_region_skip_wal_with_pending_data() {
    test_close_region_skip_wal(true).await;
}

#[tokio::test]
async fn test_close_region_skip_wal_without_pending_data() {
    test_close_region_skip_wal(false).await;
}

#[tokio::test]
async fn test_alter_skip_wal_stops_wal_and_flushes_on_close() {
    let mut env = TestEnv::with_prefix("alter-skip-wal").await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    let mut request = CreateRequestBuilder::new().build();
    let schema = rows_schema(&request);

    engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: schema.clone(),
            rows: build_rows(0, 3),
        },
    )
    .await;

    let before_alter = engine
        .get_region(region_id)
        .unwrap()
        .version_control
        .current();
    assert!(!matches!(
        &engine.get_region(region_id).unwrap().provider,
        Provider::Noop
    ));
    assert!(!before_alter.version.memtables.is_empty());

    engine
        .handle_request(
            region_id,
            RegionRequest::Alter(RegionAlterRequest {
                kind: AlterKind::SetRegionOptions {
                    options: vec![SetRegionOption::SkipWal],
                },
            }),
        )
        .await
        .unwrap();
    engine
        .handle_request(
            region_id,
            RegionRequest::Alter(RegionAlterRequest {
                kind: AlterKind::SetRegionOptions {
                    options: vec![SetRegionOption::SkipWal],
                },
            }),
        )
        .await
        .unwrap();

    let after_alter = engine
        .get_region(region_id)
        .unwrap()
        .version_control
        .current();
    assert!(after_alter.version.options.skip_wal);
    assert_eq!(before_alter.last_entry_id, after_alter.last_entry_id);
    assert_eq!(
        before_alter.version.flushed_sequence,
        after_alter.version.flushed_sequence
    );

    put_rows(
        &engine,
        region_id,
        Rows {
            schema,
            rows: build_rows(3, 6),
        },
    )
    .await;
    assert_eq!(
        before_alter.last_entry_id,
        engine
            .get_region(region_id)
            .unwrap()
            .version_control
            .current()
            .last_entry_id
    );

    engine
        .handle_request(
            region_id,
            RegionRequest::Close(RegionCloseRequest::default()),
        )
        .await
        .unwrap();

    request
        .options
        .insert(SKIP_WAL_KEY.to_string(), "true".to_string());
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir: request.table_dir,
                path_type: store_api::region_request::PathType::Bare,
                options: request.options,
                skip_wal_replay: false,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();

    let batches = common_recordbatch::RecordBatches::try_collect(
        engine
            .scan_to_stream(region_id, ScanRequest::default())
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        6,
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>()
    );
}

#[tokio::test]
async fn test_alter_skip_wal_on_follower_survives_promotion() {
    let mut env = TestEnv::with_prefix("alter-skip-wal-follower").await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let schema = rows_schema(&request);

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    engine
        .set_region_role(region_id, RegionRole::Follower)
        .unwrap();
    engine
        .handle_request(
            region_id,
            RegionRequest::Alter(RegionAlterRequest {
                kind: AlterKind::SetRegionOptions {
                    options: vec![SetRegionOption::SkipWal],
                },
            }),
        )
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    assert!(region.is_follower());
    assert!(region.version().options.skip_wal);

    engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap();
    let last_entry_id = region.version_control.current().last_entry_id;
    put_rows(
        &engine,
        region_id,
        Rows {
            schema,
            rows: build_rows(0, 1),
        },
    )
    .await;
    assert_eq!(
        last_entry_id,
        region.version_control.current().last_entry_id
    );
}

async fn test_close_region_skip_wal(insert: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix(&format!("close-skip-wal-{}", insert)).await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let mut request = CreateRequestBuilder::new().build();

    // Set skip_wal = true via WalOptions::Noop
    let wal_options = WalOptions::Noop;
    request.options.insert(
        WAL_OPTIONS_KEY.to_string(),
        serde_json::to_string(&wal_options).unwrap(),
    );

    engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();

    if insert {
        let column_schemas = rows_schema(&request);
        let rows = Rows {
            schema: column_schemas.clone(),
            rows: build_rows(0, 3),
        };
        put_rows(&engine, region_id, rows).await;
    }

    // The region should have data in memtable.
    let region = engine.get_region(region_id).unwrap();
    if insert {
        assert!(!region.version().memtables.is_empty());
    } else {
        assert!(region.version().memtables.is_empty());
    }

    // Close the region. This should trigger a flush.
    engine
        .handle_request(
            region_id,
            RegionRequest::Close(RegionCloseRequest::default()),
        )
        .await
        .unwrap();

    // After closing, we reopen it and check if data is persisted.
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(store_api::region_request::RegionOpenRequest {
                engine: String::new(),
                table_dir: request.table_dir.clone(),
                path_type: store_api::region_request::PathType::Bare,
                options: request.options.clone(),
                skip_wal_replay: false,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();
    let scan_request = ScanRequest::default();
    let stream = engine
        .scan_to_stream(region_id, scan_request)
        .await
        .unwrap();
    let batches = common_recordbatch::RecordBatches::try_collect(stream)
        .await
        .unwrap();
    // If flush was triggered, data should be there even though WAL was skipped.
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    if insert {
        assert_eq!(3, total_rows);
    } else {
        assert_eq!(0, total_rows);
    }
}

#[tokio::test]
async fn test_close_follower_region_skip_wal() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("close-follower-skip-wal").await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let mut request = CreateRequestBuilder::new().build();

    // Set skip_wal = true via WalOptions::Noop
    let wal_options = WalOptions::Noop;
    request.options.insert(
        WAL_OPTIONS_KEY.to_string(),
        serde_json::to_string(&wal_options).unwrap(),
    );

    engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();

    // Set the region to Follower state.
    engine
        .set_region_role(region_id, RegionRole::Follower)
        .unwrap();
    assert!(region.is_follower());

    // Close the region. This should trigger a flush.
    engine
        .handle_request(
            region_id,
            RegionRequest::Close(RegionCloseRequest::default()),
        )
        .await
        .unwrap();

    // After closing, we reopen it and check if data is persisted.
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(store_api::region_request::RegionOpenRequest {
                engine: String::new(),
                table_dir: request.table_dir.clone(),
                path_type: store_api::region_request::PathType::Bare,
                options: request.options.clone(),
                skip_wal_replay: false,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();
    let scan_request = ScanRequest::default();
    let stream = engine
        .scan_to_stream(region_id, scan_request)
        .await
        .unwrap();
    let batches = common_recordbatch::RecordBatches::try_collect(stream)
        .await
        .unwrap();
    // If flush was triggered, data should be there even though WAL was skipped.
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(0, total_rows);
}

#[tokio::test]
async fn test_close_follower_region_skip_wal_with_pending_data() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("close-follower-skip-wal-pending-data").await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let mut request = CreateRequestBuilder::new().build();

    let wal_options = WalOptions::Noop;
    request.options.insert(
        WAL_OPTIONS_KEY.to_string(),
        serde_json::to_string(&wal_options).unwrap(),
    );

    engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();

    let rows = Rows {
        schema: rows_schema(&request),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    let region = engine.get_region(region_id).unwrap();
    assert!(!region.version().memtables.is_empty());

    engine
        .set_region_role(region_id, RegionRole::Follower)
        .unwrap();
    assert!(region.is_follower());

    engine
        .handle_request(
            region_id,
            RegionRequest::Close(RegionCloseRequest::default()),
        )
        .await
        .unwrap();

    assert!(!engine.is_region_exists(region_id));
}

#[tokio::test]
async fn test_close_region_skip_wal_while_flush_in_flight_closes_region() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("close-skip-wal-while-flush-in-flight").await;
    let listener = Arc::new(AlterFlushListener::default());
    let engine = env
        .create_engine_with(MitoConfig::default(), None, Some(listener.clone()), None)
        .await;

    let region_id = RegionId::new(1, 1);
    let mut request = CreateRequestBuilder::new().build();
    let wal_options = WalOptions::Noop;
    request.options.insert(
        WAL_OPTIONS_KEY.to_string(),
        serde_json::to_string(&wal_options).unwrap(),
    );

    engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();

    let rows = Rows {
        schema: rows_schema(&request),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;
    assert!(
        !engine
            .get_region(region_id)
            .unwrap()
            .version()
            .memtables
            .is_empty()
    );

    let request_count = listener.request_count();
    let engine_cloned = engine.clone();
    let flush_job = tokio::spawn(async move {
        flush_region(&engine_cloned, region_id, None).await;
    });
    listener.wait_flush_begin().await;
    listener.wait_request_count(request_count + 1).await;

    let engine_cloned = engine.clone();
    let close_job = tokio::spawn(async move {
        engine_cloned
            .handle_request(
                region_id,
                RegionRequest::Close(RegionCloseRequest::default()),
            )
            .await
            .unwrap();
    });
    listener.wait_request_count(request_count + 2).await;

    listener.wake_flush();
    tokio::time::timeout(Duration::from_secs(5), flush_job)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), close_job)
        .await
        .unwrap()
        .unwrap();

    assert!(!engine.is_region_exists(region_id));
}

#[tokio::test]
async fn test_close_region_skip_wal_rejects_writes_queued_after_close() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("close-skip-wal-rejects-writes-after-close").await;
    let listener = Arc::new(AlterFlushListener::default());
    let engine = env
        .create_engine_with(MitoConfig::default(), None, Some(listener.clone()), None)
        .await;

    let region_id = RegionId::new(1, 1);
    let mut request = CreateRequestBuilder::new().build();
    request.options.insert(
        WAL_OPTIONS_KEY.to_string(),
        serde_json::to_string(&WalOptions::Noop).unwrap(),
    );

    engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: rows_schema(&request),
            rows: build_rows(0, 3),
        },
    )
    .await;

    let request_count = listener.request_count();
    let engine_cloned = engine.clone();
    let flush_job = tokio::spawn(async move {
        flush_region(&engine_cloned, region_id, None).await;
    });
    listener.wait_flush_begin().await;
    listener.wait_request_count(request_count + 1).await;

    let pre_close_write = engine
        .handle_request(
            region_id,
            RegionRequest::Put(RegionPutRequest {
                rows: Rows {
                    schema: rows_schema(&request),
                    rows: build_rows(3, 4),
                },
                hint: None,
                partition_expr_version: None,
            }),
        )
        .await
        .unwrap();
    assert_eq!(1, pre_close_write.affected_rows);

    let engine_cloned = engine.clone();
    let close_job = tokio::spawn(async move {
        engine_cloned
            .handle_request(
                region_id,
                RegionRequest::Close(RegionCloseRequest::default()),
            )
            .await
            .unwrap();
    });
    listener.wait_request_count(request_count + 3).await;

    let engine_cloned = engine.clone();
    let request_cloned = request.clone();
    let write_job = tokio::spawn(async move {
        engine_cloned
            .handle_request(
                region_id,
                RegionRequest::Put(RegionPutRequest {
                    rows: Rows {
                        schema: rows_schema(&request_cloned),
                        rows: build_rows(4, 5),
                    },
                    hint: None,
                    partition_expr_version: None,
                }),
            )
            .await
    });
    listener.wait_request_count(request_count + 4).await;

    listener.wake_flush();
    tokio::time::timeout(Duration::from_secs(5), flush_job)
        .await
        .unwrap()
        .unwrap();
    listener.wait_flush_begin().await;

    let engine_cloned = engine.clone();
    let request_cloned = request.clone();
    let late_write_job = tokio::spawn(async move {
        engine_cloned
            .handle_request(
                region_id,
                RegionRequest::Put(RegionPutRequest {
                    rows: Rows {
                        schema: rows_schema(&request_cloned),
                        rows: build_rows(5, 6),
                    },
                    hint: None,
                    partition_expr_version: None,
                }),
            )
            .await
    });
    listener.wait_request_count(request_count + 5).await;

    listener.wake_flush();
    tokio::time::timeout(Duration::from_secs(5), close_job)
        .await
        .unwrap()
        .unwrap();
    let write_result = tokio::time::timeout(Duration::from_secs(5), write_job)
        .await
        .unwrap()
        .unwrap();
    let late_write_result = tokio::time::timeout(Duration::from_secs(5), late_write_job)
        .await
        .unwrap()
        .unwrap();

    assert!(write_result.is_err());
    assert!(late_write_result.is_err());
    assert!(!engine.is_region_exists(region_id));
}

#[tokio::test]
async fn test_concurrent_close_region_skip_wal_while_flush_in_flight_succeeds() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("concurrent-close-skip-wal-while-flush-in-flight").await;
    let listener = Arc::new(AlterFlushListener::default());
    let engine = env
        .create_engine_with(MitoConfig::default(), None, Some(listener.clone()), None)
        .await;

    let region_id = RegionId::new(1, 1);
    let mut request = CreateRequestBuilder::new().build();
    request.options.insert(
        WAL_OPTIONS_KEY.to_string(),
        serde_json::to_string(&WalOptions::Noop).unwrap(),
    );

    engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();

    let rows = Rows {
        schema: rows_schema(&request),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    let request_count = listener.request_count();
    let engine_cloned = engine.clone();
    let first_close = tokio::spawn(async move {
        engine_cloned
            .handle_request(
                region_id,
                RegionRequest::Close(RegionCloseRequest::default()),
            )
            .await
            .unwrap();
    });
    listener.wait_flush_begin().await;
    listener.wait_request_count(request_count + 1).await;

    let engine_cloned = engine.clone();
    let second_close = tokio::spawn(async move {
        engine_cloned
            .handle_request(
                region_id,
                RegionRequest::Close(RegionCloseRequest::default()),
            )
            .await
            .unwrap();
    });
    listener.wait_request_count(request_count + 2).await;

    listener.wake_flush();
    tokio::time::timeout(Duration::from_secs(5), first_close)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), second_close)
        .await
        .unwrap()
        .unwrap();

    assert!(!engine.is_region_exists(region_id));
}

#[tokio::test]
async fn test_close_region_after_truncate_skip_wal() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("close-truncate-skip-wal").await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let mut request = CreateRequestBuilder::new().build();
    let wal_options = WalOptions::Noop;
    request.options.insert(
        WAL_OPTIONS_KEY.to_string(),
        serde_json::to_string(&wal_options).unwrap(),
    );

    engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();

    engine
        .handle_request(
            region_id,
            RegionRequest::Truncate(RegionTruncateRequest::All),
        )
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    let version_data = region.version_control.current();
    assert_eq!(
        version_data.version.truncated_entry_id,
        Some(version_data.last_entry_id)
    );

    let rows = Rows {
        schema: rows_schema(&request),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    let region = engine.get_region(region_id).unwrap();
    assert!(!region.version().memtables.is_empty());

    engine
        .handle_request(
            region_id,
            RegionRequest::Close(RegionCloseRequest::default()),
        )
        .await
        .unwrap();

    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir: request.table_dir,
                path_type: store_api::region_request::PathType::Bare,
                options: request.options,
                skip_wal_replay: false,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();
    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = common_recordbatch::RecordBatches::try_collect(stream)
        .await
        .unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(3, total_rows);
}
