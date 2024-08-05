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

use std::ops::Range;
use std::sync::Arc;

use api::v1::{ColumnSchema, Rows};
use common_recordbatch::{RecordBatches, SendableRecordBatchStream};
use datatypes::prelude::ScalarVector;
use datatypes::vectors::TimestampMillisecondVector;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{
    RegionCompactRequest, RegionDeleteRequest, RegionFlushRequest, RegionRequest,
};
use store_api::storage::{RegionId, ScanRequest};
use tokio::sync::Notify;

use crate::config::MitoConfig;
use crate::engine::listener::CompactionListener;
use crate::engine::MitoEngine;
use crate::test_util::{
    build_rows_for_key, column_metadata_to_column_schema, put_rows, CreateRequestBuilder, TestEnv,
};

async fn put_and_flush(
    engine: &MitoEngine,
    region_id: RegionId,
    column_schemas: &[ColumnSchema],
    rows: Range<usize>,
) {
    let rows = Rows {
        schema: column_schemas.to_vec(),
        rows: build_rows_for_key("a", rows.start, rows.end, 0),
    };
    put_rows(engine, region_id, rows).await;

    let result = engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest {
                row_group_size: None,
            }),
        )
        .await
        .unwrap();
    assert_eq!(0, result.affected_rows);
}

async fn delete_and_flush(
    engine: &MitoEngine,
    region_id: RegionId,
    column_schemas: &[ColumnSchema],
    rows: Range<usize>,
) {
    let row_cnt = rows.len();
    let rows = Rows {
        schema: column_schemas.to_vec(),
        rows: build_rows_for_key("a", rows.start, rows.end, 0),
    };

    let result = engine
        .handle_request(
            region_id,
            RegionRequest::Delete(RegionDeleteRequest { rows }),
        )
        .await
        .unwrap();
    assert_eq!(row_cnt, result.affected_rows);

    let result = engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest {
                row_group_size: None,
            }),
        )
        .await
        .unwrap();
    assert_eq!(0, result.affected_rows);
}

async fn collect_stream_ts(stream: SendableRecordBatchStream) -> Vec<i64> {
    let mut res = Vec::new();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    for batch in batches {
        let ts_col = batch
            .column_by_name("ts")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMillisecondVector>()
            .unwrap();
        res.extend(ts_col.iter_data().map(|t| t.unwrap().0.value()));
    }
    res
}

#[tokio::test]
async fn test_compaction_region() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.max_active_window_runs", "1")
        .insert_option("compaction.twcs.max_inactive_window_runs", "1")
        .build();

    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    // Flush 5 SSTs for compaction.
    put_and_flush(&engine, region_id, &column_schemas, 0..10).await;
    put_and_flush(&engine, region_id, &column_schemas, 10..20).await;
    put_and_flush(&engine, region_id, &column_schemas, 20..30).await;
    delete_and_flush(&engine, region_id, &column_schemas, 15..30).await;
    put_and_flush(&engine, region_id, &column_schemas, 15..25).await;

    let result = engine
        .handle_request(
            region_id,
            RegionRequest::Compact(RegionCompactRequest::default()),
        )
        .await
        .unwrap();
    assert_eq!(result.affected_rows, 0);

    let scanner = engine.scanner(region_id, ScanRequest::default()).unwrap();
    // Input:
    // [0..9]
    //       [10...19]
    //                [20....29]
    //          -[15.........29]-
    //           [15.....24]
    // Output:
    // [0..9]
    //     [10..14]
    //            [15..24]
    assert_eq!(
        3,
        scanner.num_files(),
        "unexpected files: {:?}",
        scanner.file_ids()
    );
    let stream = scanner.scan().await.unwrap();

    let vec = collect_stream_ts(stream).await;
    assert_eq!((0..25).map(|v| v * 1000).collect::<Vec<_>>(), vec);
}

#[tokio::test]
async fn test_compaction_region_with_overlapping() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.max_active_window_runs", "2")
        .insert_option("compaction.twcs.max_inactive_window_runs", "2")
        .insert_option("compaction.twcs.time_window", "1h")
        .build();

    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    // Flush 4 SSTs for compaction.
    put_and_flush(&engine, region_id, &column_schemas, 0..1200).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 0..2400).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 3600..10800).await; // window 10800
    delete_and_flush(&engine, region_id, &column_schemas, 0..3600).await; // window 3600

    let result = engine
        .handle_request(
            region_id,
            RegionRequest::Compact(RegionCompactRequest::default()),
        )
        .await
        .unwrap();
    assert_eq!(result.affected_rows, 0);

    let scanner = engine.scanner(region_id, ScanRequest::default()).unwrap();
    let stream = scanner.scan().await.unwrap();
    let vec = collect_stream_ts(stream).await;
    assert_eq!((3600..10800).map(|i| { i * 1000 }).collect::<Vec<_>>(), vec);
}

#[tokio::test]
async fn test_compaction_region_with_overlapping_delete_all() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.max_active_window_runs", "2")
        .insert_option("compaction.twcs.max_active_window_files", "2")
        .insert_option("compaction.twcs.max_inactive_window_runs", "2")
        .insert_option("compaction.twcs.max_inactive_window_files", "2")
        .insert_option("compaction.twcs.time_window", "1h")
        .build();

    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    // Flush 4 SSTs for compaction.
    put_and_flush(&engine, region_id, &column_schemas, 0..1200).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 0..2400).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 0..3600).await; // window 3600
    delete_and_flush(&engine, region_id, &column_schemas, 0..10800).await; // window 10800

    let result = engine
        .handle_request(
            region_id,
            RegionRequest::Compact(RegionCompactRequest::default()),
        )
        .await
        .unwrap();
    assert_eq!(result.affected_rows, 0);

    let scanner = engine.scanner(region_id, ScanRequest::default()).unwrap();
    assert_eq!(
        4,
        scanner.num_files(),
        "unexpected files: {:?}",
        scanner.file_ids()
    );
    let stream = scanner.scan().await.unwrap();
    let vec = collect_stream_ts(stream).await;
    assert!(vec.is_empty());
}

// For issue https://github.com/GreptimeTeam/greptimedb/issues/3633
#[tokio::test]
async fn test_readonly_during_compaction() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new();
    let listener = Arc::new(CompactionListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig {
                // Ensure there is only one background worker for purge task.
                max_background_jobs: 1,
                ..Default::default()
            },
            None,
            Some(listener.clone()),
        )
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.max_active_window_runs", "1")
        .build();

    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    // Flush 2 SSTs for compaction.
    put_and_flush(&engine, region_id, &column_schemas, 0..10).await;
    put_and_flush(&engine, region_id, &column_schemas, 5..20).await;

    // Waits until the engine receives compaction finished request.
    listener.wait_handle_finished().await;

    // Sets the region to read only mode.
    engine.set_writable(region_id, false).unwrap();
    // Wakes up the listener.
    listener.wake();

    let notify = Arc::new(Notify::new());
    // We already sets max background jobs to 1, so we can submit a task to the
    // purge scheduler to ensure all purge tasks are finished.
    let job_notify = notify.clone();
    engine
        .purge_scheduler()
        .schedule(Box::pin(async move {
            job_notify.notify_one();
        }))
        .unwrap();
    notify.notified().await;

    let scanner = engine.scanner(region_id, ScanRequest::default()).unwrap();
    assert_eq!(
        2,
        scanner.num_files(),
        "unexpected files: {:?}",
        scanner.file_ids()
    );
    let stream = scanner.scan().await.unwrap();

    let vec = collect_stream_ts(stream).await;
    assert_eq!((0..20).map(|v| v * 1000).collect::<Vec<_>>(), vec);
}
