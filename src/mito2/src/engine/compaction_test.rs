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

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use api::v1::{ColumnSchema, Rows};
use common_recordbatch::{RecordBatches, SendableRecordBatchStream};
use datatypes::prelude::ScalarVector;
use datatypes::vectors::TimestampMillisecondVector;
use store_api::region_engine::{RegionEngine, RegionRole};
use store_api::region_request::AlterKind::SetRegionOptions;
use store_api::region_request::{
    PathType, RegionAlterRequest, RegionCompactRequest, RegionDeleteRequest, RegionFlushRequest,
    RegionOpenRequest, RegionRequest, SetRegionOption,
};
use store_api::storage::{RegionId, ScanRequest};
use tokio::sync::Notify;

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::engine::listener::CompactionListener;
use crate::test_util::{
    CreateRequestBuilder, TestEnv, build_rows_for_key, column_metadata_to_column_schema, put_rows,
};

pub(crate) async fn put_and_flush(
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

async fn flush(engine: &MitoEngine, region_id: RegionId) {
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

pub(crate) async fn compact(engine: &MitoEngine, region_id: RegionId) {
    let result = engine
        .handle_request(
            region_id,
            RegionRequest::Compact(RegionCompactRequest::default()),
        )
        .await
        .unwrap();
    assert_eq!(result.affected_rows, 0);
}

pub(crate) async fn delete_and_flush(
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
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
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

    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
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

    compact(&engine, region_id).await;

    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    // Input:
    // [0..9]
    //       [10...19]
    //                [20....29]
    //          -[15.........29]- (delete)
    //           [15.....24]
    // Output:
    // [0..9]
    //       [10............29] (contains delete)
    //           [15....24]
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
async fn test_infer_compaction_time_window() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
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

    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
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
    // time window should be absent
    assert!(
        engine
            .get_region(region_id)
            .unwrap()
            .version_control
            .current()
            .version
            .compaction_time_window
            .is_none()
    );

    put_and_flush(&engine, region_id, &column_schemas, 1..2).await;
    put_and_flush(&engine, region_id, &column_schemas, 2..3).await;
    put_and_flush(&engine, region_id, &column_schemas, 3..4).await;
    put_and_flush(&engine, region_id, &column_schemas, 4..5).await;

    compact(&engine, region_id).await;

    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(
        1,
        scanner.num_files(),
        "unexpected files: {:?}",
        scanner.file_ids()
    );

    assert_eq!(
        Duration::from_secs(3600),
        engine
            .get_region(region_id)
            .unwrap()
            .version_control
            .current()
            .version
            .compaction_time_window
            .unwrap()
    );

    // write two rows to trigger another flush.
    // note: this two rows still use the original part_duration (1day by default), so they are written
    // to the same time partition and flushed to one file.
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows_for_key("a", 3601, 3602, 0),
        },
    )
    .await;
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows_for_key("a", 7201, 7202, 0),
        },
    )
    .await;
    // this flush should update part_duration in TimePartitions.
    flush(&engine, region_id).await;
    compact(&engine, region_id).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(
        2,
        scanner.num_files(),
        "unexpected files: {:?}",
        scanner.file_ids()
    );

    // These data should use new part_duration in TimePartitions and get written to two different
    // time partitions so we end up with 4 ssts.
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows_for_key("a", 3601, 3602, 0),
        },
    )
    .await;
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows_for_key("a", 7201, 7202, 0),
        },
    )
    .await;
    flush(&engine, region_id).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(
        4,
        scanner.num_files(),
        "unexpected files: {:?}",
        scanner.file_ids()
    );
}

#[tokio::test]
async fn test_compaction_overlapping_files() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
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

    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
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
    delete_and_flush(&engine, region_id, &column_schemas, 10..20).await;
    put_and_flush(&engine, region_id, &column_schemas, 20..30).await;
    delete_and_flush(&engine, region_id, &column_schemas, 30..40).await;

    compact(&engine, region_id).await;

    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(
        1,
        scanner.num_files(),
        "unexpected files: {:?}",
        scanner.file_ids()
    );
    let stream = scanner.scan().await.unwrap();

    let vec = collect_stream_ts(stream).await;
    assert_eq!(
        vec,
        (0..=9)
            .map(|v| v * 1000)
            .chain((20..=29).map(|v| v * 1000))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn test_compaction_region_with_overlapping() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);

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

    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
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

    compact(&engine, region_id).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    let stream = scanner.scan().await.unwrap();
    let vec = collect_stream_ts(stream).await;
    assert_eq!((3600..10800).map(|i| { i * 1000 }).collect::<Vec<_>>(), vec);
}

#[tokio::test]
async fn test_compaction_region_with_overlapping_delete_all() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);

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

    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
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
    tokio::time::sleep(Duration::from_millis(2)).await;
    compact(&engine, region_id).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(
        2,
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
    let mut env = TestEnv::new().await;
    let listener = Arc::new(CompactionListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig {
                // Ensure there is only one background worker for purge task.
                max_background_purges: 1,
                ..Default::default()
            },
            None,
            Some(listener.clone()),
            None,
        )
        .await;

    let region_id = RegionId::new(1, 1);
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

    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
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

    // Converts region to follower.
    engine
        .set_region_role(region_id, RegionRole::Follower)
        .unwrap();
    // Wakes up the listener.
    listener.wake();

    let notify = Arc::new(Notify::new());
    // We already sets max background purges to 1, so we can submit a task to the
    // purge scheduler to ensure all purge tasks are finished.
    let job_notify = notify.clone();
    engine
        .purge_scheduler()
        .schedule(Box::pin(async move {
            job_notify.notify_one();
        }))
        .unwrap();
    notify.notified().await;

    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
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

#[tokio::test]
async fn test_compaction_update_time_window() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);

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

    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
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
    // Flush 3 SSTs for compaction.
    put_and_flush(&engine, region_id, &column_schemas, 0..900).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 900..1800).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 1800..2700).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 2700..3600).await; // window 3600

    compact(&engine, region_id).await;
    assert_eq!(
        engine
            .get_region(region_id)
            .unwrap()
            .version_control
            .current()
            .version
            .compaction_time_window,
        Some(Duration::from_secs(3600))
    );
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(0, scanner.num_memtables());
    // We keep all 3 files because no enough file to merge
    assert_eq!(
        1,
        scanner.num_files(),
        "unexpected files: {:?}",
        scanner.file_ids()
    );

    // Flush a new SST and the time window is applied.
    put_and_flush(&engine, region_id, &column_schemas, 0..1200).await; // window 3600

    // Puts window 7200.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 3600, 4000, 0),
    };
    put_rows(&engine, region_id, rows).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(1, scanner.num_memtables());
    let stream = scanner.scan().await.unwrap();
    let vec = collect_stream_ts(stream).await;
    assert_eq!((0..4000).map(|v| v * 1000).collect::<Vec<_>>(), vec);

    // Puts window 3600.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 2400, 3600, 0),
    };
    put_rows(&engine, region_id, rows).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(2, scanner.num_memtables());
    let stream = scanner.scan().await.unwrap();
    let vec = collect_stream_ts(stream).await;
    assert_eq!((0..4000).map(|v| v * 1000).collect::<Vec<_>>(), vec);
}

#[tokio::test]
async fn test_change_region_compaction_window() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);

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

    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .build();
    let table_dir = request.table_dir.clone();
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
    put_and_flush(&engine, region_id, &column_schemas, 0..600).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 600..1200).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 1200..1800).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 1800..2400).await; // window 3600

    compact(&engine, region_id).await;

    // Put window 7200
    put_and_flush(&engine, region_id, &column_schemas, 4000..5000).await;

    // Check compaction window.
    let region = engine.get_region(region_id).unwrap();
    {
        let version = region.version();
        assert_eq!(
            Some(Duration::from_secs(3600)),
            version.compaction_time_window,
        );
        assert!(version.options.compaction.time_window().is_none());
    }

    // Change compaction window.
    let request = RegionRequest::Alter(RegionAlterRequest {
        kind: SetRegionOptions {
            options: vec![SetRegionOption::Twsc(
                "compaction.twcs.time_window".to_string(),
                "2h".to_string(),
            )],
        },
    });
    engine.handle_request(region_id, request).await.unwrap();
    assert_eq!(
        engine
            .get_region(region_id)
            .unwrap()
            .version_control
            .current()
            .version
            .options
            .compaction
            .time_window(),
        Some(Duration::from_secs(7200))
    );

    put_and_flush(&engine, region_id, &column_schemas, 5000..5100).await;
    put_and_flush(&engine, region_id, &column_schemas, 5100..5200).await;
    put_and_flush(&engine, region_id, &column_schemas, 5200..5300).await;

    // Compaction again. It should compacts window 3600 and 7200
    // into 7200.
    compact(&engine, region_id).await;
    // Check compaction window.
    {
        let region = engine.get_region(region_id).unwrap();
        let version = region.version();
        assert_eq!(
            Some(Duration::from_secs(7200)),
            version.compaction_time_window,
        );
        assert_eq!(
            Some(Duration::from_secs(7200)),
            version.options.compaction.time_window()
        );
    }

    // Reopen region.
    let engine = env.reopen_engine(engine, MitoConfig::default()).await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir,
                path_type: PathType::Bare,
                options: Default::default(),
                skip_wal_replay: false,
                checkpoint: None,
            }),
        )
        .await
        .unwrap();
    // Check compaction window.
    {
        let region = engine.get_region(region_id).unwrap();
        let version = region.version();
        // We open the region without options, so the time window should be None.
        assert!(version.options.compaction.time_window().is_none());
        assert_eq!(
            Some(Duration::from_secs(7200)),
            version.compaction_time_window,
        );
    }
}

#[tokio::test]
async fn test_open_overwrite_compaction_window() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);

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

    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .build();
    let table_dir = request.table_dir.clone();
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
    put_and_flush(&engine, region_id, &column_schemas, 0..600).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 600..1200).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 1200..1800).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 1800..2400).await; // window 3600

    compact(&engine, region_id).await;

    // Check compaction window.
    {
        let region = engine.get_region(region_id).unwrap();
        let version = region.version();
        assert_eq!(
            Some(Duration::from_secs(3600)),
            version.compaction_time_window,
        );
        assert!(version.options.compaction.time_window().is_none());
    }

    // Reopen region.
    let options = HashMap::from([
        ("compaction.type".to_string(), "twcs".to_string()),
        ("compaction.twcs.time_window".to_string(), "2h".to_string()),
    ]);
    let engine = env.reopen_engine(engine, MitoConfig::default()).await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir,
                path_type: PathType::Bare,
                options,
                skip_wal_replay: false,
                checkpoint: None,
            }),
        )
        .await
        .unwrap();
    // Check compaction window.
    {
        let region = engine.get_region(region_id).unwrap();
        let version = region.version();
        assert_eq!(
            Some(Duration::from_secs(7200)),
            version.compaction_time_window,
        );
        assert_eq!(
            Some(Duration::from_secs(7200)),
            version.options.compaction.time_window()
        );
    }
}
