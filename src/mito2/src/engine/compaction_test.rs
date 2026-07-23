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

use api::v1::region::{StrictWindow, compact_request};
use api::v1::{ColumnSchema, Rows};
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_recordbatch::{RecordBatches, SendableRecordBatchStream};
use datatypes::arrow::array::AsArray;
use datatypes::arrow::datatypes::TimestampMillisecondType;
use store_api::region_engine::{RegionEngine, RegionRole};
use store_api::region_request::AlterKind::SetRegionOptions;
use store_api::region_request::{
    EnterStagingRequest, PathType, RegionAlterRequest, RegionCloseRequest, RegionCompactRequest,
    RegionDeleteRequest, RegionFlushRequest, RegionOpenRequest, RegionRequest, SetRegionOption,
    StagingPartitionDirective,
};
use store_api::storage::{RegionId, ScanRequest};
use tokio::sync::Notify;

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::engine::listener::{CompactionListener, CompactionPlanningGate};
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
            RegionRequest::Flush(RegionFlushRequest::default()),
        )
        .await
        .unwrap();
    assert_eq!(0, result.affected_rows);
}

async fn flush(engine: &MitoEngine, region_id: RegionId) {
    let result = engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest::default()),
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
            RegionRequest::Delete(RegionDeleteRequest {
                rows,
                hint: None,
                partition_expr_version: None,
            }),
        )
        .await
        .unwrap();
    assert_eq!(row_cnt, result.affected_rows);

    let result = engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest::default()),
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
            .as_primitive::<TimestampMillisecondType>();
        res.extend((0..ts_col.len()).map(|i| ts_col.value(i)));
    }
    res
}

struct CompactionListenerGuard(Option<Arc<CompactionListener>>);

impl CompactionListenerGuard {
    fn new(listener: Arc<CompactionListener>) -> Self {
        Self(Some(listener))
    }

    fn release(mut self) {
        self.0.take().unwrap().wake();
    }
}

impl Drop for CompactionListenerGuard {
    fn drop(&mut self) {
        if let Some(listener) = self.0.take() {
            listener.wake();
        }
    }
}

#[tokio::test]
async fn test_region_b_progresses_while_same_worker_region_a_is_picking() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let region_a = RegionId::new(1, 1);
    let region_b = RegionId::new(2, 1);
    let gate = Arc::new(CompactionPlanningGate::new(region_a));
    let engine = env
        .create_engine_with(
            MitoConfig {
                num_workers: 1,
                min_compaction_interval: Duration::ZERO,
                ..Default::default()
            },
            None,
            Some(gate.clone()),
            None,
        )
        .await;

    for (region_id, table_name) in [(region_a, "region_a"), (region_b, "region_b")] {
        env.get_schema_metadata_manager()
            .register_region_table_info(
                region_id.table_id(),
                table_name,
                "test_catalog",
                "test_schema",
                None,
                env.get_kv_backend(),
            )
            .await;
        engine
            .handle_request(
                region_id,
                RegionRequest::Create(
                    CreateRequestBuilder::new()
                        .insert_option("compaction.type", "twcs")
                        .build(),
                ),
            )
            .await
            .unwrap();
    }

    let request = CreateRequestBuilder::new().build();
    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    let gate_guard = gate.arm();
    let engine_for_compaction = engine.clone();
    let region_a_compaction = tokio::spawn(async move {
        engine_for_compaction
            .handle_request(
                region_a,
                RegionRequest::Compact(RegionCompactRequest::default()),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_entered())
        .await
        .expect("region A planning did not reach the gate");

    tokio::time::timeout(
        Duration::from_secs(5),
        put_and_flush(&engine, region_a, &column_schemas, 10..20),
    )
    .await
    .expect("region A automatic compaction trigger did not finish");
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_schedule_attempts(2))
        .await
        .expect("region A automatic trigger did not reach the scheduler");
    assert_eq!(2, gate.schedule_attempt_count());
    assert_eq!(1, gate.invocation_count());

    let engine_for_region_b = engine.clone();
    let mut region_b_work = tokio::spawn(async move {
        put_and_flush(&engine_for_region_b, region_b, &column_schemas, 0..10).await;
    });
    tokio::time::timeout(Duration::from_secs(5), &mut region_b_work)
        .await
        .expect("region B was blocked by region A compaction planning")
        .expect("region B work task panicked");
    let followup_guard = gate.arm();
    gate_guard.release();
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_entered())
        .await
        .expect("coalesced region A trigger did not start its follow-up plan");
    tokio::time::timeout(Duration::from_secs(5), region_a_compaction)
        .await
        .expect("region A compaction task did not finish after gate release")
        .expect("region A compaction task panicked")
        .expect("region A compaction failed");
    assert_eq!(2, gate.invocation_count());
    followup_guard.release();
}

#[tokio::test]
async fn test_regular_trigger_while_picking_replans_after_no_plan() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let region_id = RegionId::new(7, 1);
    let gate = Arc::new(CompactionPlanningGate::new(region_id));
    let engine = env
        .create_engine_with(
            MitoConfig {
                min_compaction_interval: Duration::ZERO,
                ..Default::default()
            },
            None,
            Some(gate.clone()),
            None,
        )
        .await;
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "replan_after_no_plan",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;
    let create = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .build();
    let column_schemas = create
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(create))
        .await
        .unwrap();

    let first_plan_guard = gate.arm();
    put_and_flush(&engine, region_id, &column_schemas, 0..10).await;
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_entered())
        .await
        .expect("first automatic compaction did not reach the planning gate");

    put_and_flush(&engine, region_id, &column_schemas, 5..20).await;
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_schedule_attempts(2))
        .await
        .expect("second flush did not trigger automatic compaction");
    assert_eq!(1, gate.invocation_count());

    let second_plan_guard = gate.arm();
    first_plan_guard.release();
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_entered())
        .await
        .expect("coalesced regular trigger was lost after the first plan returned no plan");
    assert_eq!(2, gate.invocation_count());
    second_plan_guard.release();
}

#[tokio::test]
async fn test_regular_trigger_while_picking_replans_after_prepared_execution() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let region_id = RegionId::new(8, 1);
    let gate = Arc::new(CompactionPlanningGate::new(region_id));
    let engine = env
        .create_engine_with(
            MitoConfig {
                min_compaction_interval: Duration::from_secs(60 * 60),
                ..Default::default()
            },
            None,
            Some(gate.clone()),
            None,
        )
        .await;
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "replan_after_prepared",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;
    let create = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .build();
    let column_schemas = create
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(create))
        .await
        .unwrap();
    put_and_flush(&engine, region_id, &column_schemas, 0..10).await;
    put_and_flush(&engine, region_id, &column_schemas, 5..20).await;

    let first_plan_guard = gate.arm();
    let first_engine = engine.clone();
    let first = tokio::spawn(async move {
        first_engine
            .handle_request(
                region_id,
                RegionRequest::Compact(RegionCompactRequest::default()),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_entered())
        .await
        .expect("first regular compaction did not reach the planning gate");

    let expected_attempts = gate.schedule_attempt_count() + 1;
    let second_engine = engine.clone();
    let second = tokio::spawn(async move {
        second_engine
            .handle_request(
                region_id,
                RegionRequest::Compact(RegionCompactRequest::default()),
            )
            .await
    });
    tokio::time::timeout(
        Duration::from_secs(5),
        gate.wait_until_schedule_attempts(expected_attempts),
    )
    .await
    .expect("second regular compaction did not reach the scheduler");

    let commit_guard = gate.arm_commit();
    first_plan_guard.release();
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_commit_entered())
        .await
        .expect("first regular compaction did not produce a prepared execution");

    let followup_plan_guard = gate.arm();
    commit_guard.release();
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_entered())
        .await
        .expect("prepared execution did not immediately admit the retained regular follow-up");
    tokio::time::timeout(Duration::from_secs(5), first)
        .await
        .expect("first regular compaction waiter was not notified")
        .expect("first regular compaction task panicked")
        .expect("first regular compaction failed");
    assert!(!second.is_finished());

    followup_plan_guard.release();
    tokio::time::timeout(Duration::from_secs(5), second)
        .await
        .expect("retained regular compaction waiter was not notified")
        .expect("retained regular compaction task panicked")
        .expect("retained regular compaction failed");
}

#[tokio::test]
async fn test_pending_manual_compaction_finishes_before_queued_ddl() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let region_id = RegionId::new(6, 1);
    let gate = Arc::new(CompactionPlanningGate::new(region_id));
    let engine = env
        .create_engine_with(
            MitoConfig {
                num_workers: 1,
                min_compaction_interval: Duration::from_secs(60 * 60),
                ..Default::default()
            },
            None,
            Some(gate.clone()),
            None,
        )
        .await;
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "pending_manual_before_ddl",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;
    let create = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .build();
    let column_schemas = create
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(create))
        .await
        .unwrap();
    put_and_flush(&engine, region_id, &column_schemas, 0..10).await;
    put_and_flush(&engine, region_id, &column_schemas, 5..20).await;

    let commit_guard = gate.arm_commit();
    let regular_engine = engine.clone();
    let regular_task = tokio::spawn(async move {
        regular_engine
            .handle_request(
                region_id,
                RegionRequest::Compact(RegionCompactRequest::default()),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_commit_entered())
        .await
        .expect("regular compaction did not reach its non-cancellable commit gate");

    let manual_plan_guard = gate.arm();
    let manual_engine = engine.clone();
    let manual_task = tokio::spawn(async move {
        manual_engine
            .handle_request(
                region_id,
                RegionRequest::Compact(RegionCompactRequest {
                    options: compact_request::Options::StrictWindow(StrictWindow {
                        window_seconds: 60,
                    }),
                    ..Default::default()
                }),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_schedule_attempts(2))
        .await
        .expect("manual compaction did not reach the scheduler");
    assert!(!manual_task.is_finished());

    let ddl_engine = engine.clone();
    let ddl_task = tokio::spawn(async move {
        ddl_engine
            .handle_request(
                region_id,
                RegionRequest::EnterStaging(EnterStagingRequest {
                    partition_directive: StagingPartitionDirective::RejectAllWrites,
                }),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_cancel_requested())
        .await
        .expect("enter-staging DDL was not queued behind regular compaction");
    assert!(!ddl_task.is_finished());

    commit_guard.release();
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_entered())
        .await
        .expect("pending manual compaction was not planned after regular completion");
    assert_eq!(2, gate.invocation_count());
    assert!(!manual_task.is_finished());
    assert!(!ddl_task.is_finished());

    let pending_ddl_guard = gate.arm_pending_ddl_dispatch();
    manual_plan_guard.release();
    tokio::time::timeout(
        Duration::from_secs(5),
        gate.wait_until_pending_ddl_dispatch(),
    )
    .await
    .expect("manual result was not notified before pending DDL dispatch");
    tokio::time::timeout(Duration::from_secs(5), manual_task)
        .await
        .expect("manual compaction did not notify its waiter")
        .expect("manual compaction task panicked")
        .expect("manual compaction failed");
    assert!(!ddl_task.is_finished());

    pending_ddl_guard.release();
    tokio::time::timeout(Duration::from_secs(5), regular_task)
        .await
        .expect("regular compaction did not finish after commit release")
        .expect("regular compaction task panicked")
        .expect("regular compaction failed");
    tokio::time::timeout(Duration::from_secs(5), ddl_task)
        .await
        .expect("queued DDL did not finish after manual compaction")
        .expect("queued DDL task panicked")
        .expect("queued DDL failed");
    assert!(engine.get_region(region_id).unwrap().is_staging());
}

#[tokio::test]
async fn test_picking_close_reopen_ignores_old_plan() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let region_id = RegionId::new(3, 1);
    let gate = Arc::new(CompactionPlanningGate::new(region_id));
    let engine = env
        .create_engine_with(
            MitoConfig {
                num_workers: 1,
                ..Default::default()
            },
            None,
            Some(gate.clone()),
            None,
        )
        .await;
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "close_reopen",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;
    let create = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .build();
    let table_dir = create.table_dir.clone();
    let options = create.options.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(create))
        .await
        .unwrap();

    let gate_guard = gate.arm();
    let compact_engine = engine.clone();
    let compact_task = tokio::spawn(async move {
        compact_engine
            .handle_request(
                region_id,
                RegionRequest::Compact(RegionCompactRequest::default()),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_entered())
        .await
        .expect("planning did not reach the gate");

    engine
        .handle_request(
            region_id,
            RegionRequest::Close(RegionCloseRequest::default()),
        )
        .await
        .unwrap();
    let compact_err = tokio::time::timeout(Duration::from_secs(5), compact_task)
        .await
        .expect("closed region compaction waiter was not released")
        .expect("closed region compaction task panicked")
        .unwrap_err();
    assert_eq!(compact_err.status_code(), StatusCode::Cancelled);
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
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();
    engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap();

    gate_guard.release();
    tokio::time::timeout(Duration::from_secs(5), compact(&engine, region_id))
        .await
        .expect("replacement compaction was blocked by the stale plan");
    assert!(engine.is_region_exists(region_id));
    assert_eq!(2, gate.invocation_count());
}

#[tokio::test]
async fn test_enter_staging_waits_for_picking_logical_cancellation_ack() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let region_id = RegionId::new(4, 1);
    let gate = Arc::new(CompactionPlanningGate::new(region_id));
    let engine = env
        .create_engine_with(
            MitoConfig {
                num_workers: 1,
                ..Default::default()
            },
            None,
            Some(gate.clone()),
            None,
        )
        .await;
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "enter_staging",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Create(
                CreateRequestBuilder::new()
                    .insert_option("compaction.type", "twcs")
                    .build(),
            ),
        )
        .await
        .unwrap();

    let gate_guard = gate.arm();
    let compact_engine = engine.clone();
    let compact_task = tokio::spawn(async move {
        compact_engine
            .handle_request(
                region_id,
                RegionRequest::Compact(RegionCompactRequest::default()),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_entered())
        .await
        .expect("planning did not reach the gate");
    let staging_engine = engine.clone();
    let staging_task = tokio::spawn(async move {
        staging_engine
            .handle_request(
                region_id,
                RegionRequest::EnterStaging(EnterStagingRequest {
                    partition_directive: StagingPartitionDirective::RejectAllWrites,
                }),
            )
            .await
    });

    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_cancel_requested())
        .await
        .expect("enter-staging did not request picking cancellation");
    assert!(!compact_task.is_finished());
    assert!(!staging_task.is_finished());

    gate_guard.release();
    let compact_err = tokio::time::timeout(Duration::from_secs(5), compact_task)
        .await
        .expect("cancelled compaction waiter was not released")
        .expect("cancelled compaction task panicked")
        .unwrap_err();
    assert_eq!(compact_err.status_code(), StatusCode::Cancelled);
    tokio::time::timeout(Duration::from_secs(5), staging_task)
        .await
        .expect("enter-staging did not finish after cancellation acknowledgment")
        .expect("enter-staging task panicked")
        .expect("enter-staging request failed");
    assert!(engine.get_region(region_id).unwrap().is_staging());
}

#[tokio::test]
async fn test_worker_shutdown_fails_picking_waiter() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let region_id = RegionId::new(5, 1);
    let gate = Arc::new(CompactionPlanningGate::new(region_id));
    let engine = env
        .create_engine_with(
            MitoConfig {
                num_workers: 1,
                ..Default::default()
            },
            None,
            Some(gate.clone()),
            None,
        )
        .await;
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "worker_shutdown",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Create(
                CreateRequestBuilder::new()
                    .insert_option("compaction.type", "twcs")
                    .build(),
            ),
        )
        .await
        .unwrap();

    let gate_guard = gate.arm();
    let compact_engine = engine.clone();
    let compact_task = tokio::spawn(async move {
        compact_engine
            .handle_request(
                region_id,
                RegionRequest::Compact(RegionCompactRequest::default()),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), gate.wait_until_entered())
        .await
        .expect("planning did not reach the gate");

    tokio::time::timeout(Duration::from_secs(5), engine.stop())
        .await
        .expect("worker shutdown blocked on picking")
        .unwrap();
    let compact_err = tokio::time::timeout(Duration::from_secs(5), compact_task)
        .await
        .expect("worker shutdown did not release the compaction waiter")
        .expect("compaction task panicked during worker shutdown")
        .unwrap_err();
    assert_eq!(compact_err.status_code(), StatusCode::Cancelled);
    gate_guard.release();
}

#[tokio::test]
async fn test_compaction_region() {
    test_compaction_region_with_format(false).await;
    test_compaction_region_with_format(true).await;
}

async fn test_compaction_region_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
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
    test_infer_compaction_time_window_with_format(false).await;
    test_infer_compaction_time_window_with_format(true).await;
}

async fn test_infer_compaction_time_window_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
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
    test_compaction_overlapping_files_with_format(false).await;
    test_compaction_overlapping_files_with_format(true).await;
}

async fn test_compaction_overlapping_files_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
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
    test_compaction_region_with_overlapping_with_format(false).await;
    test_compaction_region_with_overlapping_with_format(true).await;
}

async fn test_compaction_region_with_overlapping_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
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
    test_compaction_region_with_overlapping_delete_all_with_format(false).await;
    test_compaction_region_with_overlapping_delete_all_with_format(true).await;
}

async fn test_compaction_region_with_overlapping_delete_all_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
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
    put_and_flush(&engine, region_id, &column_schemas, 1200..2400).await; // window 3600
    put_and_flush(&engine, region_id, &column_schemas, 2400..3600).await; // window 3600
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
    test_readonly_during_compaction_with_format(false).await;
    test_readonly_during_compaction_with_format(true).await;
}

async fn test_readonly_during_compaction_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let listener = Arc::new(CompactionListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig {
                default_flat_format: flat_format,
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
    let listener_guard = CompactionListenerGuard::new(listener.clone());
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
    listener_guard.release();

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
async fn test_enter_staging_cancels_inflight_local_compaction_before_commit() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let listener = Arc::new(CompactionListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig {
                max_background_purges: 1,
                ..Default::default()
            },
            None,
            Some(listener.clone()),
            None,
        )
        .await;

    let region_id = RegionId::new(2048, 1);
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
    let _listener_guard = CompactionListenerGuard::new(listener.clone());

    put_and_flush(&engine, region_id, &column_schemas, 0..10).await;
    put_and_flush(&engine, region_id, &column_schemas, 5..20).await;

    tokio::time::timeout(Duration::from_secs(5), listener.wait_handle_finished())
        .await
        .expect("local compaction did not reach its pre-commit gate");

    tokio::time::timeout(
        Duration::from_secs(5),
        engine.handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::RejectAllWrites,
            }),
        ),
    )
    .await
    .expect("enter-staging waited for the blocked local compaction")
    .expect("enter-staging request failed");
    assert!(engine.get_region(region_id).unwrap().is_staging());
}

#[tokio::test]
async fn test_manual_compaction_returns_cancelled_when_enter_staging_cancels_it() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let listener = Arc::new(CompactionListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig {
                max_background_purges: 1,
                ..Default::default()
            },
            None,
            Some(listener.clone()),
            None,
        )
        .await;

    let region_id = RegionId::new(2050, 1);
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
    let _listener_guard = CompactionListenerGuard::new(listener.clone());

    put_and_flush(&engine, region_id, &column_schemas, 0..10).await;
    put_and_flush(&engine, region_id, &column_schemas, 5..20).await;

    let engine_cloned = engine.clone();
    let compact = tokio::spawn(async move {
        engine_cloned
            .handle_request(
                region_id,
                RegionRequest::Compact(RegionCompactRequest::default()),
            )
            .await
    });

    tokio::time::timeout(Duration::from_secs(5), listener.wait_handle_finished())
        .await
        .expect("manual compaction did not reach its pre-commit gate");

    tokio::time::timeout(
        Duration::from_secs(5),
        engine.handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::RejectAllWrites,
            }),
        ),
    )
    .await
    .expect("enter-staging waited for the blocked manual compaction")
    .expect("enter-staging request failed");

    let err = tokio::time::timeout(Duration::from_secs(5), compact)
        .await
        .expect("cancelled manual compaction waiter was not released")
        .expect("manual compaction task panicked")
        .unwrap_err();
    assert_eq!(err.status_code(), StatusCode::Cancelled);
}

#[tokio::test]
async fn test_compaction_update_time_window() {
    test_compaction_update_time_window_with_format(false).await;
    test_compaction_update_time_window_with_format(true).await;
}

async fn test_compaction_update_time_window_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
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
    test_change_region_compaction_window_with_format(false).await;
    test_change_region_compaction_window_with_format(true).await;
}

async fn test_change_region_compaction_window_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
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
    let engine = env
        .reopen_engine(
            engine,
            MitoConfig {
                default_flat_format: flat_format,
                ..Default::default()
            },
        )
        .await;
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
                requirements: Default::default(),
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
    test_open_overwrite_compaction_window_with_format(false).await;
    test_open_overwrite_compaction_window_with_format(true).await;
}

async fn test_open_overwrite_compaction_window_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
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
    let engine = env
        .reopen_engine(
            engine,
            MitoConfig {
                default_flat_format: flat_format,
                ..Default::default()
            },
        )
        .await;
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
                requirements: Default::default(),
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
