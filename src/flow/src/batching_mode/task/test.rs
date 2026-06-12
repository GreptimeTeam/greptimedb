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

use std::collections::{BTreeMap, BTreeSet, HashMap};

use catalog::RegisterTableRequest;
use catalog::memory::MemoryCatalogManager;
use client::OutputWithMetrics;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_error::ext::BoxedError;
use common_error::mock::MockError;
use common_error::status_code::StatusCode;
use common_query::Output;
use common_recordbatch::RecordBatch;
use common_recordbatch::adapter::{RecordBatchMetrics, RegionWatermarkEntry};
use datatypes::data_type::ConcreteDataType as CDT;
use datatypes::schema::ColumnSchema;
use datatypes::vectors::{TimestampMillisecondVector, UInt32Vector, VectorRef};
use pretty_assertions::assert_eq;
use query::options::{
    FLOW_INCREMENTAL_AFTER_SEQS, FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY, QueryOptions,
};
use session::context::QueryContext;
use snafu::ResultExt;
use table::test_util::MemTable;

use super::*;
use crate::batching_mode::checkpoint::{
    CHECKPOINT_DECISION_ADVANCE, CHECKPOINT_DECISION_FALLBACK, CHECKPOINT_REASON_NONE,
    FlowCheckpointDecision, FlowQueryFallbackReason,
};
use crate::batching_mode::state::CheckpointMode;
use crate::batching_mode::time_window::find_time_window_expr;
use crate::test_utils::create_test_query_engine;

fn incremental_batch_opts() -> Arc<BatchingModeOptions> {
    Arc::new(BatchingModeOptions {
        experimental_enable_incremental_read: true,
        ..Default::default()
    })
}

async fn new_test_task_and_plan_with_missing_sink() -> (BatchingTask, LogicalPlan) {
    new_test_task_engine_and_plan_with_query(
        "SELECT number, ts FROM numbers_with_ts",
        "missing_sink",
    )
    .await
    .into_task_and_plan()
}

struct TestTaskParts {
    task: BatchingTask,
    query_engine: QueryEngineRef,
    plan: LogicalPlan,
}

impl TestTaskParts {
    fn into_task_and_plan(self) -> (BatchingTask, LogicalPlan) {
        (self.task, self.plan)
    }
}

async fn new_test_task_engine_and_plan_with_query(query: &str, sink_table: &str) -> TestTaskParts {
    new_test_task_engine_and_plan_with_query_and_opts(query, sink_table, incremental_batch_opts())
        .await
}

async fn new_test_task_engine_and_plan_with_query_and_opts(
    query: &str,
    sink_table: &str,
    batch_opts: Arc<BatchingModeOptions>,
) -> TestTaskParts {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let plan = sql_to_df_plan(
        ctx.clone(),
        query_engine.clone(),
        "SELECT number, ts FROM numbers_with_ts",
        true,
    )
    .await
    .unwrap();
    let (_tx, rx) = tokio::sync::oneshot::channel();

    let task = BatchingTask::try_new(TaskArgs {
        flow_id: 1,
        query,
        plan: plan.clone(),
        time_window_expr: None,
        expire_after: None,
        sink_table_name: [
            "greptime".to_string(),
            "public".to_string(),
            sink_table.to_string(),
        ],
        source_table_names: vec![[
            "greptime".to_string(),
            "public".to_string(),
            "numbers_with_ts".to_string(),
        ]],
        query_ctx: ctx,
        catalog_manager: query_engine.engine_state().catalog_manager().clone(),
        shutdown_rx: rx,
        batch_opts,
        flow_eval_interval: None,
    })
    .unwrap();

    TestTaskParts {
        task,
        query_engine,
        plan,
    }
}

#[tokio::test]
async fn test_incremental_read_is_disabled_by_default() {
    let task = new_test_task_engine_and_plan_with_query_and_opts(
        "SELECT number, ts FROM numbers_with_ts",
        "numbers_with_ts",
        Arc::new(BatchingModeOptions::default()),
    )
    .await
    .task;

    assert!(task.state.read().unwrap().is_incremental_disabled());
}

#[tokio::test]
async fn test_dirty_time_windows_uses_batch_opts() {
    let task = new_test_task_engine_and_plan_with_query_and_opts(
        "SELECT number, ts FROM numbers_with_ts",
        "numbers_with_ts",
        Arc::new(BatchingModeOptions {
            experimental_max_filter_num_per_query: 7,
            experimental_time_window_merge_threshold: 11,
            ..Default::default()
        }),
    )
    .await
    .task;

    let state = task.state.read().unwrap();
    assert_eq!(7, state.dirty_time_windows.max_filter_num_per_query());
    assert_eq!(11, state.dirty_time_windows.time_window_merge_threshold());
}

#[tokio::test]
async fn test_execute_once_serialized_waits_for_execution_lock() {
    let TestTaskParts {
        task, query_engine, ..
    } = new_test_task_engine_and_plan_with_query(
        "SELECT number, ts FROM numbers_with_ts",
        "missing_sink",
    )
    .await;
    let (frontend_client, _handler) =
        FrontendClient::from_empty_grpc_handler(QueryOptions::default());
    let frontend_client = Arc::new(frontend_client);

    let guard = task.execution_lock.clone().lock_owned().await;
    let task_to_run = task.clone();
    let query_engine_to_run = query_engine.clone();
    let frontend_client_to_run = frontend_client.clone();
    let exec = tokio::spawn(async move {
        task_to_run
            .execute_once_serialized(&query_engine_to_run, &frontend_client_to_run, None)
            .await
    });

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !exec.is_finished(),
        "execute_once_serialized should wait for execution_lock"
    );

    drop(guard);
    tokio::time::timeout(Duration::from_secs(1), exec)
        .await
        .expect("execute_once_serialized should finish once execution_lock is released")
        .expect("execute_once_serialized task should not panic")
        .expect_err("missing sink should fail after acquiring execution_lock");
}

async fn new_time_window_test_task_with_query(query: &str) -> TestTaskParts {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let plan_query = "SELECT number, date_bin(INTERVAL '5 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window, number";
    let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), plan_query, true)
        .await
        .unwrap();
    let (column_name, time_window_expr, _, df_schema) = find_time_window_expr(
        &plan,
        query_engine.engine_state().catalog_manager().clone(),
        ctx.clone(),
    )
    .await
    .unwrap();
    let time_window_expr = time_window_expr.map(|expr| {
        TimeWindowExpr::from_expr(
            &expr,
            &column_name,
            &df_schema,
            &query_engine.engine_state().session_state(),
        )
        .unwrap()
    });
    let (_tx, rx) = tokio::sync::oneshot::channel();

    let task = BatchingTask::try_new(TaskArgs {
        flow_id: 1,
        query,
        plan: plan.clone(),
        time_window_expr,
        expire_after: None,
        sink_table_name: [
            "greptime".to_string(),
            "public".to_string(),
            "missing_sink".to_string(),
        ],
        source_table_names: vec![[
            "greptime".to_string(),
            "public".to_string(),
            "numbers_with_ts".to_string(),
        ]],
        query_ctx: ctx,
        catalog_manager: query_engine.engine_state().catalog_manager().clone(),
        shutdown_rx: rx,
        batch_opts: incremental_batch_opts(),
        flow_eval_interval: None,
    })
    .unwrap();

    TestTaskParts {
        task,
        query_engine,
        plan,
    }
}

fn register_number_only_sink(query_engine: &QueryEngineRef, table_name: &str) {
    let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
        "number",
        CDT::uint32_datatype(),
        false,
    )]));
    let columns: Vec<VectorRef> = vec![Arc::new(UInt32Vector::from_slice([1_u32]))];
    let recordbatch = RecordBatch::new(schema, columns).unwrap();
    let table = MemTable::table(table_name, recordbatch);
    let request = RegisterTableRequest {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_SCHEMA_NAME.to_string(),
        table_name: table_name.to_string(),
        table_id: 9001,
        table,
    };
    let catalog_manager = query_engine.engine_state().catalog_manager();
    let memory_catalog = catalog_manager
        .as_any()
        .downcast_ref::<MemoryCatalogManager>()
        .unwrap();
    memory_catalog.register_table_sync(request).unwrap();
}

fn register_auto_created_aggregate_sink(query_engine: &QueryEngineRef, table_name: &str) {
    let schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("number", CDT::uint32_datatype(), true),
        ColumnSchema::new("ts", CDT::timestamp_millisecond_datatype(), false).with_time_index(true),
        ColumnSchema::new("update_at", CDT::timestamp_millisecond_datatype(), true),
    ]));
    let columns: Vec<VectorRef> = vec![
        Arc::new(UInt32Vector::from_slice([1_u32])),
        Arc::new(TimestampMillisecondVector::from_slice([0_i64])),
        Arc::new(TimestampMillisecondVector::from_slice([0_i64])),
    ];
    let recordbatch = RecordBatch::new(schema, columns).unwrap();
    let table = MemTable::table(table_name, recordbatch);
    let request = RegisterTableRequest {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_SCHEMA_NAME.to_string(),
        table_name: table_name.to_string(),
        table_id: 9002,
        table,
    };
    let catalog_manager = query_engine.engine_state().catalog_manager();
    let memory_catalog = catalog_manager
        .as_any()
        .downcast_ref::<MemoryCatalogManager>()
        .unwrap();
    memory_catalog.register_table_sync(request).unwrap();
}

fn dirty_marker() -> DirtyTimeWindows {
    let mut dirty = DirtyTimeWindows::default();
    dirty.set_dirty();
    dirty
}

fn flow_error_with_status(status_code: StatusCode) -> Error {
    Err::<(), _>(BoxedError::new(MockError::new(status_code)))
        .context(crate::error::ExternalSnafu)
        .unwrap_err()
}

fn dirty_range(start: i64, end: i64) -> DirtyTimeWindows {
    let mut dirty = DirtyTimeWindows::default();
    dirty.add_window(
        Timestamp::new_second(start),
        Some(Timestamp::new_second(end)),
    );
    dirty
}

fn expire_after_for_retention_filter_test() -> i64 {
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs();
    (now_secs - 10) as i64
}

fn aggregate_time_window_sink_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        ColumnSchema::new("number", CDT::uint32_datatype(), false),
        ColumnSchema::new("time_window", CDT::timestamp_millisecond_datatype(), false)
            .with_time_index(true),
    ]))
}

async fn assert_unscoped_failure_restore(
    consumed_dirty_windows: DirtyTimeWindows,
    current_dirty_windows: DirtyTimeWindows,
    expected_len: usize,
    expected_window_size_secs: u64,
) {
    let (task, plan) = new_test_task_and_plan_with_missing_sink().await;
    {
        let mut state = task.state.write().unwrap();
        state.dirty_time_windows.clean();
        state
            .dirty_time_windows
            .add_dirty_windows(&current_dirty_windows);
    }
    let unscoped_query = PlanInfo {
        plan,
        dirty_restore: DirtyRestore::Unscoped(consumed_dirty_windows),
        coverage: QueryCoverage::UnfilteredFull,
    };

    task.handle_executed_query_failure(Some(&unscoped_query));

    let state = task.state.read().unwrap();
    assert_eq!(state.dirty_time_windows.len(), expected_len);
    assert_eq!(
        state.dirty_time_windows.window_size(),
        std::time::Duration::from_secs(expected_window_size_secs)
    );
}

fn output_with_region_watermarks(
    watermarks: impl IntoIterator<Item = (u64, Option<u64>)>,
) -> OutputWithMetrics {
    let result = OutputWithMetrics::from_output(Output::new_with_affected_rows(0));
    result.metrics.update(Some(RecordBatchMetrics {
        region_watermarks: watermarks
            .into_iter()
            .map(|(region_id, watermark)| RegionWatermarkEntry {
                region_id,
                watermark,
            })
            .collect(),
        ..Default::default()
    }));
    result.metrics.mark_ready();
    result
}

#[test]
fn test_apply_query_result_to_state_advances_full_snapshot_to_incremental() {
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);
    let result = output_with_region_watermarks([(1_u64, Some(10_u64)), (2_u64, Some(20_u64))]);

    let decision = BatchingTask::apply_query_result_to_state(
        &mut state,
        &result,
        std::time::Duration::from_millis(1),
        &QueryCoverage::UnfilteredFull,
    );

    assert_eq!(
        decision,
        FlowCheckpointDecision::AdvancedFromFullSnapshot {
            participating_regions: 2,
            watermarks: 2,
        }
    );
    assert_eq!(state.checkpoint_mode(), CheckpointMode::Incremental);
    assert_eq!(
        state.checkpoints(),
        &BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)])
    );
}

#[test]
fn test_apply_query_result_to_state_stays_full_snapshot_when_incremental_disabled() {
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);
    state.disable_incremental();
    assert!(state.is_incremental_disabled());
    assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);

    let result = output_with_region_watermarks([(1_u64, Some(10_u64)), (2_u64, Some(20_u64))]);
    let decision = BatchingTask::apply_query_result_to_state(
        &mut state,
        &result,
        std::time::Duration::from_millis(1),
        &QueryCoverage::UnfilteredFull,
    );

    // Should NOT claim advancement to incremental; should fallback with correct reason.
    assert_eq!(
        decision,
        FlowCheckpointDecision::FallbackToFullSnapshot {
            previous_mode: CheckpointMode::FullSnapshot,
            reason: FlowQueryFallbackReason::IncrementalDisabled,
        }
    );
    assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
    assert!(state.is_incremental_disabled());
    // Checkpoints are still updated even if mode doesn't advance.
    assert_eq!(
        state.checkpoints(),
        &BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)])
    );
}

#[test]
fn test_apply_query_result_to_state_rejects_unproved_watermark() {
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);
    let result = output_with_region_watermarks([(1_u64, Some(10_u64)), (2_u64, None)]);

    let decision = BatchingTask::apply_query_result_to_state(
        &mut state,
        &result,
        std::time::Duration::from_millis(1),
        &QueryCoverage::UnfilteredFull,
    );

    assert_eq!(
        decision,
        FlowCheckpointDecision::FallbackToFullSnapshot {
            previous_mode: CheckpointMode::FullSnapshot,
            reason: FlowQueryFallbackReason::IncompleteRegionWatermark,
        }
    );
    assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
    assert!(state.checkpoints().is_empty());
}

#[test]
fn test_apply_query_result_to_state_reports_missing_watermark() {
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);
    let result = OutputWithMetrics::from_output(Output::new_with_affected_rows(0));

    let decision = BatchingTask::apply_query_result_to_state(
        &mut state,
        &result,
        std::time::Duration::from_millis(1),
        &QueryCoverage::UnfilteredFull,
    );

    assert_eq!(
        decision,
        FlowCheckpointDecision::FallbackToFullSnapshot {
            previous_mode: CheckpointMode::FullSnapshot,
            reason: FlowQueryFallbackReason::MissingRegionWatermark,
        }
    );
    assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
    assert!(state.checkpoints().is_empty());
}

#[test]
fn test_apply_query_result_to_state_advances_incremental_subset() {
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);
    state.advance_checkpoints(HashMap::from([
        (1_u64, 10_u64),
        (2_u64, 20_u64),
        (3_u64, 30_u64),
    ]));
    let result = output_with_region_watermarks([(1_u64, Some(12_u64)), (3_u64, Some(35_u64))]);

    let decision = BatchingTask::apply_query_result_to_state(
        &mut state,
        &result,
        std::time::Duration::from_millis(1),
        &QueryCoverage::IncrementalDelta,
    );

    assert_eq!(
        decision,
        FlowCheckpointDecision::AdvancedIncremental {
            participating_regions: 2,
            watermarks: 2,
        }
    );
    assert_eq!(state.checkpoint_mode(), CheckpointMode::Incremental);
    assert_eq!(
        state.checkpoints(),
        &BTreeMap::from([(1_u64, 12_u64), (2_u64, 20_u64), (3_u64, 35_u64)])
    );
}

#[test]
fn test_scoped_base_repair_with_dirty_backlog_starts_fenced_repair_from_full_snapshot() {
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);
    // Set a dirty window so that ScopedBaseRepair enters fenced repair instead
    // of advancing directly; coverage type plus live dirty-window presence now
    // determines this transition.
    state
        .dirty_time_windows
        .add_window(Timestamp::new_second(10), Some(Timestamp::new_second(20)));
    let high = BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]);
    let result = output_with_region_watermarks([(1_u64, Some(10_u64)), (2_u64, Some(20_u64))]);

    let decision = BatchingTask::apply_query_result_to_state(
        &mut state,
        &result,
        std::time::Duration::from_millis(1),
        &QueryCoverage::ScopedBaseRepair,
    );

    assert_eq!(
        decision,
        FlowCheckpointDecision::ContinuedFencedRepair {
            pending_windows: 1,
            watermarks: 2,
        }
    );
    assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
    assert_eq!(state.dirty_time_windows.len(), 1);
    assert_eq!(state.pending_fenced_repair().unwrap().high(), &high);
}

fn next_fenced_repair_filter(state: &mut TaskState, window_cnt: usize) -> FilterExprInfo {
    state
        .gen_scoped_filter_exprs(
            "ts",
            None,
            chrono::Duration::seconds(10),
            window_cnt,
            1,
            None,
        )
        .unwrap()
        .unwrap()
}

#[test]
fn test_fenced_repair_chunk_with_pending_windows_stays_full_snapshot() {
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);
    state
        .dirty_time_windows
        .add_window(Timestamp::new_second(10), Some(Timestamp::new_second(20)));
    state
        .dirty_time_windows
        .add_window(Timestamp::new_second(100), Some(Timestamp::new_second(110)));

    let high = BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]);
    state.start_fenced_repair(high.clone()).unwrap();
    let _filter = next_fenced_repair_filter(&mut state, 1);
    assert_eq!(
        state
            .pending_fenced_repair()
            .unwrap()
            .pending_windows()
            .len(),
        1
    );

    let decision = BatchingTask::apply_query_result_to_state(
        &mut state,
        &output_with_region_watermarks([(1_u64, Some(10_u64)), (2_u64, Some(20_u64))]),
        std::time::Duration::from_millis(1),
        &QueryCoverage::FencedRepairChunk { high },
    );

    assert_eq!(
        decision,
        FlowCheckpointDecision::ContinuedFencedRepair {
            pending_windows: 1,
            watermarks: 2,
        }
    );
    assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
    assert!(state.checkpoints().is_empty());
    assert_eq!(
        state
            .pending_fenced_repair()
            .unwrap()
            .pending_windows()
            .len(),
        1
    );
    assert_eq!(state.dirty_time_windows.len(), 2);
}

#[test]
fn test_continued_fenced_repair_uses_pending_snapshot_not_later_live_dirty() {
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);
    state
        .dirty_time_windows
        .add_window(Timestamp::new_second(10), Some(Timestamp::new_second(15)));
    state
        .dirty_time_windows
        .add_window(Timestamp::new_second(100), Some(Timestamp::new_second(105)));

    let high = BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]);
    state.start_fenced_repair(high.clone()).unwrap();

    // Make the two queues distinguishable: the fenced repair should keep using
    // the pending snapshot captured above, not this later live dirty window.
    state.dirty_time_windows.clean();
    state.dirty_time_windows.add_window(
        Timestamp::new_second(1000),
        Some(Timestamp::new_second(1005)),
    );
    assert_eq!(state.dirty_time_windows.len(), 1);

    let first_filter = next_fenced_repair_filter(&mut state, 1);
    assert_eq!(
        first_filter.time_ranges,
        vec![(Timestamp::new_second(10), Timestamp::new_second(15))]
    );

    let decision = BatchingTask::apply_query_result_to_state(
        &mut state,
        &output_with_region_watermarks([(1_u64, Some(10_u64)), (2_u64, Some(20_u64))]),
        std::time::Duration::from_millis(1),
        &QueryCoverage::FencedRepairChunk { high },
    );
    assert_eq!(
        decision,
        FlowCheckpointDecision::ContinuedFencedRepair {
            pending_windows: 1,
            watermarks: 2,
        }
    );

    let second_filter = next_fenced_repair_filter(&mut state, 1);
    assert_eq!(
        second_filter.time_ranges,
        vec![(Timestamp::new_second(100), Timestamp::new_second(105))]
    );
    assert!(state.fenced_repair_pending_is_empty());
    assert_eq!(state.dirty_time_windows.len(), 1);
}

#[test]
fn test_final_fenced_repair_chunk_advances_to_high() {
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);
    state
        .dirty_time_windows
        .add_window(Timestamp::new_second(10), Some(Timestamp::new_second(20)));

    let high = BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]);
    state.start_fenced_repair(high.clone()).unwrap();
    let _filter = next_fenced_repair_filter(&mut state, 1);
    assert!(state.fenced_repair_pending_is_empty());

    let decision = BatchingTask::apply_query_result_to_state(
        &mut state,
        &output_with_region_watermarks([(1_u64, Some(10_u64)), (2_u64, Some(20_u64))]),
        std::time::Duration::from_millis(1),
        &QueryCoverage::FencedRepairChunk { high: high.clone() },
    );

    assert_eq!(
        decision,
        FlowCheckpointDecision::AdvancedFromFullSnapshot {
            participating_regions: 2,
            watermarks: 2,
        }
    );
    assert_eq!(state.checkpoint_mode(), CheckpointMode::Incremental);
    assert_eq!(state.checkpoints(), &high);
    assert!(state.pending_fenced_repair().is_none());
    assert_eq!(state.dirty_time_windows.len(), 1);
}

#[test]
fn test_fenced_repair_watermarks_require_exact_high() {
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);
    state
        .dirty_time_windows
        .add_window(Timestamp::new_second(10), Some(Timestamp::new_second(20)));

    state
        .start_fenced_repair(BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]))
        .unwrap();
    let participating_regions = BTreeSet::from([1_u64, 2_u64]);

    assert!(state.fenced_repair_watermarks_match_high(
        &participating_regions,
        &HashMap::from([(1_u64, 10_u64), (2_u64, 20_u64)])
    ));
    assert!(!state.fenced_repair_watermarks_match_high(
        &participating_regions,
        &HashMap::from([(1_u64, 11_u64), (2_u64, 20_u64)])
    ));
    assert!(!state.fenced_repair_watermarks_match_high(
        &participating_regions,
        &HashMap::from([(1_u64, 10_u64)])
    ));
}

#[test]
fn test_fenced_repair_chunk_watermark_mismatch_restores_pending_but_consumes_inflight() {
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);
    state
        .dirty_time_windows
        .add_window(Timestamp::new_second(10), Some(Timestamp::new_second(15)));
    state
        .dirty_time_windows
        .add_window(Timestamp::new_second(100), Some(Timestamp::new_second(105)));

    let high = BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]);
    state.start_fenced_repair(high.clone()).unwrap();
    // Isolate the restore assertions: the real state keeps live dirty windows
    // during fenced repair, but clearing them here proves mismatch restoration
    // brings back only the remaining pending window. The successful in-flight
    // chunk is consumed.
    state.dirty_time_windows.clean();

    let _filter = next_fenced_repair_filter(&mut state, 1);
    assert_eq!(
        state
            .pending_fenced_repair()
            .unwrap()
            .pending_windows()
            .len(),
        1
    );
    assert!(state.dirty_time_windows.is_empty());

    let decision = BatchingTask::apply_query_result_to_state(
        &mut state,
        &output_with_region_watermarks([(1_u64, Some(11_u64)), (2_u64, Some(20_u64))]),
        std::time::Duration::from_millis(1),
        &QueryCoverage::FencedRepairChunk { high },
    );

    assert_eq!(
        decision,
        FlowCheckpointDecision::FallbackToFullSnapshot {
            previous_mode: CheckpointMode::FullSnapshot,
            reason: FlowQueryFallbackReason::IncompleteRegionWatermark,
        }
    );
    assert!(state.pending_fenced_repair().is_none());
    assert_eq!(state.dirty_time_windows.len(), 1);
}

#[tokio::test]
async fn test_fenced_repair_mismatch_next_plan_is_scoped_base_repair() {
    let TestTaskParts {
        task,
        query_engine,
        ..
    } = new_time_window_test_task_with_query(
        "SELECT number, date_bin(INTERVAL '5 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window, number",
    )
    .await;
    let high = BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]);
    let _filter = {
        let mut state = task.state.write().unwrap();
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(10), Some(Timestamp::new_second(15)));
        state.start_fenced_repair(high.clone()).unwrap();
        next_fenced_repair_filter(&mut state, 1)
    };

    {
        let mut state = task.state.write().unwrap();
        let decision = BatchingTask::apply_query_result_to_state(
            &mut state,
            &output_with_region_watermarks([(1_u64, Some(11_u64)), (2_u64, Some(20_u64))]),
            std::time::Duration::from_millis(1),
            &QueryCoverage::FencedRepairChunk { high },
        );
        assert_eq!(
            decision,
            FlowCheckpointDecision::FallbackToFullSnapshot {
                previous_mode: CheckpointMode::FullSnapshot,
                reason: FlowQueryFallbackReason::IncompleteRegionWatermark,
            }
        );
        assert!(state.pending_fenced_repair().is_none());
    }

    let plan = task
        .gen_query_with_time_window(
            query_engine,
            &aggregate_time_window_sink_schema(),
            &[],
            false,
            Some(1),
        )
        .await
        .unwrap()
        .expect("mismatch should keep live dirty backlog for a fresh scoped repair");
    assert!(matches!(plan.coverage, QueryCoverage::ScopedBaseRepair));
}

#[test]
fn test_apply_query_failure_to_state_falls_back_from_incremental() {
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);
    state.advance_checkpoints(HashMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]));
    assert_eq!(state.checkpoint_mode(), CheckpointMode::Incremental);

    let decision = BatchingTask::apply_query_failure_to_state(
        &mut state,
        std::time::Duration::from_millis(1),
        &QueryCoverage::IncrementalDelta,
        FlowQueryFallbackReason::IncrementalQueryFailure,
    );

    assert_eq!(
        decision,
        Some(FlowCheckpointDecision::FallbackToFullSnapshot {
            previous_mode: CheckpointMode::Incremental,
            reason: FlowQueryFallbackReason::IncrementalQueryFailure,
        })
    );
    assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
    assert_eq!(
        state.checkpoints(),
        &BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)])
    );
}

#[test]
fn test_apply_query_failure_to_state_records_full_snapshot_failure() {
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);

    let decision = BatchingTask::apply_query_failure_to_state(
        &mut state,
        std::time::Duration::from_millis(1),
        &QueryCoverage::UnfilteredFull,
        FlowQueryFallbackReason::QueryFailure,
    );

    assert_eq!(
        decision,
        Some(FlowCheckpointDecision::FallbackToFullSnapshot {
            previous_mode: CheckpointMode::FullSnapshot,
            reason: FlowQueryFallbackReason::QueryFailure,
        })
    );
    assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
    assert!(state.checkpoints().is_empty());
}

#[test]
fn test_query_failure_reason_distinguishes_fenced_repair_stale_fence() {
    let err = flow_error_with_status(StatusCode::RequestOutdated);

    assert_eq!(
        BatchingTask::query_failure_reason(
            &err,
            &QueryCoverage::FencedRepairChunk {
                high: BTreeMap::new(),
            },
        ),
        FlowQueryFallbackReason::SnapshotFenceExpired
    );
    assert_eq!(
        BatchingTask::query_failure_reason(&err, &QueryCoverage::IncrementalDelta),
        FlowQueryFallbackReason::StaleCursor
    );

    let generic_err = flow_error_with_status(StatusCode::Unexpected);
    assert_eq!(
        BatchingTask::query_failure_reason(&generic_err, &QueryCoverage::ScopedBaseRepair),
        FlowQueryFallbackReason::QueryFailure
    );
    assert_eq!(
        BatchingTask::query_failure_reason(&generic_err, &QueryCoverage::IncrementalDelta),
        FlowQueryFallbackReason::IncrementalQueryFailure
    );
}

#[test]
fn test_fenced_repair_coverage_produces_snapshot_seq_map_for_distributed_metadata_path() {
    // Covers the metadata boundary between QueryCoverage and the
    // frontend/distributed client API: only FencedRepairChunk carries a
    // non-empty snapshot_seqs map so the datanode can bind per-region
    // snapshot upper bounds against the frozen high H. Other coverage
    // variants must produce an empty map.
    let high = BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]);
    let coverage = QueryCoverage::FencedRepairChunk { high: high.clone() };
    assert_eq!(
        coverage.snapshot_seqs(),
        HashMap::from([(1_u64, 10_u64), (2_u64, 20_u64)])
    );

    assert!(QueryCoverage::UnfilteredFull.snapshot_seqs().is_empty());
    assert!(QueryCoverage::ScopedBaseRepair.snapshot_seqs().is_empty());
    assert!(QueryCoverage::IncrementalDelta.snapshot_seqs().is_empty());
}

#[tokio::test]
async fn test_fenced_repair_stale_fence_next_plan_is_scoped_base_repair() {
    let TestTaskParts {
        task,
        query_engine,
        ..
    } = new_time_window_test_task_with_query(
        "SELECT number, date_bin(INTERVAL '5 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window, number",
    )
    .await;
    let high = BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]);
    let filter = {
        let mut state = task.state.write().unwrap();
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(10), Some(Timestamp::new_second(15)));
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(100), Some(Timestamp::new_second(105)));
        state.start_fenced_repair(high.clone()).unwrap();
        next_fenced_repair_filter(&mut state, 1)
    };

    {
        let mut state = task.state.write().unwrap();
        let decision = BatchingTask::apply_query_failure_to_state(
            &mut state,
            std::time::Duration::from_millis(1),
            &QueryCoverage::FencedRepairChunk { high },
            FlowQueryFallbackReason::SnapshotFenceExpired,
        );
        assert_eq!(
            decision,
            Some(FlowCheckpointDecision::FallbackToFullSnapshot {
                previous_mode: CheckpointMode::FullSnapshot,
                reason: FlowQueryFallbackReason::SnapshotFenceExpired,
            })
        );
        assert!(state.pending_fenced_repair().is_none());

        // Simulate the outer execution failure restore for the in-flight chunk.
        state.restore_scoped_windows(&filter);
    }

    let plan = task
        .gen_query_with_time_window(
            query_engine,
            &aggregate_time_window_sink_schema(),
            &[],
            false,
            Some(1),
        )
        .await
        .unwrap()
        .expect("stale fence should restore dirty windows for a fresh scoped repair");
    assert!(matches!(plan.coverage, QueryCoverage::ScopedBaseRepair));
}

#[test]
fn test_fenced_repair_transient_non_stale_failure_retries_same_high() {
    // Opposite of stale-fence abandon: a non-RequestOutdated failure on a
    // fenced repair chunk should NOT abandon the pending repair. The same
    // high H is retained, the failed in-flight window goes back into
    // pending_windows (not live dirty_time_windows), and the next execution
    // can re-attempt the same fenced repair chunk.
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);
    state
        .dirty_time_windows
        .add_window(Timestamp::new_second(10), Some(Timestamp::new_second(15)));
    state
        .dirty_time_windows
        .add_window(Timestamp::new_second(100), Some(Timestamp::new_second(105)));

    let high = BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]);
    state.start_fenced_repair(high.clone()).unwrap();
    let filter = next_fenced_repair_filter(&mut state, 1);
    assert_eq!(
        state
            .pending_fenced_repair()
            .unwrap()
            .pending_windows()
            .len(),
        1
    );

    let decision = BatchingTask::apply_query_failure_to_state(
        &mut state,
        std::time::Duration::from_millis(1),
        &QueryCoverage::FencedRepairChunk { high: high.clone() },
        FlowQueryFallbackReason::QueryFailure,
    );

    assert_eq!(
        decision,
        Some(FlowCheckpointDecision::FallbackToFullSnapshot {
            previous_mode: CheckpointMode::FullSnapshot,
            reason: FlowQueryFallbackReason::QueryFailure,
        })
    );
    // Pending repair is NOT abandoned: high H is unchanged.
    let repair = state.pending_fenced_repair().unwrap();
    assert_eq!(repair.high(), &high);
    assert_eq!(repair.pending_windows().len(), 1);

    // Simulate the outer execution failure restore for the in-flight chunk.
    state.restore_scoped_windows(&filter);

    // After restore, the in-flight chunk goes back into pending_windows
    // (because pending_fenced_repair is still Some), NOT into live
    // dirty_time_windows.
    assert_eq!(
        state
            .pending_fenced_repair()
            .unwrap()
            .pending_windows()
            .len(),
        2,
        "in-flight window restored into pending_windows"
    );
    assert_eq!(
        state.dirty_time_windows.len(),
        2,
        "live dirty windows unchanged (not where in-flight was restored)"
    );
}

#[test]
fn test_checkpoint_decision_labels_are_stable() {
    let advance = FlowCheckpointDecision::AdvancedIncremental {
        participating_regions: 1,
        watermarks: 1,
    };
    let fallback = FlowCheckpointDecision::FallbackToFullSnapshot {
        previous_mode: CheckpointMode::Incremental,
        reason: FlowQueryFallbackReason::StaleCursor,
    };

    assert_eq!(advance.mode_label(), "incremental");
    assert_eq!(advance.decision_label(), CHECKPOINT_DECISION_ADVANCE);
    assert_eq!(advance.reason_label(), CHECKPOINT_REASON_NONE);
    assert_eq!(fallback.mode_label(), "incremental");
    assert_eq!(fallback.decision_label(), CHECKPOINT_DECISION_FALLBACK);
    assert_eq!(fallback.reason_label(), "stale_cursor");
    assert_eq!(
        FlowQueryFallbackReason::SnapshotFenceExpired.as_label(),
        "snapshot_fence_expired"
    );
    assert_eq!(
        FlowQueryFallbackReason::DirtyBacklogPending.as_label(),
        "dirty_backlog_pending"
    );
    assert_eq!(
        FlowQueryFallbackReason::QueryFailure.as_label(),
        "query_failure"
    );
}

#[tokio::test]
async fn test_build_flow_query_extensions_switches_with_checkpoint_mode() {
    let (task, _) = new_test_task_engine_and_plan_with_query(
        "SELECT number, ts FROM numbers_with_ts",
        "numbers_with_ts",
    )
    .await
    .into_task_and_plan();

    let extensions = task.build_flow_query_extensions(false, true).await.unwrap();
    assert_eq!(
        extensions,
        vec![("flow.return_region_seq", "true".to_string())]
    );

    task.state
        .write()
        .unwrap()
        .advance_checkpoints(HashMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]));

    let extensions = task.build_flow_query_extensions(false, true).await.unwrap();
    assert!(extensions.contains(&("flow.return_region_seq", "true".to_string())));
    assert!(
        !extensions
            .iter()
            .any(|(key, _)| *key == FLOW_INCREMENTAL_MODE)
    );
    assert!(
        !extensions
            .iter()
            .any(|(key, _)| *key == FLOW_INCREMENTAL_AFTER_SEQS)
    );

    let extensions = task.build_flow_query_extensions(true, true).await.unwrap();

    assert!(extensions.contains(&("flow.return_region_seq", "true".to_string())));
    assert!(extensions.contains(&(
        FLOW_INCREMENTAL_MODE,
        FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY.to_string()
    )));
    assert!(extensions.contains(&(
        FLOW_INCREMENTAL_AFTER_SEQS,
        serde_json::json!({"1": 10, "2": 20}).to_string(),
    )));

    let extensions = task.build_flow_query_extensions(true, false).await.unwrap();
    assert!(extensions.contains(&("flow.return_region_seq", "true".to_string())));
    assert!(
        !extensions
            .iter()
            .any(|(key, _)| *key == FLOW_INCREMENTAL_MODE)
    );
    assert!(
        !extensions
            .iter()
            .any(|(key, _)| *key == FLOW_INCREMENTAL_AFTER_SEQS)
    );

    task.state.write().unwrap().disable_incremental();
    let extensions = task.build_flow_query_extensions(true, true).await.unwrap();
    assert!(extensions.contains(&("flow.return_region_seq", "true".to_string())));
    assert!(
        !extensions
            .iter()
            .any(|(key, _)| *key == FLOW_INCREMENTAL_MODE)
    );
    assert!(
        !extensions
            .iter()
            .any(|(key, _)| *key == FLOW_INCREMENTAL_AFTER_SEQS)
    );
}

#[tokio::test]
async fn test_full_snapshot_scoped_plan_marks_checkpoint_advance_safe_only_after_backlog_drained() {
    let TestTaskParts {
        task,
        query_engine,
        ..
    } = new_time_window_test_task_with_query(
        "SELECT number, date_bin(INTERVAL '5 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window, number",
    )
    .await;
    {
        let mut state = task.state.write().unwrap();
        state.disable_incremental();
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(0), Some(Timestamp::new_second(5)));
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(30), Some(Timestamp::new_second(35)));
    }
    let sink_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("number", CDT::uint32_datatype(), false),
        ColumnSchema::new("time_window", CDT::timestamp_millisecond_datatype(), false)
            .with_time_index(true),
    ]));

    let first = task
        .gen_query_with_time_window(query_engine.clone(), &sink_schema, &[], false, Some(1))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(first.coverage, QueryCoverage::ScopedBaseRepair));
    assert_eq!(task.state.read().unwrap().dirty_time_windows.len(), 1);

    let second = task
        .gen_query_with_time_window(query_engine, &sink_schema, &[], false, Some(1))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(second.coverage, QueryCoverage::ScopedBaseRepair));
    assert!(task.state.read().unwrap().dirty_time_windows.is_empty());
}

#[tokio::test]
async fn test_expired_empty_fenced_repair_generates_scoped_base_repair_plan() {
    let TestTaskParts {
        mut task,
        query_engine,
        ..
    } = new_time_window_test_task_with_query(
        "SELECT number, date_bin(INTERVAL '5 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window, number",
    )
    .await;
    Arc::get_mut(&mut task.config)
        .expect("test task config should be uniquely owned")
        .expire_after = Some(expire_after_for_retention_filter_test());

    {
        let mut state = task.state.write().unwrap();
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(0), Some(Timestamp::new_second(5)));
        state
            .start_fenced_repair(BTreeMap::from([(1_u64, 10_u64)]))
            .unwrap();

        state.dirty_time_windows.clean();
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(100), Some(Timestamp::new_second(105)));
    }

    let plan = task
        .gen_query_with_time_window(
            query_engine,
            &aggregate_time_window_sink_schema(),
            &[],
            false,
            Some(1),
        )
        .await
        .unwrap()
        .expect("expired empty repair should fall back to live dirty");

    assert!(matches!(plan.coverage, QueryCoverage::ScopedBaseRepair));
    assert!(plan.coverage.snapshot_seqs().is_empty());
    assert!(task.state.read().unwrap().pending_fenced_repair().is_none());
}

#[tokio::test]
async fn test_incremental_plan_consumes_dirty_signal_for_checkpoint_safety() {
    let TestTaskParts {
        task,
        query_engine,
        ..
    } = new_time_window_test_task_with_query(
        "SELECT number, date_bin(INTERVAL '5 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window, number",
    )
    .await;
    {
        let mut state = task.state.write().unwrap();
        state.advance_checkpoints(HashMap::from([(1_u64, 10_u64)]));
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(0), Some(Timestamp::new_second(5)));
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(30), Some(Timestamp::new_second(35)));
    }
    let sink_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("number", CDT::uint32_datatype(), false),
        ColumnSchema::new("time_window", CDT::timestamp_millisecond_datatype(), false)
            .with_time_index(true),
    ]));

    let plan = task
        .gen_query_with_time_window(query_engine, &sink_schema, &[], false, Some(1))
        .await
        .unwrap()
        .unwrap();

    assert!(matches!(plan.coverage, QueryCoverage::IncrementalDelta));
    assert!(task.state.read().unwrap().dirty_time_windows.is_empty());
}

#[tokio::test]
async fn test_scoped_base_repair_plan_applies_dirty_window_filter() {
    let TestTaskParts {
        task,
        query_engine,
        ..
    } = new_time_window_test_task_with_query(
        "SELECT max(number) AS number, date_bin(INTERVAL '5 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window",
    )
    .await;
    {
        let mut state = task.state.write().unwrap();
        assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
        assert!(!state.is_incremental_disabled());
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(0), Some(Timestamp::new_second(5)));
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(30), Some(Timestamp::new_second(35)));
    }
    let sink_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("number", CDT::uint32_datatype(), false),
        ColumnSchema::new("time_window", CDT::timestamp_millisecond_datatype(), false)
            .with_time_index(true),
    ]));

    let plan = task
        .gen_query_with_time_window(query_engine, &sink_schema, &[], false, Some(1))
        .await
        .unwrap()
        .unwrap();

    let plan_text = plan.plan.to_string();
    assert!(matches!(plan.coverage, QueryCoverage::ScopedBaseRepair));
    assert_eq!(task.state.read().unwrap().dirty_time_windows.len(), 1);
    assert!(plan_text.contains("Filter:"), "{plan_text}");
}

#[tokio::test]
async fn test_full_snapshot_seeding_applies_expire_after_retention_filter() {
    let TestTaskParts {
        mut task,
        query_engine,
        ..
    } = new_time_window_test_task_with_query(
        "SELECT max(number) AS number, date_bin(INTERVAL '5 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window",
    )
    .await;
    {
        let mut state = task.state.write().unwrap();
        assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
        assert!(!state.is_incremental_disabled());
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(100), Some(Timestamp::new_second(105)));
    }
    let sink_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("number", CDT::uint32_datatype(), false),
        ColumnSchema::new("time_window", CDT::timestamp_millisecond_datatype(), false)
            .with_time_index(true),
    ]));

    Arc::get_mut(&mut task.config)
        .expect("test task config should be uniquely owned")
        .expire_after = Some(expire_after_for_retention_filter_test());
    let plan = task
        .gen_query_with_time_window(query_engine, &sink_schema, &[], false, Some(1))
        .await
        .unwrap()
        .unwrap();

    assert!(matches!(plan.coverage, QueryCoverage::ScopedBaseRepair));
    assert!(task.state.read().unwrap().dirty_time_windows.is_empty());
    let plan_text = plan.plan.to_string();
    assert!(
        plan_text.contains("Filter: ts >= TimestampMillisecond("),
        "{plan_text}"
    );
}

#[tokio::test]
async fn test_incremental_plan_does_not_add_dirty_window_filter() {
    let TestTaskParts {
        task,
        query_engine,
        ..
    } = new_time_window_test_task_with_query(
        "SELECT max(number) AS number, date_bin(INTERVAL '5 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window",
    )
    .await;
    {
        let mut state = task.state.write().unwrap();
        state.advance_checkpoints(HashMap::from([(1_u64, 10_u64)]));
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(0), Some(Timestamp::new_second(5)));
    }
    let sink_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("number", CDT::uint32_datatype(), false),
        ColumnSchema::new("time_window", CDT::timestamp_millisecond_datatype(), false)
            .with_time_index(true),
    ]));

    let plan = task
        .gen_query_with_time_window(query_engine, &sink_schema, &[], false, Some(1))
        .await
        .unwrap()
        .unwrap();

    let plan_text = plan.plan.to_string();
    assert!(matches!(plan.coverage, QueryCoverage::IncrementalDelta));
    assert!(!plan_text.contains("Filter:"), "{plan_text}");
}

#[tokio::test]
async fn test_incremental_delta_applies_expire_after_retention_filter() {
    let TestTaskParts {
        mut task,
        query_engine,
        ..
    } = new_time_window_test_task_with_query(
        "SELECT max(number) AS number, date_bin(INTERVAL '5 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window",
    )
    .await;
    {
        let mut state = task.state.write().unwrap();
        state.advance_checkpoints(HashMap::from([(1_u64, 10_u64)]));
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(0), Some(Timestamp::new_second(5)));
    }
    let sink_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("number", CDT::uint32_datatype(), false),
        ColumnSchema::new("time_window", CDT::timestamp_millisecond_datatype(), false)
            .with_time_index(true),
    ]));

    Arc::get_mut(&mut task.config)
        .expect("test task config should be uniquely owned")
        .expire_after = Some(expire_after_for_retention_filter_test());
    let plan = task
        .gen_query_with_time_window(query_engine, &sink_schema, &[], false, Some(1))
        .await
        .unwrap()
        .unwrap();

    assert!(matches!(plan.coverage, QueryCoverage::IncrementalDelta));
    assert!(task.state.read().unwrap().dirty_time_windows.is_empty());
    let plan_text = plan.plan.to_string();
    assert!(
        plan_text.contains("Filter: ts >= TimestampMillisecond("),
        "{plan_text}"
    );
}

#[tokio::test]
async fn test_successful_incremental_checkpoint_fallback_consumes_unscoped_dirty_signal() {
    let TestTaskParts {
        task,
        query_engine,
        ..
    } = new_time_window_test_task_with_query(
        "SELECT max(number) AS number, date_bin(INTERVAL '5 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window",
    )
    .await;
    {
        let mut state = task.state.write().unwrap();
        state.advance_checkpoints(HashMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]));
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(0), Some(Timestamp::new_second(5)));
    }
    let sink_schema = aggregate_time_window_sink_schema();

    let plan_info = task
        .gen_query_with_time_window(query_engine.clone(), &sink_schema, &[], false, Some(1))
        .await
        .unwrap()
        .unwrap();

    assert!(matches!(
        plan_info.coverage,
        QueryCoverage::IncrementalDelta
    ));
    assert!(matches!(
        &plan_info.dirty_restore,
        DirtyRestore::Unscoped(_)
    ));
    assert!(task.state.read().unwrap().dirty_time_windows.is_empty());

    let result = output_with_region_watermarks([(1_u64, Some(12_u64)), (2_u64, None)]);
    let decision = {
        let mut state = task.state.write().unwrap();
        BatchingTask::apply_query_result_to_state(
            &mut state,
            &result,
            std::time::Duration::from_millis(1),
            &plan_info.coverage,
        )
    };
    assert_eq!(
        decision,
        FlowCheckpointDecision::FallbackToFullSnapshot {
            previous_mode: CheckpointMode::Incremental,
            reason: FlowQueryFallbackReason::IncompleteRegionWatermark,
        }
    );

    {
        let state = task.state.read().unwrap();
        assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
        assert!(state.dirty_time_windows.is_empty());
    }

    let followup = task
        .gen_query_with_time_window(query_engine, &sink_schema, &[], false, Some(1))
        .await
        .unwrap();
    assert!(
        followup.is_none(),
        "successful fallback consumes the dirty signal instead of re-running it"
    );
}

#[tokio::test]
async fn test_explicit_full_query_paths_generate_unfiltered_full() {
    for (case_name, query_type, flow_eval_interval) in [
        ("TQL", QueryType::Tql, None),
        (
            "eval-interval SQL",
            QueryType::Sql,
            Some(Duration::from_secs(60)),
        ),
    ] {
        let TestTaskParts {
            mut task,
            query_engine,
            ..
        } = new_test_task_engine_and_plan_with_query(
            "SELECT number, ts FROM numbers_with_ts",
            "missing_sink",
        )
        .await;
        {
            let config =
                Arc::get_mut(&mut task.config).expect("test task config should be uniquely owned");
            config.query_type = query_type;
            config.flow_eval_interval = flow_eval_interval;
        }
        task.state.write().unwrap().dirty_time_windows.set_dirty();
        let sink_schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("number", CDT::uint32_datatype(), false),
            ColumnSchema::new("ts", CDT::timestamp_millisecond_datatype(), false)
                .with_time_index(true),
        ]));

        let plan = task
            .gen_query_with_time_window(query_engine, &sink_schema, &[], false, None)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("{case_name} full-query path should generate a plan"));

        assert!(
            matches!(plan.coverage, QueryCoverage::UnfilteredFull),
            "{case_name} should use UnfilteredFull"
        );
        assert!(
            task.state.read().unwrap().dirty_time_windows.is_empty(),
            "{case_name} should consume the dirty signal"
        );
    }
}

#[tokio::test]
async fn test_executed_query_failure_restores_scoped_dirty_windows_for_flush_path() {
    let (task, plan) = new_test_task_and_plan_with_missing_sink().await;
    {
        let mut state = task.state.write().unwrap();
        state.dirty_time_windows.clean();
    }
    let scoped_query = PlanInfo {
        plan,
        dirty_restore: DirtyRestore::Scoped(FilterExprInfo {
            expr: datafusion_expr::lit(true),
            col_name: "ts".to_string(),
            time_ranges: vec![(Timestamp::new_second(10), Timestamp::new_second(20))],
            window_size: chrono::Duration::seconds(10),
        }),
        coverage: QueryCoverage::ScopedBaseRepair,
    };

    task.handle_executed_query_failure(Some(&scoped_query));

    let state = task.state.read().unwrap();
    assert_eq!(state.dirty_time_windows.len(), 1);
    assert_eq!(
        state.dirty_time_windows.window_size(),
        std::time::Duration::from_secs(10)
    );
}

#[tokio::test]
async fn test_prepare_plan_for_incremental_disables_on_non_aggregate() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let plan = sql_to_df_plan(
        ctx.clone(),
        query_engine.clone(),
        "SELECT number, ts FROM numbers_with_ts",
        true,
    )
    .await
    .unwrap();

    // Build a DML wrapper using a real sink table from the test engine.
    let (sink_table, _) = get_table_info_df_schema(
        query_engine.engine_state().catalog_manager().clone(),
        [
            "greptime".to_string(),
            "public".to_string(),
            "numbers_with_ts".to_string(),
        ],
    )
    .await
    .unwrap();
    let table_provider = Arc::new(DfTableProviderAdapter::new(sink_table));
    let table_source = Arc::new(DefaultTableSource::new(table_provider));
    let dml_plan = LogicalPlan::Dml(DmlStatement::new(
        datafusion_common::TableReference::bare("test"),
        table_source,
        WriteOp::Insert(datafusion_expr::dml::InsertOp::Append),
        Arc::new(plan),
    ));

    let (_tx, rx) = tokio::sync::oneshot::channel();
    let task = BatchingTask::try_new(TaskArgs {
        flow_id: 1,
        query: "SELECT number, ts FROM numbers_with_ts",
        plan: dml_plan.clone(),
        time_window_expr: None,
        expire_after: None,
        sink_table_name: [
            "greptime".to_string(),
            "public".to_string(),
            "numbers_with_ts".to_string(),
        ],
        source_table_names: vec![[
            "greptime".to_string(),
            "public".to_string(),
            "numbers_with_ts".to_string(),
        ]],
        query_ctx: ctx,
        catalog_manager: query_engine.engine_state().catalog_manager().clone(),
        shutdown_rx: rx,
        batch_opts: incremental_batch_opts(),
        flow_eval_interval: None,
    })
    .unwrap();

    // Put the state into Incremental mode with checkpoints.
    task.state
        .write()
        .unwrap()
        .advance_checkpoints(HashMap::from([(1_u64, 10_u64)]));
    assert_eq!(
        task.state.read().unwrap().checkpoint_mode(),
        CheckpointMode::Incremental
    );

    let incremental_plan = task.prepare_plan_for_incremental(&dml_plan).await.unwrap();
    assert!(incremental_plan.is_none());
    let state = task.state.read().unwrap();
    assert!(state.is_incremental_disabled());
    assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
}

#[tokio::test]
async fn test_unsafe_incremental_plan_skip_restores_dirty_without_query() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let plan = sql_to_df_plan(
        ctx.clone(),
        query_engine.clone(),
        "SELECT sum(number) AS total, ts FROM numbers_with_ts GROUP BY ts",
        true,
    )
    .await
    .unwrap();

    let (sink_table, _) = get_table_info_df_schema(
        query_engine.engine_state().catalog_manager().clone(),
        [
            "greptime".to_string(),
            "public".to_string(),
            "numbers_with_ts".to_string(),
        ],
    )
    .await
    .unwrap();
    let table_provider = Arc::new(DfTableProviderAdapter::new(sink_table));
    let table_source = Arc::new(DefaultTableSource::new(table_provider));
    let dml_plan = LogicalPlan::Dml(DmlStatement::new(
        datafusion_common::TableReference::bare("test"),
        table_source,
        WriteOp::Insert(datafusion_expr::dml::InsertOp::Append),
        Arc::new(plan),
    ));

    let (_tx, rx) = tokio::sync::oneshot::channel();
    let task = BatchingTask::try_new(TaskArgs {
        flow_id: 1,
        query: "SELECT sum(number) AS total, ts FROM numbers_with_ts GROUP BY ts",
        plan: dml_plan.clone(),
        time_window_expr: None,
        expire_after: None,
        // The sink table exists, but does not have the rewritten aggregate
        // output column `total`, so incremental rewrite fails before any
        // frontend query should be sent.
        sink_table_name: [
            "greptime".to_string(),
            "public".to_string(),
            "numbers_with_ts".to_string(),
        ],
        source_table_names: vec![[
            "greptime".to_string(),
            "public".to_string(),
            "numbers_with_ts".to_string(),
        ]],
        query_ctx: ctx,
        catalog_manager: query_engine.engine_state().catalog_manager().clone(),
        shutdown_rx: rx,
        batch_opts: incremental_batch_opts(),
        flow_eval_interval: None,
    })
    .unwrap();

    task.state
        .write()
        .unwrap()
        .advance_checkpoints(HashMap::from([(1_u64, 10_u64)]));
    let dirty_restore = DirtyRestore::Unscoped(dirty_range(10, 15));
    let (frontend_client, _) = FrontendClient::from_empty_grpc_handler(QueryOptions::default());

    let result = task
        .execute_logical_plan_unlocked(
            &Arc::new(frontend_client),
            &dml_plan,
            &dirty_restore,
            &QueryCoverage::IncrementalDelta,
        )
        .await
        .unwrap();

    assert!(
        result.is_none(),
        "unsafe incremental fallback must skip query"
    );
    let state = task.state.read().unwrap();
    assert_eq!(state.checkpoint_mode(), CheckpointMode::Incremental);
    assert!(!state.is_incremental_disabled());
    assert_eq!(state.dirty_time_windows.len(), 1);
    assert_eq!(
        state.dirty_time_windows.window_size(),
        std::time::Duration::from_secs(5)
    );
}

#[tokio::test]
async fn test_prepare_plan_for_incremental_group_by_without_merge_columns_uses_original_plan() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let plan = sql_to_df_plan(
        ctx.clone(),
        query_engine.clone(),
        "SELECT ts FROM numbers_with_ts GROUP BY ts",
        true,
    )
    .await
    .unwrap();

    let (sink_table, _) = get_table_info_df_schema(
        query_engine.engine_state().catalog_manager().clone(),
        [
            "greptime".to_string(),
            "public".to_string(),
            "numbers_with_ts".to_string(),
        ],
    )
    .await
    .unwrap();
    let table_provider = Arc::new(DfTableProviderAdapter::new(sink_table));
    let table_source = Arc::new(DefaultTableSource::new(table_provider));
    let dml_plan = LogicalPlan::Dml(DmlStatement::new(
        datafusion_common::TableReference::bare("test"),
        table_source,
        WriteOp::Insert(datafusion_expr::dml::InsertOp::Append),
        Arc::new(plan),
    ));

    let (_tx, rx) = tokio::sync::oneshot::channel();
    let task = BatchingTask::try_new(TaskArgs {
        flow_id: 1,
        query: "SELECT ts FROM numbers_with_ts GROUP BY ts",
        plan: dml_plan.clone(),
        time_window_expr: None,
        expire_after: None,
        sink_table_name: [
            "greptime".to_string(),
            "public".to_string(),
            "numbers_with_ts".to_string(),
        ],
        source_table_names: vec![[
            "greptime".to_string(),
            "public".to_string(),
            "numbers_with_ts".to_string(),
        ]],
        query_ctx: ctx,
        catalog_manager: query_engine.engine_state().catalog_manager().clone(),
        shutdown_rx: rx,
        batch_opts: incremental_batch_opts(),
        flow_eval_interval: None,
    })
    .unwrap();

    task.state
        .write()
        .unwrap()
        .advance_checkpoints(HashMap::from([(1_u64, 10_u64)]));

    let incremental_plan = task
        .prepare_plan_for_incremental(&dml_plan)
        .await
        .unwrap()
        .expect("plain GROUP BY is incremental-safe without a rewrite");

    assert_eq!(format!("{incremental_plan}"), format!("{dml_plan}"));
    assert!(!task.state.read().unwrap().is_incremental_disabled());
}

#[tokio::test]
async fn test_auto_created_sql_aggregate_sink_reaches_incremental_safe() {
    let sink_table = "auto_created_aggregate_sink";
    let query = "SELECT max(number) AS number, ts FROM numbers_with_ts GROUP BY ts";
    let TestTaskParts {
        task, query_engine, ..
    } = new_test_task_engine_and_plan_with_query(query, sink_table).await;
    register_auto_created_aggregate_sink(&query_engine, sink_table);

    let ctx = task.state.read().unwrap().query_ctx.clone();
    let plan = sql_to_df_plan(ctx, query_engine.clone(), query, true)
        .await
        .unwrap();
    let (sink_table, _) = get_table_info_df_schema(
        query_engine.engine_state().catalog_manager().clone(),
        [
            "greptime".to_string(),
            "public".to_string(),
            sink_table.to_string(),
        ],
    )
    .await
    .unwrap();
    let table_provider = Arc::new(DfTableProviderAdapter::new(sink_table));
    let table_source = Arc::new(DefaultTableSource::new(table_provider));
    let dml_plan = LogicalPlan::Dml(DmlStatement::new(
        datafusion_common::TableReference::bare("test"),
        table_source,
        WriteOp::Insert(datafusion_expr::dml::InsertOp::Append),
        Arc::new(plan),
    ));

    task.state
        .write()
        .unwrap()
        .advance_checkpoints(HashMap::from([(1_u64, 10_u64)]));
    let incremental_plan = task.prepare_plan_for_incremental(&dml_plan).await.unwrap();
    let incremental_safe = incremental_plan.is_some();

    assert!(incremental_safe);
    assert!(!task.state.read().unwrap().is_incremental_disabled());

    let extensions = task
        .build_flow_query_extensions(incremental_safe, true)
        .await
        .unwrap();
    assert!(extensions.contains(&(
        FLOW_INCREMENTAL_MODE,
        FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY.to_string()
    )));
    assert!(
        extensions
            .iter()
            .any(|(key, _)| *key == FLOW_INCREMENTAL_AFTER_SEQS)
    );
}

#[tokio::test]
async fn test_unscoped_failure_restores_consumed_dirty_signal() {
    assert_unscoped_failure_restore(dirty_marker(), DirtyTimeWindows::default(), 1, 0).await;
    assert_unscoped_failure_restore(dirty_range(30, 40), dirty_range(10, 20), 2, 20).await;
    assert_unscoped_failure_restore(dirty_range(30, 40), dirty_range(30, 50), 1, 20).await;
}

#[tokio::test]
async fn test_unscoped_runtime_invariant_error_preserves_dirty_signal() {
    let TestTaskParts {
        task, query_engine, ..
    } = new_test_task_engine_and_plan_with_query(
        "SELECT missing_column FROM numbers_with_ts",
        "missing_sink",
    )
    .await;
    task.state.write().unwrap().dirty_time_windows.set_dirty();
    let sink_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("number", CDT::uint32_datatype(), false),
        ColumnSchema::new("ts", CDT::timestamp_millisecond_datatype(), false).with_time_index(true),
    ]));

    let result = task
        .gen_query_with_time_window(query_engine, &sink_schema, &[], false, None)
        .await;

    let err = match result {
        Err(err) => err,
        Ok(_) => panic!("runtime should reject SQL without TWE or EVAL INTERVAL"),
    };
    assert!(matches!(err, Error::Unexpected { .. }), "{err}");
    assert!(
        err.to_string()
            .contains("create-flow validation should have rejected it"),
        "{err}"
    );
    let state = task.state.read().unwrap();
    assert_eq!(state.dirty_time_windows.len(), 1);
    assert_eq!(
        state.dirty_time_windows.window_size(),
        std::time::Duration::from_secs(0)
    );
}

#[tokio::test]
async fn test_scoped_plan_generation_failure_restores_consumed_dirty_windows() {
    let TestTaskParts {
        task,
        query_engine,
        ..
    } = new_time_window_test_task_with_query(
        "SELECT missing_column, date_bin(INTERVAL '5 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window, missing_column",
    )
    .await;
    task.state
        .write()
        .unwrap()
        .dirty_time_windows
        .add_window(Timestamp::new_second(10), Some(Timestamp::new_second(15)));
    let sink_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("number", CDT::uint32_datatype(), false),
        ColumnSchema::new("time_window", CDT::timestamp_millisecond_datatype(), false)
            .with_time_index(true),
    ]));

    let result = task
        .gen_query_with_time_window(query_engine, &sink_schema, &[], false, None)
        .await;

    assert!(result.is_err());
    let state = task.state.read().unwrap();
    assert_eq!(state.dirty_time_windows.len(), 1);
    assert_eq!(
        state.dirty_time_windows.window_size(),
        std::time::Duration::from_secs(5)
    );
}

#[tokio::test]
async fn test_insert_plan_matching_failure_restores_consumed_dirty_marker() {
    let sink_table = "partial_sink";
    let TestTaskParts {
        mut task,
        query_engine,
        ..
    } = new_time_window_test_task_with_query(
        "SELECT number, date_bin(INTERVAL '5 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window, number",
    )
    .await;
    Arc::get_mut(&mut task.config)
        .expect("test task config should be uniquely owned")
        .sink_table_name[2] = sink_table.to_string();
    register_number_only_sink(&query_engine, sink_table);
    task.state.write().unwrap().dirty_time_windows.set_dirty();

    let result = task.gen_insert_plan_unlocked(&query_engine, None).await;

    assert!(result.is_err());
    let _err = match result {
        Ok(_) => panic!("gen_insert_plan_unlocked should fail with a sink column mismatch"),
        Err(err) => err,
    };
    let state = task.state.read().unwrap();
    assert_eq!(state.dirty_time_windows.len(), 1);
    assert_eq!(
        state.dirty_time_windows.window_size(),
        std::time::Duration::from_secs(5)
    );
}
