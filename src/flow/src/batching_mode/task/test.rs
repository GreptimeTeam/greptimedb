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

use std::collections::BTreeMap;

use catalog::RegisterTableRequest;
use catalog::memory::MemoryCatalogManager;
use client::OutputWithMetrics;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_query::Output;
use common_recordbatch::RecordBatch;
use common_recordbatch::adapter::{RecordBatchMetrics, RegionWatermarkEntry};
use datatypes::data_type::ConcreteDataType as CDT;
use datatypes::schema::ColumnSchema;
use datatypes::vectors::{TimestampMillisecondVector, UInt32Vector, VectorRef};
use pretty_assertions::assert_eq;
use query::options::{FLOW_INCREMENTAL_AFTER_SEQS, FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY};
use session::context::QueryContext;
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

fn dirty_range(start: i64, end: i64) -> DirtyTimeWindows {
    let mut dirty = DirtyTimeWindows::default();
    dirty.add_window(
        Timestamp::new_second(start),
        Some(Timestamp::new_second(end)),
    );
    dirty
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
        can_advance_checkpoints: true,
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
        true,
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
        true,
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
        true,
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
        true,
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
        true,
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
fn test_apply_query_result_to_state_blocks_full_snapshot_when_dirty_backlog_pending() {
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);
    let result = output_with_region_watermarks([(1_u64, Some(10_u64)), (2_u64, Some(20_u64))]);

    let decision = BatchingTask::apply_query_result_to_state(
        &mut state,
        &result,
        std::time::Duration::from_millis(1),
        false,
    );

    assert_eq!(
        decision,
        FlowCheckpointDecision::FallbackToFullSnapshot {
            previous_mode: CheckpointMode::FullSnapshot,
            reason: FlowQueryFallbackReason::DirtyBacklogPending,
        }
    );
    assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
    assert!(state.checkpoints().is_empty());
}

#[test]
fn test_apply_query_result_to_state_blocks_incremental_when_dirty_backlog_pending() {
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);
    state.advance_checkpoints(HashMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]));
    let result = output_with_region_watermarks([(1_u64, Some(12_u64)), (2_u64, Some(25_u64))]);

    let decision = BatchingTask::apply_query_result_to_state(
        &mut state,
        &result,
        std::time::Duration::from_millis(1),
        false,
    );

    assert_eq!(
        decision,
        FlowCheckpointDecision::FallbackToFullSnapshot {
            previous_mode: CheckpointMode::Incremental,
            reason: FlowQueryFallbackReason::DirtyBacklogPending,
        }
    );
    assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
    assert_eq!(
        state.checkpoints(),
        &BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)])
    );
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
fn test_apply_query_failure_to_state_keeps_full_snapshot_without_decision() {
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);

    let decision = BatchingTask::apply_query_failure_to_state(
        &mut state,
        std::time::Duration::from_millis(1),
        FlowQueryFallbackReason::StaleCursor,
    );

    assert_eq!(decision, None);
    assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
    assert!(state.checkpoints().is_empty());
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
        FlowQueryFallbackReason::DirtyBacklogPending.as_label(),
        "dirty_backlog_pending"
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
    assert!(!first.can_advance_checkpoints);
    assert_eq!(task.state.read().unwrap().dirty_time_windows.len(), 1);

    let second = task
        .gen_query_with_time_window(query_engine, &sink_schema, &[], false, Some(1))
        .await
        .unwrap()
        .unwrap();
    assert!(second.can_advance_checkpoints);
    assert!(task.state.read().unwrap().dirty_time_windows.is_empty());
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

    assert!(plan.can_advance_checkpoints);
    assert!(task.state.read().unwrap().dirty_time_windows.is_empty());
}

#[tokio::test]
async fn test_full_snapshot_seeding_for_incremental_does_not_add_dirty_window_filter() {
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
    assert!(plan.can_advance_checkpoints);
    assert!(task.state.read().unwrap().dirty_time_windows.is_empty());
    assert!(!plan_text.contains("Filter:"), "{plan_text}");
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
    assert!(plan.can_advance_checkpoints);
    assert!(!plan_text.contains("Filter:"), "{plan_text}");
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
        can_advance_checkpoints: true,
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
async fn test_prepare_plan_for_incremental_falls_back_without_disable_on_rewrite_error() {
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
        // output column `total`, so the rewrite fails deterministically.
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
    assert_eq!(
        task.state.read().unwrap().checkpoint_mode(),
        CheckpointMode::Incremental
    );

    let incremental_plan = task.prepare_plan_for_incremental(&dml_plan).await.unwrap();
    assert!(incremental_plan.is_none());
    let state = task.state.read().unwrap();
    assert!(!state.is_incremental_disabled());
    assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
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
    let TestTaskParts {
        task, query_engine, ..
    } = new_test_task_engine_and_plan_with_query(
        "SELECT max(number) AS number, ts FROM numbers_with_ts GROUP BY ts",
        sink_table,
    )
    .await;
    register_auto_created_aggregate_sink(&query_engine, sink_table);
    task.state.write().unwrap().dirty_time_windows.set_dirty();

    let plan_info = task
        .gen_insert_plan(&query_engine, None)
        .await
        .unwrap()
        .unwrap();
    assert!(plan_info.can_advance_checkpoints);

    task.state
        .write()
        .unwrap()
        .advance_checkpoints(HashMap::from([(1_u64, 10_u64)]));
    let incremental_plan = task
        .prepare_plan_for_incremental(&plan_info.plan)
        .await
        .unwrap();
    let incremental_safe = incremental_plan.is_some();

    assert!(incremental_safe);
    assert!(!task.state.read().unwrap().is_incremental_disabled());

    let extensions = task
        .build_flow_query_extensions(incremental_safe, plan_info.can_advance_checkpoints)
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
async fn test_unscoped_plan_generation_failure_restores_consumed_dirty_signal() {
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

    assert!(result.is_err());
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
        task, query_engine, ..
    } = new_test_task_engine_and_plan_with_query(
        "SELECT number, ts FROM numbers_with_ts",
        sink_table,
    )
    .await;
    register_number_only_sink(&query_engine, sink_table);
    task.state.write().unwrap().dirty_time_windows.set_dirty();

    let result = task.gen_insert_plan(&query_engine, None).await;

    assert!(result.is_err());
    let _err = match result {
        Ok(_) => panic!("gen_insert_plan should fail with a sink column mismatch"),
        Err(err) => err,
    };
    let state = task.state.read().unwrap();
    assert_eq!(state.dirty_time_windows.len(), 1);
    assert_eq!(
        state.dirty_time_windows.window_size(),
        std::time::Duration::from_secs(0)
    );
}
