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

use common_error::ext::PlainError;
use common_error::status_code::StatusCode;
use common_query::Output;
use common_recordbatch::adapter::{RecordBatchMetrics, RegionWatermarkEntry};
use pretty_assertions::assert_eq;
use session::context::QueryContext;
use snafu::GenerateImplicitData;

use super::*;
use crate::batching_mode::checkpoint::{
    CHECKPOINT_DECISION_ADVANCE, CHECKPOINT_DECISION_FALLBACK, CHECKPOINT_REASON_NONE,
};
use crate::test_utils::create_test_query_engine;

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

async fn new_test_task_with_missing_sink() -> BatchingTask {
    new_test_task_and_plan_with_missing_sink().await.0
}

async fn new_test_task_and_plan_with_missing_sink() -> (BatchingTask, LogicalPlan) {
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
        query: "SELECT number, ts FROM numbers_with_ts",
        plan: plan.clone(),
        time_window_expr: None,
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
        batch_opts: Arc::new(BatchingModeOptions::default()),
        flow_eval_interval: None,
    })
    .unwrap();

    (task, plan)
}

fn ordinary_query_error() -> Error {
    Error::External {
        source: BoxedError::new(PlainError::new(
            "ordinary query failure".to_string(),
            StatusCode::EngineExecuteQuery,
        )),
        location: snafu::Location::generate(),
    }
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
fn test_apply_query_result_to_state_rejects_unproved_watermark() {
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);
    let result = output_with_region_watermarks([(1_u64, Some(10_u64)), (2_u64, None)]);

    let decision = BatchingTask::apply_query_result_to_state(
        &mut state,
        &result,
        std::time::Duration::from_millis(1),
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
fn test_checkpoint_decision_labels_are_stable() {
    let advance = FlowCheckpointDecision::AdvancedIncremental {
        participating_regions: 1,
        watermarks: 1,
    };
    let fallback = FlowCheckpointDecision::FallbackToFullSnapshot {
        previous_mode: CheckpointMode::Incremental,
        reason: FlowQueryFallbackReason::CheckpointReadyQueryFailure,
    };

    assert_eq!(advance.mode_label(), "incremental");
    assert_eq!(advance.decision_label(), CHECKPOINT_DECISION_ADVANCE);
    assert_eq!(advance.reason_label(), CHECKPOINT_REASON_NONE);
    assert_eq!(fallback.mode_label(), "incremental");
    assert_eq!(fallback.decision_label(), CHECKPOINT_DECISION_FALLBACK);
    assert_eq!(fallback.reason_label(), "checkpoint_ready_query_failure");
}

#[tokio::test]
async fn test_incremental_failure_falls_back_to_full_snapshot() {
    let task = new_test_task_with_missing_sink().await;
    task.state
        .write()
        .unwrap()
        .advance_checkpoints(HashMap::from([(1_u64, 10_u64)]));

    let checkpoint_was_ready = task.handle_flow_query_failure(&ordinary_query_error(), None);

    assert!(checkpoint_was_ready);
    let state = task.state.read().unwrap();
    assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
    assert_eq!(state.checkpoints(), &BTreeMap::from([(1_u64, 10_u64)]));
}

#[tokio::test]
async fn test_incremental_failure_without_query_scope_marks_full_flow_dirty() {
    let task = new_test_task_with_missing_sink().await;
    {
        let mut state = task.state.write().unwrap();
        state.advance_checkpoints(HashMap::from([(1_u64, 10_u64)]));
        state.dirty_time_windows.clean();
    }

    let checkpoint_was_ready = task.handle_flow_query_failure(&ordinary_query_error(), None);

    assert!(checkpoint_was_ready);
    let state = task.state.read().unwrap();
    assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
    assert_eq!(state.dirty_time_windows.len(), 1);
}

#[tokio::test]
async fn test_executed_query_failure_restores_scoped_dirty_windows_for_flush_path() {
    let (task, plan) = new_test_task_and_plan_with_missing_sink().await;
    {
        let mut state = task.state.write().unwrap();
        state.advance_checkpoints(HashMap::from([(1_u64, 10_u64)]));
        state.dirty_time_windows.clean();
    }
    let scoped_query = PlanInfo {
        plan,
        filter: Some(FilterExprInfo {
            expr: datafusion_expr::lit(true),
            col_name: "ts".to_string(),
            time_ranges: vec![(Timestamp::new_second(10), Timestamp::new_second(20))],
            window_size: chrono::Duration::seconds(10),
        }),
    };

    let checkpoint_was_ready =
        task.handle_executed_query_failure(&ordinary_query_error(), Some(&scoped_query));

    assert!(checkpoint_was_ready);
    let state = task.state.read().unwrap();
    assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
    assert_eq!(state.dirty_time_windows.len(), 1);
}

#[tokio::test]
async fn test_build_flow_query_extensions_switches_with_checkpoint_mode() {
    let task = new_test_task_with_missing_sink().await;

    // PR3a only sends plumbing extensions.
    let extensions = task.build_flow_query_extensions().await.unwrap();
    assert_eq!(
        extensions,
        vec![("flow.return_region_seq", "true".to_string())]
    );

    task.state
        .write()
        .unwrap()
        .advance_checkpoints(HashMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]));

    // Even after advancing to Incremental mode, still only plumbing extensions.
    let extensions = task.build_flow_query_extensions().await.unwrap();
    assert_eq!(
        extensions,
        vec![("flow.return_region_seq", "true".to_string())]
    );
}
