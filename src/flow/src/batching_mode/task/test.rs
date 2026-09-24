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
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use catalog::memory::MemoryCatalogManager;
use catalog::table_source::dummy_catalog::DummyCatalogList;
use catalog::{DeregisterTableRequest, RegisterTableRequest};
use client::OutputWithMetrics;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_error::ext::BoxedError;
use common_error::mock::MockError;
use common_error::status_code::StatusCode;
use common_query::Output;
use common_recordbatch::RecordBatch;
use common_recordbatch::adapter::{RecordBatchMetrics, RegionWatermarkEntry};
use common_time::timestamp::TimeUnit;
use common_time::{TimeToLive, Timestamp};
use datafusion::execution::SessionStateBuilder;
use datafusion_expr::Expr;
use datatypes::data_type::ConcreteDataType as CDT;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::{
    TimestampMicrosecondVector, TimestampMillisecondVector, TimestampNanosecondVector,
    TimestampSecondVector, UInt32Vector, VectorRef,
};
use pretty_assertions::assert_eq;
use prost::Message;
use query::options::{
    FLOW_INCREMENTAL_AFTER_SEQS, FLOW_INCREMENTAL_MODE, FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY,
    FLOW_SCHEDULED_TIME_MILLIS, FLOW_SINK_TABLE_ID, QueryOptions,
};
use session::context::QueryContext;
use snafu::ResultExt;
use store_api::mito_engine_options::PRESERVE_ROW_SEQUENCE;
use substrait::substrait_proto_df::proto::Plan;
use table::Table;
use table::metadata::FilterPushDownType;
use table::test_util::MemTable;

use super::*;
use crate::batching_mode::checkpoint::{
    CHECKPOINT_DECISION_ADVANCE, CHECKPOINT_DECISION_FALLBACK, CHECKPOINT_REASON_NONE,
    FlowCheckpointDecision, FlowQueryFallbackReason,
};
use crate::batching_mode::eval_schedule::{FlowMissedTickPolicy, FlowScheduleConfig};
use crate::batching_mode::state::CheckpointMode;
use crate::batching_mode::time_window::find_time_window_expr;
use crate::test_utils::create_test_query_engine;

fn incremental_batch_opts() -> Arc<BatchingModeOptions> {
    Arc::new(BatchingModeOptions {
        experimental_enable_incremental_read: true,
        ..Default::default()
    })
}

struct CountingExecution {
    calls: std::sync::atomic::AtomicUsize,
    active: std::sync::atomic::AtomicUsize,
    max_active: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl crate::BatchingExecution for CountingExecution {
    async fn execute_once(
        self: Arc<Self>,
        _guard: BatchingExecutionGuard,
        _task: &BatchingTask,
        _engine: &QueryEngineRef,
        _frontend: &Arc<FrontendClient>,
        _max_window_cnt: Option<usize>,
    ) -> ExecuteOnceOutcome {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let active = self
            .active
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        self.max_active
            .fetch_max(active, std::sync::atomic::Ordering::SeqCst);
        tokio::task::yield_now().await;
        self.active
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        ExecuteOnceOutcome {
            new_query: None,
            result: Ok(None),
        }
    }
}

struct RetainingExecution {
    calls: std::sync::atomic::AtomicUsize,
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    finished: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl crate::BatchingExecution for RetainingExecution {
    async fn execute_once(
        self: Arc<Self>,
        guard: BatchingExecutionGuard,
        _task: &BatchingTask,
        _engine: &QueryEngineRef,
        _frontend: &Arc<FrontendClient>,
        _max_window_cnt: Option<usize>,
    ) -> ExecuteOnceOutcome {
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) != 0 {
            return ExecuteOnceOutcome {
                new_query: None,
                result: Ok(None),
            };
        }

        let started = self.started.clone();
        let release = self.release.clone();
        let finished = self.finished.clone();
        let child = tokio::spawn(async move {
            started.notify_one();
            release.notified().await;
            drop(guard);
            finished.notify_one();
            ExecuteOnceOutcome {
                new_query: None,
                result: Ok(None),
            }
        });
        match child.await {
            Ok(outcome) => outcome,
            Err(err) => ExecuteOnceOutcome {
                new_query: None,
                result: Err(Error::Unexpected {
                    reason: format!("retaining test child failed: {err}"),
                    location: snafu::location!(),
                }),
            },
        }
    }
}

/// Records the completed plan handed to it by the task.
#[derive(Default)]
struct PlanRecordingExecution {
    plans: std::sync::Mutex<Vec<LogicalPlan>>,
}

#[async_trait::async_trait]
impl crate::BatchingExecution for PlanRecordingExecution {
    async fn execute_once(
        self: Arc<Self>,
        _guard: BatchingExecutionGuard,
        _task: &BatchingTask,
        _engine: &QueryEngineRef,
        _frontend: &Arc<FrontendClient>,
        _max_window_cnt: Option<usize>,
    ) -> ExecuteOnceOutcome {
        ExecuteOnceOutcome {
            new_query: None,
            result: Ok(None),
        }
    }

    fn rewrite_plan(&self, _task: &BatchingTask, plan: LogicalPlan) -> crate::Result<LogicalPlan> {
        self.plans.lock().unwrap().push(plan.clone());
        Ok(plan)
    }
}

#[tokio::test]
async fn test_execution_hook_receives_the_completed_incremental_plan() {
    let sink_table = "hook_sink";
    let query = "SELECT max(number) AS number, ts FROM numbers_with_ts GROUP BY ts";
    let TestTaskParts {
        task, query_engine, ..
    } = new_test_task_engine_and_plan_with_query(query, sink_table).await;
    register_auto_created_aggregate_sink(&query_engine, sink_table);

    let ctx = task.state.read().unwrap().query_ctx.clone();
    let plan = sql_to_df_plan(ctx, query_engine.clone(), query, true)
        .await
        .unwrap();
    let (sink, _) = get_table_info_df_schema(
        query_engine.engine_state().catalog_manager().clone(),
        [
            "greptime".to_string(),
            "public".to_string(),
            sink_table.to_string(),
        ],
    )
    .await
    .unwrap();
    let table_provider = Arc::new(DfTableProviderAdapter::new(sink));
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

    let execution = Arc::new(PlanRecordingExecution::default());
    let task = task.with_execution(Some(execution.clone()));
    let (frontend_client, _handler) =
        FrontendClient::from_empty_grpc_handler(QueryOptions::default());
    let frontend = Arc::new(frontend_client);
    let dirty_restore = DirtyRestore::Unscoped(dirty_range(10, 15));

    let _ = task
        .execute_plan_unlocked(
            &query_engine,
            &frontend,
            &dml_plan,
            &dirty_restore,
            &QueryCoverage::IncrementalDelta,
        )
        .await;

    let recorded = execution.plans.lock().unwrap();
    let plan = recorded
        .first()
        .expect("the execution hook must see the plan that is dispatched");
    let plan_text = plan.to_string();
    assert!(
        plan_text.contains("Left Join"),
        "the hook must receive the completed delta-sink merge plan, got:\n{plan_text}"
    );
}

#[tokio::test]
async fn test_execution_delegate_dispatch_is_serialized() {
    let TestTaskParts {
        task, query_engine, ..
    } = new_test_task_engine_and_plan_with_query("SELECT number, ts FROM numbers_with_ts", "sink")
        .await;
    let execution = Arc::new(CountingExecution {
        calls: Default::default(),
        active: Default::default(),
        max_active: Default::default(),
    });
    let task = task.with_execution(Some(execution.clone()));
    let (frontend, _handler) = FrontendClient::from_empty_grpc_handler(QueryOptions::default());
    let frontend = Arc::new(frontend);

    let first = task.execute_once_serialized(&query_engine, &frontend, None);
    let second = task.execute_once_serialized(&query_engine, &frontend, None);
    let (first, second) = tokio::join!(first, second);
    assert_eq!(first.unwrap(), None);
    assert_eq!(second.unwrap(), None);
    assert_eq!(execution.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(
        execution
            .max_active
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the existing execution_lock must span delegate execution"
    );
}

#[tokio::test]
async fn test_delegate_guard_survives_caller_cancellation_until_child_finishes() {
    let TestTaskParts {
        task, query_engine, ..
    } = new_test_task_engine_and_plan_with_query("SELECT number, ts FROM numbers_with_ts", "sink")
        .await;
    let execution = Arc::new(RetainingExecution {
        calls: Default::default(),
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
        finished: Arc::new(tokio::sync::Notify::new()),
    });
    let task = task.with_execution(Some(execution.clone()));
    let (frontend, _handler) = FrontendClient::from_empty_grpc_handler(QueryOptions::default());
    let frontend = Arc::new(frontend);

    let first_task = task.clone();
    let first_engine = query_engine.clone();
    let first_frontend = frontend.clone();
    let first = tokio::spawn(async move {
        first_task
            .execute_once_serialized(&first_engine, &first_frontend, None)
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), execution.started.notified())
        .await
        .expect("delegate child did not retain the guard");
    first.abort();
    assert!(
        first
            .await
            .expect_err("caller cancellation should abort")
            .is_cancelled()
    );

    let second = task.execute_once_serialized(&query_engine, &frontend, None);
    futures::pin_mut!(second);
    assert!(
        matches!(futures::poll!(second.as_mut()), Poll::Pending),
        "the next round must remain pending while the retained guard is held"
    );
    assert_eq!(
        execution.calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the pending waiter must not enter the collaborator"
    );
    execution.release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), execution.finished.notified())
        .await
        .expect("delegate child did not release its guard");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), second)
            .await
            .expect("next round should proceed after child release")
            .unwrap(),
        None
    );
    assert_eq!(execution.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_scheduled_context_is_retained_until_delegate_child_releases_guard() {
    let TestTaskParts {
        task, query_engine, ..
    } = new_test_task_engine_and_plan_with_query("SELECT number, ts FROM numbers_with_ts", "sink")
        .await;
    let execution = Arc::new(RetainingExecution {
        calls: Default::default(),
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
        finished: Arc::new(tokio::sync::Notify::new()),
    });
    let task = task.with_execution(Some(execution.clone()));
    let (frontend, _handler) = FrontendClient::from_empty_grpc_handler(QueryOptions::default());
    let frontend = Arc::new(frontend);
    let scheduled = 1_700_000_000;

    let task_to_run = task.clone();
    let engine_to_run = query_engine.clone();
    let frontend_to_run = frontend.clone();
    let execution_call = tokio::spawn(async move {
        task_to_run
            .execute_once_serialized_at_scheduled_time(&engine_to_run, &frontend_to_run, scheduled)
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), execution.started.notified())
        .await
        .expect("scheduled delegate child did not start");
    assert_eq!(
        task.state
            .read()
            .unwrap()
            .query_ctx
            .extension(FLOW_SCHEDULED_TIME_MILLIS),
        Some("1700000000000"),
        "scheduled context restored before the delegate child released the guard"
    );
    execution_call.abort();
    match execution_call.await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(_) => panic!("scheduled caller cancellation should abort"),
    }
    assert_eq!(
        task.state
            .read()
            .unwrap()
            .query_ctx
            .extension(FLOW_SCHEDULED_TIME_MILLIS),
        Some("1700000000000"),
        "scheduled context restored after caller cancellation but before child release"
    );

    execution.release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), execution.finished.notified())
        .await
        .expect("scheduled delegate child did not release its guard");
    assert_eq!(
        task.state
            .read()
            .unwrap()
            .query_ctx
            .extension(FLOW_SCHEDULED_TIME_MILLIS),
        None,
        "scheduled context was not restored when child released guard"
    );
}

struct BlockingDefaultExecutionHandler {
    entered: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    dropped: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

struct DropAck(Option<tokio::sync::oneshot::Sender<()>>);

impl Drop for DropAck {
    fn drop(&mut self) {
        if let Some(dropped) = self.0.take() {
            let _ = dropped.send(());
        }
    }
}

#[async_trait::async_trait]
impl crate::batching_mode::frontend_client::GrpcQueryHandlerWithBoxedError
    for BlockingDefaultExecutionHandler
{
    async fn do_query(
        &self,
        _query: api::v1::greptime_request::Request,
        _ctx: QueryContextRef,
    ) -> std::result::Result<Output, BoxedError> {
        let _ack = DropAck(self.dropped.lock().unwrap().take());
        if let Some(entered) = self.entered.lock().unwrap().take() {
            let _ = entered.send(());
        }
        std::future::pending().await
    }
}

#[tokio::test]
async fn test_default_execution_remains_inline_and_cancellable() {
    let query = "SELECT number, date_bin(INTERVAL '5 second', ts) AS time_window \
                 FROM numbers_with_ts GROUP BY time_window, number";
    let TestTaskParts {
        task, query_engine, ..
    } = new_time_window_test_task_with_query(query).await;
    register_twe_sink(&query_engine, "missing_sink", 9200);
    task.mark_all_windows_as_dirty().unwrap();

    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
    let handler: Arc<dyn crate::batching_mode::frontend_client::GrpcQueryHandlerWithBoxedError> =
        Arc::new(BlockingDefaultExecutionHandler {
            entered: std::sync::Mutex::new(Some(entered_tx)),
            dropped: std::sync::Mutex::new(Some(dropped_tx)),
        });
    let frontend = Arc::new(FrontendClient::from_grpc_handler(
        Arc::downgrade(&handler),
        QueryOptions::default(),
    ));

    let task_to_cancel = task.clone();
    let engine_to_cancel = query_engine.clone();
    let frontend_to_cancel = frontend.clone();
    let caller = tokio::spawn(async move {
        task_to_cancel
            .execute_once_serialized(&engine_to_cancel, &frontend_to_cancel, None)
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), entered_rx)
        .await
        .expect("default execution did not dispatch a frontend query")
        .expect("default execution handler entry notification dropped");
    caller.abort();
    match caller.await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(_) => panic!("default caller cancellation should abort"),
    }
    tokio::time::timeout(Duration::from_secs(1), dropped_rx)
        .await
        .expect("cancelling default execution did not drop the active frontend future")
        .expect("default execution drop acknowledgement was not sent");
    tokio::time::timeout(
        Duration::from_secs(1),
        task.execution_lock.clone().lock_owned(),
    )
    .await
    .expect("default execution must not leave an owned child holding the lock");
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
    new_test_task_engine_and_plan_with_query_and_opts_and_required(
        query, sink_table, batch_opts, false,
    )
    .await
}

async fn new_test_task_engine_and_plan_with_query_and_opts_and_required(
    query: &str,
    sink_table: &str,
    batch_opts: Arc<BatchingModeOptions>,
    exact_sequence_range_required: bool,
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

    let task = BatchingTask::try_new_with_exact_sequence_range_required(
        TaskArgs {
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
            eval_schedule: None,
        },
        exact_sequence_range_required,
    )
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
async fn test_non_aggregate_scheduled_sql_honors_eval_offset_phase() {
    // A non-aggregate SQL flow with `EVAL INTERVAL` runs as an explicit
    // full-query flow on the batching scheduler. The typed schedule must reach
    // the task config unchanged and the offset must not be silently ignored:
    // due scheduled times follow the `anchor + k * interval` phase.
    let query = "SELECT number, ts FROM numbers_with_ts";
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), query, true)
        .await
        .unwrap();
    let (_tx, rx) = tokio::sync::oneshot::channel();

    let schedule = EvalSchedule::from_config(
        Some(3600),
        Some(&FlowScheduleConfig {
            anchor_secs: 120, // `EVAL OFFSET '2 minutes'`
            start_secs: 3720, // 120 + 1 * 3600
            missed_tick_policy: FlowMissedTickPolicy::BoundedCatchUp,
            catchup_max_runs: 3,
            catchup_max_lag_secs: 3600,
        }),
    )
    .unwrap()
    .unwrap();

    let task = BatchingTask::try_new(TaskArgs {
        flow_id: 1,
        query,
        plan,
        time_window_expr: None,
        expire_after: None,
        sink_table_name: [
            "greptime".to_string(),
            "public".to_string(),
            "scheduled_non_aggr_sink".to_string(),
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
        flow_eval_interval: Some(Duration::from_secs(3600)),
        eval_schedule: Some(schedule),
    })
    .unwrap();

    let stored = task.config.eval_schedule.as_ref().unwrap();
    assert_eq!(stored.anchor_secs, 120);
    assert_eq!(stored.start_secs, 3720);
    assert_eq!(stored.interval_secs, 3600);

    // Due-time selection proves the offset is honored: the first due time is
    // on the `:02` phase (120 + k * 3600), never 0 or 3600.
    let due = select_due_scheduled_times(stored, 0, 3720).unwrap();
    assert_eq!(due.scheduled_times_secs, vec![3720]);
    for t in &due.scheduled_times_secs {
        assert_eq!((t - 120) % 3600, 0);
    }
    let due = select_due_scheduled_times(stored, 3720, 7320).unwrap();
    assert_eq!(due.scheduled_times_secs, vec![7320]);
    for t in &due.scheduled_times_secs {
        assert_eq!((t - 120) % 3600, 0);
    }
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
        eval_schedule: None,
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

async fn configure_source_ttl(query_engine: &QueryEngineRef, ttl: Option<TimeToLive>) {
    let catalog_manager = query_engine.engine_state().catalog_manager();
    let source = catalog_manager
        .table(
            DEFAULT_CATALOG_NAME,
            DEFAULT_SCHEMA_NAME,
            "numbers_with_ts",
            None,
        )
        .await
        .unwrap()
        .unwrap();
    let mut info = (*source.table_info()).clone();
    info.meta.options.ttl = ttl;
    let source = Arc::new(Table::new(
        Arc::new(info),
        FilterPushDownType::Unsupported,
        source.data_source(),
    ));
    let memory_catalog = catalog_manager
        .as_any()
        .downcast_ref::<MemoryCatalogManager>()
        .unwrap();
    memory_catalog
        .deregister_table_sync(DeregisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: "numbers_with_ts".to_string(),
        })
        .unwrap();
    memory_catalog
        .register_table_sync(RegisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: "numbers_with_ts".to_string(),
            table_id: source.table_info().table_id(),
            table: source,
        })
        .unwrap();
}

async fn configure_source_capability(
    query_engine: &QueryEngineRef,
    engine: &str,
    preserve_row_sequence: bool,
) {
    let catalog_manager = query_engine.engine_state().catalog_manager();
    let source = catalog_manager
        .table(
            DEFAULT_CATALOG_NAME,
            DEFAULT_SCHEMA_NAME,
            "numbers_with_ts",
            None,
        )
        .await
        .unwrap()
        .unwrap();
    let mut info = (*source.table_info()).clone();
    info.meta.engine = engine.to_string();
    if preserve_row_sequence {
        info.meta
            .options
            .extra_options
            .insert("preserve_row_sequence".to_string(), "true".to_string());
    } else {
        info.meta
            .options
            .extra_options
            .remove("preserve_row_sequence");
    }
    let source = Arc::new(Table::new(
        Arc::new(info),
        FilterPushDownType::Unsupported,
        source.data_source(),
    ));
    let memory_catalog = catalog_manager
        .as_any()
        .downcast_ref::<MemoryCatalogManager>()
        .unwrap();
    memory_catalog
        .deregister_table_sync(DeregisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: "numbers_with_ts".to_string(),
        })
        .unwrap();
    memory_catalog
        .register_table_sync(RegisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: "numbers_with_ts".to_string(),
            table_id: source.table_info().table_id(),
            table: source,
        })
        .unwrap();
}

#[tokio::test]
async fn test_validate_recovery_retention_explicit_source_ttls() {
    struct Case {
        name: &'static str,
        ttl: Option<TimeToLive>,
        expire_after: Option<i64>,
        retention_age: Option<Duration>,
        window_age: Option<Duration>,
        should_pass: bool,
    }

    let cases = [
        Case {
            name: "finite TTL exceeding expire_after is admissible",
            ttl: Some(TimeToLive::Duration(Duration::from_secs(2 * 60 * 60))),
            expire_after: Some(60 * 60),
            retention_age: Some(Duration::from_secs(10 * 60)),
            window_age: None,
            should_pass: true,
        },
        Case {
            name: "TTL equal to expire_after is rejected",
            ttl: Some(TimeToLive::Duration(Duration::from_secs(60 * 60))),
            expire_after: Some(60 * 60),
            retention_age: Some(Duration::from_secs(10 * 60)),
            window_age: None,
            should_pass: false,
        },
        Case {
            name: "TTL shorter than expire_after is rejected",
            ttl: Some(TimeToLive::Duration(Duration::from_secs(59 * 60))),
            expire_after: Some(60 * 60),
            retention_age: Some(Duration::from_secs(10 * 60)),
            window_age: None,
            should_pass: false,
        },
        Case {
            name: "aligned window older than TTL is rejected",
            ttl: Some(TimeToLive::Duration(Duration::from_secs(2 * 60 * 60))),
            expire_after: Some(60 * 60),
            retention_age: Some(Duration::from_secs(10 * 60)),
            window_age: Some(Duration::from_secs(3 * 60 * 60)),
            should_pass: false,
        },
        Case {
            name: "old retention cutoff is rejected even without windows",
            ttl: Some(TimeToLive::Duration(Duration::from_secs(2 * 60 * 60))),
            expire_after: Some(60 * 60),
            retention_age: Some(Duration::from_secs(3 * 60 * 60)),
            window_age: None,
            should_pass: false,
        },
        Case {
            name: "explicit forever is admissible",
            ttl: Some(TimeToLive::Forever),
            expire_after: None,
            retention_age: None,
            window_age: None,
            should_pass: true,
        },
        Case {
            name: "instant TTL is rejected",
            ttl: Some(TimeToLive::Instant),
            expire_after: Some(60 * 60),
            retention_age: Some(Duration::from_secs(10 * 60)),
            window_age: None,
            should_pass: false,
        },
    ];

    for case in cases {
        let TestTaskParts {
            mut task,
            query_engine,
            ..
        } = new_test_task_engine_and_plan_with_query(
            "SELECT number, ts FROM numbers_with_ts",
            "sink",
        )
        .await;
        Arc::get_mut(&mut task.config).unwrap().expire_after = case.expire_after;
        configure_source_ttl(&query_engine, case.ttl).await;

        let now = Timestamp::current_millis();
        let retention_lower = case.retention_age.map(|age| now.sub_duration(age).unwrap());
        let windows = case
            .window_age
            .map(|age| {
                let start = now.sub_duration(age).unwrap();
                vec![(start, now)]
            })
            .unwrap_or_default();
        assert_eq!(
            task.validate_recovery_retention(retention_lower, &windows)
                .await
                .is_ok(),
            case.should_pass,
            "{}",
            case.name
        );
    }
}

#[tokio::test]
async fn test_validate_recovery_retention_rejects_unknown_inherited_ttl() {
    let TestTaskParts {
        mut task,
        query_engine,
        ..
    } = new_test_task_engine_and_plan_with_query("SELECT number, ts FROM numbers_with_ts", "sink")
        .await;
    Arc::get_mut(&mut task.config).unwrap().expire_after = Some(60 * 60);
    configure_source_ttl(&query_engine, None).await;

    assert!(
        task.validate_recovery_retention(Some(Timestamp::current_millis()), &[],)
            .await
            .is_err()
    );
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

// --- scheduled-time QueryContext restore regression tests ---

/// Register a sink table whose schema matches the output of a `date_bin`
/// time-window-expression query (columns: `number` uint32, `time_window` timestamp).
fn register_twe_sink(query_engine: &QueryEngineRef, table_name: &str, table_id: u32) {
    let schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("number", CDT::uint32_datatype(), false),
        ColumnSchema::new("time_window", CDT::timestamp_millisecond_datatype(), false)
            .with_time_index(true),
    ]));
    let columns: Vec<VectorRef> = vec![
        Arc::new(UInt32Vector::from_slice([1_u32])),
        Arc::new(TimestampMillisecondVector::from_slice([0_i64])),
    ];
    let recordbatch = RecordBatch::new(schema, columns).unwrap();
    let table = MemTable::table(table_name, recordbatch);
    let request = RegisterTableRequest {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_SCHEMA_NAME.to_string(),
        table_name: table_name.to_string(),
        table_id,
        table,
    };
    let catalog_manager = query_engine.engine_state().catalog_manager();
    let memory_catalog = catalog_manager
        .as_any()
        .downcast_ref::<MemoryCatalogManager>()
        .unwrap();
    memory_catalog.register_table_sync(request).unwrap();
}

fn register_scheduled_now_sink(query_engine: &QueryEngineRef, table_name: &str, table_id: u32) {
    let schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("ts", CDT::timestamp_nanosecond_datatype(), false).with_time_index(true),
        ColumnSchema::new("number", CDT::uint32_datatype(), false),
    ]));
    let columns: Vec<VectorRef> = vec![
        Arc::new(TimestampNanosecondVector::from_slice([0_i64])),
        Arc::new(UInt32Vector::from_slice([1_u32])),
    ];
    let recordbatch = RecordBatch::new(schema, columns).unwrap();
    let table = MemTable::table(table_name, recordbatch);
    let request = RegisterTableRequest {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_SCHEMA_NAME.to_string(),
        table_name: table_name.to_string(),
        table_id,
        table,
    };
    let catalog_manager = query_engine.engine_state().catalog_manager();
    let memory_catalog = catalog_manager
        .as_any()
        .downcast_ref::<MemoryCatalogManager>()
        .unwrap();
    memory_catalog.register_table_sync(request).unwrap();
}

struct ExactDeltaFailureHandler {
    invoked: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl crate::batching_mode::frontend_client::GrpcQueryHandlerWithBoxedError
    for ExactDeltaFailureHandler
{
    async fn do_query(
        &self,
        _query: api::v1::greptime_request::Request,
        ctx: QueryContextRef,
    ) -> std::result::Result<Output, BoxedError> {
        self.invoked.store(true, Ordering::SeqCst);
        assert_eq!(ctx.extension(FLOW_INCREMENTAL_MODE), Some("sequence_range"));
        assert_eq!(
            ctx.extension(FLOW_INCREMENTAL_AFTER_SEQS),
            Some("{\"1\":10}")
        );
        Err(BoxedError::new(MockError::new(StatusCode::RequestOutdated)))
    }
}

/// What a recovery capture request asked the frontend to run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryTimestampPlanShape {
    /// Raw source timestamps, decoded into windows locally.
    RawTimestampProjection,
    /// One representative timestamp per time window, grouped remotely.
    RemoteWindowDedup,
}

/// What a recovery capture request encoded, audited by the test frontend handlers.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RecoveryCaptureAudit {
    shape: RecoveryTimestampPlanShape,
    expiry_lower: Option<Timestamp>,
}

struct RecoveryCaptureHandler {
    output: std::sync::Mutex<Option<Output>>,
    lower: String,
    source_table: String,
    expects_number_filter: bool,
    expire_after: Option<i64>,
    audit: std::sync::Mutex<Option<RecoveryCaptureAudit>>,
    /// When set, the handler decodes the request's logical plan with the frontend's
    /// Substrait decoder and executes it locally against this engine (no distributed
    /// execution), instead of returning canned output.
    execute: Option<(QueryEngineRef, QueryContextRef)>,
    /// Number of rows the executed plan returned.
    executed_rows: std::sync::Mutex<Option<usize>>,
}

struct RecoveryMetricsStream {
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
    metrics: Option<RecordBatchMetrics>,
    fail: bool,
}

impl futures::Stream for RecoveryMetricsStream {
    type Item = common_recordbatch::error::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.fail {
            self.fail = false;
            return Poll::Ready(Some(Err(common_recordbatch::error::Error::External {
                source: BoxedError::new(MockError::new(StatusCode::Unexpected)),
                location: snafu::Location::new(file!(), line!(), column!()),
            })));
        }
        Poll::Ready(self.batches.pop().map(Ok))
    }
}

impl common_recordbatch::RecordBatchStream for RecoveryMetricsStream {
    fn name(&self) -> &str {
        "RecoveryMetricsStream"
    }

    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn output_ordering(&self) -> Option<&[common_recordbatch::OrderOption]> {
        None
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        self.batches
            .is_empty()
            .then(|| self.metrics.clone())
            .flatten()
    }
}

fn field_selection(expr: &substrait::substrait_proto_df::proto::Expression) -> Option<i32> {
    use substrait::substrait_proto_df::proto::expression::RexType;
    use substrait::substrait_proto_df::proto::expression::field_reference::ReferenceType;
    use substrait::substrait_proto_df::proto::expression::reference_segment::ReferenceType as Segment;

    let RexType::Selection(field) = expr.rex_type.as_ref()? else {
        return None;
    };
    let ReferenceType::DirectReference(reference) = field.reference_type.as_ref()? else {
        return None;
    };
    let Segment::StructField(field) = reference.reference_type.as_ref()? else {
        return None;
    };
    Some(field.field)
}

fn timestamp_literal(expr: &substrait::substrait_proto_df::proto::Expression) -> Option<Timestamp> {
    use substrait::substrait_proto_df::proto::expression::RexType;
    use substrait::substrait_proto_df::proto::expression::literal::LiteralType;

    let RexType::Literal(literal) = expr.rex_type.as_ref()? else {
        return None;
    };
    let LiteralType::PrecisionTimestamp(timestamp) = literal.literal_type.as_ref()? else {
        return None;
    };
    match timestamp.precision {
        0 => Some(Timestamp::new_second(timestamp.value)),
        3 => Some(Timestamp::new_millisecond(timestamp.value)),
        6 => Some(Timestamp::new_microsecond(timestamp.value)),
        9 => Some(Timestamp::new_nanosecond(timestamp.value)),
        _ => None,
    }
}

/// Audits the Substrait plan the recovery capture path sends to the frontend.
///
/// Both capture shapes must preserve the qualified source table, the query's `WHERE`,
/// and the recovery retention filter. The remote window dedup shape must group by a
/// single `CASE` key and must not aggregate values; the exact guarding semantics of the
/// key are covered by the unit and decoded-plan execution tests.
fn assert_recovery_timestamp_plan(
    plan: &Plan,
    source_table: &str,
    expects_number_filter: bool,
    expire_after: Option<i64>,
) -> RecoveryCaptureAudit {
    use substrait::substrait_proto_df::proto::expression::RexType;
    use substrait::substrait_proto_df::proto::plan_rel::RelType as PlanRelType;
    use substrait::substrait_proto_df::proto::rel::RelType;

    let function_names = plan
        .extensions
        .iter()
        .filter_map(|extension| match extension.mapping_type.as_ref()? {
            substrait::substrait_proto_df::proto::extensions::simple_extension_declaration::MappingType::ExtensionFunction(function) => {
                Some((function.function_anchor, function.name.as_str()))
            }
            _ => None,
        })
        .collect::<HashMap<_, _>>();
    let [root] = plan.relations.as_slice() else {
        panic!("recovery capture must encode exactly one relation");
    };
    let Some(PlanRelType::Root(root)) = root.rel_type.as_ref() else {
        panic!("recovery capture must encode a root relation");
    };
    assert_eq!(
        root.names.len(),
        1,
        "recovery capture must return exactly one column"
    );

    let input = root.input.as_ref().and_then(|rel| rel.rel_type.as_ref());
    let (shape, inner) = match input {
        Some(RelType::Project(project)) => {
            assert_eq!(root.names, ["ts"]);
            assert_eq!(project.expressions.len(), 1);
            assert_eq!(field_selection(&project.expressions[0]), Some(1));
            (
                RecoveryTimestampPlanShape::RawTimestampProjection,
                project.input.as_ref().expect("recovery projection input"),
            )
        }
        Some(RelType::Aggregate(aggregate)) => {
            assert!(
                aggregate.measures.is_empty(),
                "recovery window dedup must not aggregate values"
            );
            let [group_expr] = aggregate.grouping_expressions.as_slice() else {
                panic!("recovery window dedup must group by exactly one key");
            };
            assert!(
                matches!(group_expr.rex_type.as_ref(), Some(RexType::IfThen(_))),
                "recovery window dedup must group by a CASE key, got {group_expr:?}"
            );
            (
                RecoveryTimestampPlanShape::RemoteWindowDedup,
                aggregate.input.as_ref().expect("recovery aggregate input"),
            )
        }
        other => panic!(
            "recovery capture must project raw timestamps or group the deduplicated window key, got {other:?}"
        ),
    };

    fn visit(
        rel: &substrait::substrait_proto_df::proto::Rel,
        function_names: &HashMap<u32, &str>,
        scans: &mut Vec<Vec<String>>,
        has_number_filter: &mut bool,
        expiry_lower: &mut Option<Timestamp>,
    ) {
        use substrait::substrait_proto_df::proto::expression::RexType;
        use substrait::substrait_proto_df::proto::expression::literal::LiteralType;
        use substrait::substrait_proto_df::proto::function_argument::ArgType;
        use substrait::substrait_proto_df::proto::read_rel::ReadType;
        use substrait::substrait_proto_df::proto::rel::RelType;

        match rel.rel_type.as_ref().expect("recovery relation type") {
            RelType::Aggregate(_) | RelType::Write(_) | RelType::Ddl(_) | RelType::Update(_) => {
                panic!("recovery capture must not aggregate or write")
            }
            RelType::Read(read) => match read.read_type.as_ref() {
                Some(ReadType::NamedTable(table)) => scans.push(table.names.clone()),
                _ => panic!("recovery capture must read a named source table"),
            },
            RelType::Filter(filter) => {
                let condition = filter
                    .condition
                    .as_ref()
                    .expect("recovery filter condition");
                let RexType::ScalarFunction(function) = condition
                    .rex_type
                    .as_ref()
                    .expect("recovery filter expression")
                else {
                    panic!("recovery filter must be a scalar function");
                };
                let args = function
                    .arguments
                    .iter()
                    .map(|arg| match arg.arg_type.as_ref() {
                        Some(ArgType::Value(expr)) => expr,
                        _ => panic!("recovery filter must use value arguments"),
                    })
                    .collect::<Vec<_>>();
                match function_names.get(&function.function_reference) {
                    Some(&"equal")
                        if args.len() == 2
                            && field_selection(args[0]) == Some(0)
                            && matches!(
                                args[1].rex_type.as_ref(),
                                Some(RexType::Literal(literal))
                                    if matches!(literal.literal_type, Some(LiteralType::I64(42)))
                            ) =>
                    {
                        *has_number_filter = true
                    }
                    Some(&"gte") if args.len() == 2 && field_selection(args[0]) == Some(1) => {
                        *expiry_lower = timestamp_literal(args[1]);
                    }
                    _ => panic!("unexpected recovery filter"),
                }
                visit(
                    filter.input.as_ref().expect("recovery filter input"),
                    function_names,
                    scans,
                    has_number_filter,
                    expiry_lower,
                );
            }
            RelType::Project(project) => visit(
                project.input.as_ref().expect("recovery project input"),
                function_names,
                scans,
                has_number_filter,
                expiry_lower,
            ),
            _ => panic!("unexpected recovery relation"),
        }
    }

    let mut scans = Vec::new();
    let mut has_number_filter = false;
    let mut expiry_lower = None;
    visit(
        inner,
        &function_names,
        &mut scans,
        &mut has_number_filter,
        &mut expiry_lower,
    );
    assert_eq!(
        scans,
        vec![vec![
            DEFAULT_CATALOG_NAME.to_string(),
            DEFAULT_SCHEMA_NAME.to_string(),
            source_table.to_string(),
        ]],
        "recovery capture must scan the fully qualified source table"
    );
    assert_eq!(
        has_number_filter, expects_number_filter,
        "recovery capture must preserve the query WHERE clause"
    );
    assert_eq!(expiry_lower.is_some(), expire_after.is_some());
    RecoveryCaptureAudit {
        shape,
        expiry_lower,
    }
}

#[async_trait::async_trait]
impl crate::batching_mode::frontend_client::GrpcQueryHandlerWithBoxedError
    for RecoveryCaptureHandler
{
    async fn do_query(
        &self,
        query: api::v1::greptime_request::Request,
        ctx: QueryContextRef,
    ) -> std::result::Result<Output, BoxedError> {
        assert_eq!(ctx.extension(FLOW_INCREMENTAL_MODE), Some("sequence_range"));
        assert_eq!(
            ctx.extension(FLOW_INCREMENTAL_AFTER_SEQS),
            Some(self.lower.as_str())
        );
        assert_eq!(
            ctx.extension(query::options::FLOW_RETURN_REGION_SEQ),
            Some("true")
        );
        let api::v1::greptime_request::Request::Query(request) = query else {
            panic!("recovery capture must issue a query request");
        };
        let Some(api::v1::query_request::Query::LogicalPlan(plan_bytes)) = request.query else {
            panic!("recovery capture must issue a logical plan read");
        };
        let plan = Plan::decode(plan_bytes.as_slice()).unwrap();
        *self.audit.lock().unwrap() = Some(assert_recovery_timestamp_plan(
            &plan,
            &self.source_table,
            self.expects_number_filter,
            self.expire_after,
        ));
        if let Some((engine, ctx)) = &self.execute {
            // Decode exactly like the frontend does for `Query::LogicalPlan`, then
            // execute the decoded plan locally. This exercises the serialized plan's
            // executable form; it does not run distributed execution and proves nothing
            // about region pushdown.
            let session_state =
                SessionStateBuilder::new_from_existing(engine.engine_state().session_state())
                    .with_catalog_list(Arc::new(DummyCatalogList::new_with_query_ctx(
                        engine.engine_state().catalog_manager().clone(),
                        ctx.clone(),
                    )))
                    .build();
            let plan = DFLogicalSubstraitConvertor
                .decode(plan_bytes.as_slice(), session_state)
                .await
                .unwrap();
            let output = engine.execute(plan, ctx.clone()).await.unwrap();
            let OutputData::Stream(stream) = output.data else {
                panic!("recovery capture plan must return a stream");
            };
            let batches = common_recordbatch::util::collect_batches(stream)
                .await
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>();
            *self.executed_rows.lock().unwrap() =
                Some(batches.iter().map(|batch| batch.num_rows()).sum());
            return Ok(Output::new_with_stream(Box::pin(RecoveryMetricsStream {
                schema: recovery_timestamp_schema(),
                batches,
                metrics: Some(RecordBatchMetrics {
                    region_watermarks: vec![RegionWatermarkEntry {
                        region_id: 1,
                        watermark: Some(10),
                    }],
                    ..Default::default()
                }),
                fail: false,
            })));
        }
        Ok(self.output.lock().unwrap().take().unwrap())
    }
}

fn recovery_timestamp_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        ColumnSchema::new("ts", CDT::timestamp_millisecond_datatype(), true).with_time_index(true),
    ]))
}

fn recovery_stream_output(
    batches: Vec<RecordBatch>,
    watermarks: Option<Vec<(u64, Option<u64>)>>,
    fail: bool,
) -> Output {
    Output::new_with_stream(Box::pin(RecoveryMetricsStream {
        schema: recovery_timestamp_schema(),
        batches,
        metrics: watermarks.map(|watermarks| RecordBatchMetrics {
            region_watermarks: watermarks
                .into_iter()
                .map(|(region_id, watermark)| RegionWatermarkEntry {
                    region_id,
                    watermark,
                })
                .collect(),
            ..Default::default()
        }),
        fail,
    }))
}

fn recovery_timestamp_batch(values: Vec<Option<i64>>) -> RecordBatch {
    RecordBatch::new(
        recovery_timestamp_schema(),
        vec![Arc::new(TimestampMillisecondVector::from(values)) as VectorRef],
    )
    .unwrap()
}

fn seed_recovery_state(
    task: &BatchingTask,
    lower: &BTreeMap<u64, u64>,
) -> (BTreeMap<u64, u64>, String) {
    let mut state = task.state.write().unwrap();
    state.advance_checkpoints(lower.iter().map(|(region, seq)| (*region, *seq)).collect());
    state.dirty_time_windows.add_window(
        Timestamp::new_millisecond(20_000),
        Some(Timestamp::new_millisecond(25_000)),
    );
    (
        state.checkpoints().clone(),
        format!("{:?}", state.dirty_time_windows),
    )
}

fn assert_recovery_state_unchanged(
    task: &BatchingTask,
    checkpoints: &BTreeMap<u64, u64>,
    dirty: &str,
) {
    let state = task.state.read().unwrap();
    assert_eq!(state.checkpoints(), checkpoints);
    assert_eq!(format!("{:?}", state.dirty_time_windows), dirty);
}

struct CaptureScheduledNowHandler {
    expected_extension: String,
    captured_sql: Arc<std::sync::Mutex<Option<String>>>,
    query_engine: QueryEngineRef,
}

#[async_trait::async_trait]
impl crate::batching_mode::frontend_client::GrpcQueryHandlerWithBoxedError
    for CaptureScheduledNowHandler
{
    async fn do_query(
        &self,
        query: api::v1::greptime_request::Request,
        ctx: QueryContextRef,
    ) -> std::result::Result<Output, BoxedError> {
        assert_eq!(
            ctx.extension(FLOW_SCHEDULED_TIME_MILLIS),
            Some(self.expected_extension.as_str())
        );

        let api::v1::greptime_request::Request::Query(api::v1::QueryRequest {
            query: Some(api::v1::query_request::Query::Sql(sql)),
            ..
        }) = query
        else {
            panic!("expected scheduled SQL flow to send a SQL query, got {query:?}");
        };

        let planned = sql_to_df_plan(ctx, self.query_engine.clone(), &sql, true)
            .await
            .unwrap();
        assert_sql_uses_scheduled_time(&planned.to_string());

        *self.captured_sql.lock().unwrap() = Some(sql);
        Ok(Output::new_with_affected_rows(1))
    }
}

fn assert_sql_uses_scheduled_time(sql: &str) {
    let lower = sql.to_ascii_lowercase();
    assert!(!lower.contains("now()"), "SQL still contains now(): {sql}");
    assert!(
        sql.contains("2023-11-14") || sql.contains("1700000000"),
        "SQL does not contain the scheduled date or epoch value: {sql}"
    );
    assert!(
        sql.contains("22:13:20") || sql.contains("1700000000"),
        "SQL does not contain the scheduled time or epoch value: {sql}"
    );
}

/// After a scheduled-time attempt fails (missing sink), the
/// `FLOW_SCHEDULED_TIME_MILLIS` extension must not leak into
/// `TaskState.query_ctx` or `frontend_extensions()`.
#[tokio::test]
async fn test_scheduled_time_ctx_restored_on_error() {
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

    // Before: no scheduled time extension
    assert_eq!(
        task.state
            .read()
            .unwrap()
            .query_ctx
            .extension(FLOW_SCHEDULED_TIME_MILLIS),
        None
    );
    assert!(
        !task
            .frontend_extensions()
            .contains_key(FLOW_SCHEDULED_TIME_MILLIS)
    );

    let scheduled_time_secs = 1700000000i64;
    let outcome = task
        .execute_once_serialized_at_scheduled_time(
            &query_engine,
            &frontend_client,
            scheduled_time_secs,
        )
        .await;

    // Missing sink table → gen_insert_plan_unlocked should fail.
    assert!(
        outcome.result.is_err(),
        "Expected an error (missing sink), got {:?}",
        outcome.result
    );

    // After: extension must be restored to absent.
    assert_eq!(
        task.state
            .read()
            .unwrap()
            .query_ctx
            .extension(FLOW_SCHEDULED_TIME_MILLIS),
        None,
        "FLOW_SCHEDULED_TIME_MILLIS leaked into query_ctx after error"
    );
    assert!(
        !task
            .frontend_extensions()
            .contains_key(FLOW_SCHEDULED_TIME_MILLIS),
        "FLOW_SCHEDULED_TIME_MILLIS leaked into frontend_extensions after error"
    );
}

/// After a scheduled-time attempt returns `Ok(None)` (no dirty windows),
/// the `FLOW_SCHEDULED_TIME_MILLIS` extension must not leak into
/// `TaskState.query_ctx`.
#[tokio::test]
async fn test_scheduled_time_ctx_restored_on_no_dirty_windows() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let plan_query = "SELECT number, date_bin(INTERVAL '5 second', ts) AS time_window \
                      FROM numbers_with_ts GROUP BY time_window, number";
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
    let time_window_expr = time_window_expr
        .map(|expr| {
            TimeWindowExpr::from_expr(
                &expr,
                &column_name,
                &df_schema,
                &query_engine.engine_state().session_state(),
            )
        })
        .transpose()
        .unwrap();

    let sink_table_name = "twe_sink_for_ctx_restore";
    register_twe_sink(&query_engine, sink_table_name, 9101);

    let (_tx, rx) = tokio::sync::oneshot::channel();
    let task = BatchingTask::try_new(TaskArgs {
        flow_id: 1,
        query: plan_query,
        plan: plan.clone(),
        time_window_expr,
        expire_after: None,
        sink_table_name: [
            "greptime".to_string(),
            "public".to_string(),
            sink_table_name.to_string(),
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
        eval_schedule: None,
    })
    .unwrap();

    let (frontend_client, _handler) =
        FrontendClient::from_empty_grpc_handler(QueryOptions::default());
    let frontend_client = Arc::new(frontend_client);

    // Before: no scheduled time extension
    assert_eq!(
        task.state
            .read()
            .unwrap()
            .query_ctx
            .extension(FLOW_SCHEDULED_TIME_MILLIS),
        None
    );

    let scheduled_time_secs = 1700000000i64;
    let outcome = task
        .execute_once_serialized_at_scheduled_time(
            &query_engine,
            &frontend_client,
            scheduled_time_secs,
        )
        .await;

    // No dirty windows → scoped repair returns None → outcome is Ok(None).
    assert!(
        matches!(outcome.result, Ok(None)),
        "Expected Ok(None) (no dirty windows), got {:?}",
        outcome.result
    );

    // After: extension must be restored to absent.
    assert_eq!(
        task.state
            .read()
            .unwrap()
            .query_ctx
            .extension(FLOW_SCHEDULED_TIME_MILLIS),
        None,
        "FLOW_SCHEDULED_TIME_MILLIS leaked into query_ctx after Ok(None)"
    );
}

#[tokio::test]
async fn test_scheduled_time_now_is_bound_to_selected_attempt() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let query = "SELECT date_trunc('second', now()) AS ts, number FROM numbers_with_ts";
    let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), query, true)
        .await
        .unwrap();
    let sink_table_name = "scheduled_now_sink";
    register_scheduled_now_sink(&query_engine, sink_table_name, 9102);
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let task = BatchingTask::try_new(TaskArgs {
        flow_id: 1,
        query,
        plan,
        time_window_expr: None,
        expire_after: None,
        sink_table_name: [
            "greptime".to_string(),
            "public".to_string(),
            sink_table_name.to_string(),
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
        flow_eval_interval: Some(Duration::from_secs(1)),
        eval_schedule: None,
    })
    .unwrap();

    let scheduled_time_secs = 1_700_000_000_i64;
    let expected_extension = (scheduled_time_secs * 1000).to_string();
    let captured_sql = Arc::new(std::sync::Mutex::new(None));
    let handler: Arc<dyn crate::batching_mode::frontend_client::GrpcQueryHandlerWithBoxedError> =
        Arc::new(CaptureScheduledNowHandler {
            expected_extension,
            captured_sql: captured_sql.clone(),
            query_engine: query_engine.clone(),
        });
    let frontend_client = Arc::new(FrontendClient::from_grpc_handler(
        Arc::downgrade(&handler),
        QueryOptions::default(),
    ));

    let outcome = task
        .execute_once_serialized_at_scheduled_time(
            &query_engine,
            &frontend_client,
            scheduled_time_secs,
        )
        .await;

    assert!(
        matches!(outcome.result, Ok(Some((1, _)))),
        "scheduled attempt should execute once, got {:?}",
        outcome.result
    );
    let sent_sql = captured_sql
        .lock()
        .unwrap()
        .clone()
        .expect("frontend handler should capture generated SQL");
    assert!(!sent_sql.is_empty());
}

/// The scheduled-loop logical-time arithmetic must never clamp, wrap, or
/// panic near the `i64` boundaries: unrepresentable values are explicit
/// errors, representable values are exact.
#[test]
fn test_scheduled_loop_arithmetic_near_i64_boundary() {
    assert_eq!(initial_schedule_cursor(3720, 3600).unwrap(), 120);
    assert_eq!(
        initial_schedule_cursor(i64::MAX, 3600).unwrap(),
        i64::MAX - 3600
    );
    // start == i64::MIN cannot go one interval earlier: explicit error, never
    // a saturated cursor equal to start that would silently skip the first
    // scheduled tick.
    assert!(initial_schedule_cursor(i64::MIN, 3600).is_err());
    assert_eq!(
        initial_schedule_cursor(i64::MIN + 3600, 3600).unwrap(),
        i64::MIN
    );

    assert_eq!(
        scheduled_time_millis(1_700_000_000).unwrap(),
        1_700_000_000_000
    );
    // Largest seconds value whose millisecond product still fits in i64.
    let max_secs = i64::MAX / 1000;
    assert_eq!(scheduled_time_millis(max_secs).unwrap(), max_secs * 1000);
    // One more second overflows: explicit error, never a saturated i64::MAX.
    let err = scheduled_time_millis(max_secs + 1).unwrap_err();
    assert!(err.to_string().contains("milliseconds"), "{err}");
    assert!(scheduled_time_millis(i64::MAX).is_err());

    // The widest representable gap (i64::MIN..=i64::MAX) is exactly u64::MAX;
    // subtracting in i64 would panic in debug and wrap in release, so the
    // i128 path must return the exact u64 value.
    assert_eq!(
        sleep_delta_secs(i64::MAX, i64::MIN).unwrap(),
        u64::MAX,
        "i64::MAX - i64::MIN must be exactly u64::MAX, not a wrapped value"
    );
    assert_eq!(sleep_delta_secs(7320, 3720).unwrap(), 3600);
    // A negative delta (next <= wall_now, violating the caller's guard) is an
    // explicit error instead of a wrapped huge `as u64` sleep.
    assert!(sleep_delta_secs(100, 200).is_err());
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

#[tokio::test]
async fn test_capture_recovery_windows_dedups_remote_windows_with_where_and_expiry() {
    let TestTaskParts {
        mut task,
        query_engine,
        ..
    } = new_time_window_test_task_with_query(
        "SELECT max(src.number) AS output_value, date_bin(INTERVAL '5 second', src.ts) AS output_window FROM numbers_with_ts AS src WHERE src.number = 42 GROUP BY output_window",
    )
    .await;
    configure_source_capability(&query_engine, "mito", true).await;
    let expire_after = 5;
    Arc::get_mut(&mut task.config)
        .expect("test task config should be uniquely owned")
        .expire_after = Some(expire_after);
    let lower = BTreeMap::from([(1, 10)]);
    let handler = Arc::new(RecoveryCaptureHandler {
        output: std::sync::Mutex::new(Some(recovery_stream_output(
            vec![],
            Some(vec![(1, Some(10))]),
            false,
        ))),
        lower: serde_json::to_string(&lower).unwrap(),
        expire_after: Some(expire_after),
        source_table: "numbers_with_ts".to_string(),
        expects_number_filter: true,
        audit: std::sync::Mutex::new(None),
        execute: None,
        executed_rows: std::sync::Mutex::new(None),
    });
    let frontend = FrontendClient::from_grpc_handler(
        Arc::downgrade(
            &(handler.clone()
                as Arc<dyn crate::batching_mode::frontend_client::GrpcQueryHandlerWithBoxedError>),
        ),
        QueryOptions::default(),
    );
    let (before_checkpoints, before_dirty) = seed_recovery_state(&task, &lower);
    let before = Timestamp::new_second(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64,
    );

    let (high, windows) = task
        .capture_recovery_windows(&query_engine, &frontend, &lower)
        .await
        .unwrap();
    let after = Timestamp::new_second(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64,
    );

    assert_eq!(high, lower);
    assert!(windows.is_empty());
    let audit = handler.audit.lock().unwrap().clone().unwrap();
    assert_eq!(audit.shape, RecoveryTimestampPlanShape::RemoteWindowDedup);
    let expiry_lower = audit.expiry_lower.unwrap();
    let time_window_expr = task.config.time_window_expr.as_ref().unwrap();
    // The helper and samples may straddle a window boundary.
    let aligned_expiry = |now: Timestamp| {
        time_window_expr
            .eval(
                now.sub_duration(Duration::from_secs(expire_after as u64))
                    .unwrap(),
            )
            .unwrap()
            .0
            .unwrap()
    };
    assert!(aligned_expiry(before) <= expiry_lower && expiry_lower <= aligned_expiry(after));
    assert_eq!(
        time_window_expr.eval(expiry_lower).unwrap().0,
        Some(expiry_lower)
    );
    assert_recovery_state_unchanged(&task, &before_checkpoints, &before_dirty);
}

#[tokio::test]
async fn test_capture_recovery_windows_since_uses_supplied_retention_lower() {
    let TestTaskParts {
        task,
        query_engine,
        ..
    } = new_time_window_test_task_with_query(
        "SELECT max(number) AS output_value, date_bin(INTERVAL '5 second', ts) AS output_window FROM numbers_with_ts WHERE number = 42 GROUP BY output_window",
    )
    .await;
    configure_source_capability(&query_engine, "mito", true).await;
    let lower = BTreeMap::from([(1, 10)]);
    let retention_lower = Timestamp::new_second(-10);
    let handler = Arc::new(RecoveryCaptureHandler {
        output: std::sync::Mutex::new(Some(recovery_stream_output(
            vec![],
            Some(vec![(1, Some(10))]),
            false,
        ))),
        lower: serde_json::to_string(&lower).unwrap(),
        expire_after: Some(1),
        source_table: "numbers_with_ts".to_string(),
        expects_number_filter: true,
        audit: std::sync::Mutex::new(None),
        execute: None,
        executed_rows: std::sync::Mutex::new(None),
    });
    let frontend = FrontendClient::from_grpc_handler(
        Arc::downgrade(
            &(handler.clone()
                as Arc<dyn crate::batching_mode::frontend_client::GrpcQueryHandlerWithBoxedError>),
        ),
        QueryOptions::default(),
    );

    let (high, windows) = task
        .capture_recovery_windows_since(&query_engine, &frontend, &lower, Some(retention_lower))
        .await
        .unwrap();

    assert_eq!(high, lower);
    assert!(windows.is_empty());
    let audit = handler.audit.lock().unwrap().clone().unwrap();
    assert_eq!(audit.shape, RecoveryTimestampPlanShape::RemoteWindowDedup);
    assert_eq!(audit.expiry_lower, Some(retention_lower));
}

#[tokio::test]
async fn test_capture_recovery_windows_terminal_proof_cases_leave_state_unchanged() {
    struct Case {
        name: &'static str,
        lower: BTreeMap<u64, u64>,
        watermarks: Option<Vec<(u64, Option<u64>)>>,
        succeeds: bool,
    }

    let cases = vec![
        Case {
            name: "empty_stream_complete_proof_h_equals_c",
            lower: BTreeMap::from([(1, 10)]),
            watermarks: Some(vec![(1, Some(10))]),
            succeeds: true,
        },
        Case {
            name: "missing_metrics",
            lower: BTreeMap::from([(1, 10)]),
            watermarks: None,
            succeeds: false,
        },
        Case {
            name: "missing_c_region",
            lower: BTreeMap::from([(1, 10), (2, 20)]),
            watermarks: Some(vec![(1, Some(10))]),
            succeeds: false,
        },
        Case {
            name: "unknown_none_watermark",
            lower: BTreeMap::from([(1, 10), (2, 20)]),
            watermarks: Some(vec![(1, Some(10)), (2, None)]),
            succeeds: false,
        },
        Case {
            name: "unexpected_region",
            lower: BTreeMap::from([(1, 10)]),
            watermarks: Some(vec![(1, Some(10)), (2, Some(20))]),
            succeeds: false,
        },
        Case {
            name: "regressing_h",
            lower: BTreeMap::from([(1, 10)]),
            watermarks: Some(vec![(1, Some(9))]),
            succeeds: false,
        },
    ];

    for case in cases {
        let TestTaskParts {
            task, query_engine, ..
        } = new_time_window_test_task_with_query(
            "SELECT max(number) AS output_value, date_bin(INTERVAL '5 second', ts) AS output_window FROM numbers_with_ts WHERE number = 42 GROUP BY output_window",
        )
        .await;
        configure_source_capability(&query_engine, "mito", true).await;
        let handler = Arc::new(RecoveryCaptureHandler {
            output: std::sync::Mutex::new(Some(recovery_stream_output(
                vec![],
                case.watermarks,
                false,
            ))),
            lower: serde_json::to_string(&case.lower).unwrap(),
            expire_after: None,
            source_table: "numbers_with_ts".to_string(),
            expects_number_filter: true,
            audit: std::sync::Mutex::new(None),
            execute: None,
            executed_rows: std::sync::Mutex::new(None),
        });
        let handler_dyn: Arc<
            dyn crate::batching_mode::frontend_client::GrpcQueryHandlerWithBoxedError,
        > = handler.clone();
        let frontend = FrontendClient::from_grpc_handler(
            Arc::downgrade(&handler_dyn),
            QueryOptions::default(),
        );
        let (before_checkpoints, before_dirty) = seed_recovery_state(&task, &case.lower);

        let result = task
            .capture_recovery_windows(&query_engine, &frontend, &case.lower)
            .await;
        assert_eq!(result.is_ok(), case.succeeds, "{}: {result:?}", case.name);
        if case.succeeds {
            assert_eq!(
                result.unwrap(),
                (case.lower.clone(), vec![]),
                "{}",
                case.name
            );
        }
        assert_recovery_state_unchanged(&task, &before_checkpoints, &before_dirty);
    }
}

#[tokio::test]
async fn test_capture_recovery_windows_streams_sorted_unique_windows() {
    let TestTaskParts {
        task, query_engine, ..
    } = new_time_window_test_task_with_query(
        "SELECT max(number) AS output_value, date_bin(INTERVAL '5 second', ts) AS output_window FROM numbers_with_ts WHERE number = 42 GROUP BY output_window",
    )
    .await;
    configure_source_capability(&query_engine, "mito", true).await;
    let lower = BTreeMap::from([(1, 10)]);
    let handler = Arc::new(RecoveryCaptureHandler {
        output: std::sync::Mutex::new(Some(recovery_stream_output(
            vec![
                recovery_timestamp_batch(vec![Some(10_000), Some(-1), Some(1_000)]),
                recovery_timestamp_batch(vec![Some(11_000), Some(1_000), Some(-5_000)]),
            ],
            Some(vec![(1, Some(10))]),
            false,
        ))),
        lower: serde_json::to_string(&lower).unwrap(),
        expire_after: None,
        source_table: "numbers_with_ts".to_string(),
        expects_number_filter: true,
        audit: std::sync::Mutex::new(None),
        execute: None,
        executed_rows: std::sync::Mutex::new(None),
    });
    let handler_dyn: Arc<
        dyn crate::batching_mode::frontend_client::GrpcQueryHandlerWithBoxedError,
    > = handler.clone();
    let frontend =
        FrontendClient::from_grpc_handler(Arc::downgrade(&handler_dyn), QueryOptions::default());

    let (_, windows) = task
        .capture_recovery_windows(&query_engine, &frontend, &lower)
        .await
        .unwrap();
    assert_eq!(
        windows,
        vec![
            (
                Timestamp::new_millisecond(-5_000),
                Timestamp::new_millisecond(0)
            ),
            (
                Timestamp::new_millisecond(0),
                Timestamp::new_millisecond(5_000)
            ),
            (
                Timestamp::new_millisecond(10_000),
                Timestamp::new_millisecond(15_000)
            ),
        ]
    );
}

#[tokio::test]
async fn test_capture_recovery_windows_rejects_stream_error() {
    let TestTaskParts {
        task, query_engine, ..
    } = new_time_window_test_task_with_query(
        "SELECT max(number) AS output_value, date_bin(INTERVAL '5 second', ts) AS output_window FROM numbers_with_ts WHERE number = 42 GROUP BY output_window",
    )
    .await;
    configure_source_capability(&query_engine, "mito", true).await;
    let lower = BTreeMap::from([(1, 10)]);
    let handler = Arc::new(RecoveryCaptureHandler {
        output: std::sync::Mutex::new(Some(recovery_stream_output(
            vec![],
            Some(vec![(1, Some(10))]),
            true,
        ))),
        lower: serde_json::to_string(&lower).unwrap(),
        expire_after: None,
        source_table: "numbers_with_ts".to_string(),
        expects_number_filter: true,
        audit: std::sync::Mutex::new(None),
        execute: None,
        executed_rows: std::sync::Mutex::new(None),
    });
    let handler_dyn: Arc<
        dyn crate::batching_mode::frontend_client::GrpcQueryHandlerWithBoxedError,
    > = handler.clone();
    let frontend =
        FrontendClient::from_grpc_handler(Arc::downgrade(&handler_dyn), QueryOptions::default());

    assert!(
        task.capture_recovery_windows(&query_engine, &frontend, &lower)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn test_capture_recovery_windows_rejects_null_timestamp() {
    let TestTaskParts {
        task, query_engine, ..
    } = new_time_window_test_task_with_query(
        "SELECT max(number) AS output_value, date_bin(INTERVAL '5 second', ts) AS output_window FROM numbers_with_ts WHERE number = 42 GROUP BY output_window",
    )
    .await;
    configure_source_capability(&query_engine, "mito", true).await;
    let lower = BTreeMap::from([(1, 10)]);
    let handler = Arc::new(RecoveryCaptureHandler {
        output: std::sync::Mutex::new(Some(recovery_stream_output(
            vec![recovery_timestamp_batch(vec![None])],
            Some(vec![(1, Some(10))]),
            false,
        ))),
        lower: serde_json::to_string(&lower).unwrap(),
        expire_after: None,
        source_table: "numbers_with_ts".to_string(),
        expects_number_filter: true,
        audit: std::sync::Mutex::new(None),
        execute: None,
        executed_rows: std::sync::Mutex::new(None),
    });
    let handler_dyn: Arc<
        dyn crate::batching_mode::frontend_client::GrpcQueryHandlerWithBoxedError,
    > = handler.clone();
    let frontend =
        FrontendClient::from_grpc_handler(Arc::downgrade(&handler_dyn), QueryOptions::default());

    assert!(
        task.capture_recovery_windows(&query_engine, &frontend, &lower)
            .await
            .is_err()
    );
}

/// Registers a `dedup_unit_table` source with the requested time index unit and rows,
/// marks it as a sequence-range-capable mito source, and builds a batching task whose
/// time-window expression is derived from `query`.
async fn new_unit_time_window_test_task(
    query: &str,
    unit: TimeUnit,
    rows: &[i64],
) -> (BatchingTask, QueryEngineRef, LogicalPlan) {
    let query_engine = create_test_query_engine();
    let data_type = match unit {
        TimeUnit::Second => CDT::timestamp_second_datatype(),
        TimeUnit::Millisecond => CDT::timestamp_millisecond_datatype(),
        TimeUnit::Microsecond => CDT::timestamp_microsecond_datatype(),
        TimeUnit::Nanosecond => CDT::timestamp_nanosecond_datatype(),
    };
    let schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("number", CDT::uint32_datatype(), false),
        ColumnSchema::new("ts", data_type, false).with_time_index(true),
    ]));
    let numbers = (0..rows.len() as u32).collect::<Vec<_>>();
    let ts: VectorRef = match unit {
        TimeUnit::Second => Arc::new(TimestampSecondVector::from_vec(rows.to_vec())),
        TimeUnit::Millisecond => Arc::new(TimestampMillisecondVector::from_vec(rows.to_vec())),
        TimeUnit::Microsecond => Arc::new(TimestampMicrosecondVector::from_vec(rows.to_vec())),
        TimeUnit::Nanosecond => Arc::new(TimestampNanosecondVector::from_vec(rows.to_vec())),
    };
    let columns: Vec<VectorRef> = vec![Arc::new(UInt32Vector::from_slice(numbers)), ts];
    let recordbatch = RecordBatch::new(schema, columns).unwrap();
    let table = MemTable::table("dedup_unit_table", recordbatch);
    let mut info = (*table.table_info()).clone();
    // Recovery window capture requires a sequence-range-capable mito source.
    info.meta.engine = "mito".to_string();
    info.meta
        .options
        .extra_options
        .insert(PRESERVE_ROW_SEQUENCE.to_string(), "true".to_string());
    let catalog_manager = query_engine.engine_state().catalog_manager();
    let memory_catalog = catalog_manager
        .as_any()
        .downcast_ref::<MemoryCatalogManager>()
        .unwrap();
    memory_catalog
        .deregister_table_sync(DeregisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: "dedup_unit_table".to_string(),
        })
        .unwrap();
    memory_catalog
        .register_table_sync(RegisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: "dedup_unit_table".to_string(),
            table_id: 4242,
            table: Arc::new(Table::new(
                Arc::new(info),
                FilterPushDownType::Unsupported,
                table.data_source(),
            )),
        })
        .unwrap();

    let ctx = QueryContext::arc();
    let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), query, true)
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
            DEFAULT_CATALOG_NAME.to_string(),
            DEFAULT_SCHEMA_NAME.to_string(),
            "missing_sink".to_string(),
        ],
        source_table_names: vec![[
            DEFAULT_CATALOG_NAME.to_string(),
            DEFAULT_SCHEMA_NAME.to_string(),
            "dedup_unit_table".to_string(),
        ]],
        query_ctx: ctx,
        catalog_manager: query_engine.engine_state().catalog_manager().clone(),
        shutdown_rx: rx,
        batch_opts: incremental_batch_opts(),
        flow_eval_interval: None,
        eval_schedule: None,
    })
    .unwrap();

    (task, query_engine, plan)
}

/// Runs `plan` through the test query engine and returns its single timestamp column,
/// so tests observe real DataFusion execution instead of mocks.
async fn execute_timestamp_plan(
    engine: &QueryEngineRef,
    ctx: QueryContextRef,
    plan: LogicalPlan,
) -> Result<Vec<Option<Timestamp>>, Error> {
    let output = engine
        .execute(plan, ctx)
        .await
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?;
    let OutputData::Stream(stream) = output.data else {
        panic!("timestamp plan must return a stream");
    };
    collect_timestamp_batches(stream).await
}

async fn collect_timestamp_batches(
    stream: common_recordbatch::SendableRecordBatchStream,
) -> Result<Vec<Option<Timestamp>>, Error> {
    let batches = common_recordbatch::util::collect_batches(stream)
        .await
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?;
    let mut values = Vec::new();
    for batch in batches.iter() {
        let vector = Helper::try_into_vector(batch.column(0).clone())
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;
        for index in 0..vector.len() {
            values.push(vector.get(index).as_timestamp());
        }
    }
    Ok(values)
}

/// Projects the remote dedup grouping key over `input` so tests can compare what the
/// frontend would group on with what the local window decoder derives.
fn dedup_group_key_plan(input: &LogicalPlan, group_expr: Expr) -> LogicalPlan {
    LogicalPlan::Projection(Projection::try_new(vec![group_expr], Arc::new(input.clone())).unwrap())
}

/// Windows the recovery decoder derives from `values` through the real decode path.
fn decoded_windows(
    time_window_expr: &TimeWindowExpr,
    unit: TimeUnit,
    values: impl IntoIterator<Item = Option<i64>>,
) -> BTreeSet<(Timestamp, Timestamp)> {
    let batch = timestamp_batch(unit, values);
    let mut windows = BTreeSet::new();
    capture_recovery_batch_windows(&batch, time_window_expr, &mut windows).unwrap();
    windows
}

/// A one-column timestamp batch in `unit`.
fn timestamp_batch(unit: TimeUnit, values: impl IntoIterator<Item = Option<i64>>) -> RecordBatch {
    let values = values.into_iter().collect::<Vec<_>>();
    let vector: VectorRef = match unit {
        TimeUnit::Second => Arc::new(TimestampSecondVector::from(values)),
        TimeUnit::Millisecond => Arc::new(TimestampMillisecondVector::from(values)),
        TimeUnit::Microsecond => Arc::new(TimestampMicrosecondVector::from(values)),
        TimeUnit::Nanosecond => Arc::new(TimestampNanosecondVector::from(values)),
    };
    let schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("ts", vector.data_type(), true).with_time_index(true),
    ]));
    RecordBatch::new(schema, vec![vector]).unwrap()
}

/// Runs recovery capture for `dedup_unit_table` with canned frontend output and
/// returns the captured windows together with the audited plan shape.
async fn capture_windows_with_shape(
    task: &BatchingTask,
    engine: &QueryEngineRef,
    output: Output,
) -> (BTreeSet<(Timestamp, Timestamp)>, RecoveryTimestampPlanShape) {
    let lower = BTreeMap::from([(1, 10)]);
    let handler = Arc::new(RecoveryCaptureHandler {
        output: std::sync::Mutex::new(Some(output)),
        lower: serde_json::to_string(&lower).unwrap(),
        expire_after: None,
        source_table: "dedup_unit_table".to_string(),
        expects_number_filter: false,
        audit: std::sync::Mutex::new(None),
        execute: None,
        executed_rows: std::sync::Mutex::new(None),
    });
    let handler_dyn: Arc<
        dyn crate::batching_mode::frontend_client::GrpcQueryHandlerWithBoxedError,
    > = handler.clone();
    let frontend =
        FrontendClient::from_grpc_handler(Arc::downgrade(&handler_dyn), QueryOptions::default());
    let (_, windows) = task
        .capture_recovery_windows(engine, &frontend, &lower)
        .await
        .unwrap();
    let audit = handler.audit.lock().unwrap().clone().unwrap();
    (windows.into_iter().collect(), audit.shape)
}

/// Canned frontend output carrying raw source timestamps of `unit`.
fn raw_timestamp_output(unit: TimeUnit, rows: &[i64]) -> Output {
    let batch = timestamp_batch(unit, rows.iter().copied().map(Some));
    let schema = batch.schema.clone();
    Output::new_with_stream(Box::pin(RecoveryMetricsStream {
        schema,
        batches: vec![batch],
        metrics: Some(RecordBatchMetrics {
            region_watermarks: vec![RegionWatermarkEntry {
                region_id: 1,
                watermark: Some(10),
            }],
            ..Default::default()
        }),
        fail: false,
    }))
}

/// The nanosecond stride of a recognized dedup grouping key.
fn group_key_stride_ns(group_expr: &Expr) -> u128 {
    let Expr::Case(case) = group_expr else {
        panic!("dedup key must be a CASE: {group_expr:?}");
    };
    let (_, then) = &case.when_then_expr[0];
    let Expr::ScalarFunction(func) = then.as_ref() else {
        panic!("dedup key THEN branch must call date_bin: {then:?}");
    };
    match &func.args[0] {
        Expr::Literal(datafusion_common::ScalarValue::IntervalMonthDayNano(Some(interval)), _) => {
            interval.nanoseconds as u128
        }
        other => panic!("unexpected dedup stride literal: {other:?}"),
    }
}

#[tokio::test]
async fn test_remote_window_dedup_group_key_matches_original_eval_for_units_and_strides() {
    let cases = [
        (
            "second_5s",
            "5 second",
            TimeUnit::Second,
            vec![-12_345, -1, 0, 4, 5, 7, 1_700_000_000, 1_700_000_002],
        ),
        (
            "second_7s",
            "7 second",
            TimeUnit::Second,
            vec![-12_345, -1, 0, 6, 7, 13, 1_700_000_000],
        ),
        (
            "millisecond_5s",
            "5 second",
            TimeUnit::Millisecond,
            vec![
                -12_345_000,
                -1,
                0,
                4_999,
                5_000,
                7_000,
                1_700_000_000_000,
                1_700_000_002_999,
            ],
        ),
        (
            "millisecond_7s",
            "7 second",
            TimeUnit::Millisecond,
            vec![-12_345_000, -1, 0, 6_999, 7_000, 1_700_000_000_000],
        ),
        (
            "microsecond_5s",
            "5 second",
            TimeUnit::Microsecond,
            vec![
                -12_345_000_000,
                -1,
                0,
                4_999_999,
                5_000_000,
                1_700_000_000_000_000,
            ],
        ),
        (
            "microsecond_7s",
            "7 second",
            TimeUnit::Microsecond,
            vec![
                -12_345_000_000,
                -1,
                0,
                6_999_999,
                7_000_000,
                1_700_000_000_000_000,
            ],
        ),
        (
            "nanosecond_5s",
            "5 second",
            TimeUnit::Nanosecond,
            vec![
                -12_345_000_000_000,
                -1,
                0,
                4_999_999_999,
                5_000_000_000,
                1_700_000_000_000_000_000,
            ],
        ),
        (
            "nanosecond_7s",
            "7 second",
            TimeUnit::Nanosecond,
            vec![
                -12_345_000_000_000,
                -1,
                0,
                6_999_999_999,
                7_000_000_000,
                1_700_000_000_000_000_000,
            ],
        ),
    ];

    for (name, stride, unit, rows) in cases {
        let query = format!(
            "SELECT max(number) AS output_value, date_bin(INTERVAL '{stride}', ts) AS output_window \
             FROM dedup_unit_table GROUP BY output_window"
        );
        let (task, engine, plan) = new_unit_time_window_test_task(&query, unit, &rows).await;
        let time_window_expr = task.config.time_window_expr.as_ref().unwrap();
        let group_expr = time_window_expr
            .safe_remote_dedup_group_expr()
            .unwrap_or_else(|| {
                panic!("{name}: epoch-anchored whole-second date_bin must be eligible")
            });
        assert_eq!(
            time_window_expr
                .time_window_size()
                .map(|size| size.as_nanos()),
            Some(group_key_stride_ns(&group_expr)),
            "{name}: the recognized stride must equal the locally cached window size"
        );

        let input = recovery_aggregate_input(&plan).unwrap();
        let ctx = task.query_context_snapshot();
        // The grouping key is evaluated by DataFusion itself, then decoded with the
        // same `eval` + set path recovery capture uses.
        let keys = execute_timestamp_plan(
            &engine,
            ctx.clone(),
            dedup_group_key_plan(&input, group_expr),
        )
        .await
        .unwrap();
        assert!(
            keys.iter().all(Option::is_some),
            "{name}: the guarded grouping key must never produce NULL"
        );
        let distinct_keys = keys
            .iter()
            .flatten()
            .map(|key| key.value())
            .collect::<BTreeSet<_>>();
        assert!(
            distinct_keys.len() < rows.len(),
            "{name}: the deduped grouping must collapse rows that share a window"
        );

        let expected = decoded_windows(time_window_expr, unit, rows.iter().copied().map(Some));
        assert_eq!(
            decoded_windows(
                time_window_expr,
                unit,
                keys.iter().map(|key| key.map(|k| k.value()))
            ),
            expected,
            "{name}: remote representatives must decode to exactly the local eval windows"
        );
        assert_eq!(
            expected.len(),
            distinct_keys.len(),
            "{name}: every window keeps its own remote representative"
        );
    }
}

#[tokio::test]
async fn test_remote_window_dedup_group_key_bounds_extreme_timestamps() {
    let max_representable = i64::MAX / 1_000_000;
    let extreme = max_representable + 1;
    let rows = vec![-1, 0, 4_999, 5_000, extreme];
    let query = "SELECT max(number) AS output_value, date_bin(INTERVAL '5 second', ts) AS output_window \
                 FROM dedup_unit_table GROUP BY output_window";
    let (task, engine, plan) =
        new_unit_time_window_test_task(query, TimeUnit::Millisecond, &rows).await;
    let time_window_expr = task.config.time_window_expr.as_ref().unwrap();
    let group_expr = time_window_expr.safe_remote_dedup_group_expr().unwrap();
    let input = recovery_aggregate_input(&plan).unwrap();
    let ctx = task.query_context_snapshot();

    // An unconditional remote `date_bin` overflows to NULL on timestamps this source
    // unit still holds, and the decoder rejects NULL.
    let naive = sql_to_df_plan(
        ctx.clone(),
        engine.clone(),
        "SELECT date_bin(INTERVAL '5 second', ts) AS output_window FROM dedup_unit_table",
        true,
    )
    .await
    .unwrap();
    let naive_values = execute_timestamp_plan(&engine, ctx.clone(), naive)
        .await
        .unwrap();
    assert_eq!(naive_values[0], Some(Timestamp::new_millisecond(-5_000)));
    assert_eq!(naive_values[1], Some(Timestamp::new_millisecond(0)));
    assert_eq!(naive_values[2], Some(Timestamp::new_millisecond(0)));
    assert_eq!(naive_values[3], Some(Timestamp::new_millisecond(5_000)));
    assert_eq!(
        naive_values[4], None,
        "unconditional date_bin must overflow the extreme row to NULL"
    );
    let mut naive_windows = BTreeSet::new();
    let err = capture_recovery_batch_windows(
        &timestamp_batch(
            TimeUnit::Millisecond,
            naive_values
                .iter()
                .map(|value| value.map(|value| value.value())),
        ),
        time_window_expr,
        &mut naive_windows,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("null or non-timestamp"),
        "unconditional remote date_bin must break the decoder: {err}"
    );

    // ... while the guarded grouping key passes unrepresentable rows through, so the
    // decoder keeps working for every row.
    let keys = execute_timestamp_plan(
        &engine,
        ctx.clone(),
        dedup_group_key_plan(&input, group_expr),
    )
    .await
    .unwrap();
    assert_eq!(keys[0], Some(Timestamp::new_millisecond(-1)));
    assert_eq!(keys[1], Some(Timestamp::new_millisecond(0)));
    assert_eq!(keys[2], Some(Timestamp::new_millisecond(0)));
    assert_eq!(keys[3], Some(Timestamp::new_millisecond(5_000)));
    assert_eq!(keys[4], Some(Timestamp::new_millisecond(extreme)));
    assert_eq!(
        decoded_windows(
            time_window_expr,
            TimeUnit::Millisecond,
            keys.iter().map(|key| key.map(|k| k.value()))
        ),
        decoded_windows(
            time_window_expr,
            TimeUnit::Millisecond,
            rows.iter().copied().map(Some)
        )
    );
}

#[tokio::test]
async fn test_remote_window_dedup_sub_second_stride_falls_back() {
    // A 400ms stride is finer than the source unit, so `date_bin` truncates its output
    // and the remote bins no longer match the local `eval` windows.
    let rows = vec![-2, 1, 2, 3, 1_700_000_000];
    let query = "SELECT max(number) AS output_value, date_bin(INTERVAL '400 millisecond', ts) AS output_window \
                 FROM dedup_unit_table GROUP BY output_window";
    let (task, engine, _plan) =
        new_unit_time_window_test_task(query, TimeUnit::Second, &rows).await;
    let time_window_expr = task.config.time_window_expr.as_ref().unwrap();
    assert!(
        time_window_expr.safe_remote_dedup_group_expr().is_none(),
        "sub-second strides must not be recognized"
    );
    let expected = decoded_windows(
        time_window_expr,
        TimeUnit::Second,
        rows.iter().copied().map(Some),
    );

    let (captured, shape) = capture_windows_with_shape(
        &task,
        &engine,
        raw_timestamp_output(TimeUnit::Second, &rows),
    )
    .await;
    assert_eq!(shape, RecoveryTimestampPlanShape::RawTimestampProjection);
    assert_eq!(captured, expected, "fallback must keep every local window");
}

#[tokio::test]
async fn test_remote_window_dedup_calendar_stride_falls_back() {
    // Calendar months are not a fixed-width epoch-anchored lattice, so `date_bin`'s
    // month bins disagree with the local 30-day `eval` lattice used by the decoder:
    // both rows share the `date_bin` month bin 2023-03-01, while the local lattice
    // puts them one full window apart.
    let rows = vec![
        1_677_628_800_000, // 2023-03-01T00:00:00Z
        1_680_220_800_000, // 2023-03-31T00:00:00Z
    ];
    let query = "SELECT max(number) AS output_value, date_bin(INTERVAL '1 month', ts) AS output_window \
                 FROM dedup_unit_table GROUP BY output_window";
    let (task, engine, _plan) =
        new_unit_time_window_test_task(query, TimeUnit::Millisecond, &rows).await;
    let time_window_expr = task.config.time_window_expr.as_ref().unwrap();
    assert!(
        time_window_expr.safe_remote_dedup_group_expr().is_none(),
        "calendar strides must not be recognized"
    );
    let expected = decoded_windows(
        time_window_expr,
        TimeUnit::Millisecond,
        rows.iter().copied().map(Some),
    );

    let naive_plan = sql_to_df_plan(
        task.query_context_snapshot(),
        engine.clone(),
        "SELECT date_bin(INTERVAL '1 month', ts) AS output_window FROM dedup_unit_table GROUP BY output_window",
        true,
    )
    .await
    .unwrap();
    let naive_windows = decoded_windows(
        time_window_expr,
        TimeUnit::Millisecond,
        execute_timestamp_plan(&engine, task.query_context_snapshot(), naive_plan)
            .await
            .unwrap()
            .into_iter()
            .map(|value| value.map(|value| value.value())),
    );
    assert!(
        naive_windows.len() < expected.len(),
        "naive GROUP BY must lose windows: naive={naive_windows:?} local={expected:?}"
    );

    let (captured, shape) = capture_windows_with_shape(
        &task,
        &engine,
        raw_timestamp_output(TimeUnit::Millisecond, &rows),
    )
    .await;
    assert_eq!(shape, RecoveryTimestampPlanShape::RawTimestampProjection);
    assert_eq!(captured, expected, "fallback must keep every local window");
}

#[tokio::test]
async fn test_remote_window_dedup_non_epoch_origin_falls_back() {
    let rows = vec![0, 4_999, 5_000, 1_700_000_000_000];
    let query = "SELECT max(number) AS output_value, \
                 date_bin(INTERVAL '5 second', ts, TIMESTAMP '2023-01-01 00:00:00') AS output_window \
                 FROM dedup_unit_table GROUP BY output_window";
    let (task, engine, _plan) =
        new_unit_time_window_test_task(query, TimeUnit::Millisecond, &rows).await;
    let time_window_expr = task.config.time_window_expr.as_ref().unwrap();
    assert!(
        time_window_expr.safe_remote_dedup_group_expr().is_none(),
        "a non-epoch origin is not the epoch-anchored lattice the remote grouping relies on"
    );

    let expected = decoded_windows(
        time_window_expr,
        TimeUnit::Millisecond,
        rows.iter().copied().map(Some),
    );

    let (captured, shape) = capture_windows_with_shape(
        &task,
        &engine,
        raw_timestamp_output(TimeUnit::Millisecond, &rows),
    )
    .await;
    assert_eq!(shape, RecoveryTimestampPlanShape::RawTimestampProjection);
    assert_eq!(captured, expected, "fallback must keep every local window");
}

/// Executes the recovery capture plan exactly as serialized for the frontend: decode
/// it with the frontend's Substrait decoder, run it locally through the test query
/// engine, and check the decoded windows. This proves the encoded plan is decodable and
/// locally executable with the expected dedup result; it does not run a distributed
/// query and is not proof of frontend or region pushdown.
#[tokio::test]
async fn test_capture_recovery_windows_executes_the_encoded_dedup_plan() {
    let rows = vec![0, 1, 4_999, 5_000, 1_700_000_000_000, 1_700_000_002_999];
    let query = "SELECT max(number) AS output_value, date_bin(INTERVAL '5 second', ts) AS output_window \
                 FROM dedup_unit_table GROUP BY output_window";
    let (task, engine, _plan) =
        new_unit_time_window_test_task(query, TimeUnit::Millisecond, &rows).await;
    let time_window_expr = task.config.time_window_expr.as_ref().unwrap();
    let expected = decoded_windows(
        time_window_expr,
        TimeUnit::Millisecond,
        rows.iter().copied().map(Some),
    );

    let lower = BTreeMap::from([(1, 10)]);
    let handler = Arc::new(RecoveryCaptureHandler {
        output: std::sync::Mutex::new(None),
        lower: serde_json::to_string(&lower).unwrap(),
        expire_after: None,
        source_table: "dedup_unit_table".to_string(),
        expects_number_filter: false,
        audit: std::sync::Mutex::new(None),
        // Decode the encoded plan with the frontend's Substrait decoder and execute
        // the decoded plan locally, mirroring the request the capture path sends.
        execute: Some((engine.clone(), task.query_context_snapshot())),
        executed_rows: std::sync::Mutex::new(None),
    });
    let handler_dyn: Arc<
        dyn crate::batching_mode::frontend_client::GrpcQueryHandlerWithBoxedError,
    > = handler.clone();
    let frontend =
        FrontendClient::from_grpc_handler(Arc::downgrade(&handler_dyn), QueryOptions::default());

    let (_, windows) = task
        .capture_recovery_windows(&engine, &frontend, &lower)
        .await
        .unwrap();

    let audit = handler.audit.lock().unwrap().clone().unwrap();
    assert_eq!(audit.shape, RecoveryTimestampPlanShape::RemoteWindowDedup);
    assert_eq!(
        windows.into_iter().collect::<BTreeSet<_>>(),
        expected,
        "windows decoded from the executed plan must match local eval"
    );
    let executed_rows = handler.executed_rows.lock().unwrap().unwrap();
    assert!(
        executed_rows < rows.len(),
        "the executed plan must return deduplicated representatives, got {executed_rows} rows"
    );
    assert_eq!(executed_rows, expected.len());
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
    assert!(state.dirty_time_windows.is_empty());
    let repair = state.pending_fenced_repair().unwrap();
    assert_eq!(repair.high(), &high);
    assert_eq!(repair.pending_windows().len(), 1);
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
    assert!(state.dirty_time_windows.is_empty());
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
    // the moved pending backlog captured above, not this later live dirty window.
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
    assert!(state.dirty_time_windows.is_empty());
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
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(100), Some(Timestamp::new_second(105)));
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
    let DirtyRestore::Scoped(filter) = &plan.dirty_restore else {
        panic!("scoped base repair should carry scoped dirty restore info");
    };
    assert_eq!(
        filter.time_ranges,
        vec![(Timestamp::new_second(100), Timestamp::new_second(105))],
        "executed pre-H repair item should not be requeued; only remaining pending window is retried"
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
        let error = Err::<(), _>(BoxedError::new(
            common_recordbatch::error::Error::PollStream {
                error: datafusion::error::DataFusionError::Shared(Arc::new(
                    datafusion::error::DataFusionError::External(Box::new(BoxedError::new(
                        MockError::new(StatusCode::RequestOutdated),
                    ))),
                )),
                location: snafu::Location::default(),
            },
        ))
        .context(crate::error::ExternalSnafu)
        .unwrap_err();
        let reason = BatchingTask::query_failure_reason(
            &error,
            &QueryCoverage::FencedRepairChunk { high: high.clone() },
        );
        assert_eq!(reason, FlowQueryFallbackReason::SnapshotFenceExpired);
        let decision = BatchingTask::apply_query_failure_to_state(
            &mut state,
            std::time::Duration::from_millis(1),
            &QueryCoverage::FencedRepairChunk { high },
            reason,
        );
        assert_eq!(
            decision,
            Some(FlowCheckpointDecision::FallbackToFullSnapshot {
                previous_mode: CheckpointMode::FullSnapshot,
                reason: FlowQueryFallbackReason::SnapshotFenceExpired,
            })
        );
        assert!(state.pending_fenced_repair().is_none());
        assert_eq!(state.dirty_time_windows.len(), 1);

        // Simulate the outer execution failure restore for the in-flight chunk.
        state.restore_scoped_windows(&filter);
        assert_eq!(state.dirty_time_windows.len(), 2);
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
        0,
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
async fn test_exact_required_attempt_rejects_revoked_capability_without_extensions() {
    let task = new_test_task_engine_and_plan_with_query_and_opts_and_required(
        "SELECT number, ts FROM numbers_with_ts",
        "exact_required_revoked",
        incremental_batch_opts(),
        true,
    )
    .await
    .into_task_and_plan()
    .0;

    task.state
        .write()
        .unwrap()
        .advance_checkpoints(HashMap::from([(1_u64, 10_u64)]));
    let checkpoints_before = task.state.read().unwrap().checkpoints().clone();

    let err = task
        .build_flow_query_extensions(true, true)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("requires exact sequence-range"));
    assert_eq!(
        task.state.read().unwrap().checkpoints(),
        &checkpoints_before
    );
}

#[tokio::test]
async fn test_sequence_range_producer_emits_capable_source_extensions() {
    let parts = new_test_task_engine_and_plan_with_query_and_opts_and_required(
        "SELECT number, ts FROM numbers_with_ts",
        "numbers_with_ts",
        incremental_batch_opts(),
        true,
    )
    .await;
    configure_source_capability(&parts.query_engine, "mito", true).await;
    let task = parts.task;
    task.state
        .write()
        .unwrap()
        .advance_checkpoints(HashMap::from([(1024_u64, 10_u64), (2048_u64, 20_u64)]));

    let extensions = task.build_flow_query_extensions(true, true).await.unwrap();

    assert_eq!(
        extensions,
        vec![
            ("flow.return_region_seq", "true".to_string()),
            (FLOW_SINK_TABLE_ID, "1".to_string()),
            (FLOW_INCREMENTAL_MODE, "sequence_range".to_string()),
            (
                FLOW_INCREMENTAL_AFTER_SEQS,
                serde_json::json!({"1024": 10, "2048": 20}).to_string(),
            ),
        ]
    );
}

#[tokio::test]
async fn test_sequence_range_producer_keeps_memtable_only_for_non_mito_source() {
    let parts = new_test_task_engine_and_plan_with_query_and_opts(
        "SELECT number, ts FROM numbers_with_ts",
        "numbers_with_ts",
        incremental_batch_opts(),
    )
    .await;
    configure_source_capability(&parts.query_engine, "file", true).await;
    let task = parts.task;
    task.state
        .write()
        .unwrap()
        .advance_checkpoints(HashMap::from([(1024_u64, 10_u64)]));

    let extensions = task.build_flow_query_extensions(true, true).await.unwrap();

    assert_eq!(
        extensions,
        vec![
            ("flow.return_region_seq", "true".to_string()),
            (FLOW_SINK_TABLE_ID, "1".to_string()),
            (
                FLOW_INCREMENTAL_MODE,
                FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY.to_string(),
            ),
            (
                FLOW_INCREMENTAL_AFTER_SEQS,
                serde_json::json!({"1024": 10}).to_string(),
            ),
        ]
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
async fn test_expired_fenced_repair_uses_frozen_scope() {
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
        .unwrap();

    // A fenced repair is a frozen explicit scope: expiry cannot discard its
    // old window or make this query consume the live post-fence signal.
    assert!(matches!(
        plan.coverage,
        QueryCoverage::FencedRepairChunk { .. }
    ));
    let state = task.state.read().unwrap();
    assert!(state.pending_fenced_repair().is_some());
    assert!(state.fenced_repair_pending_is_empty());
    assert_eq!(state.dirty_time_windows.len(), 1);
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
async fn test_force_full_snapshot_retains_expiration_and_reversible_dirty_capture() {
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
    register_twe_sink(&query_engine, "missing_sink", 9201);
    Arc::get_mut(&mut task.config)
        .expect("test task config should be uniquely owned")
        .expire_after = Some(expire_after_for_retention_filter_test());

    let plan = task
        .gen_insert_plan_with_values_unlocked(&query_engine, Some(1), &BTreeMap::new(), true)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(plan.coverage, QueryCoverage::UnfilteredFull));
    assert!(
        plan.plan
            .to_string()
            .contains("Filter: ts >= TimestampMillisecond(")
    );
    assert!(task.state.read().unwrap().dirty_time_windows.is_empty());

    task.restore_dirty_windows(&plan.dirty_restore);
    assert_eq!(task.state.read().unwrap().dirty_time_windows.len(), 1);

    task.state.write().unwrap().dirty_time_windows.clean();
    let no_dirty_plan = task
        .gen_insert_plan_with_values_unlocked(&query_engine, Some(1), &BTreeMap::new(), true)
        .await
        .unwrap()
        .expect("forced full snapshot must bypass the dirty-window notification gate");
    assert!(matches!(
        no_dirty_plan.coverage,
        QueryCoverage::UnfilteredFull
    ));
    assert!(
        no_dirty_plan
            .plan
            .to_string()
            .contains("Filter: ts >= TimestampMillisecond(")
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
async fn test_exact_required_executed_failure_selects_full_snapshot_repair() {
    let TestTaskParts {
        mut task,
        query_engine,
        ..
    } = new_time_window_test_task_with_query(
        "SELECT max(number) AS number, date_bin(INTERVAL '5 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window",
    )
    .await;
    configure_source_capability(&query_engine, "mito", true).await;
    register_twe_sink(&query_engine, "missing_sink", 9201);
    Arc::get_mut(&mut task.config)
        .expect("test task config should be uniquely owned")
        .exact_sequence_range_required = true;
    {
        let mut state = task.state.write().unwrap();
        state.advance_checkpoints(HashMap::from([(1_u64, 10_u64)]));
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(0), Some(Timestamp::new_second(5)));
    }
    let sink_schema = aggregate_time_window_sink_schema();
    let plan_info = task
        .gen_insert_plan_unlocked(&query_engine, Some(1))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        plan_info.coverage,
        QueryCoverage::IncrementalDelta
    ));

    let handler_invoked = Arc::new(AtomicBool::new(false));
    let handler: Arc<dyn crate::batching_mode::frontend_client::GrpcQueryHandlerWithBoxedError> =
        Arc::new(ExactDeltaFailureHandler {
            invoked: handler_invoked.clone(),
        });
    let frontend_client = Arc::new(FrontendClient::from_grpc_handler(
        Arc::downgrade(&handler),
        QueryOptions::default(),
    ));
    let raw = task
        .execute_plan_unlocked(
            &query_engine,
            &frontend_client,
            &plan_info.plan,
            &plan_info.dirty_restore,
            &plan_info.coverage,
        )
        .await
        .expect("raw executor should return the dispatched execution result")
        .expect("raw executor should dispatch the exact delta");
    assert!(
        raw.0.is_err(),
        "the dispatched exact delta must fail through the injected frontend handler"
    );
    assert!(
        handler_invoked.load(Ordering::SeqCst),
        "the injected frontend handler must receive the exact delta"
    );
    // The raw executor is a delegate-safe primitive: it logs failures but does not change
    // checkpoint state. Restore the consumed work, then let the default wrapper make its one
    // failure transition and restore it again.
    {
        let state = task.state.read().unwrap();
        assert_eq!(state.checkpoint_mode(), CheckpointMode::Incremental);
        assert_eq!(state.checkpoints(), &BTreeMap::from([(1_u64, 10_u64)]));
    }
    task.restore_dirty_windows(&plan_info.dirty_restore);
    let outcome = task
        .execute_once_default_unlocked(&query_engine, &frontend_client, Some(1))
        .await;
    assert!(outcome.result.is_err());

    {
        let state = task.state.read().unwrap();
        assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
        assert_eq!(state.checkpoints(), &BTreeMap::from([(1_u64, 10_u64)]));
        assert_eq!(state.dirty_time_windows.len(), 1);
        assert_eq!(
            state.dirty_time_windows.window_size(),
            std::time::Duration::from_secs(5)
        );
    }

    let repair = task
        .gen_query_with_time_window(query_engine, &sink_schema, &[], false, Some(1))
        .await
        .unwrap()
        .expect("failed exact delta must retry through existing base repair");
    assert!(matches!(repair.coverage, QueryCoverage::ScopedBaseRepair));
    assert!(repair.plan.to_string().contains("Filter:"));
    assert!(!repair.plan.to_string().contains("Left Join"));
}

#[tokio::test]
async fn test_exact_required_incomplete_proof_selects_base_recomputation() {
    let TestTaskParts {
        mut task,
        query_engine,
        ..
    } = new_time_window_test_task_with_query(
        "SELECT max(number) AS number, date_bin(INTERVAL '5 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window",
    )
    .await;
    configure_source_capability(&query_engine, "mito", true).await;
    Arc::get_mut(&mut task.config)
        .expect("test task config should be uniquely owned")
        .exact_sequence_range_required = true;
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

    task.state
        .write()
        .unwrap()
        .dirty_time_windows
        .add_window(Timestamp::new_second(5), Some(Timestamp::new_second(10)));
    let followup = task
        .gen_query_with_time_window(query_engine, &sink_schema, &[], false, Some(1))
        .await
        .unwrap()
        .expect("new dirty work must use base recomputation after incomplete delta proof");
    assert!(matches!(followup.coverage, QueryCoverage::ScopedBaseRepair));
    assert!(
        followup.plan.to_string().contains("Filter:"),
        "base recomputation must retain its dirty-window filter"
    );
    assert!(
        !followup.plan.to_string().contains("Left Join"),
        "base recomputation must not replay the additive incremental sink merge"
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
async fn test_exact_required_unsupported_plan_keeps_exact_retry_state() {
    let query_engine = create_test_query_engine();
    configure_source_capability(&query_engine, "mito", true).await;
    let ctx = QueryContext::arc();
    let plan = sql_to_df_plan(
        ctx.clone(),
        query_engine.clone(),
        "SELECT number, ts FROM numbers_with_ts",
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
    let dml_plan = LogicalPlan::Dml(DmlStatement::new(
        datafusion_common::TableReference::bare("test"),
        Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(sink_table),
        ))),
        WriteOp::Insert(datafusion_expr::dml::InsertOp::Append),
        Arc::new(plan),
    ));
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let task = BatchingTask::try_new_with_exact_sequence_range_required(
        TaskArgs {
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
            eval_schedule: None,
        },
        true,
    )
    .unwrap();
    task.state
        .write()
        .unwrap()
        .advance_checkpoints(HashMap::from([(1_u64, 10_u64)]));
    task.state
        .write()
        .unwrap()
        .dirty_time_windows
        .add_window(Timestamp::new_second(10), Some(Timestamp::new_second(15)));

    let (frontend_client, _) = FrontendClient::from_empty_grpc_handler(QueryOptions::default());
    let frontend_client = Arc::new(frontend_client);
    for _ in 0..2 {
        let err = task
            .execute_logical_plan_unlocked(
                &query_engine,
                &frontend_client,
                &dml_plan,
                &DirtyRestore::Unscoped(dirty_range(10, 15)),
                &QueryCoverage::IncrementalDelta,
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("requires exact sequence-range"));

        let state = task.state.read().unwrap();
        assert_eq!(state.checkpoint_mode(), CheckpointMode::Incremental);
        assert_eq!(state.checkpoints(), &BTreeMap::from([(1_u64, 10_u64)]));
        assert_eq!(state.dirty_time_windows.len(), 1);
    }
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
        eval_schedule: None,
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

    let incremental_plan = task
        .prepare_plan_for_incremental(&query_engine, &dml_plan)
        .await
        .unwrap();
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
        eval_schedule: None,
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
            &query_engine,
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
        eval_schedule: None,
    })
    .unwrap();

    task.state
        .write()
        .unwrap()
        .advance_checkpoints(HashMap::from([(1_u64, 10_u64)]));

    let incremental_plan = task
        .prepare_plan_for_incremental(&query_engine, &dml_plan)
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
    let incremental_plan = task
        .prepare_plan_for_incremental(&query_engine, &dml_plan)
        .await
        .unwrap();
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
async fn test_unscoped_execution_invariant_error_preserves_dirty_signal() {
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
        Ok(_) => panic!("execution should reject SQL without TWE or EVAL INTERVAL"),
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
