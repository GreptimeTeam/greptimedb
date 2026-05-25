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

use catalog::RegisterTableRequest;
use catalog::memory::MemoryCatalogManager;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_recordbatch::RecordBatch;
use datatypes::data_type::ConcreteDataType as CDT;
use datatypes::schema::ColumnSchema;
use datatypes::vectors::{UInt32Vector, VectorRef};
use pretty_assertions::assert_eq;
use session::context::QueryContext;
use table::test_util::MemTable;

use super::*;
use crate::batching_mode::time_window::find_time_window_expr;
use crate::test_utils::create_test_query_engine;

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
        batch_opts: Arc::new(BatchingModeOptions::default()),
        flow_eval_interval: None,
    })
    .unwrap();

    TestTaskParts {
        task,
        query_engine,
        plan,
    }
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
        batch_opts: Arc::new(BatchingModeOptions::default()),
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

async fn assert_filterless_failure_restore(
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
    let filterless_query = PlanInfo {
        plan,
        filter: None,
        filterless_dirty_windows_to_restore: Some(consumed_dirty_windows),
    };

    task.handle_executed_query_failure(Some(&filterless_query));

    let state = task.state.read().unwrap();
    assert_eq!(state.dirty_time_windows.len(), expected_len);
    assert_eq!(
        state.dirty_time_windows.window_size(),
        std::time::Duration::from_secs(expected_window_size_secs)
    );
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
        filter: Some(FilterExprInfo {
            expr: datafusion_expr::lit(true),
            col_name: "ts".to_string(),
            time_ranges: vec![(Timestamp::new_second(10), Timestamp::new_second(20))],
            window_size: chrono::Duration::seconds(10),
        }),
        filterless_dirty_windows_to_restore: None,
    };

    task.handle_executed_query_failure(Some(&scoped_query));

    let state = task.state.read().unwrap();
    assert_eq!(state.dirty_time_windows.len(), 1);
}

#[tokio::test]
async fn test_filterless_failure_restores_consumed_dirty_windows() {
    assert_filterless_failure_restore(dirty_marker(), DirtyTimeWindows::default(), 1, 0).await;
    assert_filterless_failure_restore(dirty_range(30, 40), dirty_range(10, 20), 2, 20).await;
    assert_filterless_failure_restore(dirty_range(30, 40), dirty_range(30, 50), 1, 20).await;
}

#[tokio::test]
async fn test_filterless_plan_generation_failure_restores_consumed_dirty_marker() {
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
