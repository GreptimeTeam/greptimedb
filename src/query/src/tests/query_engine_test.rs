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

use arrow::array::{ArrayRef, Int64Array, StringArray, UInt32Array};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use catalog::RegisterTableRequest;
use catalog::memory::MemoryCatalogManager;
use common_base::Plugins;
use common_base::memory_limit::MemoryLimit;
use common_base::readable_size::ReadableSize;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, NUMBERS_TABLE_ID};
use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_query::OutputData;
use common_recordbatch::{RecordBatch, util};
use datafusion::datasource::{DefaultTableSource, MemTable as DfMemTable};
use datafusion::execution::context::SessionConfig;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::logical_plan::builder::LogicalPlanBuilder;
use datafusion_expr::{LogicalPlan, col};
use datatypes::prelude::*;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::UInt32Vector;
use session::context::QueryContext;
use snafu::ResultExt;
use table::table::adapter::DfTableProviderAdapter;
use table::table::numbers::{NUMBERS_TABLE_NAME, NumbersTable};
use table::test_util::MemTable;

use crate::QueryEngineRef;
use crate::error::{QueryExecutionSnafu, Result};
use crate::options::QueryOptions as QueryOptionsNew;
use crate::parser::QueryLanguageParser;
use crate::query_engine::QueryEngineFactory;
use crate::query_engine::options::QueryOptions;
use crate::query_engine::runtime::{
    QueryRuntimeContext, QueryRuntimeProvider, QueryRuntimeProviderRef,
};

#[tokio::test]
async fn test_datafusion_query_engine() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    let catalog_list = catalog::memory::new_memory_catalog_manager()
        .map_err(BoxedError::new)
        .context(QueryExecutionSnafu)?;
    let factory = QueryEngineFactory::new(
        catalog_list,
        None,
        None,
        None,
        None,
        false,
        QueryOptionsNew::default(),
    );
    let engine = factory.query_engine();

    let column_schemas = vec![ColumnSchema::new(
        "number",
        ConcreteDataType::uint32_datatype(),
        false,
    )];
    let schema = Arc::new(Schema::new(column_schemas));
    let columns: Vec<VectorRef> = vec![Arc::new(UInt32Vector::from_slice(
        (0..100).collect::<Vec<_>>(),
    ))];
    let recordbatch = RecordBatch::new(schema, columns).unwrap();
    let table = MemTable::table("numbers", recordbatch);

    let limit = 10;
    let table_provider = Arc::new(DfTableProviderAdapter::new(table.clone()));
    let plan = LogicalPlanBuilder::scan(
        "numbers",
        Arc::new(DefaultTableSource { table_provider }),
        None,
    )
    .unwrap()
    .limit(0, Some(limit))
    .unwrap()
    .build()
    .unwrap();

    let output = engine.execute(plan, QueryContext::arc()).await?;

    let recordbatch = match output.data {
        OutputData::Stream(recordbatch) => recordbatch,
        _ => unreachable!(),
    };

    let numbers = util::collect(recordbatch).await.unwrap();

    assert_eq!(1, numbers.len());
    assert_eq!(numbers[0].num_columns(), 1);
    assert_eq!(1, numbers[0].schema.num_columns());
    assert_eq!("number", numbers[0].schema.column_schemas()[0].name);

    let batch = &numbers[0];
    assert_eq!(1, batch.num_columns());
    assert_eq!(batch.column(0).len(), limit);
    let expected = Arc::new(UInt32Array::from_iter_values(
        (0u32..limit as u32).collect::<Vec<_>>(),
    )) as ArrayRef;
    assert_eq!(batch.column(0), &expected);

    Ok(())
}

const MB: u64 = 1024 * 1024;
const PAYLOAD_LEN: usize = 250;
const ROWS_PER_BATCH: usize = 512;

#[derive(Debug)]
struct DiskRuntimeProvider {
    mode: DiskManagerMode,
}

impl QueryRuntimeProvider for DiskRuntimeProvider {
    fn configure_session_config(&self, _ctx: QueryRuntimeContext<'_>, config: &mut SessionConfig) {
        *config = config
            .clone()
            .with_target_partitions(1)
            .with_batch_size(ROWS_PER_BATCH)
            .with_sort_spill_reservation_bytes(256 * 1024)
            .with_sort_in_place_threshold_bytes(0);
    }

    fn build_runtime_env(
        &self,
        _ctx: QueryRuntimeContext<'_>,
        builder: RuntimeEnvBuilder,
    ) -> datafusion::error::Result<Arc<RuntimeEnv>> {
        builder
            .with_disk_manager_builder(DiskManagerBuilder::default().with_mode(self.mode.clone()))
            .build()
            .map(Arc::new)
    }
}

fn memory_ledger_engine(mode: DiskManagerMode) -> QueryEngineRef {
    let plugins = Plugins::default();
    plugins.insert::<QueryRuntimeProviderRef>(Arc::new(DiskRuntimeProvider { mode }));
    QueryEngineFactory::new_with_plugins(
        catalog::memory::new_memory_catalog_manager().unwrap(),
        None,
        None,
        None,
        None,
        None,
        false,
        plugins,
        QueryOptionsNew {
            parallelism: 1,
            memory_pool_size: MemoryLimit::Size(ReadableSize(4 * MB)),
            experimental_enable_memory_ledger: true,
            ..Default::default()
        },
    )
    .query_engine()
}

fn wide_sort_plan(rows: usize, rows_per_batch: usize) -> LogicalPlan {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("k", ArrowDataType::Int64, false),
        ArrowField::new("payload", ArrowDataType::Utf8, false),
    ]));
    let mut batches = Vec::new();
    let mut next = rows as i64;
    while next > 0 {
        let n = rows_per_batch.min(next as usize) as i64;
        let keys: Vec<i64> = (0..n).map(|i| next - i).collect();
        let payloads: Vec<String> = keys
            .iter()
            .map(|key| format!("{key:0>width$}", width = PAYLOAD_LEN))
            .collect();
        batches.push(
            arrow::record_batch::RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(keys)),
                    Arc::new(StringArray::from(payloads)),
                ],
            )
            .unwrap(),
        );
        next -= n;
    }
    let table = DfMemTable::try_new(schema, vec![batches]).unwrap();
    LogicalPlanBuilder::scan(
        "t",
        Arc::new(DefaultTableSource {
            table_provider: Arc::new(table),
        }),
        None,
    )
    .unwrap()
    .sort(vec![col("k").sort(true, true)])
    .unwrap()
    .build()
    .unwrap()
}

fn spill_metrics(plan: &dyn ExecutionPlan) -> (usize, usize) {
    let (mut count, mut bytes) = (0, 0);
    if let Some(metrics) = plan.metrics() {
        count += metrics.spill_count().unwrap_or(0);
        bytes += metrics.spilled_bytes().unwrap_or(0);
    }
    for child in plan.children() {
        let (child_count, child_bytes) = spill_metrics(child.as_ref());
        count += child_count;
        bytes += child_bytes;
    }
    (count, bytes)
}

async fn execute_sort(
    engine: &QueryEngineRef,
    rows: usize,
    rows_per_batch: usize,
) -> (Vec<RecordBatch>, Arc<dyn ExecutionPlan>) {
    let output = engine
        .execute(wide_sort_plan(rows, rows_per_batch), QueryContext::arc())
        .await
        .unwrap();
    let plan = output.meta.plan.unwrap();
    let OutputData::Stream(stream) = output.data else {
        unreachable!()
    };
    let batches = util::collect(stream).await.unwrap();
    (batches, plan)
}

fn assert_sorted(batches: &[RecordBatch], rows: usize) {
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        rows
    );
    let mut expected = 1;
    for batch in batches {
        let keys = batch
            .df_record_batch()
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for &key in keys.values() {
            assert_eq!(key, expected);
            expected += 1;
        }
    }
}

fn has_files(path: &std::path::Path) -> bool {
    std::fs::read_dir(path).unwrap().any(|entry| {
        let path = entry.unwrap().path();
        path.is_file() || (path.is_dir() && has_files(&path))
    })
}

#[tokio::test]
async fn memory_ledger_query_spills_and_cleans_up() {
    const ROWS: usize = 64_000;

    let dir = tempfile::tempdir().unwrap();
    let engine = memory_ledger_engine(DiskManagerMode::Directories(vec![dir.path().into()]));
    let account = engine.engine_state().memory_ledger_account().unwrap();

    let (batches, plan) = execute_sort(&engine, ROWS, ROWS_PER_BATCH).await;
    assert_sorted(&batches, ROWS);
    let (spill_count, spilled_bytes) = spill_metrics(plan.as_ref());
    assert!(spill_count > 0);
    assert!(spilled_bytes > 0);
    assert_eq!(account.used_bytes(), 0);
    assert_eq!(
        engine
            .engine_state()
            .session_state()
            .runtime_env()
            .memory_pool
            .reserved(),
        0
    );
    assert_eq!(
        engine
            .engine_state()
            .session_state()
            .runtime_env()
            .disk_manager
            .used_disk_space(),
        0
    );
    assert!(!has_files(dir.path()));
}

#[tokio::test]
async fn memory_ledger_query_without_spill_recovers_after_exhaustion() {
    const ROWS: usize = 40_000;

    let engine = memory_ledger_engine(DiskManagerMode::Disabled);
    let account = engine.engine_state().memory_ledger_account().unwrap();
    let output = engine
        .execute(wide_sort_plan(ROWS, ROWS), QueryContext::arc())
        .await
        .unwrap();
    let OutputData::Stream(stream) = output.data else {
        unreachable!()
    };
    let error = util::collect(stream).await.unwrap_err();
    assert_eq!(error.status_code(), StatusCode::RuntimeResourcesExhausted);
    assert_eq!(account.used_bytes(), 0);
    assert_eq!(
        engine
            .engine_state()
            .session_state()
            .runtime_env()
            .memory_pool
            .reserved(),
        0
    );
    assert_eq!(
        engine
            .engine_state()
            .session_state()
            .runtime_env()
            .disk_manager
            .used_disk_space(),
        0
    );

    let (batches, plan) = execute_sort(&engine, 100, 100).await;
    assert_sorted(&batches, 100);
    assert_eq!(spill_metrics(plan.as_ref()), (0, 0));
    assert_eq!(account.used_bytes(), 0);
}

fn catalog_manager() -> Result<Arc<MemoryCatalogManager>> {
    let catalog_manager = catalog::memory::new_memory_catalog_manager().unwrap();
    let req = RegisterTableRequest {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_SCHEMA_NAME.to_string(),
        table_name: NUMBERS_TABLE_NAME.to_string(),
        table_id: NUMBERS_TABLE_ID,
        table: NumbersTable::table(NUMBERS_TABLE_ID),
    };
    let _ = catalog_manager.register_table_sync(req).unwrap();

    Ok(catalog_manager)
}

#[tokio::test]
async fn test_query_validate() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    let catalog_list = catalog_manager()?;

    // set plugins
    let plugins = Plugins::new();
    plugins.insert(QueryOptions {
        disallow_cross_catalog_query: true,
    });

    let factory = QueryEngineFactory::new_with_plugins(
        catalog_list,
        None,
        None,
        None,
        None,
        None,
        false,
        plugins,
        QueryOptionsNew::default(),
    );
    let engine = factory.query_engine();

    let stmt =
        QueryLanguageParser::parse_sql("select number from public.numbers", &QueryContext::arc())
            .unwrap();
    assert!(
        engine
            .planner()
            .plan(&stmt, QueryContext::arc())
            .await
            .is_ok()
    );

    let stmt = QueryLanguageParser::parse_sql(
        "select number from wrongschema.numbers",
        &QueryContext::arc(),
    )
    .unwrap();
    assert!(
        engine
            .planner()
            .plan(&stmt, QueryContext::arc())
            .await
            .is_err()
    );
    Ok(())
}
