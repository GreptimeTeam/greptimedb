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

use std::time::{Duration, UNIX_EPOCH};

use catalog::RegisterTableRequest;
use catalog::memory::{MemoryCatalogManager, new_memory_catalog_manager};
use catalog::table_source::DfTableSourceProvider;
use common_base::Plugins;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_query::test_util::DummyDecoder;
use common_recordbatch::RecordBatch as GreptimeRecordBatch;
use common_time::Timezone;
use datafusion::arrow::array::{Float64Array, StringArray, TimestampMillisecondArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::{col, lit};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::expr_fn::SimpleScalarUDF;
use datafusion_expr::{ColumnarValue, LogicalPlanBuilder};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema};
use promql_parser::parser;
use promql_parser::parser::EvalStmt;
use session::context::{QueryContext, QueryContextBuilder};
use table::Table;
use table::metadata::{FilterPushDownType, TableInfoBuilder, TableMetaBuilder};
use table::table::adapter::DfTableProviderAdapter;
use table::test_util::MemTable as GreptimeMemTable;

use super::*;
use crate::QueryEngineContext;
use crate::optimizer::test_util::mock_table_provider;
use crate::options::QueryOptions;
use crate::promql::planner::PromPlanner;
use crate::query_engine::QueryEngineState;

/// Two series sampled every 30s, spanning two hours before and after the
/// evaluation start used by [`eval_stmt`]. `scale` distinguishes the data a
/// template was built from; `version` bumps the table metadata.
///
/// `other_metric` exists only so a two-source query can be planned.
async fn provider(scale: f64, version: u64) -> DfTableSourceProvider {
    let catalog = MemoryCatalogManager::with_default_setup();
    for (id, name) in [(1024, "metric"), (1025, "other_metric")] {
        register(&catalog, id, name, scale, version);
    }
    DfTableSourceProvider::new(
        catalog,
        false,
        QueryContext::arc(),
        DummyDecoder::arc(),
        false,
    )
}

fn register(
    catalog: &Arc<MemoryCatalogManager>,
    table_id: u32,
    name: &str,
    scale: f64,
    version: u64,
) {
    let schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("tag_0", ConcreteDataType::string_datatype(), false),
        ColumnSchema::new(
            "timestamp",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_time_index(true),
        ColumnSchema::new("field_0", ConcreteDataType::float64_datatype(), true),
    ]));
    let mut tags = Vec::new();
    let mut timestamps = Vec::new();
    let mut values = Vec::new();
    for host in ["a", "b"] {
        for i in 0_i64..480 {
            tags.push(host);
            timestamps.push(EVAL_START_MS - 7_200_000 + i * 30_000);
            values.push((i % 97) as f64 * if host == "a" { scale } else { 2.0 * scale });
        }
    }
    let batch = RecordBatch::try_new(
        schema.arrow_schema().clone(),
        vec![
            Arc::new(StringArray::from(tags)),
            Arc::new(TimestampMillisecondArray::from(timestamps)),
            Arc::new(Float64Array::from(values)),
        ],
    )
    .unwrap();
    let backing = GreptimeMemTable::new_with_catalog(
        name,
        GreptimeRecordBatch::from_df_record_batch(schema.clone(), batch),
        table_id,
        DEFAULT_CATALOG_NAME.to_string(),
        DEFAULT_SCHEMA_NAME.to_string(),
    );
    let info = Arc::new(
        TableInfoBuilder::default()
            .table_id(table_id)
            .table_version(version)
            .name(name)
            .meta(
                TableMetaBuilder::empty()
                    .schema(schema)
                    .primary_key_indices(vec![0])
                    .value_indices(vec![2])
                    .next_column_id(3)
                    .created_on(Default::default())
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap(),
    );
    catalog
        .register_table_sync(RegisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: name.to_string(),
            table_id,
            table: Arc::new(Table::new(
                info,
                FilterPushDownType::Unsupported,
                backing.data_source(),
            )),
        })
        .unwrap();
}

const EVAL_START_MS: i64 = 1_743_076_800_000;

/// Queries covering the supported shapes: range selector, range selector with
/// an offset (which adds a normalizer), bare instant selector, instant selector
/// with an offset, and the admitted aggregations over both.
const QUERIES: [&str; 6] = [
    "rate(metric[1h])",
    "sum(rate(metric[1h] offset 5m))",
    "avg by (tag_0) (rate(metric[1h]))",
    "metric",
    "metric offset 5m",
    "sum by (tag_0) (metric)",
];

fn eval_stmt(query: &str, shift_ms: i64) -> EvalStmt {
    let start = UNIX_EPOCH + Duration::from_millis((EVAL_START_MS + shift_ms) as u64);
    EvalStmt {
        expr: parser::parse(query).unwrap(),
        start,
        end: start + Duration::from_secs(3_600),
        interval: Duration::from_secs(300),
        lookback_delta: Duration::from_secs(300),
    }
}

fn engine_state(distributed: bool) -> QueryEngineState {
    QueryEngineState::new(
        new_memory_catalog_manager().unwrap(),
        None,
        None,
        None,
        None,
        None,
        distributed,
        Plugins::default(),
        QueryOptions::default(),
    )
}

/// Collects the plan's output, ordered deterministically so the comparison does
/// not depend on aggregation output order or on batch boundaries.
async fn execute(plan: &LogicalPlan, state: &QueryEngineState) -> RecordBatch {
    let session = state.session_state();
    let physical = session
        .query_planner()
        .create_physical_plan(plan, &session)
        .await
        .unwrap();
    let schema = physical.schema();
    let batches = datafusion::physical_plan::collect(physical, session.task_ctx())
        .await
        .unwrap();
    let batch = datafusion::arrow::compute::concat_batches(&schema, &batches).unwrap();
    assert!(batch.num_rows() > 0, "fixture produced no rows");
    let columns = batch
        .columns()
        .iter()
        .map(|column| datafusion::arrow::compute::SortColumn {
            values: column.clone(),
            options: None,
        })
        .collect::<Vec<_>>();
    let indices = datafusion::arrow::compute::lexsort_to_indices(&columns, None).unwrap();
    RecordBatch::try_new(
        schema,
        batch
            .columns()
            .iter()
            .map(|column| datafusion::arrow::compute::take(column, &indices, None).unwrap())
            .collect(),
    )
    .unwrap()
}

fn scan_sources(plan: &LogicalPlan) -> Vec<Arc<dyn TableSource>> {
    let mut sources = Vec::new();
    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            sources.push(scan.source.clone());
        }
        if let LogicalPlan::Extension(ext) = node
            && let Some(remote) = ext.node.as_any().downcast_ref::<MergeScanLogicalPlan>()
        {
            sources.extend(scan_sources(remote.input()));
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    sources
}

/// Fills the cache for `query` and returns the template's evaluation start.
async fn warm(
    cache: &PromqlPlanCache,
    state: &QueryEngineState,
    query: &str,
) -> datafusion_common::Result<()> {
    let session = state.session_state();
    let raw = PromPlanner::stmt_to_plan(provider(1.0, 0).await, &eval_stmt(query, 0), state)
        .await
        .unwrap();
    let candidate = cache
        .candidate(&raw, &session, &QueryContext::arc())
        .unwrap_or_else(|| panic!("rejected raw plan for {query}: {}", raw.display_indent()));
    assert!(cache.get(&candidate).await?.is_none());
    let optimized = session.optimize(&raw).unwrap();
    assert!(
        cache.insert(candidate, &optimized).await,
        "rejected optimized plan for {query}: {}",
        optimized.display_indent()
    );
    Ok(())
}

#[tokio::test]
async fn reused_template_matches_a_freshly_planned_plan() {
    for distributed in [false, true] {
        let state = engine_state(distributed);
        let session = state.session_state();
        for query in QUERIES {
            let cache = PromqlPlanCache::new(8);
            warm(&cache, &state, query).await.unwrap();

            for shift in [0_i64, 1, 30_000, 330_000, -600_000, 1_890_000] {
                let stmt = eval_stmt(query, shift);
                let raw = PromPlanner::stmt_to_plan(provider(2.0, 0).await, &stmt, &state)
                    .await
                    .unwrap();
                let candidate = cache
                    .candidate(&raw, &session, &QueryContext::arc())
                    .unwrap();
                let cached = cache
                    .get(&candidate)
                    .await
                    .unwrap()
                    .unwrap_or_else(|| panic!("miss for {query} at shift {shift}"));
                let fresh = session.optimize(&raw).unwrap();

                assert_eq!(cached.schema(), fresh.schema());
                assert_eq!(
                    cached.display_indent_schema().to_string(),
                    fresh.display_indent_schema().to_string(),
                    "distributed={distributed}, query={query}, shift={shift}"
                );
                // The reused plan must read through this request's own table
                // source, never through the one the template was built from.
                let cached_sources = scan_sources(&cached);
                let request_sources = scan_sources(&raw);
                assert_eq!(cached_sources.len(), 1);
                assert_eq!(request_sources.len(), 1);
                assert!(Arc::ptr_eq(&cached_sources[0], &request_sources[0]));

                if !distributed {
                    assert_eq!(
                        execute(&cached, &state).await,
                        execute(&fresh, &state).await,
                        "query={query}, shift={shift}"
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn a_hit_reads_current_data_not_the_data_the_template_was_built_from() {
    let state = engine_state(false);
    let session = state.session_state();
    for query in QUERIES {
        let cache = PromqlPlanCache::new(8);
        warm(&cache, &state, query).await.unwrap();

        let stale = PromPlanner::stmt_to_plan(provider(1.0, 0).await, &eval_stmt(query, 0), &state)
            .await
            .unwrap();
        let fresh = PromPlanner::stmt_to_plan(provider(2.0, 0).await, &eval_stmt(query, 0), &state)
            .await
            .unwrap();
        let stale_candidate = cache
            .candidate(&stale, &session, &QueryContext::arc())
            .unwrap();
        let fresh_candidate = cache
            .candidate(&fresh, &session, &QueryContext::arc())
            .unwrap();
        let stale = cache.get(&stale_candidate).await.unwrap().unwrap();
        let fresh = cache.get(&fresh_candidate).await.unwrap().unwrap();

        let stale = execute(&stale, &state).await;
        let scaled = execute(&fresh, &state).await;
        assert_ne!(stale, scaled, "{query}");
        assert_eq!(
            scaled,
            execute(
                &session
                    .optimize(
                        &PromPlanner::stmt_to_plan(
                            provider(2.0, 0).await,
                            &eval_stmt(query, 0),
                            &state
                        )
                        .await
                        .unwrap()
                    )
                    .unwrap(),
                &state
            )
            .await
        );
    }
}

#[tokio::test]
async fn key_separates_metadata_session_and_query_context() {
    let state = engine_state(false);
    let session = state.session_state();
    let cache = PromqlPlanCache::new(8);
    let query = "sum by (tag_0) (rate(metric[1h]))";
    warm(&cache, &state, query).await.unwrap();

    let plan_with = async |scale: f64, version: u64, shift: i64| {
        PromPlanner::stmt_to_plan(
            provider(scale, version).await,
            &eval_stmt(query, shift),
            &state,
        )
        .await
        .unwrap()
    };
    let base = plan_with(1.0, 0, 0).await;
    let miss = async |plan: &LogicalPlan, session: &SessionState, ctx: &QueryContext| {
        let candidate = cache.candidate(plan, session, ctx).unwrap();
        cache.get(&candidate).await.unwrap().is_none()
    };

    // A bumped table version means altered metadata: the template must not be reused.
    assert!(miss(&plan_with(1.0, 1, 0).await, &session, &QueryContext::arc()).await);
    // Grid changes are part of the plan itself, not of the rebound range.
    let mut wider = eval_stmt(query, 0);
    wider.end += Duration::from_secs(3_600);
    let wider = PromPlanner::stmt_to_plan(provider(1.0, 0).await, &wider, &state)
        .await
        .unwrap();
    assert!(miss(&wider, &session, &QueryContext::arc()).await);
    let mut stepped = eval_stmt(query, 0);
    stepped.interval = Duration::from_secs(60);
    let stepped = PromPlanner::stmt_to_plan(provider(1.0, 0).await, &stepped, &state)
        .await
        .unwrap();
    assert!(miss(&stepped, &session, &QueryContext::arc()).await);

    assert!(
        miss(
            &base,
            &session,
            &QueryContext::with("another_catalog", "public")
        )
        .await
    );
    assert!(
        miss(
            &base,
            &session,
            &QueryContextBuilder::default()
                .timezone(Timezone::from_tz_string("+08:00").unwrap())
                .build()
        )
        .await
    );
    assert!(
        miss(
            &base,
            &session,
            &QueryContextBuilder::default()
                .set_extension("greptime_test_hint".to_string(), "1".to_string())
                .build()
        )
        .await
    );

    let mut changed_session = state.session_state();
    changed_session
        .config_mut()
        .options_mut()
        .execution
        .target_partitions += 1;
    assert!(miss(&base, &changed_session, &QueryContext::arc()).await);

    // The per-request remote query id must not separate entries, otherwise
    // every region-server request would miss.
    assert!(!miss(&base, &session, &QueryContext::arc()).await);
    assert!(!miss(&base, &session, &QueryContext::arc()).await);
}

#[tokio::test]
async fn capacity_bounds_the_number_of_retained_templates() {
    let state = engine_state(false);
    let cache = PromqlPlanCache::new(2);
    let before = crate::metrics::PROMQL_PLAN_CACHE_ENTRIES.get();
    for query in QUERIES {
        warm(&cache, &state, query).await.unwrap();
    }
    let retained = cache.entry_count().await;
    assert!(retained <= 2);
    // Six inserts into a two-entry cache: without the eviction listener the
    // gauge would report every template ever stored.
    assert_eq!(
        crate::metrics::PROMQL_PLAN_CACHE_ENTRIES.get() - before,
        retained as i64
    );
}

#[tokio::test]
async fn concurrent_requests_receive_independently_bound_plans() {
    let state = Arc::new(engine_state(false));
    let cache = Arc::new(PromqlPlanCache::new(8));
    let query = "sum by (tag_0) (rate(metric[1h]))";
    warm(&cache, &state, query).await.unwrap();

    let mut handles = Vec::new();
    for shift in [
        0_i64, 30_000, 60_000, 90_000, 120_000, 150_000, 180_000, 210_000,
    ] {
        let cache = cache.clone();
        let state = state.clone();
        handles.push(tokio::spawn(async move {
            let session = state.session_state();
            let raw =
                PromPlanner::stmt_to_plan(provider(1.0, 0).await, &eval_stmt(query, shift), &state)
                    .await
                    .unwrap();
            let candidate = cache
                .candidate(&raw, &session, &QueryContext::arc())
                .unwrap();
            let cached = cache.get(&candidate).await.unwrap().unwrap();
            let fresh = session.optimize(&raw).unwrap();
            assert_eq!(
                cached.display_indent_schema().to_string(),
                fresh.display_indent_schema().to_string(),
                "shift={shift}"
            );
            execute(&cached, &state).await == execute(&fresh, &state).await
        }));
    }
    for handle in handles {
        assert!(handle.await.unwrap());
    }
}

#[tokio::test]
async fn unsupported_shapes_keep_the_uncached_path() {
    let state = engine_state(false);
    let session = state.session_state();
    let cache = PromqlPlanCache::new(8);
    for query in [
        // Two data sources.
        "metric / other_metric",
        // Neither `topk` nor `count` is an admitted aggregation.
        "topk(1, sum by (tag_0) (rate(metric[1h])))",
        "count by (tag_0) (metric)",
        // No range or instant evaluation node at all.
        "1 + 1",
    ] {
        let plan = PromPlanner::stmt_to_plan(provider(1.0, 0).await, &eval_stmt(query, 0), &state)
            .await
            .unwrap();
        assert!(
            cache
                .candidate(&plan, &session, &QueryContext::arc())
                .is_none(),
            "unexpectedly admitted {query}: {}",
            plan.display_indent()
        );
    }
}

#[test]
fn range_function_admission_requires_the_shared_implementation() {
    for real in [
        Rate::scalar_udf(),
        Increase::scalar_udf(),
        Delta::scalar_udf(),
    ] {
        let args = vec![
            col("time_range"),
            col("value"),
            col("time"),
            lit(3_600_000_i64),
        ];
        // Same name, deliberately different signature and implementation.
        let imposter = ScalarUDF::from(SimpleScalarUDF::new(
            real.name(),
            vec![DataType::Float64],
            DataType::Float64,
            datafusion_expr::Volatility::Volatile,
            Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(0.0))))),
        ));
        let admitted = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(real),
            args: args.clone(),
        });
        let rejected = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(imposter),
            args,
        });
        assert!(
            Shape::default()
                .expression(&admitted, false, false)
                .is_some()
        );
        assert!(
            Shape::default()
                .expression(&rejected, false, false)
                .is_none()
        );
    }
}

#[test]
fn only_time_index_predicates_carry_rebindable_timestamps() {
    let mut shape = Shape {
        time_column: Some("time".into()),
        ..Default::default()
    };
    let timestamp = Expr::Literal(ScalarValue::TimestampMillisecond(Some(1000), None), None);
    assert!(
        shape
            .expression(&col("time").gt_eq(timestamp.clone()), true, false)
            .is_some()
    );
    // A timestamp compared against another column, or outside a filter, would
    // be shifted without belonging to the evaluation range.
    assert!(
        shape
            .expression(&col("other").gt_eq(timestamp.clone()), true, false)
            .is_none()
    );
    assert!(
        shape
            .expression(&col("time").gt_eq(timestamp.clone()), false, false)
            .is_none()
    );
    assert!(shape.expression(&timestamp, false, false).is_none());
    assert!(shift(i64::MAX, 1).is_err());
    assert!(shift(i64::MIN, -1).is_err());
}

#[test]
fn tag_literals_survive_rebinding_unchanged() {
    let tag = ScalarValue::Dictionary(
        Box::new(DataType::UInt32),
        // A tag value that looks like a timestamp must stay a tag value.
        Box::new(ScalarValue::Utf8(Some("1743159600000".into()))),
    );
    assert!(
        Shape::default()
            .expression(
                &col("region").eq(Expr::Literal(tag.clone(), None)),
                true,
                false
            )
            .is_some()
    );
    let plan = LogicalPlanBuilder::empty(true)
        .project(vec![Expr::Literal(tag.clone(), None).alias("region")])
        .unwrap()
        .build()
        .unwrap();
    let source = provider_as_source(Arc::new(EmptyTable::new(Arc::new(
        datafusion::arrow::datatypes::Schema::empty(),
    ))));
    assert_eq!(plan, rebind(plan.clone(), 30_000, &source, false).unwrap());

    let dictionary_timestamp = Expr::Literal(
        ScalarValue::Dictionary(
            Box::new(DataType::UInt32),
            Box::new(ScalarValue::TimestampMillisecond(Some(1000), None)),
        ),
        None,
    );
    assert!(
        Shape::default()
            .expression(&dictionary_timestamp, true, true)
            .is_none()
    );
}

/// Guards the assumption behind `Dependency`: `TableScan` equality ignores its
/// source, so metadata changes are invisible to plan comparison alone.
#[tokio::test]
async fn plan_equality_ignores_the_table_source() {
    let older = provider(1.0, 0)
        .await
        .resolve_table(datafusion_common::TableReference::bare("metric"))
        .await
        .unwrap();
    let newer = provider(1.0, 1)
        .await
        .resolve_table(datafusion_common::TableReference::bare("metric"))
        .await
        .unwrap();
    let scan = |source: Arc<dyn TableSource>| {
        LogicalPlanBuilder::scan("metric", source, None)
            .unwrap()
            .build()
            .unwrap()
    };
    assert_eq!(scan(older.clone()), scan(newer.clone()));
    assert!(Dependency::from_source(&older).unwrap() != Dependency::from_source(&newer).unwrap());
    let table_info = |source: &Arc<dyn TableSource>| {
        source
            .as_any()
            .downcast_ref::<DefaultTableSource>()
            .unwrap()
            .table_provider
            .as_any()
            .downcast_ref::<DfTableProviderAdapter>()
            .unwrap()
            .table()
            .table_info()
    };
    assert_ne!(table_info(&older), table_info(&newer));
}

/// A template must never keep the request's own range-function instance: those
/// carry a per-request annotation collector in the native-histogram variants.
#[tokio::test]
async fn rebinding_replaces_range_functions_with_the_shared_instance() {
    let state = engine_state(false);
    let plan = PromPlanner::stmt_to_plan(
        provider(1.0, 0).await,
        &eval_stmt("rate(metric[1h])", 0),
        &state,
    )
    .await
    .unwrap();
    let source = provider_as_source(Arc::new(EmptyTable::new(Arc::new(
        datafusion::arrow::datatypes::Schema::empty(),
    ))));
    let mut request = Vec::new();
    collect_range_functions(&plan, &mut request);
    assert_eq!(request.len(), 1);
    assert!(
        !RANGE_FUNCTIONS
            .iter()
            .any(|shared| Arc::ptr_eq(shared, &request[0]))
    );

    let mut rebound = Vec::new();
    collect_range_functions(&rebind(plan, 0, &source, false).unwrap(), &mut rebound);
    assert_eq!(rebound.len(), 1);
    assert!(
        RANGE_FUNCTIONS
            .iter()
            .any(|shared| Arc::ptr_eq(shared, &rebound[0]))
    );
}

fn collect_range_functions(plan: &LogicalPlan, found: &mut Vec<Arc<ScalarUDF>>) {
    plan.apply(|node| {
        for expr in node.expressions() {
            expr.apply(|expr| {
                if let Expr::ScalarFunction(function) = expr
                    && canonical_range_function(&function.func).is_some()
                {
                    found.push(function.func.clone());
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .unwrap();
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
}

/// End-to-end through the query engine: enabling the option must not change
/// results, and a repeated request must reuse the template.
#[tokio::test]
async fn the_query_engine_reuses_templates_when_enabled() {
    let query = "sum by (tag_0) (rate(metric[1h]))";
    let outcome = |name: &str| {
        crate::metrics::PROMQL_PLAN_CACHE
            .with_label_values(&[name])
            .get()
    };

    let mut cached = Vec::new();
    let mut uncached = Vec::new();
    for size in [4, 0] {
        let state = Arc::new(QueryEngineState::new(
            new_memory_catalog_manager().unwrap(),
            None,
            None,
            None,
            None,
            None,
            false,
            Plugins::default(),
            QueryOptions {
                experimental_promql_plan_cache_size: size,
                ..Default::default()
            },
        ));
        let engine =
            crate::datafusion::DatafusionQueryEngine::new(state.clone(), Plugins::default());
        for shift in [0_i64, 0, 300_000] {
            let plan =
                PromPlanner::stmt_to_plan(provider(1.0, 0).await, &eval_stmt(query, shift), &state)
                    .await
                    .unwrap();
            let mut ctx = QueryEngineContext::new(state.session_state(), QueryContext::arc());
            let physical = engine.create_physical_plan(&mut ctx, &plan).await.unwrap();
            let schema = physical.schema();
            let batches = datafusion::physical_plan::collect(physical, ctx.state().task_ctx())
                .await
                .unwrap();
            let batch = datafusion::arrow::compute::concat_batches(&schema, &batches).unwrap();
            assert!(batch.num_rows() > 0);
            if size > 0 {
                cached.push(batch)
            } else {
                uncached.push(batch)
            }
        }
    }
    assert_eq!(cached, uncached);
    // The three cached requests are one miss (and insert) plus two hits; the
    // three uncached ones touch nothing.
    assert_eq!(outcome("miss"), 1);
    assert_eq!(outcome("insert"), 1);
    assert_eq!(outcome("hit"), 2);
    assert_eq!(outcome("uncacheable"), 0);
}

/// Admission alone must not publish anything, so a request that fails or is
/// cancelled before its plan is optimized leaves no half-built template.
#[tokio::test]
async fn an_abandoned_request_leaves_nothing_behind() {
    let state = engine_state(false);
    let cache = PromqlPlanCache::new(4);
    let plan = PromPlanner::stmt_to_plan(
        provider(1.0, 0).await,
        &eval_stmt("sum by (tag_0) (rate(metric[1h]))", 0),
        &state,
    )
    .await
    .unwrap();
    let candidate = cache
        .candidate(&plan, &state.session_state(), &QueryContext::arc())
        .unwrap();
    assert!(cache.get(&candidate).await.unwrap().is_none());
    drop(candidate);
    assert_eq!(cache.entry_count().await, 0);
}

/// Region-server plans read through a [`DummyTableProvider`], and the scan
/// hints the optimizer derives are written to that provider rather than into
/// the plan. A reused template carries no provider, so the hints must be
/// recomputed against the request's own one.
#[tokio::test]
async fn a_region_plan_hit_rebuilds_the_scan_hints() {
    let region_id = store_api::storage::RegionId::new(1, 1);
    let state = engine_state(false);
    let session = state.session_state();
    let cache = PromqlPlanCache::new(4);

    let warm_provider = Arc::new(mock_table_provider(region_id));
    let warm_plan = region_instant_plan(warm_provider, EVAL_START_MS, EVAL_START_MS);
    let candidate = cache
        .candidate(&warm_plan, &session, &QueryContext::arc())
        .unwrap_or_else(|| panic!("rejected region plan: {}", warm_plan.display_indent()));
    assert!(cache.get(&candidate).await.unwrap().is_none());
    let warm_optimized = session.optimize(&warm_plan).unwrap();
    assert!(cache.insert(candidate, &warm_optimized).await);

    let provider = Arc::new(mock_table_provider(region_id));
    let start = EVAL_START_MS + 30_000;
    let plan = region_instant_plan(provider.clone(), start, start);
    let candidate = cache
        .candidate(&plan, &session, &QueryContext::arc())
        .unwrap();
    let cached = cache.get(&candidate).await.unwrap().unwrap();
    let expected = session.optimize(&plan).unwrap();

    assert_eq!(
        cached.display_indent_schema().to_string(),
        expected.display_indent_schema().to_string()
    );
    let hinted = scan_requests(&cached);
    assert_eq!(hinted, scan_requests(&expected));
    // A hint that is actually derived, not an empty request compared to itself.
    assert_eq!(
        hinted[0].series_row_selector,
        Some(store_api::storage::TimeSeriesRowSelector::LastRow { after_merge: true })
    );
    // Hints belong to the forked provider used by the plan, never to the
    // provider the request handed in.
    assert_eq!(provider.scan_request().series_row_selector, None);
}

/// The shape a range or instant selector takes after being pushed down to one
/// region: a single scan below a sort, a series divide and an evaluation node.
fn region_instant_plan(provider: Arc<DummyTableProvider>, start: i64, end: i64) -> LogicalPlan {
    let timestamp =
        |value: i64| Expr::Literal(ScalarValue::TimestampMillisecond(Some(value), None), None);
    let scan = LogicalPlanBuilder::scan("t", provider_as_source(provider), None)
        .unwrap()
        .filter(
            col("ts")
                .gt_eq(timestamp(start - 300_000))
                .and(col("ts").lt_eq(timestamp(end))),
        )
        .unwrap()
        .sort(vec![
            col("k0").sort(true, false),
            col("ts").sort(true, false),
        ])
        .unwrap()
        .build()
        .unwrap();
    let divide = LogicalPlan::Extension(Extension {
        node: Arc::new(SeriesDivide::new(
            vec!["k0".to_string()],
            "ts".to_string(),
            scan,
        )),
    });
    LogicalPlan::Extension(Extension {
        node: Arc::new(InstantManipulate::new(
            start,
            end,
            300_000,
            300_000,
            "ts".to_string(),
            vec!["k0".to_string()],
            Some("v0".to_string()),
            divide,
        )),
    })
}

fn scan_requests(plan: &LogicalPlan) -> Vec<store_api::storage::ScanRequest> {
    let mut requests = Vec::new();
    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && let Some(source) = scan.source.as_any().downcast_ref::<DefaultTableSource>()
            && let Some(provider) = source
                .table_provider
                .as_any()
                .downcast_ref::<DummyTableProvider>()
        {
            requests.push(provider.scan_request());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    requests
}
