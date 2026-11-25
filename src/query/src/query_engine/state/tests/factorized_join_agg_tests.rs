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

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch as ArrowRecordBatch;
use catalog::RegisterTableRequest;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_recordbatch::RecordBatch;
use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::TableSource;
use datafusion_common::tree_node::TreeNode;
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::Expr;
use datafusion_expr::LogicalPlan;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::logical_plan::Aggregate;
use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion_physical_expr::create_physical_expr;
use datatypes::prelude::*;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::{Float64Vector, StringVector, TimestampMillisecondVector};
use session::context::QueryContext;
use table::TableRef;
use table::metadata::{FilterPushDownType, TableInfoBuilder, TableMetaBuilder, TableType};
use table::table::adapter::DfTableProviderAdapter;
use table::table::{PartitionRules, Table};
use table::test_util::MemTable;

use crate::dist_plan::MergeScanLogicalPlan;
use crate::options::QueryOptions;
use crate::parser::QueryLanguageParser;
use crate::query_engine::QueryEngineFactory;

fn engine(enabled: bool, with_dist_planner: bool) -> crate::query_engine::QueryEngineRef {
    let catalog = catalog::memory::new_memory_catalog_manager().unwrap();
    let fact = RecordBatch::new(
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("value", ConcreteDataType::float64_datatype(), true),
        ])),
        vec![
            Arc::new(TimestampMillisecondVector::from_slice(vec![0])) as _,
            Arc::new(StringVector::from(vec![Some("a")])) as _,
            Arc::new(Float64Vector::from(vec![Some(2.0)])) as _,
        ],
    )
    .unwrap();
    let dim = RecordBatch::new(
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("area", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("weight", ConcreteDataType::float64_datatype(), true),
        ])),
        vec![
            Arc::new(TimestampMillisecondVector::from_slice(vec![0])) as _,
            Arc::new(StringVector::from(vec![Some("a")])) as _,
            Arc::new(StringVector::from(vec![Some("west")])) as _,
            Arc::new(Float64Vector::from(vec![Some(3.0)])) as _,
        ],
    )
    .unwrap();
    for (name, table_id, batch) in [("fact", 101, fact), ("dim", 102, dim)] {
        let table = MemTable::new_with_catalog(
            name,
            batch,
            table_id,
            DEFAULT_CATALOG_NAME.to_string(),
            DEFAULT_SCHEMA_NAME.to_string(),
        );
        catalog
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: name.to_string(),
                table_id,
                table,
            })
            .unwrap();
    }
    QueryEngineFactory::new(
        catalog,
        None,
        None,
        None,
        None,
        with_dist_planner,
        QueryOptions {
            experimental_enable_factorized_join_agg: enabled,
            ..Default::default()
        },
    )
    .query_engine()
}

async fn optimized_logical_for(
    engine: &crate::query_engine::QueryEngineRef,
    sql: &str,
) -> datafusion_expr::LogicalPlan {
    let context = QueryContext::arc();
    let statement = QueryLanguageParser::parse_sql(sql, &context).unwrap();
    let plan = engine.planner().plan(&statement, context).await.unwrap();
    engine.engine_state().optimize_logical_plan(plan).unwrap()
}

async fn optimized_plan_for(engine: &crate::query_engine::QueryEngineRef, sql: &str) -> String {
    optimized_logical_for(engine, sql)
        .await
        .display_indent()
        .to_string()
}

/// The plan every region runs, for each merge scan of `plan` in visit order.
fn remote_inputs(plan: &datafusion_expr::LogicalPlan) -> Vec<&datafusion_expr::LogicalPlan> {
    let mut remotes = Vec::new();
    let _ = plan.apply(|node| {
        if let datafusion_expr::LogicalPlan::Extension(extension) = node
            && let Some(scan) = extension
                .node
                .as_any()
                .downcast_ref::<crate::dist_plan::MergeScanLogicalPlan>()
        {
            remotes.push(scan.input());
        }
        Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
    });
    remotes
}

/// The aggregates still computed on the frontend directly above a merge scan.
fn aggregates_above_merge_scan(
    plan: &datafusion_expr::LogicalPlan,
) -> Vec<&datafusion_expr::logical_plan::Aggregate> {
    let mut aggregates = Vec::new();
    let _ = plan.apply(|node| {
        if let datafusion_expr::LogicalPlan::Aggregate(aggregate) = node
            && let datafusion_expr::LogicalPlan::Extension(extension) = aggregate.input.as_ref()
            && extension
                .node
                .as_any()
                .is::<crate::dist_plan::MergeScanLogicalPlan>()
        {
            aggregates.push(aggregate);
        }
        Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
    });
    aggregates
}

async fn optimized_plan_schema_for(
    engine: &crate::query_engine::QueryEngineRef,
    sql: &str,
) -> Vec<(String, arrow_schema::DataType)> {
    let context = QueryContext::arc();
    let statement = QueryLanguageParser::parse_sql(sql, &context).unwrap();
    let plan = engine.planner().plan(&statement, context).await.unwrap();
    let plan = engine.engine_state().optimize_logical_plan(plan).unwrap();
    plan.schema()
        .fields()
        .iter()
        .map(|field| (field.name().clone(), field.data_type().clone()))
        .collect()
}

async fn optimized_plan(engine: &crate::query_engine::QueryEngineRef) -> String {
    optimized_plan_for(
        engine,
        "SELECT f.host, d.area, AVG(f.value * d.weight) AS score FROM fact f JOIN dim d ON f.host = d.host GROUP BY f.host, d.area",
    )
    .await
}

#[tokio::test]
async fn factorized_join_agg_matches_with_dist_planner() {
    let baseline = optimized_plan(&engine(false, true)).await;
    let rewritten = optimized_plan(&engine(true, true)).await;
    assert_ne!(baseline, rewritten, "{rewritten}");
    assert!(rewritten.contains("partial_nv"), "{rewritten}");
}

/// AVG/SUM/COUNT put `__<f>_state` in each region plan and the matching
/// `__<f>_merge` above the merge scan: a region sends one state row per group it
/// holds rows for, and the merge aggregate collapses them into the one row per
/// group the join reads.
#[tokio::test]
async fn branch_aggregates_move_into_the_merge_scan_as_states() {
    use datafusion_expr::LogicalPlan;

    let sum_sql = concat!(
        "SELECT m.mhost, n.zone, SUM(m.mval * n.wgt) AS score FROM metrics m ",
        "JOIN nodes n ON m.mhost = n.nhost GROUP BY m.mhost, n.zone"
    );
    /// One case: the query, the state and merge functions both branches call,
    /// and the partial aliases the fact and dim merge aggregates keep.
    struct Case {
        sql: &'static str,
        states: &'static [&'static str],
        merge_functions: &'static [&'static str],
        fact_aliases: &'static [&'static str],
        dim_aliases: &'static [&'static str],
    }
    let cases = [
        Case {
            sql: METRIC_AVG_SQL,
            states: &["__count_state", "__sum_state"],
            merge_functions: &["__count_merge", "__sum_merge"],
            fact_aliases: &["partial_nv", "partial_sv"],
            dim_aliases: &["partial_nw", "partial_sw"],
        },
        Case {
            sql: sum_sql,
            states: &["__count_state", "__sum_state"],
            merge_functions: &["__count_merge", "__sum_merge"],
            fact_aliases: &["partial_nv", "partial_sv"],
            dim_aliases: &["partial_nw", "partial_sw"],
        },
        // `COUNT` consumes the two counts alone, so it builds no payload sum.
        Case {
            sql: METRIC_COUNT_SQL,
            states: &["__count_state"],
            merge_functions: &["__count_merge"],
            fact_aliases: &["partial_nv"],
            dim_aliases: &["partial_nw"],
        },
    ];
    for Case {
        sql,
        states,
        merge_functions,
        fact_aliases,
        dim_aliases,
    } in cases
    {
        let (metrics, nodes) = metrics_and_nodes();
        let engine = engine_over_with(metrics.clone(), nodes.clone(), true);
        let plan = optimized_logical_for(&engine, sql).await;
        let remotes = remote_inputs(&plan);
        assert_eq!(remotes.len(), 2, "{sql}:\n{}", plan.display_indent());
        let (LogicalPlan::Aggregate(fact), LogicalPlan::Aggregate(dim)) = (remotes[0], remotes[1])
        else {
            panic!(
                "a branch aggregate is not inside its merge scan for {sql}:\n{}",
                plan.display_indent()
            );
        };
        assert_eq!(group_names(fact), ["m.mhost"], "{sql}");
        assert_eq!(group_names(dim), ["n.nhost", "n.zone"], "{sql}");
        assert_function_names(fact, states, sql);
        assert_function_names(dim, states, sql);
        // The merge aggregate keeps the output names the reconstruction reads.
        let merges = aggregates_above_merge_scan(&plan);
        assert_eq!(merges.len(), 2, "{sql}:\n{}", plan.display_indent());
        assert_function_names(merges[0], merge_functions, sql);
        assert_function_names(merges[1], merge_functions, sql);
        assert_partial_aliases(merges[0], fact_aliases, sql);
        assert_partial_aliases(merges[1], dim_aliases, sql);
        // The pushed aggregate sits on top of exactly the remote input the
        // datanode ran before: nothing else about the branch moved.
        let baseline = engine_over_with(metrics, nodes, false);
        let baseline = optimized_logical_for(&baseline, sql).await;
        let baseline_remotes = remote_inputs(&baseline);
        assert_eq!(baseline_remotes.len(), 2);
        assert_eq!(fact.input.to_string(), baseline_remotes[0].to_string());
        assert_eq!(dim.input.to_string(), baseline_remotes[1].to_string());
    }
}

/// The aggregate function each output expression of `aggregate` calls, with the
/// alias stripped, in output order.
fn function_names(aggregate: &datafusion_expr::logical_plan::Aggregate) -> Vec<String> {
    aggregate.aggr_expr.iter().map(function_name).collect()
}

/// The name of the aggregate function `expr` calls, with its alias stripped.
fn function_name(expr: &Expr) -> String {
    let mut expr = expr;
    while let Expr::Alias(alias) = expr {
        expr = &alias.expr;
    }
    match expr {
        Expr::AggregateFunction(function) => function.func.name().to_string(),
        other => panic!("not an aggregate function: {other}"),
    }
}

/// Asserts `aggregate` calls the functions `expected` names, in output order.
fn assert_function_names(
    aggregate: &datafusion_expr::logical_plan::Aggregate,
    expected: &[&str],
    sql: &str,
) {
    let names = function_names(aggregate);
    assert_eq!(names.len(), expected.len(), "{sql}: {names:?}");
    for (name, expected) in names.iter().zip(expected) {
        assert!(name.starts_with(expected), "{sql}: {name} lacks {expected}");
    }
}

/// The group keys of a branch aggregate, in output order.
fn group_names(aggregate: &datafusion_expr::logical_plan::Aggregate) -> Vec<String> {
    aggregate
        .group_expr
        .iter()
        .map(|expr| expr.to_string())
        .collect()
}

/// Asserts the branch aggregate emits its partial statistics under `expected`
/// aliases, in order; the alias generator numbers them.
fn assert_partial_aliases(
    aggregate: &datafusion_expr::logical_plan::Aggregate,
    expected: &[&str],
    sql: &str,
) {
    let names: Vec<String> = aggregate
        .aggr_expr
        .iter()
        .map(|expr| match expr {
            Expr::Alias(alias) => alias.name.clone(),
            other => panic!("a partial aggregate is not aliased: {other}"),
        })
        .collect();
    assert_eq!(names.len(), expected.len(), "{sql}: {names:?}");
    for (name, prefix) in names.iter().zip(expected) {
        assert!(name.starts_with(prefix), "{sql}: {name} lacks {prefix}");
    }
}

#[tokio::test]
async fn duplicate_right_group_key_keeps_baseline() {
    let query_engine = engine(true, false);
    for aggregate in ["AVG", "SUM"] {
        let sql = format!(
            "SELECT f.host AS left_host, d.host AS right_host, {aggregate}(f.value * d.weight) AS score FROM fact f JOIN dim d ON f.host = d.host GROUP BY f.host, d.host"
        );
        let plan = optimized_plan_for(&query_engine, &sql).await;
        let baseline = optimized_plan_for(&engine(false, false), &sql).await;
        assert!(!plan.contains("partial_nv"), "{plan}");
        assert_eq!(plan, baseline);
        assert_eq!(
            optimized_plan_schema_for(&query_engine, &sql).await,
            vec![
                ("left_host".to_string(), arrow_schema::DataType::Utf8),
                ("right_host".to_string(), arrow_schema::DataType::Utf8),
                ("score".to_string(), arrow_schema::DataType::Float64),
            ]
        );
        let rows = query_output(&query_engine, &sql).await;
        assert_eq!(
            rows,
            vec![vec!["a".to_string(), "a".to_string(), "6".to_string()]]
        );
    }
}

#[tokio::test]
async fn reconstruction_uses_float_counts_and_nulls_empty_products() {
    use datafusion_common::tree_node::TreeNode;
    use datafusion_expr::{Expr, LogicalPlan};
    use datafusion_optimizer::OptimizerRule;

    let engine = engine(true, false);
    for aggregate in ["AVG", "SUM"] {
        let sql = format!(
            "SELECT f.host, d.area, {aggregate}(f.value * d.weight) AS score FROM fact f JOIN dim d ON f.host = d.host GROUP BY f.host, d.area"
        );
        let context = QueryContext::arc();
        let statement = QueryLanguageParser::parse_sql(&sql, &context).unwrap();
        let logical = engine
            .planner()
            .plan(&statement, context.clone())
            .await
            .unwrap();
        let mut optimizer_options = datafusion::config::ConfigOptions::default();
        optimizer_options
            .extensions
            .insert(crate::options::FactorizedJoinAggOptions { enabled: true });
        let optimizer_config = datafusion_optimizer::OptimizerContext::new_with_config_options(
            Arc::new(optimizer_options),
        );
        let logical = logical
            .transform_up(|plan| {
                datafusion_optimizer::extract_equijoin_predicate::ExtractEquijoinPredicate::new()
                    .rewrite(plan, &optimizer_config)
            })
            .unwrap()
            .data;
        let LogicalPlan::Projection(planner_projection) = logical else {
            panic!("expected planner projection after equijoin extraction");
        };
        let LogicalPlan::Aggregate(agg) = planner_projection.input.as_ref() else {
            panic!("expected aggregate below planner projection");
        };
        let agg = agg.clone();
        let transformed = crate::optimizer::factorized_join_agg::FactorizedJoinAggRule
            .rewrite(LogicalPlan::Aggregate(agg), &optimizer_config)
            .unwrap();
        assert!(
            transformed.transformed,
            "factorized rule did not rewrite {sql}"
        );
        let rewritten = transformed.data;
        let LogicalPlan::Projection(projection) = &rewritten else {
            panic!("expected reconstruction projection: {rewritten:?}");
        };
        let expression = match projection.expr.last().unwrap() {
            Expr::Alias(alias) => alias.expr.as_ref(),
            expr => expr,
        };
        let input_schema = projection.input.schema();
        let physical = create_physical_expr(
            expression,
            input_schema,
            &ExecutionProps::new(),
            &PhysicalPlanningContext::default(),
        )
        .unwrap();

        let cases = [
            (1_i64 << 32, 1_i64 << 32, false),
            (1_i64 << 32, 1_i64 << 31, false),
            (0, 1_i64 << 32, true),
            (1_i64 << 32, 0, true),
        ];
        for (left_count, right_count, empty) in cases {
            let schema = Arc::new(input_schema.as_arrow().clone());
            let columns: Vec<ArrayRef> = input_schema
                .fields()
                .iter()
                .map(|field| match field.name() {
                    name if name.contains("partial_nv") => {
                        Arc::new(Int64Array::from(vec![left_count])) as ArrayRef
                    }
                    name if name.contains("partial_nw") => {
                        Arc::new(Int64Array::from(vec![right_count])) as ArrayRef
                    }
                    name if name.contains("partial_sv") => {
                        Arc::new(Float64Array::from(vec![if left_count == 0 {
                            None
                        } else {
                            Some(left_count as f64 * 2.0)
                        }])) as ArrayRef
                    }
                    name if name.contains("partial_sw") => {
                        Arc::new(Float64Array::from(vec![if right_count == 0 {
                            None
                        } else {
                            Some(right_count as f64 * 3.0)
                        }])) as ArrayRef
                    }
                    _ if field.data_type() == &arrow_schema::DataType::Utf8 => {
                        Arc::new(StringArray::from(vec![Some("a")])) as ArrayRef
                    }
                    _ => panic!("unexpected reconstruction input field: {field:?}"),
                })
                .collect();
            let batch = ArrowRecordBatch::try_new(schema, columns).unwrap();
            let result = physical.evaluate(&batch).unwrap().into_array(1).unwrap();
            let value = ScalarValue::try_from_array(result.as_ref(), 0).unwrap();
            if empty {
                assert_eq!(value, ScalarValue::Float64(None), "{aggregate}");
            } else {
                let ScalarValue::Float64(Some(value)) = value else {
                    panic!("expected Float64 result, got {value:?}");
                };
                assert!(value >= 0.0, "{aggregate}: {value}");
                let expected = if aggregate == "AVG" {
                    6.0
                } else {
                    6.0 * left_count as f64 * right_count as f64
                };
                assert_eq!(value, expected, "{aggregate}");
            }
        }
    }
}

#[tokio::test]
async fn factorized_join_agg_is_opt_in() {
    let disabled_plan = optimized_plan(&engine(false, false)).await;
    let enabled_plan = optimized_plan(&engine(true, false)).await;
    assert!(disabled_plan.contains("Aggregate"), "{disabled_plan}");
    assert!(enabled_plan.contains("Aggregate"), "{enabled_plan}");
    assert_ne!(disabled_plan, enabled_plan);
}

// ---------- Fixtures for the review-fix test matrix ----------

/// Two tables NOT named fact/dim, with NULLs, duplicates, and a valid=0 group:
/// metrics(host mhost, mval) x nodes(host nhost, zone, wgt).
/// hosts: m1{1.0, NULL, 2.0}, m2{NULL, 4.0}; zones: m1:{west(2.0), east(NULL)},
/// m2:{west(0.5)}. The (m2, west) group has 1 valid value; (m1, east) has
/// valid=0 -> NULL AVG/SUM expected.
fn metrics_and_nodes() -> (RecordBatch, RecordBatch) {
    let metrics = RecordBatch::new(
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("mhost", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("mval", ConcreteDataType::float64_datatype(), true),
        ])),
        vec![
            Arc::new(TimestampMillisecondVector::from_slice(vec![0; 5])) as _,
            Arc::new(StringVector::from(vec![
                Some("m1"),
                Some("m1"),
                Some("m1"),
                Some("m2"),
                Some("m2"),
            ])) as _,
            Arc::new(Float64Vector::from(vec![
                Some(1.0),
                None,
                Some(2.0),
                None,
                Some(4.0),
            ])) as _,
        ],
    )
    .unwrap();
    let nodes = RecordBatch::new(
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("nhost", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("zone", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("wgt", ConcreteDataType::float64_datatype(), true),
        ])),
        vec![
            Arc::new(TimestampMillisecondVector::from_slice(vec![0; 3])) as _,
            Arc::new(StringVector::from(vec![Some("m1"), Some("m1"), Some("m2")])) as _,
            Arc::new(StringVector::from(vec![
                Some("west"),
                Some("east"),
                Some("west"),
            ])) as _,
            Arc::new(Float64Vector::from(vec![Some(2.0), None, Some(0.5)])) as _,
        ],
    )
    .unwrap();
    (metrics, nodes)
}

/// Builds an engine over custom tables with the given option, registering
/// `metrics`/`nodes` in place of fact/dim.
fn engine_over(
    metrics: RecordBatch,
    nodes: RecordBatch,
    enabled: bool,
) -> crate::query_engine::QueryEngineRef {
    engine_over_impl(metrics, nodes, enabled, false)
}

/// Same tables, planned by the distributed planner, so every table scan is
/// wrapped in a merge scan the way it is on a frontend.
fn engine_over_with(
    metrics: RecordBatch,
    nodes: RecordBatch,
    enabled: bool,
) -> crate::query_engine::QueryEngineRef {
    engine_over_impl(metrics, nodes, enabled, true)
}

fn engine_over_impl(
    metrics: RecordBatch,
    nodes: RecordBatch,
    enabled: bool,
    with_dist_planner: bool,
) -> crate::query_engine::QueryEngineRef {
    let catalog = catalog::memory::new_memory_catalog_manager().unwrap();
    for (name, table_id, batch) in [("metrics", 201, metrics), ("nodes", 202, nodes)] {
        let table = MemTable::new_with_catalog(
            name,
            batch,
            table_id,
            DEFAULT_CATALOG_NAME.to_string(),
            DEFAULT_SCHEMA_NAME.to_string(),
        );
        catalog
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: name.to_string(),
                table_id,
                table,
            })
            .unwrap();
    }
    QueryEngineFactory::new(
        catalog,
        None,
        None,
        None,
        None,
        with_dist_planner,
        QueryOptions {
            experimental_enable_factorized_join_agg: enabled,
            ..Default::default()
        },
    )
    .query_engine()
}

async fn query_plan(engine: &crate::query_engine::QueryEngineRef, sql: &str) -> String {
    let context = QueryContext::arc();
    let statement = QueryLanguageParser::parse_sql(sql, &context).unwrap();
    let plan = engine.planner().plan(&statement, context).await.unwrap();
    let plan = engine.engine_state().optimize_logical_plan(plan).unwrap();
    plan.display_indent().to_string()
}

async fn query_output(engine: &crate::query_engine::QueryEngineRef, sql: &str) -> Vec<Vec<String>> {
    let context = QueryContext::arc();
    let statement = QueryLanguageParser::parse_sql(sql, &context).unwrap();
    let plan = engine
        .planner()
        .plan(&statement, context.clone())
        .await
        .unwrap();
    let output = engine.execute(plan, context).await.unwrap();
    let data = output.data;
    let batches = match data {
        common_query::OutputData::RecordBatches(batches) => batches,
        common_query::OutputData::Stream(stream) => {
            common_recordbatch::util::collect_batches(stream)
                .await
                .unwrap()
        }
        other => panic!("unexpected output: {other:?}"),
    };
    let mut rows = Vec::new();
    for batch in batches.iter() {
        let vectors: Vec<_> = (0..batch.num_columns())
            .map(|c| datatypes::vectors::Helper::try_into_vector(batch.column(c)).unwrap())
            .collect();
        for i in 0..batch.num_rows() {
            let row: Vec<String> = vectors
                .iter()
                .map(|v| {
                    let value = datatypes::value::Value::from(v.get_ref(i));
                    value.to_string()
                })
                .collect();
            rows.push(row);
        }
    }
    rows.sort();
    rows
}

/// The error the query fails with, so two engines can be checked to fail the
/// same way. Panics when the query succeeds.
async fn query_failure(engine: &crate::query_engine::QueryEngineRef, sql: &str) -> String {
    let context = QueryContext::arc();
    let statement = QueryLanguageParser::parse_sql(sql, &context).unwrap();
    let plan = engine
        .planner()
        .plan(&statement, context.clone())
        .await
        .unwrap();
    let output = match engine.execute(plan, context).await {
        Ok(output) => output,
        Err(err) => return format!("{err:?}"),
    };
    let batches = match output.data {
        common_query::OutputData::RecordBatches(batches) => batches,
        common_query::OutputData::Stream(stream) => {
            match common_recordbatch::util::collect_batches(stream).await {
                Ok(batches) => batches,
                Err(err) => return format!("{err:?}"),
            }
        }
        other => panic!("unexpected output: {other:?}"),
    };
    panic!("expected the query to fail, got {batches:?}");
}

/// A NULL score displays as the type name ("Null"); a real one parses as f64.
fn parse_score(cell: &str) -> Option<f64> {
    if cell.contains("Null") {
        None
    } else {
        Some(cell.parse().expect("score cell parses as f64"))
    }
}

const METRIC_AVG_SQL: &str = "SELECT m.mhost, n.zone, AVG(m.mval * n.wgt) AS score FROM metrics m JOIN nodes n ON m.mhost = n.nhost GROUP BY m.mhost, n.zone";

#[tokio::test]
async fn renamed_tables_and_columns_are_rewritten() {
    let (metrics, nodes) = metrics_and_nodes();
    let baseline = query_plan(
        &engine_over(metrics.clone(), nodes.clone(), false),
        METRIC_AVG_SQL,
    )
    .await;
    let rewritten = query_plan(&engine_over(metrics, nodes, true), METRIC_AVG_SQL).await;
    assert_ne!(baseline, rewritten, "{rewritten}");
    assert!(rewritten.contains("partial_nv"), "{rewritten}");
}

#[tokio::test]
async fn rewritten_results_match_baseline_within_tolerance() {
    let (metrics, nodes) = metrics_and_nodes();
    let baseline_rows = query_output(
        &engine_over(metrics.clone(), nodes.clone(), false),
        METRIC_AVG_SQL,
    )
    .await;
    let rewritten_rows = query_output(&engine_over(metrics, nodes, true), METRIC_AVG_SQL).await;
    assert_eq!(baseline_rows.len(), rewritten_rows.len());
    for (b, r) in baseline_rows.iter().zip(&rewritten_rows) {
        assert_eq!(b[0], r[0], "host mismatch");
        assert_eq!(b[1], r[1], "zone mismatch");
        match (parse_score(&b[2]), parse_score(&r[2])) {
            (Some(bv), Some(rv)) => assert!(
                (bv - rv).abs() <= 1e-9 * bv.abs().max(1.0),
                "score mismatch: baseline {bv} vs rewritten {rv}"
            ),
            (None, None) => {} // both NULL
            (b, r) => panic!("nullness mismatch: baseline {b:?} vs rewritten {r:?}"),
        }
    }
    // valid=0 group (m1, east) must be NULL in both arms.
    let null_row = baseline_rows
        .iter()
        .find(|r| r[1] == "east")
        .expect("east group present");
    assert_eq!(
        parse_score(&null_row[2]),
        None,
        "valid=0 group must be NULL"
    );
}

#[tokio::test]
async fn computed_alias_projection_keeps_the_baseline_plan() {
    let (metrics, nodes) = metrics_and_nodes();
    let sql = "SELECT host, zone, AVG(mval * wgt) AS score FROM (SELECT m.mhost AS host, n.zone, m.mval + 1 AS mval, n.wgt FROM metrics m JOIN nodes n ON m.mhost = n.nhost) GROUP BY host, zone";
    let baseline_rows =
        query_output(&engine_over(metrics.clone(), nodes.clone(), false), sql).await;
    let enabled = engine_over(metrics, nodes, true);
    let plan = query_plan(&enabled, sql).await;
    assert!(
        !plan.contains("partial_nv"),
        "computed-alias projection must not rewrite:\n{plan}"
    );
    let rows = query_output(&enabled, sql).await;
    assert_eq!(baseline_rows, rows);

    let unchanged_groups_sql = "SELECT m.mhost, n.zone, AVG(mval * wgt) AS score FROM (SELECT m.mhost, n.zone, m.mval + 1 AS mval, n.wgt FROM metrics m JOIN nodes n ON m.mhost = n.nhost) GROUP BY m.mhost, n.zone";
    let plan = query_plan(&enabled, unchanged_groups_sql).await;
    assert!(
        !plan.contains("partial_nv"),
        "computed payload must not rewrite even when grouping columns pass through unchanged:\n{plan}"
    );
}

#[tokio::test]
async fn projected_product_rewrites_and_matches_baseline() {
    let (metrics, nodes) = metrics_and_nodes();
    let sql = "SELECT mhost, zone, AVG(p) AS score FROM (SELECT m.mhost, n.zone, m.mval * n.wgt AS p, n.wgt AS mval, m.mval AS wgt FROM metrics m JOIN nodes n ON m.mhost = n.nhost) GROUP BY mhost, zone";
    let baseline = engine_over(metrics.clone(), nodes.clone(), false);
    let rewritten = engine_over(metrics, nodes, true);
    let plan = query_plan(&rewritten, sql).await;
    assert!(
        plan.contains("partial_nv"),
        "projected product should rewrite:\n{plan}"
    );
    let plan_lower = plan.to_lowercase();
    assert!(
        plan_lower.contains("m.mval"),
        "fact payload source missing:\n{plan}"
    );
    assert!(
        plan_lower.contains("n.wgt"),
        "dim payload source missing:\n{plan}"
    );
    assert_eq!(
        query_output(&baseline, sql).await,
        query_output(&rewritten, sql).await
    );
}

#[tokio::test]
async fn sum_and_reversed_product_rewrites_match_baseline() {
    let (metrics, nodes) = metrics_and_nodes();
    let cases = [
        "SELECT m.mhost, n.zone, SUM(m.mval * n.wgt) AS score FROM metrics m JOIN nodes n ON m.mhost = n.nhost GROUP BY m.mhost, n.zone",
        "SELECT m.mhost, n.zone, AVG(n.wgt * m.mval) AS score FROM metrics m JOIN nodes n ON m.mhost = n.nhost GROUP BY m.mhost, n.zone",
    ];
    for sql in cases {
        let baseline = engine_over(metrics.clone(), nodes.clone(), false);
        let rewritten = engine_over(metrics.clone(), nodes.clone(), true);
        let plan = query_plan(&rewritten, sql).await;
        assert!(plan.contains("partial_nv"), "should rewrite {sql}:\n{plan}");
        assert_eq!(
            query_output(&baseline, sql).await,
            query_output(&rewritten, sql).await
        );
    }
}

#[tokio::test]
async fn non_float64_payload_keeps_the_baseline_plan() {
    let metrics = RecordBatch::new(
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("mhost", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("mval", ConcreteDataType::int64_datatype(), true),
        ])),
        vec![
            Arc::new(TimestampMillisecondVector::from_slice(vec![0; 2])) as _,
            Arc::new(StringVector::from(vec![Some("m1"), Some("m1")])) as _,
            Arc::new(datatypes::vectors::Int64Vector::from(vec![
                Some(1),
                Some(2),
            ])) as _,
        ],
    )
    .unwrap();
    let (_, nodes) = metrics_and_nodes();
    let enabled = engine_over(metrics, nodes, true);
    let plan = query_plan(&enabled, METRIC_AVG_SQL).await;
    assert!(
        !plan.contains("partial_nv"),
        "Int64 payload must not rewrite:\n{plan}"
    );
}

#[tokio::test]
async fn near_miss_shapes_stay_on_the_baseline() {
    let (metrics, nodes) = metrics_and_nodes();
    let enabled = engine_over(metrics, nodes, true);
    for sql in [
        "SELECT m.mhost, n.zone, AVG(m.mval) AS score FROM metrics m JOIN nodes n ON m.mhost = n.nhost GROUP BY m.mhost, n.zone",
        "SELECT m.mhost, n.zone, AVG(DISTINCT m.mval * n.wgt) AS score FROM metrics m JOIN nodes n ON m.mhost = n.nhost GROUP BY m.mhost, n.zone",
        "SELECT m.mhost, n.zone, AVG(m.mval * n.wgt) FILTER (WHERE m.mval > 0) AS score FROM metrics m JOIN nodes n ON m.mhost = n.nhost GROUP BY m.mhost, n.zone",
        "SELECT m.mhost, n.zone, AVG(m.mval * n.wgt) AS score, COUNT(*) AS c FROM metrics m JOIN nodes n ON m.mhost = n.nhost GROUP BY m.mhost, n.zone",
        "SELECT m.mhost, n.zone, SUM(m.mval + n.wgt) AS score FROM metrics m JOIN nodes n ON m.mhost = n.nhost GROUP BY m.mhost, n.zone",
    ] {
        let plan = query_plan(&enabled, sql).await;
        assert!(
            !plan.contains("partial_nv"),
            "must not rewrite {sql}:\n{plan}"
        );
    }
}

/// Optimize through the per-query state so session SET overrides exercise the
/// actual optimizer extension, not just the option value.
#[tokio::test]
async fn session_set_false_overrides_server_true() {
    use crate::options::FactorizedJoinAggOptions;
    let (metrics, nodes) = metrics_and_nodes();
    let engine = engine_over(metrics, nodes, true);
    let df_engine = engine
        .as_any()
        .downcast_ref::<crate::datafusion::DatafusionQueryEngine>()
        .expect("datafusion engine");

    // Session never SET: the server-level ON default reaches the query state.
    let ctx = QueryContext::arc();
    let engine_ctx = df_engine.engine_context_for_tests(ctx.clone());
    let inherited = engine_ctx.state();
    let inherited_on = inherited
        .config()
        .options()
        .extensions
        .get::<FactorizedJoinAggOptions>()
        .expect("server default present")
        .enabled;
    assert!(inherited_on);
    let statement = QueryLanguageParser::parse_sql(METRIC_AVG_SQL, &ctx).unwrap();
    let logical = engine
        .planner()
        .plan(&statement, ctx.clone())
        .await
        .unwrap();
    let inherited_plan = inherited.optimize(&logical).unwrap();
    assert!(
        inherited_plan
            .display_indent()
            .to_string()
            .contains("partial_nv"),
        "server true must rewrite"
    );

    // Session SET false: the same state now reports disabled.
    ctx.configuration_parameter()
        .set_query_option("experimental_enable_factorized_join_agg", "false")
        .unwrap();
    let engine_ctx2 = df_engine.engine_context_for_tests(ctx.clone());
    let overridden = engine_ctx2.state();
    let now = overridden
        .config()
        .options()
        .extensions
        .get::<FactorizedJoinAggOptions>()
        .expect("extension present after SET")
        .enabled;
    assert!(!now, "session SET false must override server true");
    let overridden_plan = overridden.optimize(&logical).unwrap();
    assert!(
        !overridden_plan
            .display_indent()
            .to_string()
            .contains("partial_nv"),
        "session false must keep baseline plan"
    );

    // Session SET true on a server-OFF engine: enabled.
    let (metrics2, nodes2) = metrics_and_nodes();
    let engine_off = engine_over(metrics2, nodes2, false);
    let df_engine_off = engine_off
        .as_any()
        .downcast_ref::<crate::datafusion::DatafusionQueryEngine>()
        .unwrap();
    let ctx2 = QueryContext::arc();
    ctx2.configuration_parameter()
        .set_query_option("experimental_enable_factorized_join_agg", "true")
        .unwrap();
    let enabled = df_engine_off
        .engine_context_for_tests(ctx2.clone())
        .state()
        .config()
        .options()
        .extensions
        .get::<FactorizedJoinAggOptions>()
        .expect("extension present after SET true")
        .enabled;
    assert!(enabled);
    let statement2 = QueryLanguageParser::parse_sql(METRIC_AVG_SQL, &ctx2).unwrap();
    let logical2 = engine_off
        .planner()
        .plan(&statement2, ctx2.clone())
        .await
        .unwrap();
    let enabled_ctx = df_engine_off.engine_context_for_tests(ctx2);
    let enabled_state = enabled_ctx.state();
    let enabled_plan = enabled_state.optimize(&logical2).unwrap();
    assert!(
        enabled_plan
            .display_indent()
            .to_string()
            .contains("partial_nv"),
        "session true must rewrite"
    );
}

/// Two tables whose group/join keys are literally named like the aliases the
/// rewrite generates: `partial_nv_1` is the left grouping/join key and
/// `partial_nw_3` the right group column, so both land next to a qualified
/// group field of the same bare name in a partial aggregate's output schema.
/// `partial_sv_2`/`partial_sw_4` are plain payload-side columns named like the
/// remaining two aliases. hosts: a{2.0}, b{4.0};
/// areas: a:west(3.0), b:east(0.5).
fn alias_like_column_names() -> (RecordBatch, RecordBatch) {
    let metrics = RecordBatch::new(
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("partial_nv_1", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("value", ConcreteDataType::float64_datatype(), true),
            ColumnSchema::new("partial_sv_2", ConcreteDataType::string_datatype(), true),
        ])),
        vec![
            Arc::new(TimestampMillisecondVector::from_slice(vec![0, 1])) as _,
            Arc::new(StringVector::from(vec![Some("a"), Some("b")])) as _,
            Arc::new(Float64Vector::from(vec![Some(2.0), Some(4.0)])) as _,
            Arc::new(StringVector::from(vec![Some("x"), Some("y")])) as _,
        ],
    )
    .unwrap();
    let nodes = RecordBatch::new(
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("partial_nw_3", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("weight", ConcreteDataType::float64_datatype(), true),
            ColumnSchema::new("partial_sw_4", ConcreteDataType::string_datatype(), true),
        ])),
        vec![
            Arc::new(TimestampMillisecondVector::from_slice(vec![0, 1])) as _,
            Arc::new(StringVector::from(vec![Some("a"), Some("b")])) as _,
            Arc::new(StringVector::from(vec![Some("west"), Some("east")])) as _,
            Arc::new(Float64Vector::from(vec![Some(3.0), Some(0.5)])) as _,
            Arc::new(StringVector::from(vec![Some("p"), Some("q")])) as _,
        ],
    )
    .unwrap();
    (metrics, nodes)
}

/// A column named like a generated alias must not make the partial aggregates'
/// schemas ambiguous: the aliases are renamed instead, and the results still
/// match the baseline exactly.
#[tokio::test]
async fn aliases_do_not_collide_with_column_names() {
    let (metrics, nodes) = alias_like_column_names();
    let sql = "SELECT m.partial_nv_1, n.partial_nw_3, AVG(m.value * n.weight) AS score \
               FROM metrics m JOIN nodes n ON m.partial_nv_1 = n.host \
               GROUP BY m.partial_nv_1, n.partial_nw_3";
    let baseline = engine_over(metrics.clone(), nodes.clone(), false);
    let rewritten = engine_over(metrics, nodes, true);
    let baseline_plan = query_plan(&baseline, sql).await;
    let rewritten_plan = query_plan(&rewritten, sql).await;
    assert!(
        rewritten_plan.contains("_partial_nv_1") && rewritten_plan.contains("_partial_nw_3"),
        "colliding column names must be renamed in the rewrite:\n{rewritten_plan}"
    );
    assert!(
        !baseline_plan.contains("_partial_"),
        "switch off must keep the baseline plan:\n{baseline_plan}"
    );
    assert_eq!(
        query_output(&baseline, sql).await,
        query_output(&rewritten, sql).await
    );
}

// ---------- `COUNT(<fact payload> * <dim payload>)` ----------

const METRIC_COUNT_SQL: &str = "SELECT m.mhost, n.zone, COUNT(m.mval * n.wgt) AS pairs FROM metrics m JOIN nodes n ON m.mhost = n.nhost GROUP BY m.mhost, n.zone";

/// Facts and dims covering every nullness combination a counted product can
/// have, plus a NULL group value and hosts present on one side only.
///
/// metrics hosts m1{1.0, NULL, 2.0}, m2{NULL, NULL}, m3{5.0}, m5{7.0};
/// nodes m1:{west(2.0), east(NULL), NULL zone(4.0)}, m2:{north(3.0)},
/// m3:{west(1.0), west(0.5)}, m4:{south(1.0)}. m4 and m5 have no partner, so
/// the inner join gives them no group in either plan. Expected counts:
/// (m1, NULL) 2, (m1, east) 0, (m1, west) 2, (m2, north) 0, (m3, west) 2.
fn count_fixture() -> (RecordBatch, RecordBatch) {
    let metrics = RecordBatch::new(
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("mhost", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("mval", ConcreteDataType::float64_datatype(), true),
        ])),
        vec![
            Arc::new(TimestampMillisecondVector::from_slice(vec![0; 7])) as _,
            Arc::new(StringVector::from(vec![
                Some("m1"),
                Some("m1"),
                Some("m1"),
                Some("m2"),
                Some("m2"),
                Some("m3"),
                Some("m5"),
            ])) as _,
            Arc::new(Float64Vector::from(vec![
                Some(1.0),
                None,
                Some(2.0),
                None,
                None,
                Some(5.0),
                Some(7.0),
            ])) as _,
        ],
    )
    .unwrap();
    let nodes = RecordBatch::new(
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("nhost", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("zone", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("wgt", ConcreteDataType::float64_datatype(), true),
        ])),
        vec![
            Arc::new(TimestampMillisecondVector::from_slice(vec![0; 7])) as _,
            Arc::new(StringVector::from(vec![
                Some("m1"),
                Some("m1"),
                Some("m1"),
                Some("m2"),
                Some("m3"),
                Some("m3"),
                Some("m4"),
            ])) as _,
            Arc::new(StringVector::from(vec![
                Some("west"),
                Some("east"),
                None,
                Some("north"),
                Some("west"),
                Some("west"),
                Some("south"),
            ])) as _,
            Arc::new(Float64Vector::from(vec![
                Some(2.0),
                None,
                Some(4.0),
                Some(3.0),
                Some(1.0),
                Some(0.5),
                Some(1.0),
            ])) as _,
        ],
    )
    .unwrap();
    (metrics, nodes)
}

/// Runs the factorized rule on its own, the way the optimizer does after
/// equijoin extraction, and returns the reconstruction expression with the
/// schema of the join it reads, so the expression can be evaluated directly.
async fn count_reconstruction(
    engine: &crate::query_engine::QueryEngineRef,
    sql: &str,
) -> (Arc<DFSchema>, Expr) {
    use datafusion_common::tree_node::TreeNode;
    use datafusion_expr::LogicalPlan;
    use datafusion_optimizer::OptimizerRule;

    let context = QueryContext::arc();
    let statement = QueryLanguageParser::parse_sql(sql, &context).unwrap();
    let logical = engine.planner().plan(&statement, context).await.unwrap();
    let mut optimizer_options = datafusion::config::ConfigOptions::default();
    optimizer_options
        .extensions
        .insert(crate::options::FactorizedJoinAggOptions { enabled: true });
    let optimizer_config = datafusion_optimizer::OptimizerContext::new_with_config_options(
        Arc::new(optimizer_options),
    );
    let logical = logical
        .transform_up(|plan| {
            datafusion_optimizer::extract_equijoin_predicate::ExtractEquijoinPredicate::new()
                .rewrite(plan, &optimizer_config)
        })
        .unwrap()
        .data;
    let LogicalPlan::Projection(planner_projection) = logical else {
        panic!("expected planner projection after equijoin extraction");
    };
    let LogicalPlan::Aggregate(agg) = planner_projection.input.as_ref() else {
        panic!("expected aggregate below planner projection");
    };
    let transformed = crate::optimizer::factorized_join_agg::FactorizedJoinAggRule
        .rewrite(LogicalPlan::Aggregate(agg.clone()), &optimizer_config)
        .unwrap();
    assert!(
        transformed.transformed,
        "factorized rule did not rewrite {sql}"
    );
    let LogicalPlan::Projection(projection) = transformed.data else {
        panic!("expected reconstruction projection");
    };
    let expression = match projection.expr.last().unwrap() {
        Expr::Alias(alias) => alias.expr.as_ref().clone(),
        expr => expr.clone(),
    };
    (projection.input.schema().clone(), expression)
}

#[tokio::test]
async fn count_query_rewrites_and_matches_the_flat_plan() {
    let (metrics, nodes) = count_fixture();
    let baseline = engine_over(metrics.clone(), nodes.clone(), false);
    let rewritten = engine_over(metrics, nodes, true);

    let plan = query_plan(&rewritten, METRIC_COUNT_SQL).await;
    assert!(plan.contains("partial_nv_"), "{plan}");
    assert!(plan.contains("partial_nw_"), "{plan}");
    // `COUNT` consumes the two counts alone, so no payload sum is built.
    assert!(!plan.contains("partial_sv_"), "{plan}");
    assert!(!plan.contains("partial_sw_"), "{plan}");

    let expected = vec![
        // A NULL group value forms a group like any other.
        vec!["m1".to_string(), "Null".to_string(), "2".to_string()],
        // The dim payload is NULL for every pair of the group: 0, not NULL.
        vec!["m1".to_string(), "east".to_string(), "0".to_string()],
        vec!["m1".to_string(), "west".to_string(), "2".to_string()],
        // The fact payload is NULL for every pair of the group: 0, not NULL.
        vec!["m2".to_string(), "north".to_string(), "0".to_string()],
        vec!["m3".to_string(), "west".to_string(), "2".to_string()],
    ];
    // The flat plan's own answers, so parity below is parity with the baseline.
    assert_eq!(query_output(&baseline, METRIC_COUNT_SQL).await, expected);
    assert_eq!(query_output(&rewritten, METRIC_COUNT_SQL).await, expected);
}

#[tokio::test]
async fn count_preserves_the_aggregate_output_name_and_type() {
    let (metrics, nodes) = count_fixture();
    let baseline = engine_over(metrics.clone(), nodes.clone(), false);
    let rewritten = engine_over(metrics, nodes, true);
    let sql = format!("{METRIC_COUNT_SQL} ORDER BY m.mhost, n.zone");
    let expected = vec![
        ("mhost".to_string(), arrow_schema::DataType::Utf8),
        ("zone".to_string(), arrow_schema::DataType::Utf8),
        ("pairs".to_string(), arrow_schema::DataType::Int64),
    ];
    assert_eq!(optimized_plan_schema_for(&baseline, &sql).await, expected);
    assert_eq!(optimized_plan_schema_for(&rewritten, &sql).await, expected);
    assert!(query_plan(&rewritten, &sql).await.contains("partial_nv_"));
    assert_eq!(
        query_output(&baseline, &sql).await,
        query_output(&rewritten, &sql).await
    );
}

#[tokio::test]
async fn count_star_and_near_miss_counts_keep_the_plain_aggregate() {
    let (metrics, nodes) = count_fixture();
    let baseline = engine_over(metrics.clone(), nodes.clone(), false);
    let rewritten = engine_over(metrics, nodes, true);
    // `COUNT(*)` counts join pairs, not non-NULL products; `DISTINCT`, a filter
    // and a single-column count are all outside the admitted shape.
    for sql in [
        "SELECT m.mhost, n.zone, COUNT(*) AS pairs FROM metrics m JOIN nodes n ON m.mhost = n.nhost GROUP BY m.mhost, n.zone ORDER BY m.mhost, n.zone",
        "SELECT m.mhost, n.zone, COUNT(DISTINCT m.mval * n.wgt) AS pairs FROM metrics m JOIN nodes n ON m.mhost = n.nhost GROUP BY m.mhost, n.zone ORDER BY m.mhost, n.zone",
        "SELECT m.mhost, n.zone, COUNT(m.mval * n.wgt) FILTER (WHERE m.mval > 0) AS pairs FROM metrics m JOIN nodes n ON m.mhost = n.nhost GROUP BY m.mhost, n.zone ORDER BY m.mhost, n.zone",
        "SELECT m.mhost, n.zone, COUNT(m.mval) AS pairs FROM metrics m JOIN nodes n ON m.mhost = n.nhost GROUP BY m.mhost, n.zone ORDER BY m.mhost, n.zone",
    ] {
        let plan = query_plan(&rewritten, sql).await;
        assert!(
            !plan.contains("partial_n"),
            "must not rewrite {sql}:\n{plan}"
        );
        assert_eq!(plan, query_plan(&baseline, sql).await, "{sql}");
        assert_eq!(
            query_output(&baseline, sql).await,
            query_output(&rewritten, sql).await,
            "{sql}"
        );
    }
}

/// A pair of tables whose payloads are both `Int64`: `COUNT` consumes only
/// their nullness, so it is admitted, while the same payloads keep `AVG` on the
/// flat plan, whose `Float64` requirement is not widened.
/// metrics(mhost, mval DECIMAL(38, 0)) x nodes(nhost, zone, wgt DECIMAL(38, 0))
/// with payloads of `10^30`, whose product `10^60` is out of range.
fn decimal_payload_fixture() -> (RecordBatch, RecordBatch) {
    let decimal = |value: i128| {
        datatypes::vectors::Decimal128Vector::new(
            arrow::array::Decimal128Array::from(vec![Some(value)])
                .with_data_type(arrow_schema::DataType::Decimal128(38, 0)),
        )
    };
    let metrics = RecordBatch::new(
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("mhost", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("mval", ConcreteDataType::decimal128_datatype(38, 0), true),
        ])),
        vec![
            Arc::new(TimestampMillisecondVector::from_slice(vec![0])) as _,
            Arc::new(StringVector::from(vec![Some("m1")])) as _,
            Arc::new(decimal(10i128.pow(30))) as _,
        ],
    )
    .unwrap();
    let nodes = RecordBatch::new(
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("nhost", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("zone", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("wgt", ConcreteDataType::decimal128_datatype(38, 0), true),
        ])),
        vec![
            Arc::new(TimestampMillisecondVector::from_slice(vec![0])) as _,
            Arc::new(StringVector::from(vec![Some("m1")])) as _,
            Arc::new(StringVector::from(vec![Some("west")])) as _,
            Arc::new(decimal(10i128.pow(30))) as _,
        ],
    )
    .unwrap();
    (metrics, nodes)
}

fn count_integer_payload_fixture() -> (RecordBatch, RecordBatch) {
    let metrics = RecordBatch::new(
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("mhost", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("mval", ConcreteDataType::int64_datatype(), true),
        ])),
        vec![
            Arc::new(TimestampMillisecondVector::from_slice(vec![0; 3])) as _,
            Arc::new(StringVector::from(vec![Some("m1"), Some("m1"), Some("m2")])) as _,
            Arc::new(datatypes::vectors::Int64Vector::from(vec![
                Some(1),
                Some(2),
                None,
            ])) as _,
        ],
    )
    .unwrap();
    let nodes = RecordBatch::new(
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("nhost", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("zone", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("wgt", ConcreteDataType::int64_datatype(), true),
        ])),
        vec![
            Arc::new(TimestampMillisecondVector::from_slice(vec![0; 3])) as _,
            Arc::new(StringVector::from(vec![Some("m1"), Some("m1"), Some("m2")])) as _,
            Arc::new(StringVector::from(vec![
                Some("west"),
                Some("east"),
                Some("west"),
            ])) as _,
            Arc::new(datatypes::vectors::Int64Vector::from(vec![
                Some(2),
                None,
                Some(3),
            ])) as _,
        ],
    )
    .unwrap();
    (metrics, nodes)
}

/// All three functions require `Float64` payloads. An integer product wraps or
/// errors depending on the kernel's overflow setting, so it is not a product the
/// count rewrite can rely on never failing where the flat plan evaluates it.
#[tokio::test]
async fn count_requires_float64_payloads_like_avg() {
    let (metrics, nodes) = count_integer_payload_fixture();
    let baseline = engine_over(metrics.clone(), nodes.clone(), false);
    let rewritten = engine_over(metrics, nodes, true);

    let plan = query_plan(&rewritten, METRIC_COUNT_SQL).await;
    assert!(
        !plan.contains("partial_nv_"),
        "COUNT must keep its Float64 payload requirement:\n{plan}"
    );
    assert_eq!(plan, query_plan(&baseline, METRIC_COUNT_SQL).await);
    assert_eq!(
        optimized_plan_schema_for(&rewritten, METRIC_COUNT_SQL)
            .await
            .last()
            .map(|(name, data_type)| (name.clone(), data_type.clone())),
        Some(("pairs".to_string(), arrow_schema::DataType::Int64))
    );
    assert_eq!(
        query_output(&baseline, METRIC_COUNT_SQL).await,
        query_output(&rewritten, METRIC_COUNT_SQL).await
    );

    let avg_sql = "SELECT m.mhost, n.zone, AVG(m.mval * n.wgt) AS score FROM metrics m JOIN nodes n ON m.mhost = n.nhost GROUP BY m.mhost, n.zone";
    let avg_plan = query_plan(&rewritten, avg_sql).await;
    assert!(
        !avg_plan.contains("partial_nv_"),
        "AVG must keep its Float64 payload requirement:\n{avg_plan}"
    );
}

/// A `DECIMAL(38, 0)` payload product is out of range where the flat plan
/// evaluates it, so the count rewrite has to decline the shape: it never
/// multiplies a payload value, and would answer a group the baseline errors on.
#[tokio::test]
async fn decimal_payload_product_keeps_the_plain_plan_and_still_errors() {
    let (metrics, nodes) = decimal_payload_fixture();
    let baseline = engine_over(metrics.clone(), nodes.clone(), false);
    let rewritten = engine_over(metrics, nodes, true);

    let plan = query_plan(&rewritten, METRIC_COUNT_SQL).await;
    assert!(
        !plan.contains("partial_nv_"),
        "COUNT must decline a decimal payload product:\n{plan}"
    );
    assert_eq!(plan, query_plan(&baseline, METRIC_COUNT_SQL).await);

    // The flat plan, kept by the decline, still fails the query the same way.
    let error = query_failure(&rewritten, METRIC_COUNT_SQL).await;
    assert_eq!(error, query_failure(&baseline, METRIC_COUNT_SQL).await);
    assert!(error.contains("Overflow"), "{error}");
}

/// The reconstructed count is an exact `Int64` product that fails the query when
/// it does not fit, instead of wrapping the way a plain `Int64 * Int64` does.
#[tokio::test]
async fn count_product_overflow_fails_instead_of_wrapping() {
    use datafusion_common::Column;
    use datafusion_expr::Operator;
    use datafusion_expr::expr::BinaryExpr;

    let (metrics, nodes) = count_fixture();
    let engine = engine_over(metrics, nodes, true);
    let (input_schema, expression) = count_reconstruction(&engine, METRIC_COUNT_SQL).await;

    let evaluate = |nv: i64, nw: i64, expression: &Expr| {
        let schema = Arc::new(input_schema.as_arrow().clone());
        let columns: Vec<ArrayRef> = input_schema
            .fields()
            .iter()
            .map(|field| match field.name() {
                name if name.contains("partial_nv") => {
                    Arc::new(Int64Array::from(vec![nv])) as ArrayRef
                }
                name if name.contains("partial_nw") => {
                    Arc::new(Int64Array::from(vec![nw])) as ArrayRef
                }
                _ if field.data_type() == &arrow_schema::DataType::Utf8 => {
                    Arc::new(StringArray::from(vec![Some("a")])) as ArrayRef
                }
                other => panic!("unexpected reconstruction input field: {other:?}"),
            })
            .collect();
        let batch = ArrowRecordBatch::try_new(schema, columns).unwrap();
        let physical = create_physical_expr(
            expression,
            &input_schema,
            &ExecutionProps::new(),
            &PhysicalPlanningContext::default(),
        )
        .unwrap();
        let array = physical.evaluate(&batch)?.into_array(1)?;
        ScalarValue::try_from_array(array.as_ref(), 0)
    };

    // Both counts at 2^32 have a product of 2^64, which no `Int64` can hold.
    let error = evaluate(1 << 32, 1 << 32, &expression).expect_err("overflow must fail the query");
    assert!(error.to_string().contains("out of range"), "{error}");

    // A plain `Int64 * Int64` would have wrapped that product to 0 and answered
    // a wrong count; the checked reconstruction errors instead.
    let count_column = |needle: &str| {
        Expr::Column(Column::new_unqualified(
            input_schema
                .fields()
                .iter()
                .find(|field| field.name().contains(needle))
                .expect("partial count column present")
                .name(),
        ))
    };
    let wrapping = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(count_column("partial_nv")),
        Operator::Multiply,
        Box::new(count_column("partial_nw")),
    ));
    assert_eq!(
        evaluate(1 << 32, 1 << 32, &wrapping).unwrap(),
        ScalarValue::Int64(Some(0))
    );

    // In range, the product is exact, and an empty side counts 0, not NULL.
    assert_eq!(
        evaluate(3, 4, &expression).unwrap(),
        ScalarValue::Int64(Some(12))
    );
    assert_eq!(
        evaluate(1 << 31, 1 << 31, &expression).unwrap(),
        ScalarValue::Int64(Some(1 << 62))
    );
    assert_eq!(
        evaluate(0, 1 << 40, &expression).unwrap(),
        ScalarValue::Int64(Some(0))
    );
}

// ---------- Multi-region layouts, hidden partition keys, codec round trip ----------

/// Where the rule put one branch's aggregate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Placement {
    /// The whole aggregate runs inside the region plan. The rule no longer builds
    /// this; the variant exists so a shortcut that ships one is caught.
    Whole,
    /// `__<f>_state` runs in the region plan and `__<f>_merge` above the merge
    /// scan.
    StateMerge,
    /// The aggregate stays on the frontend, above the merge scan.
    Local,
}

/// The two branch plans of a rewritten aggregate, in join order.
fn branch_plans(plan: &LogicalPlan) -> [&LogicalPlan; 2] {
    let LogicalPlan::Projection(projection) = plan else {
        panic!(
            "expected the reconstruction projection:\n{}",
            plan.display_indent()
        );
    };
    let LogicalPlan::Join(join) = projection.input.as_ref() else {
        panic!("expected the rewritten join:\n{}", plan.display_indent());
    };
    [join.left.as_ref(), join.right.as_ref()]
}

/// Where the rule put the aggregate of the branch plan `branch`.
fn placement(branch: &LogicalPlan) -> Placement {
    match branch {
        LogicalPlan::Extension(extension) => {
            assert!(
                extension.node.as_any().is::<MergeScanLogicalPlan>(),
                "expected a merge scan:\n{branch}"
            );
            Placement::Whole
        }
        LogicalPlan::Aggregate(aggregate) => {
            let LogicalPlan::Extension(extension) = aggregate.input.as_ref() else {
                panic!("expected a merge scan below the branch aggregate:\n{branch}");
            };
            assert!(
                extension.node.as_any().is::<MergeScanLogicalPlan>(),
                "expected a merge scan below the branch aggregate:\n{branch}"
            );
            if function_names(aggregate)
                .iter()
                .any(|name| name.starts_with("__"))
            {
                Placement::StateMerge
            } else {
                Placement::Local
            }
        }
        other => panic!("unexpected branch plan: {other}"),
    }
}

/// A table over `batch` that reads the batch's rows and whose metadata names
/// `partition_key_indices` and `extra_phy_cols` as its partition layout.
fn partitioned_rows(
    name: &str,
    batch: &RecordBatch,
    partition_key_indices: Vec<usize>,
    extra_phy_cols: Vec<String>,
) -> TableRef {
    let source = memtable(name, batch);
    let info = source.table_info();
    let meta = TableMetaBuilder::empty()
        .schema(info.meta.schema.clone())
        .primary_key_indices(vec![])
        .value_indices(vec![])
        .engine("mito".to_string())
        .next_column_id(0)
        .options(Default::default())
        .created_on(Default::default())
        .partition_key_indices(partition_key_indices)
        .build()
        .unwrap();
    let info = TableInfoBuilder::default()
        .table_id(info.ident.table_id)
        .table_version(info.ident.version)
        .name(info.name.clone())
        .schema_name(info.schema_name.clone())
        .catalog_name(info.catalog_name.clone())
        .desc(None)
        .table_type(TableType::Base)
        .meta(meta)
        .build()
        .unwrap();
    // The rows are the batch's; only the metadata says the table is partitioned.
    let rules = PartitionRules {
        extra_phy_cols_not_in_logical_table: extra_phy_cols,
    };
    Arc::new(Table::new_partitioned(
        Arc::new(info),
        FilterPushDownType::Unsupported,
        source.data_source(),
        Some(rules),
    ))
}

fn memtable(name: &str, batch: &RecordBatch) -> TableRef {
    MemTable::new_with_catalog(
        name,
        batch.clone(),
        1,
        DEFAULT_CATALOG_NAME.to_string(),
        DEFAULT_SCHEMA_NAME.to_string(),
    )
}

/// A table source over `table`, reached through the catalog adapter.
fn table_source(table: TableRef) -> Arc<dyn TableSource> {
    Arc::new(DefaultTableSource::new(Arc::new(
        DfTableProviderAdapter::new(table),
    )))
}

/// The branch takes the state/merge path even when the group keys cover every
/// *visible* partition column: the table also partitions on a physical column the
/// logical table does not expose, so no group key can cover the whole key.
///
/// With no whole-aggregate path left this holds for every admitted branch, and
/// the assertion is kept explicit so a future shortcut has to face this case.
#[tokio::test]
async fn a_hidden_physical_partition_key_still_takes_the_state_path() {
    let (metrics, nodes) = metrics_and_nodes();
    // `mhost` (column 1) is the fact branch's only group key and is the table's
    // visible partition key; `dc` is a physical partition column the logical
    // table does not have.
    let fact = partitioned_rows("metrics", &metrics, vec![1], vec!["dc".to_string()]);
    let plan = rewrite_branch_shape(table_source(fact), table_source(memtable("nodes", &nodes)));
    let [fact_branch, dim_branch] = branch_plans(&plan);
    assert_eq!(
        placement(fact_branch),
        Placement::StateMerge,
        "a hidden physical partition key must keep the branch on the state path:\n{}",
        plan.display_indent()
    );
    assert_eq!(placement(dim_branch), Placement::StateMerge);
    // The branch really is `__<f>_merge` over a region plan that computes
    // `__<f>_state`, not the aggregate itself.
    let LogicalPlan::Aggregate(merge) = fact_branch else {
        panic!("expected the merge aggregate:\n{}", plan.display_indent());
    };
    assert_function_names(merge, &["__count_merge", "__sum_merge"], "hidden key");
    let LogicalPlan::Extension(extension) = merge.input.as_ref() else {
        panic!("expected a merge scan below the merge aggregate");
    };
    let scan = extension
        .node
        .as_any()
        .downcast_ref::<MergeScanLogicalPlan>()
        .unwrap();
    let LogicalPlan::Aggregate(state) = scan.input() else {
        panic!("expected the state aggregate inside the merge scan");
    };
    assert_function_names(state, &["__count_state", "__sum_state"], "hidden key");
}

/// Fact rows as (host, value), with one row per entry.
fn fact_rows(rows: &[(Option<&str>, Option<f64>)]) -> RecordBatch {
    RecordBatch::new(
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("mhost", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("mval", ConcreteDataType::float64_datatype(), true),
        ])),
        vec![
            Arc::new(TimestampMillisecondVector::from_slice(vec![0; rows.len()])) as _,
            Arc::new(StringVector::from(
                rows.iter().map(|(host, _)| *host).collect::<Vec<_>>(),
            )) as _,
            Arc::new(Float64Vector::from(
                rows.iter().map(|(_, value)| *value).collect::<Vec<_>>(),
            )) as _,
        ],
    )
    .unwrap()
}

/// A single dim row for `m1`, so every fact group joins exactly once.
fn single_node_row() -> RecordBatch {
    RecordBatch::new(
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("nhost", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("zone", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("wgt", ConcreteDataType::float64_datatype(), true),
        ])),
        vec![
            Arc::new(TimestampMillisecondVector::from_slice(vec![0])) as _,
            Arc::new(StringVector::from(vec![Some("m1")])) as _,
            Arc::new(StringVector::from(vec![Some("west")])) as _,
            Arc::new(Float64Vector::from(vec![Some(2.0)])) as _,
        ],
    )
    .unwrap()
}

/// `schema`'s first three fields as columns: the group key and the two branch
/// statistics.
fn group_and_statistics(schema: &DFSchema) -> [datafusion_common::Column; 3] {
    [0, 1, 2].map(|index| {
        let (qualifier, field) = schema.qualified_field(index);
        datafusion_common::Column::new(qualifier.cloned(), field.name())
    })
}

/// Joins `branch` - one row per fact group - to the single dim row and projects
/// the group key with the branch's two statistics.
fn join_single_node(branch: LogicalPlan) -> LogicalPlan {
    use datafusion::logical_expr::LogicalPlanBuilder;

    let [group, first, second] = group_and_statistics(branch.schema());
    let dim = LogicalPlanBuilder::scan(
        "nodes",
        table_source(memtable("nodes", &single_node_row())),
        None,
    )
    .unwrap()
    .alias("n")
    .unwrap()
    .build()
    .unwrap();
    LogicalPlanBuilder::from(branch)
        .join(
            dim,
            datafusion_expr::JoinType::Inner,
            (
                vec![group.clone()],
                vec![datafusion_common::Column::new_unqualified("nhost")],
            ),
            None,
        )
        .unwrap()
        .project(vec![
            Expr::Column(group),
            Expr::Column(first),
            Expr::Column(second),
        ])
        .unwrap()
        .build()
        .unwrap()
}

/// One region running `aggregate`'s group and aggregate expressions over `rows`.
fn region_aggregate(aggregate: &Aggregate, table: &str, rows: &RecordBatch) -> LogicalPlan {
    use datafusion::logical_expr::LogicalPlanBuilder;

    let scan = LogicalPlanBuilder::scan(table, table_source(memtable(table, rows)), None)
        .unwrap()
        .alias("m")
        .unwrap()
        .build()
        .unwrap();
    LogicalPlan::Aggregate(
        Aggregate::try_new(
            Arc::new(scan),
            aggregate.group_expr.clone(),
            aggregate.aggr_expr.clone(),
        )
        .unwrap(),
    )
}

/// One region running the whole `SUM(mval)`/`COUNT(mval)` aggregate over `rows`,
/// which is what the rule used to put in the region plan.
fn whole_region_aggregate(table: &str, rows: &RecordBatch) -> LogicalPlan {
    use datafusion::functions_aggregate::expr_fn::{count, sum};
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion_expr::col;

    let scan = LogicalPlanBuilder::scan(table, table_source(memtable(table, rows)), None)
        .unwrap()
        .alias("m")
        .unwrap()
        .build()
        .unwrap();
    LogicalPlan::Aggregate(
        Aggregate::try_new(
            Arc::new(scan),
            vec![col("m.mhost")],
            vec![
                count(col("m.mval")).alias("partial_nv_1"),
                sum(col("m.mval")).alias("partial_sv_2"),
            ],
        )
        .unwrap(),
    )
}

/// `merge` re-bound to `input`, with its group keys taken by position.
///
/// The schemas on both sides of the codec and of a concatenation are not
/// guaranteed to carry the qualifiers the rewrite wrote - a decoded plan binds
/// the grouping column to the table, a union derives its own schema - while the
/// columns are in the same order, so position is what identifies them.
fn rebind_merge(merge: &Aggregate, input: LogicalPlan) -> Aggregate {
    let group_expr = (0..merge.group_expr.len())
        .map(|index| {
            let (qualifier, field) = input.schema().qualified_field(index);
            Expr::Column(datafusion_common::Column::new(
                qualifier.cloned(),
                field.name(),
            ))
        })
        .collect::<Vec<_>>();
    Aggregate::try_new(Arc::new(input), group_expr, merge.aggr_expr.clone()).unwrap()
}

/// Concatenates `plans` the way a merge scan concatenates the rows its regions
/// returned.
fn concatenate(plans: Vec<LogicalPlan>) -> LogicalPlan {
    use datafusion_expr::logical_plan::Union;

    LogicalPlan::Union(
        Union::try_new_with_loose_types(plans.into_iter().map(Arc::new).collect()).unwrap(),
    )
}

/// Runs `plan` in a bare session: it is a plain DataFusion plan, so it needs no
/// region handler.
async fn plan_rows(plan: LogicalPlan) -> Vec<Vec<String>> {
    use datafusion::dataframe::DataFrame;
    use datafusion::execution::SessionStateBuilder;

    let state = SessionStateBuilder::new().with_default_features().build();
    let batches = DataFrame::new(state, plan).collect().await.unwrap();
    arrow_rows(&batches)
}

/// The decisive layout test: the fact rows of one join key are spread over two
/// regions, and the frontend must still emit one row per group, with the counts
/// and sums of both.
///
/// A real two-region route is not expressible in this fixture: a
/// [`MergeScanLogicalPlan`] holds one region plan, and the memory catalog gives
/// the planner no route to fan it out over. The rig therefore builds what
/// `MergeScanExec` returns for two regions - the concatenation of the two region
/// plans' rows - and applies the frontend half of the plan the rule produced.
/// That concatenation is where a group split across regions would show up, so it
/// is what the state/merge placement is checked against, and what a whole
/// pushdown is shown to get wrong on the same rows.
#[tokio::test]
async fn per_region_states_merge_into_one_row_per_group() {
    // m1 is in both regions, and one of its rows has a NULL payload.
    let region_a = fact_rows(&[(Some("m1"), Some(1.0)), (Some("m1"), None)]);
    let region_b = fact_rows(&[(Some("m1"), Some(2.0)), (Some("m2"), Some(4.0))]);
    let all_rows = fact_rows(&[
        (Some("m1"), Some(1.0)),
        (Some("m1"), None),
        (Some("m1"), Some(2.0)),
        (Some("m2"), Some(4.0)),
    ]);
    let engine = engine_over_with(all_rows, single_node_row(), true);
    let plan = optimized_logical_for(&engine, METRIC_AVG_SQL).await;
    let [fact_branch, _dim_branch] = branch_plans(&plan);
    assert_eq!(
        placement(fact_branch),
        Placement::StateMerge,
        "{}",
        plan.display_indent()
    );
    let LogicalPlan::Aggregate(merge) = fact_branch else {
        panic!("expected the merge aggregate:\n{}", plan.display_indent());
    };
    let LogicalPlan::Extension(extension) = merge.input.as_ref() else {
        panic!("expected a merge scan below the merge aggregate");
    };
    let scan = extension
        .node
        .as_any()
        .downcast_ref::<MergeScanLogicalPlan>()
        .unwrap();
    let LogicalPlan::Aggregate(state) = scan.input() else {
        panic!("expected the state aggregate inside the merge scan");
    };

    // Two regions, concatenated the way the merge scan concatenates them, and
    // merged the way the frontend does: one row per group.
    let regions = concatenate(vec![
        region_aggregate(state, "metrics", &region_a),
        region_aggregate(state, "metrics", &region_b),
    ]);
    let merged = rebind_merge(merge, regions);
    assert_eq!(
        plan_rows(join_single_node(LogicalPlan::Aggregate(merged))).await,
        vec![vec!["m1".to_string(), "2".to_string(), "3".to_string()]]
    );

    // The whole pushdown the single-region guard used to take is wrong on the
    // same layout: two whole per-region aggregates, concatenated, pass the join
    // twice for m1.
    let concatenated = concatenate(vec![
        whole_region_aggregate("metrics", &region_a),
        whole_region_aggregate("metrics", &region_b),
    ]);
    assert_eq!(
        plan_rows(join_single_node(concatenated)).await,
        vec![
            vec!["m1".to_string(), "1".to_string(), "1".to_string()],
            vec!["m1".to_string(), "1".to_string(), "2".to_string()],
        ]
    );
}

/// An aggregate output field that carries a qualifier keeps it: the
/// reconstruction projection must not turn `q.pairs` into an unqualified
/// `pairs`, or a parent reading `q.pairs` stops resolving.
#[test]
fn qualified_aggregate_output_keeps_its_qualifier() {
    use datafusion::functions_aggregate::expr_fn::count;
    use datafusion_expr::{LogicalPlanBuilder, col};
    use table::table::adapter::DfTableProviderAdapter;
    use table::test_util::MemTable;

    let (metrics, nodes) = metrics_and_nodes();
    let table_source = |name: &str, batch: &RecordBatch| {
        Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(MemTable::new_with_catalog(
                name,
                batch.clone(),
                1,
                "greptime".to_string(),
                "public".to_string(),
            )),
        ))) as Arc<dyn datafusion::logical_expr::TableSource>
    };
    let plan = rewrite_shape_with(
        table_source("metrics", &metrics),
        table_source("nodes", &nodes),
        vec![count(col("metrics.mval") * col("nodes.wgt")).alias_qualified(Some("q"), "pairs")],
    );
    let LogicalPlan::Projection(projection) = &plan else {
        panic!(
            "expected the reconstruction projection:\n{}",
            plan.display_indent()
        );
    };
    let (qualifier, field) = projection.schema.qualified_field(2);
    assert_eq!(field.name(), "pairs");
    assert_eq!(qualifier.map(ToString::to_string), Some("q".to_string()));
    // A parent that reads the qualified field still resolves it.
    LogicalPlanBuilder::from(plan)
        .project(vec![col("q.pairs")])
        .unwrap();
}

/// Builds the narrow `AVG(fact * dim)` branch shape over two table sources,
/// one merge scan per side, and runs the factorized rule over it.
fn rewrite_branch_shape(
    fact: Arc<dyn datafusion::logical_expr::TableSource>,
    dim: Arc<dyn datafusion::logical_expr::TableSource>,
) -> LogicalPlan {
    use datafusion::functions_aggregate::expr_fn::avg;
    use datafusion_expr::col;

    rewrite_shape_with(
        fact,
        dim,
        vec![avg(col("metrics.mval") * col("nodes.wgt")).alias("score")],
    )
}

/// [`rewrite_branch_shape`] with the aggregate expressions given, so a test can
/// build a differently aliased output field.
fn rewrite_shape_with(
    fact: Arc<dyn datafusion::logical_expr::TableSource>,
    dim: Arc<dyn datafusion::logical_expr::TableSource>,
    aggr: Vec<Expr>,
) -> LogicalPlan {
    use datafusion_expr::{LogicalPlanBuilder, col};
    use datafusion_optimizer::OptimizerRule;

    use crate::dist_plan::MergeScanLogicalPlan;
    use crate::optimizer::factorized_join_agg::FactorizedJoinAggRule;

    let branch = |source: Arc<dyn datafusion::logical_expr::TableSource>, alias: &str| {
        let scan = LogicalPlanBuilder::scan(alias, source, None)
            .unwrap()
            .alias(alias)
            .unwrap()
            .build()
            .unwrap();
        MergeScanLogicalPlan::new(scan, false).into_logical_plan()
    };
    let join = LogicalPlanBuilder::from(branch(fact, "metrics"))
        .join(
            branch(dim, "nodes"),
            datafusion_expr::JoinType::Inner,
            (vec!["metrics.mhost"], vec!["nodes.nhost"]),
            None,
        )
        .unwrap();
    let aggregate = join
        .aggregate(vec![col("metrics.mhost"), col("nodes.zone")], aggr)
        .unwrap()
        .build()
        .unwrap();
    let mut options = datafusion::config::ConfigOptions::default();
    options
        .extensions
        .insert(crate::options::FactorizedJoinAggOptions { enabled: true });
    let config = datafusion_optimizer::OptimizerContext::new_with_config_options(Arc::new(options));
    let transformed = FactorizedJoinAggRule.rewrite(aggregate, &config).unwrap();
    assert!(
        transformed.transformed,
        "the rule did not rewrite the shape"
    );
    transformed.data
}

/// The state plan a region runs survives the codec a frontend ships it through:
/// it is encoded, decoded with the datanode's decoder and executed, and merging
/// the decoded states answers what the frontend's branch aggregate computes over
/// the same rows.
#[tokio::test]
async fn region_states_survive_the_codec_round_trip() {
    use catalog::table_source::dummy_catalog::DummyCatalogList;
    use common_query::logical_plan::SubstraitPlanDecoder;
    use datafusion::dataframe::DataFrame;
    use datafusion::execution::SessionStateBuilder;
    use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};

    use crate::query_engine::{DefaultPlanDecoder, DefaultSerializer};

    let (metrics, nodes) = metrics_and_nodes();
    let engine = engine_over_with(metrics.clone(), nodes.clone(), true);
    let plan = optimized_logical_for(&engine, METRIC_AVG_SQL).await;
    let remotes = remote_inputs(&plan);
    assert_eq!(remotes.len(), 2, "{}", plan.display_indent());
    let merges = aggregates_above_merge_scan(&plan);
    assert_eq!(merges.len(), 2, "{}", plan.display_indent());

    let query_ctx = QueryContext::arc();
    let catalog_list: Arc<dyn datafusion::catalog::CatalogProviderList> =
        Arc::new(DummyCatalogList::new_with_query_ctx(
            engine.engine_state().catalog_manager().clone(),
            query_ctx.clone(),
        ));
    // Region plans name their table without a catalog, so the decoding session
    // has to default to the one the tables live in.
    let mut config = datafusion::execution::context::SessionConfig::new()
        .with_default_catalog_and_schema(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME);
    config.set_extension(query_ctx.clone());
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(config)
        .build();
    let decoder = DefaultPlanDecoder::new(state.clone(), &query_ctx).unwrap();
    // What the frontend's branch aggregate computes over the same rows, as plain
    // aggregate queries.
    let local = engine_over(metrics, nodes, false);
    let references = [
        "SELECT mhost, COUNT(mval) AS nv, SUM(mval) AS sv FROM metrics \
         WHERE mhost IS NOT NULL GROUP BY mhost",
        "SELECT nhost, zone, COUNT(wgt) AS nw, SUM(wgt) AS sw FROM nodes \
         WHERE nhost IS NOT NULL GROUP BY nhost, zone",
    ];
    for ((remote, merge), reference) in remotes.iter().zip(&merges).zip(references) {
        assert!(
            matches!(remote, LogicalPlan::Aggregate(aggregate)
                if function_names(aggregate).iter().any(|name| name.starts_with("__"))),
            "the region plan does not compute states:\n{}",
            plan.display_indent()
        );
        let bytes = DFLogicalSubstraitConvertor
            .encode(remote, DefaultSerializer)
            .unwrap();
        let decoded = decoder
            .decode(bytes, catalog_list.clone(), false)
            .await
            .unwrap();
        assert!(
            decoded.to_string().contains("__count_state"),
            "the decoded plan lost the state aggregate:\n{decoded}"
        );
        // Merging the decoded states is all the frontend asks of the region plan.
        let merged = rebind_merge(merge, decoded);
        let batches = DataFrame::new(state.clone(), LogicalPlan::Aggregate(merged))
            .collect()
            .await
            .unwrap();
        assert_eq!(arrow_rows(&batches), query_output(&local, reference).await);
    }
}

/// Rows of arrow batches as strings, sorted the way `query_output` reports
/// them, so the two can be compared directly.
fn arrow_rows(batches: &[arrow::record_batch::RecordBatch]) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for batch in batches {
        let vectors = datatypes::vectors::Helper::try_into_vectors(batch.columns()).unwrap();
        for i in 0..batch.num_rows() {
            let row: Vec<String> = vectors
                .iter()
                .map(|v| datatypes::value::Value::from(v.get_ref(i)).to_string())
                .collect();
            rows.push(row);
        }
    }
    rows.sort();
    rows
}
