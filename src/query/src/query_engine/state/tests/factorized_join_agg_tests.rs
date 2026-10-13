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

use catalog::RegisterTableRequest;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_recordbatch::RecordBatch;
use datatypes::prelude::*;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::{Float64Vector, StringVector, TimestampMillisecondVector};
use session::context::QueryContext;
use table::test_util::MemTable;

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

async fn optimized_plan(engine: &crate::query_engine::QueryEngineRef) -> String {
    let context = QueryContext::arc();
    let statement = QueryLanguageParser::parse_sql(
        "SELECT f.host, d.area, AVG(f.value * d.weight) AS score FROM fact f JOIN dim d ON f.host = d.host GROUP BY f.host, d.area",
        &context,
    )
    .unwrap();
    let plan = engine.planner().plan(&statement, context).await.unwrap();
    let plan = engine.engine_state().optimize_logical_plan(plan).unwrap();
    plan.display_indent().to_string()
}

#[tokio::test]
async fn factorized_join_agg_matches_with_dist_planner() {
    let baseline = optimized_plan(&engine(false, true)).await;
    let rewritten = optimized_plan(&engine(true, true)).await;
    assert_ne!(baseline, rewritten, "{rewritten}");
    assert!(rewritten.contains("partial_nv"), "{rewritten}");
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
        false,
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

/// The per-query session config that `engine_context` builds is where the
/// rewrite reads its switch, so the contract is asserted on that state
/// directly (the logical `optimize_logical_plan` path never sees SET).
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

    // Session SET false: the same state now reports disabled.
    ctx.configuration_parameter()
        .set_experimental_enable_factorized_join_agg(false);
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

    // Session SET true on a server-OFF engine: enabled.
    let (metrics2, nodes2) = metrics_and_nodes();
    let engine_off = engine_over(metrics2, nodes2, false);
    let df_engine_off = engine_off
        .as_any()
        .downcast_ref::<crate::datafusion::DatafusionQueryEngine>()
        .unwrap();
    let ctx2 = QueryContext::arc();
    ctx2.configuration_parameter()
        .set_experimental_enable_factorized_join_agg(true);
    let enabled = df_engine_off
        .engine_context_for_tests(ctx2)
        .state()
        .config()
        .options()
        .extensions
        .get::<FactorizedJoinAggOptions>()
        .expect("extension present after SET true")
        .enabled;
    assert!(enabled);
}
