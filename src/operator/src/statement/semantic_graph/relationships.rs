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

//! The `semantic_relationships` derivation: the plan builders for every edge
//! branch (trace-derived `calls`, the declared-edge union) behind the computed
//! table. Shared expression helpers and the entity registry live in the parent
//! module.

use common_catalog::consts::{
    DURATION_NANO_COLUMN, PARENT_SPAN_ID_COLUMN, SPAN_ID_COLUMN, SPAN_KIND_CLIENT,
    SPAN_KIND_COLUMN, SPAN_KIND_SERVER, SPAN_STATUS_CODE_COLUMN, SPAN_STATUS_ERROR,
    TRACE_ID_COLUMN, TRACE_TIMESTAMP_COLUMN,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::dataframe::DataFrame;
use datafusion::functions::core as core_fns;
use datafusion::functions_aggregate::expr_fn::{count, sum};
use datafusion::functions_window::expr_fn::row_number;
use datafusion_common::Result as DfResult;
use datafusion_expr::{ExprFunctionExt, JoinType, LogicalPlan, cast, ident, lit};

use crate::statement::semantic_graph::{
    DECLARED_EDGE_IDENTITY_COLUMNS, EntityDeclaration, GraphQueryWindow, OBSERVED_AT_COLUMN,
    bin_interval, bin_ms, entity_id_expr, interval, null_json, qcol, union_all,
};

/// The projected columns of `semantic_relationships`, in order. Every derived
/// branch and the declared-edge branch must project exactly these so the
/// top-level `UNION ALL` type-aligns; `build_relationships_plan` re-selects
/// them over the union to enforce the contract. (The physical declared table
/// additionally stores `valid_from`/`valid_until`, which feed the validity
/// filter and the projected window columns.)
const RELATIONSHIP_COLUMNS: [&str; 16] = [
    "observed_at",
    "window_start",
    "window_end",
    "fresh_until",
    "src_type",
    "src_id",
    "dst_type",
    "dst_id",
    "rel_type",
    "provenance",
    "confidence",
    "request_count",
    "error_count",
    "duration_sum",
    "duration_count",
    "attributes",
];

/// A child server span starts no earlier than 5 minutes before its client span
/// (clock-skew allowance) and no later than 1 hour after it; the bounds keep the
/// join windowed instead of pairing arbitrarily distant spans of a long-lived
/// trace.
const CHILD_SPAN_EARLY_NANOS: i64 = 5 * 60 * 1_000_000_000;
const CHILD_SPAN_LATE_NANOS: i64 = 60 * 60 * 1_000_000_000;

/// A trace table's scan paired with the `service` declaration it derives
/// `calls` edges for — a unit of `build_relationships_plan`.
pub struct CallsSource {
    pub service: EntityDeclaration,
    pub scan: DataFrame,
}

/// The declared-edge table's scan (`semantic_relationships_declared`), whose
/// rows `build_relationships_plan` unions into the edge set.
pub struct DeclaredSource {
    pub scan: DataFrame,
}

/// Builds the `semantic_relationships` plan: the trace-derived `calls` branch
/// unioned with the declared-edge branch, re-projected to the 16-column
/// contract. Returns `None` when there is neither a trace table nor a
/// declared-edge table, so the computed table streams empty.
pub fn build_relationships_plan(
    traces: Vec<CallsSource>,
    declared: Option<DeclaredSource>,
    window: &GraphQueryWindow,
) -> DfResult<Option<LogicalPlan>> {
    let mut union_df = calls_branch(traces, window)?;
    if let Some(declared) = declared {
        union_df = union_all(union_df, declared_edges(declared.scan, window)?)?;
    }
    union_df
        .map(|df| {
            Ok(df
                .select_columns(&RELATIONSHIP_COLUMNS)?
                .into_unoptimized_plan())
        })
        .transpose()
}

/// Ranking column used to pick the latest declared-edge revision per edge key.
const DECLARED_REVISION_COLUMN: &str = "__declared_revision";

/// The declared-edge branch: the latest revision per edge key *as of the
/// queried window* (mito dedups on primary key + `observed_at`, so re-asserting
/// an edge stores a new revision), filtered by business-validity overlap.
///
/// `valid_from` defaults to the declaration time; a NULL `valid_until` means
/// the edge holds while its row exists (TTL retires it), so `window_end` /
/// `fresh_until` take the window's upper bound. The output `observed_at` is
/// synthesized inside the queried range: the scan's filters are re-applied
/// above the computed table, and the physical revision time would fail them.
fn declared_edges(scan: DataFrame, window: &GraphQueryWindow) -> DfResult<DataFrame> {
    let eff_valid_from =
        core_fns::coalesce().call(vec![ident("valid_from"), ident(OBSERVED_AT_COLUMN)]);
    let eff_valid_until =
        core_fns::coalesce().call(vec![ident("valid_until"), window.observed_end()]);

    // As-of the queried window: a revision recorded after its end, or whose
    // validity starts after it, was not the edge's state inside the window and
    // must not outrank (and thereby hide) the revision that was.
    let as_of = ident(OBSERVED_AT_COLUMN)
        .lt(window.observed_end())
        .and(eff_valid_from.clone().lt(window.observed_end()));

    let revision = row_number()
        .partition_by(
            DECLARED_EDGE_IDENTITY_COLUMNS
                .iter()
                .map(|c| ident(*c))
                .collect(),
        )
        // generation_id/scope break observed_at ties deterministically (rows
        // differing only in them are not merged by the storage dedup).
        .order_by(vec![
            ident(OBSERVED_AT_COLUMN).sort(false, false),
            ident("generation_id").sort(false, false),
            ident("scope").sort(false, false),
        ])
        .build()?
        .alias(DECLARED_REVISION_COLUMN);

    // The as-of filter already bounds eff_valid_from below the window's end;
    // only the retirement side of the overlap remains to check.
    let still_valid = ident("valid_until")
        .is_null()
        .or(ident("valid_until").gt(window.observed_start()));
    // Tags come out of the storage engine dictionary-encoded; cast to plain
    // strings so the union with the derived branches type-aligns.
    let tag_utf8 = |name: &str| cast(ident(name), DataType::Utf8).alias(name);

    scan.filter(as_of)?
        .window(vec![revision])?
        .filter(ident(DECLARED_REVISION_COLUMN).eq(lit(1_u64)))?
        .filter(still_valid)?
        .select(vec![
            core_fns::greatest()
                .call(vec![eff_valid_from.clone(), window.observed_start()])
                .alias(OBSERVED_AT_COLUMN),
            eff_valid_from.alias("window_start"),
            eff_valid_until.clone().alias("window_end"),
            eff_valid_until.alias("fresh_until"),
            tag_utf8("src_type"),
            tag_utf8("src_id"),
            tag_utf8("dst_type"),
            tag_utf8("dst_id"),
            tag_utf8("rel_type"),
            tag_utf8("provenance"),
            ident("confidence"),
            ident("request_count"),
            ident("error_count"),
            ident("duration_sum"),
            ident("duration_count"),
            ident("attributes"),
        ])
}

/// The `calls` derivation: pair each client span with its child server span on
/// `trace_id` + `parent_span_id`, project to `service`, union the pairs of all
/// trace tables, and aggregate RED metrics per 60s window in one pass — an edge
/// observed across several trace tables yields one row, not per-table
/// fragments (the plan form of the Tempo servicegraph connector). Column names
/// are the fixed `greptime_trace_v1` schema, the reason
/// `table_data_model = greptime_trace_v1` is required. Returns `None` when
/// there is no trace table.
fn calls_branch(
    traces: Vec<CallsSource>,
    window: &GraphQueryWindow,
) -> DfResult<Option<DataFrame>> {
    let mut union_df: Option<DataFrame> = None;
    for trace in traces {
        union_df = union_all(union_df, calls_pairs(&trace.service, trace.scan, window)?)?;
    }
    let Some(pairs) = union_df else {
        return Ok(None);
    };

    let df = pairs
        .aggregate(
            vec![ident("observed_at"), ident("src_id"), ident("dst_id")],
            vec![
                count(lit(1)).alias("request_count"),
                count(lit(1))
                    .filter(ident("status_code").eq(lit(SPAN_STATUS_ERROR)))
                    .build()?
                    .alias("error_count"),
                sum(ident("duration_nano")).alias("duration_nano_sum"),
            ],
        )?
        .select(vec![
            ident("observed_at"),
            ident("observed_at").alias("window_start"),
            (ident("observed_at") + bin_interval()).alias("window_end"),
            (ident("observed_at") + bin_interval()).alias("fresh_until"),
            lit("service").alias("src_type"),
            ident("src_id"),
            lit("service").alias("dst_type"),
            ident("dst_id"),
            lit("calls").alias("rel_type"),
            lit("trace").alias("provenance"),
            lit(1.0_f64).alias("confidence"),
            ident("request_count"),
            ident("error_count"),
            // duration_nano sums in nanoseconds; the contract column is seconds.
            (cast(ident("duration_nano_sum"), DataType::Float64) / lit(1e9_f64))
                .alias("duration_sum"),
            ident("request_count").alias("duration_count"),
            null_json().alias("attributes"),
        ])?;
    Ok(Some(df))
}

/// One trace table's client/server span pairs, projected to the aggregation
/// inputs `(observed_at, src_id, dst_id, status_code, duration_nano)`.
/// Endpoint ids are built from the table's `service` entity declaration, so
/// edges land on exactly the entity ids the registry emits (a composite
/// service identity renders the same sorted `k=v` form on both sides).
fn calls_pairs(
    service: &EntityDeclaration,
    trace: DataFrame,
    window: &GraphQueryWindow,
) -> DfResult<DataFrame> {
    let mut client_pred = ident(SPAN_KIND_COLUMN)
        .eq(lit(SPAN_KIND_CLIENT))
        .and(ident(TRACE_TIMESTAMP_COLUMN).gt_eq(window.source_start()))
        .and(ident(TRACE_TIMESTAMP_COLUMN).lt(window.source_end()));
    let mut server_pred = ident(SPAN_KIND_COLUMN)
        .eq(lit(SPAN_KIND_SERVER))
        // Static bounds implied by the window and the join's time-proximity
        // conditions below; the join bounds reference client.timestamp and
        // cannot prune the server-side scan.
        .and(
            ident(TRACE_TIMESTAMP_COLUMN)
                .gt_eq(window.source_start() - interval(CHILD_SPAN_EARLY_NANOS)),
        )
        .and(
            ident(TRACE_TIMESTAMP_COLUMN).lt(window.source_end() + interval(CHILD_SPAN_LATE_NANOS)),
        );
    // A NULL identity component identifies nothing, on either endpoint.
    for id in &service.id_columns {
        client_pred = client_pred.and(ident(id).is_not_null());
        server_pred = server_pred.and(ident(id).is_not_null());
    }

    let client = trace.clone().filter(client_pred)?.alias("client")?;
    let server = trace.filter(server_pred)?.alias("server")?;

    let join_conditions = vec![
        qcol("client", TRACE_ID_COLUMN).eq(qcol("server", TRACE_ID_COLUMN)),
        qcol("server", PARENT_SPAN_ID_COLUMN).eq(qcol("client", SPAN_ID_COLUMN)),
        qcol("server", TRACE_TIMESTAMP_COLUMN)
            .gt_eq(qcol("client", TRACE_TIMESTAMP_COLUMN) - interval(CHILD_SPAN_EARLY_NANOS)),
        qcol("server", TRACE_TIMESTAMP_COLUMN)
            .lt_eq(qcol("client", TRACE_TIMESTAMP_COLUMN) + interval(CHILD_SPAN_LATE_NANOS)),
    ];

    client
        .join_on(server, JoinType::Inner, join_conditions)?
        .select(vec![
            bin_ms(qcol("client", TRACE_TIMESTAMP_COLUMN)).alias("observed_at"),
            // The cast inside entity_id_expr also normalizes tag columns, which
            // come out of the storage engine dictionary-encoded.
            entity_id_expr(&service.id_columns, &|c| qcol("client", c)).alias("src_id"),
            entity_id_expr(&service.id_columns, &|c| qcol("server", c)).alias("dst_id"),
            qcol("server", SPAN_STATUS_CODE_COLUMN).alias("status_code"),
            qcol("server", DURATION_NANO_COLUMN).alias("duration_nano"),
        ])?
        // Exclude self-calls on the composed identity: an edge needs two
        // distinct service entities.
        .filter(ident("src_id").not_eq(ident("dst_id")))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_catalog::consts::{SEMANTIC_RELATIONSHIPS_DECLARED_TABLE_NAME, SERVICE_NAME_COLUMN};
    use datafusion::arrow::array::{
        ArrayRef, BinaryArray, Float64Array, Int64Array, StringArray, TimestampMillisecondArray,
        TimestampNanosecondArray, UInt64Array,
    };
    use datafusion::arrow::datatypes::{Field, Schema, TimeUnit};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    use super::super::test_util::{collect, json_texts, strings, ts_values};
    use super::*;

    fn test_window() -> GraphQueryWindow {
        GraphQueryWindow::from_observed(0, 10 * 60 * 1000)
    }

    /// A trace-like table in the fixed `greptime_trace_v1` shape (ns timestamps).
    fn trace_table_ctx() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                TRACE_TIMESTAMP_COLUMN,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(TRACE_ID_COLUMN, DataType::Utf8, false),
            Field::new(SPAN_ID_COLUMN, DataType::Utf8, false),
            Field::new(PARENT_SPAN_ID_COLUMN, DataType::Utf8, true),
            Field::new(SPAN_KIND_COLUMN, DataType::Utf8, false),
            Field::new(SPAN_STATUS_CODE_COLUMN, DataType::Utf8, false),
            Field::new(SERVICE_NAME_COLUMN, DataType::Utf8, false),
            Field::new("service_namespace", DataType::Utf8, false),
            Field::new(DURATION_NANO_COLUMN, DataType::UInt64, false),
        ]));
        const MS: i64 = 1_000_000;
        // Two client->server pairs frontend->cart (one errored), one pair
        // cart->cart (self-call, excluded), one unmatched client span.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_000 * MS, // client frontend->cart
                    1_010 * MS, //   server cart
                    2_000 * MS, // client frontend->cart (error)
                    2_010 * MS, //   server cart (error)
                    3_000 * MS, // client cart->cart (self-call)
                    3_010 * MS, //   server cart
                    4_000 * MS, // client with no matching server
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    "t1", "t1", "t2", "t2", "t3", "t3", "t4",
                ])),
                Arc::new(StringArray::from(vec![
                    "c1", "s1", "c2", "s2", "c3", "s3", "c4",
                ])),
                Arc::new(StringArray::from(vec![
                    None,
                    Some("c1"),
                    None,
                    Some("c2"),
                    None,
                    Some("c3"),
                    None,
                ])),
                Arc::new(StringArray::from(vec![
                    "SPAN_KIND_CLIENT",
                    "SPAN_KIND_SERVER",
                    "SPAN_KIND_CLIENT",
                    "SPAN_KIND_SERVER",
                    "SPAN_KIND_CLIENT",
                    "SPAN_KIND_SERVER",
                    "SPAN_KIND_CLIENT",
                ])),
                Arc::new(StringArray::from(vec![
                    "STATUS_CODE_UNSET",
                    "STATUS_CODE_UNSET",
                    "STATUS_CODE_UNSET",
                    "STATUS_CODE_ERROR",
                    "STATUS_CODE_UNSET",
                    "STATUS_CODE_UNSET",
                    "STATUS_CODE_UNSET",
                ])),
                Arc::new(StringArray::from(vec![
                    "frontend", "cart", "frontend", "cart", "cart", "cart", "frontend",
                ])),
                Arc::new(StringArray::from(vec!["ns1"; 7])),
                Arc::new(UInt64Array::from(vec![
                    0,
                    500_000_000, // 0.5s
                    0,
                    1_500_000_000, // 1.5s
                    0,
                    100,
                    0,
                ])),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table(
            "opentelemetry_traces",
            Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch.clone()]]).unwrap()),
        )
        .unwrap();
        ctx.register_table(
            "opentelemetry_traces_2",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
        ctx
    }

    fn trace_service_decl(id_columns: &[&str]) -> EntityDeclaration {
        EntityDeclaration {
            schema: "public".to_string(),
            table: "opentelemetry_traces".to_string(),
            time_index: TRACE_TIMESTAMP_COLUMN.to_string(),
            entity_type: "service".to_string(),
            id_columns: id_columns.iter().map(|s| s.to_string()).collect(),
            descriptive_columns: vec![],
            scope_columns: vec![],
        }
    }

    #[tokio::test]
    async fn calls_plan_aggregates_red_metrics() {
        let ctx = trace_table_ctx();
        let trace = ctx.table("opentelemetry_traces").await.unwrap();
        let plan = build_relationships_plan_for_test(
            vec![CallsSource {
                service: trace_service_decl(&[SERVICE_NAME_COLUMN]),
                scan: trace,
            }],
            &test_window(),
        )
        .unwrap()
        .unwrap();

        let names = plan
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, RELATIONSHIP_COLUMNS);

        let batches = collect(&ctx, plan).await;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        // frontend->cart only: the self-call and the unmatched client drop out;
        // both pairs land in the same 60s bin.
        assert_eq!(total, 1);
        let batch = &batches[0];
        assert_eq!(strings(batch, 5), vec!["frontend"]);
        assert_eq!(strings(batch, 7), vec!["cart"]);
        assert_eq!(strings(batch, 8), vec!["calls"]);
        assert_eq!(strings(batch, 9), vec!["trace"]);

        let request_count = batch
            .column(11)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(request_count.value(0), 2);
        let error_count = batch
            .column(12)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(error_count.value(0), 1);
        let duration_sum = batch
            .column(13)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((duration_sum.value(0) - 2.0).abs() < 1e-9);
        let duration_count = batch
            .column(14)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(duration_count.value(0), 2);
        // Derived calls edges carry no attributes: a typed-JSON NULL.
        assert!(json_texts(batch, 15)[0].is_none());
    }

    #[tokio::test]
    async fn calls_plan_merges_edges_across_trace_tables() {
        let ctx = trace_table_ctx();
        let t1 = ctx.table("opentelemetry_traces").await.unwrap();
        let t2 = ctx.table("opentelemetry_traces_2").await.unwrap();
        let plan = build_relationships_plan_for_test(
            vec![
                CallsSource {
                    service: trace_service_decl(&[SERVICE_NAME_COLUMN]),
                    scan: t1,
                },
                CallsSource {
                    service: trace_service_decl(&[SERVICE_NAME_COLUMN]),
                    scan: t2,
                },
            ],
            &test_window(),
        )
        .unwrap()
        .unwrap();

        let batches = collect(&ctx, plan).await;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        // The same frontend->cart edge from both tables folds into one row with
        // summed RED metrics.
        assert_eq!(total, 1);
        let batch = &batches[0];
        let request_count = batch
            .column(11)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(request_count.value(0), 4);
        let error_count = batch
            .column(12)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(error_count.value(0), 2);
        let duration_sum = batch
            .column(13)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((duration_sum.value(0) - 4.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn calls_endpoints_follow_service_declaration() {
        let ctx = trace_table_ctx();
        let trace = ctx.table("opentelemetry_traces").await.unwrap();
        let plan = build_relationships_plan_for_test(
            vec![CallsSource {
                service: trace_service_decl(&[SERVICE_NAME_COLUMN, "service_namespace"]),
                scan: trace,
            }],
            &test_window(),
        )
        .unwrap()
        .unwrap();

        let batches = collect(&ctx, plan).await;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1);
        let batch = &batches[0];
        // Composite service identity renders the same sorted `k=v` form the
        // registry emits, so edges land on registry entity ids.
        assert_eq!(
            strings(batch, 5),
            vec!["service_name=frontend,service_namespace=ns1"]
        );
        assert_eq!(
            strings(batch, 7),
            vec!["service_name=cart,service_namespace=ns1"]
        );
    }

    fn build_relationships_plan_for_test(
        traces: Vec<CallsSource>,
        window: &GraphQueryWindow,
    ) -> DfResult<Option<LogicalPlan>> {
        build_relationships_plan(traces, None, window)
    }

    /// Registers a declared-edge table holding:
    /// - frontend->db: two revisions (0.5 then 1.0), the newer retiring the edge at 300s;
    /// - api->cache: expired at 100s;
    /// - agent-1->search: open-ended `provenance = 'agent'` with JSON attributes;
    /// - webapp->auth: two revisions, the newer declared at 900s;
    /// - batch->queue: declared at 100s but only valid from 700s;
    /// - gateway->redis: asserted twice at the same instant as generations g1/g2.
    fn declared_table(ctx: &SessionContext) {
        let ts_field = |name: &str| {
            Field::new(
                name,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                name != OBSERVED_AT_COLUMN,
            )
        };
        let schema = Arc::new(Schema::new(vec![
            ts_field(OBSERVED_AT_COLUMN),
            ts_field("window_start"),
            ts_field("window_end"),
            ts_field("fresh_until"),
            ts_field("valid_from"),
            ts_field("valid_until"),
            Field::new("src_type", DataType::Utf8, false),
            Field::new("src_id", DataType::Utf8, false),
            Field::new("rel_type", DataType::Utf8, false),
            Field::new("dst_type", DataType::Utf8, false),
            Field::new("dst_id", DataType::Utf8, false),
            Field::new("provenance", DataType::Utf8, false),
            Field::new("scope", DataType::Utf8, false),
            Field::new("generation_id", DataType::Utf8, false),
            Field::new("confidence", DataType::Float64, true),
            Field::new("request_count", DataType::Int64, true),
            Field::new("error_count", DataType::Int64, true),
            Field::new("duration_sum", DataType::Float64, true),
            Field::new("duration_count", DataType::Int64, true),
            Field::new("attributes", DataType::Binary, true),
        ]));
        let attrs = jsonb::parse_value(br#"{"connection_type":"virtual_node"}"#)
            .unwrap()
            .to_vec();
        let no_ts = || Arc::new(TimestampMillisecondArray::from(vec![None::<i64>; 9])) as ArrayRef;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![
                    100_000, 200_000, 50_000, 90_000, 100_000, 900_000, 100_000, 100_000, 100_000,
                ])) as ArrayRef,
                no_ts(), // window_start
                no_ts(), // window_end
                no_ts(), // fresh_until
                Arc::new(TimestampMillisecondArray::from(vec![
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(700_000),
                    None,
                    None,
                ])),
                Arc::new(TimestampMillisecondArray::from(vec![
                    None,
                    Some(300_000),
                    Some(100_000),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                ])),
                Arc::new(StringArray::from(vec![
                    "service", "service", "service", "agent", "service", "service", "service",
                    "service", "service",
                ])),
                Arc::new(StringArray::from(vec![
                    "frontend", "frontend", "api", "agent-1", "webapp", "webapp", "batch",
                    "gateway", "gateway",
                ])),
                Arc::new(StringArray::from(vec![
                    "depends_on",
                    "depends_on",
                    "depends_on",
                    "uses",
                    "depends_on",
                    "depends_on",
                    "depends_on",
                    "depends_on",
                    "depends_on",
                ])),
                Arc::new(StringArray::from(vec![
                    "service", "service", "service", "tool", "service", "service", "service",
                    "service", "service",
                ])),
                Arc::new(StringArray::from(vec![
                    "db", "db", "cache", "search", "auth", "auth", "queue", "redis", "redis",
                ])),
                Arc::new(StringArray::from(vec![
                    "declared", "declared", "declared", "agent", "declared", "declared",
                    "declared", "declared", "declared",
                ])),
                Arc::new(StringArray::from(vec![""; 9])),
                Arc::new(StringArray::from(vec![
                    "", "", "", "", "", "", "", "g1", "g2",
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(0.5),
                    Some(1.0),
                    Some(1.0),
                    Some(0.8),
                    Some(0.5),
                    Some(1.0),
                    Some(1.0),
                    Some(0.5),
                    Some(1.0),
                ])),
                Arc::new(Int64Array::from(vec![None::<i64>; 9])),
                Arc::new(Int64Array::from(vec![None::<i64>; 9])),
                Arc::new(Float64Array::from(vec![None::<f64>; 9])),
                Arc::new(Int64Array::from(vec![None::<i64>; 9])),
                Arc::new(BinaryArray::from(vec![
                    None,
                    None,
                    None,
                    Some(attrs.as_slice()),
                    None,
                    None,
                    None,
                    None,
                    None,
                ])),
            ],
        )
        .unwrap();
        ctx.register_table(
            SEMANTIC_RELATIONSHIPS_DECLARED_TABLE_NAME,
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
    }

    #[tokio::test]
    async fn declared_edges_latest_revision_validity_and_synthetic_time() {
        let ctx = SessionContext::new();
        declared_table(&ctx);
        let scan = ctx
            .table(SEMANTIC_RELATIONSHIPS_DECLARED_TABLE_NAME)
            .await
            .unwrap();

        // Queried window [120s, 600s).
        let window = GraphQueryWindow::from_observed(120_000, 600_000);
        let plan = build_relationships_plan(vec![], Some(DeclaredSource { scan }), &window)
            .unwrap()
            .unwrap();
        let names = plan
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, RELATIONSHIP_COLUMNS);

        // (src_id, observed_at, window_start, window_end, fresh_until,
        //  provenance, attributes)
        type DeclaredRow = (String, i64, i64, i64, i64, String, Option<String>);

        let batches = collect(&ctx, plan).await;
        let mut rows: Vec<DeclaredRow> = vec![];
        for batch in &batches {
            let observed = ts_values(batch, 0);
            let window_start = ts_values(batch, 1);
            let window_end = ts_values(batch, 2);
            let fresh_until = ts_values(batch, 3);
            let src = strings(batch, 5);
            let provenance = strings(batch, 9);
            let attributes = json_texts(batch, 15);
            for i in 0..batch.num_rows() {
                rows.push((
                    src[i].clone(),
                    observed[i],
                    window_start[i],
                    window_end[i],
                    fresh_until[i],
                    provenance[i].clone(),
                    attributes[i].clone(),
                ));
            }
        }
        rows.sort();

        // api->cache expired before the window; batch->queue is not yet valid;
        // frontend->db keeps only the latest revision; webapp->auth keeps the
        // first revision (the second postdates the window); gateway->redis
        // collapses to one row across generations.
        assert_eq!(rows.len(), 4);

        // agent->tool: open-ended validity declared at 90s. The synthesized
        // observed_at is clamped into the queried window; window_end and
        // fresh_until take the window's upper bound.
        assert_eq!(
            rows[0],
            (
                "agent-1".to_string(),
                120_000,
                90_000,
                600_000,
                600_000,
                "agent".to_string(),
                Some(r#"{"connection_type":"virtual_node"}"#.to_string()),
            )
        );

        // frontend->db: the latest revision (declared at 200s, retired at 300s)
        // supersedes the open-ended first revision.
        assert_eq!(
            rows[1],
            (
                "frontend".to_string(),
                200_000,
                200_000,
                300_000,
                300_000,
                "declared".to_string(),
                None,
            )
        );

        assert_eq!(
            rows[2],
            (
                "gateway".to_string(),
                120_000,
                100_000,
                600_000,
                600_000,
                "declared".to_string(),
                None,
            )
        );

        assert_eq!(
            rows[3],
            (
                "webapp".to_string(),
                120_000,
                100_000,
                600_000,
                600_000,
                "declared".to_string(),
                None,
            )
        );
    }

    #[tokio::test]
    async fn declared_edges_pick_the_revision_as_of_the_window() {
        let ctx = SessionContext::new();
        declared_table(&ctx);
        let scan = ctx
            .table(SEMANTIC_RELATIONSHIPS_DECLARED_TABLE_NAME)
            .await
            .unwrap();

        // A historical window that predates frontend->db's second revision:
        // the first revision was the edge's state then and must not be hidden
        // by the newer one.
        let window = GraphQueryWindow::from_observed(0, 150_000);
        let plan = build_relationships_plan(vec![], Some(DeclaredSource { scan }), &window)
            .unwrap()
            .unwrap();

        let batches = collect(&ctx, plan).await;
        let mut rows: Vec<(String, f64)> = vec![];
        for batch in &batches {
            let src = strings(batch, 5);
            let confidence = batch
                .column(10)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for (i, src) in src.into_iter().enumerate() {
                rows.push((src, confidence.value(i)));
            }
        }
        rows.sort_by(|a, b| a.0.cmp(&b.0));

        // api->cache is valid inside this window; batch->queue is not yet.
        assert_eq!(
            rows,
            vec![
                ("agent-1".to_string(), 0.8),
                ("api".to_string(), 1.0),
                ("frontend".to_string(), 0.5),
                ("gateway".to_string(), 1.0),
                ("webapp".to_string(), 0.5),
            ]
        );
    }

    #[tokio::test]
    async fn declared_edges_union_calls_branch() {
        let ctx = trace_table_ctx();
        declared_table(&ctx);
        let trace = ctx.table("opentelemetry_traces").await.unwrap();
        let declared = ctx
            .table(SEMANTIC_RELATIONSHIPS_DECLARED_TABLE_NAME)
            .await
            .unwrap();

        let plan = build_relationships_plan(
            vec![CallsSource {
                service: trace_service_decl(&[SERVICE_NAME_COLUMN]),
                scan: trace,
            }],
            Some(DeclaredSource { scan: declared }),
            &test_window(),
        )
        .unwrap()
        .unwrap();

        let batches = collect(&ctx, plan).await;
        let mut provenance: Vec<String> = batches.iter().flat_map(|b| strings(b, 9)).collect();
        provenance.sort();
        // One trace-derived calls edge plus the declared edges valid somewhere
        // inside [0s, 600s): frontend, api, webapp, gateway, and the
        // agent-asserted one.
        assert_eq!(
            provenance,
            vec![
                "agent", "declared", "declared", "declared", "declared", "trace"
            ]
        );
    }
}
