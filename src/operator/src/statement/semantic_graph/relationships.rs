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
use datafusion::functions_aggregate::expr_fn::{bool_or, count, min, sum};
use datafusion::functions_window::expr_fn::row_number;
use datafusion_common::{Result as DfResult, ScalarValue};
use datafusion_expr::{Expr, ExprFunctionExt, JoinType, LogicalPlan, cast, ident, lit, when};

use crate::statement::semantic_graph::{
    DECLARED_EDGE_IDENTITY_COLUMNS, EntityDeclaration, GraphQueryWindow, OBSERVED_AT_COLUMN,
    bin_interval, bin_ms, entity_id_expr, interval, null_json, parse_json_expr, qcol, union_all,
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

/// A declaring table's scan paired with its entity declarations, from which
/// the same-row co-declaration rules derive edges. `is_trace` gates the
/// agent-edge vocabulary, which the RFC ties to span structure.
pub struct CoDeclaredSource {
    pub declarations: Vec<EntityDeclaration>,
    pub is_trace: bool,
    pub scan: DataFrame,
}

/// The declared-edge table's scan (`semantic_relationships_declared`), whose
/// rows `build_relationships_plan` unions into the edge set.
pub struct DeclaredSource {
    pub scan: DataFrame,
}

/// Everything `build_relationships_plan` derives edges from.
pub struct RelationshipSources {
    pub traces: Vec<CallsSource>,
    pub co_declared: Vec<CoDeclaredSource>,
    pub declared: Option<DeclaredSource>,
}

/// Builds the `semantic_relationships` plan: the trace-derived `calls` branch,
/// the same-row co-declared branch, and the declared-edge branch unioned and
/// re-projected to the 16-column contract. Returns `None` when no source can
/// contribute edges, so the computed table streams empty.
pub fn build_relationships_plan(
    sources: RelationshipSources,
    window: &GraphQueryWindow,
) -> DfResult<Option<LogicalPlan>> {
    let mut union_df = calls_branch(sources.traces, window)?;
    if let Some(co_declared) = co_declared_branch(sources.co_declared, window)? {
        union_df = union_all(union_df, co_declared)?;
    }
    if let Some(declared) = sources.declared {
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

/// The same-row co-declaration vocabulary open to any declaring table: a row
/// carrying both identities witnesses the edge, and the built-in direction is
/// `src -> dst`. Rows are `(src_type, dst_type, rel_type)`.
const ATTRIBUTE_EDGE_VOCABULARY: [(&str, &str, &str); 6] = [
    ("service.instance", "host", "runs_on"),
    ("process", "host", "runs_on"),
    ("k8s.pod", "k8s.node", "runs_on"),
    ("k8s.pod", "k8s.container", "contains"),
    ("service.instance", "service", "part_of"),
    ("k8s.pod", "k8s.workload", "part_of"),
];

/// The agent-edge vocabulary, applied only to trace sources: the RFC derives
/// `agent uses model` / `agent invoked tool` from span structure (an LLM- or
/// tool-call span carries both identities), so a non-trace table co-declaring
/// these types must not fabricate invocations.
const AGENT_EDGE_VOCABULARY: [(&str, &str, &str); 2] =
    [("agent", "model", "uses"), ("agent", "tool", "invoked")];

const CO_DECLARED_VALID_COLUMN: &str = "__edge_valid";

/// The same-row co-declared branch: for each source table, every vocabulary
/// pair whose two entity types the table declares yields an edge projection,
/// expanded from one scan via `unnest_rows` (the registry idiom). Attribute
/// edges carry `provenance = 'attribute'`; agent edges are span-structure
/// observations and carry `provenance = 'trace'`.
fn co_declared_branch(
    sources: Vec<CoDeclaredSource>,
    window: &GraphQueryWindow,
) -> DfResult<Option<DataFrame>> {
    let mut union_df: Option<DataFrame> = None;
    for source in sources {
        let find = |ty: &str| source.declarations.iter().find(|d| d.entity_type == ty);
        let mut pairs = Vec::new();
        for (src_type, dst_type, rel_type) in ATTRIBUTE_EDGE_VOCABULARY {
            if let (Some(src), Some(dst)) = (find(src_type), find(dst_type)) {
                pairs.push((src, dst, rel_type, "attribute"));
            }
        }
        if source.is_trace {
            for (src_type, dst_type, rel_type) in AGENT_EDGE_VOCABULARY {
                if let (Some(src), Some(dst)) = (find(src_type), find(dst_type)) {
                    pairs.push((src, dst, rel_type, "trace"));
                }
            }
        }
        let Some((first, _, _, _)) = pairs.first() else {
            continue;
        };

        let ts = ident(&first.time_index);
        let bin = bin_ms(ts.clone());
        let window_predicate = ts
            .clone()
            .gt_eq(window.source_start())
            .and(ts.lt(window.source_end()));
        let null_i64 = || lit(ScalarValue::Int64(None));
        let rows = pairs
            .iter()
            .map(|(src, dst, rel_type, provenance)| {
                // Both endpoints must identify something on the row for the
                // row to witness the edge.
                let valid = src
                    .id_columns
                    .iter()
                    .chain(&dst.id_columns)
                    .fold(lit(true), |predicate, id| {
                        predicate.and(ident(id).is_not_null())
                    });
                vec![
                    valid,
                    bin.clone(),
                    bin.clone(),
                    bin.clone() + bin_interval(),
                    bin.clone() + bin_interval(),
                    lit(src.entity_type.as_str()),
                    entity_id_expr(&src.id_columns, &|c| ident(c)),
                    lit(dst.entity_type.as_str()),
                    entity_id_expr(&dst.id_columns, &|c| ident(c)),
                    lit(*rel_type),
                    lit(*provenance),
                    lit(1.0_f64),
                    null_i64(),
                    null_i64(),
                    lit(ScalarValue::Float64(None)),
                    null_i64(),
                    null_json(),
                ]
            })
            .collect();

        let branch = super::unnest_rows(
            source.scan,
            window_predicate,
            CO_DECLARED_VALID_COLUMN,
            &RELATIONSHIP_COLUMNS,
            rows,
        )?;
        union_df = union_all(union_df, branch)?;
    }
    Ok(union_df)
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

/// Span-attribute columns naming an uninstrumented peer, in precedence order,
/// each with the `connection_type` its match implies. Attribute columns are
/// dynamic in `greptime_trace_v1` (created on first use), so only columns
/// present in a table's schema participate.
const VIRTUAL_DST_CANDIDATES: [(&str, &str); 3] = [
    ("span_attributes.peer.service", "virtual_node"),
    ("span_attributes.db.name", "database"),
    ("span_attributes.server.address", "virtual_node"),
];

/// Confidence of a virtual-node edge: the peer was named by a client-side
/// attribute, not witnessed by a server span (the RFC requires `< 1.0`).
const VIRTUAL_NODE_CONFIDENCE: f64 = 0.5;

/// The `calls` derivation: union the client spans and the server spans of all
/// trace tables (each projected to a normalized shape with its own table's
/// `service` identity), left-join clients to their child server spans on
/// `trace_id` + `parent_span_id`, and aggregate RED metrics per 60s window —
/// the plan form of the Tempo servicegraph connector. Unioning spans before
/// the join pairs a client with a server stored in a *different* trace table
/// (`x-greptime-trace-table-name` routing); with a single table it degenerates
/// to the previous self-join. Returns `None` when there is no trace table.
///
/// A client with no matching server span is an edge to a **virtual node**
/// named by [`VIRTUAL_DST_CANDIDATES`], with the client's own status/duration
/// and `confidence < 1.0`. Real pairs win: when a window's edge key holds any
/// pair, the edge reports only the pair population (the RFC pins RED metrics
/// to observed span pairs) and the unmatched clients are suppressed, so one
/// `(window, edge)` never yields two rows.
fn calls_branch(
    traces: Vec<CallsSource>,
    window: &GraphQueryWindow,
) -> DfResult<Option<DataFrame>> {
    let mut clients: Option<DataFrame> = None;
    let mut servers: Option<DataFrame> = None;
    for trace in traces {
        clients = union_all(clients, client_spans(&trace, window)?)?;
        servers = union_all(servers, server_spans(&trace.service, trace.scan, window)?)?;
    }
    let (Some(clients), Some(servers)) = (clients, servers) else {
        return Ok(None);
    };

    let client = clients.alias("client")?;
    let server = servers.alias("server")?;
    let join_conditions = vec![
        qcol("client", TRACE_ID_COLUMN).eq(qcol("server", TRACE_ID_COLUMN)),
        qcol("server", PARENT_SPAN_ID_COLUMN).eq(qcol("client", SPAN_ID_COLUMN)),
        qcol("server", TRACE_TIMESTAMP_COLUMN)
            .gt_eq(qcol("client", TRACE_TIMESTAMP_COLUMN) - interval(CHILD_SPAN_EARLY_NANOS)),
        qcol("server", TRACE_TIMESTAMP_COLUMN)
            .lt_eq(qcol("client", TRACE_TIMESTAMP_COLUMN) + interval(CHILD_SPAN_LATE_NANOS)),
    ];

    let observations = client
        .join_on(server, JoinType::Left, join_conditions)?
        .select(vec![
            bin_ms(qcol("client", TRACE_TIMESTAMP_COLUMN)).alias("observed_at"),
            qcol("client", "src_id").alias("src_id"),
            core_fns::coalesce()
                .call(vec![
                    qcol("server", "dst_id"),
                    qcol("client", "virtual_dst"),
                ])
                .alias("dst_id"),
            qcol("server", TRACE_ID_COLUMN)
                .is_not_null()
                .alias("paired"),
            qcol("server", SPAN_STATUS_CODE_COLUMN).alias("server_status"),
            qcol("server", DURATION_NANO_COLUMN).alias("server_duration_nano"),
            qcol("client", SPAN_STATUS_CODE_COLUMN).alias("client_status"),
            qcol("client", DURATION_NANO_COLUMN).alias("client_duration_nano"),
            qcol("client", "virtual_conn").alias("virtual_conn"),
        ])?
        // An unmatched client without a peer-naming attribute observes no edge;
        // a self-call (on either the paired or the attribute-named identity) is
        // not an edge between two distinct entities.
        .filter(
            ident("dst_id")
                .is_not_null()
                .and(ident("src_id").not_eq(ident("dst_id"))),
        )?;

    let paired = ident("paired");
    let df = observations
        .aggregate(
            vec![ident("observed_at"), ident("src_id"), ident("dst_id")],
            vec![
                count(lit(1))
                    .filter(paired.clone())
                    .build()?
                    .alias("pair_count"),
                count(lit(1))
                    .filter(
                        paired
                            .clone()
                            .and(ident("server_status").eq(lit(SPAN_STATUS_ERROR))),
                    )
                    .build()?
                    .alias("pair_errors"),
                sum(ident("server_duration_nano"))
                    .filter(paired.clone())
                    .build()?
                    .alias("pair_duration_nano"),
                count(lit(1))
                    .filter(!paired.clone())
                    .build()?
                    .alias("unmatched_count"),
                count(lit(1))
                    .filter(
                        (!paired.clone()).and(ident("client_status").eq(lit(SPAN_STATUS_ERROR))),
                    )
                    .build()?
                    .alias("unmatched_errors"),
                sum(ident("client_duration_nano"))
                    .filter(!paired.clone())
                    .build()?
                    .alias("unmatched_duration_nano"),
                bool_or(paired.clone()).alias("has_pair"),
                // `min` makes a mixed-provenance virtual edge deterministic
                // (`database` sorts before `virtual_node`).
                min(ident("virtual_conn"))
                    .filter(!paired)
                    .build()?
                    .alias("virtual_conn"),
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
            real_wins(lit(1.0_f64), lit(VIRTUAL_NODE_CONFIDENCE))?.alias("confidence"),
            real_wins(ident("pair_count"), ident("unmatched_count"))?.alias("request_count"),
            real_wins(ident("pair_errors"), ident("unmatched_errors"))?.alias("error_count"),
            // duration sums in nanoseconds; the contract column is seconds.
            (cast(
                real_wins(
                    ident("pair_duration_nano"),
                    ident("unmatched_duration_nano"),
                )?,
                DataType::Float64,
            ) / lit(1e9_f64))
            .alias("duration_sum"),
            real_wins(ident("pair_count"), ident("unmatched_count"))?.alias("duration_count"),
            real_wins(null_json(), virtual_attrs_expr()?)?.alias("attributes"),
        ])?;
    Ok(Some(df))
}

/// `CASE WHEN has_pair THEN real ELSE virtual END` — the real-wins projection
/// over the mixed aggregate.
fn real_wins(real: Expr, r#virtual: Expr) -> DfResult<Expr> {
    when(ident("has_pair"), real).otherwise(r#virtual)
}

/// The `attributes` JSON of a virtual edge, from the aggregated
/// `connection_type`.
fn virtual_attrs_expr() -> DfResult<Expr> {
    when(
        ident("virtual_conn").eq(lit("database")),
        parse_json_expr(lit(r#"{"connection_type":"database"}"#)),
    )
    .otherwise(parse_json_expr(lit(
        r#"{"connection_type":"virtual_node"}"#,
    )))
}

/// The window + span-kind + identity predicate shared by both join sides.
/// `strict` bounds the scan to the source window; the non-strict side widens
/// by the join's time-proximity allowance (the join bounds reference the other
/// side's timestamp and cannot prune this side's scan on their own).
fn span_predicate(service: &EntityDeclaration, window: &GraphQueryWindow, strict: bool) -> Expr {
    let ts = ident(TRACE_TIMESTAMP_COLUMN);
    let mut predicate = if strict {
        ts.clone()
            .gt_eq(window.source_start())
            .and(ts.lt(window.source_end()))
    } else {
        ts.clone()
            .gt_eq(window.source_start() - interval(CHILD_SPAN_EARLY_NANOS))
            .and(ts.lt(window.source_end() + interval(CHILD_SPAN_LATE_NANOS)))
    };
    // A NULL identity component identifies nothing, on either endpoint.
    for id in &service.id_columns {
        predicate = predicate.and(ident(id).is_not_null());
    }
    predicate
}

/// One trace table's client spans, normalized to
/// `(timestamp, trace_id, span_id, src_id, status_code, duration_nano,
/// virtual_dst, virtual_conn)`. `src_id` is built from the table's `service`
/// declaration, so edges land on exactly the entity ids the registry emits (a
/// composite identity renders the same sorted `k=v` form); the per-table
/// projection is what lets tables with different declarations union.
fn client_spans(source: &CallsSource, window: &GraphQueryWindow) -> DfResult<DataFrame> {
    // Peer-naming candidates present in this table's schema, normalized so an
    // empty value falls through to the next candidate.
    let present: Vec<(Expr, &str)> = {
        let schema = source.scan.schema();
        VIRTUAL_DST_CANDIDATES
            .iter()
            .filter(|(column, _)| schema.has_column_with_unqualified_name(column))
            .map(|(column, conn)| {
                let value =
                    core_fns::nullif().call(vec![cast(ident(*column), DataType::Utf8), lit("")]);
                (value, *conn)
            })
            .collect()
    };
    let null_utf8 = || lit(ScalarValue::Utf8(None));
    let virtual_dst = if present.is_empty() {
        null_utf8()
    } else {
        core_fns::coalesce().call(present.iter().map(|(value, _)| value.clone()).collect())
    };
    let mut virtual_conn = null_utf8();
    for (value, conn) in present.into_iter().rev() {
        virtual_conn = when(value.is_not_null(), lit(conn)).otherwise(virtual_conn)?;
    }

    source
        .scan
        .clone()
        .filter(
            ident(SPAN_KIND_COLUMN)
                .eq(lit(SPAN_KIND_CLIENT))
                .and(span_predicate(&source.service, window, true)),
        )?
        .select(vec![
            ident(TRACE_TIMESTAMP_COLUMN),
            ident(TRACE_ID_COLUMN),
            ident(SPAN_ID_COLUMN),
            // The cast inside entity_id_expr also normalizes tag columns, which
            // come out of the storage engine dictionary-encoded.
            entity_id_expr(&source.service.id_columns, &|c| ident(c)).alias("src_id"),
            ident(SPAN_STATUS_CODE_COLUMN),
            ident(DURATION_NANO_COLUMN),
            virtual_dst.alias("virtual_dst"),
            virtual_conn.alias("virtual_conn"),
        ])
}

/// One trace table's server spans, normalized to
/// `(timestamp, trace_id, parent_span_id, dst_id, status_code, duration_nano)`.
fn server_spans(
    service: &EntityDeclaration,
    trace: DataFrame,
    window: &GraphQueryWindow,
) -> DfResult<DataFrame> {
    trace
        .filter(
            ident(SPAN_KIND_COLUMN)
                .eq(lit(SPAN_KIND_SERVER))
                .and(span_predicate(service, window, false)),
        )?
        .select(vec![
            ident(TRACE_TIMESTAMP_COLUMN),
            ident(TRACE_ID_COLUMN),
            ident(PARENT_SPAN_ID_COLUMN),
            entity_id_expr(&service.id_columns, &|c| ident(c)).alias("dst_id"),
            ident(SPAN_STATUS_CODE_COLUMN),
            ident(DURATION_NANO_COLUMN),
        ])
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_catalog::consts::{SEMANTIC_RELATIONSHIPS_DECLARED_TABLE_NAME, SERVICE_NAME_COLUMN};
    use datafusion::arrow::array::{
        Array, ArrayRef, BinaryArray, Float64Array, Int64Array, StringArray,
        TimestampMillisecondArray, TimestampNanosecondArray, UInt64Array,
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

    /// One span row of the fixed `greptime_trace_v1` shape:
    /// `(ts_ms, trace_id, span_id, parent_span_id, span_kind, status_code,
    /// service_name, duration_nano)`.
    type Span<'a> = (
        i64,
        &'a str,
        &'a str,
        Option<&'a str>,
        &'a str,
        &'a str,
        &'a str,
        u64,
    );

    /// Registers a trace-v1-shaped table (ns timestamps) plus optional dynamic
    /// string columns (`extra`), one value per span.
    fn register_trace_table(
        ctx: &SessionContext,
        name: &str,
        extra: &[(&str, &[Option<&str>])],
        spans: &[Span<'_>],
    ) {
        let mut fields = vec![
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
            Field::new(DURATION_NANO_COLUMN, DataType::UInt64, false),
        ];
        for (column, _) in extra {
            fields.push(Field::new(*column, DataType::Utf8, true));
        }
        let schema = Arc::new(Schema::new(fields));
        const MS: i64 = 1_000_000;
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(TimestampNanosecondArray::from(
                spans.iter().map(|s| s.0 * MS).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                spans.iter().map(|s| s.1).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                spans.iter().map(|s| s.2).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                spans.iter().map(|s| s.3).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                spans.iter().map(|s| s.4).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                spans.iter().map(|s| s.5).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                spans.iter().map(|s| s.6).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                spans.iter().map(|s| s.7).collect::<Vec<_>>(),
            )),
        ];
        for (_, values) in extra {
            columns.push(Arc::new(StringArray::from(values.to_vec())));
        }
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        ctx.register_table(
            name,
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
    }

    const UNSET: &str = "STATUS_CODE_UNSET";
    const ERROR: &str = "STATUS_CODE_ERROR";
    const CLIENT: &str = "SPAN_KIND_CLIENT";
    const SERVER: &str = "SPAN_KIND_SERVER";

    /// Two client->server pairs frontend->cart (one errored), one pair
    /// cart->cart (self-call, excluded), one unmatched client span.
    const BASE_SPANS: [Span<'static>; 7] = [
        (1_000, "t1", "c1", None, CLIENT, UNSET, "frontend", 0),
        (
            1_010,
            "t1",
            "s1",
            Some("c1"),
            SERVER,
            UNSET,
            "cart",
            500_000_000,
        ),
        (2_000, "t2", "c2", None, CLIENT, UNSET, "frontend", 0),
        (
            2_010,
            "t2",
            "s2",
            Some("c2"),
            SERVER,
            ERROR,
            "cart",
            1_500_000_000,
        ),
        (3_000, "t3", "c3", None, CLIENT, UNSET, "cart", 0),
        (3_010, "t3", "s3", Some("c3"), SERVER, UNSET, "cart", 100),
        (4_000, "t4", "c4", None, CLIENT, UNSET, "frontend", 0),
    ];

    fn trace_table_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        let namespaces = [Some("ns1"); 7];
        register_trace_table(
            &ctx,
            "opentelemetry_traces",
            &[("service_namespace", &namespaces)],
            &BASE_SPANS,
        );
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

    /// RED columns of one output row: `(request_count, error_count,
    /// duration_sum, duration_count)`.
    fn red_metrics(batch: &RecordBatch, row: usize) -> (i64, i64, f64, i64) {
        let i64_at = |column: usize| {
            batch
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row)
        };
        let duration_sum = batch
            .column(13)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(row);
        (i64_at(11), i64_at(12), duration_sum, i64_at(14))
    }

    fn confidence(batch: &RecordBatch, row: usize) -> f64 {
        batch
            .column(10)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(row)
    }

    fn sources(traces: &[(&str, DataFrame)]) -> Vec<CallsSource> {
        traces
            .iter()
            .map(|(_, scan)| CallsSource {
                service: trace_service_decl(&[SERVICE_NAME_COLUMN]),
                scan: scan.clone(),
            })
            .collect()
    }

    #[tokio::test]
    async fn calls_plan_merges_edges_across_trace_tables() {
        let ctx = SessionContext::new();
        // Distinct traces observing the same frontend->cart edge, one per table.
        register_trace_table(
            &ctx,
            "trace_a",
            &[],
            &[
                (1_000, "t1", "c1", None, CLIENT, UNSET, "frontend", 0),
                (
                    1_010,
                    "t1",
                    "s1",
                    Some("c1"),
                    SERVER,
                    UNSET,
                    "cart",
                    500_000_000,
                ),
            ],
        );
        register_trace_table(
            &ctx,
            "trace_b",
            &[],
            &[
                (2_000, "t2", "c2", None, CLIENT, UNSET, "frontend", 0),
                (
                    2_010,
                    "t2",
                    "s2",
                    Some("c2"),
                    SERVER,
                    ERROR,
                    "cart",
                    1_500_000_000,
                ),
            ],
        );
        let a = ctx.table("trace_a").await.unwrap();
        let b = ctx.table("trace_b").await.unwrap();
        let plan =
            build_relationships_plan_for_test(sources(&[("a", a), ("b", b)]), &test_window())
                .unwrap()
                .unwrap();

        let batches = collect(&ctx, plan).await;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        // One edge row with the RED metrics of both tables' pairs.
        assert_eq!(total, 1);
        assert_eq!(red_metrics(&batches[0], 0), (2, 1, 2.0, 2));
    }

    #[tokio::test]
    async fn calls_pair_split_across_trace_tables() {
        let ctx = SessionContext::new();
        // The client span and its child server span land in different tables
        // (per-table trace routing): the union-then-join pairing must find it.
        register_trace_table(
            &ctx,
            "trace_a",
            &[],
            &[(1_000, "t1", "c1", None, CLIENT, UNSET, "frontend", 0)],
        );
        register_trace_table(
            &ctx,
            "trace_b",
            &[],
            &[(
                1_010,
                "t1",
                "s1",
                Some("c1"),
                SERVER,
                UNSET,
                "cart",
                500_000_000,
            )],
        );
        let a = ctx.table("trace_a").await.unwrap();
        let b = ctx.table("trace_b").await.unwrap();
        let plan =
            build_relationships_plan_for_test(sources(&[("a", a), ("b", b)]), &test_window())
                .unwrap()
                .unwrap();

        let batches = collect(&ctx, plan).await;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1);
        let batch = &batches[0];
        assert_eq!(strings(batch, 5), vec!["frontend"]);
        assert_eq!(strings(batch, 7), vec!["cart"]);
        assert_eq!(confidence(batch, 0), 1.0);
        assert_eq!(red_metrics(batch, 0), (1, 0, 0.5, 1));
    }

    #[tokio::test]
    async fn empty_trace_table_does_not_change_edges() {
        let ctx = trace_table_ctx();
        register_trace_table(&ctx, "trace_empty", &[], &[]);
        let base = ctx.table("opentelemetry_traces").await.unwrap();
        let empty = ctx.table("trace_empty").await.unwrap();
        let plan = build_relationships_plan_for_test(
            sources(&[("base", base), ("empty", empty)]),
            &test_window(),
        )
        .unwrap()
        .unwrap();

        let batches = collect(&ctx, plan).await;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        // Same single edge, same RED numbers as the single-table derivation.
        assert_eq!(total, 1);
        assert_eq!(red_metrics(&batches[0], 0), (2, 1, 2.0, 2));
    }

    #[tokio::test]
    async fn virtual_node_edge_from_unmatched_client() {
        let ctx = SessionContext::new();
        register_trace_table(
            &ctx,
            "trace_a",
            &[(
                "span_attributes.peer.service",
                &[Some("redis"), None, Some("frontend")],
            )],
            &[
                // Unmatched client naming its peer: a virtual-node edge with
                // the client's own status and duration.
                (
                    1_000,
                    "t1",
                    "c1",
                    None,
                    CLIENT,
                    ERROR,
                    "frontend",
                    250_000_000,
                ),
                // Unmatched client without a peer attribute: no edge.
                (2_000, "t2", "c2", None, CLIENT, UNSET, "frontend", 0),
                // Peer attribute naming the client's own service: excluded.
                (3_000, "t3", "c3", None, CLIENT, UNSET, "frontend", 0),
            ],
        );
        let a = ctx.table("trace_a").await.unwrap();
        let plan = build_relationships_plan_for_test(sources(&[("a", a)]), &test_window())
            .unwrap()
            .unwrap();

        let batches = collect(&ctx, plan).await;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1);
        let batch = &batches[0];
        assert_eq!(strings(batch, 5), vec!["frontend"]);
        assert_eq!(strings(batch, 7), vec!["redis"]);
        assert_eq!(strings(batch, 8), vec!["calls"]);
        assert_eq!(strings(batch, 9), vec!["trace"]);
        assert_eq!(confidence(batch, 0), VIRTUAL_NODE_CONFIDENCE);
        assert_eq!(red_metrics(batch, 0), (1, 1, 0.25, 1));
        assert_eq!(
            json_texts(batch, 15),
            vec![Some(r#"{"connection_type":"virtual_node"}"#.to_string())]
        );
    }

    #[tokio::test]
    async fn virtual_endpoint_fallback_and_connection_type() {
        let ctx = SessionContext::new();
        register_trace_table(
            &ctx,
            "trace_a",
            &[
                // An empty peer.service must fall through to db.name, whose
                // match maps connection_type to `database`.
                ("span_attributes.peer.service", &[Some("")]),
                ("span_attributes.db.name", &[Some("mysql")]),
            ],
            &[(1_000, "t1", "c1", None, CLIENT, UNSET, "frontend", 100)],
        );
        let a = ctx.table("trace_a").await.unwrap();
        let plan = build_relationships_plan_for_test(sources(&[("a", a)]), &test_window())
            .unwrap()
            .unwrap();

        let batches = collect(&ctx, plan).await;
        let batch = &batches[0];
        assert_eq!(strings(batch, 7), vec!["mysql"]);
        assert_eq!(
            json_texts(batch, 15),
            vec![Some(r#"{"connection_type":"database"}"#.to_string())]
        );
    }

    /// A metric-like declaring table: ms timestamps, one row with a NULL
    /// `instance` (witnessing nothing for instance edges), two rows duplicating
    /// the same identities inside one bin (collapsed by DISTINCT).
    fn co_declared_ctx() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("instance", DataType::Utf8, true),
            Field::new("host", DataType::Utf8, false),
            Field::new("service", DataType::Utf8, false),
            Field::new("agent_id", DataType::Utf8, false),
            Field::new("model_name", DataType::Utf8, false),
            Field::new("tool_name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![1_000, 2_000, 3_000])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("i-1"), Some("i-1"), None])),
                Arc::new(StringArray::from(vec!["h-1", "h-1", "h-2"])),
                Arc::new(StringArray::from(vec!["svc", "svc", "svc"])),
                Arc::new(StringArray::from(vec!["agent-1"; 3])),
                Arc::new(StringArray::from(vec!["gpt"; 3])),
                Arc::new(StringArray::from(vec!["search"; 3])),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table(
            "co_metrics",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
        ctx
    }

    fn co_decl(entity_type: &str, id_columns: &[&str]) -> EntityDeclaration {
        EntityDeclaration {
            schema: "public".to_string(),
            table: "co_metrics".to_string(),
            time_index: "ts".to_string(),
            entity_type: entity_type.to_string(),
            id_columns: id_columns.iter().map(|s| s.to_string()).collect(),
            descriptive_columns: vec![],
            scope_columns: vec![],
        }
    }

    async fn co_declared_edges(
        ctx: &SessionContext,
        declarations: Vec<EntityDeclaration>,
        is_trace: bool,
    ) -> Option<Vec<(String, String, String, String, String, String)>> {
        let scan = ctx.table("co_metrics").await.unwrap();
        let plan = build_relationships_plan(
            RelationshipSources {
                traces: vec![],
                co_declared: vec![CoDeclaredSource {
                    declarations,
                    is_trace,
                    scan,
                }],
                declared: None,
            },
            &test_window(),
        )
        .unwrap()?;
        assert_eq!(
            plan.schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            RELATIONSHIP_COLUMNS
        );
        let batches = collect(ctx, plan).await;
        let mut rows = vec![];
        for batch in &batches {
            for i in 0..batch.num_rows() {
                rows.push((
                    strings(batch, 4)[i].clone(),
                    strings(batch, 5)[i].clone(),
                    strings(batch, 6)[i].clone(),
                    strings(batch, 7)[i].clone(),
                    strings(batch, 8)[i].clone(),
                    strings(batch, 9)[i].clone(),
                ));
                assert_eq!(confidence(batch, i), 1.0);
                // Co-declared edges carry no RED metrics or attributes.
                for column in 11..=15 {
                    assert!(batch.column(column).is_null(i));
                }
            }
        }
        rows.sort();
        Some(rows)
    }

    #[tokio::test]
    async fn co_declared_direction_follows_vocabulary() {
        let ctx = co_declared_ctx();
        // Declaration order must not matter: the vocabulary fixes direction.
        let rows = co_declared_edges(
            &ctx,
            vec![
                co_decl("host", &["host"]),
                co_decl("service", &["service"]),
                co_decl("service.instance", &["instance"]),
            ],
            false,
        )
        .await
        .unwrap();
        // The two non-NULL-instance rows share one 60s bin, so DISTINCT folds
        // them into one row per edge; the NULL-instance row witnesses nothing.
        assert_eq!(
            rows,
            vec![
                (
                    "service.instance".to_string(),
                    "i-1".to_string(),
                    "host".to_string(),
                    "h-1".to_string(),
                    "runs_on".to_string(),
                    "attribute".to_string(),
                ),
                (
                    "service.instance".to_string(),
                    "i-1".to_string(),
                    "service".to_string(),
                    "svc".to_string(),
                    "part_of".to_string(),
                    "attribute".to_string(),
                ),
            ]
        );
    }

    #[tokio::test]
    async fn co_declared_ignores_types_outside_the_vocabulary() {
        let ctx = co_declared_ctx();
        // (service, host) is not a vocabulary pair: no edge, no plan.
        let rows = co_declared_edges(
            &ctx,
            vec![co_decl("service", &["service"]), co_decl("host", &["host"])],
            false,
        )
        .await;
        assert!(rows.is_none());
    }

    #[tokio::test]
    async fn agent_edges_require_a_trace_source() {
        let ctx = co_declared_ctx();
        let declarations = || {
            vec![
                co_decl("agent", &["agent_id"]),
                co_decl("model", &["model_name"]),
                co_decl("tool", &["tool_name"]),
            ]
        };
        // A non-trace table co-declaring agent/model/tool must not fabricate
        // invocations.
        assert!(
            co_declared_edges(&ctx, declarations(), false)
                .await
                .is_none()
        );

        let rows = co_declared_edges(&ctx, declarations(), true).await.unwrap();
        assert_eq!(
            rows,
            vec![
                (
                    "agent".to_string(),
                    "agent-1".to_string(),
                    "model".to_string(),
                    "gpt".to_string(),
                    "uses".to_string(),
                    "trace".to_string(),
                ),
                (
                    "agent".to_string(),
                    "agent-1".to_string(),
                    "tool".to_string(),
                    "search".to_string(),
                    "invoked".to_string(),
                    "trace".to_string(),
                ),
            ]
        );
    }

    #[tokio::test]
    async fn real_pair_suppresses_virtual_edge() {
        let ctx = SessionContext::new();
        register_trace_table(
            &ctx,
            "trace_a",
            &[(
                "span_attributes.peer.service",
                &[Some("cart"), None, Some("cart")],
            )],
            &[
                // A sampled-out server: the client names the same peer an
                // actual pair witnesses in the same window.
                (1_000, "t1", "c1", None, CLIENT, ERROR, "frontend", 999),
                (
                    2_010,
                    "t2",
                    "s2",
                    Some("c2"),
                    SERVER,
                    UNSET,
                    "cart",
                    500_000_000,
                ),
                (2_000, "t2", "c2", None, CLIENT, UNSET, "frontend", 0),
            ],
        );
        let a = ctx.table("trace_a").await.unwrap();
        let plan = build_relationships_plan_for_test(sources(&[("a", a)]), &test_window())
            .unwrap()
            .unwrap();

        let batches = collect(&ctx, plan).await;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        // One (window, edge) row: the pair population wins; the unmatched
        // client neither adds a second row nor inflates the pair RED metrics.
        assert_eq!(total, 1);
        let batch = &batches[0];
        assert_eq!(strings(batch, 7), vec!["cart"]);
        assert_eq!(confidence(batch, 0), 1.0);
        assert_eq!(red_metrics(batch, 0), (1, 0, 0.5, 1));
        assert!(json_texts(batch, 15)[0].is_none());
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
        build_relationships_plan(
            RelationshipSources {
                traces,
                co_declared: vec![],
                declared: None,
            },
            window,
        )
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
        let plan = build_relationships_plan(
            RelationshipSources {
                traces: vec![],
                co_declared: vec![],
                declared: Some(DeclaredSource { scan }),
            },
            &window,
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
        let plan = build_relationships_plan(
            RelationshipSources {
                traces: vec![],
                co_declared: vec![],
                declared: Some(DeclaredSource { scan }),
            },
            &window,
        )
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
            RelationshipSources {
                traces: vec![CallsSource {
                    service: trace_service_decl(&[SERVICE_NAME_COLUMN]),
                    scan: trace,
                }],
                co_declared: vec![],
                declared: Some(DeclaredSource { scan: declared }),
            },
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
