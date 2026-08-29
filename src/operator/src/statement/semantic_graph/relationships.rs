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
//! branch behind the computed table — trace-derived service `calls` (with
//! virtual-node edges for unmatched clients), agent `calls` from span
//! structure, same-row co-declared edges, and the declared-edge union. Shared
//! expression helpers and the entity registry live in the parent module.

use common_catalog::consts::{
    CONFIDENCE_COLUMN, DST_ID_COLUMN, DST_TYPE_COLUMN, DURATION_COUNT_COLUMN, DURATION_MAX_COLUMN,
    DURATION_NANO_COLUMN, DURATION_SUM_COLUMN, EDGE_ATTRIBUTES_COLUMN, ENTITY_SCOPE_COLUMN,
    ERROR_COUNT_COLUMN, FRESH_UNTIL_COLUMN, GENERATION_ID_COLUMN, OBSERVED_AT_COLUMN,
    PARENT_SPAN_ID_COLUMN, PROVENANCE_COLUMN, REL_TYPE_COLUMN, REQUEST_COUNT_COLUMN,
    SPAN_ID_COLUMN, SPAN_KIND_CLIENT, SPAN_KIND_COLUMN, SPAN_KIND_SERVER, SPAN_STATUS_CODE_COLUMN,
    SPAN_STATUS_ERROR, SRC_ID_COLUMN, SRC_TYPE_COLUMN, TRACE_ID_COLUMN, TRACE_TIMESTAMP_COLUMN,
    UNMATCHED_COUNT_COLUMN, VALID_FROM_COLUMN, VALID_UNTIL_COLUMN, WINDOW_END_COLUMN,
    WINDOW_START_COLUMN,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::dataframe::DataFrame;
use datafusion::functions::core as core_fns;
use datafusion::functions_aggregate::expr_fn::{bool_or, count, max, min, sum};
use datafusion::functions_window::expr_fn::row_number;
use datafusion_common::{Result as DfResult, ScalarValue};
use datafusion_expr::{Expr, ExprFunctionExt, JoinType, LogicalPlan, cast, ident, lit, when};

use crate::statement::semantic_graph::conventions::{
    CONNECTION_TYPE_DATABASE, CONNECTION_TYPE_VIRTUAL_NODE, Conventions, ENTITY_TYPE_GEN_AI_AGENT,
    ENTITY_TYPE_SERVICE, PROVENANCE_ATTRIBUTE, PROVENANCE_TRACE, REL_TYPE_CALLS,
};
use crate::statement::semantic_graph::{
    DECLARED_EDGE_IDENTITY_COLUMNS, EntityDeclaration, GraphQueryWindow, bin_interval, bin_ms,
    conventions, declaration_predicate, entity_id_expr, interval, null_json, parse_json_expr, qcol,
    union_all, unnest_rows,
};

/// The embedded conventions, with a broken file surfaced as a plan error.
fn builtin() -> DfResult<&'static Conventions> {
    conventions().map_err(datafusion_common::DataFusionError::Internal)
}

/// The projected columns of `semantic_relationships`, in order. Every derived
/// branch and the declared-edge branch must project exactly these so the
/// top-level `UNION ALL` type-aligns; `build_relationships_plan` re-selects
/// them over the union to enforce the contract. (The physical declared table
/// additionally stores `valid_from`/`valid_until`, which feed the validity
/// filter and the projected window columns.)
const RELATIONSHIP_COLUMNS: [&str; 18] = [
    OBSERVED_AT_COLUMN,
    WINDOW_START_COLUMN,
    WINDOW_END_COLUMN,
    FRESH_UNTIL_COLUMN,
    SRC_TYPE_COLUMN,
    SRC_ID_COLUMN,
    DST_TYPE_COLUMN,
    DST_ID_COLUMN,
    REL_TYPE_COLUMN,
    PROVENANCE_COLUMN,
    CONFIDENCE_COLUMN,
    REQUEST_COUNT_COLUMN,
    UNMATCHED_COUNT_COLUMN,
    ERROR_COUNT_COLUMN,
    DURATION_SUM_COLUMN,
    DURATION_COUNT_COLUMN,
    DURATION_MAX_COLUMN,
    EDGE_ATTRIBUTES_COLUMN,
];

/// A child server span starts no earlier than 5 minutes before its client span
/// (clock-skew allowance) and no later than 1 hour after it; the bounds keep the
/// join windowed instead of pairing arbitrarily distant spans of a long-lived
/// trace.
const CHILD_SPAN_EARLY_NANOS: i64 = 5 * 60 * 1_000_000_000;
const CHILD_SPAN_LATE_NANOS: i64 = 60 * 60 * 1_000_000_000;

/// A trace table's scan paired with the entity declarations its derivations
/// key on — a unit of `build_relationships_plan`. The `service` declaration
/// feeds service calls, the `agent` declaration agent calls; the two are
/// independent, so a table whose service declaration is unusable still
/// derives agent edges (and vice versa).
pub struct CallsSource {
    pub service: Option<EntityDeclaration>,
    pub agent: Option<EntityDeclaration>,
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

/// Builds the `semantic_relationships` plan: the service-calls, agent-calls,
/// co-declared, and declared-edge branches unioned and re-projected to the
/// 18-column contract. Returns `None` when no source can contribute edges, so
/// the computed table streams empty.
pub fn build_relationships_plan(
    sources: RelationshipSources,
    window: &GraphQueryWindow,
) -> DfResult<Option<LogicalPlan>> {
    let mut union_df = calls_branch(&sources.traces, window)?;
    if let Some(agent_calls) = agent_calls_branch(&sources.traces, window)? {
        union_df = union_all(union_df, agent_calls)?;
    }
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
    let vocabulary = builtin()?;
    let mut union_df: Option<DataFrame> = None;
    for source in sources {
        let find = |ty: &str| source.declarations.iter().find(|d| d.entity_type == ty);
        let mut pairs = Vec::new();
        for rule in &vocabulary.co_declared_edges {
            if let (Some(src), Some(dst)) = (find(&rule.src), find(&rule.dst)) {
                pairs.push((src, dst, rule.rel.as_str(), PROVENANCE_ATTRIBUTE));
            }
        }
        if source.is_trace {
            // Agent edges are tied to span structure by the RFC: a non-trace
            // table co-declaring these types must not fabricate invocations.
            for rule in &vocabulary.trace_co_declared_edges {
                if let (Some(src), Some(dst)) = (find(&rule.src), find(&rule.dst)) {
                    pairs.push((src, dst, rule.rel.as_str(), PROVENANCE_TRACE));
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
                let valid = declaration_predicate(src).and(declaration_predicate(dst));
                vec![
                    valid,
                    bin.clone(),
                    bin.clone(),
                    bin.clone() + bin_interval(),
                    bin.clone() + bin_interval(),
                    lit(src.entity_type.as_str()),
                    entity_id_expr(&src.id_columns, src.id_qualifier.as_deref(), &|c| ident(c)),
                    lit(dst.entity_type.as_str()),
                    entity_id_expr(&dst.id_columns, dst.id_qualifier.as_deref(), &|c| ident(c)),
                    lit(*rel_type),
                    lit(*provenance),
                    lit(1.0_f64),
                    null_i64(),
                    null_i64(),
                    null_i64(),
                    lit(ScalarValue::Float64(None)),
                    null_i64(),
                    lit(ScalarValue::Float64(None)),
                    null_json(),
                ]
            })
            .collect();

        let branch = unnest_rows(
            source.scan,
            window_predicate,
            CO_DECLARED_VALID_COLUMN,
            &RELATIONSHIP_COLUMNS,
            rows,
        )?;
        union_df = union_all(union_df, branch)?;
    }
    // The per-source DISTINCT inside `unnest_rows` cannot see other sources:
    // two tables witnessing the same edge in the same window must still fold
    // into one row per (window, edge).
    union_df.map(DataFrame::distinct).transpose()
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
        core_fns::coalesce().call(vec![ident(VALID_FROM_COLUMN), ident(OBSERVED_AT_COLUMN)]);
    let eff_valid_until =
        core_fns::coalesce().call(vec![ident(VALID_UNTIL_COLUMN), window.observed_end()]);

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
            ident(GENERATION_ID_COLUMN).sort(false, false),
            ident(ENTITY_SCOPE_COLUMN).sort(false, false),
        ])
        .build()?
        .alias(DECLARED_REVISION_COLUMN);

    // The as-of filter already bounds eff_valid_from below the window's end;
    // only the retirement side of the overlap remains to check.
    let still_valid = ident(VALID_UNTIL_COLUMN)
        .is_null()
        .or(ident(VALID_UNTIL_COLUMN).gt(window.observed_start()));
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
            eff_valid_from.alias(WINDOW_START_COLUMN),
            eff_valid_until.clone().alias(WINDOW_END_COLUMN),
            eff_valid_until.alias(FRESH_UNTIL_COLUMN),
            tag_utf8(SRC_TYPE_COLUMN),
            tag_utf8(SRC_ID_COLUMN),
            tag_utf8(DST_TYPE_COLUMN),
            tag_utf8(DST_ID_COLUMN),
            tag_utf8(REL_TYPE_COLUMN),
            tag_utf8(PROVENANCE_COLUMN),
            ident(CONFIDENCE_COLUMN),
            ident(REQUEST_COUNT_COLUMN),
            // A hand-declared edge asserts a dependency, not an observation:
            // it has no span population to leave unmatched or to time.
            lit(ScalarValue::Int64(None)).alias(UNMATCHED_COUNT_COLUMN),
            ident(ERROR_COUNT_COLUMN),
            ident(DURATION_SUM_COLUMN),
            ident(DURATION_COUNT_COLUMN),
            lit(ScalarValue::Float64(None)).alias(DURATION_MAX_COLUMN),
            ident(EDGE_ATTRIBUTES_COLUMN),
        ])
}

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
/// to the previous self-join. Returns `None` when no trace table has a
/// usable `service` declaration.
///
/// A client with no matching server span is an edge to a **virtual node**
/// named by the conventions' virtual-destination candidates, with the
/// client's own status/duration
/// and `confidence < 1.0`. Real pairs win: when a window's edge key holds any
/// pair, the edge reports only the pair population (the RFC pins RED metrics
/// to observed span pairs) and the unmatched clients are suppressed, so one
/// `(window, edge)` never yields two rows. `unmatched_count` stays outside that
/// rule and counts them on the same row, or a callee that stopped answering
/// would be indistinguishable from a caller that stopped calling.
fn calls_branch(traces: &[CallsSource], window: &GraphQueryWindow) -> DfResult<Option<DataFrame>> {
    let mut clients: Option<DataFrame> = None;
    let mut servers: Option<DataFrame> = None;
    for trace in traces {
        let Some(service) = &trace.service else {
            continue;
        };
        clients = union_all(clients, client_spans(service, &trace.scan, window)?)?;
        servers = union_all(servers, server_spans(service, trace.scan.clone(), window)?)?;
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
            bin_ms(qcol("client", TRACE_TIMESTAMP_COLUMN)).alias(OBSERVED_AT_COLUMN),
            qcol("client", SRC_ID_COLUMN).alias(SRC_ID_COLUMN),
            core_fns::coalesce()
                .call(vec![
                    qcol("server", DST_ID_COLUMN),
                    qcol("client", "virtual_dst"),
                ])
                .alias(DST_ID_COLUMN),
            qcol("server", TRACE_ID_COLUMN)
                .is_not_null()
                .alias("paired"),
            qcol("server", SPAN_STATUS_CODE_COLUMN).alias("server_status"),
            qcol("server", DURATION_NANO_COLUMN).alias("server_duration_nano"),
            qcol("client", SPAN_STATUS_CODE_COLUMN).alias("client_status"),
            qcol("client", DURATION_NANO_COLUMN).alias("client_duration_nano"),
            qcol("client", "virtual_conn").alias("virtual_conn"),
        ])?
        // No destination means no edge; a self-call is not an edge between
        // two distinct entities.
        .filter(
            ident(DST_ID_COLUMN)
                .is_not_null()
                .and(ident(SRC_ID_COLUMN).not_eq(ident(DST_ID_COLUMN))),
        )?;

    let paired = ident("paired");
    let df = observations
        .aggregate(
            vec![
                ident(OBSERVED_AT_COLUMN),
                ident(SRC_ID_COLUMN),
                ident(DST_ID_COLUMN),
            ],
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
                max(ident("server_duration_nano"))
                    .filter(paired.clone())
                    .build()?
                    .alias("pair_duration_max_nano"),
                count(lit(1))
                    .filter(!paired.clone())
                    .build()?
                    .alias(UNMATCHED_COUNT_COLUMN),
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
                max(ident("client_duration_nano"))
                    .filter(!paired.clone())
                    .build()?
                    .alias("unmatched_duration_max_nano"),
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
            ident(OBSERVED_AT_COLUMN),
            ident(OBSERVED_AT_COLUMN).alias(WINDOW_START_COLUMN),
            (ident(OBSERVED_AT_COLUMN) + bin_interval()).alias(WINDOW_END_COLUMN),
            (ident(OBSERVED_AT_COLUMN) + bin_interval()).alias(FRESH_UNTIL_COLUMN),
            lit(ENTITY_TYPE_SERVICE).alias(SRC_TYPE_COLUMN),
            ident(SRC_ID_COLUMN),
            lit(ENTITY_TYPE_SERVICE).alias(DST_TYPE_COLUMN),
            ident(DST_ID_COLUMN),
            lit(REL_TYPE_CALLS).alias(REL_TYPE_COLUMN),
            lit(PROVENANCE_TRACE).alias(PROVENANCE_COLUMN),
            real_wins(lit(1.0_f64), lit(VIRTUAL_NODE_CONFIDENCE))?.alias(CONFIDENCE_COLUMN),
            real_wins(ident("pair_count"), ident(UNMATCHED_COUNT_COLUMN))?
                .alias(REQUEST_COUNT_COLUMN),
            // Deliberately outside `real_wins`: reporting the clients the pair
            // population swallowed is the whole point of the column.
            ident(UNMATCHED_COUNT_COLUMN),
            real_wins(ident("pair_errors"), ident("unmatched_errors"))?.alias(ERROR_COUNT_COLUMN),
            // duration sums in nanoseconds; the contract column is seconds.
            (cast(
                real_wins(
                    ident("pair_duration_nano"),
                    ident("unmatched_duration_nano"),
                )?,
                DataType::Float64,
            ) / lit(1e9_f64))
            .alias(DURATION_SUM_COLUMN),
            real_wins(ident("pair_count"), ident(UNMATCHED_COUNT_COLUMN))?
                .alias(DURATION_COUNT_COLUMN),
            // A pair is timed by the server span, an unmatched client by its own
            // span (network wait included). Mixing the two would make the max
            // describe a different population than duration_sum/duration_count.
            (cast(
                real_wins(
                    ident("pair_duration_max_nano"),
                    ident("unmatched_duration_max_nano"),
                )?,
                DataType::Float64,
            ) / lit(1e9_f64))
            .alias(DURATION_MAX_COLUMN),
            real_wins(null_json(), virtual_attrs_expr()?)?.alias(EDGE_ATTRIBUTES_COLUMN),
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
        ident("virtual_conn").eq(lit(CONNECTION_TYPE_DATABASE)),
        parse_json_expr(lit(format!(
            r#"{{"connection_type":"{CONNECTION_TYPE_DATABASE}"}}"#
        ))),
    )
    .otherwise(parse_json_expr(lit(format!(
        r#"{{"connection_type":"{CONNECTION_TYPE_VIRTUAL_NODE}"}}"#
    ))))
}

/// The window + span-kind + identity predicate shared by both join sides.
/// `strict` bounds the scan to the source window; the non-strict side widens
/// by the join's time-proximity allowance (the join bounds reference the other
/// side's timestamp and cannot prune this side's scan on their own).
fn span_predicate(service: &EntityDeclaration, window: &GraphQueryWindow, strict: bool) -> Expr {
    let ts = ident(TRACE_TIMESTAMP_COLUMN);
    let window_predicate = if strict {
        ts.clone()
            .gt_eq(window.source_start())
            .and(ts.lt(window.source_end()))
    } else {
        ts.clone()
            .gt_eq(window.source_start() - interval(CHILD_SPAN_EARLY_NANOS))
            .and(ts.lt(window.source_end() + interval(CHILD_SPAN_LATE_NANOS)))
    };
    // An absent identity component identifies nothing, on either endpoint.
    window_predicate.and(declaration_predicate(service))
}

/// One trace table's client spans, normalized to
/// A trace table's `duration_nano` is UInt64 on tables created before the
/// signed-integer ingest change and Int64 after it. The per-table selects are
/// unioned, and those two have no common integer type.
fn duration_nano_expr() -> Expr {
    cast(ident(DURATION_NANO_COLUMN), DataType::Int64).alias(DURATION_NANO_COLUMN)
}

/// `(timestamp, trace_id, span_id, src_id, status_code, duration_nano,
/// virtual_dst, virtual_conn)`. `src_id` is built from the table's `service`
/// declaration, so edges land on exactly the entity ids the registry emits (a
/// composite identity renders the same sorted `k=v` form); the per-table
/// projection is what lets tables with different declarations union.
fn client_spans(
    service: &EntityDeclaration,
    scan: &DataFrame,
    window: &GraphQueryWindow,
) -> DfResult<DataFrame> {
    // Attribute columns are dynamic in `greptime_trace_v1` (created on first
    // use), so only candidate columns present in the table's schema
    // participate. NULLIF('') lets an empty value fall through to the next
    // candidate.
    let present: Vec<(Expr, &str)> = {
        let schema = scan.schema();
        builtin()?
            .virtual_dst_candidates
            .iter()
            .filter(|candidate| schema.has_column_with_unqualified_name(&candidate.column))
            .map(|candidate| {
                let value = core_fns::nullif().call(vec![
                    cast(ident(&candidate.column), DataType::Utf8),
                    lit(""),
                ]);
                (value, candidate.connection_type.as_str())
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

    scan.clone()
        .filter(
            ident(SPAN_KIND_COLUMN)
                .eq(lit(SPAN_KIND_CLIENT))
                .and(span_predicate(service, window, true)),
        )?
        .select(vec![
            ident(TRACE_TIMESTAMP_COLUMN),
            ident(TRACE_ID_COLUMN),
            ident(SPAN_ID_COLUMN),
            // The cast inside entity_id_expr also normalizes tag columns, which
            // come out of the storage engine dictionary-encoded.
            entity_id_expr(&service.id_columns, service.id_qualifier.as_deref(), &|c| {
                ident(c)
            })
            .alias(SRC_ID_COLUMN),
            ident(SPAN_STATUS_CODE_COLUMN),
            duration_nano_expr(),
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
            entity_id_expr(&service.id_columns, service.id_qualifier.as_deref(), &|c| {
                ident(c)
            })
            .alias(DST_ID_COLUMN),
            ident(SPAN_STATUS_CODE_COLUMN),
            duration_nano_expr(),
        ])
}

/// The `parent_agent calls agent` derivation over trace tables declaring an
/// `agent` entity: pair each span with its child span (no span-kind filter —
/// agent spans are typically INTERNAL) across all agent-declaring tables, keep
/// pairs whose agent identities differ, and aggregate RED metrics per 60s
/// window. Anchored like the service derivation on the caller: the parent side
/// is bounded to the source window and stamps `observed_at`, the child side
/// widens by the time-proximity allowance and supplies status/duration.
fn agent_calls_branch(
    traces: &[CallsSource],
    window: &GraphQueryWindow,
) -> DfResult<Option<DataFrame>> {
    let mut parents: Option<DataFrame> = None;
    let mut children: Option<DataFrame> = None;
    for trace in traces {
        let Some(agent) = &trace.agent else {
            continue;
        };
        let parent = trace
            .scan
            .clone()
            .filter(span_predicate(agent, window, true))?
            .select(vec![
                ident(TRACE_TIMESTAMP_COLUMN),
                ident(TRACE_ID_COLUMN),
                ident(SPAN_ID_COLUMN),
                entity_id_expr(&agent.id_columns, agent.id_qualifier.as_deref(), &|c| {
                    ident(c)
                })
                .alias(SRC_ID_COLUMN),
            ])?;
        let child = trace
            .scan
            .clone()
            .filter(span_predicate(agent, window, false))?
            .select(vec![
                ident(TRACE_TIMESTAMP_COLUMN),
                ident(TRACE_ID_COLUMN),
                ident(PARENT_SPAN_ID_COLUMN),
                entity_id_expr(&agent.id_columns, agent.id_qualifier.as_deref(), &|c| {
                    ident(c)
                })
                .alias(DST_ID_COLUMN),
                ident(SPAN_STATUS_CODE_COLUMN),
                duration_nano_expr(),
            ])?;
        parents = union_all(parents, parent)?;
        children = union_all(children, child)?;
    }
    let (Some(parents), Some(children)) = (parents, children) else {
        return Ok(None);
    };

    let parent = parents.alias("parent")?;
    let child = children.alias("child")?;
    let join_conditions = vec![
        qcol("parent", TRACE_ID_COLUMN).eq(qcol("child", TRACE_ID_COLUMN)),
        qcol("child", PARENT_SPAN_ID_COLUMN).eq(qcol("parent", SPAN_ID_COLUMN)),
        qcol("child", TRACE_TIMESTAMP_COLUMN)
            .gt_eq(qcol("parent", TRACE_TIMESTAMP_COLUMN) - interval(CHILD_SPAN_EARLY_NANOS)),
        qcol("child", TRACE_TIMESTAMP_COLUMN)
            .lt_eq(qcol("parent", TRACE_TIMESTAMP_COLUMN) + interval(CHILD_SPAN_LATE_NANOS)),
    ];

    let df = parent
        .join_on(child, JoinType::Inner, join_conditions)?
        .select(vec![
            bin_ms(qcol("parent", TRACE_TIMESTAMP_COLUMN)).alias(OBSERVED_AT_COLUMN),
            qcol("parent", SRC_ID_COLUMN).alias(SRC_ID_COLUMN),
            qcol("child", DST_ID_COLUMN).alias(DST_ID_COLUMN),
            qcol("child", SPAN_STATUS_CODE_COLUMN).alias("status_code"),
            qcol("child", DURATION_NANO_COLUMN).alias(DURATION_NANO_COLUMN),
        ])?
        // A sub-span of the same agent is internal structure, not a call
        // between two agents.
        .filter(ident(SRC_ID_COLUMN).not_eq(ident(DST_ID_COLUMN)))?
        .aggregate(
            vec![
                ident(OBSERVED_AT_COLUMN),
                ident(SRC_ID_COLUMN),
                ident(DST_ID_COLUMN),
            ],
            vec![
                count(lit(1)).alias(REQUEST_COUNT_COLUMN),
                count(lit(1))
                    .filter(ident("status_code").eq(lit(SPAN_STATUS_ERROR)))
                    .build()?
                    .alias(ERROR_COUNT_COLUMN),
                sum(ident(DURATION_NANO_COLUMN)).alias("duration_nano_sum"),
                max(ident(DURATION_NANO_COLUMN)).alias("duration_nano_max"),
            ],
        )?
        .select(vec![
            ident(OBSERVED_AT_COLUMN),
            ident(OBSERVED_AT_COLUMN).alias(WINDOW_START_COLUMN),
            (ident(OBSERVED_AT_COLUMN) + bin_interval()).alias(WINDOW_END_COLUMN),
            (ident(OBSERVED_AT_COLUMN) + bin_interval()).alias(FRESH_UNTIL_COLUMN),
            lit(ENTITY_TYPE_GEN_AI_AGENT).alias(SRC_TYPE_COLUMN),
            ident(SRC_ID_COLUMN),
            lit(ENTITY_TYPE_GEN_AI_AGENT).alias(DST_TYPE_COLUMN),
            ident(DST_ID_COLUMN),
            lit(REL_TYPE_CALLS).alias(REL_TYPE_COLUMN),
            lit(PROVENANCE_TRACE).alias(PROVENANCE_COLUMN),
            lit(1.0_f64).alias(CONFIDENCE_COLUMN),
            ident(REQUEST_COUNT_COLUMN),
            // The agent join is an inner join: an unanswered delegation leaves
            // no pair to count, so there is no unmatched population here.
            lit(ScalarValue::Int64(None)).alias(UNMATCHED_COUNT_COLUMN),
            ident(ERROR_COUNT_COLUMN),
            (cast(ident("duration_nano_sum"), DataType::Float64) / lit(1e9_f64))
                .alias(DURATION_SUM_COLUMN),
            ident(REQUEST_COUNT_COLUMN).alias(DURATION_COUNT_COLUMN),
            (cast(ident("duration_nano_max"), DataType::Float64) / lit(1e9_f64))
                .alias(DURATION_MAX_COLUMN),
            null_json().alias(EDGE_ATTRIBUTES_COLUMN),
        ])?;
    Ok(Some(df))
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
        register_typed_trace_table(ctx, name, extra, spans, DataType::UInt64)
    }

    fn register_typed_trace_table(
        ctx: &SessionContext,
        name: &str,
        extra: &[(&str, &[Option<&str>])],
        spans: &[Span<'_>],
        duration_type: DataType,
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
            Field::new(DURATION_NANO_COLUMN, duration_type.clone(), false),
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
            match duration_type {
                DataType::Int64 => Arc::new(Int64Array::from(
                    spans.iter().map(|s| s.7 as i64).collect::<Vec<_>>(),
                )) as ArrayRef,
                _ => Arc::new(UInt64Array::from(
                    spans.iter().map(|s| s.7).collect::<Vec<_>>(),
                )),
            },
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
            id_qualifier: None,
            superseded_by_columns: vec![],
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
                service: Some(trace_service_decl(&[SERVICE_NAME_COLUMN])),
                agent: None,
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

        assert_eq!(red_metrics(batch, 0), (2, 1, 2.0, 2));
        // The slower of the two paired server spans.
        assert_eq!(duration_max(batch, 0), 1.5);
        // Derived calls edges carry no attributes: a typed-JSON NULL.
        assert!(json_texts(batch, 17)[0].is_none());
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
            .column(14)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(row);
        (i64_at(11), i64_at(13), duration_sum, i64_at(15))
    }

    fn unmatched_count(batch: &RecordBatch, row: usize) -> i64 {
        batch
            .column(12)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(row)
    }

    fn duration_max(batch: &RecordBatch, row: usize) -> f64 {
        batch
            .column(16)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(row)
    }

    fn confidence(batch: &RecordBatch, row: usize) -> f64 {
        batch
            .column(10)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(row)
    }

    fn sources(scans: &[DataFrame]) -> Vec<CallsSource> {
        scans
            .iter()
            .map(|scan| CallsSource {
                service: Some(trace_service_decl(&[SERVICE_NAME_COLUMN])),
                agent: None,
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
        register_typed_trace_table(
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
            DataType::Int64,
        );
        let a = ctx.table("trace_a").await.unwrap();
        let b = ctx.table("trace_b").await.unwrap();
        let plan = build_relationships_plan_for_test(sources(&[a, b]), &test_window())
            .unwrap()
            .unwrap();

        let batches = collect(&ctx, plan).await;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
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
        let plan = build_relationships_plan_for_test(sources(&[a, b]), &test_window())
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
        let plan = build_relationships_plan_for_test(sources(&[a]), &test_window())
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
        // With no pair to prefer, both columns come from the client spans.
        assert_eq!(duration_max(batch, 0), 0.25);
        assert_eq!(unmatched_count(batch, 0), 1);
        assert_eq!(
            json_texts(batch, 17),
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
                // An empty high-precedence value must fall through to the next
                // candidate, whose match maps connection_type to `database`.
                ("span_attributes.service.peer.name", &[Some("")]),
                ("span_attributes.db.namespace", &[Some("mysql")]),
            ],
            &[(1_000, "t1", "c1", None, CLIENT, UNSET, "frontend", 100)],
        );
        let a = ctx.table("trace_a").await.unwrap();
        let plan = build_relationships_plan_for_test(sources(&[a]), &test_window())
            .unwrap()
            .unwrap();

        let batches = collect(&ctx, plan).await;
        let batch = &batches[0];
        assert_eq!(strings(batch, 7), vec!["mysql"]);
        assert_eq!(
            json_texts(batch, 17),
            vec![Some(r#"{"connection_type":"database"}"#.to_string())]
        );
    }

    #[tokio::test]
    async fn agent_calls_from_parent_child_spans() {
        let ctx = SessionContext::new();
        const INTERNAL: &str = "SPAN_KIND_INTERNAL";
        register_trace_table(
            &ctx,
            "agent_traces",
            &[(
                "agent_id",
                &[
                    Some("orchestrator"),
                    Some("researcher"),
                    Some("researcher"),
                    Some("researcher"),
                ],
            )],
            &[
                // orchestrator delegates to researcher twice (one errored);
                // the researcher's own sub-span is same-agent structure.
                (1_000, "t1", "p1", None, INTERNAL, UNSET, "app", 0),
                (
                    1_010,
                    "t1",
                    "a1",
                    Some("p1"),
                    INTERNAL,
                    ERROR,
                    "app",
                    2_000_000_000,
                ),
                (
                    2_000,
                    "t1",
                    "a2",
                    Some("p1"),
                    INTERNAL,
                    UNSET,
                    "app",
                    1_000_000_000,
                ),
                (1_020, "t1", "a3", Some("a1"), INTERNAL, UNSET, "app", 100),
            ],
        );
        let scan = ctx.table("agent_traces").await.unwrap();
        let plan = build_relationships_plan(
            RelationshipSources {
                traces: vec![CallsSource {
                    service: None,
                    agent: Some(co_decl("gen_ai.agent", &["agent_id"])),
                    scan,
                }],
                co_declared: vec![],
                declared: None,
            },
            &test_window(),
        )
        .unwrap()
        .unwrap();

        let batches = collect(&ctx, plan).await;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        // The same-agent child (a3 under a1) is excluded; agent edges need
        // no service declaration.
        assert_eq!(total, 1);
        let batch = &batches[0];
        assert_eq!(strings(batch, 4), vec!["gen_ai.agent"]);
        assert_eq!(strings(batch, 5), vec!["orchestrator"]);
        assert_eq!(strings(batch, 6), vec!["gen_ai.agent"]);
        assert_eq!(strings(batch, 7), vec!["researcher"]);
        assert_eq!(strings(batch, 8), vec!["calls"]);
        assert_eq!(strings(batch, 9), vec!["trace"]);
        assert_eq!(confidence(batch, 0), 1.0);
        assert_eq!(red_metrics(batch, 0), (2, 1, 3.0, 2));
        // The child spans carry the durations, so the max is real here; an
        // unanswered delegation leaves no row to count, so there is no
        // unmatched population.
        assert_eq!(duration_max(batch, 0), 2.0);
        assert!(batch.column(12).is_null(0));
    }

    #[tokio::test]
    async fn agent_calls_anchor_on_the_parent_span() {
        let ctx = SessionContext::new();
        const INTERNAL: &str = "SPAN_KIND_INTERNAL";
        // The parent is inside the queried window; the child starts after its
        // end but within the proximity allowance and must still pair.
        register_trace_table(
            &ctx,
            "agent_traces",
            &[("agent_id", &[Some("orchestrator"), Some("researcher")])],
            &[
                (599_000, "t1", "p1", None, INTERNAL, UNSET, "app", 0),
                (
                    601_000,
                    "t1",
                    "a1",
                    Some("p1"),
                    INTERNAL,
                    UNSET,
                    "app",
                    500_000_000,
                ),
            ],
        );
        let scan = ctx.table("agent_traces").await.unwrap();
        let plan = build_relationships_plan(
            RelationshipSources {
                traces: vec![CallsSource {
                    service: None,
                    agent: Some(co_decl("gen_ai.agent", &["agent_id"])),
                    scan,
                }],
                co_declared: vec![],
                declared: None,
            },
            &test_window(),
        )
        .unwrap()
        .unwrap();

        let batches = collect(&ctx, plan).await;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1);
        let batch = &batches[0];
        // observed_at is the parent's bin.
        assert_eq!(ts_values(batch, 0), vec![540_000]);
        assert_eq!(red_metrics(batch, 0), (1, 0, 0.5, 1));
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
        for name in ["co_metrics", "co_metrics_2"] {
            ctx.register_table(
                name,
                Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch.clone()]]).unwrap()),
            )
            .unwrap();
        }
        ctx
    }

    fn co_decl(entity_type: &str, id_columns: &[&str]) -> EntityDeclaration {
        EntityDeclaration {
            schema: "public".to_string(),
            table: "co_metrics".to_string(),
            time_index: "ts".to_string(),
            entity_type: entity_type.to_string(),
            id_columns: id_columns.iter().map(|s| s.to_string()).collect(),
            id_qualifier: None,
            superseded_by_columns: vec![],
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
                for column in 11..=17 {
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
    async fn co_declared_edge_folds_across_sources() {
        let ctx = co_declared_ctx();
        let declarations = || {
            vec![
                co_decl("service.instance", &["instance"]),
                co_decl("host", &["host"]),
            ]
        };
        let source = |scan| CoDeclaredSource {
            declarations: declarations(),
            is_trace: false,
            scan,
        };
        let a = ctx.table("co_metrics").await.unwrap();
        let b = ctx.table("co_metrics_2").await.unwrap();
        let plan = build_relationships_plan(
            RelationshipSources {
                traces: vec![],
                co_declared: vec![source(a), source(b)],
                declared: None,
            },
            &test_window(),
        )
        .unwrap()
        .unwrap();

        let batches = collect(&ctx, plan).await;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        // Both tables witness the same runs_on edge in the same window: one
        // row per (window, edge), not one per source.
        assert_eq!(total, 1);
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
                co_decl("gen_ai.agent", &["agent_id"]),
                co_decl("gen_ai.model", &["model_name"]),
                co_decl("gen_ai.tool", &["tool_name"]),
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
                    "gen_ai.agent".to_string(),
                    "agent-1".to_string(),
                    "gen_ai.model".to_string(),
                    "gpt".to_string(),
                    "uses".to_string(),
                    "trace".to_string(),
                ),
                (
                    "gen_ai.agent".to_string(),
                    "agent-1".to_string(),
                    "gen_ai.tool".to_string(),
                    "search".to_string(),
                    "invokes".to_string(),
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
                // actual pair witnesses in the same window. Its duration is the
                // larger of the two, and it is a client-side measurement.
                (
                    1_000,
                    "t1",
                    "c1",
                    None,
                    CLIENT,
                    ERROR,
                    "frontend",
                    9_000_000_000,
                ),
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
        let plan = build_relationships_plan_for_test(sources(&[a]), &test_window())
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
        // The suppressed client is timed client-side and is the longer of the
        // two: taking it would make duration_max describe a population the rest
        // of the RED columns exclude.
        assert_eq!(duration_max(batch, 0), 0.5);
        // ...but it is still counted, or "the callee stopped answering" looks
        // exactly like "the caller stopped calling".
        assert_eq!(unmatched_count(batch, 0), 1);
        assert!(json_texts(batch, 17)[0].is_none());
    }

    #[tokio::test]
    async fn calls_endpoints_follow_service_declaration() {
        let ctx = trace_table_ctx();
        let trace = ctx.table("opentelemetry_traces").await.unwrap();
        let plan = build_relationships_plan_for_test(
            vec![CallsSource {
                service: Some(trace_service_decl(&[
                    SERVICE_NAME_COLUMN,
                    "service_namespace",
                ])),
                agent: None,
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
        // Composite service identity renders exactly as the registry emits it,
        // so edges land on registry entity ids.
        assert_eq!(strings(batch, 5), vec!["frontend,ns1"]);
        assert_eq!(strings(batch, 7), vec!["cart,ns1"]);
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
            let attributes = json_texts(batch, 17);
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
                    service: Some(trace_service_decl(&[SERVICE_NAME_COLUMN])),
                    agent: None,
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
