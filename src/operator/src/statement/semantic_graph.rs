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

//! The entity-relationship graph: the physical declared-edge table DDL and the
//! typed DataFusion plan builders for the read-time derivation behind the
//! computed `semantic_entities` / `semantic_relationships` tables.
//!
//! In OSS the graph is derived at read time, so the *only* stored part is the
//! declared-edge table: edges a user asserts by hand (`provenance = 'declared'`).
//! It lives in `greptime_private`, uses the default `LastRow` merge (last-write-wins
//! on the primary key — an idempotent upsert) and a sliding TTL. Its schema is also
//! the shape the enterprise materialiser (M3) will upsert derived edges into, so the
//! computed `semantic_relationships` can later swap read-time derivation for a
//! table scan without changing the query surface. See
//! `docs/rfcs/2026-06-25-entity-relationships-and-graph-query.md`.
//!
//! The derivation is built as typed [`Expr`]s over [`DataFrame`]s (never as SQL
//! text), so user-controlled identifiers are plain values — no quoting or SQL
//! injection surface — and the plans compose with DataFusion's optimizer,
//! including filter pushdown into the source table scans.

use std::sync::{Arc, LazyLock};

use api::v1::{ColumnDataType, ColumnDef, CreateTableExpr, SemanticType};
use common_catalog::consts::{
    DEFAULT_PRIVATE_SCHEMA_NAME, DURATION_NANO_COLUMN, PARENT_SPAN_ID_COLUMN,
    SEMANTIC_RELATIONSHIPS_DECLARED_TABLE_NAME, SERVICE_NAME_COLUMN, SPAN_ID_COLUMN,
    SPAN_KIND_CLIENT, SPAN_KIND_COLUMN, SPAN_KIND_SERVER, SPAN_STATUS_CODE_COLUMN,
    SPAN_STATUS_ERROR, TRACE_ID_COLUMN, TRACE_TIMESTAMP_COLUMN,
};
use common_function::function::FunctionContext;
use common_function::function_registry::FUNCTION_REGISTRY;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::dataframe::DataFrame;
use datafusion::functions::{core as core_fns, datetime as datetime_fns, string as string_fns};
use datafusion::functions_aggregate::expr_fn::{count, sum};
use datafusion_common::{Column, Result as DfResult, ScalarValue};
use datafusion_expr::{Expr, ExprFunctionExt, JoinType, LogicalPlan, ScalarUDF, cast, ident, lit};
use store_api::mito_engine_options::TTL_KEY;

/// Time index column: the timestamp an edge observation was recorded at.
const OBSERVED_AT_COLUMN: &str = "observed_at";
/// Default retention for the declared-edge table; expiry slides the topology
/// window (New Relic / Datadog treat derived topology as a sliding window).
const DECLARED_RELATIONSHIPS_TTL: &str = "30d";

/// The primary-key (tag) columns, in key order. Starting with the source endpoint
/// makes out-edge lookup (`WHERE src_type=? AND src_id=?`) a key-prefix scan;
/// `provenance` and `generation_id` are in the key so a declared edge and a
/// (future) derived edge for the same pair coexist without clobbering.
const PRIMARY_KEY_COLUMNS: [&str; 8] = [
    "src_type",
    "src_id",
    "rel_type",
    "dst_type",
    "dst_id",
    "provenance",
    "scope",
    "generation_id",
];

fn column(
    name: &str,
    data_type: ColumnDataType,
    semantic_type: SemanticType,
    nullable: bool,
) -> ColumnDef {
    ColumnDef {
        name: name.to_string(),
        data_type: data_type as i32,
        is_nullable: nullable,
        default_constraint: vec![],
        semantic_type: semantic_type as i32,
        comment: String::new(),
        datatype_extension: None,
        options: None,
    }
}

fn tag(name: &str) -> ColumnDef {
    column(name, ColumnDataType::String, SemanticType::Tag, false)
}

fn field(name: &str, data_type: ColumnDataType) -> ColumnDef {
    column(name, data_type, SemanticType::Field, true)
}

/// Builds the `CREATE TABLE` request for the declared-edge table. Columns mirror
/// the computed `semantic_relationships` shape (temporal window + endpoints +
/// provenance/confidence + RED metrics) plus the declared-only business validity
/// window (`valid_from` / `valid_until`), which — unlike TTL (physical retention)
/// — expresses whether a hand-declared edge is still in effect.
pub fn build_declared_relationships_expr(catalog: &str) -> CreateTableExpr {
    let column_defs = vec![
        // Temporal: observation time (time index) + the window this edge covers.
        column(
            OBSERVED_AT_COLUMN,
            ColumnDataType::TimestampMillisecond,
            SemanticType::Timestamp,
            false,
        ),
        field("window_start", ColumnDataType::TimestampMillisecond),
        field("window_end", ColumnDataType::TimestampMillisecond),
        field("fresh_until", ColumnDataType::TimestampMillisecond),
        // Declared-only business validity (NULL valid_until = valid until TTL).
        field("valid_from", ColumnDataType::TimestampMillisecond),
        field("valid_until", ColumnDataType::TimestampMillisecond),
        // Endpoints + edge identity (all tags, in primary-key order).
        tag("src_type"),
        tag("src_id"),
        tag("rel_type"),
        tag("dst_type"),
        tag("dst_id"),
        tag("provenance"),
        tag("scope"),
        tag("generation_id"),
        // Confidence + RED metrics (populated for derived edges; usually NULL here).
        field("confidence", ColumnDataType::Float64),
        field("request_count", ColumnDataType::Int64),
        field("error_count", ColumnDataType::Int64),
        field("duration_sum", ColumnDataType::Float64),
        field("duration_count", ColumnDataType::Int64),
        // JSON text: connection_type, db.system, peer.service, ...
        field("attributes", ColumnDataType::String),
    ];

    let table_options = [(TTL_KEY.to_string(), DECLARED_RELATIONSHIPS_TTL.to_string())]
        .into_iter()
        .collect();

    CreateTableExpr {
        catalog_name: catalog.to_string(),
        schema_name: DEFAULT_PRIVATE_SCHEMA_NAME.to_string(),
        table_name: SEMANTIC_RELATIONSHIPS_DECLARED_TABLE_NAME.to_string(),
        desc: "Hand-declared edges of the entity-relationship graph".to_string(),
        column_defs,
        time_index: OBSERVED_AT_COLUMN.to_string(),
        primary_keys: PRIMARY_KEY_COLUMNS.iter().map(|c| c.to_string()).collect(),
        create_if_not_exists: true,
        table_options,
        table_id: None,
        // Default `LastRow` merge (append_mode unset) gives PK last-write-wins.
        engine: common_catalog::consts::MITO_ENGINE.to_string(),
    }
}

/// Bin width for the temporal window of derived rows: 60s buckets, matching the
/// service-graph convention.
const BIN_NANOS: i64 = 60 * 1_000_000_000;

/// A single table's entity-identity declaration, projected from
/// `information_schema.table_semantics` (`greptime.semantic.entity.<type>.*`).
#[derive(Debug, Clone)]
pub struct EntityDeclaration {
    /// The declaring table's name, recorded in `source_tables`.
    pub table: String,
    /// The table's time index column, used for the temporal window filter.
    pub time_index: String,
    pub entity_type: String,
    /// Identifying columns (>= 1). One column → id verbatim; several → composite.
    pub id_columns: Vec<String>,
    /// Descriptive columns snapshotted into the `descriptive` JSON (may be empty).
    pub descriptive_columns: Vec<String>,
    /// Scope columns (namespace/environment). One column → scope verbatim;
    /// several → sorted `k=v,k=v`, mirroring the composite id rendering.
    pub scope_columns: Vec<String>,
}

/// The read-time query window as scalar [`Expr`]s. Kept as expressions so the
/// provider can fill them from a scan's time predicate or the product default.
#[derive(Debug, Clone)]
pub struct GraphWindow {
    pub start: Expr,
    pub end: Expr,
}

impl GraphWindow {
    /// Conservative default when a query carries no explicit time predicate: the
    /// last hour, so a bare `SELECT * FROM semantic_entities` never scans every
    /// declaring table's full history. This is a product default, not a cap.
    ///
    /// The bounds are timestamp literals taken from the wall clock at plan-build
    /// time — the same snapshot semantics `now()` would give, but as plain
    /// constants they prune the source table scans without depending on
    /// constant-folding.
    pub fn default_last_hour() -> Self {
        let end_ms = common_time::util::current_time_millis();
        Self {
            start: lit(ScalarValue::TimestampMillisecond(
                Some(end_ms - 60 * 60 * 1000),
                None,
            )),
            end: lit(ScalarValue::TimestampMillisecond(Some(end_ms), None)),
        }
    }
}

/// An `INTERVAL` literal of `nanos` nanoseconds.
fn interval(nanos: i64) -> Expr {
    lit(ScalarValue::new_interval_mdn(0, 0, nanos))
}

fn bin_interval() -> Expr {
    interval(BIN_NANOS)
}

/// `date_bin(60s, ts)` cast to millisecond precision, so the output schema is
/// deterministic regardless of the source column's precision (trace tables are
/// nanosecond, metric tables millisecond).
fn bin_ms(ts: Expr) -> Expr {
    cast(
        datetime_fns::date_bin().call(vec![bin_interval(), ts]),
        DataType::Timestamp(TimeUnit::Millisecond, None),
    )
}

/// Folds union branches without requiring a non-empty input.
fn union_all(acc: Option<DataFrame>, branch: DataFrame) -> DfResult<Option<DataFrame>> {
    Ok(Some(match acc {
        Some(acc) => acc.union(branch)?,
        None => branch,
    }))
}

/// A column reference qualified by a join-side alias, built without string
/// parsing (so column names containing `.` or `"` stay verbatim).
fn qcol(relation: &str, name: &str) -> Expr {
    Expr::Column(Column::new(Some(relation), name))
}

fn concat_expr(parts: Vec<Expr>) -> Expr {
    string_fns::concat().call(parts)
}

/// `coalesce(CAST(column AS STRING), '')`: renders a nullable column for string
/// concatenation without collapsing the result to NULL.
fn cast_string_or_empty(column: &str) -> Expr {
    core_fns::coalesce().call(vec![cast(ident(column), DataType::Utf8), lit("")])
}

/// The `parse_json` UDF, shared by all derivation plans. Resolved from the
/// global registry once: the UDF is stateless (its `FunctionContext` is unused).
static PARSE_JSON_UDF: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(
        FUNCTION_REGISTRY
            .get_function("parse_json")
            .expect("parse_json must be registered")
            .provide(FunctionContext::default()),
    )
});

/// Parses a JSON text expression into a JSONB value, cast from the UDF's
/// `BinaryView` output to `Binary` — the storage type the computed tables'
/// declared `json` columns map to in Arrow.
fn parse_json_expr(json_text: Expr) -> Expr {
    cast(PARSE_JSON_UDF.call(vec![json_text]), DataType::Binary)
}

/// A NULL literal typed as JSONB storage (`Binary`), so branches without a JSON
/// value union-align with branches that produce one.
fn null_json() -> Expr {
    lit(ScalarValue::Binary(None))
}

/// Renders a compile-time-known string as JSON text (quoted, fully escaped —
/// including control characters, unlike the runtime value escaping).
fn json_quote(value: &str) -> String {
    serde_json::Value::from(value).to_string()
}

/// Wraps a column reference in `replace` calls so its runtime value is
/// JSON-escaped (`\` then `"`); NULL becomes `''` so one NULL column does not
/// invalidate the whole JSON text (descriptive columns are nullable).
fn json_escaped_value_expr(column: &str) -> Expr {
    let escaped_backslash =
        string_fns::replace().call(vec![cast_string_or_empty(column), lit("\\"), lit("\\\\")]);
    string_fns::replace().call(vec![escaped_backslash, lit("\""), lit("\\\"")])
}

/// Builds a JSONB object from `columns` by concatenating a JSON text and parsing
/// it — GreptimeDB has no struct→json function. Keys are JSON-escaped in Rust;
/// values are JSON-escaped at runtime via [`json_escaped_value_expr`].
///
/// TODO(entity-graph): replace the text round-trip with a UDF that assembles
/// JSONB directly from the value columns (`jsonb::ObjectBuilder`, keys baked
/// in), dropping the escaping helpers and the per-row parse cost.
fn json_object_expr(columns: &[String]) -> Expr {
    if columns.is_empty() {
        return parse_json_expr(lit("{}"));
    }
    let mut parts = vec![lit("{")];
    for (i, column) in columns.iter().enumerate() {
        if i > 0 {
            parts.push(lit(","));
        }
        parts.push(lit(format!("{}:\"", json_quote(column))));
        parts.push(json_escaped_value_expr(column));
        parts.push(lit("\""));
    }
    parts.push(lit("}"));
    parse_json_expr(concat_expr(parts))
}

/// Renders pre-sorted columns as a `k=v,k=v` concatenation. `nullable`
/// coalesces each value to `''` (id columns are tags and non-null; scope
/// columns carry no such guarantee).
fn sorted_kv_expr(sorted_cols: &[String], nullable: bool) -> Expr {
    let mut parts = Vec::with_capacity(sorted_cols.len() * 3);
    for (i, column) in sorted_cols.iter().enumerate() {
        if i > 0 {
            parts.push(lit(","));
        }
        parts.push(lit(format!("{column}=")));
        parts.push(if nullable {
            cast_string_or_empty(column)
        } else {
            cast(ident(column), DataType::Utf8)
        });
    }
    concat_expr(parts)
}

/// One `DISTINCT` branch projecting a declaring table's rows into the registry
/// shape: one row per observed `(window, entity)`.
fn registry_branch(
    decl: &EntityDeclaration,
    df: DataFrame,
    window: &GraphWindow,
) -> DfResult<DataFrame> {
    let ts = ident(&decl.time_index);
    let bin = bin_ms(ts.clone());

    let (entity_id, entity_id_attrs) = if let [id] = decl.id_columns.as_slice() {
        // CAST even the single-column id: id columns must be tags but not
        // necessarily strings, and the computed table declares entity_id STRING.
        (cast(ident(id), DataType::Utf8), null_json())
    } else {
        // Composite identity: sorted `k=v,k=v` string + a JSON object of the same
        // columns (the escaping-safe source of truth, RFC Open Question 1).
        let mut cols = decl.id_columns.clone();
        cols.sort();
        (sorted_kv_expr(&cols, false), json_object_expr(&cols))
    };

    let scope = match decl.scope_columns.as_slice() {
        [] => lit(""),
        // Scope columns are not required to be tags, so guard against NULL.
        [single] => cast_string_or_empty(single),
        _ => {
            let mut cols = decl.scope_columns.clone();
            cols.sort();
            sorted_kv_expr(&cols, true)
        }
    };

    let descriptive = if decl.descriptive_columns.is_empty() {
        null_json()
    } else {
        json_object_expr(&decl.descriptive_columns)
    };

    let source_tables = parse_json_expr(lit(format!("[{}]", json_quote(&decl.table))));

    df.filter(
        ts.clone()
            .gt_eq(window.start.clone())
            .and(ts.lt(window.end.clone())),
    )?
    .select(vec![
        bin.clone().alias("observed_at"),
        bin.clone().alias("window_start"),
        (bin.clone() + bin_interval()).alias("window_end"),
        (bin + bin_interval()).alias("fresh_until"),
        lit(decl.entity_type.as_str()).alias("entity_type"),
        entity_id.alias("entity_id"),
        entity_id_attrs.alias("entity_id_attrs"),
        scope.alias("scope"),
        descriptive.alias("descriptive"),
        source_tables.alias("source_tables"),
    ])?
    .distinct()
}

/// Builds the `semantic_entities` registry plan: a `UNION ALL` of one branch per
/// declaring table, filtered to `window`. Each [`DataFrame`] is the declaring
/// table's scan (from `QueryEngine::read_table`). Returns `None` when nothing
/// declared an entity, so the computed table streams empty.
pub fn build_registry_plan(
    branches: Vec<(EntityDeclaration, DataFrame)>,
    window: &GraphWindow,
) -> DfResult<Option<LogicalPlan>> {
    let mut union_df: Option<DataFrame> = None;
    for (decl, df) in branches {
        union_df = union_all(union_df, registry_branch(&decl, df, window)?)?;
    }
    Ok(union_df.map(DataFrame::into_unoptimized_plan))
}

/// The projected columns of `semantic_relationships`, in order. Every derived
/// branch and the declared-edge scan must project exactly these so the top-level
/// `UNION ALL` type-aligns. (The physical declared table additionally stores
/// `valid_from`/`valid_until`, which are applied as a filter, not projected.)
/// Test-only until the declared-edge union branch lands and enforces it in code.
#[cfg(test)]
const RELATIONSHIP_COLUMNS: [&str; 18] = [
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
    "scope",
    "generation_id",
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

/// Builds the `calls` derivation (RFC §3a) over `traces` (one scan per trace
/// table, unioned): pair each client span with its child server span on
/// `trace_id` + `parent_span_id`, project to `service`, aggregate to RED metrics
/// per 60s window. This is the plan form of the Tempo servicegraph connector.
/// Virtual-node edges (uninstrumented peers) are a separate branch, added on top
/// of this. Column names are the fixed `greptime_trace_v1` schema (the reason
/// `table_data_model = greptime_trace_v1` is required); `span_status_code` is a
/// string column (`STATUS_CODE_ERROR`), verified against the trace ingest path.
/// Returns `None` when there is no trace table.
pub fn build_calls_plan(
    traces: Vec<DataFrame>,
    window: &GraphWindow,
) -> DfResult<Option<LogicalPlan>> {
    let mut union_df: Option<DataFrame> = None;
    for trace in traces {
        union_df = union_all(union_df, calls_branch(trace, window)?)?;
    }
    Ok(union_df.map(DataFrame::into_unoptimized_plan))
}

fn calls_branch(trace: DataFrame, window: &GraphWindow) -> DfResult<DataFrame> {
    // `scope` stays empty here: the fixed `greptime_trace_v1` schema declares no
    // scope columns (the auto-stamp is `entity.service.id = service_name` only).
    // Revisit when trace tables can carry scope declarations.
    let client = trace
        .clone()
        .filter(
            ident(SPAN_KIND_COLUMN)
                .eq(lit(SPAN_KIND_CLIENT))
                .and(ident(TRACE_TIMESTAMP_COLUMN).gt_eq(window.start.clone()))
                .and(ident(TRACE_TIMESTAMP_COLUMN).lt(window.end.clone())),
        )?
        .alias("client")?;
    let server = trace
        .filter(
            ident(SPAN_KIND_COLUMN)
                .eq(lit(SPAN_KIND_SERVER))
                // Static bounds implied by the window and the join's
                // time-proximity conditions below; the join bounds reference
                // client.timestamp and cannot prune the server-side scan.
                .and(
                    ident(TRACE_TIMESTAMP_COLUMN)
                        .gt_eq(window.start.clone() - interval(CHILD_SPAN_EARLY_NANOS)),
                )
                .and(
                    ident(TRACE_TIMESTAMP_COLUMN)
                        .lt(window.end.clone() + interval(CHILD_SPAN_LATE_NANOS)),
                ),
        )?
        .alias("server")?;

    let join_conditions = vec![
        qcol("client", TRACE_ID_COLUMN).eq(qcol("server", TRACE_ID_COLUMN)),
        qcol("server", PARENT_SPAN_ID_COLUMN).eq(qcol("client", SPAN_ID_COLUMN)),
        qcol("server", TRACE_TIMESTAMP_COLUMN)
            .gt_eq(qcol("client", TRACE_TIMESTAMP_COLUMN) - interval(CHILD_SPAN_EARLY_NANOS)),
        qcol("server", TRACE_TIMESTAMP_COLUMN)
            .lt_eq(qcol("client", TRACE_TIMESTAMP_COLUMN) + interval(CHILD_SPAN_LATE_NANOS)),
        // Exclude self-calls: an edge needs two distinct services.
        qcol("client", SERVICE_NAME_COLUMN).not_eq(qcol("server", SERVICE_NAME_COLUMN)),
    ];

    client
        .join_on(server, JoinType::Inner, join_conditions)?
        .select(vec![
            bin_ms(qcol("client", TRACE_TIMESTAMP_COLUMN)).alias("observed_at"),
            qcol("client", SERVICE_NAME_COLUMN).alias("src_id"),
            qcol("server", SERVICE_NAME_COLUMN).alias("dst_id"),
            qcol("server", SPAN_STATUS_CODE_COLUMN).alias("status_code"),
            qcol("server", DURATION_NANO_COLUMN).alias("duration_nano"),
        ])?
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
            lit("").alias("scope"),
            lit("").alias("generation_id"),
            ident("request_count"),
            ident("error_count"),
            // duration_nano sums in nanoseconds; the contract column is seconds.
            (cast(ident("duration_nano_sum"), DataType::Float64) / lit(1e9_f64))
                .alias("duration_sum"),
            ident("request_count").alias("duration_count"),
            null_json().alias("attributes"),
        ])
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        Array, ArrayRef, BinaryArray, Float64Array, Int64Array, StringArray,
        TimestampMillisecondArray, TimestampNanosecondArray, UInt64Array,
    };
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    use super::*;

    fn test_window() -> GraphWindow {
        GraphWindow {
            start: lit(ScalarValue::TimestampMillisecond(Some(0), None)),
            end: lit(ScalarValue::TimestampMillisecond(
                Some(10 * 60 * 1000),
                None,
            )),
        }
    }

    async fn collect(ctx: &SessionContext, plan: LogicalPlan) -> Vec<RecordBatch> {
        ctx.execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
    }

    fn json_texts(batch: &RecordBatch, column: usize) -> Vec<Option<String>> {
        let array = batch
            .column(column)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        (0..array.len())
            .map(|i| {
                array
                    .is_valid(i)
                    .then(|| jsonb::from_slice(array.value(i)).unwrap().to_string())
            })
            .collect()
    }

    fn strings(batch: &RecordBatch, column: usize) -> Vec<String> {
        let array = batch
            .column(column)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (0..array.len())
            .map(|i| array.value(i).to_string())
            .collect()
    }

    /// A metric-like table: ms timestamps, service/pid identity, nullable
    /// descriptive column with JSON-hostile characters.
    fn metric_table_ctx() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("pid", DataType::Int64, false),
            Field::new("host", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![1_000, 2_000, 61_000])) as ArrayRef,
                Arc::new(StringArray::from(vec!["cart", "cart", "cart"])),
                Arc::new(Int64Array::from(vec![42, 42, 42])),
                Arc::new(StringArray::from(vec![
                    Some(r#"we"ird\host"#),
                    None,
                    Some("h2"),
                ])),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table(
            "app_latency",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
        ctx
    }

    fn decl(entity_type: &str, id_columns: &[&str]) -> EntityDeclaration {
        EntityDeclaration {
            table: "app_latency".to_string(),
            time_index: "ts".to_string(),
            entity_type: entity_type.to_string(),
            id_columns: id_columns.iter().map(|s| s.to_string()).collect(),
            descriptive_columns: vec![],
            scope_columns: vec![],
        }
    }

    #[tokio::test]
    async fn registry_single_column_identity() {
        let ctx = metric_table_ctx();
        let df = ctx.table("app_latency").await.unwrap();
        let plan = build_registry_plan(
            vec![(decl("service", &["service_name"]), df)],
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
        assert_eq!(
            names,
            [
                "observed_at",
                "window_start",
                "window_end",
                "fresh_until",
                "entity_type",
                "entity_id",
                "entity_id_attrs",
                "scope",
                "descriptive",
                "source_tables",
            ]
        );

        let batches = collect(&ctx, plan).await;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        // 3 rows in 2 distinct 60s bins, all the same entity -> 2 rows.
        assert_eq!(total, 2);
        let batch = &batches[0];
        assert_eq!(strings(batch, 4), vec!["service"; batch.num_rows()]);
        assert_eq!(strings(batch, 5), vec!["cart"; batch.num_rows()]);
        // Single-column id -> entity_id_attrs and descriptive are typed-JSON NULLs.
        assert!(json_texts(batch, 6).iter().all(Option::is_none));
        assert!(json_texts(batch, 8).iter().all(Option::is_none));
        assert_eq!(
            json_texts(batch, 9),
            vec![Some(r#"["app_latency"]"#.to_string()); batch.num_rows()]
        );
    }

    #[tokio::test]
    async fn registry_composite_identity_and_descriptive_escaping() {
        let ctx = metric_table_ctx();
        let df = ctx.table("app_latency").await.unwrap();
        let mut declaration = decl("process", &["service_name", "pid"]);
        declaration.descriptive_columns = vec!["host".to_string()];
        let plan = build_registry_plan(vec![(declaration, df)], &test_window())
            .unwrap()
            .unwrap();

        let batches = collect(&ctx, plan).await;
        let mut rows: Vec<(String, Option<String>, Option<String>)> = batches
            .iter()
            .flat_map(|batch| {
                let ids = strings(batch, 5);
                let id_attrs = json_texts(batch, 6);
                let descriptives = json_texts(batch, 8);
                ids.into_iter()
                    .zip(id_attrs)
                    .zip(descriptives)
                    .map(|((id, attrs), descriptive)| (id, attrs, descriptive))
                    .collect::<Vec<_>>()
            })
            .collect();
        rows.sort();

        // Composite id -> sorted `k=v,k=v` plus a JSON object of the id columns;
        // descriptive JSON escapes `\` and `"` in runtime values, NULL -> "".
        assert_eq!(
            rows,
            vec![
                (
                    "pid=42,service_name=cart".to_string(),
                    Some(r#"{"pid":"42","service_name":"cart"}"#.to_string()),
                    Some(r#"{"host":""}"#.to_string()),
                ),
                (
                    "pid=42,service_name=cart".to_string(),
                    Some(r#"{"pid":"42","service_name":"cart"}"#.to_string()),
                    Some(r#"{"host":"h2"}"#.to_string()),
                ),
                (
                    "pid=42,service_name=cart".to_string(),
                    Some(r#"{"pid":"42","service_name":"cart"}"#.to_string()),
                    Some(r#"{"host":"we\"ird\\host"}"#.to_string()),
                ),
            ]
        );
    }

    #[tokio::test]
    async fn registry_scope_variants() {
        let ctx = metric_table_ctx();

        // Single scope column: its (NULL-safe) value verbatim.
        let mut single = decl("service", &["service_name"]);
        single.scope_columns = vec!["host".to_string()];
        let df = ctx.table("app_latency").await.unwrap();
        let plan = build_registry_plan(vec![(single, df)], &test_window())
            .unwrap()
            .unwrap();
        let batches = collect(&ctx, plan).await;
        let mut scopes: Vec<String> = batches.iter().flat_map(|b| strings(b, 7)).collect();
        scopes.sort();
        assert_eq!(scopes, vec!["", "h2", r#"we"ird\host"#]);

        // Multiple scope columns: sorted `k=v,k=v`.
        let mut multi = decl("service", &["service_name"]);
        multi.scope_columns = vec!["pid".to_string(), "host".to_string()];
        let df = ctx.table("app_latency").await.unwrap();
        let plan = build_registry_plan(vec![(multi, df)], &test_window())
            .unwrap()
            .unwrap();
        let batches = collect(&ctx, plan).await;
        let mut scopes: Vec<String> = batches.iter().flat_map(|b| strings(b, 7)).collect();
        scopes.sort();
        assert_eq!(
            scopes,
            vec![
                "host=,pid=42",
                "host=h2,pid=42",
                r#"host=we"ird\host,pid=42"#
            ]
        );
    }

    #[tokio::test]
    async fn registry_unions_declarations() {
        let ctx = metric_table_ctx();
        let df1 = ctx.table("app_latency").await.unwrap();
        let df2 = ctx.table("app_latency").await.unwrap();
        let plan = build_registry_plan(
            vec![
                (decl("service", &["service_name"]), df1),
                (decl("host", &["pid"]), df2),
            ],
            &test_window(),
        )
        .unwrap()
        .unwrap();

        let batches = collect(&ctx, plan).await;
        let mut types: Vec<String> = batches.iter().flat_map(|b| strings(b, 4)).collect();
        types.sort();
        assert_eq!(types, vec!["host", "host", "service", "service"]);

        // No declarations -> no plan.
        assert!(
            build_registry_plan(vec![], &test_window())
                .unwrap()
                .is_none()
        );
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
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
        ctx
    }

    #[tokio::test]
    async fn calls_plan_aggregates_red_metrics() {
        let ctx = trace_table_ctx();
        let trace = ctx.table("opentelemetry_traces").await.unwrap();
        let plan = build_calls_plan(vec![trace], &test_window())
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
            .column(13)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(request_count.value(0), 2);
        let error_count = batch
            .column(14)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(error_count.value(0), 1);
        let duration_sum = batch
            .column(15)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((duration_sum.value(0) - 2.0).abs() < 1e-9);
        let duration_count = batch
            .column(16)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(duration_count.value(0), 2);
        // Derived calls edges carry no attributes: a typed-JSON NULL.
        assert!(json_texts(batch, 17)[0].is_none());
    }

    #[tokio::test]
    async fn calls_plan_respects_window() {
        let ctx = trace_table_ctx();
        let trace = ctx.table("opentelemetry_traces").await.unwrap();
        // A window that ends before all test spans: nothing derives.
        let window = GraphWindow {
            start: lit(ScalarValue::TimestampMillisecond(Some(0), None)),
            end: lit(ScalarValue::TimestampMillisecond(Some(500), None)),
        };
        let plan = build_calls_plan(vec![trace], &window).unwrap().unwrap();
        let batches = collect(&ctx, plan).await;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0);
    }

    #[test]
    fn declared_relationships_expr_shape() {
        let expr = build_declared_relationships_expr(common_catalog::consts::DEFAULT_CATALOG_NAME);

        assert_eq!(expr.schema_name, DEFAULT_PRIVATE_SCHEMA_NAME);
        assert_eq!(expr.table_name, SEMANTIC_RELATIONSHIPS_DECLARED_TABLE_NAME);
        assert!(expr.create_if_not_exists);
        assert_eq!(expr.time_index, OBSERVED_AT_COLUMN);
        assert_eq!(expr.primary_keys, PRIMARY_KEY_COLUMNS);
        // append_mode is unset so the table gets the default LastRow (upsert) merge.
        assert!(!expr.table_options.contains_key("append_mode"));
        assert_eq!(
            expr.table_options.get(TTL_KEY).map(String::as_str),
            Some("30d")
        );

        // Every primary-key column exists and is a tag.
        for pk in PRIMARY_KEY_COLUMNS {
            let def = expr
                .column_defs
                .iter()
                .find(|c| c.name == pk)
                .unwrap_or_else(|| panic!("missing pk column {pk}"));
            assert_eq!(
                def.semantic_type,
                SemanticType::Tag as i32,
                "{pk} must be a tag"
            );
            assert!(!def.is_nullable, "{pk} must be non-null");
        }

        // The time index is a non-null timestamp.
        let ts = expr
            .column_defs
            .iter()
            .find(|c| c.name == OBSERVED_AT_COLUMN)
            .unwrap();
        assert_eq!(ts.semantic_type, SemanticType::Timestamp as i32);
        assert!(!ts.is_nullable);
    }
}
