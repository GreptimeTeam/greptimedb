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

//! The physical, hand-declared edge table of the entity-relationship graph.
//!
//! In OSS the graph is derived at read time (`semantic_entities` /
//! `semantic_relationships` are computed), so the *only* stored part is this table:
//! edges a user asserts by hand (`provenance = 'declared'`). It lives in
//! `greptime_private`, uses the default `LastRow` merge (last-write-wins on the
//! primary key — an idempotent upsert) and a sliding TTL. Its schema is also the
//! shape the enterprise materialiser (M3) will upsert derived edges into, so the
//! computed `semantic_relationships` can later swap read-time derivation for a
//! table scan without changing the query surface. See
//! `docs/rfcs/2026-06-25-entity-relationships-and-graph-query.md`.

use api::v1::{ColumnDataType, ColumnDef, CreateTableExpr, SemanticType};
use common_catalog::consts::{
    DEFAULT_PRIVATE_SCHEMA_NAME, SEMANTIC_RELATIONSHIPS_DECLARED_TABLE_NAME,
};
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

/// Bin width for the temporal window of derived rows (60s buckets, matching the
/// service-graph convention). An `INTERVAL` literal so it slots into `date_bin`.
const BIN_INTERVAL: &str = "INTERVAL '60 seconds'";

/// A single table's entity-identity declaration, projected from
/// `information_schema.table_semantics` (`greptime.semantic.entity.<type>.*`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityDeclaration {
    pub catalog: String,
    pub schema: String,
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

/// The read-time query window as SQL scalar expressions (e.g.
/// `now() - INTERVAL '1 hour'`). Kept as expressions so this builder needs no
/// clock access; the provider fills them from a scan's time predicate or the
/// product default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphWindow {
    pub start_expr: String,
    pub end_expr: String,
}

impl GraphWindow {
    /// Conservative default when a query carries no explicit time predicate: the
    /// last hour, so a bare `SELECT * FROM semantic_entities` never scans every
    /// declaring table's full history. This is a product default, not a cap.
    pub fn default_last_hour() -> Self {
        Self {
            start_expr: "now() - INTERVAL '1 hour'".to_string(),
            end_expr: "now()".to_string(),
        }
    }
}

/// Quotes a SQL identifier, escaping embedded double quotes. Column/table names
/// originate from user semantic options, so they are never interpolated raw.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Quotes a SQL string literal, escaping embedded single quotes.
fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Escapes a string for embedding inside a JSON string value (`\` and `"`).
fn json_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Wraps a column reference in SQL `replace` calls so its runtime value is
/// JSON-escaped (`\` then `"`), with `coalesce(..., '')` stopping a NULL column
/// value from collapsing the whole `||` concatenation to NULL (descriptive
/// columns are nullable).
fn json_escaped_value_expr(column: &str) -> String {
    format!(
        r#"replace(replace(coalesce(CAST({} AS STRING), ''), '\', '\\'), '"', '\"')"#,
        quote_ident(column)
    )
}

/// Builds a JSON object string from `columns` via string concat — GreptimeDB has
/// no struct→json function, and this keeps the output column a plain STRING (the
/// computed table's declared type). Keys are JSON-escaped in Rust; values are
/// JSON-escaped at runtime via [`json_escaped_value_expr`].
fn json_object_expr(columns: &[String]) -> String {
    if columns.is_empty() {
        return "'{}'".to_string();
    }
    let pairs = columns
        .iter()
        .map(|c| {
            let key = quote_literal(&format!("\"{}\":\"", json_escape(c)));
            format!("{key} || {} || '\"'", json_escaped_value_expr(c))
        })
        .collect::<Vec<_>>()
        .join(" || ',' || ");
    format!("'{{' || {pairs} || '}}'")
}

/// Renders pre-sorted columns as a `k=v,k=v` concat expression. `nullable`
/// coalesces each value to `''` (id columns are tags and non-null; scope
/// columns carry no such guarantee).
fn sorted_kv_expr(sorted_cols: &[String], nullable: bool) -> String {
    sorted_cols
        .iter()
        .map(|c| {
            let value = if nullable {
                format!("coalesce(CAST({} AS STRING), '')", quote_ident(c))
            } else {
                format!("CAST({} AS STRING)", quote_ident(c))
            };
            format!("{} || '=' || {value}", quote_literal(c))
        })
        .collect::<Vec<_>>()
        .join(" || ',' || ")
}

fn qualified_table(decl: &EntityDeclaration) -> String {
    format!(
        "{}.{}.{}",
        quote_ident(&decl.catalog),
        quote_ident(&decl.schema),
        quote_ident(&decl.table)
    )
}

/// One `SELECT DISTINCT` branch projecting a declaring table's rows into the
/// registry shape. `DISTINCT` (not `GROUP BY`) gives one row per observed
/// `(window, entity)` without listing every projected expression in a group key.
fn registry_branch(decl: &EntityDeclaration, window: &GraphWindow) -> String {
    let ts = quote_ident(&decl.time_index);
    // Cast the binned timestamp to millisecond precision so the output schema is
    // deterministic regardless of the source column's precision (trace tables are
    // nanosecond, metric tables millisecond).
    let bin = format!("CAST(date_bin({BIN_INTERVAL}, {ts}) AS TIMESTAMP(3))");

    let (entity_id, entity_id_attrs) = if decl.id_columns.len() == 1 {
        // CAST even the single-column id: id columns must be tags but not
        // necessarily strings, and the computed table declares entity_id STRING.
        (
            format!("CAST({} AS STRING)", quote_ident(&decl.id_columns[0])),
            "CAST(NULL AS STRING)".to_string(),
        )
    } else {
        // Composite identity: sorted `k=v,k=v` string + a JSON object of the same
        // columns (the escaping-safe source of truth, RFC Open Question 1).
        let mut cols = decl.id_columns.clone();
        cols.sort();
        (sorted_kv_expr(&cols, false), json_object_expr(&cols))
    };

    let scope = if decl.scope_columns.is_empty() {
        "''".to_string()
    } else if decl.scope_columns.len() == 1 {
        // Scope columns are not required to be tags, so guard against NULL.
        format!(
            "coalesce(CAST({} AS STRING), '')",
            quote_ident(&decl.scope_columns[0])
        )
    } else {
        let mut cols = decl.scope_columns.clone();
        cols.sort();
        sorted_kv_expr(&cols, true)
    };

    let descriptive = if decl.descriptive_columns.is_empty() {
        "CAST(NULL AS STRING)".to_string()
    } else {
        json_object_expr(&decl.descriptive_columns)
    };

    let source_tables = quote_literal(&format!("[\"{}\"]", decl.table));

    format!(
        "SELECT DISTINCT {bin} AS observed_at, {bin} AS window_start, \
         {bin} + {BIN_INTERVAL} AS window_end, {bin} + {BIN_INTERVAL} AS fresh_until, \
         {etype} AS entity_type, {entity_id} AS entity_id, {entity_id_attrs} AS entity_id_attrs, \
         {scope} AS scope, {descriptive} AS descriptive, {source_tables} AS source_tables \
         FROM {table} \
         WHERE {ts} >= {start} AND {ts} < {end}",
        etype = quote_literal(&decl.entity_type),
        table = qualified_table(decl),
        start = window.start_expr,
        end = window.end_expr,
    )
}

/// Builds the `semantic_entities` registry query: a `UNION ALL` of one branch per
/// declaring table, filtered to `window`. Returns `None` when nothing declared an
/// entity, so the computed table streams empty.
pub fn build_registry_sql(decls: &[EntityDeclaration], window: &GraphWindow) -> Option<String> {
    if decls.is_empty() {
        return None;
    }
    Some(
        decls
            .iter()
            .map(|decl| registry_branch(decl, window))
            .collect::<Vec<_>>()
            .join("\nUNION ALL\n"),
    )
}

/// The projected columns of `semantic_relationships`, in order. Every derived
/// branch and the declared-edge scan must project exactly these so the top-level
/// `UNION ALL` type-aligns. (The physical declared table additionally stores
/// `valid_from`/`valid_until`, which are applied as a filter, not projected.)
pub const RELATIONSHIP_COLUMNS: [&str; 18] = [
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

/// A trace table to derive the service `calls` graph from. Column names are the
/// fixed `greptime_trace_v1` schema; only the location varies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceSource {
    pub catalog: String,
    pub schema: String,
    pub table: String,
}

impl TraceSource {
    fn qualified(&self) -> String {
        format!(
            "{}.{}.{}",
            quote_ident(&self.catalog),
            quote_ident(&self.schema),
            quote_ident(&self.table)
        )
    }
}

/// Builds the `calls` derivation (RFC §3a): pair each client span with its child
/// server span on `trace_id` + `parent_span_id`, project to `service`, aggregate to
/// RED metrics per 60s window. This is the SQL form of the Tempo servicegraph
/// connector. Virtual-node edges (uninstrumented peers) are a separate branch,
/// added on top of this. `span_status_code` is a string column
/// (`STATUS_CODE_ERROR`), verified against the trace ingest path.
pub fn build_calls_sql(trace: &TraceSource, window: &GraphWindow) -> String {
    let t = trace.qualified();
    // `scope` stays empty here: the fixed `greptime_trace_v1` schema declares no
    // scope columns (the auto-stamp is `entity.service.id = service_name` only).
    // Revisit when trace tables can carry scope declarations.
    //
    // Cast to millisecond precision: trace timestamps are nanosecond, but the
    // computed table's schema declares millisecond.
    let bin = format!("CAST(date_bin({BIN_INTERVAL}, client.\"timestamp\") AS TIMESTAMP(3))");
    format!(
        "SELECT {bin} AS observed_at, {bin} AS window_start, \
         {bin} + {BIN_INTERVAL} AS window_end, {bin} + {BIN_INTERVAL} AS fresh_until, \
         'service' AS src_type, client.\"service_name\" AS src_id, \
         'service' AS dst_type, server.\"service_name\" AS dst_id, \
         'calls' AS rel_type, 'trace' AS provenance, CAST(1.0 AS DOUBLE) AS confidence, \
         '' AS scope, '' AS generation_id, \
         count(*) AS request_count, \
         count(*) FILTER (WHERE server.\"span_status_code\" = 'STATUS_CODE_ERROR') AS error_count, \
         CAST(sum(server.\"duration_nano\") / 1e9 AS DOUBLE) AS duration_sum, \
         count(*) AS duration_count, \
         CAST(NULL AS STRING) AS attributes \
         FROM {t} AS client JOIN {t} AS server \
         ON client.\"trace_id\" = server.\"trace_id\" \
         AND server.\"parent_span_id\" = client.\"span_id\" \
         AND server.\"timestamp\" >= client.\"timestamp\" - INTERVAL '5 minutes' \
         AND server.\"timestamp\" <= client.\"timestamp\" + INTERVAL '1 hour' \
         WHERE client.\"span_kind\" = 'SPAN_KIND_CLIENT' \
         AND server.\"span_kind\" = 'SPAN_KIND_SERVER' \
         AND client.\"service_name\" <> server.\"service_name\" \
         AND client.\"timestamp\" >= {start} AND client.\"timestamp\" < {end} \
         GROUP BY 1, 2, 3, 4, src_id, dst_id",
        start = window.start_expr,
        end = window.end_expr,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trace_source() -> TraceSource {
        TraceSource {
            catalog: "greptime".to_string(),
            schema: "public".to_string(),
            table: "opentelemetry_traces".to_string(),
        }
    }

    #[test]
    fn build_calls_sql_shape() {
        let sql = build_calls_sql(&trace_source(), &GraphWindow::default_last_hour());
        // Self-join on trace_id + parent/child span, projected to service.
        assert!(sql.contains(r#"client."trace_id" = server."trace_id""#));
        assert!(sql.contains(r#"server."parent_span_id" = client."span_id""#));
        assert!(sql.contains("'calls' AS rel_type"));
        assert!(sql.contains("'trace' AS provenance"));
        // Error count keys off the string status code, not an int.
        assert!(sql.contains("FILTER (WHERE server.\"span_status_code\" = 'STATUS_CODE_ERROR')"));
        // Client/server kind guard + self-call exclusion + time window.
        assert!(sql.contains("client.\"span_kind\" = 'SPAN_KIND_CLIENT'"));
        assert!(sql.contains(r#"client."service_name" <> server."service_name""#));
        assert!(sql.contains(r#"client."timestamp" >= now() - INTERVAL '1 hour'"#));
    }

    fn decl(entity_type: &str, id_columns: &[&str]) -> EntityDeclaration {
        EntityDeclaration {
            catalog: "greptime".to_string(),
            schema: "public".to_string(),
            table: "app_latency".to_string(),
            time_index: "ts".to_string(),
            entity_type: entity_type.to_string(),
            id_columns: id_columns.iter().map(|s| s.to_string()).collect(),
            descriptive_columns: vec![],
            scope_columns: vec![],
        }
    }

    #[test]
    fn build_registry_sql_single_and_composite() {
        let window = GraphWindow::default_last_hour();

        let single = build_registry_sql(&[decl("service", &["service_name"])], &window).unwrap();
        assert!(single.contains("SELECT DISTINCT"));
        assert!(single.contains(r#"CAST("service_name" AS STRING) AS entity_id"#));
        assert!(single.contains("CAST(NULL AS STRING) AS entity_id_attrs"));
        assert!(single.contains(r#"FROM "greptime"."public"."app_latency""#));
        assert!(single.contains(r#"WHERE "ts" >= now() - INTERVAL '1 hour' AND "ts" < now()"#));

        // Composite id: sorted `k=v` concat + a manually-built JSON object (no
        // struct→json function exists); no UNION for one decl.
        let composite =
            build_registry_sql(&[decl("process", &["start_time", "pid"])], &window).unwrap();
        assert!(composite.contains(r#"'"pid":"' || replace(replace(coalesce(CAST("pid" AS STRING), ''), '\', '\\'), '"', '\"')"#));
        assert!(composite.contains("'pid' || '=' || CAST(\"pid\" AS STRING)"));
        assert!(!composite.contains("UNION ALL"));

        // Two declarations union together.
        let two = build_registry_sql(
            &[
                decl("service", &["service_name"]),
                decl("host", &["host_id"]),
            ],
            &window,
        )
        .unwrap();
        assert_eq!(two.matches("UNION ALL").count(), 1);

        // No declarations → no query.
        assert!(build_registry_sql(&[], &window).is_none());
    }

    #[test]
    fn build_registry_sql_scope() {
        let window = GraphWindow::default_last_hour();

        // No scope declared → empty literal.
        let none = build_registry_sql(&[decl("service", &["service_name"])], &window).unwrap();
        assert!(none.contains("'' AS scope"));

        // Single scope column → its (NULL-safe) value verbatim.
        let mut single_decl = decl("service", &["service_name"]);
        single_decl.scope_columns = vec!["k8s_namespace".to_string()];
        let single = build_registry_sql(&[single_decl], &window).unwrap();
        assert!(single.contains(r#"coalesce(CAST("k8s_namespace" AS STRING), '') AS scope"#));

        // Multiple scope columns → sorted `k=v,k=v`, like composite ids.
        let mut multi_decl = decl("service", &["service_name"]);
        multi_decl.scope_columns = vec!["namespace".to_string(), "cluster".to_string()];
        let multi = build_registry_sql(&[multi_decl], &window).unwrap();
        let cluster = multi.find(r#"'cluster' || '=' || coalesce(CAST("cluster" AS STRING), '')"#);
        let namespace =
            multi.find(r#"'namespace' || '=' || coalesce(CAST("namespace" AS STRING), '')"#);
        assert!(
            cluster.unwrap() < namespace.unwrap(),
            "scope must be sorted"
        );
    }

    #[test]
    fn json_object_expr_escapes_keys_and_values() {
        // Key is JSON-escaped in Rust and SQL-quoted; value escaping happens at
        // runtime via nested replace (backslash first, then double quote).
        let expr = json_object_expr(&[r#"we"ird"#.to_string()]);
        assert!(expr.contains(r#"'"we\"ird":"'"#));
        assert!(expr.contains(
            r#"replace(replace(coalesce(CAST("we""ird" AS STRING), ''), '\', '\\'), '"', '\"')"#
        ));

        assert_eq!(json_object_expr(&[]), "'{}'");
    }

    #[test]
    fn quote_ident_escapes_embedded_quotes() {
        assert_eq!(quote_ident("service_name"), r#""service_name""#);
        assert_eq!(quote_ident(r#"we"ird"#), r#""we""ird""#);
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
