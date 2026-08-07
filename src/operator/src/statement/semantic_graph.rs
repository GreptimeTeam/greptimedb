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
//!
//! The derivation is built as typed [`Expr`]s over [`DataFrame`]s (never as SQL
//! text), so user-controlled identifiers are plain values — no quoting or SQL
//! injection surface — and the plans compose with DataFusion's optimizer,
//! including filter pushdown into the source table scans. See
//! `docs/rfcs/2026-06-25-entity-relationships-and-graph-query.md`.

use std::sync::{Arc, LazyLock};

use api::v1::column_data_type_extension::TypeExt;
use api::v1::{
    ColumnDataType, ColumnDataTypeExtension, ColumnDef, CreateTableExpr, JsonTypeExtension,
    SemanticType,
};
use common_catalog::consts::{
    DEFAULT_PRIVATE_SCHEMA_NAME, DURATION_NANO_COLUMN, PARENT_SPAN_ID_COLUMN,
    SEMANTIC_RELATIONSHIPS_DECLARED_TABLE_NAME, SPAN_ID_COLUMN, SPAN_KIND_CLIENT, SPAN_KIND_COLUMN,
    SPAN_KIND_SERVER, SPAN_STATUS_CODE_COLUMN, SPAN_STATUS_ERROR, TRACE_ID_COLUMN,
    TRACE_TIMESTAMP_COLUMN,
};
use common_function::function::FunctionContext;
use common_function::function_registry::FUNCTION_REGISTRY;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::dataframe::DataFrame;
use datafusion::functions::{core as core_fns, datetime as datetime_fns, string as string_fns};
use datafusion::functions_aggregate::expr_fn::{count, sum};
use datafusion::functions_nested::expr_fn::make_array;
use datafusion::functions_window::expr_fn::row_number;
use datafusion_common::{Column, Result as DfResult, ScalarValue};
use datafusion_expr::{Expr, ExprFunctionExt, JoinType, LogicalPlan, ScalarUDF, cast, ident, lit};
use store_api::mito_engine_options::TTL_KEY;

/// Whether an existing declared-edge table still matches the canonical
/// definition ([`build_declared_relationships_expr`]): same columns, in order,
/// with the same types. The union branch projects the table by name and type,
/// so a mismatched table (only possible through upgrade skew — user DDL against
/// it is rejected) must be surfaced, not silently mis-derived.
pub fn declared_relationships_schema_matches(schema: &datatypes::schema::Schema) -> bool {
    let canonical = build_declared_relationships_expr("greptime");
    if schema.column_schemas().len() != canonical.column_defs.len() {
        return false;
    }
    canonical
        .column_defs
        .iter()
        .zip(schema.column_schemas())
        .all(|(def, column)| {
            let Ok(wrapper) = api::helper::ColumnDataTypeWrapper::try_new(
                def.data_type,
                def.datatype_extension.clone(),
            ) else {
                return false;
            };
            def.name == column.name
                && datatypes::prelude::ConcreteDataType::from(wrapper) == column.data_type
        })
}

/// Bin width for the temporal window of derived rows: 60s buckets, matching the
/// service-graph convention.
const BIN_NANOS: i64 = 60 * 1_000_000_000;

/// Time index column of the graph tables: when an edge/entity observation was
/// recorded. The provider extracts the query window from predicates on it.
pub const OBSERVED_AT_COLUMN: &str = "observed_at";

/// Default retention for the declared-edge table; expiry slides the topology
/// window (New Relic / Datadog treat derived topology as a sliding window).
const DECLARED_RELATIONSHIPS_TTL: &str = "30d";

/// The primary-key (tag) columns, in key order. Starting with the source endpoint
/// makes out-edge lookup (`WHERE src_type=? AND src_id=?`) a key-prefix scan;
/// `provenance` and `generation_id` are in the key so a declared edge and a
/// (future) derived edge for the same pair coexist without clobbering.
pub const DECLARED_PRIMARY_KEY_COLUMNS: [&str; 8] = [
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

fn json_field(name: &str) -> ColumnDef {
    let mut def = field(name, ColumnDataType::Binary);
    def.datatype_extension = Some(ColumnDataTypeExtension {
        type_ext: Some(TypeExt::JsonType(JsonTypeExtension::JsonBinary.into())),
    });
    def
}

/// Builds the `CREATE TABLE` request for the declared-edge table. Columns mirror
/// the computed `semantic_relationships` shape (temporal window + endpoints +
/// provenance/confidence + RED metrics) plus the declared-only business validity
/// window (`valid_from` / `valid_until`), which — unlike TTL (physical retention)
/// — expresses whether a hand-declared edge is still in effect.
///
/// The default `LastRow` merge dedups on primary key **plus** `observed_at`
/// (the time index is part of mito's dedup key): re-asserting an edge at a new
/// `observed_at` stores a new revision, and the read-side union keeps only the
/// latest revision per edge key.
pub fn build_declared_relationships_expr(catalog: &str) -> CreateTableExpr {
    let column_defs = vec![
        // Temporal: when this revision of the edge was declared (time index,
        // also the TTL clock).
        column(
            OBSERVED_AT_COLUMN,
            ColumnDataType::TimestampMillisecond,
            SemanticType::Timestamp,
            false,
        ),
        field("window_start", ColumnDataType::TimestampMillisecond),
        field("window_end", ColumnDataType::TimestampMillisecond),
        field("fresh_until", ColumnDataType::TimestampMillisecond),
        // Declared-only business validity. NULL valid_from = valid since the
        // declaration; NULL valid_until = valid for as long as the row exists
        // (TTL expiry retires the edge with the row).
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
        // JSON edge attributes: connection_type, db.system, peer.service, ...
        // Stored as JSONB so the union branch matches the computed table's
        // json column without a per-scan parse.
        json_field("attributes"),
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
        primary_keys: DECLARED_PRIMARY_KEY_COLUMNS
            .iter()
            .map(|c| c.to_string())
            .collect(),
        create_if_not_exists: true,
        table_options,
        table_id: None,
        engine: common_catalog::consts::MITO_ENGINE.to_string(),
    }
}

/// A single table's entity-identity declaration, projected from
/// `information_schema.table_semantics` (`greptime.semantic.entity.<type>.*`).
#[derive(Debug, Clone)]
pub struct EntityDeclaration {
    /// The declaring table's schema, for the qualified `source_tables` lineage.
    pub schema: String,
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

/// The read-time query window, resolved from the scan's `observed_at` predicate
/// (or the product default). Two half-open `[start, end)` millisecond ranges:
///
/// - *observed*: the queried `observed_at` range. The scan's filters are
///   re-applied above the computed table (`FilterPushDownType::Inexact`), so
///   every emitted row's `observed_at` must fall inside it. The declared-edge
///   branch also runs its validity overlap against this range.
/// - *source scan*: `observed` widened to whole 60s buckets. Derived rows are
///   `date_bin('60s')` aggregates keyed by the bucket start, so a bucket the
///   observed range selects must be scanned over its full extent — filtering
///   the source rows to the observed range instead would silently truncate the
///   RED numbers of the boundary buckets.
///
/// The bounds are plain millisecond values turned into timestamp literals on
/// demand — wall-clock snapshot semantics like `now()`, but as constants they
/// prune the source table scans without depending on constant-folding.
#[derive(Debug, Clone)]
pub struct GraphQueryWindow {
    observed_start_ms: i64,
    observed_end_ms: i64,
    source_start_ms: i64,
    source_end_ms: i64,
}

impl GraphQueryWindow {
    /// Builds the window for the queried `observed_at` range `[start_ms, end_ms)`.
    pub fn from_observed(start_ms: i64, end_ms: i64) -> Self {
        const BIN_MS: i64 = BIN_NANOS / 1_000_000;
        // Ceiling division that stays correct for pre-epoch (negative) values.
        let ceil_to_bin = |ms: i64| {
            ms.div_euclid(BIN_MS) * BIN_MS
                + if ms.rem_euclid(BIN_MS) == 0 {
                    0
                } else {
                    BIN_MS
                }
        };
        // A bucket `b` (a 60s multiple, `date_bin`'s floor of its rows) is
        // selected when `start <= b < end`: the first is ceil(start) and the
        // last one's rows extend to ceil(end).
        Self {
            observed_start_ms: start_ms,
            observed_end_ms: end_ms,
            source_start_ms: ceil_to_bin(start_ms),
            source_end_ms: ceil_to_bin(end_ms),
        }
    }

    /// Conservative default when a query carries no explicit time predicate: the
    /// last hour, so a bare `SELECT * FROM semantic_entities` never scans every
    /// declaring table's full history. This is a product default, not a cap.
    pub fn default_last_hour() -> Self {
        let end_ms = common_time::util::current_time_millis();
        Self::from_observed(end_ms - 60 * 60 * 1000, end_ms)
    }

    /// Start of the queried `observed_at` range (inclusive).
    pub fn observed_start(&self) -> Expr {
        ts_ms_lit(self.observed_start_ms)
    }

    /// End of the queried `observed_at` range (exclusive).
    pub fn observed_end(&self) -> Expr {
        ts_ms_lit(self.observed_end_ms)
    }

    /// Start of the source-table scan range (inclusive).
    pub fn source_start(&self) -> Expr {
        ts_ms_lit(self.source_start_ms)
    }

    /// End of the source-table scan range (exclusive).
    pub fn source_end(&self) -> Expr {
        ts_ms_lit(self.source_end_ms)
    }
}

fn ts_ms_lit(ms: i64) -> Expr {
    lit(ScalarValue::TimestampMillisecond(Some(ms), None))
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

/// The canonical entity-id expression for `id_columns`: the value verbatim for
/// a single column, the sorted `k=v,k=v` rendering for a composite. `col`
/// constructs the column reference (unqualified for registry branches,
/// join-side-qualified for the calls derivation).
fn entity_id_expr(id_columns: &[String], col: &dyn Fn(&str) -> Expr) -> Expr {
    if let [id] = id_columns {
        cast(col(id), DataType::Utf8)
    } else {
        let mut cols = id_columns.to_vec();
        cols.sort();
        sorted_kv_expr_with(&cols, false, col)
    }
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
fn sorted_kv_expr_with(sorted_cols: &[String], nullable: bool, col: &dyn Fn(&str) -> Expr) -> Expr {
    let mut parts = Vec::with_capacity(sorted_cols.len() * 3);
    for (i, column) in sorted_cols.iter().enumerate() {
        if i > 0 {
            parts.push(lit(","));
        }
        parts.push(lit(format!("{column}=")));
        parts.push(if nullable {
            core_fns::coalesce().call(vec![cast(col(column), DataType::Utf8), lit("")])
        } else {
            cast(col(column), DataType::Utf8)
        });
    }
    concat_expr(parts)
}

const REGISTRY_COLUMNS: [&str; 10] = [
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
];

const REGISTRY_VALID_COLUMN: &str = "__entity_valid";

/// Projects all entity declarations of one source table with one source scan.
///
/// Each output field is first built as an array whose entries correspond to the
/// table's declarations. Unnesting the arrays expands one source row into one
/// row per declared entity without cloning the table scan under `UNION ALL`.
fn registry_source(
    first: &EntityDeclaration,
    rest: &[EntityDeclaration],
    df: DataFrame,
    window: &GraphQueryWindow,
) -> DfResult<DataFrame> {
    let ts = ident(&first.time_index);
    let bin = bin_ms(ts.clone());
    let window_predicate = ts
        .clone()
        .gt_eq(window.source_start())
        .and(ts.lt(window.source_end()));

    let mut arrays = vec![Vec::new(); REGISTRY_COLUMNS.len() + 1];
    for decl in std::iter::once(first).chain(rest) {
        // CAST even a single-column id: id columns must be tags but not
        // necessarily strings, and the computed table declares entity_id
        // STRING. Composite ids additionally carry a JSON object of the id
        // columns in entity_id_attrs.
        let entity_id = entity_id_expr(&decl.id_columns, &|c| ident(c));
        let entity_id_attrs = if decl.id_columns.len() == 1 {
            null_json()
        } else {
            let mut cols = decl.id_columns.clone();
            cols.sort();
            json_object_expr(&cols)
        };

        let scope = match decl.scope_columns.as_slice() {
            [] => lit(""),
            // Scope columns are not required to be tags, so guard against NULL.
            [single] => cast_string_or_empty(single),
            _ => {
                let mut cols = decl.scope_columns.clone();
                cols.sort();
                sorted_kv_expr_with(&cols, true, &|c| ident(c))
            }
        };

        let descriptive = if decl.descriptive_columns.is_empty() {
            null_json()
        } else {
            json_object_expr(&decl.descriptive_columns)
        };

        let source_tables = parse_json_expr(lit(format!(
            "[{}]",
            json_quote(&format!("{}.{}", decl.schema, decl.table))
        )));

        // Tag columns may still be nullable; a NULL identity component
        // identifies nothing. Keep this predicate per declaration so a NULL
        // identity for one entity does not remove other entities on the row.
        let valid = decl.id_columns.iter().fold(lit(true), |predicate, id| {
            predicate.and(ident(id).is_not_null())
        });

        let row = [
            valid,
            bin.clone(),
            bin.clone(),
            bin.clone() + bin_interval(),
            bin.clone() + bin_interval(),
            lit(decl.entity_type.as_str()),
            entity_id,
            entity_id_attrs,
            scope,
            descriptive,
            source_tables,
        ];
        for (array, value) in arrays.iter_mut().zip(row) {
            array.push(value);
        }
    }

    let array_names = std::iter::once(REGISTRY_VALID_COLUMN)
        .chain(REGISTRY_COLUMNS)
        .collect::<Vec<_>>();
    let array_projection = array_names
        .iter()
        .zip(arrays)
        .map(|(name, values)| make_array(values).alias(*name))
        .collect::<Vec<_>>();

    df.filter(window_predicate)?
        .select(array_projection)?
        .unnest_columns(&array_names)?
        .filter(ident(REGISTRY_VALID_COLUMN))?
        .select(REGISTRY_COLUMNS.into_iter().map(ident).collect::<Vec<_>>())?
        .distinct()
}

/// A declaring table's scan paired with the entity declarations it carries —
/// the unit `build_registry_plan` derives registry rows from.
pub struct RegistrySource {
    pub declarations: Vec<EntityDeclaration>,
    pub scan: DataFrame,
}

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

/// Builds the `semantic_entities` registry plan: one branch and source scan per
/// declaring table, filtered to `window`, then `UNION ALL` across source tables.
/// Returns `None` when nothing declared an entity, so the computed table streams
/// empty.
pub fn build_registry_plan(
    sources: Vec<RegistrySource>,
    window: &GraphQueryWindow,
) -> DfResult<Option<LogicalPlan>> {
    let mut union_df: Option<DataFrame> = None;
    for source in sources {
        let Some((first, rest)) = source.declarations.split_first() else {
            continue;
        };
        union_df = union_all(union_df, registry_source(first, rest, source.scan, window)?)?;
    }
    Ok(union_df.map(DataFrame::into_unoptimized_plan))
}

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

/// The declared-edge branch: the latest revision per edge key (mito dedups on
/// primary key + `observed_at`, so re-asserting an edge stores a new revision),
/// filtered to declarations whose business validity overlaps the queried
/// window, projected to the relationship contract.
///
/// Validity semantics: `valid_from` defaults to the declaration time
/// (`observed_at`); a NULL `valid_until` means the edge holds for as long as
/// its row exists — TTL expiry retires it — so the projected `window_end` /
/// `fresh_until` take the queried window's upper bound. The output
/// `observed_at` is synthesized inside the queried range (its intersection
/// with the validity window): the scan's filters are re-applied above the
/// computed table, and the physical revision timestamp would fail them.
fn declared_edges(scan: DataFrame, window: &GraphQueryWindow) -> DfResult<DataFrame> {
    let revision = row_number()
        .partition_by(
            DECLARED_PRIMARY_KEY_COLUMNS
                .iter()
                .map(|c| ident(*c))
                .collect(),
        )
        .order_by(vec![ident(OBSERVED_AT_COLUMN).sort(false, false)])
        .build()?
        .alias(DECLARED_REVISION_COLUMN);

    let eff_valid_from =
        core_fns::coalesce().call(vec![ident("valid_from"), ident(OBSERVED_AT_COLUMN)]);
    let eff_valid_until =
        core_fns::coalesce().call(vec![ident("valid_until"), window.observed_end()]);
    let overlap = eff_valid_from.clone().lt(window.observed_end()).and(
        ident("valid_until")
            .is_null()
            .or(ident("valid_until").gt(window.observed_start())),
    );
    // Tags come out of the storage engine dictionary-encoded; cast to plain
    // strings so the union with the derived branches type-aligns.
    let tag_utf8 = |name: &str| cast(ident(name), DataType::Utf8).alias(name);

    scan.window(vec![revision])?
        .filter(ident(DECLARED_REVISION_COLUMN).eq(lit(1_u64)))?
        .filter(overlap)?
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

/// The `calls` derivation (RFC §3a) over `traces`: pair each client span
/// with its child server span on `trace_id` + `parent_span_id`, project to
/// `service`, union the pairs of all trace tables, and aggregate to RED metrics
/// per 60s window in one pass — so an edge observed across several trace tables
/// yields one row, not per-table fragments. This is the plan form of the Tempo
/// servicegraph connector. Virtual-node edges (uninstrumented peers) are a
/// separate branch, added on top of this. Column names are the fixed
/// `greptime_trace_v1` schema (the reason `table_data_model = greptime_trace_v1`
/// is required); `span_status_code` is a string column (`STATUS_CODE_ERROR`),
/// verified against the trace ingest path. Returns `None` when there is no
/// trace table.
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

    use common_catalog::consts::SERVICE_NAME_COLUMN;
    use datafusion::arrow::array::{
        Array, ArrayRef, BinaryArray, Float64Array, Int64Array, StringArray,
        TimestampMillisecondArray, TimestampNanosecondArray, UInt64Array,
    };
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    use super::*;

    fn test_window() -> GraphQueryWindow {
        GraphQueryWindow::from_observed(0, 10 * 60 * 1000)
    }

    #[test]
    fn window_source_scan_covers_whole_buckets() {
        // Buckets selected by [10:30.5', 11:30') is exactly {11:00'}; its source
        // rows span [11:00', 12:00').
        let window = GraphQueryWindow::from_observed(630_000, 690_000);
        assert_eq!(
            (window.source_start_ms, window.source_end_ms),
            (660_000, 720_000)
        );

        // Aligned bounds stay put.
        let window = GraphQueryWindow::from_observed(600_000, 720_000);
        assert_eq!(
            (window.source_start_ms, window.source_end_ms),
            (600_000, 720_000)
        );

        // A sub-bucket range that selects no bucket start scans nothing.
        let window = GraphQueryWindow::from_observed(610_000, 650_000);
        assert!(window.source_start_ms >= window.source_end_ms);

        // Pre-epoch bounds round toward the correct buckets.
        let window = GraphQueryWindow::from_observed(-90_000, -30_000);
        assert_eq!((window.source_start_ms, window.source_end_ms), (-60_000, 0));
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
            schema: "public".to_string(),
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
            vec![RegistrySource {
                declarations: vec![decl("service", &["service_name"])],
                scan: df,
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
            vec![Some(r#"["public.app_latency"]"#.to_string()); batch.num_rows()]
        );
    }

    #[tokio::test]
    async fn registry_composite_identity_and_descriptive_escaping() {
        let ctx = metric_table_ctx();
        let df = ctx.table("app_latency").await.unwrap();
        let mut declaration = decl("process", &["service_name", "pid"]);
        declaration.descriptive_columns = vec!["host".to_string()];
        let plan = build_registry_plan(
            vec![RegistrySource {
                declarations: vec![declaration],
                scan: df,
            }],
            &test_window(),
        )
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
        let plan = build_registry_plan(
            vec![RegistrySource {
                declarations: vec![single],
                scan: df,
            }],
            &test_window(),
        )
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
        let plan = build_registry_plan(
            vec![RegistrySource {
                declarations: vec![multi],
                scan: df,
            }],
            &test_window(),
        )
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
    async fn registry_skips_null_identity_rows() {
        let ctx = metric_table_ctx();
        let df = ctx.table("app_latency").await.unwrap();
        let plan = build_registry_plan(
            vec![RegistrySource {
                declarations: vec![decl("host", &["host"])],
                scan: df,
            }],
            &test_window(),
        )
        .unwrap()
        .unwrap();
        let batches = collect(&ctx, plan).await;
        let mut ids: Vec<String> = batches.iter().flat_map(|b| strings(b, 5)).collect();
        ids.sort();
        assert_eq!(ids, vec!["h2", r#"we"ird\host"#]);
    }

    #[tokio::test]
    async fn registry_expands_declarations_with_one_source_scan() {
        let ctx = metric_table_ctx();
        let df = ctx.table("app_latency").await.unwrap();
        let plan = build_registry_plan(
            vec![RegistrySource {
                declarations: vec![decl("service", &["service_name"]), decl("host", &["pid"])],
                scan: df,
            }],
            &test_window(),
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            plan.display_indent()
                .to_string()
                .matches("TableScan: app_latency")
                .count(),
            1
        );
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

    #[test]
    fn declared_relationships_expr_shape() {
        let expr = build_declared_relationships_expr(common_catalog::consts::DEFAULT_CATALOG_NAME);

        assert_eq!(expr.schema_name, DEFAULT_PRIVATE_SCHEMA_NAME);
        assert_eq!(expr.table_name, SEMANTIC_RELATIONSHIPS_DECLARED_TABLE_NAME);
        assert!(expr.create_if_not_exists);
        assert_eq!(expr.time_index, OBSERVED_AT_COLUMN);
        assert_eq!(expr.primary_keys, DECLARED_PRIMARY_KEY_COLUMNS);
        // append_mode is unset so the table gets the default LastRow merge
        // (last-write-wins per primary key + observed_at).
        assert!(!expr.table_options.contains_key("append_mode"));
        assert_eq!(
            expr.table_options.get(TTL_KEY).map(String::as_str),
            Some("30d")
        );

        // Every primary-key column exists and is a tag.
        for pk in DECLARED_PRIMARY_KEY_COLUMNS {
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

        // `attributes` is a JSON (JSONB-encoded Binary) field, matching the
        // computed table's json column so the union needs no per-scan parse.
        let attributes = expr
            .column_defs
            .iter()
            .find(|c| c.name == "attributes")
            .unwrap();
        assert_eq!(attributes.data_type, ColumnDataType::Binary as i32);
        assert_eq!(
            attributes.datatype_extension,
            Some(ColumnDataTypeExtension {
                type_ext: Some(TypeExt::JsonType(JsonTypeExtension::JsonBinary.into())),
            })
        );
    }

    fn build_relationships_plan_for_test(
        traces: Vec<CallsSource>,
        window: &GraphQueryWindow,
    ) -> DfResult<Option<LogicalPlan>> {
        build_relationships_plan(traces, None, window)
    }

    fn ts_values(batch: &RecordBatch, column: usize) -> Vec<i64> {
        let array = batch
            .column(column)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        (0..array.len()).map(|i| array.value(i)).collect()
    }

    /// Registers a declared-edge table holding:
    /// - frontend->db: two revisions, the newer retiring the edge at 300s;
    /// - api->cache: expired at 100s;
    /// - agent-1->search: open-ended `provenance = 'agent'` with JSON attributes.
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
        let no_ts = || Arc::new(TimestampMillisecondArray::from(vec![None::<i64>; 4])) as ArrayRef;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![
                    100_000, 200_000, 50_000, 90_000,
                ])) as ArrayRef,
                no_ts(), // window_start
                no_ts(), // window_end
                no_ts(), // fresh_until
                no_ts(), // valid_from
                Arc::new(TimestampMillisecondArray::from(vec![
                    None,
                    Some(300_000),
                    Some(100_000),
                    None,
                ])),
                Arc::new(StringArray::from(vec![
                    "service", "service", "service", "agent",
                ])),
                Arc::new(StringArray::from(vec![
                    "frontend", "frontend", "api", "agent-1",
                ])),
                Arc::new(StringArray::from(vec![
                    "depends_on",
                    "depends_on",
                    "depends_on",
                    "uses",
                ])),
                Arc::new(StringArray::from(vec![
                    "service", "service", "service", "tool",
                ])),
                Arc::new(StringArray::from(vec!["db", "db", "cache", "search"])),
                Arc::new(StringArray::from(vec![
                    "declared", "declared", "declared", "agent",
                ])),
                Arc::new(StringArray::from(vec![""; 4])),
                Arc::new(StringArray::from(vec![""; 4])),
                Arc::new(Float64Array::from(vec![
                    Some(1.0),
                    Some(1.0),
                    Some(1.0),
                    Some(0.8),
                ])),
                Arc::new(Int64Array::from(vec![None::<i64>; 4])),
                Arc::new(Int64Array::from(vec![None::<i64>; 4])),
                Arc::new(Float64Array::from(vec![None::<f64>; 4])),
                Arc::new(Int64Array::from(vec![None::<i64>; 4])),
                Arc::new(BinaryArray::from(vec![
                    None,
                    None,
                    None,
                    Some(attrs.as_slice()),
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

        // api->cache expired before the window; frontend->db keeps only the
        // latest revision.
        assert_eq!(rows.len(), 2);

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
        // One trace-derived calls edge plus all three declared edges (the
        // [0s, 600s) window overlaps every declared validity).
        assert_eq!(provenance, vec!["agent", "declared", "declared", "trace"]);
    }
}
