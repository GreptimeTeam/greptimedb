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

mod conventions;
mod relationships;

use std::sync::{Arc, LazyLock};

use api::v1::column_data_type_extension::TypeExt;
use api::v1::{
    ColumnDataType, ColumnDataTypeExtension, ColumnDef, CreateTableExpr, JsonTypeExtension,
    SemanticType,
};
use common_catalog::consts::{
    CONFIDENCE_COLUMN, DEFAULT_CATALOG_NAME, DEFAULT_PRIVATE_SCHEMA_NAME, DST_ID_COLUMN,
    DST_TYPE_COLUMN, DURATION_COUNT_COLUMN, DURATION_SUM_COLUMN, EDGE_ATTRIBUTES_COLUMN,
    ENTITY_DESCRIPTIVE_COLUMN, ENTITY_ID_ATTRS_COLUMN, ENTITY_ID_COLUMN, ENTITY_SCOPE_COLUMN,
    ENTITY_TYPE_COLUMN, ERROR_COUNT_COLUMN, FRESH_UNTIL_COLUMN, GENERATION_ID_COLUMN,
    OBSERVED_AT_COLUMN, PROVENANCE_COLUMN, REL_TYPE_COLUMN, REQUEST_COUNT_COLUMN,
    SEMANTIC_RELATIONSHIPS_DECLARED_TABLE_NAME, SOURCE_TABLES_COLUMN, SRC_ID_COLUMN,
    SRC_TYPE_COLUMN, VALID_FROM_COLUMN, VALID_UNTIL_COLUMN, WINDOW_END_COLUMN, WINDOW_START_COLUMN,
};
use common_function::function::FunctionContext;
use common_function::function_registry::FUNCTION_REGISTRY;
pub use conventions::{
    Conventions, ENTITY_TYPE_GEN_AI_AGENT, ENTITY_TYPE_GEN_AI_MODEL, ENTITY_TYPE_GEN_AI_TOOL,
    ENTITY_TYPE_HOST, ENTITY_TYPE_K8S_CONTAINER, ENTITY_TYPE_K8S_NODE, ENTITY_TYPE_K8S_POD,
    ENTITY_TYPE_K8S_WORKLOAD, ENTITY_TYPE_PROCESS, ENTITY_TYPE_SERVICE,
    ENTITY_TYPE_SERVICE_INSTANCE, ImplicitEntity, PROVENANCE_AGENT, PROVENANCE_ATTRIBUTE,
    PROVENANCE_DECLARED, PROVENANCE_TRACE, REL_TYPE_CALLS, REL_TYPE_CONTAINS, REL_TYPE_DEPENDS_ON,
    REL_TYPE_INVOKES, REL_TYPE_OWNS, REL_TYPE_PART_OF, REL_TYPE_RUNS_ON, REL_TYPE_USES,
    conventions,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::dataframe::DataFrame;
use datafusion::functions::{core as core_fns, datetime as datetime_fns, string as string_fns};
use datafusion::functions_nested::expr_fn::make_array;
use datafusion_common::{Column, Result as DfResult, ScalarValue};
use datafusion_expr::{Expr, LogicalPlan, ScalarUDF, cast, ident, lit};
pub use relationships::{
    CallsSource, CoDeclaredSource, DeclaredSource, RelationshipSources, build_relationships_plan,
};
use store_api::mito_engine_options::{APPEND_MODE_KEY, MERGE_MODE_KEY, TTL_KEY};

/// Whether an existing declared-edge table still matches the canonical
/// definition ([`build_declared_relationships_expr`]): columns, time index,
/// primary key, engine, and merge behaviour — the union branch's revision and
/// dedup semantics lean on all of these. A mismatch (upgrade skew) must be
/// surfaced, not silently derived wrong; DROP resets the table.
pub fn declared_relationships_schema_matches(table_info: &table::metadata::TableInfo) -> bool {
    let meta = &table_info.meta;
    if meta.engine != common_catalog::consts::MITO_ENGINE {
        return false;
    }
    let schema = &meta.schema;
    if schema
        .timestamp_column()
        .is_none_or(|column| column.name != OBSERVED_AT_COLUMN)
    {
        return false;
    }
    let primary_keys = meta
        .primary_key_indices
        .iter()
        .filter_map(|idx| schema.column_schemas().get(*idx))
        .map(|column| column.name.as_str());
    if !primary_keys.eq(DECLARED_PRIMARY_KEY_COLUMNS) {
        return false;
    }
    // LastRow merge is what makes a re-asserted edge a *revision*; append mode
    // or another merge mode would change the read-side dedup semantics.
    let options = &meta.options.extra_options;
    if options.get(APPEND_MODE_KEY).map(String::as_str) == Some("true")
        || options
            .get(MERGE_MODE_KEY)
            .is_some_and(|mode| mode != "last_row")
    {
        return false;
    }

    let canonical = build_declared_relationships_expr(DEFAULT_CATALOG_NAME);
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

/// Default retention for the declared-edge table; expiry slides the topology window.
const DEFAULT_DECLARED_RELATIONSHIPS_TTL: &str = "90d";
/// Environment variable overriding the declared-edge table's TTL at creation
/// time (e.g. `180d`, `forever`).
// TODO(entity-graph): promote this to a real configuration option.
const DECLARED_RELATIONSHIPS_TTL_ENV: &str = "GREPTIMEDB_DECLARED_RELATIONSHIPS_TTL";

fn declared_relationships_ttl() -> String {
    std::env::var(DECLARED_RELATIONSHIPS_TTL_ENV)
        .ok()
        .map(|ttl| ttl.trim().to_string())
        .filter(|ttl| !ttl.is_empty())
        .unwrap_or_else(|| DEFAULT_DECLARED_RELATIONSHIPS_TTL.to_string())
}

/// The primary-key (tag) columns, in key order. Starting with the source endpoint
/// makes out-edge lookup (`WHERE src_type=? AND src_id=?`) a key-prefix scan;
/// `provenance` and `generation_id` are in the key so a declared edge and a
/// (future) derived edge for the same pair coexist without clobbering.
pub const DECLARED_PRIMARY_KEY_COLUMNS: [&str; 8] = [
    SRC_TYPE_COLUMN,
    SRC_ID_COLUMN,
    REL_TYPE_COLUMN,
    DST_TYPE_COLUMN,
    DST_ID_COLUMN,
    PROVENANCE_COLUMN,
    ENTITY_SCOPE_COLUMN,
    GENERATION_ID_COLUMN,
];

/// The externally visible edge identity: the primary key minus `scope` and
/// `generation_id`, which the computed table does not expose. Revision ranking
/// uses this identity, or assertions differing only in those two columns would
/// surface as indistinguishable duplicate rows.
const DECLARED_EDGE_IDENTITY_COLUMNS: [&str; 6] = [
    SRC_TYPE_COLUMN,
    SRC_ID_COLUMN,
    REL_TYPE_COLUMN,
    DST_TYPE_COLUMN,
    DST_ID_COLUMN,
    PROVENANCE_COLUMN,
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
        field(WINDOW_START_COLUMN, ColumnDataType::TimestampMillisecond),
        field(WINDOW_END_COLUMN, ColumnDataType::TimestampMillisecond),
        field(FRESH_UNTIL_COLUMN, ColumnDataType::TimestampMillisecond),
        // Declared-only business validity. NULL valid_from = valid since the
        // declaration; NULL valid_until = valid for as long as the row exists
        // (TTL expiry retires the edge with the row).
        field(VALID_FROM_COLUMN, ColumnDataType::TimestampMillisecond),
        field(VALID_UNTIL_COLUMN, ColumnDataType::TimestampMillisecond),
        // Endpoints + edge identity (all tags, in primary-key order).
        tag(SRC_TYPE_COLUMN),
        tag(SRC_ID_COLUMN),
        tag(REL_TYPE_COLUMN),
        tag(DST_TYPE_COLUMN),
        tag(DST_ID_COLUMN),
        tag(PROVENANCE_COLUMN),
        tag(ENTITY_SCOPE_COLUMN),
        tag(GENERATION_ID_COLUMN),
        // Confidence + RED metrics (populated for derived edges; usually NULL here).
        field(CONFIDENCE_COLUMN, ColumnDataType::Float64),
        field(REQUEST_COUNT_COLUMN, ColumnDataType::Int64),
        field(ERROR_COUNT_COLUMN, ColumnDataType::Int64),
        field(DURATION_SUM_COLUMN, ColumnDataType::Float64),
        field(DURATION_COUNT_COLUMN, ColumnDataType::Int64),
        // JSONB, so the union matches the computed table's json column
        // without a per-scan parse.
        json_field(EDGE_ATTRIBUTES_COLUMN),
    ];

    let table_options = [(TTL_KEY.to_string(), declared_relationships_ttl())]
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
/// A row identifies an entity only when every identity component is present
/// and non-empty: kube-state-metrics descriptors emit empty-string labels (an
/// unscheduled pod's `node`, an owner-less pod's `owner_*`), and an empty
/// string is never a meaningful entity id.
pub(crate) fn identifies(column: &str) -> Expr {
    ident(column)
        .is_not_null()
        .and(cast(ident(column), DataType::Utf8).not_eq(lit("")))
}

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

/// The `json_object` UDF, resolved like [`PARSE_JSON_UDF`]. It assembles the
/// JSONB binary directly from the value columns, so runtime values need no JSON
/// text escaping — control characters included.
static JSON_OBJECT_UDF: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(
        FUNCTION_REGISTRY
            .get_function("json_object")
            .expect("json_object must be registered")
            .provide(FunctionContext::default()),
    )
});

/// Builds a JSONB object with one entry per column: key = the column name,
/// value = the column rendered as a string, NULL coalesced to `""` so one NULL
/// column does not null the entry (descriptive columns are nullable). Keys come
/// out sorted — JSONB objects are key-ordered regardless of input order.
fn json_object_expr(columns: &[String]) -> Expr {
    if columns.is_empty() {
        return parse_json_expr(lit("{}"));
    }
    let mut args = Vec::with_capacity(columns.len() * 2);
    for column in columns {
        args.push(lit(column.as_str()));
        args.push(cast_string_or_empty(column));
    }
    cast(JSON_OBJECT_UDF.call(args), DataType::Binary)
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
    OBSERVED_AT_COLUMN,
    WINDOW_START_COLUMN,
    WINDOW_END_COLUMN,
    FRESH_UNTIL_COLUMN,
    ENTITY_TYPE_COLUMN,
    ENTITY_ID_COLUMN,
    ENTITY_ID_ATTRS_COLUMN,
    ENTITY_SCOPE_COLUMN,
    ENTITY_DESCRIPTIVE_COLUMN,
    SOURCE_TABLES_COLUMN,
];

const REGISTRY_VALID_COLUMN: &str = "__entity_valid";

/// Expands one source row into one output row per entry of `rows` with a
/// single source scan: each output field is first built as an array whose
/// entries correspond to the rows, then unnested. Every row is `[valid,
/// values...]` aligned with `columns`; rows whose `valid` expression is false
/// are dropped, and the distinct output columns are `columns`.
fn unnest_rows(
    df: DataFrame,
    window_predicate: Expr,
    valid_column: &'static str,
    columns: &[&'static str],
    rows: Vec<Vec<Expr>>,
) -> DfResult<DataFrame> {
    let mut arrays = vec![Vec::with_capacity(rows.len()); columns.len() + 1];
    for row in rows {
        debug_assert_eq!(row.len(), arrays.len());
        for (array, value) in arrays.iter_mut().zip(row) {
            array.push(value);
        }
    }
    let array_names = std::iter::once(valid_column)
        .chain(columns.iter().copied())
        .collect::<Vec<_>>();
    let array_projection = array_names
        .iter()
        .zip(arrays)
        .map(|(name, values)| make_array(values).alias(*name))
        .collect::<Vec<_>>();

    df.filter(window_predicate)?
        .select(array_projection)?
        .unnest_columns(&array_names)?
        .filter(ident(valid_column))?
        .select(columns.iter().map(|c| ident(*c)).collect::<Vec<_>>())?
        .distinct()
}

/// Projects all entity declarations of one source table with one source scan
/// via [`unnest_rows`].
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

    let mut rows = Vec::with_capacity(1 + rest.len());
    for decl in std::iter::once(first).chain(rest) {
        // CAST even a single-column id: id columns need not be strings, and
        // the computed table declares entity_id STRING. Composite ids
        // additionally carry a JSON object of the id columns in
        // entity_id_attrs.
        let entity_id = entity_id_expr(&decl.id_columns, &|c| ident(c));
        let entity_id_attrs = if decl.id_columns.len() == 1 {
            null_json()
        } else {
            json_object_expr(&decl.id_columns)
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

        // Keep this predicate per declaration so an absent identity for one
        // entity does not remove other entities on the row.
        let valid = decl
            .id_columns
            .iter()
            .fold(lit(true), |predicate, id| predicate.and(identifies(id)));

        rows.push(vec![
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
        ]);
    }

    unnest_rows(
        df,
        window_predicate,
        REGISTRY_VALID_COLUMN,
        &REGISTRY_COLUMNS,
        rows,
    )
}

/// A declaring table's scan paired with the entity declarations it carries —
/// the unit `build_registry_plan` derives registry rows from.
pub struct RegistrySource {
    pub declarations: Vec<EntityDeclaration>,
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

/// RecordBatch readers shared by this module's tests and the `relationships`
/// tests.
#[cfg(test)]
pub(crate) mod test_util {
    use datafusion::arrow::array::{Array, BinaryArray, StringArray, TimestampMillisecondArray};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::LogicalPlan;

    pub(crate) async fn collect(ctx: &SessionContext, plan: LogicalPlan) -> Vec<RecordBatch> {
        ctx.execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
    }

    pub(crate) fn json_texts(batch: &RecordBatch, column: usize) -> Vec<Option<String>> {
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

    pub(crate) fn strings(batch: &RecordBatch, column: usize) -> Vec<String> {
        let array = batch
            .column(column)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (0..array.len())
            .map(|i| array.value(i).to_string())
            .collect()
    }

    pub(crate) fn ts_values(batch: &RecordBatch, column: usize) -> Vec<i64> {
        let array = batch
            .column(column)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        (0..array.len()).map(|i| array.value(i)).collect()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray, TimestampMillisecondArray};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    use super::test_util::{collect, json_texts, strings};
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
                    Some("we\"ird\\\nhost"),
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
        // descriptive JSON keeps `\`, `"` and control characters intact in
        // runtime values, NULL -> "".
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
                    Some(r#"{"host":"we\"ird\\\nhost"}"#.to_string()),
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
        assert_eq!(scopes, vec!["", "h2", "we\"ird\\\nhost"]);

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
                "host=we\"ird\\\nhost,pid=42"
            ]
        );
    }

    #[tokio::test]
    async fn registry_skips_absent_identity_rows() {
        // NULL identifies nothing, and so does a kube-state-metrics-style
        // empty label (an unscheduled pod's `node` arrives as "").
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("host", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![1_000, 2_000, 3_000])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("h2"), None, Some("")])),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table(
            "app_latency",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
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
        let ids: Vec<String> = batches.iter().flat_map(|b| strings(b, 5)).collect();
        assert_eq!(ids, vec!["h2"]);
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

    /// A `TableInfo` derived from the canonical declared-edge definition, with
    /// injection points for each aspect the schema matcher must check.
    fn declared_table_info(
        mutate_columns: impl FnOnce(&mut Vec<datatypes::schema::ColumnSchema>),
        engine: &str,
        primary_key_indices: Vec<usize>,
        extra_options: &[(&str, &str)],
    ) -> table::metadata::TableInfo {
        let canonical = build_declared_relationships_expr("greptime");
        let mut columns: Vec<datatypes::schema::ColumnSchema> = canonical
            .column_defs
            .iter()
            .map(|def| {
                let wrapper = api::helper::ColumnDataTypeWrapper::try_new(
                    def.data_type,
                    def.datatype_extension.clone(),
                )
                .unwrap();
                datatypes::schema::ColumnSchema::new(
                    &def.name,
                    datatypes::prelude::ConcreteDataType::from(wrapper),
                    def.is_nullable,
                )
                .with_time_index(def.name == OBSERVED_AT_COLUMN)
            })
            .collect();
        mutate_columns(&mut columns);
        let schema = Arc::new(
            datatypes::schema::SchemaBuilder::try_from_columns(columns)
                .unwrap()
                .build()
                .unwrap(),
        );
        let options = table::requests::TableOptions {
            extra_options: extra_options
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            ..Default::default()
        };
        let meta = table::metadata::TableMeta {
            schema,
            primary_key_indices,
            value_indices: vec![],
            engine: engine.to_string(),
            next_column_id: 1,
            options,
            created_on: Default::default(),
            updated_on: Default::default(),
            partition_key_indices: vec![],
            column_ids: vec![],
        };
        table::metadata::TableInfoBuilder::default()
            .table_id(1)
            .name(SEMANTIC_RELATIONSHIPS_DECLARED_TABLE_NAME)
            .catalog_name("greptime")
            .schema_name(DEFAULT_PRIVATE_SCHEMA_NAME)
            .table_version(0)
            .table_type(table::metadata::TableType::Base)
            .meta(meta)
            .build()
            .unwrap()
    }

    #[test]
    fn declared_schema_match_checks_more_than_columns() {
        let mito = common_catalog::consts::MITO_ENGINE;
        let canonical_pk = || (6..14).collect::<Vec<_>>();

        let ok = declared_table_info(|_| {}, mito, canonical_pk(), &[(TTL_KEY, "180d")]);
        assert!(declared_relationships_schema_matches(&ok));
        // The canonical expr must declare attributes as json, matching the
        // computed table's column.
        assert_eq!(
            ok.meta
                .schema
                .column_schema_by_name("attributes")
                .map(|c| c.data_type.clone()),
            Some(datatypes::prelude::ConcreteDataType::json_datatype())
        );
        let ok = declared_table_info(
            |_| {},
            mito,
            canonical_pk(),
            &[(MERGE_MODE_KEY, "last_row")],
        );
        assert!(declared_relationships_schema_matches(&ok));

        let wrong_engine = declared_table_info(|_| {}, "metric", canonical_pk(), &[]);
        assert!(!declared_relationships_schema_matches(&wrong_engine));

        let pk_missing_generation_id = declared_table_info(|_| {}, mito, (6..13).collect(), &[]);
        assert!(!declared_relationships_schema_matches(
            &pk_missing_generation_id
        ));

        let wrong_time_index = declared_table_info(
            |columns| {
                columns[0] = columns[0].clone().with_time_index(false);
                columns[4] = datatypes::schema::ColumnSchema::new(
                    "valid_from",
                    datatypes::prelude::ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true);
            },
            mito,
            canonical_pk(),
            &[],
        );
        assert!(!declared_relationships_schema_matches(&wrong_time_index));

        let append_mode =
            declared_table_info(|_| {}, mito, canonical_pk(), &[(APPEND_MODE_KEY, "true")]);
        assert!(!declared_relationships_schema_matches(&append_mode));

        let wrong_column_type = declared_table_info(
            |columns| {
                columns[14] = datatypes::schema::ColumnSchema::new(
                    "confidence",
                    datatypes::prelude::ConcreteDataType::int64_datatype(),
                    true,
                );
            },
            mito,
            canonical_pk(),
            &[],
        );
        assert!(!declared_relationships_schema_matches(&wrong_column_type));
    }
}
