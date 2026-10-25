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

//! The native fused-series route of the PromQL planner.
//!
//! [`PromPlanner::try_plan_native_fused`] offers a small experimental subset to the native
//! compiler: a vector selector (optionally parenthesized), or nested unary `rate`, `increase`,
//! `sum_over_time`, and `abs` calls over a selector. It replaces that expression with one
//! [`FusedSeries`] node over the selector's table scan. Other expression shapes keep the existing
//! planner's route.
//!
//! What the node needs from here is three things the existing planner already resolves: the raw
//! series schema the tree's input was compiled against (the selector's selected field columns), the
//! statement's evaluation grid, and the logical tag names the output carries. The output schema is
//! built to the shape the existing unary pipeline produces — the time index, then the series
//! fields, then the tags — so a node planned here can be consumed by the same code that consumes a
//! `SeriesNormalize` output.

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use common_catalog::consts::{METRIC_ENGINE, MITO_ENGINE, MITO2_ENGINE};
use common_query::prelude::OTLP_AGGREGATION_TEMPORALITY_LABEL;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, TimeUnit as ArrowTimeUnit};
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Extension, LogicalPlan, TableScanBuilder};
use datafusion::prelude::Expr as DfExpr;
use datafusion_expr::Expr;
use datatypes::series::SeriesOrdering;
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use promql::extension_plan::{FusedSeries, FusedSeriesBinding};
use promql::native::plan::{NativeExpr, NativeNode, NativeTree, reads_across_series};
use promql::vector_match::{Cardinality, MatchingLabels};
use promql_parser::label::Matchers;
use promql_parser::parser::value::ValueType;
use promql_parser::parser::{
    EvalStmt, Expr as PromExpr, MatrixSelector, ParenExpr, VectorSelector,
};
use session::context::QueryContextRef;
use snafu::ResultExt;
use store_api::metric_engine_consts::{DATA_SCHEMA_TABLE_ID_COLUMN_NAME, METRIC_ENGINE_NAME};
use table::metadata::TableMeta;

use crate::promql::error::{DataFusionPlanningSnafu, Result};
use crate::promql::native_compile::{compile_selectors, statement_grid};
use crate::promql::planner::PromPlanner;

/// The query-context key that turns the native fused-series route on for one query.
///
/// Named after the engine-wide option it mirrors ([`QueryOptions::experimental_native_promql`]),
/// and read exactly the way the other per-query override in this crate is read
/// ([`remote_dyn_filter_pushdown_enabled_from_extensions`]): a query-context extension, which a
/// client can set per request over the HTTP hints header or as a gRPC context key. The engine-wide
/// option is applied while building query-engine state; this planner reads the per-query request
/// extension at the call site; see [`native_promql_requested`].
///
/// [`QueryOptions::experimental_native_promql`]: crate::options::QueryOptions::experimental_native_promql
/// [`remote_dyn_filter_pushdown_enabled_from_extensions`]:
///     crate::options::remote_dyn_filter_pushdown_enabled_from_extensions
pub const NATIVE_PROMQL_EXTENSION_KEY: &str = "query.experimental_native_promql";

/// Whether `query_ctx` asks for the native fused-series route.
///
/// The route is off unless the query asks for it, so a query that says nothing keeps the plan the
/// existing planner builds. The value is compared literally (`true` in any case) rather than through
/// a shared boolean parser: an unrecognized value leaves the route off, which is the same plan as
/// the flag never having been read, so a typo costs a slow query and never a wrong one.
pub(crate) fn native_promql_requested(query_ctx: &QueryContextRef) -> bool {
    query_ctx
        .extension(NATIVE_PROMQL_EXTENSION_KEY)
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

/// The ordering this route compiles its trees against.
///
/// Which ordering a raw-series scan declares is decided when the region scanner is prepared, during
/// physical planning, and the logical planner cannot read it: a legacy scan declares `KeyThenTime`
/// and a two-phase scan `Contiguous`. So the route names the weakest ordering both provide —
/// `Contiguous`, every series contiguous and the series themselves unordered — rather than
/// overclaiming the legacy scan's stronger `KeyThenTime`. A native evaluation only refuses
/// `Interleaved`, so a tree compiled here runs on either scanner.
const NATIVE_ORDERING: SeriesOrdering = SeriesOrdering::Contiguous;

/// The primary-key column names that carry pipeline metadata rather than series identity.
///
/// A window, a map and a binary operation drop these three labels wherever they read them, while
/// this route reports a series under the primary key it carries. A key column with one of these
/// names therefore keeps the existing plan however whole the rest of the key is: a node that drops
/// one of them can leave two raw series on one canonical label set, and the label set then no
/// longer names the series the partition holds.
const METADATA_KEY_TAGS: [&str; 3] = ["__name__", "__type__", "__unit__"];

/// One prepared occurrence of a selector: the scan that reads it, the raw-series layout it was
/// prepared against, and the columns its series are reported under.
///
/// The last three are the node's rather than one scan's — the binding names a time index, a tag list
/// and a metric name once — so they are compared across the occurrences and then travel to the
/// binding together. The first two are what [`same_source`] compares, which is the check that two
/// occurrences of one selector are one scan.
type PreparedInput = (
    LogicalPlan,
    Arc<SeriesSchema>,
    Option<Arc<str>>,
    String,
    Vec<String>,
);

impl PromPlanner {
    /// Plans `prom_expr` as one [`FusedSeries`] node, or returns `None` to keep the existing plan.
    ///
    /// The route is offered the whole expression so its tree computes the requested expression
    /// over the planner's resolved selector context: table name, time index, tag columns, and
    /// field columns (after any `__field__` matcher). These are the raw series schema the input
    /// tree is compiled against.
    ///
    /// `None` is not a failure: unsupported shapes and queries this route cannot bind faithfully
    /// continue through the existing planner. Admission is limited to selectors and unary calls
    /// named above, plus the scan, schema, matcher, and grid constraints checked below.
    pub(super) async fn try_plan_native_fused(
        &mut self,
        prom_expr: &PromExpr,
    ) -> Result<Option<LogicalPlan>> {
        if !self.native_fused {
            return Ok(None);
        }
        // Only the statement root is offered: the tree is compiled against the statement's grid,
        // and a subquery's expression is evaluated on its own, smaller one. A range-vector root is
        // the instant query's raw sample result, which the existing path reports as a matrix rather
        // than as one point per grid position.
        if prom_expr.value_type() == ValueType::Matrix {
            return Ok(None);
        }

        let mut selectors = Vec::new();
        if !collect_selectors(prom_expr, &mut selectors)
            || selectors.is_empty()
            || selectors.len() > 2
        {
            return Ok(None);
        }
        // Preserve the original single-selector admission allowlist. Broader binary syntax is only
        // considered for a two-occurrence root, where the compiled-tree proof below constrains it.
        if selectors.len() == 1 && admitted_selector(prom_expr).is_none() {
            return Ok(None);
        }
        let Some(stmt) = self.native_statement(prom_expr) else {
            return Ok(None);
        };
        let Ok(grid) = statement_grid(&stmt) else {
            return Ok(None);
        };
        let original_context = self.ctx.clone();
        let preparation = async {
            let mut prepared: Vec<PreparedInput> = Vec::new();
            for selector in &selectors {
                self.ctx = original_context.clone();
                // These matchers are alternatives the one scan below the node cannot represent, so the
                // query keeps the plan the existing path builds for it.
                if !selector.matchers.or_matchers.is_empty() {
                    return Ok(None);
                }
                let matchers =
                    match self.preprocess_label_matchers(&selector.matchers, &selector.name) {
                        Ok(m) => m,
                        Err(_) => return Ok(None),
                    };
                if self.setup_context().await?.is_some() {
                    return Ok(None);
                }
                let (Some(metric), Some(time)) = (
                    self.ctx.table_name.clone(),
                    self.ctx.time_index_column.clone(),
                ) else {
                    return Ok(None);
                };
                let tags = self.ctx.tag_columns.clone();
                let fields = self.ctx.field_columns.clone();
                // A `__field__` matcher narrows the field set in a projection the existing planner
                // builds, and the raw series scan below this node has no equivalent: it reports every
                // field the scan carries. Binding all of them would fold fields the query excluded, so
                // the route is declined until the field selector is reused here.
                if self.ctx.field_column_matcher.is_some() || fields.is_empty() {
                    return Ok(None);
                }
                // On a delta-temporality table a series' samples are increments, which the existing
                // planner sums over the window for `increase` and divides by the range for `rate`
                // ([`PromPlanner::select_delta_range_math`]), per series because the tag can differ
                // between them. A compiled tree computes both for a cumulative series, so any
                // `rate`/`increase` over such a table keeps the existing plan; a bare selector still
                // folds. The guard reads the whole statement, not this selector's subtree: one such
                // call anywhere admits a tree the existing plan would not build.
                if tags
                    .iter()
                    .any(|tag| tag == OTLP_AGGREGATION_TEMPORALITY_LABEL)
                    && calls_rate_or_increase(prom_expr)
                {
                    return Ok(None);
                }
                // The selector's own table scan, built by the same method the existing path uses:
                // the physical planner turns it into the region scan the node reads, and building a
                // scan here instead would let the two paths disagree about the columns they read.
                let table_ref = self.table_ref()?;
                let mut input = self.create_table_scan_plan(table_ref).await?;
                let logical_scan = metric_scan_and_table_id(&input);
                let has_verified_metric_rewrite = logical_scan.is_some();
                let scan = match logical_scan {
                    Some((mut scan, predicate)) => {
                        // Preserve logical field order and the table restriction on the direct
                        // physical scan.
                        let source_schema = scan.source.schema();
                        let Some(mut ordered_projection) = fields
                            .iter()
                            .map(|field| source_schema.index_of(field).ok())
                            .collect::<Option<Vec<_>>>()
                        else {
                            return Ok(None);
                        };
                        let original_projection = scan.projection.as_ref();
                        let selected_indices = original_projection
                            .cloned()
                            .unwrap_or_else(|| (0..source_schema.fields().len()).collect());
                        for index in selected_indices {
                            let is_field = fields
                                .iter()
                                .any(|field| field == source_schema.field(index).name());
                            if !is_field && !ordered_projection.contains(&index) {
                                ordered_projection.push(index);
                            }
                        }
                        scan.filters.push(predicate);
                        scan = TableScanBuilder::from(scan)
                            .with_projection(Some(ordered_projection))
                            .build()
                            .context(DataFusionPlanningSnafu)?;
                        input = LogicalPlan::TableScan(scan.clone());
                        scan
                    }
                    None => match &input {
                        LogicalPlan::TableScan(scan) => scan.clone(),
                        _ => return Ok(None),
                    },
                };
                let table = self.table_from_source(&scan.source)?;
                let table_meta = &table.table_info().meta;
                // An engine without a raw-series scanner has no frame to give this node, and by the
                // time the physical planner asks, the logical plan is already this node: the refusal
                // there is a planning error, not a fallback. So the engine is checked here, where the
                // existing plan is still the answer. A file-engine table is the concrete case: its
                // scanner is a `SinglePartitionScanner`, which leaves `raw_series_schema` at the
                // default `None`. A metric-engine table is served only through the logical rewrite
                // recognised above, which is what makes its series the table's own.
                if !supports_native_raw_series(&table_meta.engine)
                    || (table_meta.engine == METRIC_ENGINE_NAME && !has_verified_metric_rewrite)
                {
                    return Ok(None);
                }
                // The region's raw-series preflight refuses these, and the physical layer turns that
                // refusal into an error, so decline this route here instead.
                let time_index_type = table_meta
                    .schema
                    .timestamp_column()
                    .map(|column| &column.data_type);
                if time_index_type
                    != Some(&datatypes::prelude::ConcreteDataType::timestamp_millisecond_datatype())
                    || table_meta
                        .options
                        .extra_options
                        .get(store_api::mito_engine_options::APPEND_MODE_KEY)
                        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
                {
                    return Ok(None);
                }
                // FusedSeries evaluates each region independently, so partition keys must identify a
                // series.
                if !table
                    .table_info()
                    .meta
                    .partition_column_names()
                    .all(|column| tags.iter().any(|tag| tag == column))
                {
                    return Ok(None);
                }
                // The region's columnar series-index samples, one of the readers this scan may be
                // served by, store `Float64` fields only, and the logical plan cannot tell which
                // reader arrives. So every field has to be `Float64`: an integer, a `Float32`, a
                // decimal, a string or a native histogram field keeps the existing plan, which the
                // physical layer still serves.
                if !fields.iter().all(|field| {
                    Self::field_column_type(input.schema(), field) == Some(&ArrowDataType::Float64)
                }) {
                    return Ok(None);
                }
                // Two occurrences are admitted only when the scan's raw series keys name one
                // logical series each, so that either occurrence reads the same series in whichever
                // partition holds it. A single occurrence needs no such proof: it reads its own scan
                // once, and the checks above are the ones it has always had.
                if selectors.len() == 2
                    && !identity_keys_are_injective(&matchers, table_meta, &tags)
                {
                    return Ok(None);
                }
                let series_schema = Arc::new(SeriesSchema {
                    fields: fields
                        .iter()
                        .map(|name| FieldSpec {
                            name: Arc::from(name.as_str()),
                        })
                        .collect::<Vec<_>>()
                        .into(),
                    eval_grid: None,
                });
                // The row filter is the selector's own: the time range is added after compiling,
                // because only the compiler knows whether this occurrence reads a window or one
                // instant.
                let mut filters = Self::matchers_to_expr(matchers, input.schema())?;
                if let LogicalPlan::TableScan(table_scan) = &mut input {
                    table_scan.filters.append(&mut filters);
                } else {
                    return Ok(None);
                }
                prepared.push((input, series_schema, Some(Arc::from(metric)), time, tags));
            }
            Ok(Some(prepared))
        }
        .await;
        self.ctx = original_context;
        let Some(mut prepared) = preparation? else {
            return Ok(None);
        };
        // The time index, the tag list and the metric name are the node's, not one scan's: the
        // output schema and the binding name them once, and every occurrence reports its series
        // under them. Two occurrences that disagree are two different reportings, which one node
        // cannot be built for. Both occurrences of an admitted pair share the layout, so the first
        // one's schema also speaks for the node below.
        let Some((_, series_schema, metric, time, tags)) = prepared.first() else {
            return Ok(None);
        };
        let (series_schema, metric, time, tags) = (
            Arc::clone(series_schema),
            metric.clone(),
            time.clone(),
            tags.clone(),
        );
        if prepared
            .iter()
            .any(|(_, _, other_metric, other_time, other_tags)| {
                *other_metric != metric || *other_time != time || *other_tags != tags
            })
        {
            return Ok(None);
        }
        // The compiler binds one input per selector in the same pre-order `collect_selectors`
        // walks, so position i of both lists is the same occurrence. This is the guard that the
        // two walks agree: a mismatch is a compiler defect, not a query this route can plan.
        let identities: Vec<(Option<String>, Matchers, i64, Option<i64>)> = selectors
            .iter()
            .map(|selector| {
                let offset_ms = selector
                    .offset
                    .as_ref()
                    .map(|o| match o {
                        promql_parser::parser::Offset::Pos(d) => d.as_millis() as i64,
                        promql_parser::parser::Offset::Neg(d) => -(d.as_millis() as i64),
                    })
                    .unwrap_or_default();
                Ok((
                    selector.name.clone(),
                    selector.matchers.clone(),
                    offset_ms,
                    // The compiler records the raw anchor separately from the offset; compare
                    // that same canonical identity rather than the derived anchor-minus-offset.
                    self.at_ref_time(&selector.at, &None)?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let bind_index = std::cell::Cell::new(0usize);
        let compiled = match compile_selectors(
            &stmt,
            &|req| {
                let i = bind_index.get();
                bind_index.set(i + 1);
                let (Some((_, schema, ..)), Some(identity)) = (prepared.get(i), identities.get(i))
                else {
                    return Err(DataFusionError::Internal(
                        "a selector was bound that the planner did not prepare".into(),
                    ));
                };
                if *identity
                    != (
                        req.metric.clone(),
                        req.matchers.clone(),
                        req.offset_ms,
                        req.at_ms,
                    )
                {
                    return Err(DataFusionError::Internal(
                        "the compiler and the planner walk the selectors differently".into(),
                    ));
                }
                Ok(Arc::clone(schema))
            },
            NATIVE_ORDERING,
        ) {
            Ok(v) => v,
            // The compiler refusing a shape is this route declining it.
            Err(DataFusionError::NotImplemented(_)) => return Ok(None),
            Err(e) => return Err(e).context(DataFusionPlanningSnafu),
        };
        if compiled.inputs.len() != prepared.len() {
            return Err(DataFusionError::Internal(
                "the compiled tree reads a different number of inputs than the planner prepared"
                    .into(),
            ))
            .context(DataFusionPlanningSnafu);
        }
        for (index, (compiled_input, (scan, _, _, _, _))) in
            compiled.inputs.iter().zip(prepared.iter_mut()).enumerate()
        {
            if compiled_input.source_index != index {
                return Ok(None);
            }
            // The range this selector reads is the compiler's own: a window and an instant differ
            // in which side of the lookback or range their scan starts on, and an anchor can move
            // the whole range outside the statement's. The bound belongs to the scan, so it is
            // pushed down before two occurrences are compared: two selectors over different windows
            // or offsets read different rows even when nothing else about them differs.
            let (start, end, inclusive) = compiled_input
                .requirement
                .raw_scan_bounds()
                .context(DataFusionPlanningSnafu)?;
            let filter = scan_time_filter(start, end, inclusive, &time);
            if let LogicalPlan::TableScan(table_scan) = &mut *scan {
                table_scan.filters.push(filter);
            } else {
                return Err(DataFusionError::Internal(format!(
                    "the child at {index} is not the table scan this route prepared"
                )))
                .context(DataFusionPlanningSnafu);
            }
        }
        // Two occurrences are admitted only when both read one scan plan: the node then runs one
        // execution per partition of that one child, and both sides of the match see the same series
        // in the same partition. Two actual scans — a different table, different matchers, different
        // bounds — stay with the existing planner, which is where a match over more than one source
        // is answered rather than in a region that holds part of it.
        if selectors.len() == 2 && !same_source(&prepared[0], &prepared[1]) {
            return Ok(None);
        }
        let mut children = prepared
            .into_iter()
            .map(|(scan, _, _, _, _)| scan)
            .collect::<Vec<_>>();
        // A tree run once per scan partition has to read one series at a time: an aggregate folds a
        // label set's series together and a vector operation (`sort`, `absent`, `topk`,
        // `histogram_quantile`, ...) reads the whole vector at a grid point, so one region's share
        // of the series cannot answer either. Declining here is the only place that still works:
        // once the `FusedSeries` node exists the logical plan is fixed, and the categorizer merely
        // declining to push it down would leave a node the physical layer must build anyway. What
        // does read one series at a time is one occurrence's samples - a selection, a window, a map
        // over either, or a constant broadcast - and, for a statement that names one selector
        // twice, a one-to-one match whose key holds the whole identity over one scan, which the
        // collapse below reads as that scan.
        let root = compiled.tree.root();
        if reads_across_series(root)
            && !(pointwise_operand(root)
                || (selectors.len() == 2 && two_occurrences_pair_one_source(root, &tags)))
        {
            return Ok(None);
        }
        // Two occurrences of one selector read the same series only when the tree between them is
        // point-wise, both scans are the same scan, and the match key holds the whole identity,
        // which the checks above have just admitted. Both occurrences are then one child, so the
        // tree is rebuilt with both inputs bound to it. A tree this route admits but cannot rebind
        // is a compiler defect it declines (`NotImplemented`) rather than rewrites, and the existing
        // plan answers the query; an error of any other kind is a defect to report.
        let tree = if selectors.len() == 2 {
            match collapsed_pair_tree(&compiled.tree) {
                Ok(tree) => {
                    // Both occurrences read the one plan, so the two prepared children are that
                    // same plan and only one of them is kept.
                    children.truncate(1);
                    tree
                }
                Err(DataFusionError::NotImplemented(_)) => return Ok(None),
                Err(e) => return Err(e).context(DataFusionPlanningSnafu),
            }
        } else {
            compiled.tree
        };
        let root_fields = tree
            .root()
            .output
            .samples()
            .ok_or_else(|| DataFusionError::Internal("native tree root has no samples".into()))
            .context(DataFusionPlanningSnafu)?
            .fields
            .iter()
            .map(|f| f.name.to_string())
            .collect::<Vec<_>>();
        if root_fields.iter().any(|field| {
            !series_schema
                .fields
                .iter()
                .any(|f| f.name.as_ref() == field)
        }) {
            return Ok(None);
        }
        let output_schema = fused_output_schema(&root_fields, &tags, &time)?;
        let node = FusedSeries::try_new(
            Arc::from(prom_expr.to_string()),
            tree,
            FusedSeriesBinding {
                grid,
                time_index: time,
                tags,
                metric_name: metric,
            },
            children,
            output_schema,
        )
        .context(DataFusionPlanningSnafu)?;
        Ok(Some(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        })))
    }

    /// The statement being planned, rebuilt from the evaluation numbers the planner holds.
    ///
    /// The compiler reads the statement's grid, lookback and `@` anchors rather than the planner's
    /// mutable state. The route is only offered at the statement root, where `ctx.start` and
    /// `ctx.end` are the statement's own bounds: they are rewritten while an anchored window or a
    /// subquery is folded, so a nested expression would need the bounds it was actually entered
    /// with, which is why no nested expression is offered here.
    ///
    /// `None` means the bounds are not representable as a [`SystemTime`], which is not a grid a
    /// tree can be evaluated on either.
    fn native_statement(&self, prom_expr: &PromExpr) -> Option<EvalStmt> {
        let start = UNIX_EPOCH.checked_add(Duration::from_millis(self.ctx.stmt_start as u64))?;
        let end = UNIX_EPOCH.checked_add(Duration::from_millis(self.ctx.stmt_end as u64))?;
        Some(EvalStmt {
            expr: prom_expr.clone(),
            start,
            end,
            interval: Duration::from_millis(self.ctx.interval as u64),
            lookback_delta: Duration::from_millis(self.ctx.lookback_delta as u64),
        })
    }
}

/// The columns a [`FusedSeries`] node produces.
///
/// The shape is the one the existing unary pipeline consumes — the time index, then the series
/// fields, then the tags — and each column is declared the way the node's own conversion reads it
/// back:
/// the time column is a non-nullable millisecond timestamp with no timezone, because every
/// evaluated chunk is addressed on the statement's grid and carries one; a field is a nullable
/// `Float64`, because a grid point inside the statement's interval can have no sample; and a tag is
/// a nullable `Utf8`, because a range function drops the metric name and a series can be missing a
/// label entirely.
fn fused_output_schema(
    fields: &[String],
    tags: &[String],
    time_index: &str,
) -> Result<DFSchemaRef> {
    let mut columns = Vec::with_capacity(fields.len() + tags.len() + 1);
    columns.push((
        None,
        Arc::new(Field::new(
            time_index,
            ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
            false,
        )),
    ));
    for name in fields {
        columns.push((
            None,
            Arc::new(Field::new(name, ArrowDataType::Float64, true)),
        ));
    }
    for name in tags {
        columns.push((None, Arc::new(Field::new(name, ArrowDataType::Utf8, true))));
    }

    Ok(Arc::new(
        DFSchema::new_with_metadata(columns, Default::default())
            .context(DataFusionPlanningSnafu)?,
    ))
}

/// The time-range filter a selector's scan reads, from the bounds its compiled requirement names.
///
/// The selector bounds are already checked against the millisecond timestamp schema during
/// admission; preserve the requirement's inclusive or exclusive lower edge exactly.
fn scan_time_filter(start: i64, end: i64, inclusive: bool, time_column: &str) -> DfExpr {
    let column = DfExpr::Column(datafusion::common::Column::from_name(time_column));
    let lower = DfExpr::Literal(
        datafusion::scalar::ScalarValue::TimestampMillisecond(Some(start), None),
        None,
    );
    let upper = DfExpr::Literal(
        datafusion::scalar::ScalarValue::TimestampMillisecond(Some(end), None),
        None,
    );
    let lower = if inclusive {
        column.clone().gt_eq(lower)
    } else {
        column.clone().gt(lower)
    };
    lower.and(column.lt_eq(upper))
}

/// Whether a tree is two occurrences of one selector matched point-wise over the whole identity.
///
/// The claim is about one series: it is whole in the partition it was scanned in, so a tree whose
/// nodes each read one series separately can run one execution per partition. What breaks that is a
/// node that reads a *series* rather than its samples point by point — an aggregate folds a group,
/// and two vectors matched against each other pair one series with another — so those keep the
/// existing plan. The remaining shapes are the ones a scan's own samples support: an instant
/// selection or a window directly over the raw input, a point-wise map over either, and a binary
/// operation with a constant on one side, which is a broadcast over the same positions.
///
/// A one-to-one match is admitted without rejecting its fill values: the compiler binds
/// `fill_left`/`fill_right` for a one-to-one match too, and what the admission rests on is the key
/// and the operands, not the absence of a fill — see [`matching_keys_on_identity`].
///
/// The pair's scans are the caller's claim: this function reads the tree and the tags, and a caller
/// that admits a pair on two actual scans would still be admitting a match over two sources. Both
/// the planner that collapses a pair onto one scan and the categorizer that places the node
/// region-side ask this, so the two cannot disagree about which pairs read one series: a pair
/// admitted here but refused there becomes a node the physical layer cannot build.
///
/// The raw keys themselves are proven to name one logical series each by the caller's preparation,
/// which checks the table's primary key against the tags this binding reports
/// (`identity_keys_are_injective`). That is a planning-time check over the scan the planner
/// prepared, not a property this function re-derives: a tree and binding built by anything other
/// than this planner carries no such proof, so a manual `FusedSeries` node is only as safe as the
/// binding its builder passed.
pub(crate) fn two_occurrences_pair_one_source(root: &NativeNode, tags: &[String]) -> bool {
    let NativeExpr::Binary {
        left,
        right,
        matching,
    } = &root.expr
    else {
        return false;
    };
    // A root with a constant on one side is a broadcast of that constant, not two occurrences
    // matched, and `group_left`/`group_right` pair a group against one series.
    matching.cardinality == Cardinality::OneToOne
        && pointwise_operand(left)
        && pointwise_operand(right)
        // A key that does not hold the whole identity pairs series Prometheus keeps apart, and a
        // partition that holds one of them cannot tell that the other is missing.
        && matching_keys_on_identity(&matching.labels, tags)
}

/// Whether `node` reads samples point-wise, so that one partitioned execution is one answer.
///
/// A node's output type is not checked here: [`NativeNode::validate`] already refuses a selection,
/// a map or a binary operation whose operands are not evaluated vectors, so every branch below is
/// one that produces values rather than raw samples.
pub(crate) fn pointwise_operand(node: &NativeNode) -> bool {
    match &node.expr {
        // A selection reads the raw samples below it; anything else below it is a value computed
        // from a whole series, not a scan.
        NativeExpr::Instant { input, .. } | NativeExpr::Window { input, .. } => {
            matches!(input.expr, NativeExpr::Input(_))
        }
        NativeExpr::Map { input, .. } => pointwise_operand(input),
        // A constant on one side is broadcast over the vector side's positions. Two vectors are a
        // match between two series sets, which is the shape this route cannot split.
        NativeExpr::Binary {
            left,
            right,
            matching,
        } => {
            matching.cardinality == Cardinality::OneToOne
                && match (&left.expr, &right.expr) {
                    (NativeExpr::Scalar(_), NativeExpr::Scalar(_)) => false,
                    (NativeExpr::Scalar(_), _) => pointwise_operand(right),
                    (_, NativeExpr::Scalar(_)) => pointwise_operand(left),
                    _ => false,
                }
        }
        // An aggregate folds a group, a vector operation reads the whole vector, and a bare input or
        // scalar is not a per-series evaluation.
        NativeExpr::Input(_)
        | NativeExpr::Scalar(_)
        | NativeExpr::Aggregate { .. }
        | NativeExpr::Vector { .. } => false,
    }
}

/// Whether `labels` matches on the whole series identity `tags` names.
///
/// `on(...)` keeps the identity when every tag is listed, because two series that differ in any
/// tag then differ in the key. Anything else — a tag left out of `on`, or a tag named in
/// `ignoring` — pairs two series that Prometheus reads as two, and each partition would then
/// answer for the half it happens to hold.
pub(crate) fn matching_keys_on_identity(labels: &MatchingLabels, tags: &[String]) -> bool {
    match labels {
        MatchingLabels::On(names) => tags
            .iter()
            .all(|tag| names.iter().any(|name| name.as_ref() == tag.as_str())),
        MatchingLabels::Ignoring(names) => tags
            .iter()
            .all(|tag| !names.iter().any(|name| name.as_ref() == tag.as_str())),
    }
}

/// Whether the raw series keys `meta` declares name one logical series each.
///
/// The label set this route reports is the primary key it reads off a series, so the key is only a
/// faithful identity when it is the whole label set and its values are distinct strings: a hidden
/// key column (a metric engine's internals), a key column with a metadata name a window or a map
/// drops, and a key column that can read both NULL and the empty string — Prometheus's one absent
/// label, but two raw keys — all keep the existing plan. A matcher that rejects the empty value is
/// what makes a nullable key column safe: it leaves no row whose key could read either way.
fn identity_keys_are_injective(matchers: &Matchers, meta: &TableMeta, tags: &[String]) -> bool {
    // A table with no key column has one label set for every series, and a metric engine keeps its
    // identity in columns the planner hides from `tags`.
    if tags.is_empty() || meta.engine == METRIC_ENGINE_NAME {
        return false;
    }
    // The planner hides exactly `__table_id` and `__tsid`; a key that differs from the columns this
    // route reports means part of the identity is not in the label set.
    if !meta.row_key_column_names().eq(tags.iter()) {
        return false;
    }
    tags.iter().all(|tag| {
        if METADATA_KEY_TAGS.contains(&tag.as_str()) {
            return false;
        }
        let Some(column) = meta.schema.column_schema_by_name(tag) else {
            return false;
        };
        // The label set is reported as strings, which is what a string key column already is.
        if column.data_type != datatypes::prelude::ConcreteDataType::string_datatype() {
            return false;
        }
        // A non-nullable column cannot read NULL, so an empty value and an absent one are two
        // different keys.
        !column.is_nullable()
            || matchers
                .matchers
                .iter()
                .any(|matcher| matcher.name == tag.as_str() && !matcher.is_match(""))
    })
}

/// Whether two prepared occurrences read the same scan, and so produce the same series.
///
/// Everything below the node is compared, not only the table name: two selectors over one table
/// with different matchers or different bounds read different rows, and a partition that holds one
/// of them is not the partition that holds the other. DataFusion's own scan comparison covers the
/// table name, the projection, the schema, the filters and the fetch; the source is compared by
/// identity, because it is what resolves those rows in the first place. The reporting columns — the
/// metric name, the time index and the tags — are not compared here: they are checked once for
/// every prepared occurrence, before this runs.
fn same_source(left: &PreparedInput, right: &PreparedInput) -> bool {
    let (left_input, left_schema, ..) = left;
    let (right_input, right_schema, ..) = right;
    let (LogicalPlan::TableScan(left_scan), LogicalPlan::TableScan(right_scan)) =
        (left_input, right_input)
    else {
        return false;
    };
    // Both schemas are raw series schemas, so the fields are what the tree's input was compiled
    // against, in the order it reads them.
    let same_fields = left_schema
        .fields
        .iter()
        .map(|field| &field.name)
        .eq(right_schema.fields.iter().map(|field| &field.name));
    same_fields && left_scan == right_scan && Arc::ptr_eq(&left_scan.source, &right_scan.source)
}

/// Rebuilds an admitted pair's tree so both of its occurrences read the one child prepared for them.
///
/// The compiler numbers occurrences in binding order, so an admitted pair's two input nodes are
/// numbered `0` and `1`; the planner prepared one plan for them, which is child `0`. Giving every
/// input node that index is what makes the executor hand each chunk of that child to both of them,
/// and what makes the node report one child for the whole match.
///
/// The tree is rebuilt rather than patched in place because a node's inputs are shared: the rebuilt
/// tree is validated again by [`NativeTree::try_new`], so a rebind can never produce a tree the
/// runtime would refuse. A tree whose inputs are not exactly those two occurrences is declined.
fn collapsed_pair_tree(tree: &NativeTree) -> datafusion::error::Result<NativeTree> {
    let mut root = tree.root().clone();
    let mut occurrences = 0;
    let numbered = rebind_pair_inputs(&mut root, &mut occurrences);
    if !numbered || occurrences != 2 {
        return Err(DataFusionError::NotImplemented(format!(
            "a two-occurrence tree whose {occurrences} input(s) are not the two occurrences \
             numbered from zero, which this route cannot bind to one child"
        )));
    }
    NativeTree::try_new(root)
}

/// Rebinds every input below `node` to child `0`, counting them and checking their numbering.
///
/// The walk visits inputs in the order the compiler bound them — an operand before the next one, the
/// left operand before the right — so occurrence `i` of the walk is the input the compiler numbered
/// `i`. `false` means the tree is not the pair this route admitted: a node carried a number the walk
/// did not expect, or a shape the admission does not cover. An `Aggregate` or a `Vector` below the
/// root is one of those shapes — neither is point-wise, so the pair was never admitted — and the
/// walk refuses it rather than rebinding a tree this route does not run.
fn rebind_pair_inputs(node: &mut NativeNode, next: &mut usize) -> bool {
    match &mut node.expr {
        NativeExpr::Input(bound) => {
            let numbered = bound.source_index == *next;
            bound.source_index = 0;
            *next += 1;
            numbered
        }
        NativeExpr::Scalar(_) => true,
        NativeExpr::Instant { input, .. }
        | NativeExpr::Window { input, .. }
        | NativeExpr::Map { input, .. } => rebind_pair_inputs(input, next),
        NativeExpr::Binary { left, right, .. } => {
            let left_numbered = rebind_pair_inputs(left, next);
            let right_numbered = rebind_pair_inputs(right, next);
            left_numbered && right_numbered
        }
        NativeExpr::Aggregate { .. } | NativeExpr::Vector { .. } => false,
    }
}

/// The single-selector route is limited to a selector or supported unary calls over one selector.
fn admitted_selector(expr: &PromExpr) -> Option<&VectorSelector> {
    match expr {
        PromExpr::VectorSelector(selector) => Some(selector),
        PromExpr::MatrixSelector(MatrixSelector { vs, .. }) => Some(vs),
        PromExpr::Paren(ParenExpr { expr }) => admitted_selector(expr),
        PromExpr::Call(call)
            if matches!(
                call.func.name,
                "rate" | "increase" | "sum_over_time" | "abs"
            ) && call.args.args.len() == 1 =>
        {
            admitted_selector(&call.args.args[0])
        }
        _ => None,
    }
}

/// Collect selector occurrences in compiler binding order; reject unsupported expression shapes.
fn collect_selectors<'a>(expr: &'a PromExpr, out: &mut Vec<&'a VectorSelector>) -> bool {
    match expr {
        PromExpr::VectorSelector(selector) => out.push(selector),
        PromExpr::MatrixSelector(MatrixSelector { vs, .. }) => out.push(vs),
        PromExpr::Paren(ParenExpr { expr }) => return collect_selectors(expr, out),
        PromExpr::Call(call)
            if matches!(
                call.func.name,
                "rate" | "increase" | "sum_over_time" | "abs"
            ) && call.args.args.len() == 1 =>
        {
            return collect_selectors(&call.args.args[0], out);
        }
        PromExpr::Binary(binary) => {
            return collect_selectors(&binary.lhs, out) && collect_selectors(&binary.rhs, out);
        }
        // Literals are permitted only as compiler-recognized broadcasts in a bounded binary tree.
        PromExpr::NumberLiteral(_) => return true,
        _ => return false,
    }
    true
}

/// Whether `engine` can serve raw series frames to the fused-series node.
///
/// A whitelist avoids probing a region from logical planning; declining only costs the native route.
fn supports_native_raw_series(engine: &str) -> bool {
    matches!(engine, MITO_ENGINE | MITO2_ENGINE | METRIC_ENGINE)
}

/// Recognizes only the metric logical-table scan emitted by `create_table_scan_plan`.
fn metric_scan_and_table_id(
    plan: &LogicalPlan,
) -> Option<(datafusion::logical_expr::TableScan, Expr)> {
    let plan = match plan {
        LogicalPlan::Projection(projection) => projection.input.as_ref(),
        plan => plan,
    };
    let plan = match plan {
        LogicalPlan::SubqueryAlias(alias) => alias.input.as_ref(),
        plan => plan,
    };
    let LogicalPlan::Filter(filter) = plan else {
        return None;
    };
    let Expr::BinaryExpr(binary) = &filter.predicate else {
        return None;
    };
    if binary.op != datafusion::logical_expr::Operator::Eq
        || !matches!(binary.left.as_ref(), Expr::Column(column) if column.name == DATA_SCHEMA_TABLE_ID_COLUMN_NAME)
    {
        return None;
    }
    if !matches!(
        binary.right.as_ref(),
        Expr::Literal(datafusion::scalar::ScalarValue::UInt32(Some(_)), _)
    ) {
        return None;
    }
    let LogicalPlan::TableScan(scan) = filter.input.as_ref() else {
        return None;
    };
    Some((scan.clone(), filter.predicate.clone()))
}

fn calls_rate_or_increase(expr: &PromExpr) -> bool {
    match expr {
        PromExpr::Call(call) => {
            matches!(call.func.name, "rate" | "increase")
                || call.args.args.iter().any(|arg| calls_rate_or_increase(arg))
        }
        PromExpr::Paren(ParenExpr { expr }) => calls_rate_or_increase(expr),
        PromExpr::Binary(binary) => {
            calls_rate_or_increase(&binary.lhs) || calls_rate_or_increase(&binary.rhs)
        }
        PromExpr::Unary(unary) => calls_rate_or_increase(&unary.expr),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use session::context::QueryContextBuilder;

    use std::time::{Duration, UNIX_EPOCH};

    use datatypes::series::SeriesOrdering;
    use datatypes::series::samples::{FieldSpec, SeriesSchema};
    use promql::native::plan::{NativeExpr, NativeNode, NativeTree};
    use promql::vector_match::MatchingLabels;

    use super::{
        NATIVE_PROMQL_EXTENSION_KEY, admitted_selector, collapsed_pair_tree,
        matching_keys_on_identity, native_promql_requested, pointwise_operand, scan_time_filter,
        two_occurrences_pair_one_source,
    };

    fn compiled_test_tree(query: &str) -> NativeTree {
        let statement = promql_parser::parser::EvalStmt {
            expr: promql_parser::parser::parse(query).unwrap(),
            start: UNIX_EPOCH,
            end: UNIX_EPOCH + Duration::from_secs(10),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(5),
        };
        let schema = Arc::new(SeriesSchema {
            fields: Arc::from([FieldSpec { name: "v0".into() }]),
            eval_grid: None,
        });
        crate::promql::native_compile::compile_selectors(
            &statement,
            &|_| Ok(Arc::clone(&schema)),
            SeriesOrdering::KeyThenTime,
        )
        .unwrap()
        .tree
    }

    fn input_sources(root: &NativeNode) -> Vec<usize> {
        fn collect(node: &NativeNode, sources: &mut Vec<usize>) {
            match &node.expr {
                NativeExpr::Input(input) => sources.push(input.source_index),
                NativeExpr::Instant { input, .. }
                | NativeExpr::Window { input, .. }
                | NativeExpr::Map { input, .. } => collect(input, sources),
                NativeExpr::Binary { left, right, .. } => {
                    collect(left, sources);
                    collect(right, sources);
                }
                NativeExpr::Scalar(_)
                | NativeExpr::Aggregate { .. }
                | NativeExpr::Vector { .. } => {}
            }
        }

        let mut sources = Vec::new();
        collect(root, &mut sources);
        sources
    }

    #[test]
    fn pair_input_collapse_rebinds_only_two_numbered_occurrences() {
        let tree = compiled_test_tree("x + x");
        assert_eq!(input_sources(tree.root()), vec![0, 1]);
        let collapsed = collapsed_pair_tree(&tree).unwrap();
        assert_eq!(input_sources(collapsed.root()), vec![0, 0]);
        assert_eq!(
            tree.to_string().replace("source=1", "source=0"),
            collapsed.to_string()
        );
        assert!(collapsed_pair_tree(&compiled_test_tree("x")).is_err());
        assert!(collapsed_pair_tree(&compiled_test_tree("x + x + x")).is_err());
    }

    #[test]
    fn binary_admission_requires_a_pointwise_whole_identity_match() {
        let tags = vec!["host".to_string(), "rack".to_string()];
        let pair = compiled_test_tree("x + on(host, rack) x");
        assert!(two_occurrences_pair_one_source(pair.root(), &tags));
        assert!(!pointwise_operand(pair.root()));
        let partial = compiled_test_tree("x + on(host) x");
        assert!(!two_occurrences_pair_one_source(partial.root(), &tags));
        let nested_pair = compiled_test_tree("x - (x * 2)");
        assert!(!pointwise_operand(nested_pair.root()));
        assert!(two_occurrences_pair_one_source(nested_pair.root(), &tags));
        let collapsed = collapsed_pair_tree(&nested_pair).unwrap();
        assert_eq!(input_sources(collapsed.root()), vec![0, 0]);
        assert_eq!(
            nested_pair.to_string().replace("source=1", "source=0"),
            collapsed.to_string(),
            "collapsing inputs must preserve the noncommutative operand order"
        );
        assert!(matching_keys_on_identity(
            &MatchingLabels::Ignoring(vec![]),
            &tags
        ));
        assert!(!matching_keys_on_identity(
            &MatchingLabels::Ignoring(vec!["host".into()]),
            &tags
        ));
    }

    #[test]
    fn single_selector_admission_keeps_scalar_binaries_on_fallback() {
        for query in ["x * 2", "2 - x", "(x * 2) + 3"] {
            let expression = promql_parser::parser::parse(query).unwrap();
            assert!(admitted_selector(&expression).is_none(), "{query}");
        }
        for query in ["x", "rate(x[5s])", "sum_over_time(x[5s])", "abs(x)"] {
            let expression = promql_parser::parser::parse(query).unwrap();
            assert!(admitted_selector(&expression).is_some(), "{query}");
        }
    }

    #[test]
    fn millisecond_scan_bounds_preserve_open_closed_and_maximum_edges() {
        let inclusive = scan_time_filter(0, 10, true, "ts").to_string();
        assert!(inclusive.contains(">="), "{inclusive}");
        assert!(inclusive.contains("<="), "{inclusive}");

        let exclusive = scan_time_filter(0, 10, false, "ts").to_string();
        assert!(exclusive.contains("> "), "{exclusive}");
        assert!(!exclusive.contains(">="), "{exclusive}");
        assert!(exclusive.contains("<="), "{exclusive}");

        let maximum = scan_time_filter(i64::MAX, i64::MAX, false, "ts").to_string();
        assert!(maximum.contains("> "), "{maximum}");
        assert!(!maximum.contains(">="), "{maximum}");
        assert!(maximum.contains("<="), "{maximum}");
    }

    #[test]
    fn native_promql_requested_is_false_when_extension_is_absent() {
        let query_ctx = Arc::new(QueryContextBuilder::default().build());

        assert!(!native_promql_requested(&query_ctx));
    }

    #[test]
    fn native_promql_requested_accepts_true_case_insensitively() {
        for value in ["true", "TRUE", "TrUe"] {
            let query_ctx = Arc::new(
                QueryContextBuilder::default()
                    .set_extension(NATIVE_PROMQL_EXTENSION_KEY.to_string(), value.to_string())
                    .build(),
            );

            assert!(native_promql_requested(&query_ctx), "value: {value}");
        }
    }

    #[test]
    fn native_promql_requested_rejects_false_and_unrecognized_values() {
        for value in ["false", "FALSE", "yes", "1", " true ", ""] {
            let query_ctx = Arc::new(
                QueryContextBuilder::default()
                    .set_extension(NATIVE_PROMQL_EXTENSION_KEY.to_string(), value.to_string())
                    .build(),
            );

            assert!(!native_promql_requested(&query_ctx), "value: {value:?}");
        }
    }
}
