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
//! [`PromPlanner::try_plan_native_fused`] offers a whole statement expression to the native
//! compiler and, when the compiler accepts it, replaces everything the existing path would plan
//! below the root with one [`FusedSeries`] node over the selector's own table scan. A tree that
//! folds across series - an aggregate - is split instead: its per-series prefix runs in a
//! `FusedSeries` producer and an [`AggregateFinalize`] merges the state every partition exports,
//! because a region holds only part of a group. Declining is the normal answer: every shape the
//! compiler does not accept, and every query this route cannot bind to a scan faithfully, keeps the
//! plan the existing planner builds.
//!
//! What the node needs from here is what the existing planner already resolves: the selector's own
//! table scan with its matchers and compiled time bounds pushed down, the statement's evaluation
//! grid, and the columns a series is reported under — the time index, the tags and the metric name.
//! The tree's inputs are numbered, and each number is the position of the child whose chunks that
//! input reads; a statement whose two occurrences were proven to read one scan is planned with that
//! one child and both inputs rebound to it. The output schema is built to the shape the existing
//! unary pipeline produces — the time index, then the series fields, then the tags — so a node
//! planned here can be consumed by the same code that consumes a `SeriesNormalize` output.

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use common_catalog::consts::{METRIC_ENGINE, MITO_ENGINE, MITO2_ENGINE};
use common_query::prelude::OTLP_AGGREGATION_TEMPORALITY_LABEL;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, TimeUnit as ArrowTimeUnit};
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder, TableScanBuilder};
use datafusion::prelude::Expr as DfExpr;
use datafusion_expr::Expr;
use datatypes::series::SeriesOrdering;
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use promql::extension_plan::{AggregateFinalize, FusedSeries, FusedSeriesBinding};
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

use crate::parser::{EXPLAIN_NODE_NAME, EXPLAIN_VERBOSE_NODE_NAME};
use crate::promql::error::{DataFusionPlanningSnafu, Result};
use crate::promql::native_compile::{compile_selectors, statement_grid};
use crate::promql::planner::PromPlanner;
use crate::query_engine::QueryEngineState;

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
    /// Plans `prom_expr` as one native node, or returns `None` to keep the existing plan.
    ///
    /// `query_engine_state` is only read to build the *existing* plan of an aggregate query, whose
    /// column names and grouped labels are what the rows of a split aggregate are named after. That
    /// plan is built for its schema, its names are copied, and it is dropped: it is never executed
    /// and never stored, so the batch that answers the query is the native route's own.
    ///
    /// The route is offered a whole expression because the compiler folds a range function and the
    /// math functions over it into one tree: offering a sub-expression instead would compile a tree
    /// that computes something the reader did not write. Everything else about the node comes from
    /// the planner's resolved selector context: the table name, the time index column, the tag
    /// columns and the field columns, which are exactly the raw series schema the tree's input
    /// has to be compiled against. Selectors with a `__field__` matcher keep legacy planning.
    ///
    /// `None` is not a failure. It says this query cannot take the route *unchanged*, and the
    /// caller plans it with the existing planner as if this method did not exist. The two ways that
    /// happens are a shape the compiler refuses (`DataFusionError::NotImplemented`, which names
    /// what it found) and a query this route cannot bind faithfully — a shape the aggregate split
    /// cannot carry or a binary operation that reads whole vectors, subqueries, alternative (`or`)
    /// matchers, an `__field__` matcher that narrows the field set, a missing table, a field that is
    /// not `Float64`, a `rate`/`increase` over a delta-temporality table, or an unusable grid.
    ///
    /// A statement with two occurrences of one selector is admitted only when both read one scan
    /// point-wise. That is the only shape this node can run over every partition of one child
    /// independently: both sides then see the same series in the same partition, so the series a
    /// partition holds is whole. Such a statement is planned over that one child, with both of its
    /// tree inputs bound to it; the node's binding names the columns a series is reported under —
    /// the time index, the tags and the metric name — once for all of them. Everything else — a
    /// different scan, different bounds, a match key that is not the whole series identity, a whole
    /// vector operation — keeps the existing plan rather than gathering every partition into one
    /// execution, because a match over two actual sources cannot be answered one region at a time.
    /// An aggregate is the one shape that reads across series and is still answered from a
    /// partitioned scan: it is split into an [`AggregateFinalize`] merge over a per-series producer,
    /// never run once per partition, because a partition holds only part of a group.
    pub(super) async fn try_plan_native_fused(
        &mut self,
        prom_expr: &PromExpr,
        query_engine_state: &QueryEngineState,
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

        let selectors = {
            let mut values = Vec::new();
            if !collect_selectors(prom_expr, &mut values) {
                return Ok(None);
            }
            values
        };
        if selectors.is_empty() {
            return Ok(None);
        }
        // One occurrence keeps the single-scan route below unchanged. Two are admitted only as one
        // source read point-wise; three or more would need a match over more than two series sets,
        // which the collapse at the end of this method cannot express.
        if selectors.len() > 2 {
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
            // The range this selector reads is the compiler's own: a window and an instant differ
            // in which side of the lookback or range their scan starts on, and an anchor can move
            // the whole range outside the statement's. The bound belongs to the scan, so it is
            // pushed down before two occurrences are compared: two selectors over different windows
            // or offsets read different rows even when nothing else about them differs.
            let (start, end, inclusive) = compiled_input
                .requirement
                .raw_scan_bounds()
                .context(DataFusionPlanningSnafu)?;
            let filter = scan_time_filter(start, end, inclusive, scan.schema(), &time)?;
            if let (LogicalPlan::TableScan(table_scan), Some(filter)) = (&mut *scan, filter) {
                table_scan.filters.push(filter);
            } else if !matches!(scan, LogicalPlan::TableScan(_)) {
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
        if selectors.len() == 2 && !(prepared.len() == 2 && same_source(&prepared[0], &prepared[1]))
        {
            return Ok(None);
        }
        let mut children = prepared
            .into_iter()
            .map(|(scan, _, _, _, _)| scan)
            .collect::<Vec<_>>();
        // A tree run once per scan partition has to read one series at a time: a vector operation
        // (`sort`, `absent`, `topk`, `histogram_quantile`, ...) reads the whole vector at a grid
        // point, so one region's share of the series cannot answer it. Declining here is the only
        // place that still works: once the `FusedSeries` node exists the logical plan is fixed, and
        // the categorizer merely declining to push it down would leave a node the physical layer
        // must build anyway. What does read one series at a time is one occurrence's samples - a
        // selection, a window, a map over either, or a constant broadcast - and, for a statement
        // that names one selector twice, a one-to-one match whose key holds the whole identity over
        // one scan, which the collapse below reads as that scan.
        //
        // An aggregate also reads across series, and it is the one such shape a partitioned scan can
        // still answer. It is never run once per partition - a partition folds its share of a group
        // and the fold of the shares is not the group's value - so it is split instead: the
        // per-series prefix runs in a producer per partition, and the merge of every partition's
        // state runs on the frontend.
        let per_partition = {
            let root = compiled.tree.root();
            !reads_across_series(root)
                || pointwise_operand(root)
                || (selectors.len() == 2 && two_occurrences_pair_one_source(root, &tags))
        };
        if !per_partition {
            // A split aggregate folds one source's series, so a fold over two actual sources stays
            // with the existing planner: no partition holds the whole of either source, and nothing
            // here gathers them. A shape the split cannot carry - a `quantile` whose state is the
            // group's samples, an aggregate grouped by a value field, a binary operation whose
            // sibling is not a constant scalar, a prefix that is not one series at a time - is
            // declined for the same reason.
            if selectors.len() != 1
                || !AggregateFinalize::can_split(&compiled.tree, &series_schema.fields, &time)
            {
                return Ok(None);
            }
            // The rows have to be named the way the existing plan names them, which is read off a
            // legacy plan built for its schema only: it is never stored and never run, and the
            // planning context is restored afterwards.
            let saved = self.ctx.clone();
            let planned = self
                .prom_root_expr_to_plan(prom_expr, query_engine_state)
                .await;
            let legacy_fields = self.ctx.field_columns.clone();
            let legacy_tags = self.ctx.tag_columns.clone();
            self.ctx = saved;
            // Only its schema mattered, so the plan is dropped here rather than stored.
            drop(planned?);
            let output_schema = fused_output_schema(&legacy_fields, &legacy_tags, &time)?;
            let Some(rawinput) = children.pop() else {
                return Err(DataFusionError::Internal(
                    "a single-source native tree prepared no scan".into(),
                ))
                .context(DataFusionPlanningSnafu);
            };
            let node = match AggregateFinalize::try_new(
                Arc::from(prom_expr.to_string()),
                compiled.tree,
                FusedSeriesBinding {
                    grid,
                    time_index: time,
                    tags,
                    metric_name: metric,
                },
                rawinput,
                output_schema,
            ) {
                Ok(node) => node,
                // The split refusing a shape is this route declining it, exactly as the compiler
                // refusing one is: the existing plan is the answer.
                Err(DataFusionError::NotImplemented(_)) => return Ok(None),
                Err(error) => return Err(error).context(DataFusionPlanningSnafu),
            };
            return Ok(Some(LogicalPlan::Extension(Extension {
                node: Arc::new(node),
            })));
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

    /// Offers an `EXPLAIN` statement's body to the native compiler, and wraps the plan it makes.
    ///
    /// `TQL EXPLAIN (…) <query>` is the statement whose root extension wraps the query, and the
    /// explanation a reader gets should be of the plan that query runs. Only the two plain
    /// explanations are handled: `ANALYZE`, which runs the plan and reports its metrics, and
    /// `EXPLAIN VERBOSE` are left to the existing path, so this adds nothing to what those report.
    pub(super) async fn try_plan_native_fused_explain(
        &mut self,
        prom_expr: &PromExpr,
        query_engine_state: &QueryEngineState,
    ) -> Result<Option<LogicalPlan>> {
        let PromExpr::Extension(extension) = prom_expr else {
            return Ok(None);
        };
        let explain = match extension.expr.name() {
            EXPLAIN_NODE_NAME => false,
            EXPLAIN_VERBOSE_NODE_NAME => true,
            _ => return Ok(None),
        };
        let children = extension.expr.children();
        let [body] = children else {
            return Ok(None);
        };
        let Some(plan) = self.try_plan_native_fused(body, query_engine_state).await? else {
            return Ok(None);
        };
        Ok(Some(
            LogicalPlanBuilder::from(plan)
                .explain(explain, false)
                .context(DataFusionPlanningSnafu)?
                .build()
                .context(DataFusionPlanningSnafu)?,
        ))
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

/// The time-range filter a selector's scan reads, from the bounds its compiled requirement names.
///
/// The lower edge is exclusive unless the requirement says otherwise, which is how a window and a
/// positive lookback read; an inclusive edge is what a zero lookback instant selection has. The
/// bound is expressed in the scan's own timestamp unit, and a bound that unit cannot represent is
/// left out rather than widened: the other side still prunes, and the evaluator applies the exact
/// rule either way.
fn scan_time_filter(
    start: i64,
    end: i64,
    inclusive: bool,
    schema: &DFSchema,
    time_column: &str,
) -> Result<Option<DfExpr>> {
    use datafusion::scalar::ScalarValue;
    let Some(index) = schema.index_of_column_by_name(None, time_column) else {
        return Ok(None);
    };
    let unit = match &schema.field(index).data_type() {
        arrow::datatypes::DataType::Timestamp(unit, _) => *unit,
        _ => return Ok(None),
    };
    let native = |milliseconds: i128| match unit {
        arrow::datatypes::TimeUnit::Second => milliseconds.div_euclid(1_000),
        arrow::datatypes::TimeUnit::Millisecond => milliseconds,
        arrow::datatypes::TimeUnit::Microsecond => milliseconds * 1_000,
        arrow::datatypes::TimeUnit::Nanosecond => milliseconds * 1_000_000,
    };
    let literal = |milliseconds: i128| -> Option<DfExpr> {
        let value = i64::try_from(native(milliseconds)).ok()?;
        Some(DfExpr::Literal(
            match unit {
                arrow::datatypes::TimeUnit::Second => {
                    ScalarValue::TimestampSecond(Some(value), None)
                }
                arrow::datatypes::TimeUnit::Millisecond => {
                    ScalarValue::TimestampMillisecond(Some(value), None)
                }
                arrow::datatypes::TimeUnit::Microsecond => {
                    ScalarValue::TimestampMicrosecond(Some(value), None)
                }
                arrow::datatypes::TimeUnit::Nanosecond => {
                    ScalarValue::TimestampNanosecond(Some(value), None)
                }
            },
            None,
        ))
    };
    let column = DfExpr::Column(datafusion::common::Column::from_name(time_column));
    let lower = literal(i128::from(start)).map(|literal| {
        if inclusive {
            column.clone().gt_eq(literal)
        } else if unit == arrow::datatypes::TimeUnit::Millisecond {
            // An exclusive edge on a millisecond axis is the next representable instant, which is
            // how the existing selector filter states it.
            literal_or_next(start).map_or_else(
                || column.clone().gt(literal.clone()),
                |next| column.clone().gt_eq(next),
            )
        } else {
            column.clone().gt(literal)
        }
    });
    let upper = literal(i128::from(end)).map(|literal| column.clone().lt_eq(literal));
    Ok(match (lower, upper) {
        (Some(lower), Some(upper)) => Some(lower.and(upper)),
        (Some(one), None) | (None, Some(one)) => Some(one),
        (None, None) => None,
    })
}

/// The literal one millisecond after `start`, for an exclusive lower edge on a millisecond axis.
fn literal_or_next(start: i64) -> Option<DfExpr> {
    use datafusion::scalar::ScalarValue;
    let next = start.checked_add(1)?;
    Some(DfExpr::Literal(
        ScalarValue::TimestampMillisecond(Some(next), None),
        None,
    ))
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
/// runtime would refuse. A tree whose inputs are not exactly those two occurrences is a compiler
/// defect this route declines with `NotImplemented` — the caller keeps the existing plan rather than
/// running a tree whose inputs it cannot name.
///
/// It is `pub(crate)` for the categorizer's own tests, which build the node the planner would have
/// built: the region-side placement has to accept exactly the shape this collapse produces, so a
/// fixture that bound the pair by hand could agree with the categorizer while disagreeing with the
/// planner.
pub(crate) fn collapsed_pair_tree(tree: &NativeTree) -> datafusion::error::Result<NativeTree> {
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

/// Every selector `expr` reads, in the order the compiler binds them.
///
/// The order is the compiler's own: it compiles a node's operands in the order they were written, so
/// this walk goes into the same children in the same order. The list is then matched position by
/// position against the compiled inputs, which is what makes a walk that visits a different set the
/// signal that the two disagree rather than a silent mismatch of selector to scan. `false` means the
/// expression names a shape this route cannot plan at all — a subquery, or a label rewrite whose
/// destination label no node here can declare.
fn collect_selectors<'a>(expr: &'a PromExpr, out: &mut Vec<&'a VectorSelector>) -> bool {
    match expr {
        PromExpr::VectorSelector(selector) => out.push(selector),
        PromExpr::MatrixSelector(MatrixSelector { vs, .. }) => out.push(vs),
        PromExpr::Paren(ParenExpr { expr }) => return collect_selectors(expr, out),
        // Left before right, because a non-commutative operator means what that order says.
        PromExpr::Binary(binary) => {
            return collect_selectors(&binary.lhs, out) && collect_selectors(&binary.rhs, out);
        }
        // The compiler folds a negation into a multiplication whose operand comes second, but the
        // selector is the only thing this walk collects, so the order inside one subtree does not
        // depend on where the constant sits.
        PromExpr::Unary(unary) => return collect_selectors(&unary.expr, out),
        PromExpr::Call(call) => {
            // A label rewrite adds a label the node's output schema cannot declare: that schema is
            // built from the table's own tags, so folding `label_replace`/`label_join` in would
            // silently drop the destination label the existing plan projects.
            if matches!(call.func.name, "label_replace" | "label_join") {
                return false;
            }
            for arg in &call.args.args {
                if !collect_selectors(arg, out) {
                    return false;
                }
            }
        }
        // An aggregate folds its input's series, so the selector is the one its input names; whether
        // this route can answer it is the split's own decision, which reads the compiled tree.
        // `param` is not descended into: an aggregate's parameter has to be a constant for the
        // compiler to bind it, and a parameter naming a second selector is refused there rather
        // than planned against the wrong scan.
        PromExpr::Aggregate(aggregate) => return collect_selectors(&aggregate.expr, out),
        // Subquery plans are not executed by the native route. Decline the entire expression,
        // including when one is nested beneath a supported call or binary node.
        PromExpr::Subquery(_) => return false,
        _ => {}
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

/// Whether `expr` calls `rate`/`increase`, anywhere an admitted statement can nest one: call
/// arguments, parentheses, both binary operands, a unary operand and an aggregate's input.
///
/// The aggregate's parameter is not descended into, because the compiler refuses a parameter that is
/// not a constant: a `rate` sitting there is not a call the admitted tree computes.
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
        PromExpr::Aggregate(aggregate) => calls_rate_or_increase(&aggregate.expr),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, UNIX_EPOCH};

    use datatypes::series::SeriesOrdering;
    use datatypes::series::samples::{FieldSpec, SeriesSchema};
    use promql::native::plan::{NativeExpr, NativeNode};
    use session::context::QueryContextBuilder;

    use super::{
        NATIVE_PROMQL_EXTENSION_KEY, collapsed_pair_tree, collect_selectors,
        native_promql_requested,
    };

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

    /// The statement `query` is compiled against: one selector over five seconds of grid.
    fn native_test_statement(query: &str) -> promql_parser::parser::EvalStmt {
        promql_parser::parser::EvalStmt {
            expr: promql_parser::parser::parse(query).unwrap(),
            start: UNIX_EPOCH,
            end: UNIX_EPOCH + Duration::from_secs(10),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(5),
        }
    }

    /// `query` compiled against one raw-series layout, every selector bound to it.
    ///
    /// The entry is `compile_selectors` rather than the compatibility `compile`, because the pair
    /// these tests are about names one selector twice and `compile` refuses a statement whose two
    /// occurrences it cannot tell apart. Binding both occurrences to one layout is the test's
    /// choice of scan, not a claim the compiler makes.
    fn native_test_tree(query: &str) -> promql::native::plan::NativeTree {
        let schema = Arc::new(SeriesSchema {
            fields: Arc::from([FieldSpec { name: "v0".into() }]),
            eval_grid: None,
        });
        let stmt = native_test_statement(query);
        crate::promql::native_compile::compile_selectors(
            &stmt,
            &|_| Ok(Arc::clone(&schema)),
            SeriesOrdering::KeyThenTime,
        )
        .unwrap()
        .tree
    }

    /// The child each raw input below `root` reads, in the order the compiler bound them.
    fn native_test_input_sources(root: &NativeNode) -> Vec<usize> {
        let mut sources = Vec::new();
        promql::native::metrics::walk(root, &mut |node, _| {
            if let NativeExpr::Input(bound) = &node.expr {
                sources.push(bound.source_index);
            }
        });
        sources
    }

    /// The selectors `query` names, in the order the compiler binds them, or `None` when the walk
    /// refuses the expression outright.
    fn collected_selectors(query: &str) -> Option<Vec<String>> {
        let expr = promql_parser::parser::parse(query).expect("the query parses");
        let mut selectors = Vec::new();
        if !collect_selectors(&expr, &mut selectors) {
            return None;
        }
        Some(
            selectors
                .iter()
                .map(|selector| selector.name.clone().unwrap_or_default())
                .collect(),
        )
    }

    /// The walk descends into an aggregate's input, because that is the selector the statement
    /// reads, and leaves the aggregate's parameter alone: a parameter has to be a constant for the
    /// compiler to bind it.
    #[test]
    fn collect_selectors_descends_into_an_aggregate_input() {
        for query in [
            "sum(x)",
            "sum by (job) (x)",
            "sum without (instance) (rate(x[5m]))",
            "avg(ceil(x))",
            "sum(count_over_time(x[10m]))",
            "sum(rate(x[10m] offset 1m))",
        ] {
            assert_eq!(
                collected_selectors(query),
                Some(vec!["x".to_string()]),
                "{query}"
            );
        }
        // Two occurrences are two selectors whichever side of an aggregate they sit on; whether the
        // route admits them is the two-source guard's decision, not this walk's.
        for query in ["sum(a) / sum(b)", "sum(a + b)"] {
            assert_eq!(
                collected_selectors(query),
                Some(vec!["a".to_string(), "b".to_string()]),
                "{query}"
            );
        }
        // A subquery names a shape no tree node here executes, so the whole expression is declined
        // rather than bound to the selectors around it.
        assert_eq!(collected_selectors("rate(x[5m:1m])"), None);
    }

    /// Collapsing a pair binds both occurrences to the one child the planner prepared for them.
    #[test]
    fn a_pair_tree_is_rebound_to_its_one_child() {
        let tree = native_test_tree("x + x");
        assert_eq!(
            native_test_input_sources(tree.root()),
            vec![0, 1],
            "the compiler numbers occurrences"
        );

        let collapsed = collapsed_pair_tree(&tree).unwrap();
        assert_eq!(native_test_input_sources(collapsed.root()), vec![0, 0]);
        // The rest of the tree is the one that was compiled: only the bindings moved.
        assert!(matches!(collapsed.root().expr, NativeExpr::Binary { .. }));
        assert_eq!(
            tree.to_string().replace("source=1", "source=0"),
            collapsed.to_string(),
            "the collapse changed something other than the child each input reads"
        );
    }

    /// A tree whose inputs are not the two occurrences is declined, and the refusal is the one the
    /// route answers with the existing plan — not an internal error to report.
    #[test]
    fn a_tree_that_is_not_a_pair_is_declined_by_the_collapse() {
        let error = collapsed_pair_tree(&native_test_tree("x")).unwrap_err();
        assert!(
            matches!(error, datafusion::error::DataFusionError::NotImplemented(_)),
            "{error}"
        );
        assert!(
            error.to_string().contains("cannot bind to one child"),
            "{error}"
        );
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
