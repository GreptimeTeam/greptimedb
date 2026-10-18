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

//! Local selection of one fused range-function subtree over a single region scan.
//!
//! [`try_select_fused`] runs at the existing experimental gate on the optimized logical plan and
//! the physical plan DataFusion built for it. It fuses exactly one subtree of the shape
//!
//! ```text
//! Filter: <value> IS NOT NULL     <- optional, may repeat
//!   Projection                    <- moves columns, applies the one range function
//!     Extension(RangeManipulate)  <- the folded range window
//! ```
//!
//! into one [`FusedSeriesExec`], which evaluates the range function over raw series instead of
//! folding the window into range arrays first.
//!
//! The fused subtree may sit anywhere in the plan, so an aggregation over a range function (the
//! common query shape) is fused too. Planning does not keep logical and physical nodes in step --
//! one logical aggregate becomes a partial and a final aggregate around a repartition -- so the
//! physical node to swap is located by its content, not by a position in the tree: it is the one
//! projection that outputs exactly the schema of the matched subtree and folds exactly one range
//! window below itself. Every ancestor keeps its own configuration and recomputes its properties
//! around the new child. Two or more candidate subtrees or candidate nodes are rejected, because
//! phase 1 fuses exactly one.
//!
//! Every gate failure returns `Ok(None)`, which means "run the ordinary plan". Nothing here may
//! turn an unrecognized shape into a query error: the ordinary plan is always a correct answer, so
//! an unfamiliar shape is only ever a fallback.

use std::sync::Arc;

use api::v1::SemanticType;
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::datasource::DefaultTableSource;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Result as DfResult, ScalarValue};
use datafusion_expr::logical_plan::{SubqueryAlias, TableScan};
use datafusion_expr::{Expr, Extension, LogicalPlan, Operator};
use datatypes::data_type::ConcreteDataType;
use datatypes::series::time::{EvalGrid, GridSpec};
use promql::extension_plan::fused_series::FusedSeriesExec;
use promql::extension_plan::{
    RangeManipulate, RangeManipulateExec, SeriesDivide, SeriesDivideExec, SeriesNormalize,
    SeriesNormalizeExec,
};
use promql::range_eval::{RangeFunction, RangeWindow};
use store_api::metric_engine_consts::is_metric_engine_internal_column;
use table::table::scan::RegionScanExec;

use crate::datafusion::raw_expr_supported;
use crate::dummy_catalog::DummyTableProvider;

/// Selects one fused range-function subtree, or `None` to run the ordinary plan unchanged.
pub(crate) fn try_select_fused(
    logical: &LogicalPlan,
    original: &Arc<dyn ExecutionPlan>,
) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
    let Some(shape) = FusedShape::discover(logical) else {
        return Ok(None);
    };
    // The subtree is located by its content, so it is found under an aggregate and under any
    // wrapper node the plan carries at its root.
    let Some(target) = pick(original, &shape) else {
        return Ok(None);
    };
    let Some((scan, scan_cooperative)) = fused_scan(&target) else {
        return Ok(None);
    };
    let fused = match shape.build(&scan, scan_cooperative) {
        Ok(fused) => fused,
        Err(reason) => {
            common_telemetry::debug!("Fused series selection fell back: {reason}");
            return Ok(None);
        }
    };
    let Some(replaced) = rebuild(original, &target, &fused) else {
        return Ok(None);
    };
    // A replacement that changes the caller's output is not an equivalent plan.
    if replaced.schema() != original.schema() {
        return Ok(None);
    }
    // Operators need to know which path ran; a fallback is silent by design.
    common_telemetry::debug!(
        "Fused series selection succeeded: {:?} over {}",
        shape.function,
        shape.scan.table_name
    );
    Ok(Some(replaced))
}

/// One matched fused subtree, with everything the fused node needs to run it.
struct FusedShape {
    /// The single range function the subtree applies.
    function: RangeFunction,
    /// The scan the range window reads.
    scan: TableScan,
    /// The evaluation window and grid of the folded range.
    start: i64,
    end: i64,
    interval: i64,
    range: i64,
    time_index: String,
    /// The one value column the range function consumes.
    field: String,
    /// The output schema of the fused subtree, taken from the plan itself.
    output_schema: SchemaRef,
}

/// What a matched `RangeManipulate` contributes to a [`FusedShape`].
struct Fold {
    scan: TableScan,
    start: i64,
    end: i64,
    interval: i64,
    range: i64,
    time_index: String,
    field: String,
}

impl FusedShape {
    /// Finds the single fused subtree of `plan`, or `None` if there is none or more than one.
    fn discover(plan: &LogicalPlan) -> Option<Self> {
        let mut found = Vec::new();
        collect(plan, &mut found);
        let [shape] = found.try_into().ok()?;
        Some(shape)
    }

    /// Matches the chain of projections and filters whose bottom is one `RangeManipulate`.
    fn match_chain(plan: &LogicalPlan) -> Option<Self> {
        let mut call = None;
        let fold = fold(plan, &mut call)?;
        let call = call?;
        Some(Self {
            function: call.function,
            scan: fold.scan,
            start: fold.start,
            end: fold.end,
            interval: fold.interval,
            range: fold.range,
            time_index: fold.time_index,
            field: fold.field,
            output_schema: Arc::new(plan.schema().as_arrow().clone()),
        })
    }

    /// Builds the fused node over the raw series leaf of the matched `TableScan`.
    ///
    /// The error is the name of the gate that rejected the shape; the caller falls back to the
    /// ordinary plan, so this is never a query failure.
    fn build(
        &self,
        scan: &Arc<RegionScanExec>,
        scan_cooperative: bool,
    ) -> Result<Arc<dyn ExecutionPlan>, &'static str> {
        let Some(source) = self.scan.source.downcast_ref::<DefaultTableSource>() else {
            return Err("scan source is not a default table source");
        };
        let Some(provider) = source.table_provider.downcast_ref::<DummyTableProvider>() else {
            return Err("scan source is not a dummy table provider");
        };
        let metadata = provider.region_metadata();
        let metric_name = self.scan.table_name.table().to_string();
        // The authorized logical tags are every visible tag of the table, in table order.
        let logical_tags = metadata
            .column_metadatas
            .iter()
            .filter(|column| {
                column.semantic_type == SemanticType::Tag
                    && !is_metric_engine_internal_column(&column.column_schema.name)
            })
            .map(|column| column.column_schema.name.clone())
            .collect::<Vec<_>>();
        // The output schema of the plan itself is the authority on the output columns: its tag
        // columns are the declared tags in output order, and its one `Float64` column is the value
        // column, which the planner names after the range-function call rather than the raw field.
        let mut tags = Vec::with_capacity(logical_tags.len());
        let mut values = Vec::new();
        let mut has_time = false;
        for field in self.output_schema.fields() {
            if field.name() == &self.time_index {
                if field.data_type() != &DataType::Timestamp(TimeUnit::Millisecond, None)
                    || field.is_nullable()
                {
                    return Err("time column is not a non-null millisecond timestamp");
                }
                has_time = true;
                continue;
            }
            if logical_tags.iter().any(|tag| tag == field.name()) {
                // The real plan reports labels dictionary-encoded; both encodings are supported.
                match field.data_type() {
                    DataType::Utf8 => {}
                    DataType::Dictionary(key, value)
                        if **key == DataType::UInt32 && **value == DataType::Utf8 => {}
                    _ => return Err("tag column is neither Utf8 nor Dictionary(UInt32, Utf8)"),
                }
                // Either nullability is fine: the node reports an absent label as a NULL where
                // the column allows one and as the empty label value where it does not.
                tags.push(field.name().clone());
                continue;
            }
            // Everything else is the value column: a nullable float that does not shadow a
            // non-float column of the table.
            if field.data_type() != &DataType::Float64 || !field.is_nullable() {
                return Err("value column is not a nullable Float64");
            }
            if let Some(column) = metadata.column_by_name(field.name())
                && (column.semantic_type != SemanticType::Field
                    || column.column_schema.data_type != ConcreteDataType::float64_datatype())
            {
                return Err("value column shadows a non-float column of the table");
            }
            values.push(field.name().clone());
        }
        // Exactly one time column and one value column are required. The tag columns are
        // whatever the plan still projects: an aggregation over the range function prunes the
        // labels it does not group by, and the fused node must emit the same columns its parent
        // reads. Emitting fewer labels does not lose the label-set identity, because duplicate
        // detection compares the complete label set of each series, not the projected columns.
        //
        // Requiring every table tag here would reject `sum by (one_label) (rate(..))` whenever the
        // table has more than one tag, which is the most common PromQL query shape. The projection
        // above the fold is the authority on which columns the parent reads.
        if !has_time || values.len() != 1 {
            return Err("output schema is not one time column and one value column");
        }
        // One field, and therefore a one-element `field_names`, is what keeps the fused node's
        // output classification unambiguous: `classify_output` claims an unnamed `Float64` column
        // for the next unclaimed field, which can only mean one thing while there is one field.
        // Relaxing this to several fields would make that claim positional and ambiguous.
        let field_names = vec![self.field.clone()];

        // The fused node runs a single partition: `RangeEvaluator` requires a series' chunks to
        // arrive consecutively and it tracks the label set of the series it is reading, so it
        // needs one sequential stream.
        //
        // The physical optimizer has already run by the time this selection does, so a scan that
        // `ParallelizeScan` split across `target_partitions` arrives here partitioned, and reading
        // it as one merges those ranges back into a single partition. That gives up the scan
        // parallelism the ordinary plan would have had, including the region read and merge
        // concurrency that goes with it. Reading the scan per partition is the follow-up that
        // removes that limit; it needs the node to run the partition it is given and to keep one
        // evaluator per partition, together with the binding's own single-partition requirement.
        // Until then the scan is prepared once, as one partition, and used as it is when it
        // already is.
        let collapsed = if scan.properties().output_partitioning().partition_count() == 1
            && scan.is_partition_set()
        {
            scan.clone()
        } else {
            Arc::new(
                scan.with_new_partitions(vec![scan.get_partition_ranges()], 1)
                    .map_err(|_| "scan cannot be prepared as one partition")?,
            )
        };
        let binding = match collapsed.clone().try_to_named_series_binding(
            &metric_name,
            &logical_tags,
            &field_names,
        ) {
            Ok(Some((binding, _))) => binding,
            // A leaf this shape cannot use, or one the scanner refuses, is a fallback.
            Ok(None) => return Err("scan offers no named raw-series leaf"),
            Err(_) => return Err("scan refused the named raw-series leaf"),
        };
        let prepared = collapsed;
        let native = match prepared.try_to_bound_series_scan(&binding) {
            Ok(Some(native)) => native,
            // A leaf this shape cannot use, or one the scanner refuses, is a fallback.
            Ok(None) => return Err("scan offers no named raw-series leaf"),
            Err(_) => return Err("scan refused the named raw-series leaf"),
        };
        // The leaf is what the fused node reads, and the fused node has one partition, so the
        // leaf must have one too.
        if native.properties().output_partitioning().partition_count() != 1 {
            return Err("raw-series leaf is not a single partition");
        }
        // The leaf must read exactly the value column the pattern matched, and the scan it came
        // from must still declare the columns the fused output claims, so that the output the
        // fused node produces is the output the ordinary plan would have produced. The leaf's own
        // schema is the packed raw frame, not a column schema, so it is checked by its series
        // schema instead.
        let series_fields = &native.series_schema().fields;
        if series_fields.len() != 1 || series_fields[0].name.as_ref() != field_names[0] {
            return Err("raw-series leaf reads different fields than the pattern");
        }
        let scan_schema = scan.schema();
        let logical_output = self.scan.projected_schema.as_ref();
        if logical_output
            .fields()
            .iter()
            .any(|logical| scan_schema.field_with_name(logical.name()).is_err())
        {
            return Err("scanned columns are not all present in the region scan");
        }

        // A grid needs a positive step and a non-empty, representable point count.
        let span = self
            .end
            .checked_sub(self.start)
            .ok_or("evaluation range underflows")?;
        if self.interval <= 0 || span < 0 {
            return Err("evaluation step is not positive or the range is inverted");
        }
        let count = u64::try_from(span / self.interval)
            .ok()
            .and_then(|count| count.checked_add(1))
            .ok_or("evaluation grid point count is not representable")?;
        let spec = GridSpec::try_new(self.start, self.interval, count)
            .map_err(|_| "evaluation grid specification is invalid")?;
        let grid =
            Arc::new(EvalGrid::try_new(spec).map_err(|_| "evaluation grid cannot be built")?);
        let label_source = native.label_source().clone();
        let series_schema = Arc::clone(native.series_schema());
        let ordering = native.raw_series_ordering();
        let mut child: Arc<dyn ExecutionPlan> = native;
        if scan_cooperative {
            child = Arc::new(CooperativeExec::new(child));
        }
        // A shape that passed every gate but that the node itself refuses is still a fallback.
        let fused = FusedSeriesExec::try_new(
            child,
            series_schema,
            label_source,
            ordering,
            grid,
            RangeWindow {
                range_ms: self.range,
                offset_ms: 0,
                anchor_ms: None,
            },
            self.function,
            // Phase 1: a map chain is not fused, so the plans reaching here carry none.
            vec![],
            self.output_schema.clone(),
            tags,
            self.time_index.clone(),
        )
        .map_err(|_| "FusedSeriesExec rejected the raw-series frame or the output schema")?;
        Ok(Arc::new(fused))
    }
}

/// Records `value` as the one range call of the subtree, rejecting a second one.
fn set_once<T>(slot: &mut Option<T>, value: T) -> Option<()> {
    match slot {
        Some(_) => None,
        None => {
            *slot = Some(value);
            Some(())
        }
    }
}

/// One admitted range-function call: the kernel and the columns it reads.
struct RangeCall {
    function: RangeFunction,
    /// The folded value column, which must be the one `RangeManipulate` folds.
    value: String,
    /// The folded timestamp-range column the kernel reads.
    range: String,
}

/// Maps one range-function call to the kernel that evaluates it.
///
/// Only the three functions of the phase 1 scope are admitted, and only with the argument list the
/// planner builds for them: `(timestamp_range, value, time_index, range)`. A map kernel or an
/// arithmetic layer is not a range function call and returns `None`.
fn range_call(expr: &Expr) -> Option<RangeCall> {
    let Expr::ScalarFunction(call) = expr else {
        return None;
    };
    let function = match call.func.name() {
        "prom_rate" => RangeFunction::Rate,
        "prom_increase" => RangeFunction::Increase,
        "prom_delta" => RangeFunction::Delta,
        _ => return None,
    };
    let [range, value, _time, length] = call.args.as_slice() else {
        return None;
    };
    if !matches!(length, Expr::Literal(ScalarValue::Int64(Some(_)), _)) {
        return None;
    }
    Some(RangeCall {
        function,
        value: column_name(value)?,
        range: column_name(range)?,
    })
}

/// The unqualified name of a column expression.
fn column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(column) => Some(column.name.clone()),
        _ => None,
    }
}

/// Whether `expr` is only `IS NOT NULL` predicates over columns, joined by `AND`.
fn is_not_null_only(expr: &Expr) -> bool {
    match expr {
        Expr::IsNotNull(inner) => matches!(inner.as_ref(), Expr::Column(_)),
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            is_not_null_only(&binary.left) && is_not_null_only(&binary.right)
        }
        _ => false,
    }
}

/// Walks the whole plan and collects every fused subtree.
fn collect(plan: &LogicalPlan, out: &mut Vec<FusedShape>) {
    if matches!(plan, LogicalPlan::Projection(_) | LogicalPlan::Filter(_))
        && let Some(shape) = FusedShape::match_chain(plan)
    {
        out.push(shape);
        return;
    }
    for child in plan.inputs() {
        collect(child, out);
    }
}

/// Walks one chain of projections and filters down to its `RangeManipulate`.
///
/// `call` accumulates the single range-function call of the chain, because the projection that
/// applies it is the innermost one and is only seen after the outer projections.
fn fold(plan: &LogicalPlan, call: &mut Option<RangeCall>) -> Option<Fold> {
    match plan {
        LogicalPlan::Projection(projection) => {
            for expr in &projection.expr {
                match expr {
                    // A move-only projection changes no value.
                    Expr::Column(_) => {}
                    Expr::Alias(alias) => set_once(call, range_call(&alias.expr)?)?,
                    Expr::ScalarFunction(_) => set_once(call, range_call(expr)?)?,
                    _ => return None,
                }
            }
            fold(&projection.input, call)
        }
        // The ordinary plan keeps `<value> IS NOT NULL` above the projection; anything else
        // changes which rows survive, which the fused node cannot reproduce here.
        LogicalPlan::Filter(filter) => {
            if !is_not_null_only(&filter.predicate) {
                return None;
            }
            fold(&filter.input, call)
        }
        LogicalPlan::Extension(Extension { node }) => {
            let manipulate = node.as_any().downcast_ref::<RangeManipulate>()?;
            let [field] = manipulate.field_columns() else {
                return None;
            };
            // A shifted window needs the series' own timestamps moved onto the evaluation
            // timeline, which phase 1 does not re-derive.
            if manipulate.offset() != 0 {
                return None;
            }
            // The call must read exactly the folded window and value column, so the kernel this
            // node runs computes what the call in the plan computes.
            let call = call.as_ref()?;
            if &call.value != field
                || call.range
                    != RangeManipulate::build_timestamp_range_name(manipulate.time_index())
            {
                return None;
            }
            let inputs = node.inputs();
            let [input] = inputs.as_slice() else {
                return None;
            };
            Some(Fold {
                scan: folding_pipeline(input)?,
                start: manipulate.start(),
                end: manipulate.end(),
                interval: manipulate.interval(),
                range: manipulate.range(),
                time_index: manipulate.time_index().to_string(),
                field: field.clone(),
            })
        }
        _ => None,
    }
}

/// Returns the single table scan a `RangeManipulate` reads through its folding pipeline.
fn folding_pipeline(plan: &LogicalPlan) -> Option<TableScan> {
    match plan {
        LogicalPlan::Extension(Extension { node }) => {
            let any = node.as_any();
            if any.downcast_ref::<SeriesNormalize>().is_none()
                && any.downcast_ref::<SeriesDivide>().is_none()
            {
                return None;
            }
            let inputs = node.inputs();
            let [input] = inputs.as_slice() else {
                return None;
            };
            folding_pipeline(input)
        }
        LogicalPlan::Sort(sort) if sort.fetch.is_none() => folding_pipeline(&sort.input),
        LogicalPlan::Filter(filter) if raw_expr_supported(&filter.predicate) => {
            folding_pipeline(&filter.input)
        }
        LogicalPlan::Projection(projection) => folding_pipeline(&projection.input),
        LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. }) => folding_pipeline(input),
        LogicalPlan::TableScan(scan) => {
            if scan.fetch.is_some()
                || !scan.statistics_requests.is_empty()
                || scan
                    .filters
                    .iter()
                    .any(|filter| !raw_expr_supported(filter))
            {
                return None;
            }
            Some(scan.clone())
        }
        _ => None,
    }
}

/// Returns the one physical node the fused node replaces, or `None` if it is absent or ambiguous.
///
/// Aggregation turns one logical node into several physical nodes and the optimizer inserts
/// partitioning nodes, so nothing in the physical plan can be reached by mirroring the logical
/// tree. The node is identified by its content instead: it is the only projection that outputs
/// exactly the schema of the matched subtree and that folds exactly one range window below itself.
/// The projection is what the fused node replaces; filters, aggregates and every other ancestor
/// above it keep their own configuration.
fn pick(physical: &Arc<dyn ExecutionPlan>, shape: &FusedShape) -> Option<Arc<dyn ExecutionPlan>> {
    let mut found = Vec::new();
    collect_candidates(physical, shape, &mut found);
    // Zero candidates is an unfamiliar plan; several would mean the swap is ambiguous. Both fall
    // back to the ordinary plan.
    let [candidate] = found.as_slice() else {
        return None;
    };
    Some(Arc::clone(candidate))
}

/// Collects every physical node that is the top of the matched fused subtree.
fn collect_candidates(
    node: &Arc<dyn ExecutionPlan>,
    shape: &FusedShape,
    out: &mut Vec<Arc<dyn ExecutionPlan>>,
) {
    if is_candidate(node, shape) {
        out.push(Arc::clone(node));
        return;
    }
    for child in node.children() {
        collect_candidates(child, shape, out);
    }
}

/// Whether `node` is the projection the fused node replaces.
///
/// It must be a projection, because the swap only moves columns and applies the one range function,
/// it must output exactly the columns of the matched subtree, and the single-child chain below it
/// must hold exactly one range fold. A chain that branches is not one folded range window.
fn is_candidate(node: &Arc<dyn ExecutionPlan>, shape: &FusedShape) -> bool {
    if (node.as_ref() as &dyn std::any::Any)
        .downcast_ref::<ProjectionExec>()
        .is_none()
        || node.schema() != shape.output_schema
    {
        return false;
    }
    let mut windows = 0;
    let mut node = Arc::clone(node);
    loop {
        if (node.as_ref() as &dyn std::any::Any)
            .downcast_ref::<RangeManipulateExec>()
            .is_some()
        {
            windows += 1;
            // Two folds are two windows, which one fused node cannot reproduce.
            if windows > 1 {
                return false;
            }
        }
        match node.children().as_slice() {
            [] => return windows == 1,
            [child] => node = Arc::clone(child),
            _ => return false,
        }
    }
}

/// Rebuilds the physical plan with `target` swapped for `fused`.
///
/// Only the node that is `target` itself is swapped, by pointer identity, so no other node is
/// mistaken for it. Every ancestor is kept with its own configuration and recomputes its properties
/// around its new child, so an aggregate above the fused node keeps its own plan shape.
fn rebuild(
    physical: &Arc<dyn ExecutionPlan>,
    target: &Arc<dyn ExecutionPlan>,
    fused: &Arc<dyn ExecutionPlan>,
) -> Option<Arc<dyn ExecutionPlan>> {
    let rebuilt = Arc::clone(physical)
        .transform_up(|node| {
            // Only the candidate itself is swapped; every other node is left as it is, and a parent
            // is rebuilt only because its child changed.
            if Arc::ptr_eq(&node, target) {
                Ok(Transformed::yes(Arc::clone(fused)))
            } else {
                Ok(Transformed::no(node))
            }
        })
        .ok()?
        .data;
    Some(rebuilt)
}

/// Returns the input of a `CooperativeExec`, which corresponds to no logical node.
fn unwrap_cooperative(physical: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    (physical.as_ref() as &dyn std::any::Any)
        .downcast_ref::<CooperativeExec>()
        .map(|wrapper| Arc::clone(wrapper.input()))
}

/// Returns the single region scan the matched subtree reads, and whether it was wrapped.
///
/// Only the operators the fused node reproduces may appear between the subtree root and the scan;
/// anything else would be silently dropped by the swap, so it rejects the shape.
fn fused_scan(target: &Arc<dyn ExecutionPlan>) -> Option<(Arc<RegionScanExec>, bool)> {
    let mut node = Arc::clone(target);
    let mut saw_fold = false;
    loop {
        let cooperative = unwrap_cooperative(&node);
        if let Some(inner) = &cooperative {
            node = Arc::clone(inner);
        }
        let any = node.as_ref() as &dyn std::any::Any;
        if let Some(scan) = any.downcast_ref::<RegionScanExec>() {
            return saw_fold.then(|| (Arc::new(scan.clone()), cooperative.is_some()));
        }
        // The fused node reads the scan itself, one stream per partition, so the partitioning
        // nodes the optimizer inserts between the fold and the scan are exactly what the swap
        // removes.
        let folded = is_folded_range(any);
        if !folded
            && any.downcast_ref::<ProjectionExec>().is_none()
            && any.downcast_ref::<FilterExec>().is_none()
            && any.downcast_ref::<SortExec>().is_none()
            && any.downcast_ref::<RepartitionExec>().is_none()
            && any.downcast_ref::<CoalescePartitionsExec>().is_none()
        {
            return None;
        }
        saw_fold |= folded;
        let children = node.children();
        let [child] = children.as_slice() else {
            return None;
        };
        node = Arc::clone(child);
    }
}

/// Whether `any` is a range-folding operator the fused node evaluates itself.
fn is_folded_range(any: &dyn std::any::Any) -> bool {
    any.downcast_ref::<RangeManipulateExec>().is_some()
        || any.downcast_ref::<SeriesNormalizeExec>().is_some()
        || any.downcast_ref::<SeriesDivideExec>().is_some()
}

#[cfg(test)]
mod e2e {
    //! End-to-end proof that a real PromQL query runs through the fused node.
    //!
    //! The test plans `rate(...)` with the PromQL planner, plans the physical side with the
    //! engine's own planner (which is what installs `PromExtensionPlanner`), runs the production
    //! selection entry point on that pair, and then compares the fused result against the ordinary
    //! unfused plan over the same mock region.

    use std::any::Any;
    use std::fmt;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::task::{Context, Poll};
    use std::time::{Duration, UNIX_EPOCH};

    // The production routing entry point lives one level above `fused`, so `super` is not
    // enough from inside this module.
    use crate::datafusion::native_promql::try_select;

    use api::region::RegionResponse;
    use api::v1::SemanticType;
    use async_trait::async_trait;
    use catalog::RegisterTableRequest;
    use catalog::memory::MemoryCatalogManager;
    use catalog::table_source::DfTableSourceProvider;
    use common_base::Plugins;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_error::ext::BoxedError;
    use common_query::test_util::DummyDecoder;
    use datafusion::arrow::array::{
        Array, ArrayRef, Float64Array, StringArray, TimestampMillisecondArray,
    };
    use datafusion::arrow::record_batch::RecordBatch as DfRecordBatch;
    use datafusion::datasource::DefaultTableSource;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::display::{DisplayAs, DisplayFormatType};
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datatypes::arrow::buffer::Buffer;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::series::SeriesOrdering;
    use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
    use datatypes::series::samples::{FieldSpec, Samples, SeriesFields, SeriesSchema};
    use futures::Stream;
    use promql_parser::parser;
    use promql_parser::parser::EvalStmt;
    use session::context::QueryContext;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
    use store_api::region_engine::{
        PrepareRequest, QueryScanContext, RawLabelSource, RawSeriesStream, RegionEngine,
        RegionEngineRef, RegionRole, RegionScanner, RegionScannerRef, RegionStatistic,
        RemapManifestsRequest, RemapManifestsResponse, ScannerProperties, SendableRawSeriesStream,
        SetRegionRoleStateResponse, SettableRegionRoleState, SyncRegionFromRequest,
        SyncRegionFromResponse,
    };
    use store_api::region_request::RegionRequest;
    use store_api::storage::{RegionId, ScanRequest, SequenceNumber};
    use table::metadata::{TableInfoBuilder, TableMetaBuilder};
    use table::test_util::EmptyTable;

    use crate::QueryEngineContext;
    use crate::dummy_catalog::DummyTableProvider;
    use crate::options::QueryOptions;
    use crate::promql::planner::PromPlanner;
    use crate::query_engine::QueryEngineState;

    const TABLE: &str = "some_metric";
    const TIME: &str = "ts";
    const VALUE: &str = "v0";

    /// The tags of the fixture table, in primary-key order. `sum by (instance)` keeps only
    /// `instance`, so the other three are exactly the tags a tag-subset projection drops.
    const TAGS: [&str; 4] = ["host", "instance", "job", "l1"];
    /// The one tag `sum by (instance)` keeps.
    const INSTANCE: &str = "instance";

    /// One sample per minute from 0 s to 540 s, rising 60 per sample.
    fn linear_counter(rate_per_minute: f64) -> Vec<(i64, f64)> {
        (0..=9)
            .map(|i| (i * 60_000, (i as f64) * rate_per_minute))
            .collect()
    }

    /// Two instances, each with one series in each of the two storage partitions. The four
    /// slopes are chosen so that a correct `sum by (instance)` gives a value no partial sum can
    /// imitate: `instance=x` is 1 + 2 and `instance=y` is 3 + 4 per second.
    fn aggregate_fixture() -> Fixture {
        Fixture {
            tags: TAGS.to_vec(),
            partitions: 2,
            series: vec![
                FixtureSeries {
                    tags: vec!["a", "x", "j", "l"],
                    partition: 0,
                    samples: linear_counter(60.0),
                },
                FixtureSeries {
                    tags: vec!["b", "x", "j", "l"],
                    partition: 1,
                    samples: linear_counter(120.0),
                },
                FixtureSeries {
                    tags: vec!["c", "y", "j", "l"],
                    partition: 0,
                    samples: linear_counter(180.0),
                },
                FixtureSeries {
                    tags: vec!["d", "y", "j", "l"],
                    partition: 1,
                    samples: linear_counter(240.0),
                },
            ],
        }
    }

    /// One series in the first partition, for the tests that do not need two.
    fn one_series_fixture() -> Fixture {
        Fixture {
            tags: TAGS.to_vec(),
            partitions: 1,
            series: vec![FixtureSeries {
                tags: vec!["alpha", "x", "j", "l"],
                partition: 0,
                samples: linear_counter(60.0),
            }],
        }
    }

    /// An engine whose native-PromQL flag is `native` and whose parallelism is `parallelism`.
    ///
    /// `parallelism` is the session's `target_partitions`, which is what the scan-parallelizing
    /// optimizer splits a region's ranges into. Pinning it to the fixture's own split is what
    /// makes the fused node's partition count the number this file asserts, rather than whatever
    /// the machine has cores for.
    fn engine_state(native: bool, parallelism: usize) -> QueryEngineState {
        QueryEngineState::new(
            MemoryCatalogManager::with_default_setup(),
            None,
            None,
            None,
            None,
            None,
            false,
            Plugins::default(),
            QueryOptions {
                experimental_native_promql: native,
                parallelism,
                ..Default::default()
            },
        )
    }

    /// The columns of the fixture table: the tag columns, then the field, then the time index.
    fn table_columns() -> (Vec<ColumnSchema>, usize) {
        let mut columns: Vec<ColumnSchema> = TAGS
            .iter()
            .map(|tag| {
                ColumnSchema::new(tag.to_string(), ConcreteDataType::string_datatype(), false)
            })
            .collect();
        columns.push(ColumnSchema::new(
            VALUE.to_string(),
            ConcreteDataType::float64_datatype(),
            true,
        ));
        columns.push(
            ColumnSchema::new(
                TIME,
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
        );
        (columns, TAGS.len())
    }

    /// A region metadata over [`table_columns`] whose primary key is every tag column, in order.
    fn fixture_region_metadata(region_id: RegionId) -> RegionMetadataRef {
        let (columns, tags) = table_columns();
        let mut builder = RegionMetadataBuilder::new(region_id);
        for (index, column) in columns.iter().enumerate() {
            let semantic_type = match index {
                i if i < tags => SemanticType::Tag,
                i if i == tags => SemanticType::Field,
                _ => SemanticType::Timestamp,
            };
            builder.push_column_metadata(ColumnMetadata {
                column_schema: column.clone(),
                semantic_type,
                column_id: index as u32,
            });
        }
        builder.primary_key((0..tags as u32).collect());
        Arc::new(builder.build().unwrap())
    }

    /// A catalog holding the empty logical table the PromQL planner resolves.
    fn table_provider() -> DfTableSourceProvider {
        let catalog_list = MemoryCatalogManager::with_default_setup();
        let (columns, tags) = table_columns();
        let tag_count = columns.len() - 2;
        assert_eq!(tags, tag_count);
        let schema = Arc::new(Schema::new(columns));
        let table_meta = TableMetaBuilder::empty()
            .schema(schema)
            // The field is the column between the tags and the time index.
            .primary_key_indices((0..tags).collect())
            .value_indices(vec![tags])
            .next_column_id(1024)
            .build()
            .unwrap();
        let table_info = TableInfoBuilder::default()
            .name(TABLE)
            .meta(table_meta)
            .build()
            .unwrap();
        catalog_list
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: TABLE.to_string(),
                table_id: 1024,
                table: EmptyTable::from_table_info(&table_info),
            })
            .unwrap();
        DfTableSourceProvider::new(
            catalog_list,
            false,
            QueryContext::arc(),
            DummyDecoder::arc(),
            false,
        )
    }

    /// `rate(some_metric[5m])` from 360 s to 540 s, at 60 s steps.
    ///
    /// The fixture has one sample per minute, so every window from 360 s on holds five intervals
    /// and is fully covered: no extrapolation slack enters the expected value.
    fn eval_stmt() -> EvalStmt {
        EvalStmt {
            expr: parser::parse(&format!("rate({TABLE}[5m])")).unwrap(),
            start: UNIX_EPOCH.checked_add(Duration::from_secs(360)).unwrap(),
            end: UNIX_EPOCH.checked_add(Duration::from_secs(540)).unwrap(),
            interval: Duration::from_secs(60),
            lookback_delta: Duration::from_secs(300),
        }
    }

    /// `sum by (k0) (rate(some_metric[5m]))`, the most common query shape, over the same range.
    fn aggregate_eval_stmt() -> EvalStmt {
        EvalStmt {
            expr: parser::parse(&format!("sum by ({INSTANCE}) (rate({TABLE}[5m]))")).unwrap(),
            ..eval_stmt()
        }
    }

    /// One series of the mock region: its tag values in primary-key order, the storage partition
    /// that owns it, and its samples.
    struct FixtureSeries {
        tags: Vec<&'static str>,
        partition: usize,
        samples: Vec<(i64, f64)>,
    }

    /// The rows a mock region serves, and how they are split across storage partitions.
    ///
    /// Both scanners read from this one description: the ordinary scan serves the rows of each
    /// partition and the raw scan serves the series of each partition, so the two arms of every
    /// comparison see the same data under the same ownership.
    struct Fixture {
        tags: Vec<&'static str>,
        series: Vec<FixtureSeries>,
        partitions: usize,
    }

    impl fmt::Debug for Fixture {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("Fixture")
                .field("tags", &self.tags)
                .field("series", &self.series.len())
                .field("partitions", &self.partitions)
                .finish()
        }
    }

    impl fmt::Debug for FixtureSeries {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("FixtureSeries")
                .field("tags", &self.tags)
                .field("partition", &self.partition)
                .field("samples", &self.samples.len())
                .finish()
        }
    }

    impl Fixture {
        /// The series a scanner serving `partitions` partitions puts in `partition`.
        ///
        /// The fixture assigns each series to a storage partition, but a scanner is read with the
        /// partitions the planner prepared it for, which need not be the storage split: the fused
        /// path prepares the scan as one partition. Folding the assignment into the partition count
        /// in force keeps a collapsed scan serving every series, as a real scanner reading the
        /// merged ranges does.
        fn series_of(
            &self,
            partition: usize,
            partitions: usize,
        ) -> impl Iterator<Item = &FixtureSeries> {
            self.series
                .iter()
                .filter(move |series| series.partition % partitions == partition)
        }

        /// The rows of one partition of a scan served as `partitions` partitions, series by series,
        /// each in timestamp order.
        fn rows_of(&self, partition: usize, partitions: usize) -> Vec<(&[&'static str], i64, f64)> {
            self.series_of(partition, partitions)
                .flat_map(|series| {
                    series
                        .samples
                        .iter()
                        .map(move |(ts, value)| (series.tags.as_slice(), *ts, *value))
                })
                .collect()
        }
    }

    /// A region engine serving the fixture with both an ordinary and a raw-series scan.
    struct MockEngine {
        metadata: RegionMetadataRef,
        fixture: Arc<Fixture>,
        requests: Mutex<Vec<ScanRequest>>,
    }

    #[async_trait]
    impl RegionEngine for MockEngine {
        fn name(&self) -> &str {
            "MockEngine"
        }
        async fn handle_request(
            &self,
            _region_id: RegionId,
            _request: RegionRequest,
        ) -> Result<RegionResponse, BoxedError> {
            unimplemented!("the fused path never writes")
        }
        async fn handle_query(
            &self,
            _region_id: RegionId,
            request: ScanRequest,
        ) -> Result<RegionScannerRef, BoxedError> {
            self.requests.lock().unwrap().push(request);
            Ok(Box::new(MockScanner {
                // One empty range per storage partition: the fixture's split, not the request's.
                properties: ScannerProperties::new(
                    vec![Vec::new(); self.fixture.partitions],
                    false,
                    0,
                ),
                metadata: self.metadata.clone(),
                schema: self.metadata.schema.clone(),
                fixture: self.fixture.clone(),
            }))
        }
        async fn get_metadata(
            &self,
            _region_id: RegionId,
        ) -> Result<RegionMetadataRef, BoxedError> {
            Ok(self.metadata.clone())
        }
        fn region_statistic(&self, _region_id: RegionId) -> Option<RegionStatistic> {
            None
        }
        async fn get_committed_sequence(
            &self,
            _region_id: RegionId,
        ) -> Result<SequenceNumber, BoxedError> {
            Ok(SequenceNumber::default())
        }
        async fn stop(&self) -> Result<(), BoxedError> {
            Ok(())
        }
        fn set_region_role(&self, _: RegionId, _: RegionRole) -> Result<(), BoxedError> {
            unimplemented!()
        }
        async fn set_region_role_state_gracefully(
            &self,
            _: RegionId,
            _: SettableRegionRoleState,
        ) -> Result<SetRegionRoleStateResponse, BoxedError> {
            unimplemented!()
        }
        async fn sync_region(
            &self,
            _: RegionId,
            _: SyncRegionFromRequest,
        ) -> Result<SyncRegionFromResponse, BoxedError> {
            unimplemented!()
        }
        async fn remap_manifests(
            &self,
            _: RemapManifestsRequest,
        ) -> Result<RemapManifestsResponse, BoxedError> {
            unimplemented!()
        }
        fn role(&self, _region_id: RegionId) -> Option<RegionRole> {
            None
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[derive(Debug)]
    struct MockScanner {
        properties: ScannerProperties,
        metadata: RegionMetadataRef,
        schema: datatypes::schema::SchemaRef,
        fixture: Arc<Fixture>,
    }

    impl MockScanner {
        fn raw_schema(&self) -> Arc<SeriesSchema> {
            Arc::new(SeriesSchema {
                fields: vec![FieldSpec {
                    name: Arc::from(VALUE),
                }]
                .into(),
                eval_grid: None,
            })
        }
    }

    impl DisplayAs for MockScanner {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("MockScanner")
        }
    }

    #[async_trait]
    impl RegionScanner for MockScanner {
        fn name(&self) -> &str {
            "MockScanner"
        }
        fn properties(&self) -> &ScannerProperties {
            &self.properties
        }
        fn schema(&self) -> datatypes::schema::SchemaRef {
            self.schema.clone()
        }
        fn metadata(&self) -> RegionMetadataRef {
            self.metadata.clone()
        }
        fn prepare(&mut self, request: PrepareRequest) -> Result<(), BoxedError> {
            // A real scanner applies the ranges the planner hands it, and the fused path relies on
            // that: it collapses the scan to one partition to resolve the raw-series binding and
            // then restores the split it will actually run. Ignoring the ranges here would make
            // every fused build fail at the binding gate instead of exercising that path.
            self.properties.prepare(request);
            Ok(())
        }
        fn scan_partition(
            &self,
            _ctx: &QueryScanContext,
            _metrics: &ExecutionPlanMetricsSet,
            partition: usize,
        ) -> Result<common_recordbatch::SendableRecordBatchStream, BoxedError> {
            // The ordinary scan projects the entire schema, and a storage partition only holds
            // its own series, so each output partition reports its own rows.
            let rows = self
                .fixture
                .rows_of(partition, self.properties.partitions.len());
            let mut columns: Vec<ArrayRef> = (0..self.fixture.tags.len())
                .map(|tag| {
                    Arc::new(StringArray::from_iter_values(
                        rows.iter().map(|(tags, _, _)| tags[tag]),
                    )) as ArrayRef
                })
                .collect();
            columns.push(Arc::new(Float64Array::from_iter_values(
                rows.iter().map(|(_, _, value)| *value),
            )));
            columns.push(Arc::new(TimestampMillisecondArray::from_iter_values(
                rows.iter().map(|(_, ts, _)| *ts),
            )));
            let batch = DfRecordBatch::try_new(self.schema.arrow_schema().clone(), columns)
                .map_err(|e| {
                    BoxedError::new(common_error::ext::PlainError::new(
                        e.to_string(),
                        common_error::status_code::StatusCode::Internal,
                    ))
                })?;
            let df_stream = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                batch.schema(),
                futures::stream::iter(vec![Ok::<_, datafusion_common::DataFusionError>(batch)]),
            );
            let stream =
                common_recordbatch::adapter::RecordBatchStreamAdapter::try_new(Box::pin(df_stream))
                    .map_err(|e| {
                        BoxedError::new(common_error::ext::PlainError::new(
                            e.to_string(),
                            common_error::status_code::StatusCode::Internal,
                        ))
                    })?;
            Ok(Box::pin(stream))
        }
        fn raw_series_schema(
            &self,
            _source: &RawLabelSource,
        ) -> Result<Option<Arc<SeriesSchema>>, BoxedError> {
            Ok(Some(self.raw_schema()))
        }
        fn scan_series_partition(
            &self,
            _ctx: &QueryScanContext,
            _metrics: &ExecutionPlanMetricsSet,
            partition: usize,
            label_source: &RawLabelSource,
        ) -> Result<Option<SendableRawSeriesStream>, BoxedError> {
            let schema = self.raw_schema();
            // A `PerSeries` scan owns each series in exactly one partition, so a partition only
            // serves the series assigned to it. Serving them all from every partition would report
            // each series once per partition.
            let chunks: Vec<RawSeriesChunk> = self
                .fixture
                .series_of(partition, self.properties.partitions.len())
                .map(|series| {
                    let fields = SeriesFields::try_from_vec(vec![Samples::from_float(
                        Float64Array::from_iter_values(series.samples.iter().map(|(_, v)| *v)),
                    )])
                    .unwrap();
                    RawSeriesChunk::try_new(
                        &schema,
                        Buffer::from(
                            dense_primary_key(&series.tags, self.fixture.tags.len()).as_slice(),
                        ),
                        None,
                        RawTimestamps::try_new(
                            series
                                .samples
                                .iter()
                                .map(|(ts, _)| *ts)
                                .collect::<Vec<_>>()
                                .into(),
                        )
                        .unwrap(),
                        fields,
                        true,
                    )
                    .unwrap()
                })
                .collect();
            Ok(Some(Box::pin(SampleStream {
                schema,
                label_source: label_source.clone(),
                chunks: chunks.into_iter(),
            })))
        }
        fn raw_series_ordering(&self) -> SeriesOrdering {
            SeriesOrdering::Contiguous
        }
        fn has_predicate_without_region(&self) -> bool {
            false
        }
        fn add_dyn_filter_to_predicate(
            &mut self,
            filters: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
        ) -> Vec<bool> {
            filters.iter().map(|_| false).collect()
        }
        fn set_logical_region(&mut self, logical_region: bool) {
            self.properties.set_logical_region(logical_region);
        }
        fn set_query_load_region_id(&mut self, region_id: RegionId) {
            self.properties.set_query_load_region_id(region_id);
        }
    }

    /// Encodes one `String` primary key column the way the region's dense codec does: a presence
    /// byte, the byte-string marker, then the value padded to a block with its significant
    /// length.
    fn dense_string_column(value: &str, key: &mut Vec<u8>) {
        let bytes = value.as_bytes();
        key.extend_from_slice(&[1, u8::from(!bytes.is_empty())]);
        let mut written = 0;
        for chunk in bytes.chunks(8) {
            key.extend_from_slice(chunk);
            key.resize(key.len() + (8 - chunk.len()), 0);
            written += chunk.len();
            // 9 marks "more blocks follow", 1..=8 gives this last block's significant length.
            key.push(if written == bytes.len() {
                chunk.len() as u8
            } else {
                9
            });
        }
    }

    /// The dense primary key of one fixture series: its tags in table order, concatenated.
    fn dense_primary_key(tags: &[&str], expected: usize) -> Vec<u8> {
        assert_eq!(
            tags.len(),
            expected,
            "a fixture series must tag every key column"
        );
        let mut key = Vec::new();
        for tag in tags {
            dense_string_column(tag, &mut key);
        }
        key
    }

    struct SampleStream {
        schema: Arc<SeriesSchema>,
        label_source: RawLabelSource,
        chunks: std::vec::IntoIter<RawSeriesChunk>,
    }

    impl Stream for SampleStream {
        type Item = Result<RawSeriesChunk, BoxedError>;
        fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Ready(self.chunks.next().map(Ok))
        }
    }

    impl RawSeriesStream for SampleStream {
        fn schema(&self) -> &Arc<SeriesSchema> {
            &self.schema
        }
        fn label_source(&self) -> &RawLabelSource {
            &self.label_source
        }
        fn ordering(&self) -> SeriesOrdering {
            SeriesOrdering::Contiguous
        }
    }

    /// Replaces the logical scan's source with the mock region.
    fn with_mock_source(
        plan: datafusion_expr::LogicalPlan,
        provider: &DummyTableProvider,
    ) -> datafusion_expr::LogicalPlan {
        use datafusion_common::tree_node::{Transformed, TreeNode};
        let source: Arc<dyn datafusion::logical_expr::TableSource> =
            Arc::new(DefaultTableSource::new(Arc::new(provider.clone())));
        plan.transform_down(|node| match node {
            datafusion_expr::LogicalPlan::TableScan(mut scan) => {
                scan.source = source.clone();
                Ok(Transformed::yes(datafusion_expr::LogicalPlan::TableScan(
                    scan,
                )))
            }
            node => Ok(Transformed::no(node)),
        })
        .unwrap()
        .data
    }

    fn contains_fused(plan: &Arc<dyn ExecutionPlan>) -> bool {
        if plan.name() == "FusedSeriesExec" {
            return true;
        }
        plan.children().iter().any(|child| contains_fused(child))
    }

    /// The output partition count of the one fused node in `plan`.
    fn fused_partition_count(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
        if plan.name() == "FusedSeriesExec" {
            return Some(plan.properties().output_partitioning().partition_count());
        }
        plan.children()
            .iter()
            .find_map(|child| fused_partition_count(child))
    }

    /// Whether a fused node sits below an `AggregateExec` of the same plan.
    fn has_aggregate_above_fused(plan: &Arc<dyn ExecutionPlan>) -> bool {
        fn walk(node: &Arc<dyn ExecutionPlan>, aggregate_above: bool) -> bool {
            if node.name() == "FusedSeriesExec" {
                return aggregate_above;
            }
            let aggregate_above = aggregate_above || node.name() == "AggregateExec";
            node.children()
                .iter()
                .any(|child| walk(child, aggregate_above))
        }
        walk(plan, false)
    }

    fn describe(plan: &Arc<dyn ExecutionPlan>) -> String {
        format!(
            "{}",
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
        )
    }

    /// Plans, binds and physically plans one PromQL statement against `fixture`, returning both
    /// arms of the comparison the production path would choose: the ordinary plan and, when the
    /// flag is on, the plan the fused selection entry point returns.
    ///
    /// Neither arm is collected here, so a caller can inspect the plan before running it.
    async fn plan_arms(
        stmt: &EvalStmt,
        fixture: &Arc<Fixture>,
        native: bool,
    ) -> (
        QueryEngineState,
        datafusion_expr::LogicalPlan,
        Arc<dyn ExecutionPlan>,
        Option<Arc<dyn ExecutionPlan>>,
    ) {
        let state = engine_state(native, fixture.partitions);
        let logical = PromPlanner::stmt_to_plan(table_provider(), stmt, &state)
            .await
            .unwrap();
        let context = QueryEngineContext::new(state.session_state(), QueryContext::arc());
        let logical = state
            .optimize_by_extension_rules(logical, &context)
            .unwrap();

        // Bind the plan to the mock region, then plan physically with the engine's planner.
        let region_id = RegionId::new(1, 1);
        let metadata = fixture_region_metadata(region_id);
        let engine: RegionEngineRef = Arc::new(MockEngine {
            metadata: metadata.clone(),
            fixture: fixture.clone(),
            requests: Mutex::new(Vec::new()),
        });
        let dummy = DummyTableProvider::new(region_id, engine, metadata);
        let logical = with_mock_source(logical, &dummy);
        let session = state.session_state();
        let physical = session
            .query_planner()
            .create_physical_plan(&logical, &session)
            .await
            .unwrap();
        // Route through the same function production uses, gated by the same flag the state
        // carries. A change to either the routing or the flag handling fails these tests instead
        // of silently comparing the ordinary plan with itself.
        let selected = if state.experimental_native_promql() {
            try_select(&logical, Arc::clone(&physical)).await.unwrap()
        } else {
            Arc::clone(&physical)
        };
        // `try_select` returns the input unchanged when nothing matched, so "selected" means the
        // plan actually changed.
        let selected = (!Arc::ptr_eq(&selected, &physical)).then_some(selected);
        (state, logical, physical, selected)
    }

    #[tokio::test]
    async fn a_real_rate_query_runs_through_the_fused_node() {
        let fixture = Arc::new(one_series_fixture());
        let (state, logical, ordinary, selected) = plan_arms(&eval_stmt(), &fixture, true).await;
        let session = state.session_state();
        let fused = selected
            .unwrap_or_else(|| panic!("the rate query was not fused:\n{}", describe(&ordinary)));

        // Narrow down the gates so a failure says which one rejected the plan.
        let shape = super::FusedShape::discover(&logical).unwrap_or_else(|| {
            panic!(
                "the matcher rejected the logical plan:\n{}",
                logical.display_indent()
            )
        });
        let target = super::pick(&ordinary, &shape)
            .unwrap_or_else(|| panic!("the physical plan does not hold the fused subtree"));
        let (scan, _) = super::fused_scan(&target).unwrap_or_else(|| {
            panic!(
                "no single region scan under the fused subtree:\n{}",
                describe(&target)
            )
        });
        let built = shape.build(&scan, false).unwrap_or_else(|reason| {
            panic!(
                "the fused node rejected the shape at gate {reason:?}; scan schema {:?}",
                scan.schema()
            )
        });
        assert!(
            contains_fused(&built),
            "the built node is not a FusedSeriesExec:\n{}",
            describe(&built)
        );
        assert!(
            contains_fused(&fused),
            "the selected plan has no FusedSeriesExec:\n{}",
            describe(&fused)
        );
        // This fixture has one storage partition, so the collapse is a no-op here; the aggregated
        // test below is the one that plans the scan with two and proves they are merged.
        assert_eq!(
            fused_partition_count(&fused),
            Some(1),
            "the fused node reads the scan as one partition:\n{}",
            describe(&fused)
        );

        // The fused result must equal the ordinary plan's result over the same samples.
        let ordinary = datafusion::physical_plan::collect(ordinary, session.task_ctx())
            .await
            .unwrap();
        let fused_batches = datafusion::physical_plan::collect(fused, session.task_ctx())
            .await
            .unwrap();
        assert!(!fused_batches.is_empty(), "the fused plan produced no rows");

        let fused_rows = timestamped_rows(&fused_batches);
        let ordinary_rows = timestamped_rows(&ordinary);
        // The fused path must cover exactly the evaluated grid.
        assert_eq!(
            fused_rows.iter().map(|(ts, _)| *ts).collect::<Vec<_>>(),
            ordinary_rows.iter().map(|(ts, _)| *ts).collect::<Vec<_>>(),
            "the fused path must report the same steps as the ordinary plan"
        );
        assert!(!fused_rows.is_empty());
        // A counter rising 60 per 60 s has a constant rate, so every value must be the same
        // positive reading, and the two paths must agree to within the last bits of a float.
        let expected = 60.0 / 60_000.0 * 1_000.0;
        for (timestamp, value) in &fused_rows {
            assert!(
                close_to(*value, expected),
                "the fused rate at {timestamp} is not {expected}: {fused_rows:?}"
            );
        }
        for ((timestamp, value), (_, ordinary_value)) in fused_rows.iter().zip(&ordinary_rows) {
            assert!(
                close_to(*value, *ordinary_value),
                "the fused and ordinary rates at {timestamp} disagree: \
                 fused {value}, ordinary {ordinary_value}"
            );
        }
    }

    /// `sum by (instance) (rate(...))` over a four-tag table, two storage partitions and two
    /// instances that each own a series in both partitions.
    ///
    /// The four counters rise 1, 2, 3 and 4 per second, so a correct aggregation reports 3 for
    /// `instance=x` and 7 for `instance=y` at every step. Those sums cannot be reached by
    /// dropping a partition (which would give 1/3 or 2/4), by losing the tag subset (four
    /// groups), or by mixing the instances (which would report a constant 5).
    #[tokio::test]
    async fn a_real_aggregated_rate_query_runs_through_the_fused_node() {
        let fixture = Arc::new(aggregate_fixture());
        let (state, _, _, on) = plan_arms(&aggregate_eval_stmt(), &fixture, true).await;
        let (_, _, ordinary, off) = plan_arms(&aggregate_eval_stmt(), &fixture, false).await;
        let session = state.session_state();
        // The flag off must not even ask the fused selection; the flag on must answer with a plan.
        assert!(off.is_none(), "the flag-off arm selected a fused plan");
        let fused = on.unwrap_or_else(|| {
            panic!(
                "the aggregated rate query was not fused:\n{}",
                describe(&ordinary)
            )
        });

        // The flag off arm must be the ordinary plan: no fused node anywhere.
        assert!(
            !contains_fused(&ordinary),
            "the flag-off plan must not fuse:\n{}",
            describe(&ordinary)
        );
        // The flag on arm must fuse exactly once, and only for the range subtree: one logical
        // `sum by (instance)` becomes one partial and one final aggregate, but a second fused node
        // would mean the subtree was replaced twice.
        assert_eq!(
            count_fused(&fused),
            1,
            "the flag-on plan must hold exactly one FusedSeriesExec:\n{}",
            describe(&fused)
        );
        // The aggregate must still sit above the fused node and must still be split the way the
        // ordinary plan splits it: partial, repartition, final.
        assert!(
            has_aggregate_above_fused(&fused),
            "the fused node must keep the aggregate above it:\n{}",
            describe(&fused)
        );
        // This test is only covering the aggregate case while the ordinary plan still splits one
        // logical aggregate into a partial and a final one around a repartition. Pinning that here
        // means a DataFusion change that stops splitting it shows up as a failure rather than as a
        // test that quietly stopped covering the shape it was written for.
        let mut names = Vec::new();
        collect_names(&ordinary, &mut names);
        assert_eq!(
            names.iter().filter(|name| *name == "AggregateExec").count(),
            2,
            "one logical aggregate must become a partial and a final one:\n{}",
            describe(&ordinary)
        );
        assert!(
            names.iter().any(|name| name == "RepartitionExec"),
            "the partial and final aggregates must be separated by a repartition:\n{}",
            describe(&ordinary)
        );
        assert_eq!(
            aggregate_nodes(&ordinary),
            aggregate_nodes(&fused),
            "the swap changed the aggregation:\n{}",
            describe(&fused)
        );
        // Each instance owns one series in each storage partition, with slopes 1 and 2 per second
        // for `x` and 3 and 4 for `y`. Merged, `sum by (instance)` sees 3 and 7. Read per partition
        // it would see 1 and 3 in the first and 2 and 4 in the second, so a scan that did not merge
        // both partitions cannot produce 3 and 7.
        assert_eq!(
            fused_partition_count(&fused),
            Some(1),
            "the fused node must read both storage partitions as one:\n{}",
            describe(&fused)
        );

        // The folded node itself still emits every visible tag, and the aggregate above it is what
        // selects `instance`. Pinning this is the point of the test: the tag-subset semantics live
        // in the plan above the node, not in the node. A change that narrowed the node's schema
        // would break the parent's column references, and a change that made the node refuse a
        // tag-subset plan would stop these queries from fusing at all. Checking the final result
        // alone sees neither, because the aggregate narrows the columns on both arms.
        {
            let folded = find_node(&fused, "FusedSeriesExec")
                .unwrap_or_else(|| panic!("the fused plan lost its node:\n{}", describe(&fused)));
            let folded_schema = folded.schema();
            let folded_names: Vec<&str> = folded_schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect();
            assert_eq!(
                folded_names.len(),
                2 + TAGS.len(),
                "the folded node must emit its time column, its value column and every tag: \
                 {folded_names:?}"
            );
            assert!(
                folded_schema.column_with_name(TIME).is_some(),
                "the folded node lost its time column: {folded_names:?}"
            );
            for tag in TAGS {
                assert!(
                    folded_schema.column_with_name(tag).is_some(),
                    "the folded node must still carry tag `{tag}` for its parent to select from: \
                     {folded_names:?}"
                );
            }
        }

        let ordinary_batches = datafusion::physical_plan::collect(ordinary, session.task_ctx())
            .await
            .unwrap();
        let fused_batches = datafusion::physical_plan::collect(fused, session.task_ctx())
            .await
            .unwrap();
        assert!(!fused_batches.is_empty(), "the fused plan produced no rows");

        // The aggregate above the fold is what narrows the labels: both arms must end at
        // exactly the grouping label, never at a subset of it and never with a stray tag.
        for (arm, batches) in [("fused", &fused_batches), ("ordinary", &ordinary_batches)] {
            let schema = batches
                .first()
                .unwrap_or_else(|| panic!("the {arm} plan produced no rows"))
                .schema();
            let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            assert_eq!(
                names,
                vec![INSTANCE, TIME, schema.field(2).name().as_str()],
                "the {arm} arm's result must carry `{INSTANCE}`, `{TIME}` and one value: {names:?}"
            );
            for absent in TAGS.iter().filter(|tag| **tag != INSTANCE) {
                assert!(
                    schema.column_with_name(absent).is_none(),
                    "the {arm} arm still carries tag `{absent}`: {names:?}"
                );
            }
        }

        let fused_rows = instance_rows(&fused_batches);
        let ordinary_rows = instance_rows(&ordinary_batches);
        // The hand-computed answer: four steps (360, 420, 480, 540 s) for each of two instances.
        // Summing by instance across the two partitions gives x = 1 + 2 and y = 3 + 4.
        let expected: Vec<(String, i64, f64)> = ["x", "y"]
            .into_iter()
            .flat_map(|instance| {
                [360_000i64, 420_000, 480_000, 540_000]
                    .into_iter()
                    .map(move |ts| {
                        (
                            instance.to_string(),
                            ts,
                            if instance == "x" { 3.0 } else { 7.0 },
                        )
                    })
            })
            .collect();
        // Both arms are compared against the hand-computed answer in full. Comparing them only
        // with each other would pass if both were wrong the same way, and a `zip` against the
        // expectation would pass if either list were short.
        for (arm, rows) in [("fused", &fused_rows), ("ordinary", &ordinary_rows)] {
            assert_eq!(
                rows.len(),
                expected.len(),
                "the {arm} arm returned {} rows, expected {}: {rows:?}",
                rows.len(),
                expected.len()
            );
            for (actual, want) in rows.iter().zip(&expected) {
                assert_eq!(
                    (&actual.0, actual.1),
                    (&want.0, want.1),
                    "the {arm} arm's labels or step differ: {rows:?}"
                );
                assert!(
                    close_to(actual.2, want.2),
                    "the {arm} arm has {} at {} s = {}, expected {}: {rows:?}",
                    actual.0,
                    actual.1 / 1000,
                    actual.2,
                    want.2
                );
            }
        }
        // Equal lengths are established above, so this comparison cannot silently truncate.
        for (fused_row, ordinary_row) in fused_rows.iter().zip(&ordinary_rows) {
            assert_eq!(
                (&fused_row.0, fused_row.1),
                (&ordinary_row.0, ordinary_row.1)
            );
            assert!(
                close_to(fused_row.2, ordinary_row.2),
                "the fused and ordinary sums at {}@{} disagree: fused {}, ordinary {}",
                fused_row.0,
                fused_row.1,
                fused_row.2,
                ordinary_row.2
            );
        }
    }

    /// Two finite values are equal to within the last bits of a float; non-finite values must
    /// match exactly, since no tolerance can make two different infinities agree.
    fn close_to(actual: f64, expected: f64) -> bool {
        if !actual.is_finite() || !expected.is_finite() {
            return actual.to_bits() == expected.to_bits();
        }
        (actual - expected).abs() <= 1e-9 * expected.abs().max(1.0)
    }

    /// The number of fused nodes in `plan`.
    fn count_fused(plan: &Arc<dyn ExecutionPlan>) -> usize {
        usize::from(plan.name() == "FusedSeriesExec")
            + plan
                .children()
                .iter()
                .map(|child| count_fused(child))
                .sum::<usize>()
    }

    /// The first node of `plan` named `name`, searched depth first.
    fn find_node<'a>(
        plan: &'a Arc<dyn ExecutionPlan>,
        name: &str,
    ) -> Option<&'a Arc<dyn ExecutionPlan>> {
        if plan.name() == name {
            return Some(plan);
        }
        plan.children()
            .iter()
            .find_map(|child| find_node(child, name))
    }

    /// The names of every node in `plan`, in plan order.
    fn collect_names(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<String>) {
        out.push(plan.name().to_string());
        for child in plan.children() {
            collect_names(child, out);
        }
    }

    /// The value column of a batch: the one `Float64` field, whatever the projection named it.
    fn value_column(batch: &DfRecordBatch) -> &Float64Array {
        batch
            .column_by_name(
                batch
                    .schema()
                    .fields()
                    .iter()
                    .find(|field| {
                        field.data_type() == &datafusion::arrow::datatypes::DataType::Float64
                    })
                    .expect("every result has one float value column")
                    .name(),
            )
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
    }

    /// `(timestamp, value)` for every row, sorted by timestamp.
    fn timestamped_rows(batches: &[DfRecordBatch]) -> Vec<(i64, f64)> {
        let mut rows = Vec::new();
        for batch in batches {
            let timestamps = batch
                .column_by_name(TIME)
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let value = value_column(batch);
            for row in 0..batch.num_rows() {
                rows.push((timestamps.value(row), value.value(row)));
            }
        }
        rows.sort_by_key(|row| row.0);
        rows
    }

    /// `(instance, timestamp, value)` for every row of a `sum by (instance)` result, sorted.
    fn instance_rows(batches: &[DfRecordBatch]) -> Vec<(String, i64, f64)> {
        let mut rows = Vec::new();
        for batch in batches {
            let labels = batch
                .column_by_name(INSTANCE)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let timestamps = batch
                .column_by_name(TIME)
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let value = value_column(batch);
            for row in 0..batch.num_rows() {
                rows.push((
                    labels.value(row).to_string(),
                    timestamps.value(row),
                    value.value(row),
                ));
            }
        }
        rows.sort_by(|left, right| (left.0.as_str(), left.1).cmp(&(right.0.as_str(), right.1)));
        rows
    }

    /// The configuration of every `AggregateExec`, in plan order, as its display string.
    fn aggregate_nodes(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
        fn walk(node: &Arc<dyn ExecutionPlan>, out: &mut Vec<String>) {
            if node.name() == "AggregateExec" {
                out.push(
                    datafusion::physical_plan::displayable(node.as_ref())
                        .one_line()
                        .to_string(),
                );
            }
            for child in node.children() {
                walk(child, out);
            }
        }
        let mut out = Vec::new();
        walk(plan, &mut out);
        out
    }
}

#[cfg(test)]
mod tests {
    //! Two admission tests plus the helpers they need, built from the nodes `PromPlanner` emits.

    use std::sync::Arc;

    use datafusion::datasource::DefaultTableSource;
    use datafusion::logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder, col, lit};
    use datafusion_common::ScalarValue;
    use datafusion_expr::Expr;
    use datafusion_expr::expr::ScalarFunction;
    use datafusion_expr::expr_fn::is_not_null;
    use promql::extension_plan::{RangeManipulate, SeriesDivide, SeriesNormalize};
    use promql::functions::Rate;
    use store_api::storage::RegionId;

    use super::FusedShape;

    const TIME: &str = "ts";
    const VALUE: &str = "v0";
    const TAG: &str = "k0";

    /// `prom_rate(timestamp_range, v0, ts, Int64(300000))`, the value expression of the subtree.
    fn rate_call() -> Expr {
        Expr::ScalarFunction(ScalarFunction {
            func: Rate::scalar_udf().into(),
            args: vec![col("ts_range"), col(VALUE), col(TIME), lit(300_000_i64)],
        })
    }

    /// `Filter -> Projection -> PromRangeManipulate` over the mock region's scan.
    fn rate_plan(value_expr: Expr, value_name: &str) -> LogicalPlan {
        let provider = Arc::new(crate::optimizer::test_util::mock_table_provider(
            RegionId::new(1, 1),
        ));
        let scan = LogicalPlanBuilder::scan(
            "mock_metric",
            Arc::new(DefaultTableSource::new(provider)),
            None,
        )
        .unwrap()
        .build()
        .unwrap();
        let filtered = LogicalPlanBuilder::from(scan)
            .filter(col(TIME).gt_eq(lit(ScalarValue::TimestampMillisecond(Some(-299_999), None))))
            .unwrap()
            .build()
            .unwrap();
        let sorted = LogicalPlanBuilder::from(filtered)
            .sort(vec![col(TAG).sort(true, true), col(TIME).sort(true, true)])
            .unwrap()
            .build()
            .unwrap();
        let divide = LogicalPlan::Extension(Extension {
            node: Arc::new(SeriesDivide::new(
                vec![TAG.to_string()],
                TIME.to_string(),
                sorted,
            )),
        });
        let normalize = LogicalPlan::Extension(Extension {
            node: Arc::new(SeriesNormalize::new(
                0,
                TIME,
                true,
                vec![TAG.to_string()],
                divide,
            )),
        });
        let manipulate = RangeManipulate::new(
            0,
            100_000,
            5_000,
            0,
            300_000,
            TIME.to_string(),
            vec![VALUE.to_string()],
            normalize,
        )
        .unwrap();
        let projected = LogicalPlanBuilder::from(LogicalPlan::Extension(Extension {
            node: Arc::new(manipulate),
        }))
        .project(vec![col(TIME), value_expr.alias(value_name), col(TAG)])
        .unwrap()
        .build()
        .unwrap();
        LogicalPlanBuilder::from(projected)
            .filter(is_not_null(col(value_name)))
            .unwrap()
            .build()
            .unwrap()
    }

    #[test]
    fn a_bare_rate_shape_matches() {
        let name = format!("prom_rate(timestamp_range,{VALUE},{TIME},Int64(300000))");
        let shape = FusedShape::discover(&rate_plan(rate_call(), &name))
            .expect("the bare rate shape must match");
        assert_eq!(shape.field, VALUE);
        assert_eq!(shape.time_index, TIME);
        assert_eq!(shape.range, 300_000);
        assert_eq!(shape.interval, 5_000);
        assert_eq!(shape.output_schema.fields().len(), 3);
    }

    #[test]
    fn a_map_layer_above_the_rate_shape_does_not_match() {
        // `abs(rate(...))` wraps the call in a map kernel, which phase 1 does not fuse.
        let map = Expr::ScalarFunction(ScalarFunction {
            func: datafusion::functions::math::abs(),
            args: vec![rate_call()],
        });
        let plan = rate_plan(map, "abs_of_rate");
        assert!(
            FusedShape::discover(&plan).is_none(),
            "a map layer must fall back to the ordinary plan: {plan:?}"
        );
    }
}
