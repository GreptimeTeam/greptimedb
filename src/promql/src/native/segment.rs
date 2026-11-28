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

//! Segmenting a native tree, so each per-series prefix runs where its input is read.
//!
//! A tree that reads across series runs in one partition. Its per-series prefixes are cut out as
//! [`FusedSeries`] nodes of their own so they keep their scan partitions: `cut_as_segment` decides
//! what is cut, [`cut_segments`] rewrites the tree into one segment per cut, and [`segment_tree`]
//! builds the nodes.
//!
//! Output columns are the planner's answer, read through [`OutputTags`], [`FieldNames`] and
//! [`RootSchema`].

use std::sync::Arc;

use common_query::native_histogram::native_histogram_value_type;
use common_query::series_frame::raw_series_frame_schema_with_ordering;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, TimeUnit as ArrowTimeUnit};
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::logical_expr::{Extension, LogicalPlan};
use datatypes::data_type::DataType as _;
use datatypes::series::SeriesOrdering;
use datatypes::series::frame::{FrameContent, FrameLabelMode, SeriesFrameDeclaration};
use datatypes::series::time::EvalGrid;
use store_api::region_engine::RawLabelSource;

use crate::extension_plan::{
    FieldKind, FusedOutputFormat, FusedSeries, FusedSeriesBinding, InputLocality, RawSeriesSource,
    input_locality, tree_source_fields,
};
use crate::native::plan::native_tree_supported;
use crate::native::plan::{
    BoundInput, METRIC_NAME, NativeExpr, NativeNode, NativeTree, NativeType, reads_across_series,
};
use crate::vector_match::Cardinality;

/// The planner's output tag columns for a node, given each source's tag columns.
pub type OutputTags = fn(&NativeNode, &[Vec<String>]) -> Option<Vec<String>>;

/// The planner's value column names for the compiled `fields` of a statement's expression.
pub type FieldNames = fn(&str, &[String]) -> Vec<String>;

/// The planner's row schema for a statement root over `promql`, or `None` when its columns cannot
/// be named once.
pub type RootSchema = fn(&str, &NativeTree, &[Vec<String>], &str, FieldKind) -> Option<DFSchemaRef>;

/// Whether `node` evaluates each series on its own, directly over a scan, so one partitioned
/// execution is one answer.
pub fn pointwise_operand(node: &NativeNode) -> bool {
    match &node.expr {
        NativeExpr::Instant { input, .. } | NativeExpr::Window { input, .. } => {
            matches!(input.expr, NativeExpr::Input(_))
        }
        NativeExpr::Map { input, .. } | NativeExpr::Timestamp { input } => pointwise_operand(input),
        NativeExpr::ParameterizedMap {
            input, parameters, ..
        } => pointwise_operand(input) && parameters.iter().all(NativeNode::is_data_free),
        NativeExpr::Binary {
            left,
            right,
            matching,
        } => {
            let scalar = |side: &NativeNode| matches!(side.output, NativeType::Scalar(_));
            matching.cardinality == Cardinality::OneToOne
                && match (scalar(left), scalar(right)) {
                    (true, false) => left.is_data_free() && pointwise_operand(right),
                    (false, true) => right.is_data_free() && pointwise_operand(left),
                    _ => false,
                }
        }
        NativeExpr::Input(_)
        | NativeExpr::Scalar(_)
        | NativeExpr::EvalTime { .. }
        | NativeExpr::VectorOf { .. }
        | NativeExpr::ScalarOf { .. }
        | NativeExpr::ParameterizedTopK { .. }
        | NativeExpr::ParameterizedHistogram { .. }
        | NativeExpr::ParameterizedAggregate { .. }
        | NativeExpr::Aggregate { .. }
        | NativeExpr::Vector { .. }
        | NativeExpr::Set { .. }
        | NativeExpr::Merge { .. }
        | NativeExpr::Info { .. } => false,
    }
}

pub fn independent_scan_pair(root: &NativeNode) -> bool {
    let (left, right) = match &root.expr {
        NativeExpr::Binary { left, right, .. } => match (&left.expr, &right.expr) {
            (NativeExpr::Scalar(_), NativeExpr::Scalar(_)) => return false,
            (NativeExpr::Scalar(_), _) => return independent_scan_pair(right),
            (_, NativeExpr::Scalar(_)) => return independent_scan_pair(left),
            _ => (left, right),
        },
        NativeExpr::Map { input, .. } => return independent_scan_pair(input),
        NativeExpr::Set { left, right, .. } => {
            return scan_operand(left) && scan_operand(right);
        }
        NativeExpr::Info { base, info, .. } => {
            return pointwise_operand(base) && pointwise_operand(info);
        }
        _ => return false,
    };

    scan_operand(left) && scan_operand(right)
}

fn scan_operand(node: &NativeNode) -> bool {
    pointwise_operand(node) || independent_scan_pair(node)
}

/// Whether `node` answers an evaluated vector, the only thing a segment boundary carries.
fn exportable(node: &NativeNode) -> bool {
    node.output
        .samples()
        .is_some_and(|schema| schema.eval_grid.is_some())
}

/// Whether `node` is a window over an evaluated input (a subquery).
fn evaluated_input_window(node: &NativeNode) -> bool {
    matches!(&node.expr, NativeExpr::Window { op, .. } if op.input_grid.is_some())
}

/// Whether the dist analyzer places a segment over `node` below the `MergeScan`.
///
/// Mirrors the tree-shape check of the `FusedSeries` arm of `Categorizer::check_extension_plan`
/// (`src/query/src/dist_plan/commutativity.rs`). A segment left above the merge has no scan the
/// physical planner can read, so `cut_as_segment` must not cut it.
pub fn region_local_segment(node: &NativeNode) -> bool {
    !reads_across_series(node) || pointwise_operand(node) || independent_scan_pair(node)
}

/// Whether `node` becomes a segment of its own under a parent that must gather.
///
/// Local prefixes, global consumers and canonical merges are cut separately, so a prefix keeps its
/// scan width and every global consumer sees the canonical boundary below it. A pointwise wrapper
/// of a global consumer is cut as well, so it cannot hide that consumer from recursive lowering.
fn cut_as_segment(node: &NativeNode, canonical_segmentation: bool) -> bool {
    // Where the analyzer will not place a subquery window region-locally, the per-series prefix
    // inside it is cut instead.
    if evaluated_input_window(node) && !region_local_segment(node) {
        return false;
    }
    // A subquery window answers per series on its own grid, whatever its locality reports.
    let evaluated_window = evaluated_input_window(node);
    if !exportable(node)
        || matches!(node.expr, NativeExpr::Input(_))
        || (!canonical_segmentation
            && !evaluated_window
            && matches!(input_locality(node), InputLocality::SinglePartition))
    {
        return false;
    }
    if canonical_segmentation && (contains_global_consumer(node) || is_canonical_cut(node)) {
        return true;
    }
    // Data-free subtrees stay with their parent.
    let mut sources = Vec::new();
    collect_segment_inputs(node, &mut sources);
    !sources.is_empty()
}

/// Whether the merge producer's entire prefix can run on each scan partition independently.
pub fn canonical_local_prefix(node: &NativeNode) -> bool {
    match &node.expr {
        NativeExpr::Input(_) => true,
        NativeExpr::Instant { input, .. } | NativeExpr::Window { input, .. } => {
            matches!(input.expr, NativeExpr::Input(_))
        }
        NativeExpr::Map { input, .. } | NativeExpr::Timestamp { input } => {
            canonical_local_prefix(input)
        }
        NativeExpr::ParameterizedMap {
            input, parameters, ..
        } => canonical_local_prefix(input) && parameters.iter().all(NativeNode::is_data_free),
        NativeExpr::Binary { left, right, .. } => {
            (left.is_data_free() && canonical_local_prefix(right))
                || (right.is_data_free() && canonical_local_prefix(left))
        }
        _ => false,
    }
}

pub fn contains_canonical_merge(node: &NativeNode) -> bool {
    node.any(&|node| matches!(node.expr, NativeExpr::Merge { .. }))
}

pub fn contains_global_consumer(node: &NativeNode) -> bool {
    node.any(&is_global_consumer)
}

/// Whole-vector consumers stay global, but are cut out as evaluated segments so a pointwise parent
/// does not inherit their single-partition placement.
fn is_global_consumer(node: &NativeNode) -> bool {
    matches!(
        node.expr,
        NativeExpr::Aggregate { .. }
            | NativeExpr::ParameterizedAggregate { .. }
            | NativeExpr::Vector { .. }
    ) && matches!(input_locality(node), InputLocality::SinglePartition)
}

/// Whether a subtree can be cut without inheriting its parent's global gather: a local prefix, or
/// a canonical merge that becomes its own routed boundary.
fn is_canonical_cut(node: &NativeNode) -> bool {
    matches!(node.expr, NativeExpr::Merge { .. })
        || !matches!(input_locality(node), InputLocality::SinglePartition)
        || canonical_local_prefix(node)
}

/// One segment: its tree rewritten to its own index space, the scans it reads, and the subtrees cut
/// out of it.
///
/// `FusedSeries` requires dense source indices, so a segment numbers its scans first, in the order
/// it first names them, then its cut subtrees.
pub struct Segment {
    pub node: NativeNode,
    /// Original scan indices, in the order this segment's indices name them.
    pub sources: Vec<usize>,
    /// The subtrees cut out of this segment, each still in the original index space.
    pub cut: Vec<NativeNode>,
}

/// Collects a segment's scans and cut subtrees, without descending into a cut.
fn collect_segment(
    node: &NativeNode,
    sources: &mut Vec<usize>,
    cut: &mut Vec<NativeNode>,
    canonical_segmentation: bool,
) {
    match &node.expr {
        NativeExpr::Input(input) => {
            if !sources.contains(&input.source_index) {
                sources.push(input.source_index);
            }
        }
        expr => {
            for child in expr.children() {
                if cut_as_segment(child, canonical_segmentation) {
                    cut.push(child.clone());
                } else {
                    collect_segment(child, sources, cut, canonical_segmentation);
                }
            }
        }
    }
}

/// Collects every scan input in a selected segment without looking for nested cuts.
fn collect_segment_inputs(node: &NativeNode, sources: &mut Vec<usize>) {
    for input in node.inputs() {
        if !sources.contains(&input.source_index) {
            sources.push(input.source_index);
        }
    }
}

/// Rewrites every input leaf in a selected segment to its index in the segment's own space.
fn rewrite_inner_segment(node: &NativeNode, sources: &[usize]) -> NativeNode {
    let expr = match &node.expr {
        NativeExpr::Input(input) => NativeExpr::Input(BoundInput {
            source_index: sources
                .iter()
                .position(|&source| source == input.source_index)
                .expect("the segment collected every scan it reads"),
            output: input.output.clone(),
            ordering: input.ordering,
        }),
        expr => expr.with_children(
            expr.children()
                .into_iter()
                .map(|child| rewrite_inner_segment(child, sources))
                .collect(),
        ),
    };
    NativeNode {
        expr,
        output: node.output.clone(),
    }
}

/// Rewrites a segment's tree so every leaf names its index in the segment's own space.
fn rewrite_segment(
    node: &NativeNode,
    sources: &[usize],
    cut_base: usize,
    next_cut: &mut usize,
    canonical_segmentation: bool,
) -> NativeNode {
    let expr = match &node.expr {
        NativeExpr::Input(input) => NativeExpr::Input(BoundInput {
            source_index: sources
                .iter()
                .position(|&source| source == input.source_index)
                .expect("the segment collected every scan it reads"),
            output: input.output.clone(),
            ordering: input.ordering,
        }),
        expr => {
            let children = expr
                .children()
                .into_iter()
                .map(|child| {
                    if cut_as_segment(child, canonical_segmentation) {
                        let index = cut_base + *next_cut;
                        *next_cut += 1;
                        NativeNode {
                            expr: NativeExpr::Input(BoundInput {
                                source_index: index,
                                output: child.output.clone(),
                                ordering: NATIVE_ORDERING,
                            }),
                            output: child.output.clone(),
                        }
                    } else {
                        rewrite_segment(child, sources, cut_base, next_cut, canonical_segmentation)
                    }
                })
                .collect::<Vec<_>>();
            expr.with_children(children)
        }
    };
    NativeNode {
        expr,
        output: node.output.clone(),
    }
}

/// Splits `node` into one segment and the subtrees cut out of it.
pub fn cut_segments(node: &NativeNode, canonical_segmentation: bool) -> Segment {
    let mut sources = Vec::new();
    let mut cut = Vec::new();
    collect_segment(node, &mut sources, &mut cut, canonical_segmentation);
    let cut_base = sources.len();
    let mut next_cut = 0;
    let rewritten = rewrite_segment(
        node,
        &sources,
        cut_base,
        &mut next_cut,
        canonical_segmentation,
    );
    Segment {
        node: rewritten,
        sources,
        cut,
    }
}

/// Moves or appends `__name__` to the end of a rows layout's tag list. A scalar root gets no name
/// column.
pub fn with_row_name_column(node: &NativeNode, mut tags: Vec<String>) -> Vec<String> {
    if !matches!(&node.output, NativeType::Samples(_)) {
        return tags;
    }
    if let Some(index) = tags.iter().position(|tag| tag == METRIC_NAME) {
        tags.remove(index);
    }
    tags.push(METRIC_NAME.to_string());
    tags
}

/// The ordering trees are compiled against: the weaker of a native scan's `KeyThenTime` and a
/// two-phase scan's `Contiguous`, since the logical planner cannot tell which a statement gets.
pub const NATIVE_ORDERING: SeriesOrdering = SeriesOrdering::Contiguous;

/// The output schema of a fused node: a non-nullable millisecond time index, one nullable column
/// of `field_kind` per field, then nullable `Utf8` tags. Value columns are named by the caller's
/// [`FieldNames`].
pub fn fused_output_schema(
    fields: &[String],
    tags: &[String],
    time_index: &str,
    field_kind: FieldKind,
) -> datafusion::common::Result<DFSchemaRef> {
    let field_type = match field_kind {
        FieldKind::Float => ArrowDataType::Float64,
        FieldKind::Histogram => native_histogram_value_type().as_arrow_type(),
    };
    let mut columns = Vec::with_capacity(fields.len() + tags.len() + 1);
    // Marked as the existing planner marks it, for `RANGE` queries and schema conversion.
    columns.push((
        None,
        Arc::new(
            Field::new(
                time_index,
                ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                false,
            )
            .with_metadata(std::collections::HashMap::from([(
                datatypes::schema::TIME_INDEX_KEY.to_string(),
                "true".to_string(),
            )])),
        ),
    ));
    for name in fields {
        columns.push((None, Arc::new(Field::new(name, field_type.clone(), true))));
    }
    for name in tags {
        let mut field = Field::new(name, ArrowDataType::Utf8, true);
        // Tells the response layer each row carries its own name.
        if name == METRIC_NAME {
            field = field.with_metadata(datatypes::schema::metric_name_field_metadata());
        }
        columns.push((None, Arc::new(field)));
    }

    let metadata = std::collections::HashMap::from([(
        datatypes::schema::VERSION_KEY.to_string(),
        datatypes::schema::Schema::INITIAL_VERSION.to_string(),
    )]);
    Ok(Arc::new(DFSchema::new_with_metadata(columns, metadata)?))
}

/// The grid a `FusedSeries` publishes on: its root's grid, which inside a subquery is the
/// subquery's grid, or `statement_grid` for a root without one.
fn fused_publish_grid(root: &NativeNode, statement_grid: &Arc<EvalGrid>) -> Arc<EvalGrid> {
    root.output
        .grid()
        .map_or_else(|| Arc::clone(statement_grid), Arc::clone)
}

/// Lowers a tree whose root is a canonical merge into a producer and a canonical router, and
/// returns the plan with the tags its frames carry.
///
/// - The producer runs the merge on every scan partition, so a same-partition collision fails
///   there, and exports one series per label set as a frame.
/// - The router is `SeriesRepartitionExec`'s canonical routing: it publishes a label set once every
///   producer contributed it or ended, and reports a second piece holding a taken position.
///
/// `format` is what the caller reads: rows for a statement root, frames otherwise. `None` when the
/// boundary cannot be built, so the caller keeps its plan.
#[allow(clippy::too_many_arguments)]
pub fn canonical_boundary_stages(
    promql: Arc<str>,
    tree: &NativeTree,
    children: &[LogicalPlan],
    source_tags: &[Vec<String>],
    grid: &Arc<EvalGrid>,
    time_index: &str,
    tags: &[String],
    metric: &Option<Arc<str>>,
    format: FusedOutputFormat,
    root_kind: FieldKind,
    output_tags_of: OutputTags,
    field_names: FieldNames,
) -> Option<(LogicalPlan, Vec<String>)> {
    let NativeExpr::Merge { input } = &tree.root().expr else {
        return None;
    };
    if !canonical_local_prefix(input) {
        return None;
    }
    let NativeType::Samples(schema) = &tree.root().output else {
        return None;
    };
    schema.eval_grid.as_ref()?;
    let fields = schema
        .fields
        .iter()
        .map(|field| field.name.to_string())
        .collect::<Vec<_>>();
    let output_tags = output_tags_of(tree.root(), source_tags)?;
    let row_tags = with_row_name_column(tree.root(), output_tags.clone());
    // A column-name collision is refused, so the caller keeps its plan.
    let value_names = field_names(&promql, &fields);
    let output_schema = fused_output_schema(&value_names, &row_tags, time_index, root_kind).ok()?;

    let producer = FusedSeries::try_new_canonical_producer(
        Arc::clone(&promql),
        tree.clone(),
        FusedSeriesBinding {
            grid: Arc::clone(grid),
            time_index: time_index.to_string(),
            tags: tags.to_vec(),
            metric_name: metric.clone(),
        },
        children.to_vec(),
    )
    .ok()?;
    let producer = LogicalPlan::Extension(Extension {
        node: Arc::new(producer),
    });

    // A consumer above reads the routed frames as an ordinary evaluated input; the statement root
    // converts them to rows.
    if format == FusedOutputFormat::SeriesFrame {
        return Some((producer, output_tags));
    }
    let evaluated = NativeType::Samples(Arc::clone(schema));
    let consumer_tree = NativeTree::try_new(NativeNode {
        expr: NativeExpr::Input(BoundInput {
            source_index: 0,
            output: evaluated.clone(),
            ordering: NATIVE_ORDERING,
        }),
        output: evaluated,
    })
    .ok()?;
    // A frame names each series' own metric, so the binding names one only when a tag column does.
    let consumer_metric = if output_tags.iter().any(|tag| tag == METRIC_NAME) {
        metric.clone()
    } else {
        None
    };
    let consumer = FusedSeries::try_new_with_format(
        Arc::clone(&promql),
        consumer_tree,
        FusedSeriesBinding {
            grid: Arc::clone(grid),
            time_index: time_index.to_string(),
            tags: output_tags.clone(),
            metric_name: consumer_metric,
        },
        vec![producer],
        output_schema,
        FusedOutputFormat::PlainRows,
    )
    .ok()?;
    Some((
        LogicalPlan::Extension(Extension {
            node: Arc::new(consumer),
        }),
        output_tags,
    ))
}

fn is_raw_series_source(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Extension(extension) => extension.node.as_any().is::<RawSeriesSource>(),
        _ => false,
    }
}

/// Whether `plan` is a discovered selector's [`RawSeriesSource`], or the `Union` of several. It is
/// used as is: wrapping it in another source is refused by the physical planner.
fn is_sourced_plan(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Union(_)) || is_raw_series_source(plan)
}

fn find_input_node(node: &NativeNode, source_index: usize) -> Option<&BoundInput> {
    node.inputs()
        .into_iter()
        .find(|input| input.source_index == source_index)
}

/// Whether `plan` is the typed empty input of a discovered selector that matched no table: an
/// `EmptyRelation` declaring the raw explicit-label frame.
pub fn is_empty_discovered_input(plan: &LogicalPlan) -> bool {
    let LogicalPlan::EmptyRelation(relation) = plan else {
        return false;
    };
    if relation.produce_one_row {
        return false;
    }
    let Ok(declaration) = SeriesFrameDeclaration::parse(relation.schema.as_arrow()) else {
        return false;
    };
    if declaration.content() != FrameContent::Raw
        || declaration.labels() != FrameLabelMode::Explicit
    {
        return false;
    }
    let Ok(frame) = raw_series_frame_schema_with_ordering(
        declaration.series(),
        &RawLabelSource::Explicit,
        NATIVE_ORDERING,
    ) else {
        return false;
    };
    relation.schema.as_arrow() == frame.as_ref()
}

#[allow(clippy::too_many_arguments, clippy::type_complexity)]
fn build_evaluated_segment(
    promql: Arc<str>,
    node: &NativeNode,
    children: &[LogicalPlan],
    source_tags: &[Vec<String>],
    source_ids: &[Option<usize>],
    per_source: &[(Option<Arc<str>>, String, Vec<String>)],
    grid: &Arc<EvalGrid>,
    time_index: &str,
    root_kind: FieldKind,
    output_tags_of: OutputTags,
    field_names: FieldNames,
) -> Option<(LogicalPlan, Vec<String>)> {
    let mut sources = Vec::new();
    collect_segment_inputs(node, &mut sources);
    if sources.is_empty() {
        return None;
    }
    let mut inputs = Vec::with_capacity(sources.len());
    let mut local_tags = Vec::with_capacity(sources.len());
    let tree = NativeTree::try_new(rewrite_inner_segment(node, &sources)).ok()?;
    let source_fields = tree_source_fields(&tree).ok()?;
    for (local_index, &index) in sources.iter().enumerate() {
        let child = children.get(index)?.clone();
        let input = find_input_node(node, index)?;
        if input
            .output
            .samples()
            .is_some_and(|schema| schema.eval_grid.is_some())
            || is_sourced_plan(&child)
            || is_empty_discovered_input(&child)
        {
            inputs.push(child);
            local_tags.push(source_tags.get(index)?.clone());
        } else {
            let original_index = *source_ids.get(index)?.as_ref()?;
            let (Some((Some(metric), _, tags)), Some(fields)) = (
                per_source.get(original_index),
                source_fields.get(local_index),
            ) else {
                return None;
            };
            inputs.push(LogicalPlan::Extension(Extension {
                node: Arc::new(
                    RawSeriesSource::try_new(
                        Arc::clone(metric),
                        tags.clone(),
                        fields.clone(),
                        child,
                    )
                    .ok()?,
                ),
            }));
            local_tags.push(tags.clone());
        }
    }
    if matches!(tree.root().expr, NativeExpr::Merge { .. }) {
        let (plan, tags) = canonical_boundary_stages(
            Arc::clone(&promql),
            &tree,
            &inputs,
            &local_tags,
            grid,
            time_index,
            &[],
            &None,
            FusedOutputFormat::SeriesFrame,
            root_kind,
            output_tags_of,
            field_names,
        )?;
        return Some((plan, tags));
    }
    let output_tags = output_tags_of(tree.root(), &local_tags)?;
    if !native_tree_supported(&tree) {
        return None;
    }
    let frame = FusedSeries::series_frame_schema(&tree).ok()?;
    let frame = DFSchema::try_from(frame.as_ref().clone()).ok()?;
    let publish_grid = fused_publish_grid(tree.root(), grid);
    let plan = FusedSeries::try_new_with_format(
        Arc::from("segment"),
        tree,
        FusedSeriesBinding {
            grid: publish_grid,
            time_index: time_index.to_string(),
            tags: output_tags.clone(),
            metric_name: None,
        },
        inputs,
        Arc::new(frame),
        FusedOutputFormat::SeriesFrame,
    )
    .ok()?;
    Some((
        LogicalPlan::Extension(Extension {
            node: Arc::new(plan),
        }),
        output_tags,
    ))
}

/// Builds one segment's plan from a subtree without cutting again, and returns it with its output
/// tags. `None` when any part cannot be built.
#[allow(clippy::type_complexity)]
fn build_inner_segment(
    node: &NativeNode,
    bare_scans: &[LogicalPlan],
    per_source: &[(Option<Arc<str>>, String, Vec<String>)],
    grid: &Arc<EvalGrid>,
    output_tags_of: OutputTags,
) -> Option<(LogicalPlan, Vec<String>)> {
    let mut sources = Vec::new();
    collect_segment_inputs(node, &mut sources);
    if sources.is_empty() {
        return None;
    }
    let scans = sources
        .iter()
        .map(|&index| bare_scans.get(index).cloned())
        .collect::<Option<Vec<_>>>()?;
    let source_tags = sources
        .iter()
        .map(|&index| per_source.get(index).map(|(_, _, tags)| tags.clone()))
        .collect::<Option<Vec<_>>>()?;
    let tree = NativeTree::try_new(rewrite_inner_segment(node, &sources)).ok()?;
    let output_tags = output_tags_of(tree.root(), &source_tags)?;
    let segment_grid = fused_publish_grid(tree.root(), grid);
    let (inputs, metric, time_index, tags) = if let [index] = sources.as_slice() {
        let (metric, time_index, tags) = per_source.get(*index)?;
        (scans, metric.clone(), time_index.clone(), tags.clone())
    } else {
        // One binding cannot describe several sources, so each child gets its own
        // explicit-label source.
        let source_fields = tree_source_fields(&tree).ok()?;
        let mut children = Vec::with_capacity(sources.len());
        for (local_index, &source_index) in sources.iter().enumerate() {
            let scan = scans.get(local_index)?;
            if is_sourced_plan(scan) {
                children.push(scan.clone());
                continue;
            }
            let (Some((Some(metric), _, source_tags)), Some(fields)) =
                (per_source.get(source_index), source_fields.get(local_index))
            else {
                return None;
            };
            children.push(LogicalPlan::Extension(Extension {
                node: Arc::new(
                    RawSeriesSource::try_new(
                        Arc::clone(metric),
                        source_tags.clone(),
                        fields.clone(),
                        scan.clone(),
                    )
                    .ok()?,
                ),
            }));
        }
        let time_index = per_source.get(*sources.first()?)?.1.clone();
        (children, None, time_index, output_tags.clone())
    };
    if !native_tree_supported(&tree) {
        return None;
    }
    let frame = FusedSeries::series_frame_schema(&tree).ok()?;
    let frame = DFSchema::try_from(frame.as_ref().clone()).ok()?;
    let plan = FusedSeries::try_new_with_format(
        Arc::from("segment"),
        tree,
        FusedSeriesBinding {
            grid: segment_grid,
            time_index: time_index.clone(),
            tags: tags.clone(),
            metric_name: metric.clone(),
        },
        inputs,
        Arc::new(frame),
        FusedOutputFormat::SeriesFrame,
    )
    .ok()?;
    Some((
        LogicalPlan::Extension(Extension {
            node: Arc::new(plan),
        }),
        output_tags,
    ))
}

/// Plans `tree` as several segments, or `None` when there is nothing to cut or a segment cannot be
/// built; the caller then keeps the unsegmented plan.
///
/// Nested segments publish frames for their parents; only the statement root publishes rows, named
/// through [`FieldNames`] and [`RootSchema`].
#[allow(clippy::too_many_arguments, clippy::type_complexity)]
pub fn segment_tree(
    promql: Arc<str>,
    tree: &NativeTree,
    children: &[LogicalPlan],
    source_tags: &[Vec<String>],
    bare_scans: &[LogicalPlan],
    per_source: &[(Option<Arc<str>>, String, Vec<String>)],
    grid: &Arc<EvalGrid>,
    time_index: &str,
    tags: &[String],
    metric: &Option<Arc<str>>,
    format: FusedOutputFormat,
    canonical_segmentation: bool,
    root_kind: FieldKind,
    remote_raw_boundary: bool,
    output_tags_of: OutputTags,
    field_names: FieldNames,
    root_schema: RootSchema,
) -> Option<LogicalPlan> {
    // A subquery window root may read across series even where `input_locality` reports a local
    // tree, so it is attempted too; `cut_as_segment` admits each cut.
    if !matches!(input_locality(tree.root()), InputLocality::SinglePartition)
        && !evaluated_input_window(tree.root())
    {
        return None;
    }
    let mut current_tree = tree.clone();
    let mut current_children = children.to_vec();
    let mut current_tags = source_tags.to_vec();
    let mut current_source_ids = (0..children.len()).map(Some).collect::<Vec<_>>();
    let mut first_iteration = true;
    let mut changed = false;
    loop {
        if !matches!(
            input_locality(current_tree.root()),
            InputLocality::SinglePartition
        ) && !evaluated_input_window(current_tree.root())
        {
            break;
        }
        let segment = cut_segments(current_tree.root(), canonical_segmentation);
        if segment.cut.is_empty() {
            break;
        }
        let mut inputs = Vec::with_capacity(segment.sources.len() + segment.cut.len());
        let mut child_tags = Vec::with_capacity(inputs.capacity());
        for &index in &segment.sources {
            let child = current_children.get(index)?;
            inputs.push(child.clone());
            child_tags.push(current_tags.get(index).cloned().unwrap_or_default());
        }
        let mut child_source_ids = segment
            .sources
            .iter()
            .map(|&index| current_source_ids.get(index).copied().flatten())
            .collect::<Vec<_>>();
        for cut in &segment.cut {
            // A cut wrapping a global consumer is segmented recursively, so its inner canonical
            // boundary stays below the consumer.
            let canonical = matches!(cut.expr, NativeExpr::Merge { .. });
            let built = if first_iteration && contains_global_consumer(cut) {
                let cut_tree = NativeTree::try_new(cut.clone()).ok()?;
                let plan = segment_tree(
                    Arc::clone(&promql),
                    &cut_tree,
                    &current_children,
                    &current_tags,
                    bare_scans,
                    per_source,
                    grid,
                    time_index,
                    tags,
                    metric,
                    FusedOutputFormat::SeriesFrame,
                    canonical_segmentation,
                    root_kind,
                    remote_raw_boundary,
                    output_tags_of,
                    field_names,
                    root_schema,
                );
                match plan {
                    Some(plan) => {
                        let cut_tags = output_tags_of(cut, &current_tags)?;
                        Some((plan, cut_tags))
                    }
                    None => None,
                }
            } else if first_iteration && !canonical && !remote_raw_boundary {
                build_inner_segment(cut, bare_scans, per_source, grid, output_tags_of)
            } else {
                None
            };
            if built.is_none() && contains_global_consumer(cut) {
                return None;
            }
            // Otherwise the cut is built over the frames this loop has already published.
            let built = built.or_else(|| {
                build_evaluated_segment(
                    Arc::clone(&promql),
                    cut,
                    &current_children,
                    &current_tags,
                    &current_source_ids,
                    per_source,
                    grid,
                    time_index,
                    root_kind,
                    output_tags_of,
                    field_names,
                )
            });
            let (plan, cut_tags) = built?;
            inputs.push(plan);
            child_tags.push(cut_tags);
            child_source_ids.push(None);
        }
        let parent = NativeTree::try_new(segment.node).ok()?;
        first_iteration = false;
        current_tree = parent;
        current_children = inputs;
        current_tags = child_tags;
        current_source_ids = child_source_ids;
        changed = true;
    }
    if !changed {
        return None;
    }
    let parent = current_tree;
    let mut inputs = current_children;
    let mut child_tags = current_tags;
    // A merge at this segment's root still needs its own boundary.
    if let NativeExpr::Merge { input } = &parent.root().expr {
        let source_fields = tree_source_fields(&parent).ok();
        for index in 0..inputs.len() {
            let bound = find_input_node(input, index)?;
            if bound
                .output
                .samples()
                .is_some_and(|schema| schema.eval_grid.is_none())
                && !is_raw_series_source(&inputs[index])
            {
                let source_index = current_source_ids.get(index).copied().flatten()?;
                let (Some((Some(source_metric), _, source_tags)), Some(fields)) = (
                    per_source.get(source_index),
                    source_fields.as_ref().and_then(|fields| fields.get(index)),
                ) else {
                    return None;
                };
                let Ok(source) = RawSeriesSource::try_new(
                    Arc::clone(source_metric),
                    source_tags.clone(),
                    fields.clone(),
                    inputs[index].clone(),
                ) else {
                    return None;
                };
                inputs[index] = LogicalPlan::Extension(Extension {
                    node: Arc::new(source),
                });
                child_tags[index] = source_tags.clone();
            }
        }
        return canonical_boundary_stages(
            Arc::clone(&promql),
            &parent,
            &inputs,
            &child_tags,
            grid,
            time_index,
            tags,
            metric,
            format,
            root_kind,
            output_tags_of,
            field_names,
        )
        .map(|(plan, _)| plan);
    }
    // Physical planning cannot fall back, so refuse here what the runtime would refuse.
    if !native_tree_supported(&parent) {
        return None;
    }
    let output_schema = match format {
        FusedOutputFormat::PlainRows => {
            root_schema(&promql, &parent, &child_tags, time_index, root_kind)?
        }
        FusedOutputFormat::SeriesFrame => {
            let Ok(frame) = FusedSeries::series_frame_schema(&parent) else {
                return None;
            };
            let Ok(schema) = DFSchema::try_from(frame.as_ref().clone()) else {
                return None;
            };
            Arc::new(schema)
        }
    };
    // A recursive segment keeps its own source's metric; its parent's would relabel both inputs.
    let segment_binding = |source_ids: &[Option<usize>]| -> (Option<Arc<str>>, String) {
        let mut sources = source_ids.iter().flatten().copied();
        let Some(source) = sources.next() else {
            return (metric.clone(), time_index.to_string());
        };
        if sources.next().is_some() {
            return (None, time_index.to_string());
        }
        per_source.get(source).map_or_else(
            || (metric.clone(), time_index.to_string()),
            |(metric, time, _)| (metric.clone(), time.clone()),
        )
    };
    let (segment_metric, segment_time_index) = segment_binding(&current_source_ids);
    // Recursion reaches a cut inside a subquery window with the statement's grid, not the
    // subquery's, so the root's own grid is bound.
    let publish_grid = fused_publish_grid(parent.root(), grid);
    let Ok(plan) = FusedSeries::try_new_with_format(
        Arc::clone(&promql),
        parent,
        FusedSeriesBinding {
            grid: publish_grid,
            time_index: segment_time_index,
            tags: tags.to_vec(),
            metric_name: segment_metric,
        },
        inputs,
        output_schema,
        format,
    ) else {
        return None;
    };
    Some(LogicalPlan::Extension(Extension {
        node: Arc::new(plan),
    }))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::series::SeriesOrdering;
    use datatypes::series::samples::{FieldSpec, SeriesSchema};
    use datatypes::series::time::{EvalGrid, GridSpec};

    use super::*;
    use crate::map::MathOp;
    use crate::native::plan::{
        AggregateKind, AggregateStage, BoundAggregate, BoundInput, BoundInstant, BoundMap,
        Grouping, InstantValue, MapOp,
    };

    fn grid() -> Arc<EvalGrid> {
        Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 1, 4).unwrap()).unwrap())
    }

    fn evaluated(grid: &Arc<EvalGrid>) -> NativeType {
        NativeType::Samples(Arc::new(SeriesSchema {
            fields: vec![FieldSpec {
                name: Arc::from("value"),
            }]
            .into(),
            eval_grid: Some(Arc::clone(grid)),
        }))
    }

    fn map_over(inner: NativeNode) -> NativeNode {
        let output = inner
            .output
            .grid()
            .expect("a map reads an evaluated vector")
            .clone();
        NativeNode {
            expr: NativeExpr::Map {
                input: Box::new(inner),
                op: BoundMap {
                    ops: vec![MapOp::Math(MathOp::Abs)].into(),
                },
            },
            output: evaluated(&output),
        }
    }

    fn selection(inner: NativeNode, output: NativeType) -> NativeNode {
        let grid = output
            .grid()
            .expect("a selection answers on a grid")
            .clone();
        NativeNode {
            expr: NativeExpr::Instant {
                input: Box::new(inner),
                op: BoundInstant {
                    lookback_ms: 300_000,
                    offset_ms: 0,
                    at_ms: None,
                    value: InstantValue::Sample,
                    output_grid: grid,
                },
            },
            output,
        }
    }

    #[test]
    fn a_fold_that_reads_its_parameter_is_a_global_consumer() {
        let grid = grid();
        let output = evaluated(&grid);
        let source = NativeNode {
            expr: NativeExpr::Input(BoundInput {
                output: output.clone(),
                source_index: 0,
                ordering: SeriesOrdering::Contiguous,
            }),
            output: output.clone(),
        };
        let fold = NativeNode {
            expr: NativeExpr::ParameterizedAggregate {
                input: Box::new(selection(source.clone(), output.clone())),
                op: BoundAggregate {
                    kind: AggregateKind::Quantile { quantile: 0.0 },
                    group: Grouping::By(Arc::from([])),
                    stage: AggregateStage::Complete,
                    field_kind: FieldKind::Float,
                },
                parameter: Box::new(NativeNode {
                    expr: NativeExpr::EvalTime { date: None },
                    output: NativeType::Scalar(Arc::clone(&grid)),
                }),
            },
            output: output.clone(),
        };
        assert!(
            contains_global_consumer(&fold),
            "a fold that reads its `φ` per point reads the whole vector"
        );
        assert!(
            contains_global_consumer(&map_over(fold)),
            "a pointwise parent must not hide the fold's gather"
        );
        assert!(
            !contains_global_consumer(&map_over(selection(source, output))),
            "a selection is not a whole-vector consumer"
        );
    }
}
