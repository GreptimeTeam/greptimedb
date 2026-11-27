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
//! A tree that reads across series answers one partition and gathers whatever it reads. Its
//! per-series prefixes do not need that gather, so each is cut out as a [`FusedSeries`] node of its
//! own and keeps the partitions it was scanned with: `cut_as_segment` decides what can be cut,
//! [`cut_segments`] rewrites the tree into one segment per cut, and [`segment_tree`] builds the
//! nodes, recursing into a cut that wraps a whole-vector consumer.
//!
//! Everything here reads the bound tree alone. The columns a node publishes are the planner's
//! answer, so the builders ask for them through [`OutputTags`], [`FieldNames`] and [`RootSchema`]
//! instead of deriving them a second time.

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

/// Derives the output tag columns a node publishes, from the tag columns each source declares.
///
/// The planner that owns the result layout names those columns with its own walk over the tree. A
/// segment only reads that answer - it never renames a column - so it asks through this leaf
/// rather than deriving the columns a second time.
pub type OutputTags = fn(&NativeNode, &[Vec<String>]) -> Option<Vec<String>>;

/// Derives the value column names a statement's expression gives the compiled `fields`.
///
/// Like [`OutputTags`], this is the planner's answer: a segment publishes the values its tree
/// computes under the names the statement gave them, so the builders ask through this leaf instead
/// of publishing the compiler's own field names. The tree alone does not carry the statement's
/// expression, which is why the expression text is an input rather than something derived here.
pub type FieldNames = fn(&str, &[String]) -> Vec<String>;

/// The row schema a statement root publishes for `promql`, or `None` when its columns cannot be
/// named once.
///
/// Like [`OutputTags`], this is the caller's answer: a segment that publishes rows publishes
/// exactly the columns the planner would have published for the same statement.
pub type RootSchema = fn(&str, &NativeTree, &[Vec<String>], &str, FieldKind) -> Option<DFSchemaRef>;

/// Whether `node` reads samples point-wise, so that one partitioned execution is one answer.
///
/// A node's output type is not checked here: [`NativeNode::validate`] already refuses a selection,
/// a map or a binary operation whose operands are not evaluated vectors, so every branch below is
/// one that produces values rather than raw samples.
pub fn pointwise_operand(node: &NativeNode) -> bool {
    match &node.expr {
        // A selection reads the raw samples below it; anything else below it is a value computed
        // from a whole series, not a scan.
        NativeExpr::Instant { input, .. } | NativeExpr::Window { input, .. } => {
            matches!(input.expr, NativeExpr::Input(_))
        }
        NativeExpr::Map { input, .. } | NativeExpr::Timestamp { input } => pointwise_operand(input),
        // A parameter that reads no data has the same value in every execution.
        NativeExpr::ParameterizedMap {
            input, parameters, ..
        } => pointwise_operand(input) && parameters.iter().all(NativeNode::is_data_free),
        // A scalar that reads no data is broadcast over the vector side's positions. Two vectors
        // are a match between two series sets, which is the shape this route cannot split.
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
        // An aggregate folds a group, a vector operation reads the whole vector, and a bare input or
        // scalar is not a per-series evaluation. A parameterized topk or histogram reads the whole
        // vector, `scalar()` answers from it, and a per-point `φ` is read beside the fold over the
        // whole group.
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

/// Whether a segment can hand `node`'s values to another segment.
///
/// A boundary carries an evaluated series frame: values addressed on a grid, under the labels of
/// the series they belong to. A scalar is broadcast rather than carried, aggregate state is
/// internal, and raw samples are what a scan produces - none of them is a frame, so a subtree
/// producing one stays inside the segment that reads it.
fn exportable(node: &NativeNode) -> bool {
    node.output
        .samples()
        .is_some_and(|schema| schema.eval_grid.is_some())
}

/// Whether `node` is a window over an evaluated input - the shape a subquery over anything but a
/// bare selection compiles to.
fn evaluated_input_window(node: &NativeNode) -> bool {
    matches!(&node.expr, NativeExpr::Window { op, .. } if op.input_grid.is_some())
}

/// Whether the dist analyzer places a segment over `node` region-locally.
///
/// A segment is a `FusedSeries` node the analyzer has to move below the `MergeScan` for the physical
/// planner to read the scans it names. The `FusedSeries` arm of `Categorizer::check_extension_plan`
/// (`src/query/src/dist_plan/commutativity.rs`) is what decides that, from the bound tree alone: a
/// root that [`reads_across_series`] is `NonCommutative` unless it is a per-series evaluation or the
/// `Partial` half of a split fold. A segment it refuses stays above the merge, where the physical
/// planner finds neither a region scan nor an explicit-label frame below it, so the statement fails
/// while being planned instead of keeping the plan it had. Segmentation therefore asks that question
/// before it cuts, and `cut_as_segment` refuses the nodes this refuses.
pub fn region_local_segment(node: &NativeNode) -> bool {
    !reads_across_series(node) || pointwise_operand(node) || independent_scan_pair(node)
}

/// Whether `node` becomes a segment of its own under a parent that must gather.
///
/// Cut local prefixes, global consumers and canonical merges separately so the prefix keeps its
/// scan width, and every global consumer sees the canonical boundary below it. A pointwise wrapper
/// of a global consumer is cut as well, so it cannot hide that consumer from recursive lowering.
fn cut_as_segment(node: &NativeNode, canonical_segmentation: bool) -> bool {
    // A window over an evaluated input answers per series on its own grid, so its own shape says
    // nothing about where a segment over it can go: it drops the metric name, so two of the series
    // it answers can end under one label set, and the dist analyzer reads it as cross-series
    // ([`region_local_segment`]). Cutting one where the analyzer will not place it leaves a
    // `FusedSeries` above the `MergeScan` that the physical planner cannot build, so the segment is
    // only taken where the analyzer places it region-locally; otherwise the per-series prefix
    // inside the window is cut instead, and where there is none the statement keeps the plan it had.
    if evaluated_input_window(node) && !region_local_segment(node) {
        return false;
    }
    // A window over an evaluated input is answered per series on its own grid, so it is not the
    // global gather a raw window's locality reports.
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
    // A segment builder needs an input, so data-free subtrees stay with their parent.
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

/// Whole-vector consumers remain global, but can be cut out as evaluated segments so a pointwise
/// parent does not inherit their single-partition placement.
///
/// A per-point `φ` does not make the fold it reads any less of a whole-vector consumer: a
/// `quantile` answers from every series of its group whichever node its argument comes from, so the
/// parameterized variant is admitted here exactly as the bound one is. Leaving it out would read a
/// fold that requires one partition as a node that does not, and the cut below it would be chosen
/// as though its parent could run per partition.
fn is_global_consumer(node: &NativeNode) -> bool {
    matches!(
        node.expr,
        NativeExpr::Aggregate { .. }
            | NativeExpr::ParameterizedAggregate { .. }
            | NativeExpr::Vector { .. }
    ) && matches!(input_locality(node), InputLocality::SinglePartition)
}

/// Whether a subtree can be cut without inheriting its parent's global gather.
///
/// A local prefix stays on its source partitions. A canonical merge becomes its own routed
/// boundary, and a parent reads the assembled frames rather than gathering the scan.
fn is_canonical_cut(node: &NativeNode) -> bool {
    matches!(node.expr, NativeExpr::Merge { .. })
        || !matches!(input_locality(node), InputLocality::SinglePartition)
        || canonical_local_prefix(node)
}

/// One segment: its tree rewritten to read only its own inputs, the scans it reads, and the
/// subtrees cut out of it.
///
/// A segment has one index space of its own - the scans it reads first, in the order it first names
/// them, then the subtrees cut out of it - because `FusedSeries` requires its source indices to be
/// dense over its children.
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

/// Appends the per-row name column to a rows layout's tag list.
///
/// A scalar root declares no name column: it publishes one value and no series, so the row it
/// answers has no name to report. The column is appended, so the layout keeps its order - the time
/// index, then the value fields, then the tags - and a label set that already reports the name keeps
/// the one column it has.
pub fn with_row_name_column(node: &NativeNode, mut tags: Vec<String>) -> Vec<String> {
    if !matches!(&node.output, NativeType::Samples(_)) {
        return tags;
    }
    // A layout can already report the name: a set operation takes it from its sides, a discovered
    // source sorts its tags for the union, and a statement reading more than one metric-bound input
    // declares the name each of those inputs reports. None of them leaves that column last - `or`
    // keeps the left operand's order and only appends the right side's unseen tags, and sorting puts
    // the name wherever its bytes fall - so the existing one is removed and appended instead: every
    // rows layout then answers one order, the time index, the value fields, the labels, and the name
    // last.
    if let Some(index) = tags.iter().position(|tag| tag == METRIC_NAME) {
        tags.remove(index);
    }
    tags.push(METRIC_NAME.to_string());
    tags
}

/// The ordering this route compiles its trees against: the weakest both scanners provide.
///
/// A native scan declares `KeyThenTime` and a two-phase scan `Contiguous`, and the logical planner
/// cannot read which one a statement will get. `Contiguous` is the weaker of the two - every
/// series' samples contiguous, the series themselves unordered - and a native evaluation only
/// refuses `Interleaved`, so a tree compiled against this ordering runs on either scanner.
pub const NATIVE_ORDERING: SeriesOrdering = SeriesOrdering::Contiguous;

/// The columns a [`FusedSeries`] node produces.
///
/// With `experimental_native_promql` enabled, a value column is named by the caller's [`FieldNames`]
/// answer for the statement's expression, not legacy planner expression text. Legacy headers in
/// `tests/cases/**` are unaffected because the native route is opt-in and off by default.
///
/// The shape is the one the existing unary pipeline consumes — the time index, then the series
/// fields, then the tags — and each column is declared the way the node's own conversion reads it
/// back:
/// the time column is a non-nullable millisecond timestamp with no timezone, because every
/// evaluated chunk is addressed on the statement's grid and carries one; a field is a nullable
/// `Float64`, because a grid point inside the statement's interval can have no sample; and a tag is
/// a nullable `Utf8`, because a range function drops the metric name and a series can be missing a
/// label entirely.
/// The output schema of a fused node: its time index, one column per field, then its tags.
///
/// `field_kind` says what a field column carries. The physical conversion reads the same kind off
/// this schema and dispatches on it, so the schema and the batch it describes cannot disagree.
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
    // The time index is marked as one, the way the existing planner's plans mark the table's own
    // time index: what a plan publishes is a result either way, and a reader of the marker - a
    // `RANGE` query over the plan, a schema converted back to GreptimeDB's - has to see the same
    // column on both routes.
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
        // The runtime answers `__name__` from the metric name of the series it reports, so a tag
        // column of that name is the name of each row rather than one name for the whole result.
        // Declaring it keeps the response layer from naming every row after the statement.
        if name == METRIC_NAME {
            field = field.with_metadata(datatypes::schema::metric_name_field_metadata());
        }
        columns.push((None, Arc::new(field)));
    }

    // GreptimeDB's schemas carry their version, and a result schema is no different: a schema
    // read back from this plan is one, exactly as the existing planner's answer for the same
    // statement is.
    let metadata = std::collections::HashMap::from([(
        datatypes::schema::VERSION_KEY.to_string(),
        datatypes::schema::Schema::INITIAL_VERSION.to_string(),
    )]);
    Ok(Arc::new(DFSchema::new_with_metadata(columns, metadata)?))
}

/// The grid one `FusedSeries` publishes on: the grid its root declares it produces, or the
/// statement's grid when the root declares none.
///
/// A root that produces one value per grid point says which grid that is, and that declared grid is
/// the one its consumer reads it on: a window over an evaluated input - a subquery - produces on the
/// window's own output grid, and a non-window root inside such a window, like the `Map` an
/// expression of a subquery compiles to, produces on the subquery's grid rather than on the
/// statement's. A statement root declares the statement's grid, so the two agree there. A root with
/// no declared grid - raw samples - is read where it was scanned and keeps the caller's grid.
/// Every lowering path binds the effective grid (the subquery's child grid for a matrix root, the
/// statement's grid otherwise), so a window root answers that same grid here.
fn fused_publish_grid(root: &NativeNode, statement_grid: &Arc<EvalGrid>) -> Arc<EvalGrid> {
    root.output
        .grid()
        .map_or_else(|| Arc::clone(statement_grid), Arc::clone)
}

/// Lowers a tree whose root is a canonical merge into a producer and a canonical router.
///
/// The tree says the statement answers one series per canonical label set; what it cannot say is
/// where the pieces of one set are, because a rewrite created them after the scan split the rows.
/// So the pieces have to meet before the answer is written, and there are only two honest ways to
/// make them meet: read every partition in one execution, or route each label set to one owner. The
/// first takes the caller's parallelism away for the whole query; this is the second.
///
/// Two stages come out of it, and each has one job:
///
/// - the **producer** runs the tree's root merge on every scan partition. Its merge collects the
///   pieces that partition holds - which is what makes a same-partition collision an error here,
///   before anything is routed - and it exports one series per label set as a series frame. It is
///   explicitly partition-local: the same node without that distinction would gather its inputs and
///   answer everything in one execution.
/// - the **router** is `SeriesRepartitionExec`'s canonical routing, which collects the pieces of one
///   label set from *every* input partition. It is the conservative router - a label set is
///   published only once every producer contributed it or ended - because a producer promises one
///   series per set *per partition*, not that each set lives in one partition. A second piece of one
///   set that holds a position another piece already holds is the same error, reported while that
///   label set is assembled, and each output of the router then holds whole label sets: one series
///   per set, complementary points joined.
///
/// `None` when the boundary cannot be built, so the caller keeps the plan it would have chosen.
/// The stages of a canonical boundary over `tree`, and the columns its frames carry.
///
/// `format` says what the caller above reads: a statement root reads the boundary's rows, and
/// anything else reads the assembled frames as an ordinary evaluated input.
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
    // The terminal rows of this boundary report the name of each row they publish, so the schema
    // declares it exactly as the other rows layouts do. The tags stay the walk's own: they name the
    // labels the frames carry, and the binding below is built from them.
    let row_tags = with_row_name_column(tree.root(), output_tags.clone());
    // The values are named after the expression, as in every other rows layout of this statement.
    // A schema this boundary cannot declare refuses here: the caller's error type is not one this
    // crate can build, so a name collision is reported by refusing rather than as an internal
    // error, and the caller keeps the plan it would have chosen.
    let value_names = field_names(&promql, &fields);
    let output_schema = fused_output_schema(&value_names, &row_tags, time_index, root_kind).ok()?;

    // The producer: the statement's own tree, run per partition, exporting a frame. It runs above
    // the regions, where its router collects each label set from every region's contribution.
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

    // What reads the router depends on where this boundary sits. A consumer still above it reads
    // whole label sets, and the router already merged the pieces of one label set into one series,
    // so that consumer is an ordinary evaluated input of its own and needs no merge. The statement's
    // root instead publishes rows, which is this boundary's own frame converted as usual.
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

/// Whether `plan` already answers raw series frames of its own.
///
/// A selector naming no metric is prepared as one [`RawSeriesSource`] per discovered metric, or as
/// the `Union` of them when it discovered several, and each source names the metric it reads. So
/// such a plan is a frame producer in its own right: a segment hands it to the node it builds
/// instead of binding it again. Binding it again nests a source in a source, and the physical
/// planner reads a source's child as the region scan that source binds - a union is no such child
/// either - so the nested input is refused after the statement has already been committed to this
/// route.
fn is_sourced_plan(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Union(_)) || is_raw_series_source(plan)
}

fn find_input_node(node: &NativeNode, source_index: usize) -> Option<&BoundInput> {
    node.inputs()
        .into_iter()
        .find(|input| input.source_index == source_index)
}

/// Whether `plan` is the typed empty input a discovered selector that matched no table gets.
///
/// It reads no region and holds no scan, so it is not one of the tables a node's binding is derived
/// from: two selectors are "of one table" only if the tables they scan are one. The remote
/// admission gate reads the same shape back, to tell a node with nothing to ship from one that does
/// read regions, so the declared frame is part of the check: an empty relation that does not
/// declare the raw explicit-label frame is some other plan, not this input.
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
            // A selector that discovered one metric is prepared as that metric's source, and one that
            // discovered several as the union of one source per metric; either way it already
            // declares the frames this segment reads, since it names each metric's source. So there
            // is nothing to bind it to and nothing to wrap it in, and binding a source in a source
            // is what the physical planner refuses. See [`is_sourced_plan`].
            || is_sourced_plan(&child)
            // The typed empty frame of a selector that matched no table is an input in its own
            // right: it reads no region and carries no metric, so there is nothing to wrap it in.
            // A segment over it is still an explicit-label frame with no series.
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
    // A canonical merge is not a segment: it is the middle of a boundary whose two frame nodes and
    // router are what answer one series per label set across partitions, so it is built as that
    // boundary and read as a frame.
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

/// Builds one segment's plan from a subtree, binding its scan inputs without cutting again.
///
/// Returns the plan and the output columns the segment reports, which its parent names its own
/// columns by. `None` when any part cannot be built, so the statement keeps the plan it had.
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
    // A segment publishes on the grid its own root declares, which is the grid its parent reads it
    // on: the segments of a subquery's inner expression are evaluated on the subquery's grid, not on
    // the statement's, whatever their root computes. The statement's grid is only the fallback for a
    // root that declares none.
    let segment_grid = fused_publish_grid(tree.root(), grid);
    let (inputs, metric, time_index, tags) = if let [index] = sources.as_slice() {
        let (metric, time_index, tags) = per_source.get(*index)?;
        (scans, metric.clone(), time_index.clone(), tags.clone())
    } else {
        // A binding cannot describe different source identities. Give each child the existing
        // explicit-label source boundary used for separate metric tables.
        let source_fields = tree_source_fields(&tree).ok()?;
        let mut children = Vec::with_capacity(sources.len());
        for (local_index, &source_index) in sources.iter().enumerate() {
            let scan = scans.get(local_index)?;
            // A scan that is already a source - one metric of a discovery, or the union of several -
            // is the frame producer this segment reads, so it stays as it is; see [`is_sourced_plan`].
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

/// Plans `tree` as several segments, or `None` when it is one node already.
///
/// A tree that reads across series answers one partition and gathers whatever it reads. Its
/// per-series prefixes do not need that gather, so each becomes a segment of its own and keeps the
/// partitions it was scanned with. Global consumers and canonical merges are cut in their original
/// order, and recursive segments publish frames for their parents to consume. Only the statement
/// root publishes rows, and it does so under the statement's own value names: [`FieldNames`]
/// answers those, and the rows schema asks [`RootSchema`] for the rest of the layout.
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
    // Only a tree the analyzer gathers is worth splitting: its per-series prefixes then run where
    // their input is read. A window over an evaluated input can need that split even where
    // [`input_locality`] reports a local tree - it drops the metric name, so [`reads_across_series`]
    // reads it as cross-series - and the per-series prefix inside it is what keeps the partitions it
    // scanned. Proceeding only admits the attempt: every cut is admitted by `cut_as_segment`
    // individually, which is where the analyzer's placement is asked.
    if !matches!(input_locality(tree.root()), InputLocality::SinglePartition)
        && !evaluated_input_window(tree.root())
    {
        return None;
    }
    // A segment boundary has to carry something, and the only thing it can carry is a per-series
    // prefix: a subtree that answers each series on its own and can be recomputed. A tree with no
    // such subtree inside it has nothing to cut, and cutting anything else - an aggregate's own
    // state, a constant - is either impossible or a plain recomputation of the same work in one
    // more node. Cutting on locality alone would accept one here.
    //
    // The window this tree may be rooted at is not one of those consumers, but it is no boundary
    // either: `cut_as_segment` never cuts one, and what keeps its input's partitions is the
    // per-series prefix below it, which the cuts below are admitted for.
    //
    // A grouping by `__name__` needs no refusal of its own here: `source_tags` is the authority on
    // which labels an input declares, and the planner declares the metric name of every occurrence of
    // a statement reading more than one input - so a segment's output tags name the label the grouping
    // keeps, and two groups differing only by metric are published under their own label sets.
    let mut current_tree = tree.clone();
    let mut current_children = children.to_vec();
    let mut current_tags = source_tags.to_vec();
    let mut current_source_ids = (0..children.len()).map(Some).collect::<Vec<_>>();
    let mut first_iteration = true;
    let mut changed = false;
    loop {
        // A tree the analyzer keeps partition-local needs no segment; a window over an evaluated
        // input is still worth attempting one for, so its per-series prefix keeps its partitions.
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
            // A cut may wrap a global consumer in a pointwise rewrite or a later merge. Segment
            // that subtree first so its inner canonical boundary stays below the consumer. A local
            // canonical merge instead goes directly to the boundary builder.
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
            // A cut the first iteration cannot build from the scans is built from the frames this
            // loop has already published: the canonical pass inserts a merge under a label rewrite,
            // and that boundary - rebuilt here as a producer, a frame and a router - is what answers
            // one series per label set across partitions. Which cuts are taken is
            // `cut_as_segment`'s decision, and it refuses the evaluated-input window the analyzer
            // will not place region-locally, so this fallback only ever sees the cuts it admitted.
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
    // Segmentation is useful even when the resulting parent is still single-partition: each
    // exported per-series subtree remains a separate input and is bound to its own source.
    let mut inputs = current_children;
    let mut child_tags = current_tags;
    // A merge at this segment's root still needs its own boundary. An outer rewrite above a
    // recursively cut ranking runs after the ranking, then is canonicalized before its caller.
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
    // Nothing below can fall back once physical planning starts, so every tree this route commits
    // to has to be one the runtime executes. A shape the runtime refuses - a grouping by a value
    // field, an operation with no kernel - therefore keeps the plan it had.
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
    // A recursively constructed segment still has its original source binding. Its parent may
    // combine another metric; giving the segment that parent's metric would relabel both inputs.
    // Direct scan segments keep their established builder and binding unchanged.
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
    // A segment publishes on the grid its root declares, which is the grid its consumer reads it
    // on. Segmentation recurses into a cut subtree with the caller's grid, and that subtree can sit
    // inside a subquery window: the canonical segmentation of a collapsing rewrite cuts the window's
    // input while its caller still holds the statement's grid. Binding the caller's grid here
    // rejects such a segment, and the rejection aborts the whole route.
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

    /// The evaluated layout of the only field these trees carry.
    fn evaluated(grid: &Arc<EvalGrid>) -> NativeType {
        NativeType::Samples(Arc::new(SeriesSchema {
            fields: vec![FieldSpec {
                name: Arc::from("value"),
            }]
            .into(),
            eval_grid: Some(Arc::clone(grid)),
        }))
    }

    /// `abs(…)` over `inner`, as a pointwise rewrite above a cut would be.
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

    /// `inner` selected at every grid point.
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

    /// A fold that reads its argument at every step is a whole-vector consumer.
    ///
    /// A `quantile` answers from its group's whole sample set wherever its `φ` comes from, so a
    /// fold that reads that `φ` per point needs every series of the vector in one place exactly as
    /// the fold that binds the number does. [`segment_tree`] asks [`contains_global_consumer`]
    /// where a tree may be cut and whether a cut it cannot build has to be refused, and a parent
    /// that answers one series at a time -- the `abs` below, as a canonical label rewrite would be
    /// -- must not hide the gather under it: read as a per-partition node, the cut beneath would be
    /// chosen for a parent that cannot run there.
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
        // The predicate is about that fold and not about every subtree: a pointwise rewrite of a
        // selection answers each series on its own, so it stays cuttable where it stands.
        assert!(
            !contains_global_consumer(&map_over(selection(source, output))),
            "a selection is not a whole-vector consumer"
        );
    }
}
