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

//! One node that runs a compiled native tree over a series frame.
//!
//! [`FusedSeries`] is the logical half: it holds the PromQL text the tree was compiled from, the
//! tree itself and the binding its input was compiled against. [`FusedSeriesExec`] is the physical
//! half: it runs that tree over the node's input - once per input partition for a tree that answers
//! one series from one series' rows, once over all of them for a tree that reads the whole vector -
//! and decodes its input frames into the chunks its tree's input node reads, then converts its
//! evaluated chunks back into batches.
//!
//! The two halves show different things in `EXPLAIN`, which is the point of having both. The
//! logical node prints the query a reader wrote ([`FusedSeries`]'s `fmt_for_explain`), and the
//! physical node prints the tree it will run ([`FusedSeriesExec`]'s `fmt_as`, which embeds
//! [`NativeTree`]'s own display rather than a second renderer). A reader comparing the two plans
//! can therefore see whether a query was compiled the way they expected without decoding node
//! names.
//!
//! Scope of this layer: the input is a stream of native series frames, and the frame's own
//! declaration says which kind. A raw frame
//! ([`raw_series_frame_schema`](datatypes::series::frame::raw_series_frame_schema)) holds one row
//! per chunk, carrying the series' encoded key, its finality, its timestamps and its fields, plus
//! the complete labels when the frame is explicit; an evaluated frame
//! ([`pack_evaluated_series`](common_query::series_frame::pack_evaluated_series)) holds values a
//! producer already decided, one row per position on the grid the frame declares. Both keep each
//! series' rows contiguous and its positions ascending, which is what the caller's
//! [`SeriesOrdering`] names. The output is one batch per evaluated chunk, with the statement's grid
//! timestamps in the declared time column. Everything the tree cannot do is rejected when the
//! execution plan is built, not when the first batch arrives: a tree the native runtime does not
//! execute, a frame whose schema the tree's input was not compiled against, or an output schema the
//! conversion cannot classify.

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fmt::{self, Formatter};
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use common_error::ext::BoxedError;
use common_query::series_frame::{
    SendableEvaluatedSeriesStream, ensure_frame_matches_label_source, pack_evaluated_series,
    unpack_evaluated_series, unpack_raw_series,
};
use datafusion::arrow::array::{ArrayRef, Float64Array, StringArray, TimestampMillisecondArray};
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DFSchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation, UnboundedMemoryPool};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, InputDistributionRequirements,
    Partitioning, PlanProperties, RecordBatchStream, SendableRecordBatchStream,
};
use datatypes::series::SeriesOrdering;
use datatypes::series::frame::{FrameContent, SeriesFrameDeclaration};
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::{DenseSamplesRef, FieldSpec, SampleRef, Samples, SeriesSchema};
use datatypes::series::time::EvalGrid;
use futures::Stream;
use store_api::region_engine::RawLabelSource;

use crate::evaluated::EvaluatedChunk;
use crate::extension_plan::aggregate_exchange::{PartialFold, aggregate_op, state_schema};
use crate::extension_plan::series_gather::{SeriesGather, spawn_partitions};
use crate::extension_plan::series_repartition::SeriesRepartitionExec;
use crate::grid_series::series_bytes;
use crate::native::execute::{InputId, NativeExecutionState, NativeExecutor};
use crate::native::plan::{
    AggregateKind, BoundAggregate, CanonicalOwner, CoPartitionedPair, Grouping, NativeExpr,
    NativeNode, NativeProperties, NativeTree, NativeType, merges_are_placeable_in,
    needs_single_execution_in_co, output_has_metric_name, reads_across_series,
};
use crate::source::materialize_raw_labels;

#[cfg(test)]
mod tests;

/// The label a metric name is stored under, which is not a tag column of the input.
const METRIC_NAME: &str = "__name__";

/// How many input steps one poll of [`FusedSeriesStream`] runs before it wakes itself.
///
/// A step polls the input once and hands what it settled to the executor. Settling is not
/// guaranteed: raw samples before the first evaluation point settle nothing, so an input of many
/// such chunks would otherwise be read to its end inside one poll, holding the task's thread for
/// an unbounded time and delaying both other tasks on it and the cancellation the consumer asked
/// for. The budget makes that a poll that yields instead, and the waker makes sure it is polled
/// again, so nothing is lost by stopping early.
const POLL_BUDGET: usize = 64;

/// The statement grid and output naming that travel with a compiled tree.
///
/// Source schemas belong to the tree's input nodes. This binding supplies the grid that addresses
/// evaluated chunks, the time column and the metric and full raw tag names a region scan child is
/// bound with. A child that already produces raw frames, a [`RawSeriesSource`] per table of a
/// cross-table tree, carries its own binding instead. The output tags are the output schema's. The
/// logical node checks that the root produces values on that grid; frames carry each series'
/// actual labels and timestamps.
///
/// [`RawSeriesSource`]: super::RawSeriesSource
#[derive(Clone, Debug)]
pub struct FusedSeriesBinding {
    /// The statement's grid, which is the grid every evaluated chunk is addressed on.
    pub grid: Arc<EvalGrid>,
    /// The metric name every input series is bound to, when the input reads one named metric.
    ///
    /// The logical node requires it when [`tags`](Self::tags) declares [`METRIC_NAME`]: a reader of
    /// the output has to be able to tell that a range function dropped the name, which needs a name
    /// to drop. The physical node takes the names from the frames, which carry each series' own.
    pub metric_name: Option<Arc<str>>,
    /// The output column holding the grid timestamps.
    pub time_index: String,
    /// The full logical tag names used to bind region scan children, before output-label projection.
    ///
    /// [`METRIC_NAME`] is special: it is resolved to the series' metric name instead of a column,
    /// which is how a reader sees that a range function dropped it.
    pub tags: Vec<String>,
}

impl PartialEq for FusedSeriesBinding {
    fn eq(&self, other: &Self) -> bool {
        self.metric_name == other.metric_name
            && self.time_index == other.time_index
            && self.tags == other.tags
            && self.grid.spec() == other.grid.spec()
    }
}

impl Eq for FusedSeriesBinding {}

impl Hash for FusedSeriesBinding {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.metric_name.hash(state);
        self.time_index.hash(state);
        self.tags.hash(state);
        let spec = self.grid.spec();
        (spec.start_ms, spec.step_ms, spec.len).hash(state);
    }
}

impl PartialOrd for FusedSeriesBinding {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(
            (
                self.metric_name.as_deref(),
                &self.time_index,
                &self.tags,
                self.grid.spec().start_ms,
                self.grid.spec().step_ms,
                self.grid.spec().len,
            )
                .cmp(&(
                    other.metric_name.as_deref(),
                    &other.time_index,
                    &other.tags,
                    other.grid.spec().start_ms,
                    other.grid.spec().step_ms,
                    other.grid.spec().len,
                )),
        )
    }
}

/// The output representation fixed when a fused plan is built.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum FusedOutputFormat {
    /// The existing tabular query result.
    PlainRows,
    /// A self-describing evaluated SeriesFrame.
    SeriesFrame,
}

fn evaluated_output(tree: &NativeTree) -> DataFusionResult<Arc<SeriesSchema>> {
    match &tree.root().output {
        NativeType::Samples(schema) if schema.eval_grid.is_some() => Ok(Arc::clone(schema)),
        NativeType::Samples(_) => Err(DataFusionError::Plan(
            "FusedSeries frame output requires evaluated samples with a grid".into(),
        )),
        NativeType::AggregateState(_) => Err(DataFusionError::Plan(
            "FusedSeries frame output does not support an aggregate-state root".into(),
        )),
        NativeType::Scalar(_) => Err(DataFusionError::Plan(
            "FusedSeries frame output does not support a scalar root".into(),
        )),
    }
}

fn series_frame_output_schema(schema: &SeriesSchema) -> DataFusionResult<SchemaRef> {
    let stream =
        pack_evaluated_series(Box::pin(futures::stream::empty()), Arc::new(schema.clone()))
            .map_err(|error| {
                DataFusionError::Plan(format!("FusedSeries cannot declare frame schema: {error}"))
            })?;
    Ok(stream.schema())
}

/// The logical plan of one node that runs a compiled native tree.
///
/// The node adds nothing to the tree: it records the query the tree was compiled from, so
/// `EXPLAIN` shows the reader their own query rather than the shape it compiled to, and it carries
/// the binding the tree's input was compiled against so the execution plan can be built from it.
#[derive(Debug)]
pub struct FusedSeries {
    /// The PromQL text this tree was compiled from.
    promql: Arc<str>,
    /// The compiled tree to run.
    tree: NativeTree,
    /// What the tree's input was compiled against.
    binding: FusedSeriesBinding,
    /// Plans producing the inputs, in source-index order.
    children: Vec<LogicalPlan>,
    /// The columns this node produces.
    output_schema: DFSchemaRef,
    /// The aggregate whose partition-local state this node exports instead of query rows.
    ///
    /// This row-state bridge is temporary: native final aggregation will consume series frames
    /// directly, removing state-row encoding and reconstruction.
    partial_fold: Option<BoundAggregate>,
    /// Fixed output representation; partial state export always remains PlainRows.
    format: FusedOutputFormat,
}

impl FusedSeries {
    /// The name this node is shown under.
    pub const fn name() -> &'static str {
        "FusedSeries"
    }

    /// Creates the node, checking that `tree` is legal against `binding`.
    pub fn try_new(
        promql: Arc<str>,
        tree: NativeTree,
        binding: FusedSeriesBinding,
        children: Vec<LogicalPlan>,
        output_schema: DFSchemaRef,
    ) -> DataFusionResult<Self> {
        Self::try_new_with_format(
            promql,
            tree,
            binding,
            children,
            output_schema,
            FusedOutputFormat::PlainRows,
        )
    }

    /// Creates a node with an explicitly selected output format.
    pub fn try_new_with_format(
        promql: Arc<str>,
        tree: NativeTree,
        binding: FusedSeriesBinding,
        children: Vec<LogicalPlan>,
        output_schema: DFSchemaRef,
        format: FusedOutputFormat,
    ) -> DataFusionResult<Self> {
        validate_binding(&tree, &binding, children.len())?;
        if format == FusedOutputFormat::SeriesFrame && children.len() > 1 {
            return Err(DataFusionError::Plan(
                "FusedSeries SeriesFrame output does not yet support multiple sources".into(),
            ));
        }
        if format == FusedOutputFormat::SeriesFrame {
            let expected = series_frame_output_schema(evaluated_output(&tree)?.as_ref())?;
            if output_schema.as_arrow() != expected.as_ref() {
                return Err(DataFusionError::Plan(
                    "FusedSeries SeriesFrame output schema does not match the evaluated frame layout"
                        .into(),
                ));
            }
        }
        Ok(Self {
            promql,
            tree,
            binding,
            children,
            output_schema,
            partial_fold: None,
            format,
        })
    }

    /// Creates a one-source node that exports partition-local state for a complete aggregate.
    pub(crate) fn try_new_partial(
        promql: Arc<str>,
        tree: NativeTree,
        binding: FusedSeriesBinding,
        children: Vec<LogicalPlan>,
        op: BoundAggregate,
    ) -> DataFusionResult<Self> {
        validate_binding(&tree, &binding, children.len())?;
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "FusedSeries partial fold requires one raw child".into(),
            ));
        }
        if reads_across_series(tree.root()) {
            return Err(DataFusionError::Plan(
                "FusedSeries partial fold prefix reads across series".into(),
            ));
        }
        aggregate_op(&op)?;
        let output_schema = Arc::new(datafusion::common::DFSchema::try_from(state_schema(
            &binding.tags,
        ))?);
        Ok(Self {
            promql,
            tree,
            binding,
            children,
            output_schema,
            partial_fold: Some(op),
            format: FusedOutputFormat::PlainRows,
        })
    }

    /// The aggregate whose partition-local state this node exports.
    pub fn partial_fold(&self) -> Option<&BoundAggregate> {
        self.partial_fold.as_ref()
    }

    /// The PromQL text this node was compiled from.
    pub fn promql(&self) -> &str {
        &self.promql
    }

    /// The compiled tree this node runs.
    pub fn tree(&self) -> &NativeTree {
        &self.tree
    }

    /// Child plans in source-index order.
    pub fn children(&self) -> &[LogicalPlan] {
        &self.children
    }

    /// What the tree's input was compiled against.
    pub fn binding(&self) -> &FusedSeriesBinding {
        &self.binding
    }

    /// The physical node that runs raw frame sources as this node's inputs.
    ///
    /// Each tuple binds a frame producer to its label source and ordering. Building the node
    /// rejects incompatible frames or an output
    /// schema the conversion cannot classify.
    pub fn to_execution_plan(
        &self,
        inputs: Vec<(Arc<dyn ExecutionPlan>, RawLabelSource, SeriesOrdering)>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let output_schema: SchemaRef = self.output_schema.inner().clone();
        if inputs.len() != self.children.len() {
            return Err(DataFusionError::Plan(format!(
                "FusedSeries physical input count {} does not match its {} logical children",
                inputs.len(),
                self.children.len()
            )));
        }
        let tags = if self.partial_fold.is_some() {
            self.binding.tags.clone()
        } else {
            self.output_tags()
        };
        Ok(Arc::new(FusedSeriesExec::build(
            Arc::clone(&self.promql),
            inputs,
            Arc::new(self.tree.clone()),
            output_schema,
            self.binding.time_index.clone(),
            tags,
            self.partial_fold.clone(),
            self.format,
        )?))
    }

    /// The label columns of the output: every column that is neither the time index nor a field of
    /// the root. A scalar root has no labels; its one other column is the value.
    ///
    /// The planner decides them from the whole tree - a match narrows them, `or` joins both sides'
    /// and a wrapper around either keeps them - and the output schema is where it records that.
    fn output_tags(&self) -> Vec<String> {
        let Some(root) = self.tree.root().output.samples() else {
            return Vec::new();
        };
        self.output_schema
            .fields()
            .iter()
            .map(|field| field.name())
            .filter(|name| {
                *name != &self.binding.time_index
                    && !root
                        .fields
                        .iter()
                        .any(|field| field.name.as_ref() == name.as_str())
            })
            .cloned()
            .collect()
    }
}

impl UserDefinedLogicalNodeCore for FusedSeries {
    fn name(&self) -> &str {
        Self::name()
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.children.iter().collect()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.output_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        // The tree's operands are bound inside it; there is no expression for an optimizer to
        // rewrite separately from the tree that owns it.
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> fmt::Result {
        match &self.partial_fold {
            Some(op) => write!(
                f,
                "FusedSeries: {} [partial {}]",
                self.promql,
                render_partial_fold(op)
            ),
            None if self.format == FusedOutputFormat::SeriesFrame => {
                write!(f, "FusedSeries: {} (output: series_frame)", self.promql)
            }
            None => write!(f, "FusedSeries: {}", self.promql),
        }
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        validate_binding(&self.tree, &self.binding, inputs.len())?;
        Ok(Self {
            promql: Arc::clone(&self.promql),
            tree: self.tree.clone(),
            binding: self.binding.clone(),
            children: inputs,
            output_schema: Arc::clone(&self.output_schema),
            partial_fold: self.partial_fold.clone(),
            format: self.format,
        })
    }
}

impl PartialEq for FusedSeries {
    fn eq(&self, other: &Self) -> bool {
        // `NativeTree` carries no structural equality, so the rendered tree stands in for it. The
        // renderer names every node with its bound operation and grids, which is what makes two
        // nodes the same computation; comparing the text alone would call two different trees
        // equal whenever their PromQL happens to read the same.
        self.promql == other.promql
            && self.tree.to_string() == other.tree.to_string()
            && tree_input_fields(&self.tree) == tree_input_fields(&other.tree)
            && self.binding == other.binding
            && self.children == other.children
            && self.output_schema == other.output_schema
            && partial_key(self.partial_fold.as_ref()) == partial_key(other.partial_fold.as_ref())
            && self.format == other.format
    }
}

impl Eq for FusedSeries {}

impl Hash for FusedSeries {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.promql.hash(state);
        self.binding.hash(state);
        self.children.hash(state);
        self.output_schema.hash(state);
        partial_key(self.partial_fold.as_ref()).hash(state);
        self.format.hash(state);
    }
}

impl PartialOrd for FusedSeries {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.promql.partial_cmp(&other.promql) {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        match self.binding.partial_cmp(&other.binding) {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        match self.children.partial_cmp(&other.children) {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        match self.tree.to_string().partial_cmp(&other.tree.to_string()) {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        match partial_key(self.partial_fold.as_ref())
            .partial_cmp(&partial_key(other.partial_fold.as_ref()))
        {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        match self.format.cmp(&other.format) {
            Ordering::Equal => {}
            ord => return Some(ord),
        }
        match tree_input_fields(&self.tree).cmp(&tree_input_fields(&other.tree)) {
            Ordering::Equal => {}
            ord => return Some(ord),
        }
        if self.output_schema == other.output_schema {
            Some(Ordering::Equal)
        } else {
            None
        }
    }
}

/// Renders a partial aggregate with the stable names used by the exchange plan.
fn render_partial_fold(op: &BoundAggregate) -> String {
    let group = match &op.group {
        Grouping::By(labels) => format!("by ({})", labels.join(", ")),
        Grouping::Without(labels) => format!("without ({})", labels.join(", ")),
    };
    let kind = match op.kind {
        AggregateKind::Quantile { quantile } => format!("quantile({quantile})"),
        kind => format!("{kind:?}").to_lowercase(),
    };
    format!("{kind} {group} {:?}", op.stage)
}

/// A stable comparable representation for an optional partial fold.
fn partial_key(partial: Option<&BoundAggregate>) -> Option<String> {
    partial.map(render_partial_fold)
}

/// The raw field names bound to each input leaf, retaining source association and leaf order.
fn tree_input_fields(tree: &NativeTree) -> Vec<(usize, Vec<String>)> {
    tree_inputs(tree)
        .into_iter()
        .map(|input| {
            let fields = match &input.output {
                NativeType::Samples(schema) => schema
                    .fields
                    .iter()
                    .map(|field| field.name.to_string())
                    .collect(),
                NativeType::Scalar(_) | NativeType::AggregateState(_) => Vec::new(),
            };
            (input.source_index, fields)
        })
        .collect()
}

/// The schemas read by distinct sources, in dense source-index order.
///
/// A schema names the kind of chunk its source's input node reads: one without a grid reads raw
/// samples and one bound to a grid reads values a producer already evaluated. Both are series
/// frames, and [`FusedSeriesExec`] decodes a child by the child's own declaration rather than by
/// this list, so neither kind is refused here. Which kinds may appear together is the physical
/// node's to check, because only it sees the frames the children declare.
fn collect_source_schemas(tree: &NativeTree) -> DataFusionResult<Vec<Arc<SeriesSchema>>> {
    let mut schemas: Vec<(usize, Arc<SeriesSchema>)> = Vec::new();
    for input in tree_inputs(tree) {
        let NativeType::Samples(found) = &input.output else {
            return Err(DataFusionError::Plan(
                "FusedSeries input is not samples".into(),
            ));
        };
        if let Some((_, expected)) = schemas
            .iter()
            .find(|(index, _)| *index == input.source_index)
        {
            if !same_fields(expected, found) {
                return Err(DataFusionError::Plan(
                    "FusedSeries repeated source has different fields".into(),
                ));
            }
        } else {
            schemas.push((input.source_index, Arc::clone(found)));
        }
    }
    schemas.sort_unstable_by_key(|(index, _)| *index);
    if schemas
        .iter()
        .enumerate()
        .any(|(expected, (actual, _))| expected != *actual)
    {
        return Err(DataFusionError::Plan(
            "FusedSeries source indexes are not dense".into(),
        ));
    }
    Ok(schemas.into_iter().map(|(_, schema)| schema).collect())
}

fn validate_binding(
    tree: &NativeTree,
    binding: &FusedSeriesBinding,
    child_count: usize,
) -> DataFusionResult<()> {
    if binding.tags.iter().any(|tag| tag == METRIC_NAME) && binding.metric_name.is_none() {
        return Err(DataFusionError::Plan(format!(
            "FusedSeries declares {METRIC_NAME:?} but binds no metric name"
        )));
    }
    let schemas = collect_source_schemas(tree)?;
    if schemas.is_empty()
        && (!matches!(tree.root().expr, NativeExpr::Scalar(_))
            || !matches!(tree.root().output, NativeType::Scalar(_)))
    {
        return Err(DataFusionError::Plan(
            "FusedSeries without inputs requires a constant scalar root".into(),
        ));
    }
    if schemas.len() != child_count {
        return Err(DataFusionError::Plan(
            "FusedSeries source indexes must densely match its children".into(),
        ));
    }
    if !tree
        .root()
        .output
        .grid()
        .is_some_and(|grid| grid.spec() == binding.grid.spec())
    {
        return Err(DataFusionError::Plan(
            "FusedSeries output is not on the statement grid".into(),
        ));
    }
    Ok(())
}

fn tree_inputs(tree: &NativeTree) -> Vec<&crate::native::plan::BoundInput> {
    fn collect<'a>(node: &'a NativeNode, inputs: &mut Vec<&'a crate::native::plan::BoundInput>) {
        match &node.expr {
            NativeExpr::Input(input) => inputs.push(input),
            NativeExpr::Scalar(_) => {}
            NativeExpr::Instant { input, .. }
            | NativeExpr::Window { input, .. }
            | NativeExpr::Merge { input }
            | NativeExpr::Map { input, .. }
            | NativeExpr::Timestamp { input }
            | NativeExpr::Aggregate { input, .. }
            | NativeExpr::Vector { input, .. } => collect(input, inputs),
            NativeExpr::Binary { left, right, .. }
            | NativeExpr::Set { left, right, .. }
            | NativeExpr::Info {
                base: left,
                info: right,
                ..
            } => {
                collect(left, inputs);
                collect(right, inputs);
            }
        }
    }
    let mut inputs = Vec::new();
    collect(tree.root(), &mut inputs);
    inputs
}

fn ordering_satisfies(producer: SeriesOrdering, required: SeriesOrdering) -> bool {
    match required {
        SeriesOrdering::KeyThenTime => producer == SeriesOrdering::KeyThenTime,
        SeriesOrdering::Contiguous => producer != SeriesOrdering::Interleaved,
        SeriesOrdering::Interleaved => true,
    }
}

fn same_fields(left: &SeriesSchema, right: &SeriesSchema) -> bool {
    left.fields.len() == right.fields.len()
        && left
            .fields
            .iter()
            .zip(right.fields.iter())
            .all(|(a, b)| a.name == b.name)
}

/// Whether the frame declared by `frame` carries evaluated values rather than raw samples.
///
/// The frame schema is the authority, so this asks the declaration parser the two decoders use
/// themselves: a frame that declares neither content is refused here with the parser's own reason
/// rather than defaulted to raw, and the decoder the answer selects re-checks the same declaration
/// before it reads a row. Nothing about the tree's input binding or the caller's label source takes
/// part: a producer says what its rows are, and a mismatch with what the input node was compiled
/// for is the decoder's to refuse.
fn input_frames_are_evaluated(
    frame: &datafusion::arrow::datatypes::Schema,
) -> DataFusionResult<bool> {
    let declaration = SeriesFrameDeclaration::parse(frame).map_err(|error| {
        DataFusionError::Plan(format!(
            "FusedSeries input frame is not a series frame: {error}"
        ))
    })?;
    Ok(declaration.content() == FrameContent::Evaluated)
}

/// Fields read by each distinct source in source-index order.
pub(crate) fn tree_source_fields(tree: &NativeTree) -> DataFusionResult<Vec<Vec<String>>> {
    Ok(collect_source_schemas(tree)?
        .into_iter()
        .map(|schema| {
            schema
                .fields
                .iter()
                .map(|field| field.name.to_string())
                .collect()
        })
        .collect())
}

fn validate_physical_tree(tree: &NativeTree) -> DataFusionResult<Vec<Arc<SeriesSchema>>> {
    let schemas = collect_source_schemas(tree)?;
    if tree.root().output.grid().is_none() {
        return Err(DataFusionError::Plan(
            "FusedSeries output is not evaluated samples".into(),
        ));
    }
    Ok(schemas)
}

/// What each input proves about the pieces of a label set it hands over, indexed by source.
///
/// This is the boundary [`NativeNode::properties`] takes, so it is read by the merge and by every
/// node above it that follows the ownership. The one fact this node can establish is which node
/// produced a partition: a child that places pieces by canonical key holds every piece of the label
/// sets it names and no other output holds a fragment of one of them
/// ([`ByCanonicalKey`](CanonicalOwner::ByCanonicalKey)), because that node routes each chunk by the
/// canonical label set of the series it carries. Every other child proves nothing - a scan
/// partitions by storage `read_key`, and a node that reorders without claiming a relation says
/// nothing about which series share a label set - so it is
/// [`Unknown`](CanonicalOwner::Unknown), and a merge above it is not placeable in a per-partition
/// execution ([`merges_are_placeable_in`]).
///
/// The declared partitioning is not the proof, and this is the point to be exact about.
/// `UnknownPartitioning(M)` is precisely "this node promises nothing about where a key lives": it is
/// what a `UnionExec`, a coalesce and every reordering node declare too, because the key here is
/// decoded from the rows rather than computed by an Arrow expression DataFusion could compare. So
/// the declaration is read as `Unknown` in general, and the ownership is taken as proved only for
/// this one child: such a child's own contract is to route every chunk by the canonical
/// label set of the series it carries, so its output partition does hold every piece of the label
/// sets it names and no other output holds a fragment of one of them.
///
/// That is a fact trusted by node identity ([`ExecutionPlan::downcast_ref`]) and not by anything the
/// child declares, and it stops being true the moment something reshuffles between the two: a
/// round-robin exchange, a coalesce or a union above the repartition spreads the pieces again while
/// leaving the ownership claim in place. Those nodes are not between the two here - the planner puts
/// the repartition directly below the merge it serves - and this function looks at the immediate
/// child and nothing below it, so a plan that ever interposes one has to reconsider this. It is
/// written as a match on the child's type for that reason: a node added to this condition is a claim
/// about that node's routing that has to be justified the same way.
fn input_boundary(
    inputs: &[(Arc<dyn ExecutionPlan>, RawLabelSource, SeriesOrdering)],
) -> Vec<NativeProperties> {
    // What a fused input proves about the pieces it hands over.
    //
    // A `SeriesRepartitionExec` routes every chunk by the canonical label set it carries and is
    // recognised **by node identity**, not by its declared partitioning: it publishes
    // `UnknownPartitioning(M)`, which says "this node promises nothing about where a key lives" and
    // is deliberately not upgraded to a proof. Every other child places pieces by the storage
    // `read_key`, so pieces of one canonical label set can sit in different partitions and nothing
    // is claimed about their ownership.
    //
    // What that means for a merge is [`merges_are_placeable_in`]'s question, not this function's:
    // over an unproven input a merge is not placeable in a per-partition execution, so the route
    // that would need one declines rather than collapsing the node to a single execution.
    inputs
        .iter()
        .map(|(input, _, _)| NativeProperties {
            owner: if input.downcast_ref::<SeriesRepartitionExec>().is_some() {
                CanonicalOwner::ByCanonicalKey
            } else {
                CanonicalOwner::Unknown
            },
            ..NativeProperties::source_pieces()
        })
        .collect()
}

/// What this node can prove about pairs of its children being read in step, as the match proof reads
/// it ([`CoPartitionedPair`]).
///
/// This is the *joint* half of [`match_is_co_partitioned`] and cannot be derived inside the tree: the
/// operands of the shape it serves are [`NativeExpr::Input`] nodes over evaluated samples, whose label
/// sets another execution built, so the tree answers `None` for them and states nothing either way.
/// This function asks the producers themselves instead, where the answer is a function of the tree
/// standing in front of each one.
///
/// Every fact is read off the children **on every build**: nothing is cached, stored on the node or
/// passed in by a caller. That is what makes a child swap safe - `with_new_children` hands the new
/// children back through [`FusedSeriesExec::build`], which asks these same questions again, so a
/// replacement that is not a routing, splits into a different number of outputs, or keeps a metric
/// name simply does not produce the pair and takes the proof away with it.
fn co_partitioned_pairs(
    inputs: &[(Arc<dyn ExecutionPlan>, RawLabelSource, SeriesOrdering)],
) -> Vec<CoPartitionedPair> {
    let mut pairs = Vec::new();
    for (left_index, (left, _, _)) in inputs.iter().enumerate() {
        let Some(left) = left.downcast_ref::<SeriesRepartitionExec>() else {
            continue;
        };
        for (right_index, (right, _, _)) in inputs.iter().enumerate().skip(left_index + 1) {
            let Some(right) = right.downcast_ref::<SeriesRepartitionExec>() else {
                continue;
            };
            // Two routings are one routing only when they split into the same number of outputs. A
            // partner routed into three outputs is not in the partition its two-output counterpart
            // is, so its half of the match would be missing here while it exists elsewhere, and the
            // pair is not the fact the match needs.
            if left.outputs() != right.outputs() {
                continue;
            }
            pairs.push(CoPartitionedPair {
                left: left_index,
                right: right_index,
                // A pair whose producer half fails proves nothing: [`match_is_co_partitioned`]
                // answers `pair.name_free`, so stating the pair with the failing half named is what
                // makes one named side break it. Dropping the pair instead would hide which
                // conjunct decided the fallback.
                name_free: producer_is_name_free(left) && producer_is_name_free(right),
            });
        }
    }
    pairs
}

/// Whether `repartition`'s producer answers series that carry no metric name.
///
/// The producer has to be a [`FusedSeriesExec`] - the only node whose output labels are decided by a
/// tree, and hence the only one this layer can ask - and the answer is [`output_has_metric_name`] on
/// its root, read strictly: only `Some(false)` is a proof. `None` means "not provable here", and
/// taking it as "no name" is exactly the guess that makes a named match look name-free: the match
/// then runs per partition, pairs by a key no partition holds whole, finds no partner and answers
/// nothing where the whole match answers a value.
fn producer_is_name_free(repartition: &SeriesRepartitionExec) -> bool {
    let children = repartition.children();
    let [producer] = children.as_slice() else {
        return false;
    };
    let Some(producer) = producer.downcast_ref::<FusedSeriesExec>() else {
        return false;
    };
    output_has_metric_name(producer.tree().root()) == Some(false)
}

/// Where one output column of [`FusedSeriesExec`] comes from.
#[derive(Clone, Copy, Debug)]
pub(crate) enum OutputColumn {
    /// The evaluation grid timestamps of the current chunk.
    Time,
    /// Series field by index in the bound series schema.
    Field(usize),
    /// Declared logical tag by index in the caller-supplied tag list.
    Tag(usize),
}

/// The caller-named value field used by the ordinary row conversion for a label-free scalar.
fn scalar_root_fields(
    output_schema: &SchemaRef,
    tags: &[String],
    time_column: &str,
) -> DataFusionResult<Vec<FieldSpec>> {
    if !tags.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "FusedSeriesExec scalar root cannot produce {} tag columns",
            tags.len()
        )));
    }
    let mut values = output_schema
        .fields()
        .iter()
        .filter(|field| field.name().as_str() != time_column);
    let Some(value) = values.next() else {
        return Err(DataFusionError::Plan(
            "missing value column in scalar root output schema".to_string(),
        ));
    };
    if values.next().is_some() {
        return Err(DataFusionError::Plan(
            "scalar root output schema must contain exactly one value column".into(),
        ));
    }
    Ok(vec![FieldSpec {
        name: Arc::from(value.name().as_str()),
    }])
}

/// Classifies every output column, rejecting anything the conversion cannot produce.
pub(crate) fn classify_output(
    output_schema: &SchemaRef,
    series_fields: &[FieldSpec],
    tags: &[String],
    time_column: &str,
) -> DataFusionResult<Vec<OutputColumn>> {
    for (index, tag) in tags.iter().enumerate() {
        if tags[..index].contains(tag) {
            return Err(DataFusionError::Plan(format!(
                "duplicate tag column {tag:?} in FusedSeriesExec output schema"
            )));
        }
        if tag == time_column {
            return Err(DataFusionError::Plan(format!(
                "tag column {tag:?} of FusedSeriesExec cannot be the time column"
            )));
        }
        if series_fields.iter().any(|field| field.name.as_ref() == tag) {
            return Err(DataFusionError::Plan(format!(
                "tag column {tag:?} of FusedSeriesExec clashes with a series field"
            )));
        }
    }

    let mut time_seen = false;
    let mut field_seen = vec![false; series_fields.len()];
    let mut tag_seen = vec![false; tags.len()];
    let mut classification = Vec::with_capacity(output_schema.fields().len());
    for field in output_schema.fields() {
        let name = field.name();
        if name == time_column {
            if std::mem::replace(&mut time_seen, true) {
                return Err(DataFusionError::Plan(format!(
                    "duplicate time column {name:?} in FusedSeriesExec output schema"
                )));
            }
            match field.data_type() {
                DataType::Timestamp(TimeUnit::Millisecond, None) => {}
                other => {
                    return Err(DataFusionError::Plan(format!(
                        "time column {name:?} of FusedSeriesExec must be Timestamp(Millisecond, \
                         None), got {other:?}"
                    )));
                }
            }
            if field.is_nullable() {
                return Err(DataFusionError::Plan(format!(
                    "time column {name:?} of FusedSeriesExec must not be nullable"
                )));
            }
            classification.push(OutputColumn::Time);
            continue;
        }
        if let Some(index) = series_fields
            .iter()
            .position(|field| field.name.as_ref() == name)
        {
            if std::mem::replace(&mut field_seen[index], true) {
                return Err(DataFusionError::Plan(format!(
                    "duplicate field column {name:?} in FusedSeriesExec output schema"
                )));
            }
            if field.data_type() != &DataType::Float64 {
                return Err(DataFusionError::Plan(format!(
                    "field column {name:?} of FusedSeriesExec must be Float64, got {:?}",
                    field.data_type()
                )));
            }
            // A grid point inside the statement's interval can have no value in this field.
            if !field.is_nullable() {
                return Err(DataFusionError::Plan(format!(
                    "field column {name:?} of FusedSeriesExec must be nullable, because a grid \
                     point can be missing"
                )));
            }
            classification.push(OutputColumn::Field(index));
            continue;
        }
        if let Some(index) = tags.iter().position(|tag| tag == name) {
            if std::mem::replace(&mut tag_seen[index], true) {
                return Err(DataFusionError::Plan(format!(
                    "duplicate tag column {name:?} in FusedSeriesExec output schema"
                )));
            }
            if field.data_type() != &DataType::Utf8 {
                return Err(DataFusionError::Plan(format!(
                    "tag column {name:?} of FusedSeriesExec must be Utf8, got {:?}",
                    field.data_type()
                )));
            }
            // `rate` and the map kernels drop `__name__`, so a tag can be absent from a row.
            if !field.is_nullable() {
                return Err(DataFusionError::Plan(format!(
                    "tag column {name:?} of FusedSeriesExec must be nullable, because range \
                     functions and map kernels drop the metric name"
                )));
            }
            classification.push(OutputColumn::Tag(index));
            continue;
        }
        return Err(DataFusionError::Plan(format!(
            "unknown column {name:?} in FusedSeriesExec output schema"
        )));
    }

    if !time_seen {
        return Err(DataFusionError::Plan(format!(
            "missing time column {time_column:?} in FusedSeriesExec output schema"
        )));
    }
    for (index, seen) in field_seen.iter().enumerate() {
        if !seen {
            return Err(DataFusionError::Plan(format!(
                "missing field column {:?} in FusedSeriesExec output schema",
                series_fields[index].name
            )));
        }
    }
    for (index, seen) in tag_seen.iter().enumerate() {
        if !seen {
            return Err(DataFusionError::Plan(format!(
                "missing tag column {:?} in FusedSeriesExec output schema",
                tags[index]
            )));
        }
    }
    Ok(classification)
}

/// The physical node that runs a compiled native tree over its input's series frames.
pub struct FusedSeriesExec {
    /// The PromQL text the tree was compiled from, which is what the plan line shows.
    promql: Arc<str>,
    /// The tree this node runs, shared by every partition's execution.
    tree: Arc<NativeTree>,
    /// Each source's compiled schema, which names the kind of chunk its input node reads and is
    /// used to decode only that source's frames.
    series_schemas: Vec<Arc<SeriesSchema>>,
    /// Physical input plans, preserving each source's frame binding and ordering.
    inputs: Vec<(Arc<dyn ExecutionPlan>, RawLabelSource, SeriesOrdering)>,
    /// Where each output column comes from.
    classification: Arc<[OutputColumn]>,
    /// The output column holding the grid timestamps, kept so this node can be rebuilt over a new
    /// child without losing which column its time came from.
    time_column: String,
    /// The tag column names, indexed by [`OutputColumn::Tag`].
    tags: Arc<[String]>,
    /// The columns this node produces.
    output_schema: SchemaRef,
    /// Per-node metrics, registered by each execution of the tree.
    metric: ExecutionPlanMetricsSet,
    /// Aggregate state exported by each partition, when this is the partial mode.
    partial_fold: Option<BoundAggregate>,
    format: FusedOutputFormat,
    /// This node's partitioning, emission and boundedness.
    properties: Arc<PlanProperties>,
}

impl FusedSeriesExec {
    /// Creates the node, rejecting whatever this node cannot execute or convert.
    ///
    /// `input` produces native series frames, and each child's own frame declaration says which
    /// kind: a raw frame carries the series' encoded key, its finality, its timestamps and its
    /// fields, plus the complete labels when `label_source` is [`RawLabelSource::Explicit`], while
    /// an evaluated frame carries values a producer already decided on the grid it declares. Which
    /// kind each child's input node reads is fixed by the tree; the frame has to agree with it.
    /// `time_column` and `tags` name the columns of the output, which is a row table rather than a
    /// frame.
    ///
    /// The parameters stay separate because each is a fact its own caller has: the query, the plan
    /// below, the frame's label binding and ordering, the tree, and the three things the output
    /// schema is classified by. A struct over them would be the same list under another name.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        promql: Arc<str>,
        inputs: Vec<(Arc<dyn ExecutionPlan>, RawLabelSource, SeriesOrdering)>,
        tree: Arc<NativeTree>,
        output_schema: SchemaRef,
        time_column: String,
        tags: Vec<String>,
    ) -> DataFusionResult<Self> {
        Self::build(
            promql,
            inputs,
            tree,
            output_schema,
            time_column,
            tags,
            None,
            FusedOutputFormat::PlainRows,
        )
    }

    /// The tree this node runs, which is what decides the label sets it answers under.
    ///
    /// A producer above this node reads this to answer what it cannot see in its own tree: whether
    /// the series handed over still carry a metric name
    /// ([`output_has_metric_name`]).
    pub(crate) fn tree(&self) -> &NativeTree {
        &self.tree
    }

    /// Whether `inputs`' sources, read through `tree`, have to be evaluated once over every input
    /// partition instead of once per partition.
    ///
    /// The tree is asked what it reads ([`needs_single_execution_in_co`]) with the two things this
    /// node can observe: the facts [`input_boundary`] reads off each child, and the pairs of children
    /// that were routed identically ([`co_partitioned_pairs`]). A tree that matches two sources
    /// answers one execution unless a pair proves the match pairs by the key the routing used, so a
    /// multi-source match is one execution exactly when that proof is absent - which is what keeps
    /// the fallback, and not a source count, deciding it. A single-source tree has no pair and is
    /// decided as it always was. A merge is not asked for one execution either way: what it needs is
    /// its input's, because whether a merge can be placed at all is a question about the execution,
    /// not a reason to shrink the execution to one. That question is [`merges_are_placeable_in`],
    /// asked by [`Self::reject_unplaceable_merges`] below.
    fn requires_single_execution(
        inputs: &[(Arc<dyn ExecutionPlan>, RawLabelSource, SeriesOrdering)],
        tree: &NativeTree,
    ) -> bool {
        needs_single_execution_in_co(
            tree.root(),
            &input_boundary(inputs),
            &co_partitioned_pairs(inputs),
        )
    }

    /// Refuses a tree whose merges the execution this node is about to run cannot answer.
    ///
    /// A merge collects every piece of one canonical label set and answers one series of it. An
    /// execution that reads every input partition has all of them; one that reads a single partition
    /// has them only where the boundary proves the pieces arrive together. Nothing else may make a
    /// merge work, and collapsing this node to one execution to serve a tree nobody proved was
    /// placeable is exactly that error: it takes the caller's parallelism away instead of declining
    /// the shape. The planner declines such a tree before it is built
    /// (`PromPlanner::try_plan_native_fused_with_context`), so this only fires for a plan that was
    /// built by hand or rebuilt over a different child.
    fn reject_unplaceable_merges(
        single_execution: bool,
        inputs: &[(Arc<dyn ExecutionPlan>, RawLabelSource, SeriesOrdering)],
        tree: &NativeTree,
    ) -> DataFusionResult<()> {
        // The one case a per-partition execution is not a part of its input is an input of one
        // partition: then the partition it reads *is* every partition, and every piece of every
        // label set is in it. That is not a proof the child gave but a fact about the boundary, and
        // it is why this asks the boundary facts rather than assuming they prove nothing.
        let co_located = single_execution
            || inputs
                .iter()
                .all(|(input, _, _)| input.properties().partitioning.partition_count() <= 1);
        if merges_are_placeable_in(tree.root(), &input_boundary(inputs), co_located) {
            return Ok(());
        }
        Err(DataFusionError::Plan(
            "FusedSeries cannot place this tree's canonical merge: the execution reads one input \
             partition and nothing proves the pieces of a canonical label set are co-located, so \
             the merge would present one partition's fragments as a whole series"
                .to_string(),
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        promql: Arc<str>,
        inputs: Vec<(Arc<dyn ExecutionPlan>, RawLabelSource, SeriesOrdering)>,
        tree: Arc<NativeTree>,
        output_schema: SchemaRef,
        time_column: String,
        tags: Vec<String>,
        partial_fold: Option<BoundAggregate>,
        format: FusedOutputFormat,
    ) -> DataFusionResult<Self> {
        if partial_fold.is_none() && format == FusedOutputFormat::SeriesFrame && inputs.len() > 1 {
            return Err(DataFusionError::Plan(
                "FusedSeries SeriesFrame output does not yet support multiple sources".into(),
            ));
        }
        let raw_schemas = validate_physical_tree(&tree)?;
        if inputs.len() != raw_schemas.len() {
            return Err(DataFusionError::Plan(format!(
                "FusedSeries physical input count {} does not match its {} sources",
                inputs.len(),
                raw_schemas.len()
            )));
        }
        // Building a state proves the runtime executes this tree; drop the validation state now.
        let _ = NativeExecutionState::try_new(&tree, Arc::new(UnboundedMemoryPool::default()))?;
        let output_series = tree.root().output.samples();
        let scalar_root = raw_schemas.is_empty();
        if let Some(op) = &partial_fold {
            aggregate_op(op)?;
        }
        if partial_fold.is_some() && (inputs.len() != 1 || reads_across_series(tree.root())) {
            return Err(DataFusionError::Plan(
                "FusedSeries partial fold requires one raw source and a per-series prefix".into(),
            ));
        }
        let frame_schema = if partial_fold.is_none() && format == FusedOutputFormat::SeriesFrame {
            let declared = series_frame_output_schema(evaluated_output(&tree)?.as_ref())?;
            if declared != output_schema {
                return Err(DataFusionError::Plan(
                    "FusedSeriesExec output schema does not match the evaluated frame layout"
                        .into(),
                ));
            }
            true
        } else {
            false
        };
        let classification = if partial_fold.is_some() || frame_schema {
            Vec::new()
        } else if scalar_root {
            let scalar_fields = scalar_root_fields(&output_schema, &tags, &time_column)?;
            classify_output(&output_schema, &scalar_fields, &tags, &time_column)?
        } else {
            classify_output(
                &output_schema,
                &output_series
                    .ok_or_else(|| {
                        DataFusionError::Plan("FusedSeries output is not samples".into())
                    })?
                    .fields,
                &tags,
                &time_column,
            )?
        };
        let partition_count = inputs.first().map_or(1, |(input, _, _)| {
            input.properties().partitioning.partition_count()
        });
        let mut evaluated_sources = 0usize;
        let mut raw_sources = 0usize;
        for (index, (input, label_source, ordering)) in inputs.iter().enumerate() {
            let count = input.properties().partitioning.partition_count();
            if count == 0 {
                return Err(DataFusionError::Plan(format!(
                    "FusedSeries input {index} has no partitions"
                )));
            }
            let required_ordering = if inputs.len() > 1 && count > 1 {
                SeriesOrdering::Contiguous
            } else {
                *ordering
            };
            if tree_inputs(&tree)
                .iter()
                .filter(|node| node.source_index == index)
                .any(|node| !ordering_satisfies(required_ordering, node.ordering))
            {
                return Err(DataFusionError::Plan(format!(
                    "FusedSeries producer ordering for source {index} does not satisfy every input"
                )));
            }
            ensure_frame_matches_label_source(input.schema().as_ref(), label_source)?;
            // The child is validated with the decoder the tree's input node was compiled against:
            // a raw-bound leaf reads raw samples and a grid-bound one reads evaluated values, and
            // each decoder parses the frame's own declaration first, so a frame that declares the
            // other content is refused here - by name, in the decoder's words - rather than read as
            // though it held what this leaf promised.
            let empty = || {
                Box::pin(RecordBatchStreamAdapter::new(
                    input.schema(),
                    futures::stream::empty(),
                ))
            };
            if raw_schemas[index].eval_grid.is_some() {
                evaluated_sources += 1;
                let _ = unpack_evaluated_series(empty(), Some(Arc::clone(&raw_schemas[index])))?;
            } else {
                raw_sources += 1;
                unpack_raw_series(empty(), Some(Arc::clone(&raw_schemas[index])))?;
            }
        }
        // Several evaluated sources are read, each gathered from its own child: every source's
        // partitions are read at once and kept apart, because a frame is contiguous within a
        // partition and not across them, and because the runtime routes a chunk by the source index
        // its gather hands it over with. What carries them out is the plain-row conversion, one
        // evaluated chunk at a time; a frame output over more than one source is refused above,
        // since packing several sources into one frame would have to say which label set of one
        // source belongs beside which of another.
        //
        // An evaluated source beside a raw one is still refused. A raw source is gathered across its
        // partitions by series with [`SeriesGather`], whose chunks and label materialization differ
        // from an evaluated gather's, so reading both at once would need a third merge over the two
        // kinds of gather; this node has none, and running one side through the other would read it
        // wrongly. Nothing above this node asks for that combination, so it declines instead.
        if evaluated_sources > 0 && raw_sources > 0 {
            return Err(DataFusionError::Plan(
                "FusedSeries reads evaluated sources on their own, not beside a raw source".into(),
            ));
        }
        // One execution over every input partition, rather than one execution per partition: a tree
        // that reads the whole vector has to see every series at once, more than one source is one
        // execution because its operands are matched against each other, and otherwise what the child
        // proves about the pieces of a label set decides. A merge is not one of those reasons: what a
        // merge needs from an execution is asked below, and an execution that cannot answer it is
        // refused rather than shrunk to one.
        let single_execution = Self::requires_single_execution(&inputs, &tree);
        Self::reject_unplaceable_merges(single_execution, &inputs, &tree)?;
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(if single_execution { 1 } else { partition_count }),
            if partial_fold.is_some() {
                // Partial aggregate state is only emitted after the scan reaches EOF.
                EmissionType::Final
            } else {
                inputs.first().map_or(EmissionType::Final, |(input, _, _)| {
                    input.properties().emission_type
                })
            },
            inputs
                .first()
                .map_or(Boundedness::Bounded, |(input, _, _)| {
                    input.properties().boundedness
                }),
        ));
        Ok(Self {
            promql,
            tree,
            series_schemas: raw_schemas,
            inputs,
            classification: classification.into(),
            time_column,
            tags: tags.into(),
            output_schema,
            metric: ExecutionPlanMetricsSet::new(),
            properties,
            partial_fold,
            format,
        })
    }

    /// The name this node is shown under.
    pub const fn name() -> &'static str {
        "FusedSeriesExec"
    }
}

impl std::fmt::Debug for FusedSeriesExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FusedSeriesExec").finish()
    }
}

impl DisplayAs for FusedSeriesExec {
    /// The query, then the tree it compiles to: the tree is rendered by [`NativeTree`]'s own
    /// `Display`, so `EXPLAIN` and the node's metrics name the same nodes.
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "FusedSeriesExec: {}\n{}", self.promql, self.tree)?;
        if let Some(op) = &self.partial_fold {
            write!(f, "\npartial state: {}", render_partial_fold(op))?;
        } else if self.format == FusedOutputFormat::SeriesFrame {
            write!(f, "\noutput: series_frame")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for FusedSeriesExec {
    fn name(&self) -> &str {
        Self::name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    /// Nothing is asked of the input's distribution, and nothing may be inserted above it.
    ///
    /// A hash requirement would make a hash exchange necessary as soon as the child has more than
    /// one partition - even where the child already satisfies the requirement - and a hash exchange
    /// does not preserve order. The input's own layout is already the one this node needs, because
    /// the scan it reads from guarantees a series stays in one partition, so the requirement says
    /// only that the layout is left alone.
    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        InputDistributionRequirements::new(
            self.inputs
                .iter()
                .map(|(_, _, _)| Distribution::UnspecifiedDistribution)
                .collect(),
        )
    }

    /// Splitting the input further would not help: a second partition is a second set of series
    /// answered by a second tree when the tree is per-series, and one execution already reads every
    /// partition when it is not, so raising the count is what an optimizer does by adding a
    /// round-robin exchange that reorders nothing it can rely on.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false; self.inputs.len()]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inputs.iter().map(|(input, _, _)| input).collect()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        // The tree's operands are bound inside it; this node owns no expression of its own.
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != self.inputs.len() {
            return Err(DataFusionError::Plan(format!(
                "FusedSeriesExec requires {} children, got {}",
                self.inputs.len(),
                children.len()
            )));
        }
        let children: Vec<_> = children
            .into_iter()
            .zip(&self.inputs)
            .map(|(child, (_, label_source, ordering))| (child, label_source.clone(), *ordering))
            .collect();
        // A replaced child can take the proof away: a canonical repartition swapped for a plain
        // multi-partition child hands the pieces of a label set to different executions again. The
        // rebuilt node therefore asks the placement question over the children it was handed, and
        // never inherits the answer the old ones gave.
        Self::reject_unplaceable_merges(
            Self::requires_single_execution(&children, &self.tree),
            &children,
            &self.tree,
        )?;
        // Rebuilt rather than patched: the new child's schema is what the frame is validated
        // against, so a child that changed shape is refused instead of decoded with a stale layout.
        Ok(Arc::new(Self::build(
            Arc::clone(&self.promql),
            children,
            Arc::clone(&self.tree),
            self.output_schema.clone(),
            self.time_column.clone(),
            self.tags.to_vec(),
            self.partial_fold.clone(),
            self.format,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition >= self.properties.partitioning.partition_count() {
            return Err(DataFusionError::Execution(
                "FusedSeries output partition is out of range".into(),
            ));
        }
        let state = NativeExecutionState::try_new(
            &self.tree,
            Arc::clone(&context.runtime_env().memory_pool),
        )?;
        state.register_metrics(&self.metric, partition);
        // One execution reads every partition of its input, so an output partition does not name one
        // of them here; a per-partition execution does, and must name one the input has.
        let single_execution = Self::requires_single_execution(&self.inputs, &self.tree);
        let mut streams = Vec::new();
        // One list of streams per evaluated source, indexed by source index: the index is what
        // routes a chunk into that source's own input slots, so the sources are kept apart here
        // rather than collected into one list. Every source is dense ([`Self::build`] refused
        // anything else), so the list's own index is the source index.
        let mut evaluated: Vec<Vec<SendableEvaluatedSeriesStream>> =
            (0..self.series_schemas.len()).map(|_| Vec::new()).collect();
        for (source, (input, _, _)) in self.inputs.iter().enumerate() {
            let count = input.properties().partitioning.partition_count();
            if !single_execution && partition >= count {
                return Err(DataFusionError::Execution(format!(
                    "FusedSeries input {source} has no partition {partition}"
                )));
            }
            let partitions = if single_execution {
                0..count
            } else {
                partition..partition + 1
            };
            // The frame's own declaration says which content its rows carry, and the kind is the
            // one the plan was built against: [`Self::build`] refused a frame that declares the
            // other, and the decoder chosen here re-reads the same declaration before a row.
            let decoded_input = input_frames_are_evaluated(input.schema().as_ref())?;
            for child_partition in partitions {
                let frames = input.execute(child_partition, Arc::clone(&context))?;
                if decoded_input {
                    // An evaluated frame carries values its producer already decided on the grid,
                    // so its chunks are queued at the input node as they are: there are no scan
                    // labels to materialize and no series to hold back. The stream is filed under
                    // the source it was read from, and every later chunk of it is handed over with
                    // that same index.
                    let stream = unpack_evaluated_series(
                        frames,
                        Some(Arc::clone(&self.series_schemas[source])),
                    )?;
                    evaluated[source].push(stream);
                } else {
                    let raw =
                        unpack_raw_series(frames, Some(Arc::clone(&self.series_schemas[source])))?;
                    let raw = materialize_raw_labels(raw)
                        .map_err(|error| DataFusionError::External(Box::new(error)))?;
                    streams.push((source, raw));
                }
            }
        }
        let input_ids = (0..self.series_schemas.len())
            .map(|source| state.input_ids(source).collect())
            .collect();
        let export_budget = Arc::new(Mutex::new(None));
        let partial_fold = self
            .partial_fold
            .clone()
            .map(|op| {
                PartialFold::try_new(
                    op,
                    self.tree
                        .root()
                        .output
                        .samples()
                        .ok_or_else(|| {
                            DataFusionError::Plan("FusedSeries output is not samples".into())
                        })?
                        .clone(),
                    Arc::clone(&self.tags),
                    Arc::clone(&export_budget),
                    &context.runtime_env().memory_pool,
                )
            })
            .transpose()?;
        let mut executor = NativeExecutor::new(state);
        let inputs = if evaluated.iter().any(|partitions| !partitions.is_empty()) {
            // Every evaluated source, each read through its own gather: its frames are contiguous
            // per partition by declaration, and the gather is what keeps a chain whole within that
            // source while none of its partitions waits on another. The gathers are charged
            // separately - the consumer's name carries the source *and* the output partition - so
            // one source's pressure is paid by its own reservation and never by another's.
            let gathers = evaluated
                .into_iter()
                .enumerate()
                .filter(|(_, partitions)| !partitions.is_empty())
                .map(|(source, partitions)| {
                    let reservation =
                        MemoryConsumer::new(format!("FusedSeriesEvaluated[{source}][{partition}]"))
                            .register(&context.runtime_env().memory_pool);
                    EvaluatedGather::new(source, partitions, reservation)
                })
                .collect();
            Some(FusedInputs::Evaluated(EvaluatedSources::new(gathers)))
        } else if streams.is_empty() {
            executor.end_of_input()?;
            None
        } else {
            let reservation = MemoryConsumer::new(format!("FusedSeriesGather[{partition}]"))
                .register(&context.runtime_env().memory_pool);
            Some(FusedInputs::Raw(SeriesGather::new(
                spawn_partitions(streams, &context.runtime_env().memory_pool),
                reservation,
            )))
        };
        // A frame is encoded from the tree's own evaluated output, and only when the node answers
        // values rather than state: partial state is a third output the format does not reach.
        let frame_mode =
            self.partial_fold.is_none() && self.format == FusedOutputFormat::SeriesFrame;
        let stream = FusedSeriesStream {
            inputs,
            executor: Some(executor),
            input_ids,
            classification: Arc::clone(&self.classification),
            tags: Arc::clone(&self.tags),
            output_schema: self.output_schema.clone(),
            partial_fold,
            frame_mode,
            _export_budget: Arc::clone(&export_budget),
            ready: VecDeque::new(),
            done: false,
        };
        if frame_mode {
            let schema = evaluated_output(&self.tree)?;
            let source: SendableEvaluatedSeriesStream =
                Box::pin(FusedEvaluatedStream { source: stream });
            Ok(pack_evaluated_series(source, schema)?)
        } else {
            Ok(Box::pin(stream))
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric.clone_inner())
    }
}

/// Converts one evaluated chunk into one output batch.
///
/// The time column is the absolute grid positions the chunk carries, missing samples are nulls and
/// ordinary NaN stays a value.
pub(crate) fn series_to_record_batch(
    classification: &[OutputColumn],
    tags: &[String],
    output_schema: &SchemaRef,
    chunk: &EvaluatedChunk,
) -> DataFusionResult<RecordBatch> {
    let series = &chunk.series;
    // A chunk that forms one run is read from its range, which lends the grid's buffers and so
    // allocates nothing. A chunk whose rows form several runs has no single range to read
    // ([`EvaluatedChunk::range`] answers `None` for it), and the positions between its runs are
    // holes rather than rows: the axis is walked for its carried rows instead, which is the one
    // traversal that visits exactly those.
    let timestamps = match chunk.range() {
        Some(range) => range.timestamps(),
        None => series
            .time
            .timestamps()
            .map(|timestamps| ScalarBuffer::from(timestamps.into_owned())),
    }
    .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    let rows = timestamps.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(classification.len());
    for (position, column) in classification.iter().enumerate() {
        match column {
            OutputColumn::Time => columns.push(Arc::new(TimestampMillisecondArray::new(
                timestamps.clone(),
                None,
            ))),
            OutputColumn::Field(index) => {
                let samples = series.fields.get(*index).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "evaluated series has no field at index {index}"
                    ))
                })?;
                if samples.len() != rows {
                    return Err(DataFusionError::Execution(format!(
                        "evaluated series field {index} has {} rows, time axis has {rows}",
                        samples.len()
                    )));
                }
                columns.push(field_column(samples)?);
            }
            OutputColumn::Tag(index) => {
                // `__name__` resolves to the metric name, every other tag through the label set.
                let name = tags.get(*index).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "output column {position} declares unavailable tag {index}"
                    ))
                })?;
                let value = series.metric.get(name);
                columns.push(Arc::new(StringArray::from(vec![value; rows])));
            }
        }
    }
    Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
}

/// Converts one evaluated series field into one `Float64` column.
///
/// Dense float samples are reused as they are: the array already carries exactly the right nulls
/// and keeps NaN a value, so copying it through `Samples::iter()` would only cost an allocation
/// and a re-encode. Chunked samples go through the iterator, and histograms have no float column.
fn field_column(samples: &Samples) -> DataFusionResult<ArrayRef> {
    match samples.dense() {
        Some(DenseSamplesRef::Float(values)) => Ok(Arc::new(values.clone())),
        Some(DenseSamplesRef::Histogram(_)) | Some(DenseSamplesRef::Mixed(_)) => {
            Err(DataFusionError::NotImplemented(
                "FusedSeriesExec cannot output histogram samples".into(),
            ))
        }
        None => {
            let values = samples
                .iter()
                .map(|sample| match sample {
                    SampleRef::Float(value) => Ok(Some(value)),
                    SampleRef::Missing => Ok(None),
                    SampleRef::Histogram(_) => Err(DataFusionError::NotImplemented(
                        "FusedSeriesExec cannot output histogram samples".into(),
                    )),
                })
                .collect::<DataFusionResult<Vec<_>>>()?;
            Ok(Arc::new(Float64Array::from(values)))
        }
    }
}

/// One chunk a child handed over, in the form the child's frame declared it in.
enum FusedChunk {
    /// Raw samples: one scan series, with its own timestamps.
    Raw(RawSeriesChunk),
    /// Values a producer already evaluated on the grid the frame declares.
    Evaluated(EvaluatedChunk),
}

/// One chunk and the source index it belongs to.
struct FusedFeed {
    source: usize,
    chunk: FusedChunk,
}

/// The input of one execution: its raw partitions gathered together, or its evaluated sources'
/// partitions read at once.
///
/// [`SeriesGather`] interleaves the partitions of every raw source while keeping every series
/// contiguous, because a series read from storage may span them. An evaluated source is gathered
/// the same way and for the same reason: its frames are contiguous within a partition by
/// declaration, and reading one partition to its end before the next would stall against an upstream
/// that routes by series into bounded queues, since the partition being read cannot end while the
/// read feeding it waits on a partition nobody reads. Each evaluated source gets a gather of its own
/// ([`EvaluatedSources`]), because what one source's chain holds back is only its own: the runtime
/// routes a chunk by the source index it is handed over with.
enum FusedInputs {
    Raw(SeriesGather),
    Evaluated(EvaluatedSources),
}

impl Stream for FusedInputs {
    type Item = Result<FusedFeed, BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut *self {
            Self::Raw(gather) => match Pin::new(gather).poll_next(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error))),
                Poll::Ready(Some(Ok((source, chunk)))) => Poll::Ready(Some(Ok(FusedFeed {
                    source,
                    chunk: FusedChunk::Raw(chunk),
                }))),
            },
            Self::Evaluated(sources) => Pin::new(sources).poll_next(cx),
        }
    }
}

/// Every evaluated source of one execution, read at once, one gather per source.
///
/// Each [`EvaluatedGather`] keeps one chain whole *within its own source*: a series' chunks arrive
/// contiguously, and a chunk of another series of that source is held back until its chain closes.
/// Sources do not constrain each other, and that is a fact about the runtime rather than a hope:
/// every chunk is queued at the input slots bound to its source index
/// ([`NativeExecutionState::input_ids`]), each slot belongs to exactly one source, and a slot's
/// chain rule is about the chunks queued at *it*. A chunk of source 1 handed over between two chunks
/// of source 0 therefore reaches a different slot and cannot break the chain the other slot holds,
/// so the two sources' chains may interleave.
///
/// What this type adds is the order the gathers are polled in. Every sweep starts at the source
/// after the one that last handed a chunk over, so a source that always has a chunk ready cannot
/// keep another from being polled - the same rotation [`EvaluatedGather`] keeps across one source's
/// partitions, extended across sources. The node ends only when every source ended: a source ending
/// while another still has frames is not the end, because the other's frames are still its answer.
struct EvaluatedSources {
    /// One gather per evaluated source, in source-index order.
    gathers: Vec<EvaluatedGather>,
    /// The gather polled first on the next sweep, so every source is reached even while one of them
    /// stays ready.
    next: usize,
}

impl EvaluatedSources {
    fn new(gathers: Vec<EvaluatedGather>) -> Self {
        Self { gathers, next: 0 }
    }
}

impl Stream for EvaluatedSources {
    type Item = Result<FusedFeed, BoxedError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let count = this.gathers.len();
        let mut ended = 0;
        for offset in 0..count {
            let index = (this.next + offset) % count;
            match Pin::new(&mut this.gathers[index]).poll_next(cx) {
                // Handed over under the index of the gather that read it: a chunk never leaves the
                // source it was read from, so nothing re-stamps it here.
                Poll::Ready(Some(Ok(feed))) => {
                    this.next = (index + 1) % count;
                    return Poll::Ready(Some(Ok(feed)));
                }
                // The first error ends the node, and every other source's streams are dropped with
                // it - and so are the reservations they hold - rather than read to their ends.
                Poll::Ready(Some(Err(error))) => {
                    this.gathers.clear();
                    this.next = 0;
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Ready(None) => ended += 1,
                // A source still waiting has registered this task's waker, so one of them holding a
                // read is the node still running.
                Poll::Pending => {}
            }
        }
        // A sweep that ends here either handed a chunk over (returned above) or found every source
        // ended or waiting, so the node ends exactly when every source ended. A source that ended
        // earlier is polled again and answers `None` again; that is what keeps a single source's
        // end from being read as the node's.
        if ended == count {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

/// Reads the partitions of one evaluated source at once without splitting a chain.
///
/// A series arrives whole: its chunks are contiguous, its runs increase and its identity does not
/// change before the chunk closing it. That holds for the chunks of one partition and not across
/// them - two partitions can hold two pieces of one label set, and a chunk of the other handed over
/// between them would break the contract the runtime checks - so exactly one chain is handed over at
/// a time. Every partition is polled, because an upstream that routes into bounded queues only makes
/// progress while its outputs are drained, and what a partition reads while another one's chain is
/// open is held back and handed over after it closes, one chain at a time.
///
/// Holding is bounded at one chain per partition, the same bound [`SeriesGather`] keeps: a partition
/// whose held chain ended stops being polled until that chain is handed over. Every held chunk is
/// charged to `reservation` until it leaves the gather.
struct EvaluatedGather {
    /// The source every partition of this gather belongs to: one gather is built per evaluated
    /// source ([`EvaluatedSources`]), so every chunk this gather hands over belongs to this source
    /// and to no other. [`FusedSeriesExec::build`] refuses an evaluated source beside a raw one.
    source: usize,
    partitions: Vec<EvaluatedPartition>,
    /// The partition whose open chain is being handed over, if any. Until it closes, what the other
    /// partitions read is held rather than handed over.
    open: Option<usize>,
    /// Chunks cleared for hand-over, in the order they must reach the runtime, with the bytes each
    /// is still charged.
    ready: VecDeque<(EvaluatedChunk, usize)>,
    /// The partition polled first on the next sweep, so every partition is reached even while one of
    /// them stays ready: the read behind a partition only starts once it is polled.
    next: usize,
    reservation: MemoryReservation,
}

/// One evaluated partition's stream and what it read while another partition's chain was open.
struct EvaluatedPartition {
    /// `None` once the stream ended.
    stream: Option<SendableEvaluatedSeriesStream>,
    /// Chunks of this stream's current chain, read while another partition's chain was open.
    held: Vec<(EvaluatedChunk, usize)>,
    /// Whether `held` ends with the chunk closing its chain.
    held_closed: bool,
}

impl EvaluatedGather {
    fn new(
        source: usize,
        partitions: Vec<SendableEvaluatedSeriesStream>,
        reservation: MemoryReservation,
    ) -> Self {
        Self {
            source,
            partitions: partitions
                .into_iter()
                .map(|stream| EvaluatedPartition {
                    stream: Some(stream),
                    held: Vec::new(),
                    held_closed: false,
                })
                .collect(),
            open: None,
            ready: VecDeque::new(),
            next: 0,
            reservation,
        }
    }

    /// Takes one chunk read from the partition at `index`: hands it over, or holds it back when a
    /// chain of another partition is open.
    fn take(&mut self, index: usize, chunk: EvaluatedChunk) -> Result<(), BoxedError> {
        match self.open {
            // Another partition's chain is open, so this chunk cannot be handed over yet: a chunk of
            // any other identity between two chunks of that chain is what the runtime refuses.
            Some(open) if open != index => {
                let bytes = series_bytes(&chunk.series);
                self.reservation
                    .try_grow(bytes)
                    .map_err(|error| BoxedError::new(common_query::error::Error::from(error)))?;
                let partition = &mut self.partitions[index];
                partition.held_closed = chunk.is_last;
                partition.held.push((chunk, bytes));
            }
            // Either nothing is open, or this chain is the open one and the chunk continues it.
            _ => {
                self.open = (!chunk.is_last).then_some(index);
                self.ready.push_back((chunk, 0));
                if self.open.is_none() {
                    self.release();
                }
            }
        }
        Ok(())
    }

    /// Clears held chains for hand-over, one chain at a time, while no chain is open.
    ///
    /// A chain leaves as whole as it was read: the chunks of one partition's held chain are cleared
    /// together, and a chain still open at the end of them becomes the open one, so the rest of it is
    /// then read from that partition alone.
    fn release(&mut self) {
        while self.open.is_none() {
            let Some(index) = self
                .partitions
                .iter()
                .position(|partition| !partition.held.is_empty())
            else {
                return;
            };
            let partition = &mut self.partitions[index];
            self.ready.extend(partition.held.drain(..));
            if !std::mem::take(&mut partition.held_closed) {
                self.open = Some(index);
            }
        }
    }

    fn fail(&mut self) {
        self.partitions.clear();
        self.ready.clear();
        self.reservation.free();
    }
}

impl Stream for EvaluatedGather {
    type Item = Result<FusedFeed, BoxedError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            // A held chain waits for nothing once no chain is open, so it is cleared before another
            // partition is polled and handed over one chunk per poll below.
            this.release();
            if let Some((chunk, bytes)) = this.ready.pop_front() {
                this.reservation.shrink(bytes);
                return Poll::Ready(Some(Ok(FusedFeed {
                    source: this.source,
                    chunk: FusedChunk::Evaluated(chunk),
                })));
            }
            let count = this.partitions.len();
            let mut live = false;
            let mut routed = false;
            for offset in 0..count {
                let index = (this.next + offset) % count;
                let partition = &mut this.partitions[index];
                // A partition whose held chain ended holds a whole series already: what it would
                // read next belongs after that chain and must not be read ahead of it.
                if partition.held_closed {
                    live = true;
                    continue;
                }
                let Some(stream) = partition.stream.as_mut() else {
                    continue;
                };
                match stream.as_mut().poll_next(cx) {
                    Poll::Pending => live = true,
                    Poll::Ready(None) => {
                        partition.stream = None;
                        let open_here = this.open == Some(index);
                        let holding = !partition.held.is_empty();
                        // The decoder below refuses a stream that ends with its series open, so this
                        // is a producer that bypassed it; left alone it would hold every other
                        // partition back forever.
                        if open_here || holding {
                            this.fail();
                            return Poll::Ready(Some(Err(BoxedError::new(
                                common_query::error::Error::from(DataFusionError::Execution(
                                    "evaluated series stream ended with its series open"
                                        .to_string(),
                                )),
                            ))));
                        }
                    }
                    Poll::Ready(Some(Err(error))) => {
                        this.fail();
                        return Poll::Ready(Some(Err(error)));
                    }
                    Poll::Ready(Some(Ok((series, is_last)))) => {
                        if let Err(error) = this.take(index, EvaluatedChunk { series, is_last }) {
                            this.fail();
                            return Poll::Ready(Some(Err(error)));
                        }
                        this.next = (index + 1) % count;
                        routed = true;
                        break;
                    }
                }
            }
            if routed {
                continue;
            }
            return if live {
                Poll::Pending
            } else {
                Poll::Ready(None)
            };
        }
    }
}

/// The batches one execution of [`FusedSeriesExec`] yields.
pub struct FusedSeriesStream {
    /// Every input partition, or `None` once they all ended.
    ///
    /// Each decoder holds its current batch, decoded chunks and upstream source. Dropping this on
    /// error releases them even if the consumer keeps the terminated stream.
    inputs: Option<FusedInputs>,
    /// The execution of this node's tree, or `None` once it has been closed.
    executor: Option<NativeExecutor>,
    /// Runtime input IDs for each source index.
    input_ids: Vec<Vec<InputId>>,
    /// Where each output column comes from.
    classification: Arc<[OutputColumn]>,
    /// The tag column names, indexed by `OutputColumn::Tag`.
    tags: Arc<[String]>,
    /// The schema of every batch this stream yields.
    output_schema: SchemaRef,
    /// Per-partition aggregate state fold, if this stream exports a partial result.
    partial_fold: Option<PartialFold>,
    /// Whether evaluated chunks are emitted as series frames rather than converted to rows.
    frame_mode: bool,
    /// Keeps exported state batches charged for as long as this stream holds them.
    _export_budget: Arc<Mutex<Option<MemoryReservation>>>,
    /// The batches the executor settled since the last poll of the input.
    ready: VecDeque<RecordBatch>,
    /// Whether the input ended and everything it settled has been handed out.
    done: bool,
}

impl FusedSeriesStream {
    /// Hands one chunk a child produced over and queues what it settled.
    fn push(&mut self, feed: FusedFeed) -> DataFusionResult<()> {
        let Some(executor) = self.executor.as_mut() else {
            return Err(DataFusionError::Execution(
                "FusedSeriesExec stream pushed a chunk after its execution closed".to_string(),
            ));
        };
        for input_id in self.input_ids.get(feed.source).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "FusedSeries has no runtime input IDs for source {}",
                feed.source
            ))
        })? {
            match &feed.chunk {
                // A raw chunk opens, continues or closes one series the scan read.
                FusedChunk::Raw(chunk) => executor.push_raw(*input_id, chunk.clone())?,
                // An evaluated chunk carries values a producer already decided, and the input node
                // it is queued at was compiled against a grid: the runtime refuses it anywhere else.
                FusedChunk::Evaluated(chunk) => {
                    executor.push_evaluated(*input_id, chunk.clone())?
                }
            }
        }
        self.settle()
    }

    /// Queues what the tree settled after the chunk just handed over, unless it answers frames.
    ///
    /// A frame-mode run hands its chunks out as the tree produces them, so nothing is drained here.
    fn settle(&mut self) -> DataFusionResult<()> {
        if self.frame_mode {
            return Ok(());
        }
        let Some(executor) = self.executor.as_mut() else {
            return Ok(());
        };
        let mut settled = Vec::new();
        while let Some(chunk) = executor.next_chunk()? {
            settled.push(chunk);
        }
        for chunk in settled {
            self.convert_rows(chunk)?;
        }
        Ok(())
    }

    /// Ends the input, so the tree settles what it was holding.
    ///
    /// `finish()` would be the same thing plus a drain, but the drain collects: it holds the whole
    /// answer in a `Vec` before anything is handed out, so a tree that answers many chunks after its
    /// input ends would sit on all of them while the consumer waits for the first. Only the input is
    /// ended here, and the answers are pulled one per call below, so each chunk leaves as the tree
    /// settles it and what has not been pulled yet stays charged to the node holding it.
    fn close_input(&mut self) -> DataFusionResult<()> {
        match self.executor.as_mut() {
            Some(executor) => executor.end_of_input(),
            // Every answer was pulled and the execution closed itself.
            None => Ok(()),
        }
    }

    /// Pulls the next answer the tree settles after its input ended, or `None` when it has none.
    ///
    /// One chunk per call is what keeps this a stream: collecting them first would hold the whole
    /// answer while the consumer waits for the first, and would charge the pool for all of it at
    /// once. `None` is also when the execution closes and returns its reservations, so it is
    /// dropped here rather than staying alive until the stream is.
    fn pull_one(&mut self) -> DataFusionResult<Option<EvaluatedChunk>> {
        let Some(executor) = self.executor.as_mut() else {
            return Ok(None);
        };
        match executor.next_chunk() {
            Ok(Some(chunk)) => Ok(Some(chunk)),
            Ok(None) => {
                self.executor = None;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// Ends the stream in an error, releasing the execution and the inputs.
    ///
    /// An ordinary drop of the stream releases all of this as well, so this is not about the last
    /// release: it is about a consumer that keeps the terminated stream, which would otherwise
    /// hold the evaluator, the decoder's held batch and chunks, and the plan upstream of it.
    fn fail(&mut self) {
        self.done = true;
        self.executor = None;
        self.partial_fold = None;
        self.inputs = None;
        // This reservation belongs only to this execution. Release it even if the consumer keeps
        // the failed stream and the plan alive.
        self._export_budget
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        // A batch queued before the error was settled by a run that has now stopped.
        self.ready.clear();
    }

    /// Converts one settled chunk into a row batch. Evaluated frames leave this driver as chunks.
    fn convert_rows(&mut self, chunk: EvaluatedChunk) -> DataFusionResult<()> {
        if let Some(fold) = self.partial_fold.as_mut() {
            fold.update(chunk)?;
            return Ok(());
        }
        let batch = series_to_record_batch(
            &self.classification,
            &self.tags,
            &self.output_schema,
            &chunk,
        )?;
        self.ready.push_back(batch);
        Ok(())
    }

    /// Publishes the partition's fold only after the input and native execution have drained.
    fn finish_fold(&mut self) -> DataFusionResult<()> {
        if let Some(mut fold) = self.partial_fold.take() {
            self.ready.extend(fold.finish()?);
        }
        Ok(())
    }
}

enum FusedOutput {
    Rows(RecordBatch),
    Evaluated(EvaluatedChunk),
}

impl FusedSeriesStream {
    fn poll_output(&mut self, cx: &mut Context<'_>) -> Poll<Option<DataFusionResult<FusedOutput>>> {
        for _ in 0..POLL_BUDGET {
            if let Some(batch) = self.ready.pop_front() {
                return Poll::Ready(Some(Ok(FusedOutput::Rows(batch))));
            }
            if self.frame_mode && !self.done {
                match self
                    .executor
                    .as_mut()
                    .map_or(Ok(None), NativeExecutor::next_chunk)
                {
                    Ok(Some(chunk)) => return Poll::Ready(Some(Ok(FusedOutput::Evaluated(chunk)))),
                    Ok(None) if self.inputs.is_none() => self.executor = None,
                    Ok(None) => {}
                    Err(error) => {
                        self.fail();
                        return Poll::Ready(Some(Err(error)));
                    }
                }
            }
            if self.done {
                return Poll::Ready(None);
            }
            let Some(inputs) = self.inputs.as_mut() else {
                match self.pull_one() {
                    Ok(Some(chunk)) if self.frame_mode => {
                        return Poll::Ready(Some(Ok(FusedOutput::Evaluated(chunk))));
                    }
                    Ok(Some(chunk)) => {
                        if let Err(error) = self.convert_rows(chunk) {
                            self.fail();
                            return Poll::Ready(Some(Err(error)));
                        }
                    }
                    Ok(None) => {
                        if let Err(error) = self.finish_fold() {
                            self.fail();
                            return Poll::Ready(Some(Err(error)));
                        }
                        self.done = true;
                        continue;
                    }
                    Err(error) => {
                        self.fail();
                        return Poll::Ready(Some(Err(error)));
                    }
                }
                continue;
            };
            match Pin::new(inputs).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    self.inputs = None;
                    if let Err(error) = self.close_input() {
                        self.fail();
                        return Poll::Ready(Some(Err(error)));
                    }
                }
                Poll::Ready(Some(Err(error))) => {
                    self.fail();
                    return Poll::Ready(Some(Err(DataFusionError::External(Box::new(error)))));
                }
                Poll::Ready(Some(Ok(feed))) => {
                    if let Err(error) = self.push(feed) {
                        self.fail();
                        return Poll::Ready(Some(Err(error)));
                    }
                }
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

impl RecordBatchStream for FusedSeriesStream {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

impl Stream for FusedSeriesStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.poll_output(cx) {
            Poll::Ready(Some(Ok(FusedOutput::Rows(batch)))) => Poll::Ready(Some(Ok(batch))),
            Poll::Ready(Some(Ok(FusedOutput::Evaluated(_)))) => {
                self.fail();
                Poll::Ready(Some(Err(DataFusionError::Internal(
                    "evaluated chunk reached the row stream adapter".into(),
                ))))
            }
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

struct FusedEvaluatedStream {
    source: FusedSeriesStream,
}

impl Stream for FusedEvaluatedStream {
    type Item = Result<(datatypes::series::samples::Series, bool), common_error::ext::BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.source).poll_output(cx) {
            Poll::Ready(Some(Ok(FusedOutput::Evaluated(chunk)))) => {
                Poll::Ready(Some(Ok((chunk.series, chunk.is_last))))
            }
            Poll::Ready(Some(Ok(FusedOutput::Rows(_)))) => {
                self.source.fail();
                Poll::Ready(Some(Err(common_error::ext::BoxedError::new(
                    common_query::error::Error::from(DataFusionError::Internal(
                        "row batch reached the evaluated stream adapter".into(),
                    )),
                ))))
            }
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(
                common_error::ext::BoxedError::new(common_query::error::Error::from(error)),
            ))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
