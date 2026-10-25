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

//! One node that runs a compiled native tree over raw series.
//!
//! [`FusedSeries`] is the logical half: it holds the PromQL text the tree was compiled from, the
//! tree itself and the binding its input was compiled against. [`FusedSeriesExec`] is the physical
//! half: it owns one execution of that tree per partition and decodes its input frames into raw
//! series chunks and converts its evaluated chunks back into batches.
//!
//! The two halves show different things in `EXPLAIN`, which is the point of having both. The
//! logical node prints the query a reader wrote ([`FusedSeries`]'s `fmt_for_explain`), and the
//! physical node prints the tree it will run ([`FusedSeriesExec`]'s `fmt_as`, which embeds
//! [`NativeTree`]'s own display rather than a second renderer). A reader comparing the two plans
//! can therefore see whether a query was compiled the way they expected without decoding node
//! names.
//!
//! Scope of this layer: the input is a stream of native raw series frames
//! ([`raw_series_frame_schema`](datatypes::series::frame::raw_series_frame_schema)) — one row per
//! chunk, carrying the series' encoded key, its finality, its timestamps and its fields, plus the
//! complete labels when the frame is explicit — with each series' rows contiguous and its
//! timestamps ascending, which is what the caller's [`SeriesOrdering`] names. The output is one
//! batch per evaluated chunk, with the statement's grid timestamps in the declared time column.
//! Everything the tree cannot do is rejected when the execution plan is built, not when the first
//! batch arrives: a tree the native runtime does not execute, a frame whose schema the tree's input
//! was not compiled against, or an output schema the conversion cannot classify.

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fmt::{self, Formatter};
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use common_query::series_frame::{ensure_frame_matches_label_source, unpack_raw_series};
use datafusion::arrow::array::{ArrayRef, Float64Array, StringArray, TimestampMillisecondArray};
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DFSchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::execution::memory_pool::UnboundedMemoryPool;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, InputDistributionRequirements,
    Partitioning, PlanProperties, RecordBatchStream, SendableRecordBatchStream,
};
use datatypes::series::SeriesOrdering;
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::{DenseSamplesRef, SampleRef, Samples, SeriesSchema};
use datatypes::series::time::EvalGrid;
use futures::Stream;
use store_api::region_engine::{RawLabelSource, SendableRawSeriesStream};

use crate::evaluated::EvaluatedChunk;
use crate::native::execute::{InputId, NativeExecutionState, NativeExecutor};
use crate::native::plan::{NativeExpr, NativeNode, NativeTree, NativeType};
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
/// evaluated chunks and the names of their output columns; the logical node checks that the root
/// produces values on that grid. Raw frames carry each series' labels and timestamps, rather than
/// reading them from these output names.
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
    /// The logical tag names the output carries, read from each series' labels.
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
        validate_binding(&tree, &binding, children.len())?;
        Ok(Self {
            promql,
            tree,
            binding,
            children,
            output_schema,
        })
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
    /// rejects unsupported multi-source physical execution, an incompatible frame, or an output
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
        Ok(Arc::new(FusedSeriesExec::try_new(
            Arc::clone(&self.promql),
            inputs,
            Arc::new(self.tree.clone()),
            output_schema,
            self.binding.time_index.clone(),
            self.binding.tags.clone(),
        )?))
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
        write!(f, "FusedSeries: {}", self.promql)
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
    }
}

impl Eq for FusedSeries {}

impl Hash for FusedSeries {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.promql.hash(state);
        self.binding.hash(state);
        self.children.hash(state);
        self.output_schema.hash(state);
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

/// The schemas read by distinct raw sources, in dense source-index order.
fn collect_source_schemas(tree: &NativeTree) -> DataFusionResult<Vec<Arc<SeriesSchema>>> {
    let mut schemas: Vec<(usize, Arc<SeriesSchema>)> = Vec::new();
    for input in tree_inputs(tree) {
        let NativeType::Samples(found) = &input.output else {
            return Err(DataFusionError::Plan(
                "FusedSeries input is not samples".into(),
            ));
        };
        if found.eval_grid.is_some() {
            return Err(DataFusionError::Plan(
                "FusedSeries input is not raw samples".into(),
            ));
        }
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
            | NativeExpr::Map { input, .. }
            | NativeExpr::Aggregate { input, .. }
            | NativeExpr::Vector { input, .. } => collect(input, inputs),
            NativeExpr::Binary { left, right, .. } => {
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

/// Fields read by each distinct raw source in source-index order.
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

fn validate_physical_tree(tree: &NativeTree) -> DataFusionResult<Arc<SeriesSchema>> {
    let inputs = tree_inputs(tree);
    if inputs.is_empty() || inputs.iter().any(|input| input.source_index != 0) {
        return Err(DataFusionError::Plan(
            "FusedSeries physical plan requires one raw source index 0".into(),
        ));
    }
    let schemas = collect_source_schemas(tree)?;
    let [schema] = schemas.as_slice() else {
        return Err(DataFusionError::Plan(
            "FusedSeries physical plan requires one raw source".into(),
        ));
    };
    if !tree
        .root()
        .output
        .samples()
        .is_some_and(|schema| schema.eval_grid.is_some())
    {
        return Err(DataFusionError::Plan(
            "FusedSeries output is not evaluated samples".into(),
        ));
    }
    Ok(Arc::clone(schema))
}

/// Where one output column of [`FusedSeriesExec`] comes from.
#[derive(Clone, Copy, Debug)]
enum OutputColumn {
    /// The evaluation grid timestamps of the current chunk.
    Time,
    /// Series field by index in the bound series schema.
    Field(usize),
    /// Declared logical tag by index in the caller-supplied tag list.
    Tag(usize),
}

/// Classifies every output column, rejecting anything the conversion cannot produce.
fn classify_output(
    output_schema: &SchemaRef,
    series_schema: &SeriesSchema,
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
        if series_schema
            .fields
            .iter()
            .any(|field| field.name.as_ref() == tag)
        {
            return Err(DataFusionError::Plan(format!(
                "tag column {tag:?} of FusedSeriesExec clashes with a series field"
            )));
        }
    }

    let mut time_seen = false;
    let mut field_seen = vec![false; series_schema.fields.len()];
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
        if let Some(index) = series_schema
            .fields
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
                series_schema.fields[index].name
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

/// The physical node that runs a compiled native tree over its input's raw series.
pub struct FusedSeriesExec {
    /// The PromQL text the tree was compiled from, which is what the plan line shows.
    promql: Arc<str>,
    /// The tree this node runs, shared by every partition's execution.
    tree: Arc<NativeTree>,
    /// The raw series schema the tree's input was compiled against, which every frame is decoded
    /// against.
    series_schema: Arc<SeriesSchema>,
    /// How the input frames name their series: labels encoded in the key, or carried along.
    label_source: RawLabelSource,
    /// The ordering the input frames guarantee.
    ordering: SeriesOrdering,
    /// Where each output column comes from.
    classification: Arc<[OutputColumn]>,
    /// The output column holding the grid timestamps, kept so this node can be rebuilt over a new
    /// child without losing which column its time came from.
    time_column: String,
    /// The tag column names, indexed by [`OutputColumn::Tag`].
    tags: Arc<[String]>,
    /// The plan producing the raw series frames.
    input: Arc<dyn ExecutionPlan>,
    /// The columns this node produces.
    output_schema: SchemaRef,
    /// Per-node metrics, registered by each execution of the tree.
    metric: ExecutionPlanMetricsSet,
    /// This node's partitioning, emission and boundedness.
    properties: Arc<PlanProperties>,
}

impl FusedSeriesExec {
    /// Creates the node, rejecting whatever this node cannot execute or convert.
    ///
    /// `input` produces native raw series frames: the series' encoded key, its finality, its
    /// timestamps and its fields, plus the complete labels when `label_source` is
    /// [`RawLabelSource::Explicit`]. `time_column` and `tags` name the columns of the output, which
    /// is a row table rather than a frame.
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
        let [(input, label_source, ordering)] = inputs.as_slice() else {
            return Err(DataFusionError::Plan(format!(
                "FusedSeriesExec requires exactly one physical source, got {}",
                inputs.len()
            )));
        };
        let input = Arc::clone(input);
        let label_source = label_source.clone();
        let ordering = *ordering;
        let raw_schema = validate_physical_tree(&tree)?;
        // Building a state is what proves the runtime executes this tree: a node kind it does not
        // run yet is refused here, so a plan that is built is a plan that starts. The throwaway
        // state is dropped at once, returning its (unbounded) reservations.
        let _ = NativeExecutionState::try_new(&tree, Arc::new(UnboundedMemoryPool::default()))?;
        let output_series = tree
            .root()
            .output
            .samples()
            .ok_or_else(|| DataFusionError::Plan("FusedSeries output is not samples".into()))?;
        let classification = classify_output(&output_schema, output_series, &tags, &time_column)?;
        let input_properties = input.properties();
        let partition_count = input_properties.partitioning.partition_count();
        if partition_count == 0 {
            return Err(DataFusionError::Plan(
                "FusedSeriesExec input has no partitions".into(),
            ));
        }
        if tree_inputs(&tree)
            .iter()
            .any(|input| !ordering_satisfies(ordering, input.ordering))
        {
            return Err(DataFusionError::Plan(
                "FusedSeries producer ordering does not satisfy every input".into(),
            ));
        }
        // The frame states its own label layout and series ordering; this plan's binding is an
        // expectation checked against that declaration, not an input to decoding.
        ensure_frame_matches_label_source(input.schema().as_ref(), &label_source)?;
        // The frame is checked against what the tree's input reads, so a frame this node cannot
        // decode is reported when the plan is built. The shared decoder is the authority on that
        // layout, so it is asked rather than copied: handing it an empty stream of this schema makes
        // it validate the declared frame with no batch to decode, which is the check
        // `unpack_raw_series` makes before its first poll too.
        unpack_raw_series(
            Box::pin(RecordBatchStreamAdapter::new(
                input.schema(),
                futures::stream::empty(),
            )),
            Some(raw_schema.clone()),
        )?;
        // One evaluator per input partition, and the input's own partitioning kept as it is.
        //
        // A window holds the state of the series it is reading and closes it on the chunk carrying
        // `is_last`, so it can answer a series only if the whole series arrives in the partition it
        // reads. The input guarantees that: a raw series frame is produced by `SeriesScan`, whose
        // contract is that the same primary key always reaches the same partition, and the frame
        // decoder holds each series' chunks contiguous within that partition.
        //
        // What is *not* asked of the optimizer is a distribution. A `KeyPartitioned` or
        // `HashPartitioned` requirement is classed as a hash requirement, and a hash requirement
        // makes a hash exchange necessary whenever the child has more than one partition - even if
        // the child already satisfies it, and even when the partition counts match. A hash exchange
        // does not preserve order, so it would interleave the chunks of one series and the decoder
        // would close a series it has not finished. `UnspecifiedDistribution` leaves the child's
        // layout alone, and `benefits_from_input_partitioning` is false so no round-robin is added
        // to raise parallelism either. Between them no exchange can appear between the scan and this
        // node, and the scan's guarantee arrives intact.
        // As many partitions as the input hands over: the node runs one evaluator per partition and
        // answers each as it settles.
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(partition_count),
            input_properties.emission_type,
            input_properties.boundedness,
        ));
        Ok(Self {
            promql,
            tree,
            series_schema: raw_schema,
            label_source,
            ordering,
            classification: classification.into(),
            time_column,
            tags: tags.into(),
            input,
            output_schema,
            metric: ExecutionPlanMetricsSet::new(),
            properties,
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
        write!(f, "FusedSeriesExec: {}\n{}", self.promql, self.tree)
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
        InputDistributionRequirements::new(vec![Distribution::UnspecifiedDistribution])
    }

    /// Splitting the input further would not help: a second partition is a second set of series
    /// answered by a second tree, which this node already runs, and raising the count is what an
    /// optimizer does by adding a round-robin exchange that reorders nothing it can rely on.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
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
        let [child] = <[Arc<dyn ExecutionPlan>; 1]>::try_from(children).map_err(|children| {
            DataFusionError::Plan(format!(
                "FusedSeriesExec requires exactly one child, got {}",
                children.len()
            ))
        })?;
        // Rebuilt rather than patched: the new child's schema is what the frame is validated
        // against, so a child that changed shape is refused instead of decoded with a stale layout.
        Ok(Arc::new(Self::try_new(
            Arc::clone(&self.promql),
            vec![(child, self.label_source.clone(), self.ordering)],
            Arc::clone(&self.tree),
            self.output_schema.clone(),
            self.time_column.clone(),
            self.tags.to_vec(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, Arc::clone(&context))?;
        // Batches become raw series chunks through the shared frame decoder, which is the inverse of
        // what the producer packed: one chunk per frame row, each series closed exactly once by the
        // row carrying `is_last`.
        let input = unpack_raw_series(input, Some(Arc::clone(&self.series_schema)))?;
        // A source bound in key mode carries its identity encoded and no decoded labels, and a chunk
        // handed to the tree has to name the series it holds.
        let input =
            materialize_raw_labels(input).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let state = NativeExecutionState::try_new(
            &self.tree,
            Arc::clone(&context.runtime_env().memory_pool),
        )?;
        state.register_metrics(&self.metric, partition);
        let input_ids = state.input_ids(0).collect();
        Ok(Box::pin(FusedSeriesStream {
            input: Some(input),
            executor: Some(NativeExecutor::new(state)),
            input_ids,
            classification: Arc::clone(&self.classification),
            tags: Arc::clone(&self.tags),
            output_schema: self.output_schema.clone(),
            ready: VecDeque::new(),
            done: false,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric.clone_inner())
    }
}

/// Converts one evaluated chunk into one output batch.
///
/// The time column is the absolute grid positions the chunk carries, missing samples are nulls and
/// ordinary NaN stays a value.
fn series_to_record_batch(
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

/// The batches one execution of [`FusedSeriesExec`] yields.
pub struct FusedSeriesStream {
    /// The input frames, read as raw series chunks, or `None` once the stream ended in an error.
    ///
    /// The decoder behind it holds its current batch, the chunks it decoded from it and its own
    /// source, so a terminated stream that kept it alive would hold upstream resources until the
    /// consumer drops the stream.
    input: Option<SendableRawSeriesStream>,
    /// The execution of this node's tree, or `None` once it has been closed.
    executor: Option<NativeExecutor>,
    /// Runtime inputs bound to the sole physical child.
    input_ids: Vec<InputId>,
    /// Where each output column comes from.
    classification: Arc<[OutputColumn]>,
    /// The tag column names, indexed by `OutputColumn::Tag`.
    tags: Arc<[String]>,
    /// The schema of every batch this stream yields.
    output_schema: SchemaRef,
    /// The batches the executor settled since the last poll of the input.
    ready: VecDeque<RecordBatch>,
    /// Whether the input ended and everything it settled has been handed out.
    done: bool,
}

impl FusedSeriesStream {
    /// Hands one raw chunk over and queues what it settled.
    fn push(&mut self, chunk: RawSeriesChunk) -> DataFusionResult<()> {
        let Some(executor) = self.executor.as_mut() else {
            return Err(DataFusionError::Execution(
                "FusedSeriesExec stream pushed a chunk after its execution closed".to_string(),
            ));
        };
        for input_id in &self.input_ids {
            executor.push_raw(*input_id, chunk.clone())?;
        }
        let mut settled = Vec::new();
        while let Some(chunk) = executor.next_chunk()? {
            settled.push(chunk);
        }
        self.convert(settled)
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

    /// Ends the stream in an error, releasing the execution and the input at once.
    ///
    /// An ordinary drop of the stream releases all of this as well, so this is not about the last
    /// release: it is about a consumer that keeps the terminated stream, which would otherwise
    /// hold the evaluator, the decoder's held batch and chunks, and the plan upstream of it.
    fn fail(&mut self) {
        self.done = true;
        self.executor = None;
        self.input = None;
        // A batch queued before the error was settled by a run that has now stopped.
        self.ready.clear();
    }

    /// Converts the chunks the executor settled into batches, in the order it produced them.
    fn convert(&mut self, chunks: Vec<EvaluatedChunk>) -> DataFusionResult<()> {
        for chunk in &chunks {
            let batch = series_to_record_batch(
                &self.classification,
                &self.tags,
                &self.output_schema,
                chunk,
            )?;
            self.ready.push_back(batch);
        }
        Ok(())
    }
}

impl RecordBatchStream for FusedSeriesStream {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

impl Stream for FusedSeriesStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<DataFusionResult<RecordBatch>>> {
        let this = &mut *self;
        // A batch that is already settled is handed over before anything is read, so a bounded
        // poll never delays one that is ready behind more input.
        for _ in 0..POLL_BUDGET {
            if let Some(batch) = this.ready.pop_front() {
                return Poll::Ready(Some(Ok(batch)));
            }
            if this.done {
                return Poll::Ready(None);
            }
            // An error is the only thing that clears the input, and it ends the stream with it.
            let Some(input) = this.input.as_mut() else {
                // The input ended, so what the tree answers now comes from what it held. One chunk
                // per iteration keeps it a stream, and `None` is both "nothing more" and the point
                // where the execution releases its reservations.
                match this.pull_one() {
                    Ok(Some(chunk)) => {
                        if let Err(error) = this.convert(vec![chunk]) {
                            this.fail();
                            return Poll::Ready(Some(Err(error)));
                        }
                    }
                    Ok(None) => {
                        this.done = true;
                        continue;
                    }
                    Err(error) => {
                        this.fail();
                        return Poll::Ready(Some(Err(error)));
                    }
                }
                continue;
            };
            let polled = input.as_mut().poll_next(cx);
            match polled {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    // The input ended, so every series it opened has to be closed. The tree settles
                    // what it was holding, and the answers are pulled below in this same poll
                    // rather than collected first.
                    this.input = None;
                    if let Err(error) = this.close_input() {
                        this.fail();
                        return Poll::Ready(Some(Err(error)));
                    }
                }
                Poll::Ready(Some(Err(error))) => {
                    // An input error stops the execution: a stopped evaluator must not be reused.
                    this.fail();
                    return Poll::Ready(Some(Err(DataFusionError::External(Box::new(error)))));
                }
                Poll::Ready(Some(Ok(chunk))) => {
                    if let Err(error) = this.push(chunk) {
                        this.fail();
                        return Poll::Ready(Some(Err(error)));
                    }
                }
            }
        }
        // The budget ran out on input that settled nothing. Everything read so far is kept, so the
        // next poll continues where this one stopped; waking first is what asks for that poll.
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}
