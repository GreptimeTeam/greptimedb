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
//! batch per evaluated chunk, with the statement's grid timestamps in the declared time column -
//! unless the plan was built for [`FusedOutputFormat::SeriesFrame`], which hands the same evaluated
//! chunks over as the unified series frame instead of flattening them into rows. Which of the two a
//! node answers in is fixed by the plan, not by the run.
//! Everything the tree cannot do is rejected when the execution plan is built, not when the first
//! batch arrives: a tree the native runtime does not execute, a frame whose schema the tree's input
//! was not compiled against, or an output schema the conversion cannot classify.

use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::fmt::{self, Formatter};
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use common_query::series_frame::{
    ensure_frame_matches_label_source, pack_evaluated_series, unpack_raw_series,
};
use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, StringArray, TimestampMillisecondArray,
};
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryPool, MemoryReservation, UnboundedMemoryPool,
};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, InputDistributionRequirements,
    Partitioning, PlanProperties, RecordBatchStream, SendableRecordBatchStream,
};
use datatypes::series::SeriesOrdering;
use datatypes::series::frame::{FrameLabelMode, SeriesFrameBuilder, SeriesFrameCursor};
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::{DenseSamplesRef, SampleRef, Samples, Series, SeriesSchema};
use datatypes::series::time::EvalGrid;
use futures::Stream;
use store_api::region_engine::{RawLabelSource, SendableRawSeriesStream};

use crate::evaluated::EvaluatedChunk;
use crate::grid_series::{LabelSet, label_set};
use crate::native::execute::{NativeExecutionState, NativeExecutor};
use crate::native::metrics::{self, NodePath};
use crate::native::plan::{NativeExpr, NativeTree, NativeType};
use crate::native::runs::{Run, absorb};
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

/// The two formats this node hands its answer over in, fixed when the plan is built.
///
/// The choice is part of the plan rather than of the run: it decides the node's output schema, so it
/// has to be part of the node's identity, its `EXPLAIN` text and its equality with another node -
/// two plans that answer the same rows in different formats are not the same plan.
///
/// [`PlainRows`](Self::PlainRows) is the row table a query returns: the grid timestamps, one column
/// per series field and one per declared tag. [`SeriesFrame`](Self::SeriesFrame) is the unified
/// evaluated series frame ([`datatypes::series::frame`]), which keeps each series' labels and its
/// sparse grid positions instead of flattening them into rows. The row table is what production
/// queries use; the frame exists for a consumer that needs to tell series and positions apart, and
/// is not reachable from a query yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum FusedOutputFormat {
    /// One row per carried grid position: the timestamps, the fields and the declared tags.
    PlainRows,
    /// One evaluated series frame batch per bounded group of chunks, per series.
    SeriesFrame,
}

impl FusedOutputFormat {
    /// How `EXPLAIN` spells this format, when the text has to say more than the query.
    const fn as_str(self) -> Option<&'static str> {
        match self {
            Self::PlainRows => None,
            Self::SeriesFrame => Some("series_frame"),
        }
    }
}

/// The evaluated schema of the tree's output, which a [`FusedOutputFormat::SeriesFrame`] output is
/// encoded from.
///
/// The tree's own root is the authority on it, not the schema the input was scanned with and not the
/// tags the binding declares: a label rewrite or an aggregate changes what the result names, and a
/// frame that carried the input's schema would describe a series that no longer exists.
///
/// Only samples on an evaluation grid can be framed. Aggregate state is a contribution to a later
/// merge rather than a value, and a scalar has no series to name, so neither can be written as the
/// float samples an evaluated frame carries; the refusal happens while the plan is built, so a plan
/// that is built is a plan that runs.
fn evaluated_output(tree: &NativeTree) -> DataFusionResult<Arc<SeriesSchema>> {
    match &tree.root().output {
        NativeType::Samples(schema) if schema.eval_grid.is_some() => Ok(Arc::clone(schema)),
        NativeType::Samples(_) => Err(DataFusionError::Plan(
            "FusedSeries cannot output a series frame of a tree whose output is raw samples: an \
             evaluated frame carries positions on the statement's grid"
                .to_string(),
        )),
        NativeType::AggregateState(_) => Err(DataFusionError::Plan(
            "FusedSeries cannot output a series frame of a tree whose root is aggregate state: \
             partial state is a contribution to a later merge, not a value, and writing it as \
             evaluated float samples would disguise it as one"
                .to_string(),
        )),
        NativeType::Scalar(_) => Err(DataFusionError::Plan(
            "FusedSeries cannot output a series frame of a tree whose root is a scalar: a scalar \
             has no series to name, so it cannot be written as evaluated float samples"
                .to_string(),
        )),
    }
}

/// The schema a [`FusedOutputFormat::SeriesFrame`] output declares for `schema`.
///
/// It is the codec's own declaration of an evaluated frame - the layout, the grid metadata and the
/// ordering it promises - asked for rather than spelled a second time here: packing an empty stream
/// is how this layer reads that declaration back, and nothing about it is derived from the input or
/// from the binding.
fn series_frame_output_schema(schema: &SeriesSchema) -> DataFusionResult<SchemaRef> {
    let declared =
        pack_evaluated_series(Box::pin(futures::stream::empty()), Arc::new(schema.clone()))
            .map_err(|error| {
                DataFusionError::Plan(format!(
                    "FusedSeries cannot declare a series frame output: {error}"
                ))
            })?;
    Ok(declared.schema())
}

/// What the tree's input was compiled against, and how its output columns are named.
///
/// The binding travels with the tree: a tree is legal only against the schema it was bound to, and
/// a reader of the output needs to know which grid a chunk's absolute positions are on. Keeping it
/// in one value is what lets the logical node be built from the same description, and the physical
/// node reads the same facts back off the tree rather than trusting a second copy of them.
///
/// What the logical node checks is that the schema the tree's input node declares is the one this
/// binding names. What it cannot check is whether the caller meant *some other* schema with the
/// same field names, or whether [`time_index`](Self::time_index) is the column the query reports
/// positions in: from here both are indistinguishable from a correct binding, so picking them is
/// the caller's obligation. The physical node reads neither the metric name nor a time column from
/// the input — a raw frame carries the labels of each series and its own timestamps.
#[derive(Clone, Debug)]
pub struct FusedSeriesBinding {
    /// The raw series schema the tree's input node was bound to, without an evaluation grid.
    pub series_schema: Arc<SeriesSchema>,
    /// The statement's grid, which is the grid every evaluated chunk is addressed on.
    pub grid: Arc<EvalGrid>,
    /// The metric name every input series is bound to, when the input reads one named metric.
    ///
    /// The logical node requires it when [`tags`](Self::tags) declares [`METRIC_NAME`]: a reader of
    /// the output has to be able to tell that a range function dropped the name, which needs a name
    /// to drop. The physical node takes the names from the frames, which carry each series' own.
    pub metric_name: Option<Arc<str>>,
    /// The output column holding the grid timestamps, in a row-table output.
    pub time_index: String,
    /// The logical tag names the output carries, read from each series' labels.
    ///
    /// [`METRIC_NAME`] is special: it is resolved to the series' metric name instead of a column,
    /// which is how a reader sees that a range function dropped it. A series frame carries the
    /// complete labels of each series rather than these columns, so it reads neither this list nor
    /// [`time_index`](Self::time_index); both stay on the binding because a plan that has not fixed
    /// its format yet still has to describe the row table it could answer in.
    pub tags: Vec<String>,
}

impl FusedSeriesBinding {
    /// Whether `other` binds the same fields under the same names.
    fn same_fields(&self, other: &SeriesSchema) -> bool {
        self.series_schema.fields.len() == other.fields.len()
            && self
                .series_schema
                .fields
                .iter()
                .zip(other.fields.iter())
                .all(|(mine, theirs)| mine.name == theirs.name)
    }
}

impl PartialEq for FusedSeriesBinding {
    fn eq(&self, other: &Self) -> bool {
        // The schemas are compared by the field names they declare, and the grids by their spec:
        // neither type has structural equality, and neither identity nor allocation is meant here.
        self.metric_name == other.metric_name
            && self.time_index == other.time_index
            && self.tags == other.tags
            && self.grid.spec() == other.grid.spec()
            && self.same_fields(&other.series_schema)
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
        for field in self.series_schema.fields.iter() {
            field.name.hash(state);
        }
    }
}

impl PartialOrd for FusedSeriesBinding {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // A total order over the same fields equality reads, so equal bindings compare equal.
        match self
            .metric_name
            .as_deref()
            .partial_cmp(&other.metric_name.as_deref())
        {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        match self.time_index.partial_cmp(&other.time_index) {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        match self.tags.partial_cmp(&other.tags) {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        let (mine, theirs) = (self.grid.spec(), other.grid.spec());
        match (mine.start_ms, mine.step_ms, mine.len).partial_cmp(&(
            theirs.start_ms,
            theirs.step_ms,
            theirs.len,
        )) {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        Some(
            self.series_schema
                .fields
                .iter()
                .map(|field| field.name.as_ref())
                .cmp(
                    other
                        .series_schema
                        .fields
                        .iter()
                        .map(|field| field.name.as_ref()),
                ),
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
    /// The plan producing the input series.
    input: LogicalPlan,
    /// The columns this node produces, in the shape [`format`](Self::format) asks for.
    output_schema: DFSchemaRef,
    /// Which of the two formats this node answers in, fixed when the plan is built.
    format: FusedOutputFormat,
}

impl FusedSeries {
    /// The name this node is shown under.
    pub const fn name() -> &'static str {
        "FusedSeries"
    }

    /// Creates the node, checking that `tree` is legal against `binding`.
    ///
    /// `output_schema` is the row table the node answers in, and is what
    /// [`format`](FusedOutputFormat::PlainRows) declares. A
    /// [`SeriesFrame`](FusedOutputFormat::SeriesFrame) output has no rows to describe, so its
    /// schema is the codec's own declaration for the tree's evaluated output and is derived here;
    /// a tree whose root the frame cannot carry is refused now, while the plan is built.
    pub fn try_new(
        promql: Arc<str>,
        tree: NativeTree,
        binding: FusedSeriesBinding,
        input: LogicalPlan,
        output_schema: DFSchemaRef,
        format: FusedOutputFormat,
    ) -> DataFusionResult<Self> {
        let output_schema = match format {
            FusedOutputFormat::PlainRows => output_schema,
            FusedOutputFormat::SeriesFrame => {
                let schema = series_frame_output_schema(evaluated_output(&tree)?.as_ref())?;
                Arc::new(DFSchema::try_from(schema)?)
            }
        };
        validate_binding(&tree, &binding)?;
        Ok(Self {
            promql,
            tree,
            binding,
            input,
            output_schema,
            format,
        })
    }

    /// Which format this node answers in.
    pub fn format(&self) -> FusedOutputFormat {
        self.format
    }

    /// The PromQL text this node was compiled from.
    pub fn promql(&self) -> &str {
        &self.promql
    }

    /// The compiled tree this node runs.
    pub fn tree(&self) -> &NativeTree {
        &self.tree
    }

    /// What the tree's input was compiled against.
    pub fn binding(&self) -> &FusedSeriesBinding {
        &self.binding
    }

    /// The physical node that runs `exec_input` as this node's input.
    ///
    /// `label_source` and `ordering` are the binding of the frames `exec_input` produces: the
    /// frame carries the encoded key and, in explicit mode, the complete labels, but no schema can
    /// say which of the two a producer meant or how it ordered the series, so the caller that built
    /// the frame passes both on. Building the node rejects a tree the runtime cannot execute, a
    /// frame schema the tree's input was not compiled against, and a schema the conversion cannot
    /// classify, so a plan that is built is a plan that runs.
    pub fn to_execution_plan(
        &self,
        exec_input: Arc<dyn ExecutionPlan>,
        label_source: RawLabelSource,
        ordering: SeriesOrdering,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let output_schema: SchemaRef = self.output_schema.inner().clone();
        Ok(Arc::new(FusedSeriesExec::try_new(
            Arc::clone(&self.promql),
            exec_input,
            label_source,
            ordering,
            Arc::new(self.tree.clone()),
            output_schema,
            self.binding.time_index.clone(),
            self.binding.tags.clone(),
            self.format,
        )?))
    }
}

impl UserDefinedLogicalNodeCore for FusedSeries {
    fn name(&self) -> &str {
        Self::name()
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
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
        // The row table is what a reader expects of the node, so a plain-rows plan says nothing
        // extra about it; a frame output is named, because the text is the only place a reader can
        // see that the same query answers something other than rows.
        write!(f, "FusedSeries: {}", self.promql)?;
        if let Some(format) = self.format.as_str() {
            write!(f, " (output: {format})")?;
        }
        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        if inputs.len() != 1 {
            return Err(DataFusionError::Internal(
                "FusedSeries should have at exact one input".to_string(),
            ));
        }

        let input = inputs.pop().unwrap();
        Ok(Self {
            promql: Arc::clone(&self.promql),
            tree: self.tree.clone(),
            binding: self.binding.clone(),
            input,
            output_schema: Arc::clone(&self.output_schema),
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
            && self.binding == other.binding
            && self.input == other.input
            && self.output_schema == other.output_schema
            && self.format == other.format
    }
}

impl Eq for FusedSeries {}

impl Hash for FusedSeries {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.promql.hash(state);
        self.binding.hash(state);
        self.input.hash(state);
        self.output_schema.hash(state);
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
        match self.input.partial_cmp(&other.input) {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        match self.tree.to_string().partial_cmp(&other.tree.to_string()) {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        // The format is part of what the node is, and it is cheap and total, so it is compared
        // before the schema it decides rather than folded into the incomparable case below.
        match self.format.partial_cmp(&other.format) {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        if self.output_schema == other.output_schema {
            Some(Ordering::Equal)
        } else {
            None
        }
    }
}

/// What the tree's input node reads, read off the tree itself.
///
/// The tree is the one authority on the schema its input was compiled against: a caller's binding
/// can only agree or disagree with it, and the frame this node reads has to be decoded against the
/// same schema, so both halves read it here instead of each carrying a copy.
struct TreeInput {
    /// The position the tree's raw chunks enter at: a position is how the runtime addresses a node,
    /// so nothing on the node has to carry an identity that the tree itself does not.
    path: NodePath,
    /// The raw series schema the tree's input node was compiled against.
    schema: Arc<SeriesSchema>,
}

/// Reads what the tree's input node was compiled against, rejecting a tree this node cannot feed.
///
/// A node feeds one raw input, and its output is addressed on a grid; a tree that does not have
/// that shape is one this layer cannot hand chunks to or read positions from.
fn tree_input(tree: &NativeTree) -> DataFusionResult<TreeInput> {
    let mut sources = 0;
    let mut input_path = None;
    let mut schema = None;
    metrics::walk(tree.root(), &mut |node, path| {
        let NativeExpr::Input(input) = &node.expr else {
            return;
        };
        sources += 1;
        input_path.get_or_insert_with(|| path.clone());
        // Raw, not evaluated: this node feeds the tree from a scan, so a tree whose input was
        // compiled against an already-evaluated schema would read the wrong thing.
        schema = input
            .output
            .samples()
            .filter(|schema| schema.eval_grid.is_none())
            .map(Arc::clone);
    });
    if sources != 1 {
        return Err(DataFusionError::Plan(format!(
            "FusedSeries runs a tree with {sources} inputs, and it needs exactly one"
        )));
    }
    let (Some(path), Some(schema)) = (input_path, schema) else {
        return Err(DataFusionError::Plan(
            "FusedSeries runs a tree whose input is not raw series".to_string(),
        ));
    };
    let evaluated = tree
        .root()
        .output
        .samples()
        .is_some_and(|schema| schema.eval_grid.is_some());
    if !evaluated {
        return Err(DataFusionError::Plan(
            "FusedSeries runs a tree whose output is not evaluated on a grid".to_string(),
        ));
    }
    Ok(TreeInput { path, schema })
}

/// Refuses a tree whose logical series can be split across the partitions this node runs.
///
/// Keeping the metric name is not what keeps two raw series apart. An empty tag value and an absent
/// tag are the same canonical label set, so `{host="h", zone=""}` and `{host="h"}` are one series to
/// Prometheus while the scan encodes them as two raw keys. A window that evaluated its input without
/// merging it therefore passes such a pair through as two series under one label set, exactly as the
/// functions that drop `__name__` do by merging them, and a map drops `__name__` outright.
///
/// A stateful node above one of those needs both halves together, and the runtime does provide that
/// within one execution: a window whose input can hold two series under one label set collects the
/// fragments, merges each label set once, and only then evaluates. That merge is per execution, and
/// this node runs one execution per partition. Two raw series that become one label set can land in
/// different partitions - the scan keeps a *series* in one partition, which says nothing about two
/// different series that later become indistinguishable - so each execution merges only the half it
/// was given. The consumer above then answers from one half and reports nothing for the other, which
/// is an empty result rather than an error.
///
/// Whether a window merged its input is runtime state this check does not have: it runs over a bare
/// tree, where a window's input is only something that produces samples. A window below another
/// window is therefore treated as one that did not merge, whatever function it runs, which refuses
/// `rate` under `rate` (which drops the name) and `last_over_time` under `rate` (which keeps it)
/// alike.
///
/// Reading such a tree needs an exchange above this node that merges a logical series across
/// partitions, which does not exist yet. Until it does the tree is refused where it can hurt: over
/// one partition there is nothing to split, so only a tree run over more than one is checked.
///
/// The compiler cannot build a nested window today - [`compile`](crate::promql::native_compile::compile)
/// requires a range function to apply directly to a selector, `<range function>(<selector>[<range>])`,
/// and a selector is raw samples - so no tree it produces has a window above a window, and this
/// check costs no executable query. It is checked because [`FusedSeriesExec::try_new`] accepts any
/// well-formed tree, and a tree it accepts is a tree it must answer correctly.
fn reject_a_split_logical_series(
    tree: &NativeTree,
    partition_count: usize,
) -> DataFusionResult<()> {
    if partition_count == 1 {
        return Ok(());
    }
    // The walk is outermost first, so it meets nodes in the reverse of the order data flows: a
    // consumer met before a rewriter is one that the rewriter feeds.
    let mut consumer: Option<String> = None;
    let mut rejection: Option<(String, String)> = None;
    metrics::walk(tree.root(), &mut |node, path| {
        if rejection.is_some() {
            return;
        }
        // A node can leave two raw series on one canonical label set. A map always can: it drops
        // `__name__` and every empty-valued tag. An instant selection preserves its input labels,
        // rather than dropping them, but can still leave two raw series on one canonical label set:
        // `{zone=""}` and the absent `zone` are one label set and two raw keys. A window can
        // whenever it did not merge its input, which a bare tree says nothing about: a check on the
        // function alone accepts `last_over_time`, whose output keeps the name and can still hold
        // both raw series. Every map, instant selection, and window is therefore taken to be a
        // possible rewriter, and the refusal below is what narrows that to the shape that costs
        // correctness - a window with a rewriter below it, a window being the only consumer here.
        let rewrites = matches!(
            node.expr,
            NativeExpr::Map { .. } | NativeExpr::Instant { .. } | NativeExpr::Window { .. }
        );
        if rewrites && let Some(above) = &consumer {
            rejection = Some((path.as_str().to_string(), above.clone()));
            return;
        }
        let consumes = matches!(node.expr, NativeExpr::Window { .. });
        if consumes && consumer.is_none() {
            consumer = Some(path.as_str().to_string());
        }
    });
    let Some((rewriter, above)) = rejection else {
        return Ok(());
    };
    Err(DataFusionError::Plan(format!(
        "FusedSeries runs a tree where `{rewriter}` can leave two raw series on one canonical label \
         set and `{above}` above it reads a whole series, over {partition_count} partitions: an \
         empty tag value and an absent tag are the same label set, the runtime merges such \
         fragments once per execution, and this node runs one execution per partition, so `{above}` \
         would read one half and answer an empty result. Reading this tree needs an exchange between \
         `{rewriter}` and `{above}` that merges a logical series across partitions, which is not \
         implemented"
    )))
}

/// Checks that `tree` reads the schema `binding` declares, on the grid it declares.
///
/// The output grid is compared by [`EvalGrid::spec`], not by allocation: that is the rule
/// [`NativeTree`]'s own validation and the kernels apply, and a chunk's timestamps already come
/// from its absolute grid positions, so a separately allocated grid with the same spec is the same
/// grid to everything downstream.
///
/// Answers with what the tree's input reads, which is what a caller names when it hands a raw chunk
/// over.
fn validate_binding(
    tree: &NativeTree,
    binding: &FusedSeriesBinding,
) -> DataFusionResult<TreeInput> {
    if binding.series_schema.eval_grid.is_some() {
        return Err(DataFusionError::Plan(
            "FusedSeries binds a raw series schema, which cannot have an evaluation grid"
                .to_string(),
        ));
    }
    if binding.tags.iter().any(|tag| tag == METRIC_NAME) && binding.metric_name.is_none() {
        return Err(DataFusionError::Plan(format!(
            "FusedSeries declares a {METRIC_NAME:?} tag column but binds no metric name"
        )));
    }

    // The tree's own input node says which schema it reads, so the binding is checked against the
    // tree rather than trusted: a mismatch would evaluate one schema and label the result as
    // another.
    let input = tree_input(tree)?;
    if !binding.same_fields(&input.schema) {
        return Err(DataFusionError::Plan(
            "FusedSeries binds a raw series schema the tree's input was not compiled against"
                .to_string(),
        ));
    }
    match tree.root().output.samples() {
        Some(schema)
            if schema
                .eval_grid
                .as_ref()
                .is_some_and(|grid| grid.spec() == binding.grid.spec()) =>
        {
            Ok(input)
        }
        _ => Err(DataFusionError::Plan(
            "FusedSeries runs a tree whose output is not evaluated on the statement's grid"
                .to_string(),
        )),
    }
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
    /// The position of the tree's input node, where every raw chunk a partition reads enters.
    input_path: NodePath,
    /// Which of the two formats this node answers in.
    format: FusedOutputFormat,
    /// Where each output column comes from, or `None` when the output is a frame.
    ///
    /// A frame is addressed by series and grid position rather than by column, so it has no column
    /// to classify: the whole table below the [`FusedOutputFormat::PlainRows`] case is skipped.
    classification: Option<Arc<[OutputColumn]>>,
    /// The output column holding the grid timestamps, kept so this node can be rebuilt over a new
    /// child without losing which column its time came from.
    time_column: String,
    /// The tag column names, indexed by [`OutputColumn::Tag`].
    tags: Arc<[String]>,
    /// The plan producing the raw series frames.
    input: Arc<dyn ExecutionPlan>,
    /// The columns this node produces.
    output_schema: SchemaRef,
    /// The grid positions every label set in this plan's output already occupies, and what they
    /// keep charged on the memory pool.
    ///
    /// Plan-level rather than per-partition: see [`LabelSets`].
    labels: SharedLabelSets,
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
    /// is a row table rather than a frame, and are what
    /// [`FusedOutputFormat::PlainRows`] declares. A [`FusedOutputFormat::SeriesFrame`] output
    /// declares `output_schema` instead, which has to be the codec's own declaration for the tree's
    /// evaluated output: a frame whose schema was assembled for another layout cannot be decoded.
    ///
    /// The parameters stay separate because each is a fact its own caller has: the query, the plan
    /// below, the frame's label binding and ordering, the tree, and the three things the output
    /// schema is classified by. A struct over them would be the same list under another name.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        promql: Arc<str>,
        input: Arc<dyn ExecutionPlan>,
        label_source: RawLabelSource,
        ordering: SeriesOrdering,
        tree: Arc<NativeTree>,
        output_schema: SchemaRef,
        time_column: String,
        tags: Vec<String>,
        format: FusedOutputFormat,
    ) -> DataFusionResult<Self> {
        // A frame output is decided first, so a tree it cannot carry is refused with what is wrong
        // about the tree rather than with the shape of its input.
        let frame_schema = match format {
            FusedOutputFormat::PlainRows => None,
            FusedOutputFormat::SeriesFrame => {
                let declared = series_frame_output_schema(evaluated_output(&tree)?.as_ref())?;
                if declared != output_schema {
                    return Err(DataFusionError::Plan(
                        "FusedSeriesExec was declared a series frame output of a layout the tree's \
                         evaluated output does not have"
                            .to_string(),
                    ));
                }
                Some(declared)
            }
        };
        let tree_input = tree_input(&tree)?;
        // Building a state is what proves the runtime executes this tree: a node kind it does not
        // run yet is refused here, so a plan that is built is a plan that starts. The throwaway
        // state is dropped at once, returning its (unbounded) reservations.
        let _ = NativeExecutionState::try_new(
            &tree,
            Arc::new(UnboundedMemoryPool::default()),
            &ExecutionPlanMetricsSet::new(),
            0,
        )?;
        let classification = match frame_schema {
            // A frame is addressed by series and grid position, so no column of it has to be
            // traced back to a field, a tag or the time axis.
            Some(_) => None,
            None => Some(Arc::from(classify_output(
                &output_schema,
                &tree_input.schema,
                &tags,
                &time_column,
            )?)),
        };
        // Read here rather than below, because whether the split below can happen decides whether
        // the tree shape is one this node may run at all.
        let input_properties = input.properties();
        let partition_count = input_properties.partitioning.partition_count();
        if partition_count == 0 {
            return Err(DataFusionError::Plan(
                "FusedSeriesExec input has no partitions".into(),
            ));
        }
        reject_a_split_logical_series(&tree, partition_count)?;
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
            Some(Arc::clone(&tree_input.schema)),
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
        // Shared by every partition's stream: a label set colliding is a property of the whole
        // output vector, not of one partition. Building it needs no pool; the owner's reservation is
        // attached in `execute`, which is the first point that has one.
        let labels: SharedLabelSets = Arc::new(Mutex::new(LabelSets::default()));
        Ok(Self {
            promql,
            tree,
            series_schema: tree_input.schema,
            label_source,
            ordering,
            input_path: tree_input.path,
            format,
            classification,
            time_column,
            tags: tags.into(),
            input,
            output_schema,
            labels,
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
    ///
    /// A frame output names its format on the node line, because that line is where a reader sees
    /// that this plan answers something other than the rows the query asked for.
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "FusedSeriesExec: {}", self.promql)?;
        if let Some(format) = self.format.as_str() {
            write!(f, " (output: {format})")?;
        }
        write!(f, "\n{}", self.tree)
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
            child,
            self.label_source.clone(),
            self.ordering,
            Arc::clone(&self.tree),
            self.output_schema.clone(),
            self.time_column.clone(),
            self.tags.to_vec(),
            self.format,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        // The label-set state is the plan's, so the reservation that pays for it is attached exactly
        // once, to the pool of the first execution. Every partition calls this and only the first
        // takes effect: re-registering per stream would move a charge the other partitions keep
        // growing, and would give it back when that stream ends while the entries stay.
        self.labels
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .attach(&context.runtime_env().memory_pool);
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
            &self.metric,
            partition,
        )?;
        // A frame output is encoded from the evaluated schema the tree's root declares, which is
        // the schema the plan was built against; a row table needs neither it nor a frame builder.
        let (eval_schema, frame_builder) = match self.format {
            FusedOutputFormat::PlainRows => (None, None),
            FusedOutputFormat::SeriesFrame => {
                let schema = evaluated_output(&self.tree)?;
                let builder = SeriesFrameBuilder::try_new(
                    &schema,
                    FrameLabelMode::Explicit,
                    None,
                    SeriesFrameBuilder::DEFAULT_MAX_ROWS,
                    SeriesFrameBuilder::DEFAULT_MAX_BYTES,
                )
                .map_err(|error| {
                    DataFusionError::Plan(format!(
                        "FusedSeriesExec cannot encode a series frame output: {error}"
                    ))
                })?;
                (Some(schema), Some(builder))
            }
        };
        // What the label sets hold is charged to the reservation attached above, so nothing is
        // reserved per stream: the entries outlive every stream and are released with the plan.
        Ok(Box::pin(FusedSeriesStream {
            input: Some(input),
            executor: Some(NativeExecutor::new(state)),
            input_path: self.input_path.clone(),
            format: self.format,
            classification: self.classification.clone(),
            tags: Arc::clone(&self.tags),
            output_schema: self.output_schema.clone(),
            eval_schema,
            frame_builder,
            labels: Arc::clone(&self.labels),
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

/// The bytes one label set holds, for the memory reservation.
///
/// It counts each label's name and value with the pair that refers to them, which is the same
/// convention [`crate::native::execute`]'s own label-set table keeps.
fn labels_bytes(labels: &LabelSet) -> usize {
    labels
        .iter()
        .map(|(name, value)| size_of::<(Arc<str>, Arc<str>)>() + name.len() + value.len())
        .sum()
}

/// Renders the label set Prometheus compares the way it prints one, for the duplicate-label-set
/// error.
fn render_labels(labels: &LabelSet) -> String {
    let parts = labels
        .iter()
        .map(|(name, value)| format!("{name}={value:?}"))
        .collect::<Vec<_>>();
    format!("{{{}}}", parts.join(", "))
}

/// The grid positions each final label set already occupies, per field and as merged, sorted runs.
///
/// Shared by every partition's stream, because a label-set collision is a property of the whole
/// output vector and not of one partition. Two series that carry the same labels after the range
/// function and its maps drop a label land in different partitions, and the runtime's own per-node
/// `Emitted` table never sees both: it belongs to one execution. Checking only within a partition
/// would miss exactly those collisions.
///
/// The key is [`crate::grid_series::label_set`]'s normalization of the final labels rather than
/// [`datatypes::series::SeriesLabels`] itself: a label with an empty value is no label to
/// Prometheus, so `{host="h", zone=""}` and `{host="h"}` are one label set and have to be compared
/// as one. Encoding the tags as they are would file them apart and let one point hold two values.
///
/// Occupancy is per field and per position, not per chunk: two series whose label sets became equal
/// while holding values in *different* fields merge cleanly in Prometheus, so only the same field
/// at the same position collides. That is the rule
/// [`crate::grid_series::merge_same_labelset`] applies.
///
/// The map belongs to the plan, not to one stream: a partition that ends leaves what it claimed
/// for the partitions still running, so the entries are only released with the plan itself. The
/// charge has the same lifetime, which is what makes the bytes the map holds - one entry per label
/// set of the output, kept until the plan dies - visible to the memory pool. Charging each stream
/// for what it inserted instead returned those bytes when that stream ended, while the entries the
/// later partitions are checked against stayed: everything the map held after the first partition
/// was unaccounted for.
#[derive(Default)]
struct LabelSets {
    /// The grid positions each final label set already occupies, one run list per field of the
    /// series schema. Each list is sorted, disjoint and has the runs that touch joined, which is
    /// what lets a claim look a position up instead of walking the history.
    seen: HashMap<LabelSet, Vec<Vec<Run>>>,
    /// What `seen` keeps charged on the task's memory pool. `None` until the first execution,
    /// since construction has no `TaskContext` and therefore no pool to register against.
    reservation: Option<MemoryReservation>,
}

impl LabelSets {
    /// Attaches the reservation to the pool of the first execution.
    ///
    /// Later executions keep the first reservation: it already pays for what `seen` holds, and
    /// moving it to another pool would drop the bytes other partitions are still growing.
    fn attach(&mut self, pool: &Arc<dyn MemoryPool>) {
        if self.reservation.is_none() {
            self.reservation =
                Some(MemoryConsumer::new("FusedSeriesExec label sets").register(pool));
        }
    }

    /// Claims the occupied grid positions of one chunk of a final series for its label set.
    ///
    /// A collision is an error only when both series hold a sample of *one field* at the same
    /// evaluation timestamp, which is Prometheus' range-query rule. Series with disjoint points are
    /// legal, and so are series that fill the same points field by field: Prometheus merges both
    /// into one output series, while this node emits them as separate batches and defers that merge
    /// to matrix assembly, so every sample survives.
    fn claim(&mut self, series: &Series) -> DataFusionResult<()> {
        let runs = occupied_runs(series)?;
        // A chunk holding no value occupies nothing, so there is no collision to remember and no
        // entry to write. Such a chunk is not rare: a selection over a series that staleness or the
        // range left empty still closes it, and hands over a closing chunk with no rows. Recording
        // one entry per such series would grow the table - and its charge, which the plan holds for
        // its whole life - for a query that never answers a value, until an empty result exhausted
        // the pool. The executor's own `Emitted::occupy` takes the same early return.
        if runs.iter().all(Vec::is_empty) {
            return Ok(());
        }
        let key = label_set(&series.metric);
        let Self { seen, reservation } = self;
        // Nothing reaches a claim without an `execute` first, and an execution that ran attached the
        // reservation. Claiming without one would leave the map growing uncharged, so it is refused
        // rather than silently skipped.
        let Some(reservation) = reservation.as_mut() else {
            return Err(DataFusionError::Execution(
                "FusedSeriesExec label-set state is not attached to a memory pool".to_string(),
            ));
        };
        match seen.entry(key) {
            Entry::Occupied(entry) => {
                if let Some(field) = colliding_field(entry.get(), &runs) {
                    return Err(DataFusionError::Execution(format!(
                        "vector cannot contain metrics with the same labelset: {} (field \
                         {field})",
                        render_labels(entry.key())
                    )));
                }
                let stored = entry.into_mut();
                // An entry recorded from a chunk of fewer fields is padded, so every field of this
                // chunk has a slot of its own: a slot is a field index of the schema, so both sides
                // count the same field the same way. A chunk of *fewer* fields than the entry
                // leaves the remaining slots to the chunks that fill them.
                // Stage only the affected run lists, and reserve before committing. A forward
                // append whose existing capacity suffices needs no copy of its stored history.
                let mut growth = 0usize;
                let mut shrink = 0usize;
                let mut merged_runs = Vec::with_capacity(runs.len());
                for (index, incoming) in runs.iter().enumerate() {
                    let Some(old) = stored.get(index) else {
                        let copy = incoming.clone();
                        growth += copy.capacity();
                        merged_runs.push(Some(copy));
                        continue;
                    };
                    let append_fits = incoming.is_empty()
                        || (old.last().is_none_or(|last| incoming[0].start >= last.end)
                            && old.capacity()
                                >= old.len() + incoming.len()
                                    - usize::from(old.last().is_some_and(|last| {
                                        incoming
                                            .first()
                                            .is_some_and(|first| first.start == last.end)
                                    })));
                    if append_fits {
                        merged_runs.push(None);
                    } else {
                        let mut merged = old.clone();
                        absorb(&mut merged, incoming);
                        if merged.capacity() >= old.capacity() {
                            growth += merged.capacity() - old.capacity();
                        } else {
                            shrink += old.capacity() - merged.capacity();
                        }
                        merged_runs.push(Some(merged));
                    }
                }
                reservation.try_grow(growth * size_of::<Run>())?;
                for (index, (incoming, merged)) in runs.iter().zip(merged_runs).enumerate() {
                    if let Some(merged) = merged {
                        if index < stored.len() {
                            stored[index] = merged;
                        } else {
                            stored.push(merged);
                        }
                    } else if index < stored.len() {
                        absorb(&mut stored[index], incoming);
                    }
                }
                reservation.try_shrink(shrink * size_of::<Run>())?;
            }
            Entry::Vacant(entry) => {
                // Only the label-set state is this node's own cross-poll state that grows with
                // the output, so it is the only thing reserved here. The runs are charged by the
                // capacity they were gathered with rather than by their length, so that a later
                // claim appending into that slack is charged nothing for it.
                let bytes = labels_bytes(entry.key())
                    + runs.iter().map(|runs| runs.capacity()).sum::<usize>() * size_of::<Run>();
                reservation.try_grow(bytes)?;
                entry.insert(runs);
            }
        }
        Ok(())
    }
}

type SharedLabelSets = Arc<Mutex<LabelSets>>;

/// The grid positions the rows of one chunk hold a value at, one run list per field.
///
/// Each [`Run`] is a half-open range of absolute grid indices, and the runs of one field are sorted
/// and disjoint. Occupancy is kept per field because it is what a collision is: two series whose
/// label sets became equal merge field by field in Prometheus, so only two values of the same field
/// at one position collide. Marking the whole position as taken as soon as *any* field held a value
/// would refuse the complementary pair that merges cleanly.
fn occupied_runs(series: &Series) -> DataFusionResult<Vec<Vec<Run>>> {
    // Rows are addressed on the statement's grid, so a row sits at the absolute position two
    // series can collide on. An explicit axis has no such position, and a chunk never carries
    // one: this is the refusal `EvaluatedChunk` already makes when the chunk is built.
    if series.time.grid().is_none() {
        return Err(DataFusionError::Execution(
            "FusedSeriesExec requires evaluated series on a time grid".to_string(),
        ));
    }
    let rows = series.time.len();
    let range = series.time.as_grid_range();
    let mut runs: Vec<Vec<Run>> = vec![Vec::new(); series.fields.len()];
    // A field that is one dense, null-free float run holds a value on every row the chunk carries,
    // so it occupies exactly the range the chunk reports and no row of it has to be inspected. This
    // is the shape the fused evaluator produces for a range function over ordinary float samples.
    // Every other field is walked below, and a field filled here is skipped there.
    let mut walked: Vec<(usize, &Samples)> = Vec::new();
    for (index, samples) in series.fields.iter().enumerate() {
        match (range, samples.dense()) {
            (Some(range), Some(DenseSamplesRef::Float(values)))
                if rows > 0 && values.len() == rows && values.null_count() == 0 =>
            {
                runs[index] = vec![Run {
                    start: range.start(),
                    end: range.start() + rows,
                }];
            }
            _ => walked.push((index, samples)),
        }
    }
    if walked.is_empty() {
        return Ok(runs);
    }
    // `positions` walks a chunk that forms one run and one whose rows form several the same way,
    // in position order, so the holes between two runs are never read as rows.
    let mut cursors: Vec<_> = walked.iter().map(|(_, samples)| samples.cursor()).collect();
    for (position, row) in series.time.positions() {
        for ((index, _), cursor) in walked.iter().zip(&mut cursors) {
            if !matches!(cursor.at(row), Some(sample) if !matches!(sample, SampleRef::Missing)) {
                continue;
            }
            match runs[*index].last_mut() {
                Some(run) if run.end == position => run.end += 1,
                _ => runs[*index].push(Run {
                    start: position,
                    end: position + 1,
                }),
            }
        }
    }
    Ok(runs)
}

/// The field of `runs` that holds a value where `stored` already holds one, if any.
///
/// A collision is two values of one field at one position, so each field is checked on its own.
/// Each incoming run is looked up rather than the stored history walked: the stored runs are sorted
/// and disjoint, so the only run that can share a position with `run` is the first one that does not
/// end before it, which is the look-up the runtime's own `Emitted::occupy` makes. Walking the
/// history instead costs a sparse series one pass over everything it stored per chunk - a query
/// whose chunks move forward one position at a time, a hole apart, is legal and would cost the
/// square of its chunk count under the lock every partition shares.
fn colliding_field(stored: &[Vec<Run>], runs: &[Vec<Run>]) -> Option<usize> {
    for (field, (stored, new)) in stored.iter().zip(runs).enumerate() {
        for run in new {
            let at = stored.partition_point(|stored| stored.end <= run.start);
            if stored.get(at).is_some_and(|stored| stored.overlaps(*run)) {
                return Some(field);
            }
        }
    }
    None
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
    /// The position of the tree's input node, which is where every chunk read enters.
    input_path: NodePath,
    /// Which of the two formats this stream yields.
    format: FusedOutputFormat,
    /// Where each output column comes from, in the row-table format.
    classification: Option<Arc<[OutputColumn]>>,
    /// The tag column names, indexed by `OutputColumn::Tag`.
    tags: Arc<[String]>,
    /// The schema of every batch this stream yields.
    output_schema: SchemaRef,
    /// The evaluated schema the tree's output is on, in the frame format.
    ///
    /// A chunk is validated against it before it is encoded, so a chunk that does not belong to
    /// this plan's series is refused at the chunk that reached it.
    eval_schema: Option<Arc<SeriesSchema>>,
    /// The frame rows buffered between batches, in the frame format.
    ///
    /// One builder for the whole stream, not one per chunk: a chunk that does not fill a batch's
    /// rows or bytes leaves them buffered, and the next chunk's rows join them. The closing flush is
    /// what hands the last buffered rows over; dropping the builder instead would drop them.
    frame_builder: Option<SeriesFrameBuilder>,
    /// The label-set occupancy shared with the other partitions' streams.
    ///
    /// What it holds is charged to the plan's own reservation, attached by the first execution.
    /// The frame format does not claim, so it never grows here: see [`FusedSeriesStream::convert`].
    labels: SharedLabelSets,
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
        // The chunk enters at the tree's input node by position: a position is how the runtime
        // addresses a node, and a tree that gains a second input names them the same way.
        executor.push_raw_at(&self.input_path, chunk)?;
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
        // The label-set map and its charge are the plan's, so a stream stopping early gives back
        // nothing: the entries another partition is still checking against have to stay charged, and
        // are released when the plan itself is dropped.
        // A batch queued before the error was settled by a run that has now stopped.
        self.ready.clear();
        // Rows buffered by a frame that will not be completed belong to the run that stopped.
        self.frame_builder = None;
    }

    /// Converts the chunks the executor settled into batches, in the order it produced them.
    fn convert(&mut self, chunks: Vec<EvaluatedChunk>) -> DataFusionResult<()> {
        for chunk in &chunks {
            match self.format {
                FusedOutputFormat::PlainRows => {
                    // The label set is claimed before its batch is queued, so a collision surfaces at
                    // the chunk that reached it and nothing this plan would refuse is handed out
                    // after it.
                    self.labels
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .claim(&chunk.series)?;
                    let classification = self.classification.as_deref().ok_or_else(|| {
                        DataFusionError::Internal(
                            "FusedSeriesExec stream is a row table without a column \
                             classification"
                                .to_string(),
                        )
                    })?;
                    let batch = series_to_record_batch(
                        classification,
                        &self.tags,
                        &self.output_schema,
                        chunk,
                    )?;
                    self.ready.push_back(batch);
                }
                // A frame is an intermediate hand-over of one series' positions, not the final row
                // table: the same label set may be filled by a later chain, and stopping that
                // contribution here would drop values Prometheus answers with. The runtime's own
                // `Emitted::occupy` (see `src/promql/src/native/execute.rs`) checks the conflicts
                // this layer may not throw away, and it does not treat `is_last` as the end of a
                // label set - only as the end of the chain that carried it - so dropping the plan
                // level claim here cannot hide a conflict the runtime would have reported.
                FusedOutputFormat::SeriesFrame => self.encode_frame(chunk)?,
            }
        }
        Ok(())
    }

    /// Encodes one evaluated chunk into the frame the plan declared, queueing whatever is ready.
    ///
    /// The chunk is encoded as it stands: the axis it carries becomes the row's spans, so a sparse
    /// chunk keeps its holes and a chunk that carries nothing but its finality still hands over the
    /// row that closes its series.
    fn encode_frame(&mut self, chunk: &EvaluatedChunk) -> DataFusionResult<()> {
        let (Some(schema), Some(builder)) = (self.eval_schema.clone(), self.frame_builder.as_mut())
        else {
            return Err(DataFusionError::Internal(
                "FusedSeriesExec stream is a series frame without its encoder".to_string(),
            ));
        };
        let mut cursor =
            SeriesFrameCursor::try_new_evaluated(&schema, chunk.series.clone(), chunk.is_last)
                .map_err(|error| {
                    DataFusionError::Execution(format!(
                        "FusedSeriesExec cannot frame a chunk: {error}"
                    ))
                })?;
        // The cursor buffers at most one row, which it retries after a flush, so a chunk whose rows
        // cross a batch bound leaves nothing behind when the loop ends.
        while let Some(batch) = cursor.append_to(builder).map_err(|error| {
            DataFusionError::Execution(format!("FusedSeriesExec cannot frame a chunk: {error}"))
        })? {
            let batch = batch.with_schema(self.output_schema.clone())?;
            self.ready.push_back(batch);
        }
        Ok(())
    }

    /// Hands the rows still buffered by the frame builder over as the last batch.
    ///
    /// Nothing is queued when this stream is not a frame or when nothing is buffered. A frame that
    /// buffered rows when the execution ended would lose them by dropping the builder, so this runs
    /// before the stream is done rather than on the way out.
    fn flush_frames(&mut self) -> DataFusionResult<()> {
        let Some(builder) = self.frame_builder.as_mut() else {
            return Ok(());
        };
        let batch = builder.flush().map_err(|error| {
            DataFusionError::Execution(format!("FusedSeriesExec cannot flush a frame: {error}"))
        })?;
        if let Some(batch) = batch {
            let batch = batch.with_schema(self.output_schema.clone())?;
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
                        // The execution ended, so the rows a frame still buffers are the last ones
                        // this stream owes. They are handed over now: dropping the builder without
                        // flushing would lose them, because nothing reads it after the stream is
                        // done.
                        if let Err(error) = this.flush_frames() {
                            this.fail();
                            return Poll::Ready(Some(Err(error)));
                        }
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
                Poll::Pending => {
                    // A frame that is holding rows must not hold them behind an input that settled
                    // nothing: a consumer waiting for the row that closes a series would be waiting
                    // for input that may never arrive. They are handed over now, and the pending
                    // input is polled again on the next poll of this stream.
                    if let Err(error) = this.flush_frames() {
                        this.fail();
                        return Poll::Ready(Some(Err(error)));
                    }
                    if let Some(batch) = this.ready.pop_front() {
                        return Poll::Ready(Some(Ok(batch)));
                    }
                    return Poll::Pending;
                }
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
