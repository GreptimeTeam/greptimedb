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
//! tree itself, the binding it was compiled against and one logical child per actual source.
//! [`FusedSeriesExec`] is the physical half: it owns one execution of that tree per partition and
//! decodes its input frames into raw series chunks and converts its evaluated chunks back into
//! batches.
//!
//! Which child produces a tree input is the input's own `source_index`, not a list beside the tree:
//! one actual source can feed several tree inputs - a query that names the same selector twice reads
//! the same series twice - so the tree says where every chunk goes. The logical node keeps its
//! children in that index order and checks that every index is bound. The physical node runs one
//! child: a second child would have to pair its series with the first by label, which needs an
//! exchange this node does not have, and no child at all is a constant scalar, whose rows the
//! conversion does not produce yet.
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
//!
//! One mode is not the statement's answer: the scope side of a split aggregate exports, per
//! partition, the state that partition folded, in the exchange's own layout. Its children and
//! source indices are the same as the ordinary mode's - the prefix reads the one raw source - and
//! only what the node does with the chunks it settles differs.

use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::fmt::{self, Formatter};
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use common_query::series_frame::{ensure_frame_matches_label_source, unpack_raw_series};
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
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::{
    DenseSamplesRef, FieldSpec, SampleRef, Samples, Series, SeriesSchema,
};
use datatypes::series::time::EvalGrid;
use futures::Stream;
use store_api::region_engine::{RawLabelSource, SendableRawSeriesStream};

use crate::evaluated::EvaluatedChunk;
use crate::extension_plan::aggregate_exchange::{PartialFold, state_schema};
use crate::grid_series::{LabelSet, label_set};
use crate::native::execute::{NativeExecutionState, NativeExecutor};
use crate::native::metrics::{self, NodePath};
use crate::native::plan::{
    AggregateKind, BoundAggregate, Grouping, NativeExpr, NativeTree, NativeType,
    reads_across_series,
};
use crate::native::runs::{Run, absorb};
use crate::source::materialize_raw_labels;

#[cfg(test)]
mod tests;

/// The label a metric name is stored under, which is not a tag column of the input.
const METRIC_NAME: &str = "__name__";

/// How many ready input chunks one poll of [`FusedSeriesStream`] processes before it wakes itself.
///
/// A chunk that settles no batch is the cost this bounds: raw samples before the first evaluation
/// point settle nothing, so an input of many such chunks would otherwise be read to its end inside
/// one poll, holding the task's thread for an unbounded time and delaying both other tasks on it
/// and the cancellation the consumer asked for. The budget bounds only the chunks this node hands
/// to the executor. A poll that stops early wakes itself, so nothing is lost.
const POLL_BUDGET: usize = 64;

/// What the tree was compiled against, and how its output columns are named.
///
/// The binding travels with the tree: a tree is legal only on the grid it was bound to, and a
/// reader of the output needs to know which grid a chunk's absolute positions are on. Keeping these
/// four facts in one value is what lets the logical node carry what the writer decided and the
/// physical node take the same output columns from it.
///
/// It is not a mirror of any schema: the raw layout a frame is decoded with is the one the tree's
/// own input declares, read back off the tree on both sides. What the binding cannot check is
/// whether the caller named the metric and tags the query meant, or whether [`time_index`] is the
/// column the query reports positions in - from here both are indistinguishable from a correct
/// binding, so picking them is the caller's obligation.
///
/// [`metric_name`]: Self::metric_name
/// [`tags`]: Self::tags
/// [`time_index`]: Self::time_index
#[derive(Clone, Debug)]
pub struct FusedSeriesBinding {
    /// The statement's evaluation grid.
    pub grid: Arc<EvalGrid>,
    /// The output column holding the grid timestamps.
    pub time_index: String,
    /// The logical tag columns of the output, `__name__` included when the query reports it.
    pub tags: Vec<String>,
    /// The metric name the scans below are read under, when the query binds one.
    pub metric_name: Option<Arc<str>>,
}

impl PartialEq for FusedSeriesBinding {
    fn eq(&self, other: &Self) -> bool {
        let a = self.grid.spec();
        let b = other.grid.spec();
        (a.start_ms, a.step_ms, a.len) == (b.start_ms, b.step_ms, b.len)
            && self.time_index == other.time_index
            && self.tags == other.tags
            && self.metric_name == other.metric_name
    }
}
impl Eq for FusedSeriesBinding {}
impl Hash for FusedSeriesBinding {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let s = self.grid.spec();
        (s.start_ms, s.step_ms, s.len).hash(state);
        self.time_index.hash(state);
        self.tags.hash(state);
        self.metric_name.hash(state);
    }
}
impl PartialOrd for FusedSeriesBinding {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let a = self.grid.spec();
        let b = other.grid.spec();
        match (a.start_ms, a.step_ms, a.len).partial_cmp(&(b.start_ms, b.step_ms, b.len)) {
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
        self.metric_name
            .as_deref()
            .partial_cmp(&other.metric_name.as_deref())
    }
}

/// The logical plan of one node that runs a compiled native tree.
///
/// The node adds nothing to the tree: it records the query the tree was compiled from, so
/// `EXPLAIN` shows the reader their own query rather than the shape it compiled to, and it carries
/// the binding the tree was compiled against so the execution plan can be built from it.
#[derive(Debug)]
pub struct FusedSeries {
    /// The PromQL text this tree was compiled from.
    promql: Arc<str>,
    /// The compiled tree to run.
    tree: NativeTree,
    /// What the tree was compiled against.
    binding: FusedSeriesBinding,
    /// The plans producing the input series, one per source index the tree binds.
    children: Vec<LogicalPlan>,
    /// The columns this node produces.
    output_schema: DFSchemaRef,
    /// The aggregate whose partition-local state this node exports instead of query values.
    ///
    /// `None` is the ordinary node, whose chunks are the answer. `Some` is the scope side of a
    /// split aggregate: the tree is the per-series prefix, each partition folds what it read, and
    /// the merge of those states happens above the exchange, never here.
    ///
    /// This state-row output is temporary; the final design has FusedSeries emit series frames
    /// directly for native final aggregation, removing the bridge's extra conversions.
    partial_fold: Option<BoundAggregate>,
}

impl FusedSeries {
    /// The name this node is shown under.
    pub const fn name() -> &'static str {
        "FusedSeries"
    }

    /// Creates the node, checking that `tree` is legal against `binding` and `children`.
    ///
    /// The children are the tree's actual sources, in `source_index` order: which child feeds which
    /// tree input is the input's own index, so this list carries no per-child description a caller
    /// could let drift from the tree.
    pub fn try_new(
        promql: Arc<str>,
        tree: NativeTree,
        binding: FusedSeriesBinding,
        children: Vec<LogicalPlan>,
        output_schema: DFSchemaRef,
    ) -> DataFusionResult<Self> {
        validate_binding(&tree, &binding, &children)?;
        Ok(Self {
            promql,
            tree,
            binding,
            children,
            output_schema,
            partial_fold: None,
        })
    }

    /// Creates the scope side of a split aggregate: a node that runs `tree` and exports `op`'s
    /// partition-local state.
    ///
    /// `tree` is the aggregate's input, so it computes one series at a time and a partition folds
    /// exactly the series its own scan returned; `children` is the prefix's one raw source, in the
    /// same `source_index` order the ordinary mode uses. The output schema is therefore
    /// [`state_schema`]'s layout, and a tree that reads across series is refused: it would fold a
    /// group per partition and call the halves a group.
    pub(crate) fn try_new_partial(
        promql: Arc<str>,
        tree: NativeTree,
        binding: FusedSeriesBinding,
        children: Vec<LogicalPlan>,
        op: BoundAggregate,
    ) -> DataFusionResult<Self> {
        validate_binding(&tree, &binding, &children)?;
        if reads_across_series(tree.root()) {
            return Err(DataFusionError::Plan(
                "FusedSeries exports partition-local aggregate state from a tree that reads across \
                 series, so every partition would fold part of a group and the merge above it would \
                 take the parts for the whole"
                    .to_string(),
            ));
        }
        let output_schema = Arc::new(DFSchema::try_from(state_schema(&binding.tags))?);
        Ok(Self {
            promql,
            tree,
            binding,
            children,
            output_schema,
            partial_fold: Some(op),
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

    /// What the tree was compiled against.
    pub fn binding(&self) -> &FusedSeriesBinding {
        &self.binding
    }

    /// The plans producing the input series, in `source_index` order.
    pub fn children(&self) -> &[LogicalPlan] {
        &self.children
    }

    /// The aggregate whose partition-local state this node exports, or `None` when its chunks are
    /// the statement's answer.
    pub fn partial_fold(&self) -> Option<&BoundAggregate> {
        self.partial_fold.as_ref()
    }

    /// The physical node that runs `exec_children` as this node's children.
    ///
    /// One execution input per child, in source order. `label_source` and `ordering` are the
    /// binding of the frames that child produces: the frame carries the encoded key and, in explicit
    /// mode, the complete labels, but no schema can say which of the two a producer meant or how it
    /// ordered the series, so the caller that built the frame passes both on. Building the node
    /// rejects a tree the runtime cannot execute, a frame schema the tree's input was not compiled
    /// against, and a schema the conversion cannot classify, so a plan that is built is a plan that
    /// runs.
    pub fn to_execution_plan(
        &self,
        exec_children: Vec<(Arc<dyn ExecutionPlan>, RawLabelSource, SeriesOrdering)>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Arity is checked before the two lists are paired: a zip would silently drop whichever side
        // is longer, and a node built from fewer children than it binds would run a tree over half
        // its sources.
        if exec_children.len() != self.children.len() {
            return Err(DataFusionError::Plan(format!(
                "FusedSeries binds {} inputs, but {} execution inputs were given",
                self.children.len(),
                exec_children.len()
            )));
        }
        let output_schema: SchemaRef = self.output_schema.inner().clone();
        Ok(Arc::new(FusedSeriesExec::build(
            Arc::clone(&self.promql),
            exec_children,
            Arc::new(self.tree.clone()),
            output_schema,
            self.binding.time_index.clone(),
            self.binding.tags.clone(),
            self.partial_fold.clone(),
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
        match &self.partial_fold {
            Some(op) => write!(
                f,
                "FusedSeries: {} [partial {}]",
                self.promql,
                render_partial_fold(op)
            ),
            None => write!(f, "FusedSeries: {}", self.promql),
        }
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        if inputs.len() != self.children.len() {
            return Err(DataFusionError::Internal(format!(
                "FusedSeries expected {} inputs",
                self.children.len()
            )));
        }
        // Rebuilt rather than patched: the sources each tree input names are checked against the
        // tree again, and the schema the node reports keeps its metadata. A rewrite that changed a
        // child's shape therefore cannot leave a tree whose raw inputs no longer fit it. The mode
        // travels unchanged: rebuilding must not turn a state export into a node that answers the
        // statement.
        validate_binding(&self.tree, &self.binding, &inputs)?;
        Ok(Self {
            promql: Arc::clone(&self.promql),
            tree: self.tree.clone(),
            binding: self.binding.clone(),
            children: inputs,
            output_schema: Arc::clone(&self.output_schema),
            partial_fold: self.partial_fold.clone(),
        })
    }
}

impl PartialEq for FusedSeries {
    fn eq(&self, other: &Self) -> bool {
        // `NativeTree` carries no structural equality, so the rendered tree stands in for it. The
        // renderer names every node with its bound operation, its grids and the source index its
        // input reads, which is what makes two nodes the same computation; comparing the PromQL
        // text alone would call two different trees equal whenever their query happens to read the
        // same.
        self.promql == other.promql
            && self.tree.to_string() == other.tree.to_string()
            && self.binding == other.binding
            && self.children == other.children
            && self.output_schema == other.output_schema
            && partial_key(self.partial_fold.as_ref()) == partial_key(other.partial_fold.as_ref())
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
        if self.output_schema != other.output_schema {
            return None;
        }
        partial_key(self.partial_fold.as_ref())
            .partial_cmp(&partial_key(other.partial_fold.as_ref()))
    }
}

/// The comparable form of one node's partial mode.
///
/// [`BoundAggregate`] carries no `Eq`/`Hash` - a quantile's parameter is an `f64` - so the render
/// stands in for it, as the tree's renderer stands in for [`NativeTree`].
fn partial_key(partial: Option<&BoundAggregate>) -> Option<String> {
    partial.map(render_partial_fold)
}

/// Renders a partial aggregate as `<kind> <grouping> <stage>`, for the plan line and the node's
/// equality: every bound field is named, quantile's parameter and the grouping's labels included.
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

/// The raw series schema each source index of `tree` reads, in source order.
///
/// A derived view, never stored: the tree is the one authority on where its inputs sit, what they
/// read and which child produces them, so both halves read it here instead of each keeping a copy
/// that could drift. The result is `source_index`-indexed rather than in visit order, and it is
/// exactly `0..len` - a tree that binds index `n` without binding every index below it is refused,
/// since a child at an index no input reads would be a source the tree cannot use. A caller
/// comparing it to a child list therefore only has to check that the lengths agree.
fn tree_sources(tree: &NativeTree) -> DataFusionResult<Vec<Arc<SeriesSchema>>> {
    let mut inputs: Vec<(usize, NodePath, Arc<SeriesSchema>)> = Vec::new();
    let mut not_raw: Option<NodePath> = None;
    metrics::walk(tree.root(), &mut |node, path| {
        if let NativeExpr::Input(input) = &node.expr {
            match input
                .output
                .samples()
                .filter(|schema| schema.eval_grid.is_none())
            {
                Some(schema) => inputs.push((input.source_index, path.clone(), Arc::clone(schema))),
                None => {
                    not_raw.get_or_insert_with(|| path.clone());
                }
            }
        }
    });
    if let Some(path) = not_raw {
        return Err(DataFusionError::Plan(format!(
            "FusedSeries runs a tree whose input `{path}` is not raw series"
        )));
    }
    // Indexed rather than pushed in visit order: a binary node whose operands name sources 1 and 0
    // is the same legal tree as one naming them 0 and 1, and only the set of indices says so.
    //
    // Every index is bounded by the number of input occurrences first, before any width is computed
    // or any slot is sized. A dense `0..n` set has n values of at most `n - 1`, so a legal tree
    // always satisfies this; a tree naming `usize::MAX` or one far past its own input count is
    // refused here instead of overflowing `index + 1` or reserving slots for indices no input reads.
    if let Some((index, path, _)) = inputs.iter().find(|entry| entry.0 >= inputs.len()) {
        return Err(DataFusionError::Plan(format!(
            "FusedSeries tree binds source index {index} at `{path}`, but it reads {} inputs in \
             total: every source index has to be below that count",
            inputs.len()
        )));
    }
    let width = inputs
        .iter()
        .map(|(index, ..)| index + 1)
        .max()
        .unwrap_or_default();
    let mut slots: Vec<Option<(NodePath, Arc<SeriesSchema>)>> = vec![None; width];
    for (index, path, schema) in inputs {
        match &slots[index] {
            Some((first, declared)) => {
                // One child can be read more than once, and both reads have to see the same layout:
                // the one frame this child produces is decoded once, so a second schema would name
                // a column the decode never fills.
                if !same_schema_fields(declared, &schema) {
                    return Err(DataFusionError::Plan(format!(
                        "FusedSeries tree reads source {index} at `{first}` and `{path}` through two \
                         raw series schemas, and one child has one layout"
                    )));
                }
            }
            None => slots[index] = Some((path, schema)),
        }
    }
    slots
        .into_iter()
        .enumerate()
        .map(|(index, slot)| match slot {
            Some((_, schema)) => Ok(schema),
            // `width` is the largest bound index plus one, so every hole has a bound index above
            // it: the refused tree binds a source without the ones below it.
            None => Err(DataFusionError::Plan(format!(
                "FusedSeries tree binds source index {}, but not every index below it: source \
                 {index} is unbound",
                width - 1
            ))),
        })
        .collect()
}

/// The raw series field names each source index of `tree` reads, in source order.
///
/// A caller asks the scan below for exactly these columns, so they are read off the tree that will
/// consume them rather than kept in a second description beside it.
pub(crate) fn tree_source_fields(tree: &NativeTree) -> DataFusionResult<Vec<Vec<String>>> {
    Ok(tree_sources(tree)?
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

/// Whether `tree` is the constant scalar `grid` claims and reads no raw input at all.
///
/// A scalar is declared by the type that produces it, not by a child: what makes one constant is
/// that it is a bound [`NativeExpr::Scalar`], so this is the one tree that can be legal with no
/// child. A tree whose root is samples cannot be: samples with no input would be an empty vector
/// reported as an answer, which is the shape this node must never invent.
fn is_constant_scalar(tree: &NativeTree, grid: &EvalGrid) -> bool {
    matches!(tree.root().expr, NativeExpr::Scalar(_))
        && matches!(&tree.root().output, NativeType::Scalar(spec) if spec.spec() == grid.spec())
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
/// The standalone planner declines subqueries, so its admitted trees have no nested window. This
/// check also protects [`FusedSeriesExec::try_new`] callers that supply a well-formed tree directly.
fn reject_a_split_logical_series(
    tree: &NativeTree,
    partition_count: usize,
) -> DataFusionResult<()> {
    if partition_count == 1 {
        return Ok(());
    }
    // Ancestry, not visit order: a window fills only from the nodes below it, so a rewriter is
    // dangerous exactly when a window encloses it. Reading the walk's order instead would treat the
    // two operands of one binary as each other's ancestor, and `rate(a[5m]) + rate(b[5m])` - two
    // sibling windows over disjoint inputs - would be refused for it.
    let mut windows: Vec<String> = Vec::new();
    let mut rewriters: Vec<String> = Vec::new();
    metrics::walk(tree.root(), &mut |node, path| {
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
        if rewrites {
            rewriters.push(path.as_str().to_string());
        }
        if matches!(node.expr, NativeExpr::Window { .. }) {
            windows.push(path.as_str().to_string());
        }
    });
    // A path is its parent's path plus one `.` segment, so a descendant matches at a segment
    // boundary and never inside a longer sibling name.
    let below = |path: &str, ancestor: &str| {
        path.len() > ancestor.len()
            && path.starts_with(ancestor)
            && path[ancestor.len()..].starts_with('.')
    };
    let mut rejection = None;
    for rewriter in &rewriters {
        if let Some(window) = windows
            .iter()
            .find(|window| below(rewriter, window.as_str()))
        {
            rejection = Some((rewriter.clone(), window.clone()));
            break;
        }
    }
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

/// Whether `left` and `right` hold the same fields, in the same order.
///
/// A raw frame's field list is its layout, so two schemas that name different fields are two
/// layouts: decoding one as the other would hand the tree a column it never bound.
fn same_schema_fields(left: &SeriesSchema, right: &SeriesSchema) -> bool {
    left.fields.len() == right.fields.len()
        && left
            .fields
            .iter()
            .zip(right.fields.iter())
            .all(|(a, b)| a.name == b.name)
}

/// Whether `schema` carries a field named `name`, as a column or inside a struct column.
///
/// A table scan names each series field as a column of its own, while a raw series frame names them
/// inside its `fields` struct: both can hand the tree the field it read, and a child that names it
/// nowhere cannot. This asks whether the layout can produce a field, which is all the tree needs; a
/// label identity is not read off a schema, so nothing here stands in for a label binding.
fn carries_field(schema: &DFSchema, name: &str) -> bool {
    fn inside(data_type: &DataType, name: &str) -> bool {
        match data_type {
            DataType::Struct(fields) => fields
                .iter()
                .any(|field| field.name().as_str() == name || inside(field.data_type(), name)),
            DataType::List(element) => inside(element.data_type(), name),
            _ => false,
        }
    }
    schema
        .fields()
        .iter()
        .any(|field| field.name().as_str() == name || inside(field.data_type(), name))
}

/// Checks that `tree` reads what `children` can give it, on the grid `binding` declares.
///
/// The tree names its sources: every raw input carries the `source_index` of the child that feeds
/// it, so what has to hold is that each of those indices names one of `children`, that every child
/// is read by at least one input, and that the child at an index can produce the fields the inputs
/// bound to it read. A tree with no raw input at all is legal only as a constant scalar, which reads
/// nothing and is the one shape whose every row the node can produce without a child.
///
/// The output grid is compared by [`EvalGrid::spec`], not by allocation: that is the rule
/// [`NativeTree`]'s own validation and the kernels apply, and a chunk's timestamps already come
/// from its absolute grid positions, so a separately allocated grid with the same spec is the same
/// grid to everything downstream.
fn validate_binding(
    tree: &NativeTree,
    binding: &FusedSeriesBinding,
    children: &[LogicalPlan],
) -> DataFusionResult<()> {
    if binding.tags.iter().any(|tag| tag == METRIC_NAME) && binding.metric_name.is_none() {
        return Err(DataFusionError::Plan(format!(
            "FusedSeries declares a {METRIC_NAME:?} tag column but binds no metric name"
        )));
    }
    // The sources are read first, so a child list of the wrong length is refused before anything is
    // read off the children it names.
    let sources = tree_sources(tree)?;
    if sources.is_empty() {
        if !children.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "FusedSeries runs a tree that reads no source, but {} children were given",
                children.len()
            )));
        }
        if !is_constant_scalar(tree, &binding.grid) {
            return Err(DataFusionError::Plan(
                "FusedSeries runs a tree that reads no source, and only a constant scalar on the \
                 statement's grid is legal without one"
                    .to_string(),
            ));
        }
    } else {
        if children.len() != sources.len() {
            return Err(DataFusionError::Plan(format!(
                "FusedSeries runs a tree that reads {} sources, but {} children were given",
                sources.len(),
                children.len()
            )));
        }
        for (index, (source, child)) in sources.iter().zip(children).enumerate() {
            // Which child a tree input reads is the input's own index, so a field it declared has
            // to be one *that* child can produce: reading it off another child, or off the tree
            // alone, would decode a frame the child never produced.
            for field in source.fields.iter() {
                if !carries_field(child.schema(), field.name.as_ref()) {
                    return Err(DataFusionError::Plan(format!(
                        "FusedSeries child {index} has no column {:?} for the field the tree input \
                         bound to it reads",
                        field.name
                    )));
                }
            }
        }
    }
    match tree.root().output.grid() {
        Some(grid) if grid.spec() == binding.grid.spec() => Ok(()),
        _ => Err(DataFusionError::Plan(
            "FusedSeries runs a tree whose output is not evaluated on the statement's grid"
                .to_string(),
        )),
    }
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

/// Classifies every output column, rejecting anything the conversion cannot produce.
///
/// `fields` is the field layout of the series the rows are converted from, by name and position: a
/// column naming one of them is a value column, one naming `tags` is a label column, and `time_column`
/// is the grid axis. It is passed as the layout rather than as a [`SeriesSchema`] because the
/// finalizer's series are described by the statement's own row schema, which carries the statement's
/// names for those fields rather than the compiled tree's.
///
/// Visible to the finalizer because it builds the same row adapter over the series it assembles; a
/// second copy would let the two nodes disagree about which column a name reads.
pub(crate) fn classify_output(
    output_schema: &SchemaRef,
    fields: &[FieldSpec],
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
        if fields.iter().any(|field| field.name.as_ref() == tag) {
            return Err(DataFusionError::Plan(format!(
                "tag column {tag:?} of FusedSeriesExec clashes with a series field"
            )));
        }
    }

    let mut time_seen = false;
    let mut field_seen = vec![false; fields.len()];
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
        if let Some(index) = fields.iter().position(|field| field.name.as_ref() == name) {
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
                fields[index].name
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
    /// The label source and ordering of each child's frames, in child order.
    ///
    /// The frame carries the encoded key and, in explicit mode, the complete labels, but no schema
    /// can say which of the two a producer meant or how it ordered the series, so the caller that
    /// built the frame passes both on and they are kept here to check a rebuilt child against.
    child_bindings: Vec<(RawLabelSource, SeriesOrdering)>,
    /// Where each output column comes from.
    classification: Arc<[OutputColumn]>,
    /// The output column holding the grid timestamps, kept so this node can be rebuilt over a new
    /// child without losing which column its time came from.
    time_column: String,
    /// The tag column names, indexed by [`OutputColumn::Tag`].
    tags: Arc<[String]>,
    /// The plans producing the raw series frames.
    children: Vec<Arc<dyn ExecutionPlan>>,
    /// The columns this node produces.
    output_schema: SchemaRef,
    /// The grid positions every label set in this plan's output already occupies, and what they
    /// keep charged on the memory pool.
    ///
    /// Plan-level rather than per-partition: see [`LabelSets`].
    labels: SharedLabelSets,
    /// Per-node metrics, registered by each execution of the tree.
    metric: ExecutionPlanMetricsSet,
    /// The aggregate whose partition-local state this node exports, or `None` when its batches are
    /// the statement's answer.
    partial_fold: Option<BoundAggregate>,
    /// What the exported state batches keep charged on the memory pool.
    ///
    /// Plan-level, like [`Self::labels`]: they are paid for until the plan is dropped, and the
    /// reservation is registered by the first partition that actually exports.
    export_budget: Arc<Mutex<Option<MemoryReservation>>>,
    /// This node's partitioning, emission and boundedness.
    properties: Arc<PlanProperties>,
}

impl FusedSeriesExec {
    /// Creates the node, rejecting whatever this node cannot execute or convert.
    ///
    /// Each child produces native raw series frames: the series' encoded key, its finality, its
    /// timestamps and its fields, plus the complete labels when its `label_source` is
    /// [`RawLabelSource::Explicit`]. `time_column` and `tags` name the columns of the output, which
    /// is a row table rather than a frame.
    ///
    /// The children are the tree's sources in `source_index` order, so their number must be the
    /// number of sources the tree reads. Exactly one is executed: a second child would have to pair
    /// its series with the first by label, which needs an exchange this node does not have, and no
    /// child at all is a constant scalar, whose rows the conversion does not produce yet. Both are
    /// refused rather than answered with an empty result.
    ///
    /// Which frame layout the child must produce is not passed in: it is the raw series schema the
    /// tree's own input was compiled against, so there is no second description to drift from it.
    /// The ordering the caller declares is checked against every input node's binding, so a
    /// producer weaker than one of them is refused before the tree reads a frame.
    ///
    /// The parameters stay separate because each is a fact its own caller has: the query, the plans
    /// below, the frame's label binding and ordering, the tree, and the three things the output
    /// schema is classified by. A struct over them would be the same list under another name.
    ///
    /// This builds the ordinary node, whose batches are the statement's rows.
    /// [`FusedSeries::try_new_partial`] is the other mode, and goes through the same checks before
    /// it keeps the aggregate whose state its partitions export.
    pub fn try_new(
        promql: Arc<str>,
        children: Vec<(Arc<dyn ExecutionPlan>, RawLabelSource, SeriesOrdering)>,
        tree: Arc<NativeTree>,
        output_schema: SchemaRef,
        time_column: String,
        tags: Vec<String>,
    ) -> DataFusionResult<Self> {
        Self::build(
            promql,
            children,
            tree,
            output_schema,
            time_column,
            tags,
            None,
        )
    }

    /// Creates the node, with `partial_fold` deciding whether its batches are the statement's rows
    /// or the state each partition folded.
    ///
    /// In partial mode the output schema is the state layout, so it is not classified; everything
    /// the input and the tree are checked for holds in both modes.
    fn build(
        promql: Arc<str>,
        children: Vec<(Arc<dyn ExecutionPlan>, RawLabelSource, SeriesOrdering)>,
        tree: Arc<NativeTree>,
        output_schema: SchemaRef,
        time_column: String,
        tags: Vec<String>,
        partial_fold: Option<BoundAggregate>,
    ) -> DataFusionResult<Self> {
        // Several children are refused on their count alone, before the tree is walked: what they
        // would need is an exchange that pairs their series by label, and no tree shape makes that
        // available to this node.
        if children.len() > 1 {
            return Err(DataFusionError::Plan(format!(
                "FusedSeriesExec reads {} sources as one tree: pairing several sources needs an \
                 exchange that pairs their series by label, which this node does not have",
                children.len()
            )));
        }
        let sources = tree_sources(&tree)?;
        if children.len() != sources.len() {
            return Err(DataFusionError::Plan(format!(
                "FusedSeriesExec runs a tree that reads {} sources, but {} children were given",
                sources.len(),
                children.len()
            )));
        }
        // No child is a constant scalar: a tree this node cannot run at all, and refused before the
        // output schema is read, so the refusal names the missing conversion rather than a column.
        if children.is_empty() {
            return Err(DataFusionError::Plan(
                "FusedSeriesExec runs a tree that reads no source: a constant scalar's rows are not \
                 produced by this node yet"
                    .to_string(),
            ));
        }
        // One child and one tree source, which the arity check above has just made equal in count:
        // taking both without indexing keeps this total even though neither can be empty here.
        let (Some((child, source, ordering)), Some(tree_source)) =
            (children.first(), sources.first())
        else {
            return Err(DataFusionError::Plan(
                "FusedSeriesExec runs a tree whose sources do not pair with its children"
                    .to_string(),
            ));
        };
        // The producer's ordering is a promise each input node was compiled against, so it has to
        // satisfy every one of them, not only the input the frames happen to reach first: a stream
        // that may interleave two series cannot feed a node that reads one series to its last chunk,
        // and merely contiguous frames are not yet the identity-then-time order an input may be
        // bound to. Only one child is executed, and `tree_sources` has just made every input's
        // source index name one of them, so all of them read this child's ordering.
        let producer = *ordering;
        let mut ordering_error: Option<String> = None;
        metrics::walk(tree.root(), &mut |node, path| {
            let NativeExpr::Input(bound) = &node.expr else {
                return;
            };
            let required = bound.ordering;
            let satisfied = match required {
                // The weakest guarantee asks nothing of the producer, and an input declaring
                // `Interleaved` is refused when the run state is built rather than here.
                SeriesOrdering::Interleaved => true,
                SeriesOrdering::Contiguous => producer != SeriesOrdering::Interleaved,
                SeriesOrdering::KeyThenTime => producer == SeriesOrdering::KeyThenTime,
            };
            if !satisfied && ordering_error.is_none() {
                let index = bound.source_index;
                ordering_error = Some(format!(
                    "FusedSeriesExec reads source {index} through {producer:?} frames, but the tree \
                     input at `{path}` was bound to {required:?} frames, which is a stronger ordering \
                     the producer does not provide"
                ));
            }
        });
        if let Some(message) = ordering_error {
            return Err(DataFusionError::Plan(message));
        }
        let _ = NativeExecutionState::try_new(
            &tree,
            Arc::new(UnboundedMemoryPool::default()),
            &ExecutionPlanMetricsSet::new(),
            0,
        )?;
        // The output columns are named by the fields the *tree* reports, so they are classified
        // against the tree's own output rather than against whichever scan happens to sit first:
        // `a + b` reports the fields of `a`, and reading them off the input would silently accept an
        // output schema that names a field no input ever produced. State mode has no row adapter to
        // classify: the fold writes its batches in the exchange's own layout.
        let root_schema = tree
            .root()
            .output
            .samples()
            .ok_or_else(|| {
                DataFusionError::Plan(
                    "FusedSeriesExec runs a tree whose output is not samples".to_string(),
                )
            })?
            .clone();
        let classification: Arc<[OutputColumn]> = if partial_fold.is_some() {
            // State batches are made by the fold in its own layout, not by the row adapter.
            Arc::<[OutputColumn]>::from(Vec::new())
        } else {
            classify_output(
                &output_schema,
                root_schema.fields.as_ref(),
                &tags,
                &time_column,
            )?
            .into()
        };
        let partition_count = child.properties().partitioning.partition_count();
        if partition_count == 0 {
            return Err(DataFusionError::Plan(
                "FusedSeriesExec child has no partitions".to_string(),
            ));
        }
        // The frame is checked against the schema the tree's own input was compiled against, which
        // is the one the decoder is given in `execute`: no other schema is stored, so a child that
        // changed shape cannot be decoded with a stale layout.
        ensure_frame_matches_label_source(child.schema().as_ref(), source)?;
        unpack_raw_series(
            Box::pin(RecordBatchStreamAdapter::new(
                child.schema(),
                futures::stream::empty(),
            )),
            Some(Arc::clone(tree_source)),
        )?;
        reject_a_split_logical_series(&tree, partition_count)?;
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(partition_count),
            child.properties().emission_type,
            child.properties().boundedness,
        ));
        Ok(Self {
            promql,
            tree,
            child_bindings: vec![(source.clone(), *ordering)],
            classification,
            time_column,
            tags: tags.into(),
            children: vec![Arc::clone(child)],
            output_schema,
            labels: Arc::new(Mutex::new(LabelSets::default())),
            metric: ExecutionPlanMetricsSet::new(),
            partial_fold,
            export_budget: Arc::new(Mutex::new(None)),
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
        write!(f, "FusedSeriesExec: {}\n{}", self.promql, self.tree)?;
        // State is not the statement's answer, so a reader sees the mode rather than infer it.
        if let Some(op) = &self.partial_fold {
            write!(f, "\npartial state: {}", render_partial_fold(op))?;
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

    /// Nothing is asked of any input's distribution, and nothing may be inserted above one.
    ///
    /// A hash requirement would make a hash exchange necessary as soon as the child has more than
    /// one partition - even where the child already satisfies the requirement - and a hash exchange
    /// does not preserve order. The input's own layout is already the one this node needs, because
    /// the scan it reads from guarantees a series stays in one partition, so the requirement says
    /// only that each layout is left alone.
    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        InputDistributionRequirements::new(vec![
            Distribution::UnspecifiedDistribution;
            self.children.len()
        ])
    }

    /// Splitting the input further would not help: a second partition is a second set of series
    /// answered by a second tree, which this node already runs, and raising the count is what an
    /// optimizer does by adding a round-robin exchange that reorders nothing it can rely on.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false; self.children.len()]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.children.iter().collect()
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
        if children.len() != self.children.len() {
            return Err(DataFusionError::Plan(format!(
                "FusedSeriesExec requires {} children, got {}",
                self.children.len(),
                children.len()
            )));
        }
        // Rebuilt rather than patched: the schema the tree names is what the new child's frame is
        // validated against, so a child that changed shape is refused instead of decoded with a
        // stale layout. A child keeps the position it had, and so the source index it feeds: which
        // child a tree input reads is never inferred from a schema. The mode and its reservation
        // carry over: rebuilding must not turn a state export into a node that answers the
        // statement.
        let rebuilt = Self::build(
            Arc::clone(&self.promql),
            children
                .into_iter()
                .zip(&self.child_bindings)
                .map(|(child, (source, ordering))| (child, source.clone(), *ordering))
                .collect(),
            Arc::clone(&self.tree),
            self.output_schema.clone(),
            self.time_column.clone(),
            self.tags.to_vec(),
            self.partial_fold.clone(),
        )?;
        Ok(Arc::new(Self {
            export_budget: Arc::clone(&self.export_budget),
            ..rebuilt
        }))
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
        // One fold per partition, over only the series this partition's scan returned. What it
        // folds is the tree's *root* output - the prefix's evaluated series - while the node's own
        // output schema is the state layout the fold encodes, which is why the two are read from
        // different places.
        let partial_fold = match &self.partial_fold {
            Some(op) => {
                let schema = self.tree.root().output.samples().cloned().ok_or_else(|| {
                    DataFusionError::Plan(
                        "FusedSeries exports aggregate state from a tree that produces no samples"
                            .to_string(),
                    )
                })?;
                Some(PartialFold::try_new(
                    op.clone(),
                    schema,
                    Arc::clone(&self.tags),
                    Arc::clone(&self.export_budget),
                    &context.runtime_env().memory_pool,
                )?)
            }
            None => None,
        };
        // The child is executed once, at the partition this node was asked for, because a series
        // never spans two partitions: an execution of the tree owns every chunk of every series the
        // child reports in that partition, and the chunks are queued at every tree input bound to
        // source 0, so a tree that reads one series twice reads the same rows at both.
        let (Some(child), Some((label_source, _))) =
            (self.children.first(), self.child_bindings.first())
        else {
            return Err(DataFusionError::Execution(
                "FusedSeriesExec executes one child, and its plan holds another number".to_string(),
            ));
        };
        // The layout the frame is decoded against is the one the tree's own input was compiled
        // against, read back off the tree here rather than kept on the node: a stored copy could
        // only disagree with it. The walk is tree-sized and runs once per execution, not per chunk.
        let sources = tree_sources(&self.tree)?;
        let [tree_source] = sources.as_slice() else {
            return Err(DataFusionError::Execution(
                "FusedSeriesExec runs a tree that reads no source".to_string(),
            ));
        };
        let stream = child.execute(partition, Arc::clone(&context))?;
        ensure_frame_matches_label_source(child.schema().as_ref(), label_source)?;
        let stream = unpack_raw_series(stream, Some(Arc::clone(tree_source)))?;
        let stream =
            materialize_raw_labels(stream).map_err(|e| DataFusionError::External(Box::new(e)))?;
        // The execution name is the partition it answers, which is the one it was asked for.
        let state = NativeExecutionState::try_new(
            &self.tree,
            Arc::clone(&context.runtime_env().memory_pool),
            &self.metric,
            partition,
        )?;
        // What the label sets hold is charged to the reservation attached above, so nothing is
        // reserved per stream: the entries outlive every stream and are released with the plan.
        // The stream keeps a share of the export reservation so queued state stays charged.
        Ok(Box::pin(FusedSeriesStream {
            input: Some(stream),
            executor: Some(NativeExecutor::new(state)),
            classification: Arc::clone(&self.classification),
            tags: Arc::clone(&self.tags),
            output_schema: self.output_schema.clone(),
            labels: Arc::clone(&self.labels),
            partial_fold,
            _export_budget: Arc::clone(&self.export_budget),
            ready: VecDeque::new(),
            input_ended: false,
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
/// [`datatypes::series::MetricName`] itself: a label with an empty value is no label to
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
    /// The one child's decoded chunks, or `None` once the stream let go of it.
    input: Option<SendableRawSeriesStream>,
    /// The execution of this node's tree, or `None` once it has been closed.
    executor: Option<NativeExecutor>,
    /// Where each output column comes from.
    classification: Arc<[OutputColumn]>,
    /// The tag column names, indexed by `OutputColumn::Tag`.
    tags: Arc<[String]>,
    /// The schema of every batch this stream yields.
    output_schema: SchemaRef,
    /// The label-set occupancy shared with the other partitions' streams.
    ///
    /// What it holds is charged to the plan's own reservation, attached by the first execution. Every
    /// partition's prefix series is claimed, state mode included; only the encoded state rows are
    /// not, because a group's labels may legitimately repeat across partitions.
    labels: SharedLabelSets,
    /// This partition's fold, or `None` once its input ended or it stopped early.
    ///
    /// In state mode the chunks the executor settles are folded into it instead of converted, and
    /// the state it exports when the input ends is what the stream hands over.
    partial_fold: Option<PartialFold>,
    /// A share of the plan's reservation for the exported state batches.
    ///
    /// Holding the plan's `Arc` keeps the bytes of queued batches charged until this stream drops;
    /// nothing new is reserved, and in the ordinary mode it owns no reservation.
    _export_budget: Arc<Mutex<Option<MemoryReservation>>>,
    /// The batches the executor settled since the last poll of the input.
    ready: VecDeque<RecordBatch>,
    /// Whether the executor has already been told that every input ended.
    input_ended: bool,
    /// Whether the input ended and everything it settled has been handed out.
    done: bool,
}

impl FusedSeriesStream {
    /// Pushes one chunk of the child's frames at every tree input bound to its source.
    ///
    /// Which inputs those are is the tree's own binding: a tree that reads one series twice has two
    /// input nodes on source 0, and the executor hands both the same rows.
    fn push(&mut self, chunk: RawSeriesChunk) -> DataFusionResult<()> {
        let executor = self.executor.as_mut().ok_or_else(|| {
            DataFusionError::Execution(
                "FusedSeriesExec stream pushed a chunk after its execution closed".into(),
            )
        })?;
        executor.push_raw_source(0, chunk)?;
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
        // The child's stream is dropped here: its plan, its decoder and whatever batch it was
        // holding are released as soon as this node stops reading them.
        self.input = None;
        // A fold that stops early exports nothing: only a whole partition's states may be merged,
        // so a half-read group would be taken for the whole one.
        self.partial_fold = None;
        // Its export charge is the plan's and stays until this stream drops, like the label sets.
        // The label-set map and its charge are the plan's, so a stream stopping early gives back
        // nothing: the entries another partition is still checking against have to stay charged, and
        // are released when the plan itself is dropped.
        // A batch queued before the error was settled by a run that has now stopped.
        self.ready.clear();
    }

    /// Converts the chunks the executor settled into batches, in the order it produced them.
    fn convert(&mut self, chunks: Vec<EvaluatedChunk>) -> DataFusionResult<()> {
        let Self {
            partial_fold,
            labels,
            classification,
            tags,
            output_schema,
            ready,
            ..
        } = self;
        for chunk in chunks {
            // The prefix series is claimed before it is folded or converted either way: a chunk
            // whose labels only *look* like a group's would be counted by two folds that never see
            // each other, and the merge above cannot tell the halves from two contributions.
            // Claimed before the batch is queued, so a collision surfaces at the chunk that reached
            // it.
            labels
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .claim(&chunk.series)?;
            // In state mode the chunk is folded, not converted: each partition's fold keeps its own
            // groups, and the series the exchange will see are assembled above this node.
            if let Some(fold) = partial_fold.as_mut() {
                fold.update(chunk)?;
                continue;
            }
            let batch = series_to_record_batch(classification, tags, output_schema, &chunk)?;
            ready.push_back(batch);
        }
        Ok(())
    }

    /// Ends the fold and queues the state batches it exports, once. The fold is taken first, so a
    /// refusal leaves nothing to finish twice.
    fn finish_fold(&mut self) -> DataFusionResult<()> {
        let Some(mut fold) = self.partial_fold.take() else {
            return Ok(());
        };
        for batch in fold.finish()? {
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
        let mut budget = POLL_BUDGET;
        loop {
            if let Some(batch) = this.ready.pop_front() {
                return Poll::Ready(Some(Ok(batch)));
            }
            if budget == 0 {
                break;
            }
            if this.done {
                return Poll::Ready(None);
            }
            if this.input_ended {
                match this.pull_one() {
                    Ok(Some(chunk)) => {
                        if let Err(error) = this.convert(vec![chunk]) {
                            this.fail();
                            return Poll::Ready(Some(Err(error)));
                        }
                    }
                    Ok(None) => {
                        // The execution closed, so the fold has every chunk it will see: this is
                        // where a partition's state becomes batches.
                        if let Err(error) = this.finish_fold() {
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
            }
            let Some(input) = this.input.as_mut() else {
                // Only `fail` lets go of the child, and it sets `done`, which the loop above
                // returns on, so reaching this means nothing more will be read.
                return Poll::Ready(None);
            };
            let polled = input.as_mut().poll_next(cx);
            match polled {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Err(error))) => {
                    this.fail();
                    return Poll::Ready(Some(Err(DataFusionError::External(Box::new(error)))));
                }
                Poll::Ready(Some(Ok(chunk))) => {
                    budget -= 1;
                    if let Err(error) = this.push(chunk) {
                        this.fail();
                        return Poll::Ready(Some(Err(error)));
                    }
                }
                Poll::Ready(None) => {
                    if let Err(error) = this.close_input() {
                        this.fail();
                        return Poll::Ready(Some(Err(error)));
                    }
                    this.input_ended = true;
                }
            }
        }
        // Reaching the chunk budget means there is productive input still to process. Wake the
        // parent to continue; a genuinely pending input already registered its own waker.
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}
