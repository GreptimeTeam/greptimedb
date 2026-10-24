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

//! One global `topk`/`bottomk` over the evaluated frames another node produced.
//!
//! Which `k` series a group keeps at a grid point depends on every series present there, so the
//! selection is one answer for the whole vector: a per-partition `topk` would answer with that
//! partition's best, not the query's. This node therefore reads its input as one global stream in
//! producer-then-ordinal order, which makes each series' chunks contiguous again.
//!
//! The tree is cut directly at the root, not searched for: the child of the root `Vector` node is
//! the prefix, run over the raw input and emitted as frames, and the same root node is the suffix,
//! with its child replaced by one bound input. Any other shape is
//! [`DataFusionError::NotImplemented`], reported before this node exists, so a caller can fall back
//! to the existing PromQL planner.

#[cfg(test)]
mod tests;

use std::cmp::Ordering;
use std::fmt::{self, Formatter};
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DFSchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation, UnboundedMemoryPool};
use datafusion::logical_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::{
    EquivalenceProperties, LexOrdering, LexRequirement, OrderingRequirements, PhysicalExpr,
    PhysicalSortExpr,
};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, InputDistributionRequirements,
    Partitioning, PlanProperties, RecordBatchStream, SendableRecordBatchStream,
};
use datatypes::series::SeriesOrdering;
use datatypes::series::samples::SeriesSchema;
use futures::Stream;

use crate::extension_plan::evaluated_frame::{self, Sequence};
use crate::extension_plan::fused_series::{
    FusedSeries, FusedSeriesBinding, LabelSets, OutputColumn, classify_output,
    series_to_record_batch,
};
use crate::native::execute::{InputId, NativeExecutionState, NativeExecutor};
use crate::native::plan::{
    BoundInput, BoundVectorOp, NativeExpr, NativeNode, NativeTree, reads_across_series,
};

/// How many input steps one poll runs before it wakes itself.
///
/// A frame row settles nothing before the input ends, so without a budget an input of many frames
/// would be read to its end inside one poll. The figure is the fused node's, which reads the same
/// stream the same way.
const POLL_BUDGET: usize = 64;

/// The only ordering placed below the selection: ascending, nulls last.
const ASCENDING: SortOptions = SortOptions {
    descending: false,
    nulls_first: false,
};

/// The logical plan of one global vector selection over evaluated frames.
#[derive(Debug)]
pub struct VectorSelect {
    /// The PromQL text this node was compiled from.
    promql: Arc<str>,
    /// The frames, produced by the prefix.
    input: LogicalPlan,
    /// The root selection alone, over one bound input.
    suffix: Arc<NativeTree>,
    /// The evaluated series schema the frames are packed against.
    series_schema: Arc<SeriesSchema>,
    /// The output column holding the grid timestamps.
    time_column: String,
    /// The logical tag names the output carries.
    tags: Vec<String>,
    /// The columns this node produces.
    output_schema: DFSchemaRef,
}

impl VectorSelect {
    /// The name this node is shown under.
    pub const fn name() -> &'static str {
        "VectorSelect"
    }

    /// Creates the node for `full_tree`, whose frames come from `raw_input`.
    ///
    /// The prefix is the root's child tree run over the raw input and emitted as frames; the suffix
    /// is the root itself with its child replaced by one input. A tree this node is not the answer
    /// for is [`DataFusionError::NotImplemented`], which is what lets a caller fall back before any
    /// extension node is created.
    pub fn try_new(
        promql: Arc<str>,
        full_tree: NativeTree,
        binding: FusedSeriesBinding,
        raw_input: LogicalPlan,
        output_schema: DFSchemaRef,
    ) -> DataFusionResult<Self> {
        let (op, child) = match &full_tree.root().expr {
            NativeExpr::Vector { op, input } => (op.clone(), input),
            _ => {
                return Err(unsupported(
                    "a tree whose root is not an operation over the whole input vector",
                ));
            }
        };
        if !matches!(op, BoundVectorOp::TopK { .. }) {
            return Err(unsupported("a vector operation other than topk or bottomk"));
        }
        // A frame names one series' chunk and says nothing about the vector it belongs to, so a
        // child that reads across series cannot be framed.
        if reads_across_series(child) {
            return Err(unsupported(
                "a topk over an expression that reads across series",
            ));
        }
        let Some(child_schema) = child.output.samples() else {
            return Err(unsupported(
                "a topk over an expression that is not a vector",
            ));
        };
        if child_schema.fields.len() != 1 {
            return Err(unsupported(
                "a topk over a vector of more than one field, which a frame cannot carry",
            ));
        }
        // The frames name absolute positions on the statement's grid, which is the grid the time
        // column stamps, so the two have to agree.
        let Some(child_grid) = child_schema.eval_grid.as_ref() else {
            return Err(unsupported(
                "a topk over an expression that is not evaluated",
            ));
        };
        if child_grid.spec() != binding.grid.spec() {
            return Err(unsupported(
                "a topk whose input is evaluated on another grid than the statement's",
            ));
        }

        let time_column = binding.time_index.clone();
        let tags = binding.tags.clone();
        let prefix = FusedSeries::try_new_evaluated(
            Arc::clone(&promql),
            NativeTree::try_new(child.as_ref().clone())?,
            binding,
            raw_input,
        )?;
        // The frames arrive ordered by producer and then ordinal, so each series' chunks are
        // contiguous and no series interleaves with another.
        let suffix = Arc::new(NativeTree::try_new(NativeNode {
            expr: NativeExpr::Vector {
                input: Box::new(NativeNode {
                    expr: NativeExpr::Input(BoundInput {
                        output: child.output.clone(),
                        ordering: SeriesOrdering::Contiguous,
                    }),
                    output: child.output.clone(),
                }),
                op,
            },
            output: full_tree.root().output.clone(),
        })?);
        Ok(Self {
            promql,
            input: LogicalPlan::Extension(Extension {
                node: Arc::new(prefix),
            }),
            suffix,
            series_schema: Arc::clone(child_schema),
            time_column,
            tags,
            output_schema,
        })
    }

    /// The physical node that runs `exec_input` as this node's input.
    ///
    /// The selection is one answer for the whole vector, so the frames are brought into one
    /// partition and ordered by producer and ordinal first. Both steps are ordinary DataFusion
    /// nodes, and the ordering this node requires is exactly the sort below it.
    pub fn to_execution_plan(
        &self,
        exec_input: Arc<dyn ExecutionPlan>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let global = match exec_input.properties().partitioning.partition_count() {
            0 => {
                return Err(DataFusionError::Plan(
                    "VectorSelect has an input with no partitions".to_string(),
                ));
            }
            1 => exec_input,
            _ => Arc::new(CoalescePartitionsExec::new(exec_input)),
        };
        let sort = SortExec::new(frame_ordering(global.schema().as_ref())?, global);
        Ok(Arc::new(VectorSelectExec::try_new(
            Arc::clone(&self.promql),
            Arc::new(sort),
            Arc::clone(&self.suffix),
            Arc::clone(&self.series_schema),
            self.output_schema.inner().clone(),
            self.time_column.clone(),
            self.tags.clone(),
        )?))
    }
}

impl UserDefinedLogicalNodeCore for VectorSelect {
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
        // The selection's operands are bound inside its tree.
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "VectorSelect: {}", self.promql)
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        if inputs.len() != 1 {
            return Err(DataFusionError::Internal(
                "VectorSelect should have exactly one input".to_string(),
            ));
        }
        Ok(Self {
            promql: Arc::clone(&self.promql),
            input: inputs.pop().unwrap(),
            suffix: Arc::clone(&self.suffix),
            series_schema: Arc::clone(&self.series_schema),
            time_column: self.time_column.clone(),
            tags: self.tags.clone(),
            output_schema: Arc::clone(&self.output_schema),
        })
    }
}

impl PartialEq for VectorSelect {
    fn eq(&self, other: &Self) -> bool {
        // `NativeTree` carries no structural equality, so its rendering stands in for it.
        self.promql == other.promql
            && self.suffix.to_string() == other.suffix.to_string()
            && self.input == other.input
            && self.time_column == other.time_column
            && self.tags == other.tags
            && self.output_schema == other.output_schema
    }
}

impl Eq for VectorSelect {}

impl Hash for VectorSelect {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.promql.hash(state);
        self.input.hash(state);
        self.time_column.hash(state);
        self.tags.hash(state);
        self.output_schema.hash(state);
    }
}

impl PartialOrd for VectorSelect {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.promql.partial_cmp(&other.promql) {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        match self
            .suffix
            .to_string()
            .partial_cmp(&other.suffix.to_string())
        {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        match self.input.partial_cmp(&other.input) {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        match self.time_column.partial_cmp(&other.time_column) {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        match self.tags.partial_cmp(&other.tags) {
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

/// The refusal for a statement this node does not answer.
fn unsupported(what: &str) -> DataFusionError {
    DataFusionError::NotImplemented(format!("VectorSelect does not handle {what}"))
}

/// The ordering the frames are read in: producer ascending, then frame ordinal ascending.
///
/// Both columns are taken from `schema`, so the sort can only name columns the frame reader accepts.
fn frame_ordering(schema: &Schema) -> DataFusionResult<LexOrdering> {
    let column = |index: usize| -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        let field = schema.fields().get(index).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "an evaluated frame has {} columns, so it has no column {index}",
                schema.fields().len()
            ))
        })?;
        Ok(Arc::new(Column::new(field.name(), index)))
    };
    LexOrdering::new([
        PhysicalSortExpr::new(column(0)?, ASCENDING),
        PhysicalSortExpr::new(column(1)?, ASCENDING),
    ])
    .ok_or_else(|| DataFusionError::Plan("the ordering of an evaluated frame is empty".to_string()))
}

/// The physical node that runs one selection over the whole input vector.
pub struct VectorSelectExec {
    /// The PromQL text this suffix was compiled from, which is what the plan line shows.
    promql: Arc<str>,
    /// The ordered frames, in one partition.
    input: Arc<dyn ExecutionPlan>,
    /// The root selection alone.
    suffix: Arc<NativeTree>,
    /// The evaluated series schema every frame is read against.
    series_schema: Arc<SeriesSchema>,
    /// Where each output column comes from.
    classification: Arc<[OutputColumn]>,
    /// The tag column names, indexed by [`OutputColumn::Tag`].
    tags: Arc<[String]>,
    /// The output column holding the grid timestamps, kept so the node can be rebuilt.
    time_column: String,
    /// The columns this node produces.
    output_schema: SchemaRef,
    /// The ordering the frames arrive in, which is also what this node requires.
    ordering: LexOrdering,
    /// The label sets the final series claim, shared by every execution of this plan.
    labels: Arc<Mutex<LabelSets>>,
    /// The counters every execution of the suffix registered, retained so the plan reports them
    /// after that execution is gone.
    ///
    /// The set is synchronised by DataFusion itself, so it needs no lock of this node's own.
    metrics: ExecutionPlanMetricsSet,
    /// This node's partitioning, emission and boundedness.
    properties: Arc<PlanProperties>,
}

impl VectorSelectExec {
    /// Creates the node, rejecting an input it cannot read and an output it cannot convert.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        promql: Arc<str>,
        input: Arc<dyn ExecutionPlan>,
        suffix: Arc<NativeTree>,
        series_schema: Arc<SeriesSchema>,
        output_schema: SchemaRef,
        time_column: String,
        tags: Vec<String>,
    ) -> DataFusionResult<Self> {
        if input.schema().as_ref() != evaluated_frame::schema().as_ref() {
            return Err(DataFusionError::Plan(
                "VectorSelectExec reads evaluated frames, but its input is not that layout"
                    .to_string(),
            ));
        }
        let partition_count = input.properties().partitioning.partition_count();
        if partition_count != 1 {
            return Err(DataFusionError::Plan(format!(
                "VectorSelectExec reads one global input, but its input has {partition_count} \
                 partitions"
            )));
        }
        let ordering = frame_ordering(input.schema().as_ref())?;
        // Building a throwaway state proves the runtime executes this tree.
        let _ = NativeExecutionState::try_new(&suffix, Arc::new(UnboundedMemoryPool::default()))?;
        let classification =
            classify_output(&output_schema, &series_schema.fields, &tags, &time_column)?;
        // One partition and a final result: nothing is answered before the input ended.
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            input.properties().boundedness,
        ));
        Ok(Self {
            promql,
            input,
            suffix,
            series_schema,
            classification: classification.into(),
            tags: tags.into(),
            time_column,
            output_schema,
            ordering,
            labels: Arc::new(Mutex::new(LabelSets::default())),
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
        })
    }

    /// The name this node is shown under.
    pub const fn name() -> &'static str {
        "VectorSelectExec"
    }
}

impl std::fmt::Debug for VectorSelectExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("VectorSelectExec").finish()
    }
}

impl DisplayAs for VectorSelectExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "VectorSelectExec: {}\n{}", self.promql, self.suffix)
    }
}

impl ExecutionPlan for VectorSelectExec {
    fn name(&self) -> &str {
        Self::name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    /// A global selection is one answer for the whole vector. A hash requirement would force a hash
    /// exchange as soon as the child has more than one partition, and that exchange reorders the
    /// frames the sort below just ordered.
    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        InputDistributionRequirements::new(vec![Distribution::SinglePartition])
    }

    /// The frames have to arrive in exactly the order the sort below produces; stating it in the
    /// sort's own terms is what keeps `EnforceSorting` from removing it.
    ///
    /// The requirement is the stored ordering rather than a second construction of it: this method
    /// cannot report an error, and rebuilding the columns here would need one.
    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![Some(OrderingRequirements::new(LexRequirement::from(
            self.ordering.clone(),
        )))]
    }

    /// Splitting the input cannot help: the selection ranks the whole vector.
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
        // The selection's operands are bound inside its tree.
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let [child] = <[Arc<dyn ExecutionPlan>; 1]>::try_from(children).map_err(|children| {
            DataFusionError::Plan(format!(
                "VectorSelectExec requires exactly one child, got {}",
                children.len()
            ))
        })?;
        // Rebuilt rather than patched: the new child's schema and partitioning are what the frame
        // reader and the global ordering rely on.
        Ok(Arc::new(Self::try_new(
            Arc::clone(&self.promql),
            child,
            Arc::clone(&self.suffix),
            Arc::clone(&self.series_schema),
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
        // The label-set state is the plan's, so its charge is attached once and released with the
        // plan rather than with a stream.
        self.labels
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .attach(&context.runtime_env().memory_pool);
        let input = self.input.execute(partition, Arc::clone(&context))?;
        let state = NativeExecutionState::try_new(
            &self.suffix,
            Arc::clone(&context.runtime_env().memory_pool),
        )?;
        // The counters are the plan's rather than the stream's: the execution registers them once,
        // and the plan keeps reporting them after that execution is dropped.
        state.register_metrics(&self.metrics, partition);
        // One frame batch is held at a time, so this reservation follows that batch.
        let frames = MemoryConsumer::new("VectorSelectExec frames")
            .register(&context.runtime_env().memory_pool);
        Ok(Box::pin(VectorSelectStream {
            input: Some(input),
            batch: None,
            row: 0,
            sequence: Sequence::default(),
            executor: Some(NativeExecutor::new(state)),
            input_id: InputId::new(0),
            series_schema: Some(Arc::clone(&self.series_schema)),
            classification: Arc::clone(&self.classification),
            tags: Arc::clone(&self.tags),
            output_schema: self.output_schema.clone(),
            labels: Arc::clone(&self.labels),
            frames,
            draining: false,
            done: false,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        // The set holds the executions' counter handles, so reading it again hands back the same
        // entries and registers nothing.
        Some(self.metrics.clone_inner())
    }
}

/// The batches one execution of [`VectorSelectExec`] yields.
struct VectorSelectStream {
    /// The ordered frames, or `None` once the input ended or failed.
    input: Option<SendableRecordBatchStream>,
    /// The frame batch in hand, of which `row` is next.
    batch: Option<RecordBatch>,
    /// The first row of `batch` not handed over yet.
    row: usize,
    /// The producer and ordinal order the frames have to arrive in.
    sequence: Sequence,
    /// The execution of this node's suffix, or `None` once its answer is complete.
    executor: Option<NativeExecutor>,
    /// The position the suffix's input node is fed at.
    input_id: InputId,
    /// The evaluated series schema every frame is read against, or `None` once this stream is done.
    ///
    /// It is the only thing here that can hold the statement's grid alive - the plan's copy is not
    /// reachable through this stream once the stream is gone - so it is given up when the stream
    /// reaches its end or fails, after the execution that read it has been dropped, rather than
    /// with the stream itself.
    series_schema: Option<Arc<SeriesSchema>>,
    /// Where each output column comes from.
    classification: Arc<[OutputColumn]>,
    /// The tag column names, indexed by [`OutputColumn::Tag`].
    tags: Arc<[String]>,
    /// The schema of every batch this stream yields.
    output_schema: SchemaRef,
    /// The label-set occupancy shared with the other executions of this plan.
    labels: Arc<Mutex<LabelSets>>,
    /// What the frame batch in hand keeps charged on the task's memory pool.
    frames: MemoryReservation,
    /// Whether the input ended, so what is left is the selection's own answer.
    draining: bool,
    /// Whether this stream ended.
    done: bool,
}

impl VectorSelectStream {
    /// Takes the frame batch, charging the pool for the batch as a whole.
    fn hold(&mut self, batch: RecordBatch) -> DataFusionResult<()> {
        self.frames.try_resize(batch.get_array_memory_size())?;
        self.batch = Some(batch);
        self.row = 0;
        Ok(())
    }

    /// Gives the frame batch back, returning what it held to the pool.
    fn release_batch(&mut self) {
        self.batch = None;
        self.row = 0;
        let _ = self.frames.try_resize(0);
    }

    /// Reads row `row` of the held batch and hands its chunk to the suffix.
    ///
    /// The sequence is checked before the chunk is pushed, so a producer that starts before the
    /// previous one closed, or an ordinal out of order, is refused rather than read as a
    /// continuation. Nothing may come back before the input ends: a chunk here would be an answer
    /// computed from part of the vector.
    fn feed(&mut self, row: usize) -> DataFusionResult<()> {
        let Some(batch) = self.batch.as_ref() else {
            return Err(DataFusionError::Execution(
                "VectorSelectExec read a row no batch holds".to_string(),
            ));
        };
        let Some(schema) = self.series_schema.as_ref() else {
            return Err(DataFusionError::Execution(
                "VectorSelectExec read a frame after it gave up its series schema".to_string(),
            ));
        };
        let (producer, ordinal, chunk) = evaluated_frame::read(batch, row, schema)?;
        self.sequence.check(producer, ordinal, chunk.is_last)?;
        let Some(executor) = self.executor.as_mut() else {
            return Err(DataFusionError::Execution(
                "VectorSelectExec read a frame after its execution closed".to_string(),
            ));
        };
        executor.push_evaluated(self.input_id, chunk)?;
        if executor.next_chunk()?.is_some() {
            return Err(DataFusionError::Execution(
                "the native selection produced a chunk before its input ended, so it answered from \
                 part of the vector"
                    .to_string(),
            ));
        }
        Ok(())
    }

    /// Ends the frame input and every series in it, so the selection settles.
    ///
    /// The sequence ends first: an unclosed producer is a frame-stream defect, which ending the
    /// runtime would report only as a series left open.
    fn finish_input(&mut self) -> DataFusionResult<()> {
        self.sequence.finish()?;
        match self.executor.as_mut() {
            Some(executor) => executor.end_of_input(),
            None => Ok(()),
        }
    }

    /// Pulls one series the selection kept and converts it, or `None` when there is no more.
    ///
    /// One chunk per call is what keeps this a stream, and `None` is also when the execution closes
    /// and returns its reservations, so it is dropped here rather than held until the stream is.
    fn pull_one(&mut self) -> DataFusionResult<Option<RecordBatch>> {
        let chunk = match self.executor.as_mut() {
            Some(executor) => executor.next_chunk()?,
            None => return Ok(None),
        };
        let Some(chunk) = chunk else {
            self.executor = None;
            return Ok(None);
        };
        // The frames below claimed what they held before the selection chose among them; the
        // answer is claimed here as well, so two kept series on one label set are still refused.
        self.labels
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .claim(&chunk.series)?;
        let batch = series_to_record_batch(
            &self.classification,
            &self.tags,
            &self.output_schema,
            &chunk,
        )?;
        Ok(Some(batch))
    }

    /// Ends the stream in an error, releasing the execution, the held frame and the input at once.
    ///
    /// The label-set map is the plan's, so what it holds stays charged until the plan itself goes.
    fn fail(&mut self) {
        self.done = true;
        self.executor = None;
        self.input = None;
        self.series_schema = None;
        self.release_batch();
    }
}

impl RecordBatchStream for VectorSelectStream {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

impl Stream for VectorSelectStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<DataFusionResult<RecordBatch>>> {
        let this = &mut *self;
        for _ in 0..POLL_BUDGET {
            if this.done {
                // The grid the schema names is the last thing this stream could hold after it
                // ended, so it is given up here as well as on an error.
                this.series_schema = None;
                return Poll::Ready(None);
            }
            // The input ended, so what is left is the selection's answer, one series per pull.
            if this.draining {
                match this.pull_one() {
                    Ok(Some(batch)) => return Poll::Ready(Some(Ok(batch))),
                    Ok(None) => {
                        this.done = true;
                        this.series_schema = None;
                        continue;
                    }
                    Err(error) => {
                        this.fail();
                        return Poll::Ready(Some(Err(error)));
                    }
                }
            }
            // The held batch is read a row at a time and released with its last row, so one poll
            // never pays for two batches.
            if this.batch.is_some() {
                let rows = this.batch.as_ref().map_or(0, RecordBatch::num_rows);
                if this.row < rows {
                    let row = this.row;
                    this.row += 1;
                    if let Err(error) = this.feed(row) {
                        this.fail();
                        return Poll::Ready(Some(Err(error)));
                    }
                    continue;
                }
                this.release_batch();
                continue;
            }
            let Some(input) = this.input.as_mut() else {
                this.done = true;
                this.series_schema = None;
                continue;
            };
            // `as_mut` is what turns the pinned box into the pinned stream a poll takes.
            let polled = input.as_mut().poll_next(cx);
            match polled {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    this.input = None;
                    if let Err(error) = this.finish_input() {
                        this.fail();
                        return Poll::Ready(Some(Err(error)));
                    }
                    this.draining = true;
                }
                Poll::Ready(Some(Err(error))) => {
                    // A failed input leaves a selection computed from part of the vector, so no
                    // answer may be handed out.
                    this.fail();
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Ready(Some(Ok(batch))) => {
                    if let Err(error) = this.hold(batch) {
                        this.fail();
                        return Poll::Ready(Some(Err(error)));
                    }
                }
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}
