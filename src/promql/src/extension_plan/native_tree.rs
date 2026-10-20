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

//! A DataFusion plan that drives one [`NativeTree`] over a prepared raw-series frame.
//!
//! The plan is the caller of [`NativeExecutor`]: every partition builds its own
//! [`NativeExecutionState`] from the child's pool, feeds it the raw chunks the child's frame
//! decodes into, and converts the evaluated chunks the root hands back into record batches. The
//! tree is compiled before this plan exists and never changes, so nothing here interprets a
//! function name, resolves a field or picks a column.
//!
//! Which column of this plan's output reads which part of the root output is decided once, in
//! [`NativeTreeExec::try_new`], and every batch is assembled from that binding. The time column
//! name, the tag set and the sample types are caller parameters: the tree carries the series a node
//! computes, not the table a query was written against, so it has none of them.
//!
//! # Not covered here
//!
//! A tree is fed by exactly one raw-frame child, and a raw frame is a per-series stream, so a
//! series must arrive whole in one partition. Detecting a label set that recurs across partitions
//! needs an owner above the partitions (the plan-level `Emitted` of the aggregate stage), which
//! this plan does not have: a caller must keep every series of one label set in one partition.
//!
//! The sample types are likewise part of the caller's declaration, not something this plan can
//! prove: a raw frame declares both a float and a histogram carrier for every field, so a tree
//! whose output turns out to hold native histograms is refused when such a sample is converted
//! rather than before the child runs.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use common_query::series_frame::{raw_series_frame_ordering, unpack_raw_series};
use datafusion::arrow::array::{ArrayRef, Float64Array, StringArray, TimestampMillisecondArray};
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PhysicalExpr, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream,
};
use datatypes::series::samples::{SampleRef, Series, SeriesSchema};
use datatypes::series::{MetricName, SeriesOrdering};
use futures::{Stream, ready};
use store_api::region_engine::{RawLabelSource, SendableRawSeriesStream};

use crate::evaluated::EvaluatedChunk;
use crate::native::execute::{NativeExecutionState, NativeExecutor};
use crate::native::metrics::walk;
use crate::native::plan::{NativeExpr, NativeTree};
use crate::source::materialize_raw_labels;

#[cfg(test)]
#[path = "native_tree/tests.rs"]
mod tests;

/// How many push or pull steps one poll performs before it wakes itself.
///
/// A step hands one raw chunk to the tree, asks the root for one chunk, or closes the tree. The
/// budget keeps one poll bounded even when the child's frames hold many small chunks that settle
/// nothing, and it is what stops a poll from reading the child to its end before answering.
const POLL_BUDGET: usize = 64;

/// One column of the output, and where in a root chunk it reads from.
#[derive(Clone, Debug)]
enum OutputColumn {
    /// The timestamp the chunk's own axis carries.
    Time,
    /// Field `index` of the chunk's series, by its position in the root schema.
    Field { index: usize },
    /// One label of the series' final metric.
    Tag {
        /// The label name.
        name: Arc<str>,
        /// Whether a series without this label reports NULL rather than the empty string.
        nullable: bool,
    },
}

impl OutputColumn {
    /// What this column is, for a rejection that has to say which column it reports on.
    fn describe(&self) -> &'static str {
        match self {
            Self::Time => "the time column",
            Self::Field { .. } => "a field",
            Self::Tag { .. } => "a tag",
        }
    }
}

/// Which part of a root chunk every output column reads.
///
/// The binding is built once, from the root output's field list and the caller's time column and
/// tag set, and then only read: converting a chunk is a walk over `columns`.
#[derive(Debug)]
struct OutputBinding {
    /// The batch schema this plan produces.
    schema: SchemaRef,
    /// One entry per column of `schema`, in its order.
    columns: Vec<OutputColumn>,
}

impl OutputBinding {
    /// Binds every column of `schema` to the role it reads from `root`.
    ///
    /// A column that is not the time column, one of `tags` or a field of `root` is an error rather
    /// than a column filled by guesswork, and so is a name two roles claim, a field of `root` that
    /// `schema` does not carry, a name `schema` declares twice, and a column whose declared type
    /// the role cannot fill.
    fn try_new(
        root: &SeriesSchema,
        schema: SchemaRef,
        time_column: &str,
        tags: &[String],
    ) -> DfResult<Self> {
        let names = schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        for (index, name) in names.iter().enumerate() {
            if names[..index].contains(name) {
                return Err(plan_error(format!(
                    "the output schema declares column `{name}` twice"
                )));
            }
        }
        let mut columns: Vec<Option<OutputColumn>> = vec![None; names.len()];
        let mut claim = |role: OutputColumn, name: &str| -> DfResult<()> {
            let index = names
                .iter()
                .position(|column| *column == name)
                .ok_or_else(|| {
                    plan_error(format!(
                        "the output schema has no column `{name}` for {}",
                        role.describe()
                    ))
                })?;
            let claimed = role.describe();
            match columns[index].replace(role) {
                None => Ok(()),
                Some(previous) => Err(plan_error(format!(
                    "output column `{name}` is claimed as both {} and {claimed}",
                    previous.describe()
                ))),
            }
        };
        claim(OutputColumn::Time, time_column)?;
        for tag in tags {
            // A tag column that is not nullable reports the empty string for a series without the
            // label, which is a declaration this plan honours; the schema says which it is.
            let nullable = schema
                .field_with_name(tag)
                .map(|field| field.is_nullable())
                .unwrap_or(false);
            claim(
                OutputColumn::Tag {
                    name: Arc::from(tag.as_str()),
                    nullable,
                },
                tag,
            )?;
        }
        for (index, field) in root.fields.iter().enumerate() {
            claim(OutputColumn::Field { index }, &field.name)?;
        }
        let columns = columns
            .into_iter()
            .enumerate()
            .map(|(index, column)| {
                column.ok_or_else(|| {
                    plan_error(format!(
                        "output column `{}` is not the time column, one of the tags, or a field of \
                         the native tree's root output",
                        names[index]
                    ))
                })
            })
            .collect::<DfResult<Vec<_>>>()?;
        for (field, column) in schema.fields().iter().zip(columns.iter()) {
            check_column_type(field.data_type(), field.is_nullable(), column)?;
        }
        Ok(Self { schema, columns })
    }

    /// Converts one evaluated chunk into a batch, or `None` when it carries no row.
    ///
    /// The rows are the positions the chunk's axis actually holds, so a sparse series keeps its
    /// runs and its gaps instead of being filled out to its grid. A field that holds no sample at
    /// a row reports NULL there while the row itself stays, and a sample that is a native
    /// histogram is an error: it is not a float, and neither NULL nor a dropped row would say so.
    fn convert(&self, chunk: EvaluatedChunk) -> DfResult<Option<RecordBatch>> {
        let rows = chunk.series.time.len();
        if rows == 0 {
            return Ok(None);
        }
        let mut arrays = Vec::with_capacity(self.columns.len());
        for (field, column) in self.schema.fields().iter().zip(self.columns.iter()) {
            arrays.push(match column {
                OutputColumn::Time => Arc::new(timestamps(&chunk.series, rows)?) as ArrayRef,
                OutputColumn::Field { index } => Arc::new(field_column(
                    &chunk.series,
                    *index,
                    rows,
                    field.name().as_str(),
                )?),
                OutputColumn::Tag { name, nullable } => {
                    Arc::new(tag_column(&chunk.series.metric, name, *nullable, rows))
                }
            });
        }
        Ok(Some(RecordBatch::try_new(self.schema.clone(), arrays)?))
    }
}

/// Checks that a column declared as `data_type` can hold what `column` reads.
fn check_column_type(data_type: &DataType, nullable: bool, column: &OutputColumn) -> DfResult<()> {
    let (expected, covered) = match column {
        OutputColumn::Time => (
            "a Timestamp(Millisecond, None)",
            matches!(data_type, DataType::Timestamp(TimeUnit::Millisecond, None)),
        ),
        // A position a field holds no sample at is a NULL in the batch, so a field column that
        // cannot carry one would only fail once a series was missing a value.
        OutputColumn::Field { .. } => (
            "a nullable Float64",
            matches!(data_type, DataType::Float64) && nullable,
        ),
        OutputColumn::Tag { .. } => ("a Utf8", matches!(data_type, DataType::Utf8)),
    };
    if covered {
        Ok(())
    } else {
        Err(plan_error(format!(
            "{} must be declared as {expected} (nullable: {nullable}), not {data_type}",
            column.describe()
        )))
    }
}

/// The timestamps `series` carries, one per row.
fn timestamps(series: &Series, rows: usize) -> DfResult<TimestampMillisecondArray> {
    let mut values = Vec::with_capacity(rows);
    for row in 0..rows {
        let timestamp = series.time.timestamp(row).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "the evaluated chunk carries no timestamp at row {row}"
            ))
        })?;
        values.push(timestamp);
    }
    Ok(TimestampMillisecondArray::from(values))
}

/// Field `index` of `series` as the column named `name`, one row per carried position.
fn field_column(series: &Series, index: usize, rows: usize, name: &str) -> DfResult<Float64Array> {
    let field = series.fields.get(index).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "column `{name}` reads field {index} of the root output, which the evaluated chunk does \
             not carry"
        ))
    })?;
    if field.len() != rows {
        return Err(DataFusionError::Execution(format!(
            "field {index} of column `{name}` holds {} sample(s) but the chunk's axis holds {rows}",
            field.len()
        )));
    }
    let mut values = Vec::with_capacity(rows);
    for sample in field.iter() {
        match sample {
            SampleRef::Missing => values.push(None),
            // An ordinary NaN is a value rather than a missing sample: it stays a value here.
            SampleRef::Float(value) => values.push(Some(value)),
            SampleRef::Histogram(_) => {
                return Err(DataFusionError::Execution(format!(
                    "column `{name}` reads field {index} of the root output as Float64, but the \
                     evaluated chunk holds a native histogram there"
                )));
            }
        }
    }
    Ok(Float64Array::from(values))
}

/// Label `name` of `metric`, repeated for every row.
fn tag_column(metric: &MetricName, name: &str, nullable: bool, rows: usize) -> StringArray {
    let value = match metric.get(name) {
        Some(value) => Some(value),
        // A nullable column separates "this series has no such label" from an empty value; a
        // non-nullable one cannot, and reports the empty string it declared.
        None if nullable => None,
        None => Some(""),
    };
    StringArray::from(vec![value; rows])
}

/// The raw field layout of the tree's one input node.
fn raw_source(tree: &NativeTree) -> DfResult<Arc<SeriesSchema>> {
    let mut sources = Vec::new();
    let mut unsupported = None;
    walk(tree.root(), &mut |node, path| match &node.expr {
        NativeExpr::Input(_) => sources.push((node.output.samples().cloned(), path.clone())),
        NativeExpr::Instant { .. } | NativeExpr::Window { .. } | NativeExpr::Map { .. } => {}
        // The kinds below are legal in a bound tree and are executed elsewhere; this plan is not
        // the runtime for them, so it names the one it met instead of running a partial tree.
        NativeExpr::Scalar(_) => {
            unsupported.get_or_insert(("a scalar", path.clone()));
        }
        NativeExpr::Aggregate { .. } => {
            unsupported.get_or_insert(("an aggregate", path.clone()));
        }
        NativeExpr::Binary { .. } => {
            unsupported.get_or_insert(("a binary operation", path.clone()));
        }
    });
    if let Some((kind, path)) = unsupported {
        return Err(plan_error(format!(
            "the native tree node at `{path}` is {kind}, which this plan does not execute: it runs \
             Input, Instant, Window and Map nodes only"
        )));
    }
    let [(source, path)] = sources.as_slice() else {
        return Err(plan_error(format!(
            "the native tree has {} raw input node(s), but a raw frame feeds exactly one",
            sources.len()
        )));
    };
    let schema = source.as_ref().ok_or_else(|| {
        plan_error(format!(
            "the native tree's input node at `{path}` does not produce samples"
        ))
    })?;
    if schema.eval_grid.is_some() {
        return Err(plan_error(format!(
            "the native tree's input node at `{path}` consumes evaluated chunks, but this plan \
             feeds raw series frames"
        )));
    }
    Ok(schema.clone())
}

/// The root output of `tree`, which is what every column of this plan reads.
fn root_output(tree: &NativeTree) -> DfResult<Arc<SeriesSchema>> {
    match tree.root().output.samples() {
        Some(schema) if schema.eval_grid.is_some() => Ok(schema.clone()),
        Some(_) => Err(plan_error(
            "the native tree's root produces raw samples, but this plan converts evaluated chunks",
        )),
        None => Err(plan_error(
            "the native tree's root produces a scalar or aggregate state, which is not a query \
             value this plan can convert",
        )),
    }
}

/// A DataFusion plan that executes one [`NativeTree`] over a raw-series frame.
///
/// The tree, the label source and the output binding are the same for every partition; only the
/// run-time state is per execution, and it is built when a partition is executed, from that
/// execution's pool. The plan registers no metrics of its own: it exposes the per-node metrics
/// [`NativeExecutionState::try_new`] registers, as the nodes report them.
pub struct NativeTreeExec {
    /// The prepared raw-series frame this tree reads, one chunk stream per partition.
    input: Arc<dyn ExecutionPlan>,
    /// The bound tree, shared by every partition and never mutated.
    tree: Arc<NativeTree>,
    /// Where the labels of every series come from.
    label_source: RawLabelSource,
    /// The ordering the child's frames declare.
    ordering: SeriesOrdering,
    /// The raw field layout the tree's input node consumes.
    series_schema: Arc<SeriesSchema>,
    /// Which part of a root chunk every output column reads.
    binding: Arc<OutputBinding>,
    /// The per-node metrics of every execution of this plan.
    metrics: ExecutionPlanMetricsSet,
    properties: Arc<PlanProperties>,
}

impl NativeTreeExec {
    /// Builds a plan that drives `tree` over the raw-series frame `input` produces.
    ///
    /// Every check that can be made without running the child is made here: the tree must have
    /// exactly one raw input and only the node kinds this plan executes, its root must produce an
    /// evaluated vector, and every column of `output_schema` must be `time_column`, one of `tags`
    /// or one of the root's fields. `tags` names the label set the caller wrote its query against;
    /// it is not read from the tree, which carries no tag list.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        tree: Arc<NativeTree>,
        label_source: RawLabelSource,
        output_schema: SchemaRef,
        time_column: String,
        tags: Vec<String>,
    ) -> DfResult<Self> {
        let series_schema = raw_source(&tree)?;
        let root = root_output(&tree)?;
        let binding = Arc::new(OutputBinding::try_new(
            &root,
            output_schema,
            &time_column,
            &tags,
        )?);
        Self::assemble(
            input,
            tree,
            label_source,
            series_schema,
            binding,
            ExecutionPlanMetricsSet::new(),
        )
    }

    /// Assembles a plan from bindings that were already validated, deriving what the child
    /// declares.
    ///
    /// The declared frame is validated against the tree's raw layout without executing the child:
    /// `unpack_raw_series` decodes an empty batch of the declared schema, which is where a frame
    /// that does not describe this tree's series is rejected.
    fn assemble(
        input: Arc<dyn ExecutionPlan>,
        tree: Arc<NativeTree>,
        label_source: RawLabelSource,
        series_schema: Arc<SeriesSchema>,
        binding: Arc<OutputBinding>,
        metrics: ExecutionPlanMetricsSet,
    ) -> DfResult<Self> {
        let declared = input.schema();
        let ordering = raw_series_frame_ordering(declared.as_ref())?;
        unpack_raw_series(
            Box::pin(RecordBatchStreamAdapter::new(
                declared.clone(),
                futures::stream::empty(),
            )),
            series_schema.clone(),
            label_source.clone(),
            ordering,
        )?;
        let properties = Arc::new(PlanProperties::new(
            // A root chunk's ordering is not the child's: a window or a selection stamps its own
            // grid onto what it produces, so nothing is carried over from the child.
            EquivalenceProperties::new(binding.schema.clone()),
            // Every partition of the child is kept, and this plan says nothing about how they
            // relate: which series a partition holds is the caller's arrangement. A label set that
            // recurs across two of them is not detected here -- see the module documentation -- so
            // a caller keeps every series of one label set in one partition.
            Partitioning::UnknownPartitioning(input.properties().partitioning.partition_count()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            tree,
            label_source,
            ordering,
            series_schema,
            binding,
            metrics,
            properties,
        })
    }
}

impl fmt::Debug for NativeTreeExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeTreeExec")
            .field("columns", &self.binding.columns)
            .field("label_source", &self.label_source)
            .finish()
    }
}

impl DisplayAs for NativeTreeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "NativeTreeExec: partitions={}, columns={}",
            self.properties.partitioning.partition_count(),
            self.binding.schema.fields().len()
        )
    }
}

impl ExecutionPlan for NativeTreeExec {
    fn name(&self) -> &str {
        "NativeTreeExec"
    }

    fn schema(&self) -> SchemaRef {
        self.binding.schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    /// The metrics of every node of every partition, as `NativeExecutionState::try_new` registered
    /// them. This plan registers nothing itself, so it only hands the set over.
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DfResult<TreeNodeRecursion>,
    ) -> DfResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let mut children = children.into_iter();
        let (Some(input), None) = (children.next(), children.next()) else {
            return Err(plan_error(
                "NativeTreeExec requires exactly one raw-frame child",
            ));
        };
        // Only the child changes: the tree, the label source and the output binding stay, and the
        // partition count follows the new child. No run-time state is carried over, because none
        // of it outlives one execution.
        Ok(Arc::new(Self::assemble(
            input,
            self.tree.clone(),
            self.label_source.clone(),
            self.series_schema.clone(),
            self.binding.clone(),
            self.metrics.clone(),
        )?))
    }

    fn reset_state(self: Arc<Self>) -> DfResult<Arc<dyn ExecutionPlan>> {
        // A reset execution starts from an empty metrics set and from the pool chosen when it is
        // executed: no pool is cached here, and the child is not reset through this plan.
        Ok(Arc::new(Self::assemble(
            self.input.clone(),
            self.tree.clone(),
            self.label_source.clone(),
            self.series_schema.clone(),
            self.binding.clone(),
            ExecutionPlanMetricsSet::new(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let partitions = self.properties.partitioning.partition_count();
        if partition >= partitions {
            return Err(DataFusionError::Execution(format!(
                "the native tree plan has {partitions} partition(s), so partition {partition} has \
                 no work"
            )));
        }
        let frame = self.input.execute(partition, Arc::clone(&context))?;
        let state = NativeExecutionState::try_new(
            &self.tree,
            context.runtime_env().memory_pool.clone(),
            &self.metrics,
            partition,
        )?;
        let source = unpack_raw_series(
            frame,
            self.series_schema.clone(),
            self.label_source.clone(),
            self.ordering,
        )?;
        let source = materialize_raw_labels(source)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        Ok(Box::pin(NativeTreeStream {
            source: Some(source),
            executor: Some(NativeExecutor::new(state)),
            binding: self.binding.clone(),
            closed: false,
        }))
    }
}

/// One partition's execution of the tree.
struct NativeTreeStream {
    /// The raw chunks this execution feeds the tree, until the child ends.
    source: Option<SendableRawSeriesStream>,
    /// The driver of this execution's tree, until it ends or fails.
    executor: Option<NativeExecutor>,
    /// Which part of a root chunk every output column reads.
    binding: Arc<OutputBinding>,
    /// Whether the tree is closed, so nothing can settle any more.
    closed: bool,
}

impl NativeTreeStream {
    /// Ends this execution, returning `error` once and nothing after it.
    ///
    /// The driver is dropped rather than stopped: dropping it returns every byte it reserved to
    /// the pool, and an execution that failed must not be resumed.
    fn fail(&mut self, error: DataFusionError) -> Poll<Option<DfResult<RecordBatch>>> {
        self.source = None;
        self.executor = None;
        self.closed = true;
        Poll::Ready(Some(Err(error)))
    }

    /// The next raw chunk of the child, or `None` once it ended.
    fn poll_source(&mut self, cx: &mut Context<'_>) -> Poll<Option<DfResult<()>>> {
        let Some(source) = self.source.as_mut() else {
            return Poll::Ready(None);
        };
        let chunk = match ready!(source.as_mut().poll_next(cx)) {
            Some(Ok(chunk)) => chunk,
            Some(Err(error)) => {
                return Poll::Ready(Some(Err(DataFusionError::External(Box::new(error)))));
            }
            None => return Poll::Ready(None),
        };
        // A chunk that carries a series' last flag is pushed like any other one: the flag is what
        // lets the tree decide a series is over, so it is never dropped for carrying no row.
        match self.executor.as_mut() {
            Some(executor) => Poll::Ready(Some(executor.push_raw(chunk))),
            None => Poll::Ready(None),
        }
    }

    /// Asks the root for the next chunk it settled.
    fn next_from_tree(&mut self) -> DfResult<Option<RecordBatch>> {
        let next = match self.executor.as_mut() {
            Some(executor) => executor.next_chunk(),
            None => return Ok(None),
        };
        match next? {
            Some(chunk) => self.binding.convert(chunk),
            // Nothing settled at this step; the tree either needs more input or has no chunk to
            // report yet, which is not the same as having no input left.
            None => Ok(None),
        }
    }
}

impl Stream for NativeTreeStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        for _ in 0..POLL_BUDGET {
            // A closed tree settles nothing, so there is nothing left to hand over.
            if self.closed {
                return Poll::Ready(None);
            }
            // What the tree already settled is handed over first, so a chunk that is ready now
            // never waits for another raw chunk to arrive.
            match self.next_from_tree() {
                Ok(Some(batch)) => return Poll::Ready(Some(Ok(batch))),
                // A chunk that carries no row is a lifecycle signal rather than a row: it is
                // dropped here, and no row at this step is not the same as no input left.
                Ok(None) => {}
                Err(error) => return self.fail(error),
            }
            if self.source.is_none() {
                // The child ended: closing the tree once is what reports a series that never
                // closed, and a closed tree settles nothing more, so nothing is kept from it.
                let closed = match self.executor.as_mut() {
                    Some(executor) => executor.finish(),
                    None => return Poll::Ready(None),
                };
                match closed {
                    // `finish` drains an executor whose last pull already reported no chunk, so
                    // nothing can be left to collect here. The assertion states the proof instead
                    // of leaving a reader to guess what is dropped; a series that never closed is
                    // reported by `close` as an error, not returned as a chunk.
                    Ok(chunks) => {
                        debug_assert!(
                            chunks.is_empty(),
                            "closing a drained executor yielded chunks"
                        );
                        self.closed = true;
                    }
                    Err(error) => return self.fail(error),
                }
                continue;
            }
            match self.poll_source(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Err(error))) => return self.fail(error),
                Poll::Ready(Some(Ok(()))) => {}
                // The child's stream ended here rather than at a chunk boundary: the next step
                // closes the tree.
                Poll::Ready(None) => self.source = None,
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

impl RecordBatchStream for NativeTreeStream {
    fn schema(&self) -> SchemaRef {
        self.binding.schema.clone()
    }
}

/// A rejected construction, which is a caller mistake rather than a query result.
fn plan_error(reason: impl Into<String>) -> DataFusionError {
    DataFusionError::Plan(reason.into())
}
