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

//! What the shell has to get right: the tree it drives, the columns it binds, the loop it runs,
//! and the states it refuses.

use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use api::v1::SemanticType;
use common_query::series_frame::raw_series_frame_schema_with_ordering;
use datafusion::arrow::array::{Float64Array, StringArray, UInt32Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricValue};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PhysicalExpr, PlanProperties,
    SendableRecordBatchStream,
};
use datatypes::arrow::buffer::{Buffer, ScalarBuffer};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use datatypes::series::frame::{RawLabelMode, RawSeriesChunkCursor, RawSeriesFrameBuilder};
use datatypes::series::histogram::{
    BucketSideView, CounterResetHint, HistogramBatchBuilder, HistogramView,
};
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{FieldSpec, Samples, SeriesFields, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridSpec};
use datatypes::series::{Label, MetricName, SeriesOrdering};
use futures::{Stream, StreamExt};
use mito_codec::row_converter::SortField;
use mito_codec::row_converter::sparse::SparsePrimaryKeyCodec;
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataBuilder};
use store_api::region_engine::{RawLabelSource, RawSeriesStream};
use store_api::storage::RegionId;

use super::*;
use crate::map::MathOp;
use crate::native::metrics::{NodeMetric, NodePath};
use crate::native::plan::{
    BoundInput, BoundInstant, BoundMap, BoundScalar, BoundWindow, NativeNode, NativeType,
};
use crate::range_eval::{RangeFunction, RangeWindow};

// ---------------------------------------------------------------------------------------------
// Trees
// ---------------------------------------------------------------------------------------------

fn grid(start: i64, step: i64, len: u64) -> Arc<EvalGrid> {
    Arc::new(EvalGrid::try_new(GridSpec::try_new(start, step, len).unwrap()).unwrap())
}

fn raw_schema(names: &[&str]) -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: names
            .iter()
            .map(|name| FieldSpec {
                name: Arc::from(*name),
            })
            .collect(),
        eval_grid: None,
    })
}

fn evaluated_schema(fields: &Arc<SeriesSchema>, eval_grid: Arc<EvalGrid>) -> NativeType {
    NativeType::Samples(Arc::new(SeriesSchema {
        fields: Arc::clone(&fields.fields),
        eval_grid: Some(eval_grid),
    }))
}

fn input_node(raw: Arc<SeriesSchema>) -> NativeNode {
    let output = NativeType::Samples(raw);
    NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: output.clone(),
            ordering: SeriesOrdering::KeyThenTime,
        }),
        output,
    }
}

/// An input node whose chunks are already evaluated, which a raw frame cannot feed.
fn evaluated_input_node(eval_grid: Arc<EvalGrid>) -> NativeNode {
    let output = NativeType::Samples(Arc::new(SeriesSchema {
        fields: Arc::from([FieldSpec {
            name: Arc::from("v0"),
        }]),
        eval_grid: Some(eval_grid),
    }));
    NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: output.clone(),
            ordering: SeriesOrdering::KeyThenTime,
        }),
        output,
    }
}

fn instant(inner: NativeNode, lookback_ms: i64, output_grid: Arc<EvalGrid>) -> NativeNode {
    let raw = inner.output.samples().expect("a selection reads samples");
    let output = evaluated_schema(raw, output_grid.clone());
    NativeNode {
        expr: NativeExpr::Instant {
            input: Box::new(inner),
            op: BoundInstant {
                lookback_ms,
                offset_ms: 0,
                output_grid,
            },
        },
        output,
    }
}

fn window(
    inner: NativeNode,
    function: RangeFunction,
    range_ms: i64,
    output_grid: Arc<EvalGrid>,
) -> NativeNode {
    let raw = inner.output.samples().expect("a window reads samples");
    let output = evaluated_schema(raw, output_grid.clone());
    NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(inner),
            op: BoundWindow {
                function,
                window: RangeWindow {
                    range_ms,
                    offset_ms: 0,
                    anchor_ms: None,
                },
                input_grid: None,
                output_grid,
            },
        },
        output,
    }
}

fn map(inner: NativeNode, ops: Vec<MathOp>) -> NativeNode {
    let eval_grid = inner.output.grid().expect("a map needs a grid").clone();
    let raw = inner.output.samples().expect("a map reads samples");
    let output = evaluated_schema(raw, eval_grid);
    NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(inner),
            op: BoundMap { ops: ops.into() },
        },
        output,
    }
}

// ---------------------------------------------------------------------------------------------
// Series and chunks
// ---------------------------------------------------------------------------------------------

fn metric(tags: &[(&str, &str)]) -> MetricName {
    MetricName::try_new(
        Some("requests".into()),
        tags.iter()
            .map(|(name, value)| Label {
                name: Arc::from(*name),
                value: Arc::from(*value),
            })
            .collect(),
    )
    .unwrap()
}

fn timestamps(values: &[i64]) -> RawTimestamps {
    RawTimestamps::try_new(ScalarBuffer::from(values.to_vec())).unwrap()
}

/// One chunk of the series `key` carrying `columns`, one column per field.
fn chunk(
    schema: &SeriesSchema,
    key: &[u8],
    metric: Option<MetricName>,
    time: &[i64],
    columns: Vec<Vec<Option<f64>>>,
    is_last: bool,
) -> RawSeriesChunk {
    let fields = SeriesFields::try_from_vec(
        columns
            .into_iter()
            .map(|values| Samples::from_float(Float64Array::from(values)))
            .collect(),
    )
    .unwrap();
    RawSeriesChunk::try_new(
        schema,
        Buffer::from_slice_ref(key),
        metric,
        timestamps(time),
        fields,
        is_last,
    )
    .unwrap()
}

/// One one-field chunk of `key`.
fn one_field(
    schema: &SeriesSchema,
    key: &[u8],
    metric: Option<MetricName>,
    time: &[i64],
    values: Vec<Option<f64>>,
    is_last: bool,
) -> RawSeriesChunk {
    chunk(schema, key, metric, time, vec![values], is_last)
}

/// One chunk whose one field holds a two-bucket native histogram at each timestamp.
fn histogram_chunk(schema: &SeriesSchema, key: &[u8], time: &[i64]) -> RawSeriesChunk {
    let mut builder = HistogramBatchBuilder::new();
    let offsets = [0];
    let lengths = [2];
    let counts = vec![1.0; 2];
    let indices: UInt32Array = time
        .iter()
        .map(|_| {
            Some(
                builder
                    .push(HistogramView {
                        schema: 0,
                        zero_threshold: 0.0,
                        zero_count: 0.0,
                        count: 2.0,
                        sum: 2.0,
                        reset_hint: CounterResetHint::Unknown,
                        start_timestamp: None,
                        custom_values: &[],
                        positive: BucketSideView {
                            span_offsets: &offsets,
                            span_lengths: &lengths,
                            buckets: &counts,
                        },
                        negative: BucketSideView {
                            span_offsets: &[],
                            span_lengths: &[],
                            buckets: &[],
                        },
                    })
                    .unwrap(),
            )
        })
        .collect();
    let samples = Samples::try_from_histogram(indices, builder.finish().unwrap()).unwrap();
    RawSeriesChunk::try_new(
        schema,
        Buffer::from_slice_ref(key),
        Some(metric(&[])),
        timestamps(time),
        SeriesFields::one(samples),
        true,
    )
    .unwrap()
}

// ---------------------------------------------------------------------------------------------
// A raw-frame child with a controllable number of partitions
// ---------------------------------------------------------------------------------------------

/// A child that publishes the frame batches a test hands it, and counts how they were read.
struct FrameChild {
    schema: SchemaRef,
    partitions: Vec<Vec<RecordBatch>>,
    /// Whether the stream fails once its batches are exhausted, which is what an upstream failure
    /// looks like from here.
    fails: bool,
    /// Whether the stream reports `Pending` once it has handed over its first batch, which is what
    /// a source that is still arriving looks like from here.
    stalls: bool,
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    properties: Arc<PlanProperties>,
}

impl FrameChild {
    fn new(schema: SchemaRef, partitions: Vec<Vec<RecordBatch>>) -> Arc<Self> {
        Self::build(schema, partitions, false, false)
    }

    /// A child whose stream reports a failure once it has published `partitions`.
    fn failing(schema: SchemaRef, partitions: Vec<Vec<RecordBatch>>) -> Arc<Self> {
        Self::build(schema, partitions, true, false)
    }

    /// A child whose stream stalls once after its first batch, so a series stays open.
    fn stalling(schema: SchemaRef, partitions: Vec<Vec<RecordBatch>>) -> Arc<Self> {
        Self::build(schema, partitions, false, true)
    }

    fn build(
        schema: SchemaRef,
        partitions: Vec<Vec<RecordBatch>>,
        fails: bool,
        stalls: bool,
    ) -> Arc<Self> {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(partitions.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Arc::new(Self {
            schema,
            partitions,
            fails,
            stalls,
            polls: Arc::new(AtomicUsize::new(0)),
            drops: Arc::new(AtomicUsize::new(0)),
            properties,
        })
    }

    /// How many batches the child's streams handed out, poll by poll.
    fn polls(&self) -> usize {
        self.polls.load(Ordering::SeqCst)
    }

    fn drops(&self) -> usize {
        self.drops.load(Ordering::SeqCst)
    }
}

impl fmt::Debug for FrameChild {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FrameChild").finish()
    }
}

impl DisplayAs for FrameChild {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FrameChild")
    }
}

struct FrameStream {
    schema: SchemaRef,
    batches: VecDeque<RecordBatch>,
    /// Whether one failure follows the batches, and whether it was reported already.
    fails: bool,
    failed: bool,
    /// Whether one `Pending` follows the first batch, and whether it was reported already.
    stalls: bool,
    stalled: bool,
    yielded: usize,
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

impl Stream for FrameStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        if self.stalls && !self.stalled && self.yielded > 0 {
            self.stalled = true;
            return Poll::Pending;
        }
        if let Some(batch) = self.batches.pop_front() {
            self.yielded += 1;
            return Poll::Ready(Some(Ok(batch)));
        }
        if self.fails && !self.failed {
            self.failed = true;
            return Poll::Ready(Some(Err(DataFusionError::Execution(
                "the frame child failed".to_string(),
            ))));
        }
        Poll::Ready(None)
    }
}

impl Drop for FrameStream {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl datafusion::physical_plan::RecordBatchStream for FrameStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl ExecutionPlan for FrameChild {
    fn name(&self) -> &str {
        "FrameChild"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DfResult<TreeNodeRecursion>,
    ) -> DfResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        assert!(children.is_empty(), "the frame child has no children");
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        Ok(Box::pin(FrameStream {
            schema: self.schema.clone(),
            batches: self.partitions[partition].clone().into(),
            fails: self.fails,
            failed: false,
            stalls: self.stalls,
            stalled: false,
            yielded: 0,
            polls: self.polls.clone(),
            drops: self.drops.clone(),
        }))
    }
}

/// The frame schema a child declares for `schema` under `mode`.
fn frame_schema(schema: &SeriesSchema, mode: RawLabelMode) -> SchemaRef {
    raw_series_frame_schema_with_ordering(schema, mode, SeriesOrdering::KeyThenTime).unwrap()
}

/// Packs `chunks` into the frame batches of one partition, declared under `declared`.
fn frame_batches(
    schema: &SeriesSchema,
    mode: RawLabelMode,
    declared: &SchemaRef,
    chunks: Vec<RawSeriesChunk>,
) -> Vec<RecordBatch> {
    let mut builder = RawSeriesFrameBuilder::try_new(schema, mode, 1024, 1 << 20).unwrap();
    let mut out = Vec::new();
    for chunk in chunks {
        let mut cursor = RawSeriesChunkCursor::try_new(schema, mode, chunk).unwrap();
        while let Some(batch) = cursor.append_to(&mut builder).unwrap() {
            out.push(batch.with_schema(declared.clone()).unwrap());
        }
    }
    if let Some(batch) = builder.flush().unwrap() {
        out.push(batch.with_schema(declared.clone()).unwrap());
    }
    out
}

/// A one-partition child whose frames carry `chunks` of `schema` under explicit labels.
fn explicit_child(schema: &Arc<SeriesSchema>, chunks: Vec<RawSeriesChunk>) -> Arc<FrameChild> {
    let declared = frame_schema(schema, RawLabelMode::Explicit);
    let batches = frame_batches(schema, RawLabelMode::Explicit, &declared, chunks);
    FrameChild::new(declared, vec![batches])
}

fn context(pool: Arc<dyn MemoryPool>) -> Arc<TaskContext> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .build()
        .unwrap();
    Arc::new(TaskContext::default().with_runtime(Arc::new(runtime)))
}

fn greedy(bytes: usize) -> Arc<dyn MemoryPool> {
    Arc::new(GreedyMemoryPool::new(bytes))
}

// ---------------------------------------------------------------------------------------------
// Output schemas and the plan
// ---------------------------------------------------------------------------------------------

fn time_field() -> Field {
    Field::new(
        "time",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    )
}

fn value_field(name: &str) -> Field {
    Field::new(name, DataType::Float64, true)
}

fn tag_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Utf8, nullable)
}

fn schema_of(fields: Vec<Field>) -> SchemaRef {
    Arc::new(Schema::new(fields))
}

/// The schema of the trees below: the time column, one tag and one field.
fn default_schema() -> SchemaRef {
    schema_of(vec![
        time_field(),
        tag_field("job", true),
        value_field("v0"),
    ])
}

fn plan(
    child: Arc<dyn ExecutionPlan>,
    tree: NativeTree,
    label_source: RawLabelSource,
    schema: SchemaRef,
    tags: &[&str],
) -> DfResult<NativeTreeExec> {
    NativeTreeExec::try_new(
        child,
        Arc::new(tree),
        label_source,
        schema,
        "time".to_string(),
        tags.iter().map(|tag| (*tag).to_string()).collect(),
    )
}

// ---------------------------------------------------------------------------------------------
// Reading a stream
// ---------------------------------------------------------------------------------------------

fn next_batch(stream: &mut SendableRecordBatchStream) -> Option<DfResult<RecordBatch>> {
    futures::executor::block_on(stream.next())
}

/// Polls the stream once, which is a step a stalled source can answer with `Pending`.
fn poll_once(stream: &mut SendableRecordBatchStream) -> Poll<Option<DfResult<RecordBatch>>> {
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    stream.as_mut().poll_next(&mut cx)
}

fn rest(stream: &mut SendableRecordBatchStream) -> Vec<DfResult<RecordBatch>> {
    let mut out = Vec::new();
    while let Some(batch) = next_batch(stream) {
        out.push(batch);
    }
    out
}

fn batches(stream: &mut SendableRecordBatchStream) -> Vec<RecordBatch> {
    rest(stream)
        .into_iter()
        .map(|batch| batch.expect("the stream must not fail here"))
        .collect()
}

fn first_batch(stream: &mut SendableRecordBatchStream) -> RecordBatch {
    match next_batch(stream) {
        Some(Ok(batch)) => batch,
        other => panic!("expected one batch, got {other:?}"),
    }
}

fn values_of(batch: &RecordBatch, index: usize) -> Vec<Option<f64>> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .iter()
        .collect()
}

fn strings_of(batch: &RecordBatch, index: usize) -> Vec<Option<String>> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|value| value.map(str::to_string))
        .collect()
}

fn times_of(batch: &RecordBatch, index: usize) -> Vec<i64> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::TimestampMillisecondArray>()
        .unwrap()
        .values()
        .to_vec()
}

/// How many metrics the plan reports, summed over every node and partition.
fn metrics_count(exec: &Arc<dyn ExecutionPlan>) -> usize {
    exec.metrics().map_or(0, |set| set.iter().count())
}

/// Replaces the child through `with_new_children`, which is what a plan rewrite calls.
///
/// DataFusion marks the method deprecated in favour of `replace_children`, and its default
/// `replace_children` is exactly this call with `ChildrenPropertiesMode::Recompute`, so the plan
/// has to keep answering it.
#[expect(deprecated)]
fn replace_child<T: ExecutionPlan>(
    plan: &Arc<T>,
    children: Vec<Arc<dyn ExecutionPlan>>,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    plan.clone().with_new_children(children)
}

fn error_of(stream: &mut SendableRecordBatchStream) -> DataFusionError {
    let error = match next_batch(stream) {
        Some(Err(error)) => error,
        other => panic!("expected one error, got {other:?}"),
    };
    // Every failure is reported once, and the stream is terminal afterwards.
    assert!(
        next_batch(stream).is_none(),
        "the failure was reported twice"
    );
    assert!(
        next_batch(stream).is_none(),
        "the stream resumed after a failure"
    );
    error
}

// ---------------------------------------------------------------------------------------------
// End to end
// ---------------------------------------------------------------------------------------------

/// `abs(instant[v0])` over one series: the tree evaluates and the shell converts every value.
#[test]
fn an_instant_selection_then_a_map_produces_the_trees_values() {
    let raw = raw_schema(&["v0"]);
    let tree = NativeTree::try_new(map(
        instant(input_node(raw.clone()), 0, grid(0, 10_000, 3)),
        vec![MathOp::Abs],
    ))
    .unwrap();
    let child = explicit_child(
        &raw,
        vec![one_field(
            &raw,
            b"a",
            Some(metric(&[("job", "web-1")])),
            &[0, 10_000, 20_000],
            vec![Some(-1.0), Some(2.0), Some(-3.0)],
            true,
        )],
    );
    let exec = plan(
        child.clone(),
        tree,
        RawLabelSource::Explicit,
        default_schema(),
        &["job"],
    )
    .unwrap();
    let mut stream = exec.execute(0, context(greedy(1 << 20))).unwrap();
    let batch = first_batch(&mut stream);

    assert_eq!(batch.num_rows(), 3);
    assert_eq!(times_of(&batch, 0), vec![0, 10_000, 20_000]);
    assert_eq!(strings_of(&batch, 1), vec![Some("web-1".to_string()); 3]);
    assert_eq!(values_of(&batch, 2), vec![Some(1.0), Some(2.0), Some(3.0)]);
    assert!(
        rest(&mut stream).is_empty(),
        "the stream must end after one series"
    );
    assert_eq!(child.drops(), 1, "the child's stream must be dropped");
}

/// `abs(last_over_time(v0[1ms]))`: a window drives the same kernel path, on its own grid.
#[test]
fn a_window_then_a_map_produces_the_trees_values() {
    let raw = raw_schema(&["v0"]);
    let tree = NativeTree::try_new(map(
        window(
            input_node(raw.clone()),
            RangeFunction::LastOverTime,
            1,
            grid(0, 10_000, 3),
        ),
        vec![MathOp::Abs],
    ))
    .unwrap();
    let child = explicit_child(
        &raw,
        vec![one_field(
            &raw,
            b"a",
            Some(metric(&[("job", "web-1")])),
            &[0, 10_000, 20_000],
            vec![Some(-4.0), Some(5.0), Some(-6.0)],
            true,
        )],
    );
    let exec = plan(
        child,
        tree,
        RawLabelSource::Explicit,
        default_schema(),
        &["job"],
    )
    .unwrap();
    let mut stream = exec.execute(0, context(greedy(1 << 20))).unwrap();
    let batch = first_batch(&mut stream);

    assert_eq!(times_of(&batch, 0), vec![0, 10_000, 20_000]);
    assert_eq!(values_of(&batch, 2), vec![Some(4.0), Some(5.0), Some(6.0)]);
    assert!(rest(&mut stream).is_empty());
}

/// Columns are read by name, so the batch order is the schema's, and two fields stay paired with
/// their own columns.
#[test]
fn columns_are_reordered_and_each_field_reads_its_own_column() {
    let raw = raw_schema(&["v0", "v1"]);
    let tree =
        NativeTree::try_new(instant(input_node(raw.clone()), 0, grid(0, 10_000, 2))).unwrap();
    let child = explicit_child(
        &raw,
        vec![chunk(
            &raw,
            b"a",
            Some(metric(&[("job", "web-1")])),
            &[0, 10_000],
            vec![vec![Some(1.0), Some(2.0)], vec![Some(10.0), Some(20.0)]],
            true,
        )],
    );
    let schema = schema_of(vec![
        tag_field("job", true),
        value_field("v1"),
        time_field(),
        value_field("v0"),
    ]);
    let exec = plan(
        child,
        tree,
        RawLabelSource::Explicit,
        schema.clone(),
        &["job"],
    )
    .unwrap();
    assert_eq!(
        exec.schema(),
        schema,
        "the plan must report the schema it was given"
    );
    let mut stream = exec.execute(0, context(greedy(1 << 20))).unwrap();
    let batch = first_batch(&mut stream);

    assert_eq!(batch.schema(), schema);
    assert_eq!(
        strings_of(&batch, 0),
        vec![Some("web-1".to_string()), Some("web-1".to_string())]
    );
    assert_eq!(values_of(&batch, 1), vec![Some(10.0), Some(20.0)]);
    assert_eq!(times_of(&batch, 2), vec![0, 10_000]);
    assert_eq!(values_of(&batch, 3), vec![Some(1.0), Some(2.0)]);
}

/// A series with holes on its grid keeps the timestamps it actually carries: the runs are not
/// filled out to the grid's every point.
#[test]
fn sparse_runs_keep_the_timestamps_they_carry() {
    let raw = raw_schema(&["v0"]);
    let tree =
        NativeTree::try_new(instant(input_node(raw.clone()), 0, grid(0, 10_000, 6))).unwrap();
    let child = explicit_child(
        &raw,
        vec![one_field(
            &raw,
            b"a",
            Some(metric(&[("job", "web-1")])),
            &[10_000, 40_000],
            vec![Some(2.0), Some(5.0)],
            true,
        )],
    );
    let exec = plan(
        child,
        tree,
        RawLabelSource::Explicit,
        default_schema(),
        &["job"],
    )
    .unwrap();
    let mut stream = exec.execute(0, context(greedy(1 << 20))).unwrap();
    let batch = first_batch(&mut stream);

    assert_eq!(batch.num_rows(), 2, "the two holes must not become rows");
    assert_eq!(times_of(&batch, 0), vec![10_000, 40_000]);
    assert_eq!(values_of(&batch, 2), vec![Some(2.0), Some(5.0)]);
}

/// An ordinary NaN is a value; a position a field holds no sample at is a NULL; a row another
/// field fills is kept.
#[test]
fn nan_stays_a_value_and_a_missing_sample_becomes_null() {
    let raw = raw_schema(&["v0", "v1"]);
    let tree =
        NativeTree::try_new(instant(input_node(raw.clone()), 0, grid(0, 10_000, 3))).unwrap();
    let child = explicit_child(
        &raw,
        vec![chunk(
            &raw,
            b"a",
            Some(metric(&[("job", "web-1")])),
            &[0, 10_000, 20_000],
            vec![
                vec![Some(f64::NAN), Some(2.0), Some(3.0)],
                vec![Some(1.0), None, Some(3.0)],
            ],
            true,
        )],
    );
    let schema = schema_of(vec![time_field(), value_field("v0"), value_field("v1")]);
    let exec = plan(child, tree, RawLabelSource::Explicit, schema, &[]).unwrap();
    let mut stream = exec.execute(0, context(greedy(1 << 20))).unwrap();
    let batch = first_batch(&mut stream);

    let nan = values_of(&batch, 1);
    assert!(
        nan[0].unwrap().is_nan(),
        "an ordinary NaN must stay a value"
    );
    assert_eq!(nan[1], Some(2.0));
    assert_eq!(
        values_of(&batch, 2),
        vec![Some(1.0), None, Some(3.0)],
        "a field with no sample at a row reports NULL and keeps the row"
    );
}

/// A series that produced no row still closes, and its closing chunk becomes no batch at all.
#[test]
fn a_series_with_no_row_closes_without_a_batch() {
    let raw = raw_schema(&["v0"]);
    let tree =
        NativeTree::try_new(instant(input_node(raw.clone()), 0, grid(0, 10_000, 3))).unwrap();
    let child = explicit_child(
        &raw,
        vec![
            one_field(
                &raw,
                b"a",
                Some(metric(&[("job", "web-1")])),
                &[0],
                vec![Some(7.0)],
                true,
            ),
            // Every sample of `b` lies past the grid, so its selection decides no row.
            one_field(
                &raw,
                b"b",
                Some(metric(&[("job", "web-2")])),
                &[100_000],
                vec![Some(9.0)],
                true,
            ),
        ],
    );
    let exec = plan(
        child,
        tree,
        RawLabelSource::Explicit,
        default_schema(),
        &["job"],
    )
    .unwrap();
    let mut stream = exec.execute(0, context(greedy(1 << 20))).unwrap();
    let out = batches(&mut stream);

    assert_eq!(
        out.len(),
        1,
        "the empty closing chunk must not become a batch"
    );
    assert_eq!(times_of(&out[0], 0), vec![0]);
    assert_eq!(values_of(&out[0], 2), vec![Some(7.0)]);
    assert_eq!(
        strings_of(&out[0], 1),
        vec![Some("web-1".to_string())],
        "the batch must belong to the series that had a row"
    );
}

/// A tag the series does not carry is NULL in a nullable column and the empty string otherwise,
/// while a tag it carries is its value.
#[test]
fn a_missing_tag_is_null_or_empty_as_the_column_declares() {
    let raw = raw_schema(&["v0"]);
    let tree =
        NativeTree::try_new(instant(input_node(raw.clone()), 0, grid(0, 10_000, 1))).unwrap();
    let child = explicit_child(
        &raw,
        vec![one_field(
            &raw,
            b"a",
            Some(metric(&[("job", "web-1")])),
            &[0],
            vec![Some(1.0)],
            true,
        )],
    );
    let schema = schema_of(vec![
        time_field(),
        tag_field("job", true),
        tag_field("zone", true),
        tag_field("host", false),
        value_field("v0"),
    ]);
    let exec = plan(
        child,
        tree,
        RawLabelSource::Explicit,
        schema,
        &["job", "zone", "host"],
    )
    .unwrap();
    let mut stream = exec.execute(0, context(greedy(1 << 20))).unwrap();
    let batch = first_batch(&mut stream);

    assert_eq!(strings_of(&batch, 1), vec![Some("web-1".to_string())]);
    assert_eq!(strings_of(&batch, 2), vec![None], "a nullable tag is NULL");
    assert_eq!(
        strings_of(&batch, 3),
        vec![Some(String::new())],
        "a non-nullable tag is the empty string"
    );
}

// ---------------------------------------------------------------------------------------------
// Label sources
// ---------------------------------------------------------------------------------------------

/// The metadata and label binding of a primary-key source whose one tag is `job`.
fn primary_key_source(metric_name: &str) -> (RawLabelSource, Arc<RegionMetadata>) {
    let mut builder = RegionMetadataBuilder::new(RegionId::new(7, 1));
    builder.push_column_metadata(ColumnMetadata {
        column_schema: ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        ),
        semantic_type: SemanticType::Timestamp,
        column_id: 0,
    });
    builder.push_column_metadata(ColumnMetadata {
        column_schema: ColumnSchema::new("job", ConcreteDataType::string_datatype(), true),
        semantic_type: SemanticType::Tag,
        column_id: 1,
    });
    builder.push_column_metadata(ColumnMetadata {
        column_schema: ColumnSchema::new("v0", ConcreteDataType::float64_datatype(), true),
        semantic_type: SemanticType::Field,
        column_id: 2,
    });
    builder.primary_key(vec![1]);
    builder.primary_key_encoding(PrimaryKeyEncoding::Sparse);
    let metadata = Arc::new(builder.build().unwrap());
    let source = RawLabelSource::PrimaryKey {
        metric_name: Arc::from(metric_name),
        physical_metadata: Arc::clone(&metadata),
        logical_tags: Arc::from([store_api::region_engine::BoundTag {
            column_id: 1,
            name: Arc::from("job"),
        }]),
    };
    (source, metadata)
}

/// The sparse primary key of one series whose `job` tag is `job`.
fn sparse_key(job: &str) -> Vec<u8> {
    let codec = SparsePrimaryKeyCodec::with_fields(vec![(
        1,
        SortField::new(ConcreteDataType::string_datatype()),
    )]);
    let mut key = Vec::new();
    codec.encode_internal(42, 9, &mut key).unwrap();
    codec
        .encode_raw_tag_value(std::iter::once((1, job.as_bytes())), &mut key)
        .unwrap();
    key
}

/// A primary-key source has its labels decoded on the way in, so the tag column reads them.
#[test]
fn primary_key_labels_are_materialized_before_the_tree_sees_them() {
    let raw = raw_schema(&["v0"]);
    let tree =
        NativeTree::try_new(instant(input_node(raw.clone()), 0, grid(0, 10_000, 1))).unwrap();
    let (label_source, _metadata) = primary_key_source("requests");
    let declared = frame_schema(&raw, RawLabelMode::Key);
    let batches = frame_batches(
        &raw,
        RawLabelMode::Key,
        &declared,
        vec![one_field(
            &raw,
            &sparse_key("web-1"),
            None,
            &[0],
            vec![Some(3.0)],
            true,
        )],
    );
    let child = FrameChild::new(declared, vec![batches]);
    let exec = plan(child, tree, label_source, default_schema(), &["job"]).unwrap();
    let mut stream = exec.execute(0, context(greedy(1 << 20))).unwrap();
    let batch = first_batch(&mut stream);

    assert_eq!(
        strings_of(&batch, 1),
        vec![Some("web-1".to_string())],
        "the label must be decoded from the primary key"
    );
    assert_eq!(values_of(&batch, 2), vec![Some(3.0)]);
}

// ---------------------------------------------------------------------------------------------
// Partitions
// ---------------------------------------------------------------------------------------------

/// Two partitions are each executed on their own, an out-of-range partition is refused, and the
/// partition count follows a replaced child.
#[test]
fn partitions_are_executed_separately_and_out_of_range_is_an_error() {
    let raw = raw_schema(&["v0"]);
    let tree =
        NativeTree::try_new(instant(input_node(raw.clone()), 0, grid(0, 10_000, 1))).unwrap();
    let declared = frame_schema(&raw, RawLabelMode::Explicit);
    let partition = |declared: &SchemaRef, key: &[u8], job: &str, value: f64| {
        frame_batches(
            &raw,
            RawLabelMode::Explicit,
            declared,
            vec![one_field(
                &raw,
                key,
                Some(metric(&[("job", job)])),
                &[0],
                vec![Some(value)],
                true,
            )],
        )
    };
    let child = FrameChild::new(
        declared.clone(),
        vec![
            partition(&declared, b"a", "web-1", 1.0),
            partition(&declared, b"b", "web-2", 2.0),
        ],
    );
    let exec = Arc::new(
        plan(
            child.clone(),
            tree,
            RawLabelSource::Explicit,
            default_schema(),
            &["job"],
        )
        .unwrap(),
    );
    assert_eq!(
        exec.properties().partitioning.partition_count(),
        2,
        "the child's two partitions must both survive"
    );
    assert!(
        matches!(
            exec.properties().partitioning,
            Partitioning::UnknownPartitioning(2)
        ),
        "the partitioning must be unknown with the child's count, not a scheme it cannot promise"
    );
    assert!(
        exec.properties().output_ordering().is_none(),
        "no ordering is inherited from the child"
    );

    for (partition, value, job) in [(0, 1.0, "web-1"), (1, 2.0, "web-2")] {
        let mut stream = exec
            .execute(partition, context(greedy(1 << 20)))
            .unwrap_or_else(|error| panic!("partition {partition} must run: {error}"));
        let batch = first_batch(&mut stream);
        assert_eq!(values_of(&batch, 2), vec![Some(value)]);
        assert_eq!(strings_of(&batch, 1), vec![Some(job.to_string())]);
        assert!(rest(&mut stream).is_empty());
    }
    let error = match exec.execute(2, context(greedy(1 << 20))) {
        Err(error) => error,
        Ok(_) => panic!("partition 2 does not exist"),
    };
    assert!(
        error.to_string().contains("2 partition(s)"),
        "unexpected error: {error}"
    );

    let wider = FrameChild::new(
        declared.clone(),
        vec![
            partition(&declared, b"a", "web-1", 1.0),
            partition(&declared, b"b", "web-2", 2.0),
            vec![],
        ],
    );
    let replaced =
        replace_child(&exec, vec![wider]).expect("a child with the same frames may be put in");
    assert_eq!(
        replaced.properties().partitioning.partition_count(),
        3,
        "the partition count must follow the replaced child"
    );
    assert_eq!(replaced.schema(), exec.schema(), "the binding must survive");

    let error = replace_child(&exec, vec![]).expect_err("a plan without a child is invalid");
    assert!(
        error.to_string().contains("exactly one"),
        "unexpected error: {error}"
    );
}

// ---------------------------------------------------------------------------------------------
// Reset and metrics
// ---------------------------------------------------------------------------------------------

/// A reset plan starts from an empty metrics set and takes its pool from the next execution, not
/// from the one before it.
#[test]
fn reset_state_takes_the_next_pool_and_starts_from_empty_metrics() {
    let raw = raw_schema(&["v0"]);
    let tree =
        NativeTree::try_new(instant(input_node(raw.clone()), 0, grid(0, 10_000, 1))).unwrap();
    let child = explicit_child(
        &raw,
        vec![one_field(
            &raw,
            b"a",
            Some(metric(&[("job", "web-1")])),
            &[0],
            vec![Some(4.0)],
            true,
        )],
    );
    let exec = Arc::new(
        plan(
            child,
            tree,
            RawLabelSource::Explicit,
            default_schema(),
            &["job"],
        )
        .unwrap(),
    ) as Arc<dyn ExecutionPlan>;
    assert_eq!(metrics_count(&exec), 0);

    // A pool that cannot hold the chunk the tree reserves fails at the reservation.
    let mut close = exec
        .execute(0, context(greedy(1)))
        .expect("the state is built before anything is reserved");
    let error = error_of(&mut close);
    assert!(
        matches!(error, DataFusionError::ResourcesExhausted(_)),
        "unexpected error: {error}"
    );

    let reset = exec.clone().reset_state().unwrap();
    assert_eq!(
        metrics_count(&reset),
        0,
        "a reset plan must not keep the metrics of the execution it replaced"
    );
    assert_eq!(
        reset.properties().partitioning.partition_count(),
        1,
        "a reset plan keeps the partition count"
    );
    let mut stream = reset.execute(0, context(greedy(1 << 20))).unwrap();
    let batch = first_batch(&mut stream);
    assert_eq!(values_of(&batch, 2), vec![Some(4.0)]);
    assert!(rest(&mut stream).is_empty());
    assert!(
        metrics_count(&reset) > 0,
        "the reset plan must register the nodes again on execution"
    );
}

/// The metrics the plan reports are the per-node ones the runtime registered, per partition.
#[test]
fn the_plan_exposes_the_metrics_the_runtime_registered() {
    let raw = raw_schema(&["v0"]);
    let tree = NativeTree::try_new(map(
        instant(input_node(raw.clone()), 0, grid(0, 10_000, 1)),
        vec![MathOp::Abs],
    ))
    .unwrap();
    let child = explicit_child(
        &raw,
        vec![one_field(
            &raw,
            b"a",
            Some(metric(&[("job", "web-1")])),
            &[0],
            vec![Some(-4.0)],
            true,
        )],
    );
    let exec = Arc::new(
        plan(
            child,
            tree,
            RawLabelSource::Explicit,
            default_schema(),
            &["job"],
        )
        .unwrap(),
    );
    let mut stream = exec.execute(0, context(greedy(1 << 20))).unwrap();
    batches(&mut stream);

    let set = exec.metrics().expect("the plan reports metrics");
    let source = NodePath::root().input().input();
    let name = source.metric_name(NodeMetric::INPUT_BATCHES);
    match set.sum_by_name(&name) {
        Some(MetricValue::Count { count, .. }) => {
            assert!(count.value() > 0, "the source node saw no batch");
        }
        other => panic!("no count named {name}: {other:?}"),
    }
}

// ---------------------------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------------------------

/// Every column of the output must be exactly one of the three roles, of the declared type.
#[test]
fn a_schema_that_does_not_match_the_root_output_is_refused_at_construction() {
    let raw = raw_schema(&["v0"]);
    let tree =
        || NativeTree::try_new(instant(input_node(raw.clone()), 0, grid(0, 10_000, 1))).unwrap();
    let child = || {
        explicit_child(
            &raw,
            vec![one_field(
                &raw,
                b"a",
                Some(metric(&[("job", "web-1")])),
                &[0],
                vec![Some(1.0)],
                true,
            )],
        ) as Arc<dyn ExecutionPlan>
    };
    let cases: Vec<(&str, SchemaRef, &str, &[&str], &str)> = vec![
        (
            "two columns of one name",
            schema_of(vec![
                time_field(),
                tag_field("job", true),
                value_field("v0"),
                value_field("v0"),
            ]),
            "time",
            &["job"],
            "twice",
        ),
        (
            "a column no role claims",
            schema_of(vec![
                time_field(),
                tag_field("job", true),
                value_field("v0"),
                value_field("mystery"),
            ]),
            "time",
            &["job"],
            "is not the time column, one of the tags, or a field",
        ),
        (
            "no column of the time column's name",
            schema_of(vec![tag_field("job", true), value_field("v0")]),
            "when",
            &["job"],
            "has no column `when` for the time column",
        ),
        (
            "no column of a tag's name",
            schema_of(vec![time_field(), value_field("v0")]),
            "time",
            &["job"],
            "has no column `job` for a tag",
        ),
        (
            "no column of a field's name",
            schema_of(vec![time_field(), tag_field("job", true)]),
            "time",
            &["job"],
            "has no column `v0` for a field",
        ),
        (
            "the time column is also a field",
            schema_of(vec![
                Field::new(
                    "v0",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                tag_field("job", true),
            ]),
            "v0",
            &["job"],
            "claimed as both",
        ),
        (
            "a tag is also the time column",
            schema_of(vec![
                time_field(),
                tag_field("job", true),
                value_field("v0"),
            ]),
            "job",
            &["job"],
            "claimed as both",
        ),
        (
            "the time column is not a timestamp",
            schema_of(vec![
                value_field("time"),
                tag_field("job", true),
                value_field("v0"),
            ]),
            "time",
            &["job"],
            "a Timestamp(Millisecond, None)",
        ),
        (
            "the time column is not in milliseconds",
            schema_of(vec![
                Field::new(
                    "time",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
                tag_field("job", true),
                value_field("v0"),
            ]),
            "time",
            &["job"],
            "a Timestamp(Millisecond, None)",
        ),
        (
            "a field column that cannot be null",
            schema_of(vec![
                time_field(),
                tag_field("job", true),
                Field::new("v0", DataType::Float64, false),
            ]),
            "time",
            &["job"],
            "a nullable Float64",
        ),
        (
            "a field column of another type",
            schema_of(vec![
                time_field(),
                tag_field("job", true),
                Field::new("v0", DataType::Int64, true),
            ]),
            "time",
            &["job"],
            "a nullable Float64",
        ),
        (
            "a tag column of another type",
            schema_of(vec![time_field(), value_field("job"), value_field("v0")]),
            "time",
            &["job"],
            "a Utf8",
        ),
    ];
    for (case, schema, time_column, tags, expected) in cases {
        let error = NativeTreeExec::try_new(
            child(),
            Arc::new(tree()),
            RawLabelSource::Explicit,
            schema,
            time_column.to_string(),
            tags.iter().map(|tag| (*tag).to_string()).collect(),
        )
        .expect_err("this schema must be refused");
        assert!(
            error.to_string().contains(expected),
            "{case}: expected a message holding `{expected}`, got {error}"
        );
    }
}

/// A tree this plan does not execute is refused, and the message says which node it met.
#[test]
fn an_unsupported_tree_is_refused() {
    let raw = raw_schema(&["v0"]);
    let cases: Vec<(&str, NativeTree, &str)> = vec![
        (
            "a scalar root",
            NativeTree::try_new(NativeNode {
                expr: NativeExpr::Scalar(BoundScalar { value: 1.0 }),
                output: NativeType::Scalar(grid(0, 10_000, 1)),
            })
            .unwrap(),
            "is a scalar",
        ),
        (
            "a raw root",
            NativeTree::try_new(input_node(raw.clone())).unwrap(),
            "root produces raw samples",
        ),
        (
            // A map consumes an evaluated vector, so its input node is one a raw frame cannot feed.
            "an evaluated input",
            NativeTree::try_new(map(
                evaluated_input_node(grid(0, 10_000, 1)),
                vec![MathOp::Abs],
            ))
            .unwrap(),
            "consumes evaluated chunks",
        ),
    ];
    for (case, tree, expected) in cases {
        let child = explicit_child(
            &raw,
            vec![one_field(
                &raw,
                b"a",
                Some(metric(&[("job", "web-1")])),
                &[0],
                vec![Some(1.0)],
                true,
            )],
        ) as Arc<dyn ExecutionPlan>;
        let error = NativeTreeExec::try_new(
            child,
            Arc::new(tree),
            RawLabelSource::Explicit,
            default_schema(),
            "time".to_string(),
            vec!["job".to_string()],
        )
        .expect_err("this tree must be refused");
        assert!(
            error.to_string().contains(expected),
            "{case}: expected a message holding `{expected}`, got {error}"
        );
    }
}

/// A child whose declared frame is not this tree's raw layout is refused before it runs.
#[test]
fn a_child_whose_frame_does_not_match_the_tree_is_refused() {
    let raw = raw_schema(&["v0"]);
    let other = raw_schema(&["other"]);
    let tree =
        NativeTree::try_new(instant(input_node(raw.clone()), 0, grid(0, 10_000, 1))).unwrap();
    let declared = frame_schema(&other, RawLabelMode::Explicit);
    let batches = frame_batches(
        &other,
        RawLabelMode::Explicit,
        &declared,
        vec![one_field(
            &other,
            b"a",
            Some(metric(&[("job", "web-1")])),
            &[0],
            vec![Some(1.0)],
            true,
        )],
    );
    let child = FrameChild::new(declared, vec![batches]);
    let error = plan(
        child.clone(),
        tree,
        RawLabelSource::Explicit,
        default_schema(),
        &["job"],
    )
    .expect_err("a frame of another layout must be refused");
    assert!(!error.to_string().is_empty());

    // The same child is refused when it replaces one the tree did match.
    let matching = explicit_child(&raw, Vec::new());
    let exec = Arc::new(
        plan(
            matching,
            NativeTree::try_new(instant(input_node(raw.clone()), 0, grid(0, 10_000, 1))).unwrap(),
            RawLabelSource::Explicit,
            default_schema(),
            &["job"],
        )
        .unwrap(),
    );
    let error = replace_child(&exec, vec![child])
        .expect_err("the replaced child must still describe the tree");
    assert!(!error.to_string().is_empty());
}

// ---------------------------------------------------------------------------------------------
// Failures on the way out
// ---------------------------------------------------------------------------------------------

/// A histogram is not a float: converting one is an explicit error, not a NULL and not a dropped
/// row.
#[test]
fn a_histogram_sample_is_an_error() {
    let raw = raw_schema(&["v0"]);
    let tree = NativeTree::try_new(window(
        input_node(raw.clone()),
        RangeFunction::LastOverTime,
        1,
        grid(0, 10_000, 1),
    ))
    .unwrap();
    let child = explicit_child(&raw, vec![histogram_chunk(&raw, b"a", &[0])]);
    let schema = schema_of(vec![time_field(), value_field("v0")]);
    let exec = plan(child, tree, RawLabelSource::Explicit, schema, &[]).unwrap();
    let mut stream = exec.execute(0, context(greedy(1 << 20))).unwrap();
    let error = error_of(&mut stream);
    assert!(
        error.to_string().contains("native histogram"),
        "unexpected error: {error}"
    );
}

/// A child that fails is reported once, and what it already handed over does not make the stream
/// a success.
#[test]
fn a_child_failure_is_reported_once_after_its_prefix() {
    let raw = raw_schema(&["v0"]);
    let tree =
        NativeTree::try_new(instant(input_node(raw.clone()), 0, grid(0, 10_000, 1))).unwrap();
    let declared = frame_schema(&raw, RawLabelMode::Explicit);
    let batches = frame_batches(
        &raw,
        RawLabelMode::Explicit,
        &declared,
        vec![one_field(
            &raw,
            b"a",
            Some(metric(&[("job", "web-1")])),
            &[0],
            vec![Some(1.0)],
            true,
        )],
    );
    let child = FrameChild::failing(declared, vec![batches]);
    let exec = plan(
        child,
        tree,
        RawLabelSource::Explicit,
        default_schema(),
        &["job"],
    )
    .unwrap();
    let mut stream = exec.execute(0, context(greedy(1 << 20))).unwrap();
    // The prefix the child did deliver is produced, and the failure follows it once.
    assert_eq!(values_of(&first_batch(&mut stream), 2), vec![Some(1.0)]);
    let error = error_of(&mut stream);
    assert!(
        error.to_string().contains("the frame child failed"),
        "unexpected error: {error}"
    );
}

/// A frame that ends inside a series is refused by the boundary, so the prefix is not a success.
#[test]
fn a_frame_that_ends_inside_a_series_is_refused() {
    let raw = raw_schema(&["v0"]);
    let tree =
        NativeTree::try_new(instant(input_node(raw.clone()), 0, grid(0, 10_000, 1))).unwrap();
    // The chunk does not say `is_last`, and no chunk after it says so either.
    let child = explicit_child(
        &raw,
        vec![one_field(
            &raw,
            b"a",
            Some(metric(&[("job", "web-1")])),
            &[0],
            vec![Some(1.0)],
            false,
        )],
    );
    let exec = plan(
        child,
        tree,
        RawLabelSource::Explicit,
        default_schema(),
        &["job"],
    )
    .unwrap();
    let mut stream = exec.execute(0, context(greedy(1 << 20))).unwrap();
    let error = error_of(&mut stream);
    assert!(
        error.to_string().contains("before key was final"),
        "unexpected error: {error}"
    );
}

/// A source that ends inside a series is a truncation error from the runtime itself, reported once.
#[test]
fn a_source_that_ends_inside_a_series_is_a_truncation_error() {
    let raw = raw_schema(&["v0"]);
    let tree = Arc::new(
        NativeTree::try_new(instant(input_node(raw.clone()), 0, grid(0, 10_000, 1))).unwrap(),
    );
    let set = ExecutionPlanMetricsSet::new();
    let state = NativeExecutionState::try_new(&tree, greedy(1 << 20), &set, 0).unwrap();
    let binding = Arc::new(
        OutputBinding::try_new(
            tree.root().output.samples().unwrap(),
            default_schema(),
            "time",
            &["job".to_string()],
        )
        .unwrap(),
    );
    let source = ChunkSource {
        schema: raw.clone(),
        labels: RawLabelSource::Explicit,
        chunks: vec![one_field(
            &raw,
            b"a",
            Some(metric(&[("job", "web-1")])),
            &[0],
            vec![Some(1.0)],
            false,
        )]
        .into(),
    };
    let mut stream: SendableRecordBatchStream = Box::pin(NativeTreeStream {
        source: Some(Box::pin(source)),
        executor: Some(NativeExecutor::new(state)),
        binding,
        closing: VecDeque::new(),
        closed: false,
    });
    let error = error_of(&mut stream);
    assert!(
        error
            .to_string()
            .contains("before the last chunk of a series"),
        "unexpected error: {error}"
    );
}

/// A raw-series source a test writes by hand, which the shell can be given directly.
struct ChunkSource {
    schema: Arc<SeriesSchema>,
    labels: RawLabelSource,
    chunks: VecDeque<RawSeriesChunk>,
}

impl Stream for ChunkSource {
    type Item = std::result::Result<RawSeriesChunk, common_error::ext::BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.chunks.pop_front().map(Ok))
    }
}

impl RawSeriesStream for ChunkSource {
    fn schema(&self) -> &Arc<SeriesSchema> {
        &self.schema
    }

    fn label_source(&self) -> &RawLabelSource {
        &self.labels
    }

    fn ordering(&self) -> SeriesOrdering {
        SeriesOrdering::Contiguous
    }
}

// ---------------------------------------------------------------------------------------------
// The loop
// ---------------------------------------------------------------------------------------------

/// A settled batch is handed over without draining the child first, and without waiting for it.
///
/// The first half reads a child that has two batches: handing over the first series must cost one
/// poll, so an implementation that reads the child to its end before answering is caught. The
/// second half reads a child that stalls after its single batch: the series that batch closes is
/// still handed over, which is what "answer before reading on" means for a source that never ends.
#[test]
fn a_settled_batch_does_not_wait_for_the_child() {
    let raw = raw_schema(&["v0"]);
    let tree =
        NativeTree::try_new(instant(input_node(raw.clone()), 0, grid(0, 10_000, 1))).unwrap();
    let declared = frame_schema(&raw, RawLabelMode::Explicit);
    let batch = |declared: &SchemaRef, key: &[u8], job: &str| {
        frame_batches(
            &raw,
            RawLabelMode::Explicit,
            declared,
            vec![one_field(
                &raw,
                key,
                Some(metric(&[("job", job)])),
                &[0],
                vec![Some(1.0)],
                true,
            )],
        )
    };
    // One partition, two frame batches: the second series is only reachable after the first.
    let child = FrameChild::new(
        declared.clone(),
        vec![
            [
                batch(&declared, b"a", "web-1"),
                batch(&declared, b"b", "web-2"),
            ]
            .concat(),
        ],
    );
    let exec = plan(
        child.clone(),
        tree,
        RawLabelSource::Explicit,
        default_schema(),
        &["job"],
    )
    .unwrap();
    let mut stream = exec.execute(0, context(greedy(1 << 20))).unwrap();

    assert_eq!(
        strings_of(&first_batch(&mut stream), 1),
        vec![Some("web-1".to_string())]
    );
    assert_eq!(
        child.polls(),
        1,
        "the second raw batch was read before the first series was handed over"
    );
    assert_eq!(
        strings_of(&first_batch(&mut stream), 1),
        vec![Some("web-2".to_string())]
    );
    assert_eq!(
        child.polls(),
        2,
        "the second series must have been read for the second batch"
    );
    assert!(rest(&mut stream).is_empty());

    // A child that stalls after one batch still has its series answered: the tree settled the
    // chunk, so the batch is handed over rather than waited on.
    let stalling = FrameChild::stalling(declared.clone(), vec![batch(&declared, b"a", "web-1")]);
    let exec = plan(
        stalling.clone(),
        NativeTree::try_new(instant(input_node(raw.clone()), 0, grid(0, 10_000, 1))).unwrap(),
        RawLabelSource::Explicit,
        default_schema(),
        &["job"],
    )
    .unwrap();
    let mut stream = exec.execute(0, context(greedy(1 << 20))).unwrap();
    // One poll, not a blocked read: an implementation that polls the child before answering would
    // report `Pending` here even though the tree has a batch ready.
    match poll_once(&mut stream) {
        Poll::Ready(Some(Ok(batch))) => assert_eq!(
            strings_of(&batch, 1),
            vec![Some("web-1".to_string())],
            "the batch must be handed over while the child is still open"
        ),
        other => panic!("the settled batch must be handed over at once, got {other:?}"),
    }
    assert!(
        poll_once(&mut stream).is_pending(),
        "the child stalled, so there is nothing more to hand over yet"
    );
}

/// Dropping the stream returns everything the execution reserved, and releases the child.
///
/// The series is still open when the stream is dropped -- the child has stalled between its two
/// chunks -- so what the pool holds belongs to a series nothing will ever close.
#[test]
fn dropping_the_stream_releases_the_pool() {
    let raw = raw_schema(&["v0"]);
    let tree =
        NativeTree::try_new(instant(input_node(raw.clone()), 0, grid(0, 10_000, 2))).unwrap();
    let declared = frame_schema(&raw, RawLabelMode::Explicit);
    // One frame batch per chunk, so the child can stall between the two halves of the series.
    let opening = frame_batches(
        &raw,
        RawLabelMode::Explicit,
        &declared,
        vec![one_field(
            &raw,
            b"a",
            Some(metric(&[("job", "web-1")])),
            &[0],
            vec![Some(1.0)],
            false,
        )],
    );
    let closing = frame_batches(
        &raw,
        RawLabelMode::Explicit,
        &declared,
        vec![one_field(
            &raw,
            b"a",
            Some(metric(&[("job", "web-1")])),
            &[10_000],
            vec![Some(2.0)],
            true,
        )],
    );
    let child = FrameChild::stalling(declared, vec![[opening, closing].concat()]);
    let exec = plan(
        child.clone(),
        tree,
        RawLabelSource::Explicit,
        default_schema(),
        &["job"],
    )
    .unwrap();
    let pool = greedy(1 << 20);
    let mut stream = exec.execute(0, context(pool.clone())).unwrap();

    assert!(
        poll_once(&mut stream).is_pending(),
        "an open series produces no batch, and the child has stalled"
    );
    assert!(
        pool.reserved() > 0,
        "the execution must hold the open series it reserved"
    );
    assert_eq!(child.drops(), 0, "the child's stream is still being read");

    drop(stream);
    assert_eq!(
        pool.reserved(),
        0,
        "dropping the stream must return the pool"
    );
    assert_eq!(
        child.drops(),
        1,
        "dropping the stream must drop the child's stream"
    );
}
