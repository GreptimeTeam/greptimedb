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

//! What this node has to get right: where a chunk lands, that a chain crosses it in one piece, what
//! a stalled output does to the read, what an output learns when the child fails, and that one output
//! is the degenerate case rather than a special one.
//!
//! Some properties need an input that behaves in time rather than only in sequence, so they are tested
//! with children that hold: partitions that stay silent until the test opens a [`Switch`], which is
//! how the streams' ownership of the read is checked, by making the input produce nothing before the
//! plan is dropped.
//!
//! That a partition is read on a task of its own is not one of those properties, and is deliberately
//! not tested as one: it is a structural fact about the read, so it is observed structurally, by
//! recording the task that polls each partition (see [`Recording`]), rather than by requiring two
//! tasks to run on two threads at the same instant - which the test cannot require of the runtime.
//!
//! The child here is scripted rather than planned: each of its partitions answers a list of steps,
//! so a test says which series a chunk belongs to, where on the grid it sits and whether it closes
//! the series instead of relying on where a batch boundary happens to fall. The frames are packed and
//! unpacked by the same helpers the real path uses, so an assertion about an output stream is an
//! assertion about what a consumer of this node decodes.

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Poll, Waker};
use std::time::Duration;

use datafusion::arrow::array::{Array, Float64Array, ListArray, StructArray, UInt32Array};
use datafusion::arrow::buffer::{Buffer, ScalarBuffer};
use datafusion::common::config::ConfigOptions;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool, UnboundedMemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::ensure_coop::EnsureCooperative;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::limit::LocalLimitExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use datatypes::series::Label;
use datatypes::series::histogram::{
    BucketSideView, CounterResetHint, HistogramBatchBuilder, HistogramView,
};
use datatypes::series::raw::RawTimestamps;
use datatypes::series::samples::{FieldSpec, Samples, SeriesFields};
use datatypes::series::time::{EvalGrid, GridSpan, GridSpec, TimeAxis};
use futures::TryStreamExt;

use super::*;
use crate::extension_plan::test_util::prepare_test_data;
use crate::grid_series::{render_label_set, series_bytes};
use crate::native::execute::{merge_workspace_bytes, operand_chunk_bytes};

const TEST_FIXTURE_VOLUME: usize = 4;
const GRID_START: i64 = 0;
const GRID_STEP: i64 = 1_000;
const GRID_LEN: u64 = 256;

/// The grid every frame of this module is addressed on: a value per second, 256 of them.
fn grid() -> Arc<EvalGrid> {
    Arc::new(
        EvalGrid::try_new(GridSpec::try_new(GRID_START, GRID_STEP, GRID_LEN).unwrap()).unwrap(),
    )
}

/// A frame schema of one field on [`grid`].
fn schema() -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: Arc::from([FieldSpec {
            name: Arc::from("value"),
        }]),
        eval_grid: Some(grid()),
    })
}

/// Complete labels of the `cpu` metric on one host.
fn labels(host: &str) -> SeriesLabels {
    SeriesLabels::try_new(
        Some(Arc::from("cpu")),
        vec![Label {
            name: Arc::from("host"),
            value: Arc::from(host),
        }],
    )
    .expect("the labels of these tests are legal")
}

fn histogram_chunk(metric: SeriesLabels, position: usize) -> (Series, bool) {
    histogram_rows_chunk(metric, position, 1)
}

fn histogram_rows_chunk(metric: SeriesLabels, position: usize, rows: usize) -> (Series, bool) {
    histogram_rows_chunk_with_sum(metric, position, rows, 3.0)
}

fn histogram_rows_chunk_with_sum(
    metric: SeriesLabels,
    position: usize,
    rows: usize,
    sum: f64,
) -> (Series, bool) {
    histogram_rows_chunk_with_schema(&schema(), metric, position, rows, sum)
}

fn histogram_rows_chunk_with_schema(
    schema: &Arc<SeriesSchema>,
    metric: SeriesLabels,
    position: usize,
    rows: usize,
    sum: f64,
) -> (Series, bool) {
    let mut builder = HistogramBatchBuilder::new();
    let mut indices = Vec::with_capacity(rows);
    let offsets = [0];
    let lengths = [4];
    let buckets = [1.0, 2.0, 3.0, 4.0];
    for _ in 0..rows {
        indices.push(
            builder
                .push(HistogramView {
                    schema: 0,
                    zero_threshold: 0.0,
                    sum,
                    count: 4.0,
                    zero_count: 0.0,
                    reset_hint: CounterResetHint::Unknown,
                    start_timestamp: None,
                    custom_values: &[0.5, 1.5, 2.5, 3.5],
                    positive: BucketSideView {
                        span_offsets: &offsets,
                        span_lengths: &lengths,
                        buckets: &buckets,
                    },
                    negative: BucketSideView {
                        span_offsets: &offsets,
                        span_lengths: &lengths,
                        buckets: &buckets,
                    },
                })
                .expect("histogram row builds"),
        );
    }
    let table = builder.finish().expect("histogram table builds");
    let fields = SeriesFields::one(
        Samples::try_from_histogram(datafusion::arrow::array::UInt32Array::from(indices), table)
            .expect("histogram indices reference their table"),
    );
    let time = TimeAxis::from_grid_spans(
        schema.eval_grid.clone().unwrap(),
        vec![GridSpan::new(position, rows)],
    )
    .expect("histogram rows lie on the grid");
    (Series::try_new(schema, metric, time, fields).unwrap(), true)
}

fn sparse_histogram_rows_chunk(
    metric: SeriesLabels,
    positions: impl IntoIterator<Item = usize>,
    rows: usize,
    sum: f64,
) -> (Series, bool) {
    let (mut series, is_last) = histogram_rows_chunk_with_sum(metric, 0, rows, sum);
    series.time = TimeAxis::from_grid_spans(
        schema().eval_grid.clone().unwrap(),
        positions
            .into_iter()
            .map(|position| GridSpan::new(position, 1))
            .collect(),
    )
    .expect("sparse histogram rows lie on the grid");
    (series, is_last)
}

fn sliced_chunks(metric: SeriesLabels) -> [(Series, bool); 2] {
    let schema = schema();
    let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0]));
    [0, 2].map(|start| {
        let time = TimeAxis::from_grid_spans(
            schema.eval_grid.clone().unwrap(),
            vec![GridSpan::new(start, 2)],
        )
        .expect("slice lies on the grid");
        let samples = Samples::from_float(values.slice(start, 2));
        let series = Series::try_new(&schema, metric.clone(), time, SeriesFields::one(samples))
            .expect("sliced series builds");
        (series, start == 2)
    })
}

/// A single chain split into more output fragments than one frame can encode.
#[tokio::test]
async fn public_frame_retains_large_series_charge_until_remainder_is_dropped() {
    const FRAGMENTS: usize = 257;
    let metric = labels("frame-remainder");
    let eval_grid = Arc::new(
        EvalGrid::try_new(GridSpec::try_new(GRID_START, GRID_STEP, FRAGMENTS as u64).unwrap())
            .unwrap(),
    );
    let series_schema = Arc::new(SeriesSchema {
        fields: Arc::from([FieldSpec {
            name: Arc::from("value"),
        }]),
        eval_grid: Some(eval_grid),
    });
    let steps: Vec<Step> = (0..FRAGMENTS)
        .map(|position| {
            let time = TimeAxis::from_grid_spans(
                series_schema.eval_grid.clone().unwrap(),
                vec![GridSpan::new(position, 1)],
            )
            .unwrap();
            let fields = SeriesFields::one(Samples::from_float(Float64Array::from(vec![
                position as f64,
            ])));
            let series = Series::try_new(&series_schema, metric.clone(), time, fields).unwrap();
            Step::Chunk(series, position + 1 == FRAGMENTS)
        })
        .collect();
    let probe_pool = Arc::new(GreedyMemoryPool::new(usize::MAX));
    let probe_context = task_context(&probe_pool);
    let probe = Scripted::plan_with_schema(vec![steps.clone()], Arc::clone(&series_schema));
    let mut probe =
        unpack_evaluated_series(probe.execute(0, probe_context).unwrap(), None).unwrap();
    let (decoded, errors) = until_end(&mut probe).await;
    assert!(errors.is_empty());
    assert_eq!(FRAGMENTS, decoded.len());
    let payload = decoded
        .iter()
        .map(|(series, _)| operand_chunk_bytes(series))
        .sum::<usize>();
    let expected_values = decoded
        .iter()
        .flat_map(|(series, _)| {
            series
                .fields
                .iter()
                .flat_map(|field| field.iter())
                .map(|sample| match sample {
                    datatypes::series::samples::SampleRef::Float(value) => value,
                    _ => panic!("expected float sample"),
                })
        })
        .collect::<Vec<_>>();
    assert_eq!(
        (0..FRAGMENTS).map(|p| p as f64).collect::<Vec<_>>(),
        expected_values
    );
    let pool = Arc::new(GreedyMemoryPool::new(usize::MAX));
    let context = task_context(&pool);
    let plan = node(
        Scripted::plan_with_schema(vec![steps.clone()], Arc::clone(&series_schema)),
        1,
    );
    let mut frames = plan.execute(0, Arc::clone(&context)).unwrap();
    wait_until("the producer queues the completed series", || {
        producer_stopped(&execution(&plan))
    })
    .await;
    let first_frame = futures::future::poll_fn(|cx| frames.as_mut().poll_next(cx))
        .await
        .expect("public frame arrives")
        .expect("public frame succeeds");
    assert_eq!(
        256,
        first_frame.num_rows(),
        "first public RecordBatch is full"
    );
    assert!(
        pool.reserved() >= payload + merge_workspace_bytes(FRAGMENTS, 1, 1),
        "the unread remainder retains its payload plus merge workspace"
    );
    drop(frames);
    assert_eq!(
        0,
        pool.reserved(),
        "dropping output releases remainder charges"
    );
    drop(first_frame);

    let pool = Arc::new(GreedyMemoryPool::new(usize::MAX));
    let context = task_context(&pool);
    let plan = node(Scripted::plan_with_schema(vec![steps], series_schema), 1);
    let mut frames = plan.execute(0, Arc::clone(&context)).unwrap();
    let mut retained_frames = Vec::new();
    while let Some(frame) = futures::future::poll_fn(|cx| frames.as_mut().poll_next(cx)).await {
        retained_frames.push(frame.expect("public frame succeeds"));
    }
    assert_eq!(2, retained_frames.len());
    assert_eq!(256, retained_frames[0].num_rows());
    assert_eq!(1, retained_frames[1].num_rows());
    // The public frames are one series split by the encoder's row budget, so they are decoded as
    // one stream: only the last frame's row closes the chain.
    let frame_schema = retained_frames[0].schema();
    let decoded = unpack_evaluated_series(
        Box::pin(RecordBatchStreamAdapter::new(
            frame_schema,
            futures::stream::iter(std::mem::take(&mut retained_frames).into_iter().map(Ok)),
        )),
        None,
    )
    .unwrap();
    futures::pin_mut!(decoded);
    let mut values = Vec::new();
    let mut closures = 0usize;
    while let Some(item) = decoded.as_mut().next().await {
        let (series, is_last) = item.unwrap();
        closures += usize::from(is_last);
        let field = series.fields.get(0).expect("one field");
        for (position, row) in series.time.positions() {
            let value = match field.sample(row) {
                datatypes::series::samples::SampleRef::Float(value) => value,
                _ => panic!("expected float sample"),
            };
            values.push((position, value));
        }
    }
    assert_eq!(1, closures, "the split series closes exactly once");
    let expected_values = (0..FRAGMENTS)
        .map(|position| (position, position as f64))
        .collect::<Vec<_>>();
    assert_eq!(
        expected_values, values,
        "all decoded positions stay in order"
    );
    // The decoder still owns the frames it drained until it is dropped.
    let _ = decoded;
    drop(frames);
    assert_eq!(
        0,
        pool.reserved(),
        "dropping delivered batches releases all charges"
    );
}

/// One evaluated chunk: `len` rows from grid position `start`, and whether it closes its series.
///
/// A chunk of no rows is the closing chunk a series sends when nothing is left of it, which is a
/// shape of its own: it carries the finality and no position.
fn chunk(metric: SeriesLabels, start: usize, len: usize, is_last: bool) -> (Series, bool) {
    let schema = schema();
    let spans = if len == 0 {
        Vec::new()
    } else {
        vec![GridSpan::new(start, len)]
    };
    let time = TimeAxis::from_grid_spans(schema.eval_grid.clone().unwrap(), spans)
        .expect("one run on the grid");
    let values = (0..len)
        .map(|index| Some((start + index) as f64))
        .collect::<Vec<_>>();
    let fields = SeriesFields::one(Samples::from_float(Float64Array::from(values)));
    (
        Series::try_new(&schema, metric, time, fields).expect("a chunk of this schema"),
        is_last,
    )
}

/// One step of a scripted child partition.
#[derive(Clone)]
enum Step {
    /// A chunk and its finality.
    Chunk(Series, bool),
    /// The child fails here.
    Error,
    /// The child fails here with a message of its own, for a test that has to tell two failures apart.
    ErrorSaying(&'static str),
}

impl Step {
    fn chunk((series, is_last): (Series, bool)) -> Self {
        Self::Chunk(series, is_last)
    }
}

/// What one scripted step hands over.
fn step_answer(step: Step) -> Result<(Series, bool), BoxedError> {
    match step {
        Step::Chunk(series, is_last) => Ok((series, is_last)),
        Step::Error => Err(boxed(DataFusionError::Execution(
            "scripted child failure".to_string(),
        ))),
        Step::ErrorSaying(message) => Err(boxed(DataFusionError::Execution(message.to_string()))),
    }
}

/// The steps of one partition, as the frame stream that hands them over.
struct Script {
    steps: std::vec::IntoIter<Step>,
}

impl Stream for Script {
    type Item = Result<(Series, bool), BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.steps.next().map(step_answer))
    }
}

/// A child whose partitions answer scripted steps, each as one evaluated frame.
struct Scripted {
    steps: Vec<Vec<Step>>,
    series_schema: Arc<SeriesSchema>,
    properties: Arc<PlanProperties>,
}

impl Scripted {
    /// The child plan over `steps`, one partition per list.
    fn plan(steps: Vec<Vec<Step>>) -> Arc<dyn ExecutionPlan> {
        Self::plan_with_schema(steps, schema())
    }

    /// The child plan over `steps` with an explicitly declared evaluated frame schema.
    fn plan_with_schema(
        steps: Vec<Vec<Step>>,
        series_schema: Arc<SeriesSchema>,
    ) -> Arc<dyn ExecutionPlan> {
        let frame = pack_evaluated_series(
            Box::pin(futures::stream::empty()),
            Arc::clone(&series_schema),
        )
        .expect("an evaluated schema declares a frame")
        .schema();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(frame),
            Partitioning::UnknownPartitioning(steps.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Arc::new(Self {
            steps,
            series_schema,
            properties,
        })
    }
}

impl fmt::Debug for Scripted {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Scripted")
            .field("partitions", &self.steps.len())
            .finish()
    }
}

impl DisplayAs for Scripted {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "Scripted: partitions={}", self.steps.len())
    }
}

impl ExecutionPlan for Scripted {
    fn name(&self) -> &str {
        "Scripted"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        assert!(children.is_empty(), "a scripted child has no children");
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let steps = self.steps.get(partition).cloned().ok_or_else(|| {
            DataFusionError::Execution(format!("scripted child has no partition {partition}"))
        })?;
        pack_evaluated_series(
            Box::pin(Script {
                steps: steps.into_iter(),
            }),
            Arc::clone(&self.series_schema),
        )
    }
}

/// A task context whose memory pool is `pool`, so what the node charges can be read afterwards.
fn task_context(pool: &Arc<GreedyMemoryPool>) -> Arc<TaskContext> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(pool) as Arc<dyn MemoryPool>)
        .build()
        .expect("the runtime environment builds");
    Arc::new(TaskContext::default().with_runtime(Arc::new(runtime)))
}

/// The node over `child` routing into `outputs` partitions.
fn node(child: Arc<dyn ExecutionPlan>, outputs: usize) -> Arc<SeriesRepartitionExec> {
    Arc::new(SeriesRepartitionExec::try_new(child, outputs).expect("the node is built"))
}

/// The raw schema of these tests: one float field.
fn raw_schema() -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: Arc::from([FieldSpec {
            name: "value".into(),
        }]),
        eval_grid: None,
    })
}

/// One chunk of primary key `key`, carrying `metric`'s labels, over `times`/`values`.
fn raw_chunk(
    key: &str,
    metric: Option<SeriesLabels>,
    times: &[i64],
    values: Vec<Option<f64>>,
    last: bool,
) -> RawSeriesChunk {
    let schema = raw_schema();
    let fields = vec![Samples::from_float(Float64Array::from(values))];
    RawSeriesChunk::try_new(
        &schema,
        Buffer::from_slice_ref(key.as_bytes()),
        metric,
        RawTimestamps::try_new(ScalarBuffer::from(times.to_vec())).expect("ascending timestamps"),
        SeriesFields::try_from_vec(fields).expect("one sample per timestamp per field"),
        last,
    )
    .expect("a legal raw chunk")
}

/// `{__name__="m", host="<host>"}`.
fn raw_metric(host: &str) -> SeriesLabels {
    SeriesLabels::try_new(
        Some(Arc::from("m")),
        vec![Label {
            name: Arc::from("host"),
            value: Arc::from(host),
        }],
    )
    .expect("legal labels")
}

/// One raw partition: its chunks, and an optional gate that keeps the stream open afterwards.
#[derive(Clone)]
struct RawPartition {
    chunks: Vec<RawSeriesChunk>,
    gate: Option<Arc<Switch>>,
    gate_chunks: bool,
}

struct RawScript {
    chunks: std::vec::IntoIter<RawSeriesChunk>,
    schema: Arc<SeriesSchema>,
    binding: store_api::region_engine::RawLabelSource,
    gate: Option<Arc<Switch>>,
    gate_chunks: bool,
}

impl Stream for RawScript {
    type Item = Result<RawSeriesChunk, BoxedError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.gate_chunks
            && let Some(gate) = &self.gate
            && !gate.is_open()
        {
            gate.wait(cx);
            return Poll::Pending;
        }
        if let Some(chunk) = self.chunks.next() {
            return Poll::Ready(Some(Ok(chunk)));
        }
        if let Some(gate) = &self.gate
            && !gate.is_open()
        {
            gate.wait(cx);
            return Poll::Pending;
        }
        Poll::Ready(None)
    }
}

impl RawSeriesStream for RawScript {
    fn schema(&self) -> &Arc<SeriesSchema> {
        &self.schema
    }
    fn label_source(&self) -> &store_api::region_engine::RawLabelSource {
        &self.binding
    }
    fn ordering(&self) -> SeriesOrdering {
        SeriesOrdering::Contiguous
    }
}

/// A child whose partitions hand over raw chunks, one partition per list.
struct RawScripted {
    partitions: Vec<RawPartition>,
    series_schema: Arc<SeriesSchema>,
    properties: Arc<PlanProperties>,
}

impl RawScripted {
    fn plan(partitions: Vec<RawPartition>) -> Arc<dyn ExecutionPlan> {
        Self::plan_with_ordering(partitions, SeriesOrdering::Contiguous)
    }

    /// The child over `partitions` declaring `ordering` for its keys.
    fn plan_with_ordering(
        partitions: Vec<RawPartition>,
        ordering: SeriesOrdering,
    ) -> Arc<dyn ExecutionPlan> {
        let series_schema = raw_schema();
        let frame = common_query::series_frame::raw_series_frame_schema_with_ordering(
            &series_schema,
            &store_api::region_engine::RawLabelSource::Explicit,
            ordering,
        )
        .expect("a raw schema declares an explicit-label frame");
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(frame),
            Partitioning::UnknownPartitioning(partitions.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Arc::new(Self {
            partitions,
            series_schema,
            properties,
        })
    }
}

impl fmt::Debug for RawScripted {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawScripted")
            .field("partitions", &self.partitions.len())
            .finish()
    }
}

impl DisplayAs for RawScripted {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "RawScripted: partitions={}", self.partitions.len())
    }
}

impl ExecutionPlan for RawScripted {
    fn name(&self) -> &str {
        "RawScripted"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        assert!(children.is_empty(), "a raw scripted child has no children");
        Ok(self)
    }
    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let spec = self.partitions.get(partition).cloned().ok_or_else(|| {
            DataFusionError::Execution(format!("raw child has no partition {partition}"))
        })?;
        pack_raw_series(Box::pin(RawScript {
            chunks: spec.chunks.into_iter(),
            schema: Arc::clone(&self.series_schema),
            binding: store_api::region_engine::RawLabelSource::Explicit,
            gate: spec.gate,
            gate_chunks: spec.gate_chunks,
        }))
    }
}

/// The raw owner over `child` grouping by complete stored primary key.
fn raw_node(child: Arc<dyn ExecutionPlan>, outputs: usize) -> Arc<SeriesRepartitionExec> {
    Arc::new(
        SeriesRepartitionExec::try_new_with_routing(child, outputs, SeriesRouting::RawPrimaryKey)
            .expect("the raw owner is built"),
    )
}

/// Every chunk of `plan`'s one output, decoded, plus the error it ended with if any.
async fn drain_raw(
    plan: &Arc<SeriesRepartitionExec>,
    context: &Arc<TaskContext>,
) -> (Vec<RawSeriesChunk>, Option<BoxedError>) {
    let frames = plan
        .execute(0, Arc::clone(context))
        .expect("partition 0 exists");
    let mut decoded = unpack_raw_series(frames, Some(raw_schema())).expect("the frame decodes");
    let mut chunks = Vec::new();
    while let Some(next) = decoded.next().await {
        match next {
            Ok(chunk) => chunks.push(chunk),
            Err(error) => return (chunks, Some(error)),
        }
    }
    (chunks, None)
}

/// Every output partition of `plan`, decoded, and the error it ended with if it ended with one.
///
/// All partitions are read at once, as in normal execution. Draining them concurrently makes this
/// helper work for outputs that can publish before global EOF as well as outputs that cannot.
async fn drain(
    plan: &Arc<dyn ExecutionPlan>,
    context: &Arc<TaskContext>,
) -> Vec<Result<Vec<(Series, bool)>, BoxedError>> {
    let mut streams = Vec::new();
    for partition in 0..plan.properties().partitioning.partition_count() {
        let stream = plan
            .execute(partition, Arc::clone(context))
            .expect("the output executes");
        streams.push(
            unpack_evaluated_series(stream, None).expect("the output declares an evaluated frame"),
        );
    }
    futures::future::join_all(
        streams
            .into_iter()
            .map(|stream| stream.try_collect::<Vec<_>>()),
    )
    .await
}

/// The output partition of `plan` at `partition`, as the frame stream a consumer reads.
/// The evaluated view of one owner publication, for tests that drive the internal stream.
fn evaluated(output: ReadyOutput) -> (Series, bool) {
    match output {
        ReadyOutput::Evaluated(series) => (series, true),
        ReadyOutput::Raw(_) => panic!("an evaluated output carried a raw chunk"),
    }
}

fn output(
    plan: &Arc<dyn ExecutionPlan>,
    partition: usize,
    context: &Arc<TaskContext>,
) -> SendableEvaluatedSeriesStream {
    unpack_evaluated_series(
        plan.execute(partition, Arc::clone(context))
            .expect("the output executes"),
        None,
    )
    .expect("the output declares an evaluated frame")
}

/// The output partition of `plan` at `partition`, for a test that keeps the node itself.
///
/// [`execution`] reaches the read through the node rather than through the plan a consumer would hold,
/// so a test that has to look at the tasks behind an output claims it here instead of coercing the
/// node to the plan it is: the two are the same call, one on a reference the test already has.
fn output_of(
    plan: &Arc<SeriesRepartitionExec>,
    partition: usize,
    context: &Arc<TaskContext>,
) -> SendableEvaluatedSeriesStream {
    unpack_evaluated_series(
        plan.execute(partition, Arc::clone(context))
            .expect("the output executes"),
        None,
    )
    .expect("the output declares an evaluated frame")
}

/// The labels of a chunk in the form a failure message can show.
fn named(metric: &SeriesLabels) -> String {
    render_label_set(&label_set(metric))
}

/// What one chunk says, in the terms a test compares: its canonical label set, where its rows start,
/// how many there are, and whether it closed its series.
fn summary((series, is_last): &(Series, bool)) -> (String, Option<usize>, usize, bool) {
    (
        named(&series.metric),
        series.time.first_position(),
        series.time.len(),
        *is_last,
    )
}

/// The summaries of a whole output stream.
fn summaries(chunks: &[(Series, bool)]) -> Vec<(String, Option<usize>, usize, bool)> {
    chunks.iter().map(summary).collect()
}

/// Asserts that every chain `chunks` hands over is whole, in the terms the frame validator reads a
/// chain in.
///
/// A chunk that leaves a series open must be followed by the same series' next chunk, at a position
/// after the last one that chain carried. Another series' chunk in between is what a merge downstream
/// cannot repair, since it would have to hold two open series to read either; a position that does not
/// advance is a chunk the merge would have to reorder to read. Ending with a chain open fails too,
/// because that means the finality of a chunk was lost on the way, and a chunk of no rows fails unless
/// it is the closing chunk of the chain it belongs to - that being the shape a series ends in when
/// nothing of it is left.
///
/// Identity here is the label set the frame carries and not its rendering: two label sets that print
/// the same are still two series to a merge - an empty label value is not printed - so comparing the
/// displayed labels would accept a stream that changed identity mid-chain.
fn assert_chains_are_whole(chunks: &[(Series, bool)]) {
    // What the validator keeps of a chain it has not seen close: the identity it opened with, and the
    // last position it carried.
    let mut open: Option<(SeriesLabels, usize)> = None;
    for (series, is_last) in chunks {
        let (Some(first), Some(last)) = (series.time.first_position(), series.time.last_position())
        else {
            // No position, so the chunk carries no rows: only a closing chunk is shaped that way, and
            // it closes the chain it belongs to without advancing it.
            assert!(
                *is_last,
                "{} carries no positions and does not close its series",
                named(&series.metric)
            );
            if let Some((open_labels, _)) = &open {
                assert_eq!(
                    open_labels, &series.metric,
                    "another series' chunk interrupted an open chain"
                );
            }
            open = None;
            continue;
        };
        if let Some((open_labels, previous)) = &open {
            assert_eq!(
                open_labels, &series.metric,
                "another series' chunk interrupted an open chain"
            );
            assert!(
                first > *previous,
                "the positions of one chain did not advance across its chunks: {} starts at {first} \
                 after the chain's previous chunk ended at {previous}",
                named(&series.metric),
            );
        }
        open = (!*is_last).then(|| (series.metric.clone(), last));
    }
    assert!(open.is_none(), "the output ended with a chain open");
}

/// Checks completed owner outputs against exact series identities, positions, and finality.
fn assert_output_contract(chunks: &[(Series, bool)], expected: &[(SeriesLabels, Vec<usize>)]) {
    assert_chains_are_whole(chunks);
    let mut actual: Vec<(SeriesLabels, Vec<usize>, usize)> = Vec::new();
    let mut closed_identities = Vec::<SeriesLabels>::new();
    for (series, is_last) in chunks {
        assert!(
            !closed_identities.contains(&series.metric),
            "a closed full identity must not reappear: {}",
            named(&series.metric)
        );
        assert_eq!(series.time.len(), series.time.positions().count());
        let (_, positions, closes) = match actual
            .iter_mut()
            .find(|(metric, _, _)| metric == &series.metric)
        {
            Some(entry) => entry,
            None => {
                actual.push((series.metric.clone(), Vec::new(), 0));
                actual.last_mut().unwrap()
            }
        };
        positions.extend(series.time.positions().map(|(position, _)| position));
        *closes += usize::from(*is_last);
        if *is_last {
            closed_identities.push(series.metric.clone());
        }
    }
    assert_eq!(
        expected.len(),
        actual.len(),
        "one closed series per full identity"
    );
    for (metric, positions) in expected {
        let index = actual
            .iter()
            .position(|(identity, _, _)| identity == metric)
            .expect("expected exact full label identity");
        let (_, actual_positions, closes) = actual.remove(index);
        assert_eq!(
            positions,
            &actual_positions,
            "positions for {}",
            named(metric)
        );
        assert_eq!(1, closes, "one terminal output close for {}", named(metric));
    }
    assert!(actual.is_empty());
}

/// A label set whose output is not `other`'s, found rather than assumed.
fn labels_elsewhere(routing: &Routing, other: &SeriesLabels) -> SeriesLabels {
    let taken = routing.output(other);
    for index in 0..64 {
        let candidate = labels(&format!("elsewhere-{index}"));
        if routing.output(&candidate) != taken {
            return candidate;
        }
    }
    panic!("no label set of these tests routes to another output");
}

/// A node with no outputs, or over a child that does not declare a series frame, is refused before
/// the plan can run.
#[test]
fn construction_rejects_a_zero_output_count_and_a_plain_child() {
    let none = SeriesRepartitionExec::try_new(Scripted::plan(vec![Vec::new()]), 0)
        .expect_err("a node has to have an output");
    assert!(
        none.to_string().contains("at least one output partition"),
        "{none}"
    );

    let plain: Arc<dyn ExecutionPlan> = Arc::new(prepare_test_data());
    let refused = SeriesRepartitionExec::try_new(plain, 2)
        .expect_err("a plain record batch stream carries no series labels");
    assert!(
        refused.to_string().contains("is not a series frame"),
        "{refused}"
    );
}

/// T1: two partitions each hold a piece of one canonical label set, and both pieces reach the output
/// that label set hashes to; another label set reaches the output its own hash names.
#[tokio::test]
async fn pieces_of_one_label_set_meet_on_one_output() {
    let routing = Routing::new(3);
    let shared = labels("web-1");
    let other = labels_elsewhere(&routing, &shared);
    assert_ne!(
        routing.output(&shared),
        routing.output(&other),
        "the two label sets have to be able to land apart for this test to mean anything"
    );
    // One label set split over the child's two partitions: the first piece opens and closes on its
    // own, the second continues at a later position. Neither piece alone is the series.
    let child = Scripted::plan(vec![
        vec![
            Step::chunk(chunk(shared.clone(), 0, 2, false)),
            Step::chunk(chunk(shared.clone(), 0, 0, true)),
            Step::chunk(chunk(other.clone(), 0, 1, true)),
        ],
        vec![Step::chunk(chunk(shared.clone(), 8, 2, true))],
    ]);
    let plan: Arc<dyn ExecutionPlan> = node(child, 3);
    assert_eq!(3, plan.properties().partitioning.partition_count());

    let outputs = drain(&plan, &SessionContext::default().task_ctx()).await;
    assert_eq!(3, outputs.len());
    let outputs = outputs
        .into_iter()
        .map(|chunks| {
            let chunks = chunks.expect("every output decodes");
            assert_chains_are_whole(&chunks);
            summaries(&chunks)
        })
        .collect::<Vec<_>>();

    let other_piece = named(&other);
    let shared_output = routing.output(&shared);
    let shared_chunks = outputs[shared_output]
        .iter()
        .map(|summary| summary.2)
        .sum::<usize>();
    assert_eq!(
        4, shared_chunks,
        "both producer partitions contribute positions"
    );
    assert_eq!(
        1,
        outputs[shared_output]
            .iter()
            .filter(|chunk| chunk.3)
            .count()
    );
    assert_eq!(
        vec![(other_piece, Some(0), 1, true)],
        outputs[routing.output(&other)],
        "another full identity lands on its own output"
    );
    let empty = outputs
        .iter()
        .enumerate()
        .filter(|(partition, chunks)| *partition != shared_output && chunks.is_empty())
        .map(|(partition, _)| partition)
        .collect::<Vec<_>>();
    assert_eq!(
        1,
        empty.len(),
        "the third output receives neither label set"
    );
}

/// T2: two chains of one label set cross the node whole, in the order the read reached them, and a
/// closing chunk that carries no row survives the round trip.
#[tokio::test]
async fn chains_cross_the_node_whole_and_a_closing_chunk_survives() {
    let routing = Routing::new(2);
    let shared = labels("web-1");
    let other = labels_elsewhere(&routing, &shared);
    // Both of these chains hash to the same output, so a node that routed chunk by chunk without
    // reading one chain to its end would interleave them there - which is the failure this test is
    // built to see.
    let child = Scripted::plan(vec![
        vec![
            Step::chunk(chunk(shared.clone(), 0, 2, false)),
            Step::chunk(chunk(shared.clone(), 2, 2, true)),
            Step::chunk(chunk(other.clone(), 0, 1, true)),
        ],
        vec![
            Step::chunk(chunk(shared.clone(), 8, 2, true)),
            Step::chunk(chunk(other.clone(), 0, 0, true)),
        ],
    ]);
    let plan: Arc<dyn ExecutionPlan> = node(child, 2);

    let outputs = drain(&plan, &SessionContext::default().task_ctx()).await;
    let shared_output = routing.output(&shared);
    let chunks = outputs
        .into_iter()
        .nth(shared_output)
        .expect("an output per partition")
        .expect("the output decodes");
    assert_chains_are_whole(&chunks);

    assert_output_contract(&chunks, &[(shared.clone(), vec![0, 1, 2, 3, 8, 9])]);
}

/// An unread output does not prevent routing to another owner, and a completed series is readable
/// while a later input chain is gated.
#[tokio::test]
async fn an_unread_owner_does_not_stall_routing_and_outputs_publish_incrementally() {
    let routing = Routing::new(2);
    let shared = labels("web-1");
    let other = labels_elsewhere(&routing, &shared);
    let stalled = routing.output(&shared);
    let moving = routing.output(&other);
    // More chains than one queue can hold, so the producer has to stop mid-read rather than finish.
    let chains = TEST_FIXTURE_VOLUME + 2;
    let completed = (0..chains)
        .map(|index| labels(&format!("unread-{index}")))
        .filter(|metric| routing.output(metric) == stalled)
        .collect::<Vec<_>>();
    assert!(
        completed.len() > 1,
        "enough completed identities route to the stalled owner"
    );
    let mut steps = completed
        .iter()
        .enumerate()
        .map(|(index, metric)| Step::chunk(chunk(metric.clone(), index, 1, true)))
        .collect::<Vec<_>>();
    steps.push(Step::chunk(chunk(other.clone(), 0, 1, true)));
    let switch = Arc::new(Switch::default());
    let child = Gated::plan(
        vec![GatedPartition {
            before: steps,
            after: Vec::new(),
            switch: None,
        }],
        Arc::clone(&switch),
        Arc::new(AtomicUsize::new(0)),
    );
    let concrete_plan = node(child, 2);
    let plan: Arc<dyn ExecutionPlan> = concrete_plan.clone();
    let context = SessionContext::default().task_ctx();

    let mut moving_stream = output(&plan, moving, &context);
    let stalled_stream = output(&plan, stalled, &context);
    let execution = execution(&concrete_plan);
    // The source holds terminal EOF behind the switch. Thus the producer cannot finish early merely
    // because this finite input happened to reach its legitimate end.
    assert!(!switch.is_open());
    let moving_chunk = tokio::time::timeout(Duration::from_secs(10), moving_stream.next())
        .await
        .expect("completed moving identity arrives before gated EOF")
        .expect("moving identity exists")
        .expect("moving output succeeds");
    assert_eq!(other, moving_chunk.0.metric);
    assert!(moving_chunk.1);
    assert!(
        futures::future::poll_fn(|cx| match moving_stream.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Ready(true),
            Poll::Ready(_) => Poll::Ready(false),
        })
        .await,
        "no second identity is ready before the gate"
    );
    assert!(
        !producer_stopped(&execution),
        "terminal EOF gate remains closed"
    );
    switch.open();
    wait_until("unbounded producer finishes after terminal EOF", || {
        producer_stopped(&execution)
    })
    .await;
    // The unread owner's completed identities remain queued while the moving output was drained.
    let stalled_chunks = tokio::time::timeout(
        Duration::from_secs(10),
        stalled_stream.try_collect::<Vec<_>>(),
    )
    .await
    .expect("stalled owner publishes after global EOF")
    .expect("the stalled output decodes");
    let expected_stalled = completed
        .iter()
        .enumerate()
        .map(|(index, metric)| (metric.clone(), vec![index]))
        .collect::<Vec<_>>();
    assert_output_contract(&stalled_chunks, &expected_stalled);

    let moving_rest = tokio::time::timeout(
        Duration::from_secs(10),
        moving_stream.try_collect::<Vec<_>>(),
    )
    .await
    .expect("the moving output ends after global EOF")
    .expect("the moving output decodes");
    assert!(
        moving_rest.is_empty(),
        "completed identity is emitted only once"
    );
}

/// An exactly full output frame releases its completed identity before the next poll yields.
#[tokio::test]
async fn exact_full_frame_releases_completed_identity_while_other_chains_stay_open() {
    let metric = labels("exact-full-frame");
    let a_bytes = (0..GRID_LEN as usize)
        .map(|position| {
            operand_chunk_bytes(
                &chunk(
                    metric.clone(),
                    position,
                    1,
                    position + 1 == GRID_LEN as usize,
                )
                .0,
            )
        })
        .sum::<usize>();
    let held_b = labels("exact-full-held-b");
    let held_c = labels("exact-full-held-c");
    let held_bytes = operand_chunk_bytes(&chunk(held_b.clone(), 0, 2, false).0)
        + operand_chunk_bytes(&chunk(held_c.clone(), 0, 2, false).0);
    let switch = Arc::new(Switch::default());
    let ranges = [(0, 86), (86, 171), (171, GRID_LEN as usize)];
    let partitions = ranges
        .into_iter()
        .enumerate()
        .map(|(partition, (start, end))| {
            let mut before = (start..end)
                .map(|position| {
                    Step::chunk(chunk(metric.clone(), position, 1, position + 1 == end))
                })
                .collect::<Vec<_>>();
            let held = match partition {
                1 => &held_b,
                2 => &held_c,
                _ => return GatedPartition::open(before),
            };
            before.push(Step::chunk(chunk(held.clone(), 0, 2, false)));
            GatedPartition {
                before,
                after: vec![Step::chunk(chunk(held.clone(), 2, 1, true))],
                switch: None,
            }
        })
        .collect();
    let child = Gated::plan(
        partitions,
        Arc::clone(&switch),
        Arc::new(AtomicUsize::new(0)),
    );
    let plan = node(child, 1);
    let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
    let context = task_context(&pool);
    let mut frames = plan.execute(0, Arc::clone(&context)).unwrap();
    wait_until("A and the open B/C prefixes are retained", || {
        pool.reserved() >= a_bytes + held_bytes
    })
    .await;
    let reserved_before = pool.reserved();
    let first = futures::future::poll_fn(|cx| frames.as_mut().poll_next(cx))
        .await
        .expect("the exact-full public frame arrives")
        .expect("the frame succeeds");
    assert_eq!(256, first.num_rows());
    assert!(
        futures::future::poll_fn(|cx| match frames.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Ready(true),
            Poll::Ready(_) => Poll::Ready(false),
        })
        .await,
        "with B/C still open, the next poll waits rather than requiring a later frame"
    );
    assert!(
        pool.reserved() < reserved_before,
        "completed A charge is released"
    );
    assert!(
        pool.reserved() >= held_bytes,
        "open B/C payload charges remain retained"
    );
    switch.open();
    drop(frames);
    wait_until("dropping the stream releases remaining charges", || {
        pool.reserved() == 0
    })
    .await;
}

/// An absent identity contribution waits for that input's normal EOF, not another identity's close.
#[tokio::test]
async fn absent_identity_waits_for_normal_eof_before_publishing_other_input_chain() {
    let metric = labels_routed_to(0, 2).remove(0);
    let other = labels_routed_to(1, 2).remove(0);
    let input_zero = Arc::new(Switch::default());
    let input_one = Arc::new(Switch::default());
    let child = Gated::plan(
        vec![
            GatedPartition {
                before: vec![Step::chunk(chunk(metric.clone(), 0, 2, true))],
                after: vec![Step::chunk(chunk(other.clone(), 0, 1, true))],
                switch: Some(Arc::clone(&input_zero)),
            },
            GatedPartition {
                before: vec![Step::chunk(chunk(other.clone(), 2, 1, true))],
                after: Vec::new(),
                switch: Some(Arc::clone(&input_one)),
            },
        ],
        Arc::new(Switch::default()),
        Arc::new(AtomicUsize::new(0)),
    );
    let plan = node(child, 2);
    let context = SessionContext::default().task_ctx();
    let mut stream = output_of(&plan, 0, &context);
    assert!(
        futures::future::poll_fn(|cx| match stream.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Ready(true),
            Poll::Ready(_) => Poll::Ready(false),
        })
        .await,
        "A is not ready while input 1 has contributed only another identity and remains open"
    );
    input_one.open();
    let mut chunks = Vec::new();
    loop {
        let next = tokio::time::timeout(Duration::from_secs(10), async {
            futures::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await
        })
        .await
        .expect("input 1 EOF releases A while input 0 remains gated");
        match next {
            Some(Ok(chunk)) => {
                let done = chunk.1;
                chunks.push(chunk);
                if done {
                    break;
                }
            }
            Some(Err(error)) => panic!("A fails: {error}"),
            None => panic!("output ended while input 0 remains open"),
        }
    }
    assert_output_contract(&chunks, &[(metric, vec![0, 1])]);
    assert!(!input_zero.is_open(), "input 0 remains independently gated");
    input_zero.open();
    let (rest, errors) = tokio::time::timeout(Duration::from_secs(10), until_end(&mut stream))
        .await
        .expect("the remaining input finishes after its independent release");
    assert!(errors.is_empty(), "remaining output succeeds: {errors:?}");
    assert_chains_are_whole(&rest);
}

/// A complete identity can be read from the public output while every input remains live.
#[tokio::test]
async fn completed_identity_is_published_before_inputs_finish() {
    let metric = labels("early-series");
    let routing = Routing::new(2);
    let switch = Arc::new(Switch::default());
    let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
    let context = task_context(&pool);
    let fragments = (0..3)
        .map(|input| {
            vec![
                Step::chunk(chunk(metric.clone(), input * 10, 1, false)),
                Step::chunk(chunk(metric.clone(), input * 10 + 1, 1, true)),
            ]
        })
        .collect::<Vec<_>>();
    let partitions = fragments
        .into_iter()
        .enumerate()
        .map(|(input, before)| {
            let held = labels(&format!("held-{input}"));
            GatedPartition {
                before: {
                    let mut before = before;
                    before.push(Step::chunk(chunk(held.clone(), 40, 2, false)));
                    before
                },
                after: vec![Step::chunk(chunk(held, 42, 1, true))],
                switch: None,
            }
        })
        .collect::<Vec<_>>();
    let child = Gated::plan(
        partitions,
        Arc::clone(&switch),
        Arc::new(AtomicUsize::new(0)),
    );
    let plan = node(child, 2);
    let owner = routing.output(&metric);
    let mut stream = output_of(&plan, owner, &context);
    let a_bytes = (0..3)
        .flat_map(|input| [input * 10, input * 10 + 1])
        .map(|position| operand_chunk_bytes(&chunk(metric.clone(), position, 1, true).0))
        .sum::<usize>();
    let held_bytes = (0..3)
        .map(|input| operand_chunk_bytes(&chunk(labels(&format!("held-{input}")), 40, 2, false).0))
        .sum::<usize>();
    assert!(held_bytes >= 3 * operand_chunk_bytes(&chunk(labels("held-0"), 40, 2, false).0));
    wait_until(
        "completed A and open held fragments are retained before handoff",
        || pool.reserved() >= a_bytes + held_bytes,
    )
    .await;
    let reserved_before_a = pool.reserved();

    let mut received = Vec::new();
    let first = tokio::time::timeout(Duration::from_secs(10), async {
        futures::future::poll_fn(|cx| match stream.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(chunk))) => Poll::Ready(Ok(chunk)),
            Poll::Ready(Some(Err(error))) => Poll::Ready(Err(error)),
            Poll::Ready(None) => Poll::Ready(Err(boxed(DataFusionError::Execution(
                "output ended before the gated inputs".into(),
            )))),
        })
        .await
    })
    .await
    .expect("completed identity is delivered while inputs are gated")
    .expect("output succeeds");
    received.push(first);
    while !received
        .last()
        .is_some_and(|(series, is_last)| series.metric == metric && *is_last)
    {
        let next = tokio::time::timeout(Duration::from_secs(10), async {
            futures::future::poll_fn(|cx| match stream.as_mut().poll_next(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Some(Ok(chunk))) => Poll::Ready(Ok(chunk)),
                Poll::Ready(Some(Err(error))) => Poll::Ready(Err(error)),
                Poll::Ready(None) => Poll::Ready(Err(boxed(DataFusionError::Execution(
                    "output ended before the gated inputs".into(),
                )))),
            })
            .await
        })
        .await
        .expect("completed identity continues to arrive")
        .expect("output succeeds");
        received.push(next);
    }
    assert_output_contract(&received, &[(metric.clone(), vec![0, 1, 10, 11, 20, 21])]);
    assert!(!switch.is_open(), "later identities are still blocked");
    assert!(
        pool.reserved() < reserved_before_a,
        "handing off A releases its per-identity charge: before {}, after {}",
        reserved_before_a,
        pool.reserved()
    );
    assert!(
        pool.reserved() >= held_bytes,
        "the held identities retain their independent payload charge: expected at least {held_bytes}, got {}",
        pool.reserved()
    );

    switch.open();
    let (rest, errors) = tokio::time::timeout(Duration::from_secs(10), until_end(&mut stream))
        .await
        .expect("gated inputs finish");
    assert!(errors.is_empty(), "later output succeeds: {errors:?}");
    received.extend(rest);
    assert_chains_are_whole(&received);
}

/// A one-fragment large identity needs no merge workspace and fits its own finite payload budget.
#[tokio::test]
async fn single_fragment_large_identity_fits_finite_payload_budget() {
    let metric = labels("single-fragment-large");
    let long_grid = Arc::new(
        EvalGrid::try_new(GridSpec::try_new(GRID_START, GRID_STEP, 1_000).unwrap()).unwrap(),
    );
    let long_schema = Arc::new(SeriesSchema {
        fields: Arc::from([FieldSpec {
            name: Arc::from("value"),
        }]),
        eval_grid: Some(Arc::clone(&long_grid)),
    });
    let time = TimeAxis::from_grid_spans(long_grid, vec![GridSpan::new(0, 1_000)])
        .expect("one run on the long grid");
    let fields = SeriesFields::one(Samples::from_float(Float64Array::from(
        (0..1_000)
            .map(|value| Some(value as f64))
            .collect::<Vec<_>>(),
    )));
    let series = Series::try_new(&long_schema, metric.clone(), time, fields)
        .expect("one large float series chunk");
    let payload = operand_chunk_bytes(&series);
    let budget = payload + 8 * 1024;
    let workspace = merge_workspace_bytes(1_000, 1, 1);
    assert!(
        workspace > budget,
        "estimated workspace must exceed payload budget: workspace={workspace}, budget={budget}"
    );
    let pool = Arc::new(GreedyMemoryPool::new(budget));
    let context = task_context(&pool);
    let plan = node(
        Scripted::plan_with_schema(vec![vec![Step::Chunk(series, true)]], long_schema),
        1,
    );
    let mut output = output_of(&plan, 0, &context);
    let (chunks, errors) = tokio::time::timeout(Duration::from_secs(10), until_end(&mut output))
        .await
        .expect("single-fragment identity fits its finite pool");
    assert!(
        errors.is_empty(),
        "single fragment needs no merge workspace: {errors:?}"
    );
    assert_eq!(1, chunks.len());
    let positions = chunks[0]
        .0
        .time
        .positions()
        .map(|(position, _)| position)
        .collect::<Vec<_>>();
    assert_eq!((0..1_000).collect::<Vec<_>>(), positions);
    assert_eq!(metric, chunks[0].0.metric);
    assert!(chunks[0].1);
    assert!(payload < budget);
    assert_eq!(
        0,
        pool.reserved(),
        "delivery releases the single-fragment charge"
    );
}

/// An input EOF already consumed still counts when a different input first contributes an identity.
#[tokio::test]
async fn eof_before_first_identity_contribution_makes_group_ready() {
    let metric = labels("late-first-chain");
    let switch = Arc::new(Switch::default());
    let child = Gated::plan(
        vec![
            GatedPartition::open(Vec::new()),
            GatedPartition::held(vec![Step::chunk(chunk(metric.clone(), 7, 1, true))]),
        ],
        Arc::clone(&switch),
        Arc::new(AtomicUsize::new(0)),
    );
    let plan = node(child, 1);
    let context = SessionContext::default().task_ctx();
    let mut stream = output_of(&plan, 0, &context);
    assert!(
        futures::future::poll_fn(|cx| match stream.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Ready(true),
            Poll::Ready(_) => Poll::Ready(false),
        })
        .await,
        "output waits for the not-yet-arrived identity"
    );
    switch.open();
    let (chunks, errors) = tokio::time::timeout(Duration::from_secs(10), until_end(&mut stream))
        .await
        .expect("late first chain is processed after the other input EOF");
    assert!(errors.is_empty(), "output succeeds: {errors:?}");
    assert_output_contract(&chunks, &[(metric, vec![7])]);
}

/// T3, second half: what the producer queues is charged to the execution's pool, and dropping the
/// execution releases it.
#[tokio::test]
async fn dropping_the_execution_releases_what_the_producer_reserved() {
    let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
    let context = task_context(&pool);
    let shared = labels("web-1");
    let child = Scripted::plan(vec![vec![
        Step::chunk(chunk(shared.clone(), 0, 1, false)),
        Step::chunk(chunk(shared.clone(), 1, 1, false)),
        Step::chunk(chunk(shared.clone(), 2, 1, true)),
    ]]);
    let plan: Arc<dyn ExecutionPlan> = node(child, 2);
    let stream = plan
        .execute(0, Arc::clone(&context))
        .expect("the first output starts the read");

    // The producer reads ahead into the queue, and what it holds is charged to the pool.
    let held = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let reserved = pool.reserved();
            if reserved > 0 {
                return reserved;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the producer charges what it queues");
    assert!(
        held > 0,
        "the queued chain has a nonzero reservation: {held}"
    );

    drop(stream);
    drop(plan);
    tokio::time::timeout(Duration::from_secs(10), async {
        while pool.reserved() != 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "the dropped execution still holds {} bytes",
            pool.reserved()
        )
    });
}

/// T5: one output is the same node, not a special case - every chunk arrives, in one chain after
/// another, with nothing filtered out.
#[tokio::test]
async fn one_output_keeps_every_chunk_and_its_chains_whole() {
    let shared = labels("web-1");
    let other = labels("elsewhere-1");
    let child = Scripted::plan(vec![
        vec![
            Step::chunk(chunk(shared.clone(), 0, 2, false)),
            Step::chunk(chunk(shared.clone(), 2, 2, true)),
            Step::chunk(chunk(other.clone(), 0, 1, false)),
            Step::chunk(chunk(other.clone(), 0, 0, true)),
        ],
        vec![Step::chunk(chunk(shared.clone(), 8, 2, true))],
    ]);
    let plan: Arc<dyn ExecutionPlan> = node(child, 1);
    assert_eq!(1, plan.properties().partitioning.partition_count());

    let outputs = drain(&plan, &SessionContext::default().task_ctx()).await;
    assert_eq!(1, outputs.len());
    let chunks = outputs
        .into_iter()
        .next()
        .expect("one output")
        .expect("the output decodes");
    assert_chains_are_whole(&chunks);
    assert_output_contract(
        &chunks,
        &[
            (shared.clone(), vec![0, 1, 2, 3, 8, 9]),
            (other.clone(), vec![0]),
        ],
    );
    assert_eq!(2, chunks.iter().filter(|(_, closed)| *closed).count());
}

/// A switch a test opens to let gated input streams produce.
///
/// Opening it wakes the streams already parked, and a stream polled after the opening sees it
/// straight away: registering before the second read is what makes an opening that lands between the
/// two impossible to lose.
#[derive(Default)]
struct Switch {
    open: AtomicBool,
    wakers: Mutex<Vec<Waker>>,
}

impl Switch {
    /// Lets every stream waiting on this switch produce, and wakes the ones already parked.
    fn open(&self) {
        self.open.store(true, Ordering::Release);
        let wakers = std::mem::take(&mut *self.wakers.lock().unwrap());
        for waker in wakers {
            waker.wake();
        }
    }

    fn is_open(&self) -> bool {
        self.open.load(Ordering::Acquire)
    }

    /// Parks `cx` until the switch is opened, unless it already is.
    fn wait(&self, cx: &Context<'_>) {
        if self.is_open() {
            return;
        }
        self.wakers.lock().unwrap().push(cx.waker().clone());
        if self.is_open() {
            // The opening landed between the two reads, so its wake did not find this waker.
            cx.waker().wake_by_ref();
        }
    }
}

/// One partition of a [`Gated`] child: what it hands over at once, and what it holds back.
/// `switch` overrides the child's shared gate when a test needs independent input progress.
#[derive(Clone, Default)]
struct GatedPartition {
    before: Vec<Step>,
    after: Vec<Step>,
    switch: Option<Arc<Switch>>,
}

impl GatedPartition {
    /// A partition that hands over `steps` only once the switch is opened.
    fn held(steps: Vec<Step>) -> Self {
        Self {
            before: Vec::new(),
            after: steps,
            switch: None,
        }
    }

    /// A partition that hands over `steps` as soon as it is polled.
    fn open(steps: Vec<Step>) -> Self {
        Self {
            before: steps,
            after: Vec::new(),
            switch: None,
        }
    }
}

/// A child whose partitions hold their second half back until `switch` is opened.
///
/// The gate is how a test states that the read starts only after the plan is gone: the input is a
/// real source that has not produced yet, not one that has already answered before the interesting
/// moment. `dropped` counts the streams a consumer let go of, which is how a test sees the read
/// cancelling the child it was reading.
struct Gated {
    partitions: Vec<GatedPartition>,
    switch: Arc<Switch>,
    dropped: Arc<AtomicUsize>,
    series_schema: Arc<SeriesSchema>,
    properties: Arc<PlanProperties>,
}

impl Gated {
    /// The child over `partitions`, gated on `switch` and counting drops in `dropped`.
    fn plan(
        partitions: Vec<GatedPartition>,
        switch: Arc<Switch>,
        dropped: Arc<AtomicUsize>,
    ) -> Arc<dyn ExecutionPlan> {
        Self::plan_with_schema(partitions, switch, dropped, schema())
    }

    /// The same child over an explicit schema, for trees with more than one field.
    fn plan_with_schema(
        partitions: Vec<GatedPartition>,
        switch: Arc<Switch>,
        dropped: Arc<AtomicUsize>,
        series_schema: Arc<SeriesSchema>,
    ) -> Arc<dyn ExecutionPlan> {
        let frame = pack_evaluated_series(
            Box::pin(futures::stream::empty()),
            Arc::clone(&series_schema),
        )
        .expect("an evaluated schema declares a frame")
        .schema();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(frame),
            Partitioning::UnknownPartitioning(partitions.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Arc::new(Self {
            partitions,
            switch,
            dropped,
            series_schema,
            properties,
        })
    }
}

impl fmt::Debug for Gated {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Gated")
            .field("partitions", &self.partitions.len())
            .finish()
    }
}

impl DisplayAs for Gated {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "Gated: partitions={}", self.partitions.len())
    }
}

impl ExecutionPlan for Gated {
    fn name(&self) -> &str {
        "Gated"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        assert!(children.is_empty(), "a gated child has no children");
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let steps = self.partitions.get(partition).cloned().ok_or_else(|| {
            DataFusionError::Execution(format!("gated child has no partition {partition}"))
        })?;
        let switch = steps.switch.unwrap_or_else(|| Arc::clone(&self.switch));
        pack_evaluated_series(
            Box::pin(GatedScript {
                before: steps.before.into_iter(),
                after: steps.after.into_iter(),
                switch,
                dropped: Arc::clone(&self.dropped),
            }),
            Arc::clone(&self.series_schema),
        )
    }
}

/// One partition of a [`Gated`] child.
struct GatedScript {
    before: std::vec::IntoIter<Step>,
    after: std::vec::IntoIter<Step>,
    switch: Arc<Switch>,
    dropped: Arc<AtomicUsize>,
}

impl Stream for GatedScript {
    type Item = Result<(Series, bool), BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        if let Some(step) = this.before.next() {
            return Poll::Ready(Some(step_answer(step)));
        }
        // Nothing of the second half is handed over before the test opens the switch, so the read
        // can be started and then left waiting on a source that has not produced anything yet.
        this.switch.wait(cx);
        if !this.switch.is_open() {
            return Poll::Pending;
        }
        Poll::Ready(this.after.next().map(step_answer))
    }
}

impl Drop for GatedScript {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

/// The tokio task that first polled each partition of a [`Recording`] child.
type ReadTasks = Arc<Mutex<Vec<Option<tokio::task::Id>>>>;

/// A child whose partitions answer scripted steps, recording the task that first polls each one.
///
/// A partition count says how many parts a plan declares, not how many tasks compute them: one task
/// polling every partition in turn declares the same number. What is recorded here is the task
/// itself, read from [`tokio::task::try_id`] inside the poll - which is where a child partition is
/// really read, and hence what the partition count actually bought.
struct Recording {
    steps: Vec<Vec<Step>>,
    polled: ReadTasks,
    series_schema: Arc<SeriesSchema>,
    properties: Arc<PlanProperties>,
}

impl Recording {
    /// The child over `steps`, one partition per list, and the record of which task polled each.
    fn plan(steps: Vec<Vec<Step>>) -> (Arc<dyn ExecutionPlan>, ReadTasks) {
        let polled: ReadTasks = Arc::new(Mutex::new(vec![None; steps.len()]));
        let series_schema = schema();
        let frame = pack_evaluated_series(
            Box::pin(futures::stream::empty()),
            Arc::clone(&series_schema),
        )
        .expect("an evaluated schema declares a frame")
        .schema();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(frame),
            Partitioning::UnknownPartitioning(steps.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        let child = Arc::new(Self {
            steps,
            polled: Arc::clone(&polled),
            series_schema,
            properties,
        });
        (child, polled)
    }
}

impl fmt::Debug for Recording {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Recording")
            .field("partitions", &self.steps.len())
            .finish()
    }
}

impl DisplayAs for Recording {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "Recording: partitions={}", self.steps.len())
    }
}

impl ExecutionPlan for Recording {
    fn name(&self) -> &str {
        "Recording"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        assert!(children.is_empty(), "a recording child has no children");
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let steps = self.steps.get(partition).cloned().ok_or_else(|| {
            DataFusionError::Execution(format!("recording child has no partition {partition}"))
        })?;
        pack_evaluated_series(
            Box::pin(RecordingScript {
                partition,
                steps: steps.into_iter(),
                polled: Arc::clone(&self.polled),
                recorded: false,
            }),
            Arc::clone(&self.series_schema),
        )
    }
}

/// One partition of a [`Recording`] child.
struct RecordingScript {
    partition: usize,
    steps: std::vec::IntoIter<Step>,
    polled: ReadTasks,
    /// Whether this partition's first poll has been recorded.
    recorded: bool,
}

impl Stream for RecordingScript {
    type Item = Result<(Series, bool), BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        // Recorded on the first poll rather than in `execute`: `execute` may run on the consumer's
        // task while the reading is what another task does.
        if !this.recorded {
            this.recorded = true;
            let mut polled = this
                .polled
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            polled[this.partition] = tokio::task::try_id();
        }
        Poll::Ready(this.steps.next().map(step_answer))
    }
}

/// Waits until `done` holds, so a test states a condition rather than a duration.
async fn wait_until(what: &str, done: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting until {what}"));
}

/// The read a node started, for a test that has to look at the task behind the outputs.
fn execution(plan: &Arc<SeriesRepartitionExec>) -> Arc<SharedExecution> {
    let state = plan.shared.lock().unwrap();
    Arc::clone(state.as_ref().expect("the read was started by execute"))
}

/// Whether every input task of the read has stopped.
///
/// There is one task per input partition and no single producer, so "the read stopped" is what has to
/// be read off the tasks themselves: none of them is still running.
fn producer_stopped(execution: &SharedExecution) -> bool {
    execution.running.lock().unwrap().inputs.live() == 0
}

/// The error `stream` reports after the batches it delivers before it, or `None` if it ends.
async fn terminal_error(stream: &mut SendableRecordBatchStream) -> Option<DataFusionError> {
    loop {
        match std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
            None => return None,
            Some(Ok(_)) => {}
            Some(Err(error)) => return Some(error),
        }
    }
}

async fn evaluated_terminal_error(
    stream: &mut SendableEvaluatedSeriesStream,
) -> Option<BoxedError> {
    loop {
        match std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
            None => return None,
            Some(Ok(_)) => {}
            Some(Err(error)) => return Some(error),
        }
    }
}

/// Everything `stream` hands over until it ends: the chunks it delivered and the errors it reported.
///
/// Reading past the first error is what a test of the failure path needs. An error item is terminal,
/// so a stream that reports one and then answers again is what these tests are looking for - and so
/// is one that ends without ever reporting one, which is the clean end a failed read must not produce.
/// It also assumes the stream does end: a failure that leaves an output waiting for a chunk that will
/// never come is a hang, and the caller's timeout is what turns that into a failure.
async fn until_end(
    stream: &mut SendableEvaluatedSeriesStream,
) -> (Vec<(Series, bool)>, Vec<String>) {
    let mut chunks = Vec::new();
    let mut errors = Vec::new();
    loop {
        match std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
            None => return (chunks, errors),
            Some(Ok(chunk)) => chunks.push(chunk),
            Some(Err(error)) => errors.push(error.to_string()),
        }
    }
}

/// the read drives each input partition on a task of its own.
///
/// The child records the task that first polls each of its partitions, and what is asserted is that
/// the two partitions were polled by two distinct tasks, neither of them the consumer's. That is the
/// property the node provides - one task per child partition - and it is what a partition count on
/// its own cannot say: one task polling every partition in turn declares the same count.
///
/// Two distinct tasks is deliberately not a claim about simultaneous wall-clock execution: the two
/// tasks may share a worker thread and be polled one after the other, and the read is correct either
/// way. Nor does the assertion need two threads to be free at the same instant, which is what ties it
/// to the scheduling of the runtime the read runs on rather than to the read. What a read that drove
/// the partitions from one task would record instead is one id for both, or the consumer's own.
#[tokio::test]
async fn every_input_partition_is_read_on_a_task_of_its_own() {
    let shared = labels("web-1");
    let (child, polled) = Recording::plan(vec![
        vec![Step::chunk(chunk(shared.clone(), 0, 1, true))],
        vec![Step::chunk(chunk(shared.clone(), 4, 1, true))],
    ]);
    let plan: Arc<dyn ExecutionPlan> = node(child, 1);
    // Taken before anything is read: the task that asks for an output is the one a partition must
    // not be read on.
    let consumer = tokio::task::try_id();
    let outputs = tokio::time::timeout(
        Duration::from_secs(10),
        drain(&plan, &SessionContext::default().task_ctx()),
    )
    .await
    .expect("both partitions of the child answer, so the output ends");

    let recorded = polled
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clone();
    assert_eq!(
        2,
        recorded.len(),
        "the child holds one partition per scripted list"
    );
    let tasks = recorded
        .iter()
        .map(|task| task.unwrap_or_else(|| panic!("a child partition was never polled")))
        .collect::<Vec<_>>();
    assert_ne!(
        tasks[0], tasks[1],
        "the two partitions were polled by two different tasks, so neither was read on the task \
         that read the other: {tasks:?}"
    );
    assert!(
        tasks.iter().all(|task| Some(*task) != consumer),
        "neither partition ran on the consumer's task, so the read stayed on the tasks the node \
         started for the child: {tasks:?} against {consumer:?}"
    );

    let mut chunks = outputs
        .into_iter()
        .next()
        .expect("one output")
        .expect("the output decodes");
    chunks.sort_by_key(|(series, _)| series.time.first_position());
    assert_output_contract(&chunks, &[(shared.clone(), vec![0, 4])]);
    assert_eq!(1, chunks.iter().filter(|(_, closed)| *closed).count());
}

/// the streams own the read, so dropping the plan does not truncate them.
///
/// The input produces nothing at all until the switch is opened, and the switch is opened only after
/// every plan reference is gone. Before that, an output is polled once and delivers nothing, which is
/// what makes the read that follows a read of a plan that no longer exists: if dropping the plan had
/// aborted the producer, the outputs would end here - the answer would be missing every chunk rather
/// than late.
#[tokio::test]
async fn dropping_the_plan_leaves_the_read_to_its_streams() {
    let routing = Routing::new(2);
    let shared = labels("web-1");
    let other = labels_elsewhere(&routing, &shared);
    let switch = Arc::new(Switch::default());
    let dropped = Arc::new(AtomicUsize::new(0));
    let child = Gated::plan(
        vec![
            GatedPartition::held(vec![
                Step::chunk(chunk(shared.clone(), 0, 2, false)),
                Step::chunk(chunk(shared.clone(), 2, 1, true)),
            ]),
            GatedPartition::held(vec![Step::chunk(chunk(other.clone(), 0, 1, true))]),
        ],
        Arc::clone(&switch),
        Arc::clone(&dropped),
    );
    let plan: Arc<dyn ExecutionPlan> = node(child, 2);
    let context = SessionContext::default().task_ctx();
    let mut streams = (0..2)
        .map(|partition| output(&plan, partition, &context))
        .collect::<Vec<_>>();
    // Every plan reference goes away here: DataFusion's `execute_stream_partitioned` hands out the
    // streams and drops the plan, and this node has to keep reading for them.
    drop(plan);

    assert!(
        tokio::time::timeout(Duration::from_millis(200), streams[0].next())
            .await
            .is_err(),
        "the gated input produced before the plan was dropped, so this test proves nothing"
    );
    switch.open();

    let outputs = tokio::time::timeout(
        Duration::from_secs(10),
        futures::future::join_all(
            streams
                .into_iter()
                .map(|stream| stream.try_collect::<Vec<_>>()),
        ),
    )
    .await
    .expect("the read outlives the plan it was executed from")
    .into_iter()
    .map(|output| output.expect("every output decodes"))
    .collect::<Vec<_>>();
    for chunks in &outputs {
        assert_chains_are_whole(chunks);
    }

    let piece = named(&shared);
    assert_eq!(
        vec![
            (piece.clone(), Some(0), 2, false),
            (piece, Some(2), 1, true),
        ],
        summaries(&outputs[routing.output(&shared)]),
        "both chunks of the chain that was held back arrive, in order"
    );
    assert_eq!(
        vec![(named(&other), Some(0), 1, true)],
        summaries(&outputs[routing.output(&other)]),
        "the other partition's chunk arrives too"
    );
}

/// EOF makes every queued identity ready, but each poll hands off only one merged identity.
#[tokio::test]
async fn eof_readies_two_groups_but_poll_merges_one_at_a_time() {
    let first_metric = labels("eof-ready-first");
    let second_metric = labels("eof-ready-second");
    let child = Scripted::plan(vec![vec![
        Step::chunk(chunk(first_metric.clone(), 0, 1, true)),
        Step::chunk(chunk(second_metric.clone(), 0, 1, true)),
    ]]);
    let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
    let context = task_context(&pool);
    let shared =
        SharedExecution::start(&child, Routing::new(1), Arc::clone(&schema()), &context).unwrap();
    let (receiver, lease) = SharedExecution::claim(&shared, 0).expect("claim output");
    let mut stream = RepartitionStream::new(
        receiver,
        lease,
        Arc::clone(&schema()),
        &(Arc::clone(&pool) as Arc<dyn MemoryPool>),
        1,
        SeriesOrdering::Contiguous,
    );
    wait_until("both identities and EOF are queued", || {
        producer_stopped(&shared)
    })
    .await;

    let first = futures::future::poll_fn(|cx| Pin::new(&mut stream).poll_next(cx))
        .await
        .expect("first ready identity")
        .expect("first identity succeeds");
    let (first_series, first_closed) = evaluated(first);
    assert_eq!(first_metric, first_series.metric);
    assert!(first_closed);
    assert!(
        stream.output.is_empty(),
        "no eagerly merged second result is buffered"
    );

    let second = futures::future::poll_fn(|cx| Pin::new(&mut stream).poll_next(cx))
        .await
        .expect("second ready identity")
        .expect("second identity succeeds");
    let (second_series, second_closed) = evaluated(second);
    assert_eq!(second_metric, second_series.metric);
    assert!(second_closed);
    assert!(stream.groups.is_empty());
    drop(first_series);
    drop(second_series);
    assert!(
        futures::future::poll_fn(|cx| Pin::new(&mut stream).poll_next(cx))
            .await
            .is_none()
    );
    drop(stream);
    assert_eq!(0, pool.reserved(), "finished stream releases all charges");
}

/// when every output is dropped the read stops, even though the plan is still alive.
///
/// The plan is kept for the whole test, as a metrics collector holding a plan would keep it: what
/// stops the read is the outputs going away, not the plan being freed. Asserted are the three things
/// that has to release - the reading task, the child it was reading, and the memory the read
/// reserved.
#[tokio::test]
async fn dropping_every_output_stops_the_read_and_releases_it() {
    let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
    let context = task_context(&pool);
    let switch = Arc::new(Switch::default());
    let dropped = Arc::new(AtomicUsize::new(0));
    let shared = labels("web-1");
    // More closed chains than the two outputs can queue, so the read cannot finish on its own.
    let steps = (0..TEST_FIXTURE_VOLUME * 3)
        .map(|index| Step::chunk(chunk(shared.clone(), index, 1, true)))
        .collect::<Vec<_>>();
    let child = Gated::plan(
        vec![
            GatedPartition::open(steps.clone()),
            GatedPartition::open(steps),
        ],
        Arc::clone(&switch),
        Arc::clone(&dropped),
    );
    let plan = node(child, 2);
    let streams = (0..2)
        .map(|partition| {
            unpack_evaluated_series(
                plan.execute(partition, Arc::clone(&context))
                    .expect("the output executes"),
                None,
            )
            .expect("the output declares an evaluated frame")
        })
        .collect::<Vec<_>>();
    let execution = execution(&plan);
    wait_until("the read reserved what it queued", || pool.reserved() > 0).await;

    drop(streams);

    wait_until("the read's reservation is released", || {
        pool.reserved() == 0
    })
    .await;
    wait_until("the child's streams are dropped", || {
        dropped.load(Ordering::SeqCst) == 2
    })
    .await;
    wait_until("the reading task stops", || producer_stopped(&execution)).await;
    assert!(
        plan.shared.lock().unwrap().is_some(),
        "the plan is still alive, so it is the outputs that stopped the read"
    );
}

/// a child failure is a terminal item on every output, and the end comes after it.
///
/// The second half matters as much as the first: an output that reports the failure and then answers
/// again would be a stream whose error was not terminal, and one that answers with a chunk before the
/// error is already covered by the contents. What must never happen is a clean end in place of the
/// failure, so the error is required on every output.
#[tokio::test]
async fn a_child_failure_is_terminal_on_every_output() {
    let shared = labels("web-1");
    let child = Scripted::plan(vec![
        vec![Step::chunk(chunk(shared.clone(), 0, 2, true)), Step::Error],
        vec![Step::chunk(chunk(shared.clone(), 8, 1, true)), Step::Error],
    ]);
    let plan: Arc<dyn ExecutionPlan> = node(child, 3);
    let context = SessionContext::default().task_ctx();
    let mut streams = (0..3)
        .map(|partition| {
            plan.execute(partition, Arc::clone(&context))
                .expect("the output executes")
        })
        .collect::<Vec<_>>();

    for (partition, stream) in streams.iter_mut().enumerate() {
        let error = tokio::time::timeout(Duration::from_secs(10), terminal_error(stream))
            .await
            .unwrap_or_else(|_| panic!("output partition {partition} never ended"))
            .unwrap_or_else(|| {
                panic!("output partition {partition} reported a clean end, not the failure")
            });
        assert!(
            error.to_string().contains("scripted child failure"),
            "output partition {partition} ended with {error}"
        );
        let after = std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
        assert!(
            after.is_none(),
            "output partition {partition} answered again after its terminal error"
        );
    }
}

/// an output that ends takes nothing else down with it when another output was never executed.
///
/// This is DataFusion's ordinary order rather than an unusual one: `CoalescePartitionsExec` executes
/// and consumes one partition per task, and nothing makes every stream exist before the first one is
/// read. The input here is empty, so the read finishes while partition 0 alone is handed out, and
/// reading that partition to its end releases its lease - which is exactly the moment the read must
/// not be taken as over: partition 1 has not been asked for yet, and its queue is what a later
/// `execute` comes for. A read that stopped there would answer `execute(1)` with "executed twice" for
/// a partition that was never executed.
#[tokio::test]
async fn an_output_that_ends_does_not_close_the_queue_of_an_unclaimed_output() {
    let plan: Arc<dyn ExecutionPlan> = node(Scripted::plan(vec![Vec::new()]), 2);
    let context = SessionContext::default().task_ctx();

    let first = output(&plan, 0, &context);
    let first_chunks = tokio::time::timeout(Duration::from_secs(10), first.try_collect::<Vec<_>>())
        .await
        .expect("an empty input still ends the first output")
        .expect("the first output ends without a failure");
    assert!(
        first_chunks.is_empty(),
        "an empty input routed nothing, so the first output has nothing to hand over"
    );

    let second = unpack_evaluated_series(
        plan.execute(1, Arc::clone(&context))
            .unwrap_or_else(|error| panic!("the second output is still claimable: {error}")),
        None,
    )
    .expect("the second output declares an evaluated frame");
    let second_chunks =
        tokio::time::timeout(Duration::from_secs(10), second.try_collect::<Vec<_>>())
            .await
            .expect("the second output is executed after the first one ended")
            .expect("the second output ends without a failure");
    assert!(
        second_chunks.is_empty(),
        "an empty input routed nothing, so the second output has nothing to hand over"
    );
}

/// the output claimed last still receives its whole share after a claimed sibling ended.
///
/// Partition 0 is read to its end before partition 1 is executed. Its released lease must not discard
/// the unclaimed sibling queue, whose complete result is verified after the later claim.
#[tokio::test]
async fn an_unclaimed_output_delivers_its_whole_share_after_a_sibling_ended() {
    let routing = Routing::new(2);
    let shared = labels("web-1");
    let other = labels_elsewhere(&routing, &shared);
    let ended_first = routing.output(&shared);
    let claimed_later = routing.output(&other);
    assert_ne!(
        ended_first, claimed_later,
        "the two label sets have to land apart for this test to mean anything"
    );
    // One distinct label set per identity and output; chunks belonging to an identity form one chain.
    let chains = TEST_FIXTURE_VOLUME - 1;
    let identities = labels_routed_to(claimed_later, chains);
    let steps = (0..chains)
        .map(|index| Step::chunk(chunk(shared.clone(), index, 1, index + 1 == chains)))
        .chain(
            identities
                .iter()
                .enumerate()
                .map(|(index, metric)| Step::chunk(chunk(metric.clone(), index, 1, true))),
        )
        .collect::<Vec<_>>();
    let plan: Arc<dyn ExecutionPlan> = node(Scripted::plan(vec![steps]), 2);
    let context = SessionContext::default().task_ctx();

    let first = output(&plan, ended_first, &context);
    let first_chunks = tokio::time::timeout(Duration::from_secs(10), first.try_collect::<Vec<_>>())
        .await
        .expect("the first output reaches its end before the other is executed")
        .expect("the first output ends without a failure");
    assert_output_contract(&first_chunks, &[(shared.clone(), (0..chains).collect())]);

    let second = unpack_evaluated_series(
        plan.execute(claimed_later, Arc::clone(&context))
            .unwrap_or_else(|error| panic!("the other output is still claimable: {error}")),
        None,
    )
    .expect("the other output declares an evaluated frame");
    let second_chunks =
        tokio::time::timeout(Duration::from_secs(10), second.try_collect::<Vec<_>>())
            .await
            .expect("the output executed last reaches its end")
            .expect("the output executed last ends without a failure");
    let expected = identities
        .iter()
        .enumerate()
        .map(|(index, metric)| (metric.clone(), vec![index]))
        .collect::<Vec<_>>();
    assert_output_contract(&second_chunks, &expected);
}

/// A claimed owner completes while the other owner has received no chain yet. After opening the gate,
/// the sibling's result remains available for a later claim.
#[tokio::test]
async fn an_unclaimed_output_keeps_the_producer_of_a_claimed_one_running() {
    let routing = Routing::new(2);
    let shared = labels("web-1");
    let other = labels_elsewhere(&routing, &shared);
    let read_first = routing.output(&shared);
    let claimed_later = routing.output(&other);
    assert_ne!(
        read_first, claimed_later,
        "the two label sets have to land apart for this test to mean anything"
    );
    let chains = TEST_FIXTURE_VOLUME + 2;
    let queued_identities = labels_routed_to(claimed_later, 2 * chains);
    let switch = Arc::new(Switch::default());
    let child = Gated::plan(
        vec![
            GatedPartition {
                before: vec![Step::chunk(chunk(shared.clone(), 0, 1, true))],
                after: queued_identities[..chains]
                    .iter()
                    .enumerate()
                    .map(|(index, metric)| Step::chunk(chunk(metric.clone(), index, 1, true)))
                    .collect(),
                switch: None,
            },
            GatedPartition {
                before: vec![Step::chunk(chunk(shared.clone(), 1, 1, true))],
                after: queued_identities[chains..]
                    .iter()
                    .enumerate()
                    .map(|(index, metric)| Step::chunk(chunk(metric.clone(), index, 1, true)))
                    .collect(),
                switch: None,
            },
        ],
        Arc::clone(&switch),
        Arc::new(AtomicUsize::new(0)),
    );
    let concrete_plan = node(child, 2);
    let plan: Arc<dyn ExecutionPlan> = concrete_plan.clone();
    let context = SessionContext::default().task_ctx();
    let mut first = unpack_evaluated_series(
        plan.execute(read_first, Arc::clone(&context))
            .expect("claimed output executes"),
        None,
    )
    .expect("the output declares an evaluated frame");
    let execution = execution(&concrete_plan);

    let mut first_chunks = Vec::new();
    loop {
        let item = tokio::time::timeout(Duration::from_secs(10), first.next())
            .await
            .expect("completed identity is emitted while sibling chain is gated")
            .expect("output remains open until all inputs finish")
            .expect("output succeeds");
        let done = item.1;
        first_chunks.push(item);
        if done {
            break;
        }
    }
    assert_output_contract(&first_chunks, &[(shared.clone(), vec![0, 1])]);
    assert!(!switch.is_open(), "sibling input remains gated");
    assert!(
        !producer_stopped(&execution),
        "another input remains live despite the completed output"
    );
    switch.open();
    wait_until("the read finishes after terminal EOF", || {
        producer_stopped(&execution)
    })
    .await;

    // The sibling has ended, but the unclaimed owner's entire queue remains available to claim.
    let second = output(&plan, claimed_later, &context);
    let second_chunks =
        tokio::time::timeout(Duration::from_secs(10), second.try_collect::<Vec<_>>())
            .await
            .expect("the retained unclaimed owner completes")
            .expect("the retained owner succeeds");
    let expected = queued_identities
        .iter()
        .enumerate()
        .map(|(index, metric)| (metric.clone(), vec![index % chains]))
        .collect::<Vec<_>>();
    assert_output_contract(&second_chunks, &expected);
}

/// A task context whose memory pool never refuses a reservation.
///
/// The composition below reads further ahead than a bounded pool would allow, and a read that failed
/// on memory would end with an error instead of showing whether it finishes at all - which is the one
/// thing this test is about.
fn unbounded_context() -> Arc<TaskContext> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(UnboundedMemoryPool::default()) as Arc<dyn MemoryPool>)
        .build()
        .expect("the runtime environment builds");
    Arc::new(TaskContext::default().with_runtime(Arc::new(runtime)))
}

/// `count` label sets of distinct series that a two-output node routes to `output`.
///
/// Found with the node's own [`Routing`] over candidate labels rather than assumed: which output a
/// label set belongs on is a hash of it, and a label set hard-coded from watching one run would stop
/// being a label set of `output` the moment the key or the seed changed, leaving the test about
/// something else. Reading it off the same function the node reduces with is what keeps the premise
/// - every chunk lands on `output` - true by construction.
fn labels_routed_to(output: usize, count: usize) -> Vec<SeriesLabels> {
    let routing = Routing::new(2);
    let found = (0..)
        .filter_map(|index| {
            let candidate = labels(&format!("many-{index}"));
            (routing.output(&candidate) == output).then_some(candidate)
        })
        .take(count)
        .collect::<Vec<_>>();
    assert_eq!(
        count,
        found.len(),
        "two outputs split the candidates between them, so {count} of each are found"
    );
    found
}

/// two of these nodes composed do not deadlock while every output of the outer one is drained.
///
/// The child produces `SERIES` single-point chains, each with a label set of its own, every one of
/// them hashing to output 1 of a two-output node - so the inner node's output 0 is empty and output 1
/// carries all of them. Composed as `outer(inner(child))`, the outer node's input 0 is the inner
/// node's empty output, and what the read does with that is what this test states.
///
/// The input has many more chains than the bounded output queues can hold, so the test needs the
/// empty inner output to close through independent input-task progress rather than by buffering the
/// child whole. Both outer outputs are drained concurrently, as required by the node.
///
/// Both outputs of the outer node are read at once and each is drained to its end, so nothing here
/// can be blamed on an output nobody drained - which is the one requirement this node states of its
/// caller. The count that comes out is asserted against the count that went in, so a passing run is a
/// run that lost nothing.
///
/// Independent input tasks let the outer node route the inner node's non-empty output while its
/// other task waits for the inner empty output to close, allowing that inner execution to finish.
#[tokio::test]
async fn composing_two_nodes_does_not_deadlock_when_the_inner_output_is_empty() {
    const SERIES: usize = 4096;
    let routed = labels_routed_to(1, SERIES);
    let source = || {
        Scripted::plan(vec![
            routed
                .iter()
                .map(|metric| Step::chunk(chunk(metric.clone(), 0, 1, true)))
                .collect::<Vec<_>>(),
        ])
    };
    let context = unbounded_context();

    // The same child over one node finishes, and finishes with everything on output 1: what stalls
    // below is the composition, not the child and not the volume.
    let alone: Arc<dyn ExecutionPlan> = node(source(), 2);
    let alone_outputs = tokio::time::timeout(Duration::from_secs(10), drain(&alone, &context))
        .await
        .expect("one node over this child finishes");
    assert!(
        alone_outputs[0]
            .as_ref()
            .expect("the first output decodes")
            .is_empty(),
        "every label set of this test hashes to output 1, so output 0 has nothing to carry"
    );
    assert_eq!(
        SERIES,
        alone_outputs[1]
            .as_ref()
            .expect("the second output decodes")
            .len(),
        "one node delivers every series it was given"
    );

    let inner: Arc<dyn ExecutionPlan> = node(source(), 2);
    let plan: Arc<dyn ExecutionPlan> = node(inner, 2);

    // Both outputs at once, each to its end, and each chunk counted where it arrives: the count is
    // what a failure has to say when the read never gets to the assertion below.
    let delivered = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
    let reads = (0..2)
        .map(|partition| {
            let plan = Arc::clone(&plan);
            let context = Arc::clone(&context);
            let delivered = Arc::clone(&delivered);
            async move {
                let mut stream = output(&plan, partition, &context);
                let mut chunks = Vec::new();
                while let Some(chunk) = stream.try_next().await? {
                    delivered[partition].fetch_add(1, Ordering::Relaxed);
                    chunks.push(chunk);
                }
                Ok::<_, BoxedError>(chunks)
            }
        })
        .collect::<Vec<_>>();
    let outputs = tokio::time::timeout(Duration::from_secs(10), futures::future::join_all(reads))
        .await
        .unwrap_or_else(|_| {
            panic!(
                "two composed nodes never finished although both outputs of the outer one were \
                 drained: {} of {SERIES} series arrived on output 0 and {} on output 1",
                delivered[0].load(Ordering::Relaxed),
                delivered[1].load(Ordering::Relaxed),
            )
        })
        .into_iter()
        .map(|chunks| chunks.expect("both outputs decode"))
        .collect::<Vec<_>>();

    assert!(
        outputs[0].is_empty(),
        "the same label sets hash to output 1 of the outer node as of the inner one"
    );
    for chunks in &outputs {
        assert_chains_are_whole(chunks);
    }
    let mut names = outputs
        .iter()
        .flatten()
        .map(|(series, is_last)| {
            assert!(*is_last, "every chain of this test closes on its own");
            named(&series.metric)
        })
        .collect::<Vec<_>>();
    names.sort();
    names.dedup();
    assert_eq!(
        SERIES,
        names.len(),
        "every series that went in comes out exactly once, none twice and none truncated"
    );
    assert_eq!(
        SERIES,
        outputs.iter().map(Vec::len).sum::<usize>(),
        "the composition delivers as many chunks as the child handed over"
    );
}

#[tokio::test]
async fn owner_merges_across_inputs_and_sorts_late_positions_until_eof() {
    let metric = labels("owner-order");
    let switch = Arc::new(Switch::default());
    let child = Gated::plan(
        vec![
            GatedPartition::open(vec![Step::chunk(chunk(metric.clone(), 10, 1, true))]),
            GatedPartition::held(vec![Step::chunk(chunk(metric.clone(), 0, 1, true))]),
        ],
        Arc::clone(&switch),
        Arc::new(AtomicUsize::new(0)),
    );
    let plan: Arc<dyn ExecutionPlan> = node(child, 1);
    let context = SessionContext::default().task_ctx();
    let mut stream = output(&plan, 0, &context);
    assert!(
        futures::future::poll_fn(|cx| match stream.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Ready(true),
            Poll::Ready(_) => Poll::Ready(false),
        })
        .await,
        "the identity waits while input 1 has not reached its final chain"
    );
    switch.open();
    let chunks = tokio::time::timeout(Duration::from_secs(10), stream.try_collect::<Vec<_>>())
        .await
        .expect("gated input completes")
        .expect("owner succeeds");
    assert_output_contract(&chunks, &[(metric.clone(), vec![0, 10])]);
    assert_eq!(1, chunks.iter().filter(|(_, is_last)| *is_last).count());
}

#[tokio::test]
async fn multiple_producers_and_owners_merge_exact_full_label_identities() {
    let routing = Routing::new(2);
    let on_zero = labels_routed_to(0, 1).remove(0);
    let on_one = labels_routed_to(1, 1).remove(0);
    // These identities must not collapse merely because rendering omits empty values or a name.
    let empty_value = SeriesLabels::try_new(
        Some(Arc::from("cpu")),
        vec![Label {
            name: Arc::from("zone"),
            value: Arc::from(""),
        }],
    )
    .unwrap();
    let no_empty_value = SeriesLabels::try_new(Some(Arc::from("cpu")), Vec::new()).unwrap();
    let absent_name = SeriesLabels::try_new(None, Vec::new()).unwrap();
    let present_name = SeriesLabels::try_new(Some(Arc::from("")), Vec::new()).unwrap();
    assert_ne!(
        empty_value, no_empty_value,
        "empty label presence is identity"
    );
    assert_ne!(absent_name, present_name, "name presence is identity");
    let identities = [
        on_zero,
        on_one,
        empty_value,
        no_empty_value,
        absent_name,
        present_name,
    ];
    let mut partitions = vec![Vec::new(), Vec::new(), Vec::new()];
    for metric in identities.iter() {
        for (partition, position) in [0, 10, 20].into_iter().enumerate() {
            partitions[partition].push(Step::chunk(chunk(metric.clone(), position, 1, true)));
        }
    }
    let plan: Arc<dyn ExecutionPlan> = node(Scripted::plan(partitions), 2);
    let outputs = drain(&plan, &SessionContext::default().task_ctx()).await;
    for (owner, output) in outputs.iter().enumerate() {
        let chunks = output.as_ref().expect("each owner succeeds");
        let expected = identities
            .iter()
            .filter(|metric| routing.output(metric) == owner)
            .map(|metric| (metric.clone(), vec![0, 10, 20]))
            .collect::<Vec<_>>();
        assert!(!expected.is_empty(), "each output receives a real identity");
        assert_output_contract(chunks, &expected);
        assert_eq!(
            expected.len(),
            chunks.iter().filter(|(_, closed)| *closed).count()
        );
    }
}

#[tokio::test]
async fn error_after_early_public_chain_is_reported_once_and_releases_pool() {
    let metric = labels_routed_to(0, 2).remove(0);
    let switch = Arc::new(Switch::default());
    let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
    let context = task_context(&pool);
    // Both inputs contribute A before their gates; only input 0 fails after the gate opens.
    let child = Gated::plan(
        vec![
            GatedPartition {
                before: vec![Step::chunk(chunk(metric.clone(), 0, 1, true))],
                after: vec![Step::ErrorSaying("late gated failure")],
                switch: None,
            },
            GatedPartition {
                before: vec![Step::chunk(chunk(metric.clone(), 1, 1, true))],
                after: Vec::new(),
                switch: None,
            },
        ],
        Arc::clone(&switch),
        Arc::new(AtomicUsize::new(0)),
    );
    let plan = node(child, 2);
    let mut stream = output_of(&plan, 0, &context);
    let mut emitted = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match futures::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
                Some(Ok(chunk)) => {
                    let done = chunk.1;
                    emitted.push(chunk);
                    if done {
                        break;
                    }
                }
                Some(Err(error)) => panic!("A failed before publication: {error}"),
                None => panic!("A ended before publication"),
            }
        }
    })
    .await
    .expect("A is published before the late failure");
    assert_output_contract(&emitted, &[(metric, vec![0, 1])]);
    switch.open();
    let error = evaluated_terminal_error(&mut stream)
        .await
        .expect("late failure is reported");
    assert!(error.to_string().contains("late gated failure"));
    assert!(
        evaluated_terminal_error(&mut stream).await.is_none(),
        "failure is reported once"
    );
    assert_eq!(0, pool.reserved(), "failure releases all retained charges");
}

#[tokio::test]
async fn late_unclaimed_output_receives_first_merge_error_once_after_early_output_fails() {
    let metric = labels_routed_to(0, 1).remove(0);
    let other = labels_routed_to(1, 1).remove(0);
    let conflicting = |value| {
        let time = TimeAxis::from_grid_spans(grid(), vec![GridSpan::new(0, 1)]).unwrap();
        let fields = SeriesFields::one(Samples::from_float(Float64Array::from(vec![value])));
        Step::Chunk(
            Series::try_new(&schema(), metric.clone(), time, fields).unwrap(),
            true,
        )
    };
    let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
    let context = task_context(&pool);
    let child = Scripted::plan(vec![
        vec![
            Step::chunk(chunk(other.clone(), 0, 1, true)),
            conflicting(1.0),
        ],
        vec![conflicting(2.0)],
    ]);
    let plan = node(child, 2);
    let mut early = output_of(&plan, 0, &context);
    let mut emitted = Vec::new();
    let error = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match futures::future::poll_fn(|cx| early.as_mut().poll_next(cx)).await {
                None => panic!("conflicting merge ended without its error"),
                Some(Ok(chunk)) => emitted.push(chunk),
                Some(Err(error)) => break error.to_string(),
            }
        }
    })
    .await
    .expect("failure terminates the claimed output");
    assert!(
        error.contains("cannot contain metrics with the same labelset"),
        "actual merge error preserved: {error}"
    );
    assert!(
        emitted.is_empty(),
        "the conflicting identity is never emitted"
    );
    assert_eq!(
        0,
        pool.reserved(),
        "failure clears queued receiver data immediately"
    );
    assert!(
        futures::future::poll_fn(|cx| early.as_mut().poll_next(cx))
            .await
            .is_none(),
        "error is terminal"
    );

    let mut late = plan
        .execute(1, Arc::clone(&context))
        .expect("late output executes");
    let late_error = terminal_error(&mut late)
        .await
        .expect("late owner gets failure");
    assert_eq!(error, late_error.to_string(), "first error is retained");
    assert!(
        terminal_error(&mut late).await.is_none(),
        "late output reports it once"
    );
    assert_eq!(0, pool.reserved(), "late error leaves no retained charges");
}

#[tokio::test]
async fn owner_merges_complementary_fields_across_input_positions() {
    let metric = labels("complementary-fields");
    let schema = Arc::new(SeriesSchema {
        fields: Arc::from([
            FieldSpec {
                name: Arc::from("left"),
            },
            FieldSpec {
                name: Arc::from("right"),
            },
        ]),
        eval_grid: Some(grid()),
    });
    let make = |position, left, right| {
        let time = TimeAxis::from_grid_spans(
            schema.eval_grid.clone().unwrap(),
            vec![GridSpan::new(position, 1)],
        )
        .unwrap();
        let fields = SeriesFields::try_from_vec(vec![
            Samples::from_float(Float64Array::from(vec![left])),
            Samples::from_float(Float64Array::from(vec![right])),
        ])
        .unwrap();
        Step::Chunk(
            Series::try_new(&schema, metric.clone(), time, fields).unwrap(),
            true,
        )
    };
    let switch = Arc::new(Switch::default());
    let child = Gated::plan_with_schema(
        vec![
            GatedPartition::open(vec![make(0, Some(10.0), None)]),
            GatedPartition {
                before: Vec::new(),
                after: vec![make(10, None, Some(20.0))],
                switch: None,
            },
        ],
        Arc::clone(&switch),
        Arc::new(AtomicUsize::new(0)),
        Arc::clone(&schema),
    );
    let plan: Arc<dyn ExecutionPlan> = node(child, 1);
    let mut output = output(&plan, 0, &SessionContext::default().task_ctx());
    match tokio::time::timeout(Duration::from_millis(50), output.next()).await {
        Err(_) => {}
        Ok(Some(Ok((series, last)))) => panic!(
            "the identity must wait while one input has not reached its final chain, got {} last={last}",
            named(&series.metric)
        ),
        Ok(Some(Err(error))) => panic!("the output failed instead of waiting: {error}"),
        Ok(None) => panic!("the output ended before the gated input contributed"),
    }
    switch.open();
    let chunks = output
        .try_collect::<Vec<_>>()
        .await
        .expect("merged output succeeds");
    assert_output_contract(&chunks, &[(metric.clone(), vec![0, 10])]);
    assert_eq!(1, chunks.iter().filter(|(_, is_last)| *is_last).count());
    let series = chunks.iter().map(|(series, _)| series).collect::<Vec<_>>();
    assert_eq!(2, series[0].fields.len());
    for (index, expected) in [10.0, 20.0].into_iter().enumerate() {
        let values = series
            .iter()
            .flat_map(|series| series.fields.get(index).unwrap().iter())
            .filter_map(|sample| match sample {
                datatypes::series::samples::SampleRef::Float(value) => Some(value),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(1, values.len());
        assert_eq!(expected, values[0]);
    }
}

#[tokio::test]
async fn owner_rejects_conflicting_values_at_same_identity_and_position() {
    let metric = labels("owner-conflict");
    let first = chunk(metric.clone(), 0, 1, true).0;
    let schema = schema();
    let time =
        TimeAxis::from_grid_spans(schema.eval_grid.clone().unwrap(), vec![GridSpan::new(0, 1)])
            .unwrap();
    let conflicting = Series::try_new(
        &schema,
        metric.clone(),
        time,
        SeriesFields::one(Samples::from_float(Float64Array::from(vec![99.0]))),
    )
    .unwrap();
    let child = Scripted::plan(vec![
        vec![Step::Chunk(first, true)],
        vec![Step::Chunk(conflicting, true)],
    ]);
    let plan: Arc<dyn ExecutionPlan> = node(child, 1);
    let outputs = drain(&plan, &SessionContext::default().task_ctx()).await;
    assert!(
        outputs[0].is_err(),
        "conflicting populated cell is an error"
    );
}

/// Same-identity input chunks are collected and emitted as one closed series.
#[tokio::test]
async fn same_canonical_key_chains_merge_into_one_series() {
    let metric = labels("same");
    let child = Scripted::plan(vec![
        vec![Step::chunk(chunk(metric.clone(), 0, 2, true))],
        vec![Step::chunk(chunk(metric.clone(), 8, 2, true))],
    ]);
    let plan: Arc<dyn ExecutionPlan> = node(child, 2);
    let outputs = drain(&plan, &SessionContext::default().task_ctx()).await;
    let owner = Routing::new(2).output(&metric);
    let chunks = outputs[owner].as_ref().expect("the owner output decodes");
    assert_output_contract(chunks, &[(metric.clone(), vec![0, 1, 8, 9])]);
    assert_eq!(1, chunks.iter().filter(|(_, closed)| *closed).count());
    assert!(outputs[1 - owner].as_ref().unwrap().is_empty());
}

#[tokio::test]
async fn canonical_label_key_variants_route_consistently_without_merging_chains() {
    let base = SeriesLabels::try_new(Some(Arc::from("cpu")), Vec::new()).unwrap();
    let metric_name_change = SeriesLabels::try_new(Some(Arc::from("memory")), Vec::new()).unwrap();
    let extra_tag = labels("host-a");
    let empty_tag = SeriesLabels::try_new(
        Some(Arc::from("cpu")),
        vec![Label {
            name: Arc::from("host"),
            value: Arc::from(""),
        }],
    )
    .unwrap();
    let absent_tag = base.clone();
    let no_name = SeriesLabels::try_new(None, Vec::new()).unwrap();
    let empty_name = SeriesLabels::try_new(Some(Arc::from("")), Vec::new()).unwrap();

    let cases = [
        (
            "metric name is part of key",
            base.clone(),
            metric_name_change,
            false,
        ),
        (
            "additional tag is part of key",
            base.clone(),
            extra_tag,
            false,
        ),
        ("empty tag value is absent", empty_tag, absent_tag, true),
        (
            "missing and empty metric names canonicalize alike",
            no_name,
            empty_name,
            true,
        ),
    ];
    for (description, left, right, expected_same_key) in cases {
        let left_key = label_set(&left);
        let right_key = label_set(&right);
        assert_eq!(
            expected_same_key,
            left_key == right_key,
            "canonical keys follow the expected label semantics: {description}"
        );
        if expected_same_key {
            assert_eq!(
                Routing::new(5).output(&left),
                Routing::new(5).output(&right),
                "equal canonical keys must share an owner: {description}"
            );
            assert_eq!(
                Routing::new(3).output(&left),
                Routing::new(3).output(&right),
                "canonical equivalence holds for any output count: {description}"
            );
        }

        let child = Scripted::plan(vec![
            vec![Step::chunk(chunk(left.clone(), 0, 1, true))],
            vec![Step::chunk(chunk(right.clone(), 4, 1, true))],
        ]);
        let plan: Arc<dyn ExecutionPlan> = node(child, 5);
        let outputs = drain(&plan, &SessionContext::default().task_ctx()).await;
        let seen = outputs
            .iter()
            .enumerate()
            .flat_map(|(partition, output)| {
                output
                    .as_ref()
                    .expect("output decodes")
                    .iter()
                    .map(move |(series, is_last)| {
                        let owner = Routing::new(5).output(&series.metric);
                        assert_eq!(
                            partition, owner,
                            "each returned chain is on its computed owner: {description}"
                        );
                        (label_set(&series.metric), *is_last)
                    })
            })
            .collect::<Vec<_>>();
        assert_eq!(
            2,
            seen.len(),
            "both independent closed chains survive: {description}"
        );
        assert!(seen.iter().all(|(_, is_last)| *is_last));
        let mut expected_keys = vec![left_key, right_key];
        expected_keys.sort();
        let mut observed_keys = seen.into_iter().map(|(key, _)| key).collect::<Vec<_>>();
        observed_keys.sort();
        assert_eq!(
            expected_keys, observed_keys,
            "each chain's canonical key remains observable: {description}"
        );
    }
}

/// two sources whose chains belong on one output land there whole, never inside one another.
///
/// Both label sets hash to the same output and both sources have a chain for it. One of the chains is
/// held open across a gate, so the other source's chain is the only thing that can be delivered; what
/// must not happen is that the held chain's first chunk sits on the output ahead of the other source's
/// chain, because two sources' chunks inside one open series is exactly what a downstream frame reader
/// refuses. What does happen is that the closed chain arrives first, whole, and the held one follows.
#[tokio::test]
async fn two_sources_keep_their_chains_whole_on_one_output() {
    let on_one = labels_routed_to(0, 2);
    let (held, closed) = (on_one[0].clone(), on_one[1].clone());
    assert_eq!(
        Routing::new(2).output(&held),
        Routing::new(2).output(&closed),
        "two label sets of one output, found rather than assumed"
    );
    let switch = Arc::new(Switch::default());
    let child = Gated::plan(
        vec![
            // The chain that stays open across the gate: its first chunk is handed over at once and
            // the chunk that closes it only after the switch opens.
            GatedPartition {
                before: vec![
                    Step::chunk(chunk(closed.clone(), 0, 2, true)),
                    Step::chunk(chunk(held.clone(), 0, 2, false)),
                ],
                after: vec![Step::chunk(chunk(held.clone(), 2, 2, true))],
                switch: None,
            },
            // The other source's final complementary chunk is handed over as soon as it is polled.
            GatedPartition {
                before: vec![Step::chunk(chunk(closed.clone(), 2, 2, true))],
                after: Vec::new(),
                switch: None,
            },
        ],
        Arc::clone(&switch),
        Arc::new(AtomicUsize::new(0)),
    );
    let plan: Arc<dyn ExecutionPlan> = node(child, 2);
    let context = SessionContext::default().task_ctx();
    let mut routed = output(&plan, 0, &context);
    let other = output(&plan, 1, &context);

    // The owner publishes the other identity even while the first input's chain remains open.
    let mut chunks = Vec::new();
    loop {
        let chunk = tokio::time::timeout(Duration::from_secs(10), routed.next())
            .await
            .expect("closed identity emits before gated input completes")
            .expect("closed identity is present")
            .expect("owner succeeds");
        assert_eq!(closed, chunk.0.metric);
        let done = chunk.1;
        chunks.push(chunk);
        if done {
            break;
        }
    }
    assert!(
        futures::future::poll_fn(|cx| match routed.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Ready(true),
            Poll::Ready(_) => Poll::Ready(false),
        })
        .await,
        "held identity cannot emit before its chain closes"
    );
    switch.open();
    chunks.extend(
        tokio::time::timeout(Duration::from_secs(10), routed.try_collect::<Vec<_>>())
            .await
            .expect("held chain emits after gate opens")
            .expect("owner succeeds"),
    );
    assert_chains_are_whole(&chunks);
    assert_output_contract(
        &chunks,
        &[
            (closed.clone(), vec![0, 1, 2, 3]),
            (held.clone(), vec![0, 1, 2, 3]),
        ],
    );
    let empty = tokio::time::timeout(Duration::from_secs(10), other.try_collect::<Vec<_>>())
        .await
        .expect("the other output ends")
        .expect("the other output decodes");
    assert!(
        empty.is_empty(),
        "neither label set hashes to the other output"
    );
}

/// a closing chunk that carries no rows is delivered, and is not mistaken for the end of the
/// stream.
///
/// A series can close with nothing left of it, and that chunk carries the finality and no position. It
/// is a chunk of its own on the way out - it is how a consumer closes the series - and a stream that is
/// still going must not be ended by it: the chunk after it is what says the empty one was a chunk and
/// not the end.
#[tokio::test]
async fn an_empty_closing_chunk_is_a_chunk_and_not_the_end() {
    let routing = Routing::new(2);
    let shared = labels("web-1");
    let other = labels_elsewhere(&routing, &shared);
    let child = Scripted::plan(vec![
        // One source closes a series with nothing left of it, and keeps going afterwards.
        vec![
            Step::chunk(chunk(other.clone(), 0, 0, true)),
            Step::chunk(chunk(labels("after-empty-close"), 0, 1, true)),
        ],
        // The other source hands over a chain with positions, and closes it with an empty chunk.
        vec![
            Step::chunk(chunk(labels("empty-before-data"), 0, 0, true)),
            Step::chunk(chunk(shared.clone(), 4, 2, false)),
            Step::chunk(chunk(shared.clone(), 6, 0, true)),
        ],
    ]);
    let plan: Arc<dyn ExecutionPlan> = node(child, 2);

    let outputs = tokio::time::timeout(
        Duration::from_secs(10),
        drain(&plan, &SessionContext::default().task_ctx()),
    )
    .await
    .expect("an input that only closes empty chains still ends")
    .into_iter()
    .map(|output| output.expect("no output ends with an error"))
    .collect::<Vec<_>>();
    for chunks in &outputs {
        assert_chains_are_whole(chunks);
    }

    let fixture = [
        (shared.clone(), vec![4, 5]),
        (other.clone(), vec![]),
        (labels("empty-before-data"), vec![]),
        (labels("after-empty-close"), vec![0]),
    ];
    for (owner, chunks) in outputs.iter().enumerate() {
        let expected = fixture
            .iter()
            .filter(|(metric, _)| routing.output(metric) == owner)
            .cloned()
            .collect::<Vec<_>>();
        assert_output_contract(chunks, &expected);
    }
}

/// while one owner's chain is still open, the other owner's output still delivers.
///
/// Two label sets on two outputs, one long chain each, and the first one is held open across a gate.
/// The other output has to deliver its whole chain in that time: the wait on the gated source belongs
/// to the task reading that source, and no other source's chunks wait with it. What this rules out is
/// the shape this node used to have - one reader for every input, parked on the gated source and
/// delivering nothing at all until the chain closed.
#[tokio::test]
async fn an_open_chain_on_one_output_does_not_hold_the_other_output_back() {
    let routing = Routing::new(2);
    let held = labels("web-1");
    let free = labels_elsewhere(&routing, &held);
    let held_output = routing.output(&held);
    let free_output = routing.output(&free);
    assert_ne!(
        held_output, free_output,
        "the two label sets have to land apart for this test to mean anything"
    );
    let switch = Arc::new(Switch::default());
    let child = Gated::plan(
        vec![
            GatedPartition::open(vec![
                Step::chunk(chunk(free.clone(), 0, 2, false)),
                Step::chunk(chunk(free.clone(), 2, 2, true)),
            ]),
            GatedPartition::open(vec![
                Step::chunk(chunk(free.clone(), 4, 2, false)),
                Step::chunk(chunk(free.clone(), 6, 2, true)),
            ]),
            // A long chain, gated after its first chunk.
            GatedPartition {
                before: vec![
                    Step::chunk(chunk(free.clone(), 8, 2, false)),
                    Step::chunk(chunk(free.clone(), 10, 2, true)),
                    Step::chunk(chunk(held.clone(), 0, 2, false)),
                ],
                after: vec![
                    Step::chunk(chunk(held.clone(), 2, 2, false)),
                    Step::chunk(chunk(held.clone(), 4, 2, false)),
                    Step::chunk(chunk(held.clone(), 6, 2, true)),
                ],
                switch: None,
            },
        ],
        Arc::clone(&switch),
        Arc::new(AtomicUsize::new(0)),
    );
    let plan: Arc<dyn ExecutionPlan> = node(child, 2);
    let context = SessionContext::default().task_ctx();
    let mut held_stream = output(&plan, held_output, &context);
    let mut free_stream = output(&plan, free_output, &context);

    // All contributors to the free identity close it before the separate held chain gates.
    let mut free_chunks = Vec::new();
    loop {
        let chunk = tokio::time::timeout(Duration::from_secs(10), free_stream.next())
            .await
            .expect("completed free identity emits before gate opens")
            .expect("free identity is present")
            .expect("free owner succeeds");
        assert_eq!(free, chunk.0.metric);
        let done = chunk.1;
        free_chunks.push(chunk);
        if done {
            break;
        }
    }
    assert_output_contract(
        &free_chunks,
        &[(free.clone(), vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])],
    );
    assert!(
        futures::future::poll_fn(|cx| match held_stream.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Ready(true),
            Poll::Ready(_) => Poll::Ready(false),
        })
        .await,
        "the gated chain cannot complete before the switch opens"
    );
    switch.open();
    let free_rest = tokio::time::timeout(
        Duration::from_secs(10),
        (&mut free_stream).try_collect::<Vec<_>>(),
    )
    .await
    .expect("free output ends after remaining input closes")
    .expect("free owner succeeds");
    assert!(
        free_rest.is_empty(),
        "completed identity is not emitted twice"
    );

    switch.open();
    let held_chunks =
        tokio::time::timeout(Duration::from_secs(10), held_stream.try_collect::<Vec<_>>())
            .await
            .expect("the held chain arrives once the gate opens")
            .expect("the held output decodes");
    assert_chains_are_whole(&held_chunks);
    assert_eq!(
        vec![
            (named(&held), Some(0), 2, false),
            (named(&held), Some(2), 2, false),
            (named(&held), Some(4), 2, false),
            (named(&held), Some(6), 2, true),
        ],
        summaries(&held_chunks),
        "the long chain arrives whole and in order once its source closes it"
    );
    let rest = tokio::time::timeout(
        Duration::from_secs(10),
        (&mut free_stream).try_collect::<Vec<_>>(),
    )
    .await
    .expect("the other output ends")
    .expect("the other output decodes");
    assert!(
        rest.is_empty(),
        "the other output carried its whole chain already"
    );
}

/// a partition that fails mid-stream ends every output with that error, none of them cleanly.
///
/// One of the two input partitions fails after handing over a chunk and the other ends normally, so
/// one output has data and an end of its own that the failure has to replace. Three outputs make sure
/// the error is not merely the fate of the output the failed partition was routing to: an output that
/// ended cleanly would report the series that never arrived as series that do not exist.
#[tokio::test]
async fn a_child_partition_that_fails_mid_stream_ends_every_output_with_its_error() {
    let shared = labels("web-1");
    let other = labels("elsewhere");
    let child = Scripted::plan(vec![
        // This partition fails with its series open.
        vec![Step::chunk(chunk(shared.clone(), 0, 2, false)), Step::Error],
        // This one ends normally.
        vec![Step::chunk(chunk(other.clone(), 0, 1, true))],
    ]);
    let plan: Arc<dyn ExecutionPlan> = node(child, 3);

    let outputs = tokio::time::timeout(
        Duration::from_secs(10),
        drain(&plan, &SessionContext::default().task_ctx()),
    )
    .await
    .expect("a failed read still ends every output");
    assert_eq!(3, outputs.len());
    for (partition, output) in outputs.iter().enumerate() {
        let error = output
            .as_ref()
            .err()
            .unwrap_or_else(|| panic!("output partition {partition} reported a clean end"));
        assert!(
            error.to_string().contains("scripted child failure"),
            "output partition {partition} ended with {error}"
        );
    }
}

/// two inputs make their own routing decisions, and neither waits for the other to make one.
///
/// The child will not hand over a second chunk of a partition until every partition has handed over a
/// first, so nothing gets past the first chunk of either source unless both are being read at the same
/// time: a read that took one input's chain to its end before it looked at the other waits forever
/// there for a chunk that only the other input's own progress can release. Each source's chain is two
/// chunks long, so that wait lands in the middle of an open chain - the point at which a
/// chain-continuous reader that drove the inputs itself would have to stop.
///
/// The two label sets hash to two different outputs, so what each task does here is a routing decision
/// of its own: one hash over one chain, sent to one owner. Two outputs means each of them has a
/// different decision to make and no decision of the other's to wait for, which is the property this
/// test states; a node with a single output is [`one_output_keeps_every_chunk_and_its_chains_whole`],
/// where there is nothing to decide and every chunk still arrives.
///
/// The gate is this child and not a stopwatch: nothing here depends on two cores happening to run at
/// the same instant, only on the two inputs being driven, and routed, independently of each other.
#[tokio::test]
async fn two_inputs_are_routed_without_waiting_for_each_other() {
    let routing = Routing::new(2);
    let first = labels("web-1");
    let second = labels_elsewhere(&routing, &first);
    assert_ne!(
        routing.output(&first),
        routing.output(&second),
        "the two label sets have to land apart for this test to mean anything"
    );
    let child = Together::plan(vec![
        vec![
            Step::chunk(chunk(first.clone(), 0, 1, false)),
            Step::chunk(chunk(first.clone(), 1, 1, true)),
        ],
        vec![
            Step::chunk(chunk(second.clone(), 0, 1, false)),
            Step::chunk(chunk(second.clone(), 1, 1, true)),
        ],
    ]);
    let plan: Arc<dyn ExecutionPlan> = node(child, 2);

    let outputs = tokio::time::timeout(
        Duration::from_secs(10),
        drain(&plan, &SessionContext::default().task_ctx()),
    )
    .await
    .expect("both inputs are read at once, so the rendezvous between them opens")
    .into_iter()
    .map(|output| output.expect("no output ends with an error"))
    .collect::<Vec<_>>();
    assert_eq!(2, outputs.len());
    for chunks in &outputs {
        assert_chains_are_whole(chunks);
    }
    assert_eq!(
        vec![
            (named(&first), Some(0), 1, false),
            (named(&first), Some(1), 1, true),
        ],
        summaries(&outputs[routing.output(&first)]),
        "the first input's chain is routed, whole and in order, to the output its own labels name"
    );
    assert_eq!(
        vec![
            (named(&second), Some(0), 1, false),
            (named(&second), Some(1), 1, true),
        ],
        summaries(&outputs[routing.output(&second)]),
        "and the second input's chain to its own output, neither decision waiting on the other"
    );
}

/// The chunks each partition of a [`Together`] child has handed over so far.
#[derive(Default)]
struct FirstChunks {
    /// Whether each partition handed over its first chunk, by partition.
    handed: Mutex<Vec<bool>>,
    /// The streams parked until the partitions that have not handed theirs over do.
    wakers: Mutex<Vec<Waker>>,
}

impl FirstChunks {
    fn new(partitions: usize) -> Arc<Self> {
        Arc::new(Self {
            handed: Mutex::new(vec![false; partitions]),
            wakers: Mutex::new(Vec::new()),
        })
    }

    /// Records that `partition` handed over its first chunk, and wakes what waited for it.
    fn handed(&self, partition: usize) {
        self.handed
            .lock()
            .unwrap_or_else(|error| error.into_inner())[partition] = true;
        for waker in std::mem::take(
            &mut *self
                .wakers
                .lock()
                .unwrap_or_else(|error| error.into_inner()),
        ) {
            waker.wake();
        }
    }

    /// Whether every partition handed over its first chunk.
    fn all(&self) -> bool {
        self.handed
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .all(|handed| *handed)
    }

    /// Parks `cx` until every partition has, unless they already have.
    fn wait(&self, cx: &Context<'_>) {
        if self.all() {
            return;
        }
        self.wakers
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(cx.waker().clone());
        if self.all() {
            // The last partition handed its chunk over between the two reads, so its wake did not
            // find this waker.
            cx.waker().wake_by_ref();
        }
    }
}

/// A child each of whose partitions hands over a second chunk only once every partition handed over a
/// first.
///
/// Progress through one partition's chain requires every partition to have progressed too. This is
/// a rendezvous between inputs, not a stopwatch: nothing depends on two threads running at once.
struct Together {
    steps: Vec<Vec<Step>>,
    chunks: Arc<FirstChunks>,
    series_schema: Arc<SeriesSchema>,
    properties: Arc<PlanProperties>,
}

impl Together {
    /// The child over `steps`, one partition per list.
    fn plan(steps: Vec<Vec<Step>>) -> Arc<dyn ExecutionPlan> {
        let series_schema = schema();
        let frame = pack_evaluated_series(
            Box::pin(futures::stream::empty()),
            Arc::clone(&series_schema),
        )
        .expect("an evaluated schema declares a frame")
        .schema();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(frame),
            Partitioning::UnknownPartitioning(steps.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Arc::new(Self {
            chunks: FirstChunks::new(steps.len()),
            steps,
            series_schema,
            properties,
        })
    }
}

impl fmt::Debug for Together {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Together")
            .field("partitions", &self.steps.len())
            .finish()
    }
}

impl DisplayAs for Together {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "Together: partitions={}", self.steps.len())
    }
}

impl ExecutionPlan for Together {
    fn name(&self) -> &str {
        "Together"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        assert!(children.is_empty(), "a together child has no children");
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let steps = self.steps.get(partition).cloned().ok_or_else(|| {
            DataFusionError::Execution(format!("together child has no partition {partition}"))
        })?;
        pack_evaluated_series(
            Box::pin(TogetherScript {
                partition,
                steps: steps.into_iter(),
                chunks: Arc::clone(&self.chunks),
                handed: false,
            }),
            Arc::clone(&self.series_schema),
        )
    }
}

/// One partition of a [`Together`] child.
struct TogetherScript {
    partition: usize,
    steps: std::vec::IntoIter<Step>,
    chunks: Arc<FirstChunks>,
    /// Whether this partition handed over its first chunk already.
    handed: bool,
}

impl Stream for TogetherScript {
    type Item = Result<(Series, bool), BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        if this.handed {
            // Everything after the first chunk waits for the other partitions to have handed theirs
            // over, so this stream cannot be read past its first chunk on its own.
            this.chunks.wait(cx);
            if !this.chunks.all() {
                return Poll::Pending;
            }
        }
        match this.steps.next() {
            Some(step) => {
                if !this.handed {
                    this.handed = true;
                    this.chunks.handed(this.partition);
                }
                Poll::Ready(Some(step_answer(step)))
            }
            None => Poll::Ready(None),
        }
    }
}

/// A child whose partitions hand over their `before` steps, wait until every partition got that far,
/// and only then hand over their `after` steps.
///
/// The barrier sits between the two halves rather than before a partition's first chunk, which is what
/// [`Together`] gates: what a test of the failure path needs is two partitions that fail *at once*.
/// Every partition that gets to the gate is parked until the last one arrives, so the `after` steps of
/// two partitions are handed over in the same wake - which is the interleaving
/// [`two_failures_hand_every_output_one_error`] is built on. The gate itself is [`FirstChunks`], the
/// all-arrived barrier [`Together`] already uses, since "has every partition got here" is what both
/// need.
struct Simultaneous {
    partitions: Vec<GatedPartition>,
    arrived: Arc<FirstChunks>,
    series_schema: Arc<SeriesSchema>,
    properties: Arc<PlanProperties>,
}

impl Simultaneous {
    /// The child over `partitions`, and the barrier every one of them waits at.
    fn plan(partitions: Vec<GatedPartition>) -> (Arc<dyn ExecutionPlan>, Arc<FirstChunks>) {
        let series_schema = schema();
        let frame = pack_evaluated_series(
            Box::pin(futures::stream::empty()),
            Arc::clone(&series_schema),
        )
        .expect("an evaluated schema declares a frame")
        .schema();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(frame),
            Partitioning::UnknownPartitioning(partitions.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        let arrived = FirstChunks::new(partitions.len());
        let child = Arc::new(Self {
            partitions,
            arrived: Arc::clone(&arrived),
            series_schema,
            properties,
        });
        (child, arrived)
    }
}

impl fmt::Debug for Simultaneous {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Simultaneous")
            .field("partitions", &self.partitions.len())
            .finish()
    }
}

impl DisplayAs for Simultaneous {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "Simultaneous: partitions={}", self.partitions.len())
    }
}

impl ExecutionPlan for Simultaneous {
    fn name(&self) -> &str {
        "Simultaneous"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        assert!(children.is_empty(), "a simultaneous child has no children");
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let steps = self.partitions.get(partition).cloned().ok_or_else(|| {
            DataFusionError::Execution(format!("simultaneous child has no partition {partition}"))
        })?;
        pack_evaluated_series(
            Box::pin(SimultaneousScript {
                partition,
                before: steps.before.into_iter(),
                after: steps.after.into_iter(),
                arrived: Arc::clone(&self.arrived),
                arrived_here: false,
            }),
            Arc::clone(&self.series_schema),
        )
    }
}

/// One partition of a [`Simultaneous`] child.
struct SimultaneousScript {
    partition: usize,
    before: std::vec::IntoIter<Step>,
    after: std::vec::IntoIter<Step>,
    arrived: Arc<FirstChunks>,
    /// Whether this partition already reached the barrier.
    arrived_here: bool,
}

impl Stream for SimultaneousScript {
    type Item = Result<(Series, bool), BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        if let Some(step) = this.before.next() {
            return Poll::Ready(Some(step_answer(step)));
        }
        if !this.arrived_here {
            // Everything after the first half waits for every partition to have handed its first half
            // over, so the second halves - and the failures among them - leave two tasks together.
            this.arrived_here = true;
            this.arrived.handed(this.partition);
        }
        this.arrived.wait(cx);
        if !this.arrived.all() {
            return Poll::Pending;
        }
        Poll::Ready(this.after.next().map(step_answer))
    }
}

/// Every output of `plan`, read to its end at once: the chunks each delivered and the errors each
/// reported.
///
/// All partitions are drained concurrently, which is what an executor does and what a failed read
/// needs: an input suspends on an output nobody is draining, so an output drained to its end before
/// the next one is started would wait for a sender that is waiting for that next output.
async fn every_output_until_end(
    plan: &Arc<SeriesRepartitionExec>,
    context: &Arc<TaskContext>,
) -> Vec<(Vec<(Series, bool)>, Vec<String>)> {
    let mut streams = Vec::new();
    for partition in 0..plan.outputs() {
        streams.push(
            unpack_evaluated_series(
                plan.execute(partition, Arc::clone(context))
                    .expect("the output executes"),
                None,
            )
            .expect("the output declares an evaluated frame"),
        );
    }
    futures::future::join_all(streams.iter_mut().map(until_end)).await
}

#[tokio::test]
async fn coalesce_local_limit_reads_repartitioned_outputs_without_sibling_error() {
    let first = labels_routed_to(0, 1).remove(0);
    let second = labels_routed_to(1, 1).remove(0);
    let steps = labels_routed_to(0, 3 * TEST_FIXTURE_VOLUME)
        .into_iter()
        .map(|metric| Step::chunk(chunk(metric, 0, 1, true)))
        .chain(std::iter::once(Step::chunk(chunk(
            second.clone(),
            0,
            1,
            true,
        ))))
        .collect();
    let routed: Arc<dyn ExecutionPlan> = node(
        Fragile::plan(
            vec![Behaviour::OneAtATime(steps)],
            Arc::new(Switch::default()),
        )
        .plan,
        2,
    );
    let limited: Arc<dyn ExecutionPlan> = Arc::new(LocalLimitExec::new(routed, 1));
    let plan = CoalescePartitionsExec::new(limited);
    let context = SessionContext::default().task_ctx();
    let stream = plan.execute(0, context).expect("coalesce executes");
    let mut output = unpack_evaluated_series(stream, None).expect("evaluated frame");
    let (chunks, errors) = tokio::time::timeout(Duration::from_secs(10), until_end(&mut output))
        .await
        .expect("coalesce and local limit make progress");
    assert!(errors.is_empty(), "no sibling output failure: {errors:?}");
    assert_eq!(
        2,
        chunks.len(),
        "LocalLimit fetch is applied per output partition"
    );
    let mut actual = chunks
        .iter()
        .map(|(series, is_last)| {
            assert_eq!(
                1,
                series.time.len(),
                "fetch=1 returns one row per partition"
            );
            assert!(*is_last, "each returned series is closed");
            series.metric.clone()
        })
        .collect::<Vec<_>>();
    actual.sort_by_key(|metric| metric.tags()[0].value.clone());
    let mut expected = vec![first, second];
    expected.sort_by_key(|metric| metric.tags()[0].value.clone());
    assert_eq!(
        expected, actual,
        "both routed outputs contribute their expected series"
    );
}

#[test]
fn ensure_cooperative_preserves_direct_repartition_child() {
    let repartition: Arc<dyn ExecutionPlan> = node(Scripted::plan(vec![Vec::new()]), 2);
    let optimized = EnsureCooperative::new()
        .optimize(repartition, &ConfigOptions::default())
        .expect("EnsureCooperative optimizes the plan");
    assert!(
        optimized.downcast_ref::<SeriesRepartitionExec>().is_some(),
        "EnsureCooperative preserves the bare repartition root"
    );
}
/// Regression: a paced producer fills output 0 with closed chains, then fails while output 1 is drained first.
#[tokio::test]
async fn failure_after_filling_another_output_does_not_hang_early_output_drain() {
    const MESSAGE: &str = "failure after output 0 filled";
    let mut steps = labels_routed_to(0, TEST_FIXTURE_VOLUME - 1)
        .into_iter()
        .map(|metric| Step::chunk(chunk(metric, 0, 1, true)))
        .collect::<Vec<_>>();
    steps.extend(
        labels_routed_to(1, 1)
            .into_iter()
            .map(|metric| Step::chunk(chunk(metric, 0, 1, true))),
    );
    steps.push(Step::ErrorSaying(MESSAGE));
    let plan = node(
        Fragile::plan(
            vec![Behaviour::OneAtATime(steps)],
            Arc::new(Switch::default()),
        )
        .plan,
        2,
    );
    let context = SessionContext::default().task_ctx();
    let mut output1 = output_of(&plan, 1, &context);
    let (chunks, errors) = tokio::time::timeout(Duration::from_secs(10), until_end(&mut output1))
        .await
        .expect("output 1 sees the failure despite output 0 being full");
    let _ = chunks;
    assert_eq!(1, errors.len());
    assert!(errors[0].contains(MESSAGE));
}

/// two partitions that fail at the same moment leave every output exactly one error.
///
/// Between them the two partitions hand over [`TEST_FIXTURE_VOLUME`] chains per output before they fail,
/// so both queues are exactly full and then two failures land on a read whose outputs are not being
/// drained. A failing task cancels its peers while another input may be suspended on a full output;
/// every output must still learn the elected failure from the shared ending record when senders close.
///
/// Both partitions are known to have reached the barrier before either failed, which is what makes
/// the two failures overlap rather than merely possible: neither can hand its failing step over before
/// the other has arrived. What is asserted is that the first failure and its error are retained, and
/// every output ends with exactly one error, never a clean end and never a duplicate.
#[tokio::test]
async fn two_failures_hand_every_output_one_error() {
    const FIRST: &str = "the first partition failed";
    const SECOND: &str = "the second partition failed";
    assert_eq!(
        0,
        TEST_FIXTURE_VOLUME % 2,
        "the chains of one queue are split evenly between the two partitions"
    );
    let half = TEST_FIXTURE_VOLUME / 2;
    let to_first = labels_routed_to(0, TEST_FIXTURE_VOLUME);
    let to_second = labels_routed_to(1, TEST_FIXTURE_VOLUME);

    /// The steps of one partition: its half of every output's queue, a place at the barrier, and then
    /// its failure.
    ///
    /// The failure is what comes *after* the barrier, which is the whole point: a partition cannot hand
    /// it over until every partition has handed over its chains, so the two failures are released into
    /// the same wake instead of one of them landing while the other task is still reading.
    fn failing(
        first: &[SeriesLabels],
        second: &[SeriesLabels],
        message: &'static str,
    ) -> GatedPartition {
        GatedPartition {
            before: first
                .iter()
                .chain(second)
                .map(|labels| Step::chunk(chunk(labels.clone(), 0, 1, true)))
                .collect::<Vec<_>>(),
            after: vec![Step::ErrorSaying(message)],
            switch: None,
        }
    }

    let (child, arrived) = Simultaneous::plan(vec![
        failing(&to_first[..half], &to_second[..half], FIRST),
        failing(&to_first[half..], &to_second[half..], SECOND),
    ]);
    let plan = node(child, 2);
    let context = SessionContext::default().task_ctx();
    // Claim both owners before reading so either partition can observe its terminal failure.
    let mut streams = Vec::new();
    for partition in 0..plan.outputs() {
        streams.push(
            unpack_evaluated_series(
                plan.execute(partition, Arc::clone(&context))
                    .expect("the output executes"),
                None,
            )
            .expect("the output declares an evaluated frame"),
        );
    }
    let execution = execution(&plan);

    wait_until("both partitions reach the failure path", || arrived.all()).await;
    wait_until("the read records its failure", || {
        matches!(execution.ending(), Ending::Failed(_))
    })
    .await;
    let recorded = match execution.ending() {
        Ending::Failed(error) => error,
        other => panic!("the read records its failure: {other:?}"),
    };
    let expected = recorded.to_string();
    let named = [FIRST, SECOND]
        .into_iter()
        .find(|message| expected.contains(message))
        .unwrap_or_else(|| panic!("the recorded error names one of the two failures: {expected}"));

    let outputs = tokio::time::timeout(
        Duration::from_secs(10),
        futures::future::join_all(streams.iter_mut().map(until_end)),
    )
    .await
    .expect("a read with two failures still ends every output");

    for (partition, (chunks, errors)) in outputs.iter().enumerate() {
        assert_eq!(
            1,
            errors.len(),
            "output partition {partition} reports the failure once, not once per failing input: \
             {errors:?}"
        );
        // The item and the recorded error are the same failure, but each is wrapped by the path it
        // takes out of the read, so what they are compared on is the failure they name.
        assert!(
            errors[0].contains(named),
            "output partition {partition} reports the failure the read recorded ({named}), not {}",
            errors[0]
        );
        assert_chains_are_whole(chunks);
    }
}

/// a task attached after the failure is swept is aborted instead of waited on.
///
/// Partition 0 fails on its first poll, which can happen before the tasks of the partitions after it
/// are even spawned - and a handle attached after the failure's sweep is one the sweep cannot see.
/// Stored as live, it waits on a child that never answers while holding a sender to every output, so
/// the queues never close and both outputs hang here. The other partitions are gated on a switch
/// nothing opens, so the only way this test finishes is those tasks being aborted: their streams
/// record the drop an aborted task's future goes through.
#[tokio::test]
async fn a_partition_failing_before_its_peers_are_attached_still_ends_every_output() {
    const MESSAGE: &str = "the first partition failed at once";
    let switch = Arc::new(Switch::default());
    let dropped = Arc::new(AtomicUsize::new(0));
    let shared = labels("web-1");
    // Ten partitions, so the failing one's sweep runs while the later ones are still being spawned and
    // attached: the read must stop them anyway. `read_partitions` counts them with the failing one.
    let mut partitions = vec![GatedPartition::open(vec![Step::ErrorSaying(MESSAGE)])];
    partitions.extend(
        (0..10).map(|_| GatedPartition::held(vec![Step::chunk(chunk(shared.clone(), 0, 1, true))])),
    );
    let read_partitions = partitions.len();
    let child = Gated::plan(partitions, switch, Arc::clone(&dropped));
    let plan = node(child, 2);
    let context = SessionContext::default().task_ctx();

    let outputs = tokio::time::timeout(
        Duration::from_secs(10),
        every_output_until_end(&plan, &context),
    )
    .await
    .expect(
        "a read whose first partition fails before its peers were attached still ends: a task left \
         waiting on a child holds a sender to every output, so nothing would close",
    );
    for (partition, (_, errors)) in outputs.iter().enumerate() {
        assert_eq!(
            1,
            errors.len(),
            "output partition {partition} reports the failure: {errors:?}"
        );
        assert!(
            errors[0].contains(MESSAGE),
            "output partition {partition} ended with {}",
            errors[0]
        );
    }
    wait_until("every child partition's stream is dropped", || {
        dropped.load(Ordering::SeqCst) == read_partitions
    })
    .await;
    // The read is over, which it could not be if a task attached after the sweep were still holding
    // a sender to every output.
    let execution = execution(&plan);
    wait_until("the read stops", || producer_stopped(&execution)).await;
}

/// A route task attached after failure is aborted by `attach`; its captured report guard counts the
/// partition out when the aborted future is dropped.
#[tokio::test]
async fn a_task_attached_after_the_failure_is_aborted_and_not_counted() {
    let ending = Arc::new(Mutex::new(Ending::default()));
    let inputs = Inputs::new(Arc::clone(&ending), 2, Weak::<SharedExecution>::new());
    let error = Arc::new(DataFusionError::Execution(
        "the first partition failed".to_string(),
    ));
    inputs.failed(0, &error);
    assert_eq!(
        1,
        inputs.live(),
        "the late partition is still counted until its guard drops"
    );

    let task = tokio::spawn(route_partition(
        RoutedInput::Evaluated(Box::pin(futures::stream::pending())),
        1,
        Routing::new(1),
        Vec::new(),
        MemoryConsumer::new("late-attach-test")
            .register(&(Arc::new(GreedyMemoryPool::new(1 << 20)) as Arc<dyn MemoryPool>)),
        Arc::clone(&inputs),
        ReportIfUnreported {
            inputs: Arc::clone(&inputs),
            partition: 1,
            reported: false,
        },
    ));
    inputs.attach(1, task);
    assert!(
        inputs
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .tasks[1]
            .is_none(),
        "the task of a cancelled read is aborted rather than stored as one to wait for"
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        while inputs.live() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the attached route task's ReportIfUnreported guard counts it out on abort");
}

/// a read that finished owes its outputs no error, whatever a failure elsewhere recorded.
///
/// A read that finished records `Finished`, and every output ends cleanly once.
#[tokio::test]
async fn a_finished_read_ends_cleanly_and_owes_no_error() {
    let shared = labels("web-1");
    let child = Scripted::plan(vec![
        vec![Step::chunk(chunk(shared.clone(), 0, 2, true))],
        vec![Step::chunk(chunk(shared.clone(), 8, 2, true))],
    ]);
    let plan = node(child, 2);
    let context = SessionContext::default().task_ctx();

    let outputs = tokio::time::timeout(
        Duration::from_secs(10),
        every_output_until_end(&plan, &context),
    )
    .await
    .expect("both partitions end, so both outputs end");
    // Read after the outputs are done, so the read has recorded everything it is going to.
    let execution = execution(&plan);
    for (partition, (chunks, errors)) in outputs.iter().enumerate() {
        assert!(
            errors.is_empty(),
            "output partition {partition} reported {errors:?} for a read that finished"
        );
        assert_chains_are_whole(chunks);
    }
    assert!(
        matches!(execution.ending(), Ending::Finished),
        "a read that finished records no failure for its outputs to report"
    );
}

/// a failure recorded before outputs are drained is still reported by every output.
///
/// One input fills the output queues and then fails while the outputs are not being drained. The
/// failure cancels its peers; outputs observe closed queues and must report the recorded terminal
/// error exactly once rather than ending cleanly.
#[tokio::test]
async fn a_recorded_failure_reaches_every_output_once() {
    const MESSAGE: &str = "the partition failed";
    let to_zero = labels_routed_to(0, TEST_FIXTURE_VOLUME);
    let to_one = labels_routed_to(1, TEST_FIXTURE_VOLUME);
    let mut steps = to_zero
        .into_iter()
        .zip(to_one)
        .flat_map(|(zero, one)| [zero, one].map(|metric| Step::chunk(chunk(metric, 0, 1, true))))
        .collect::<Vec<_>>();
    steps.push(Step::ErrorSaying(MESSAGE));
    // The producer is gated so it cannot reach its error before this test owns both queues. A
    // failure drains every queue nobody has claimed yet, so a producer that ran first would release
    // the very charges this test reads, and the retention assertion below could never hold.
    let switch = Arc::new(Switch::default());
    let plan = node(
        Fragile::plan(vec![Behaviour::Gated(steps)], Arc::clone(&switch)).plan,
        2,
    );
    let pool = Arc::new(GreedyMemoryPool::new(usize::MAX));
    let context = task_context(&pool);
    // Claim internal queues so assertions observe retained chains, not encoded frame batches.
    let child = plan.children()[0].clone();
    let shared =
        SharedExecution::start(&child, Routing::new(2), Arc::clone(&schema()), &context).unwrap();
    let mut streams = Vec::new();
    for partition in 0..2 {
        let (receiver, lease) = SharedExecution::claim(&shared, partition).expect("output claim");
        streams.push(Box::pin(EvaluatedOutputStream {
            inner: Some(Box::pin(RepartitionStream::new(
                receiver,
                lease,
                Arc::clone(&schema()),
                &(Arc::clone(&pool) as Arc<dyn MemoryPool>),
                2,
                SeriesOrdering::Contiguous,
            ))),
        }) as SendableEvaluatedSeriesStream);
    }
    let execution = shared;

    // Both queues are claimed, so the gates below are the only thing between the queued prefix and
    // the failure: what is retained now is retained until this test drains it.
    assert!(
        !switch.is_open(),
        "the producer is held before its error, so nothing drained the queues yet"
    );
    wait_until("queued output chains retain memory before drain", || {
        pool.reserved() > 0
    })
    .await;
    switch.open();
    wait_until(
        "the read records its failure while no output is drained",
        || matches!(execution.ending(), Ending::Failed(_)),
    )
    .await;
    let outputs = tokio::time::timeout(
        Duration::from_secs(10),
        futures::future::join_all(streams.iter_mut().map(until_end)),
    )
    .await
    .expect("a failed read still ends every output");
    for (partition, (chunks, errors)) in outputs.iter().enumerate() {
        assert!(
            chunks.is_empty(),
            "no incomplete owner series escapes on failure"
        );
        assert_eq!(
            1,
            errors.len(),
            "output partition {partition} reports the failure the read recorded exactly once: \
             {errors:?}"
        );
        assert!(
            errors[0].contains(MESSAGE),
            "output partition {partition} ended with {} rather than the failure",
            errors[0]
        );
    }
    assert_eq!(0, pool.reserved(), "draining releases all queued charges");
}

/// What one partition of a [`Fragile`] child does when it is polled.
#[derive(Clone)]
enum Behaviour {
    /// Hands `steps` over only once the switch is opened, then ends.
    Held(Vec<Step>),
    /// Panics on the first poll, which is a task dying where no arm of the read's ending match is
    /// reached: the unwind takes the whole future with it.
    Panics,
    /// Never answers, so the task reading it stays parked on it until it is aborted.
    Parked,
    /// Hands one step over per poll, staying pending in between, so each of them becomes a frame
    /// batch of its own.
    ///
    /// The frame boundary coalesces what a producer hands over in one poll into one batch - the
    /// builder only flushes when the source is pending or done - so a child that hands several chunks
    /// over back to back reaches a consumer as ONE chunk. A chunk per poll is what keeps them apart,
    /// and a test about how a chain accumulates is about chunks rather than about batches.
    OneAtATime(Vec<Step>),
    /// Delivers one chunk, waits until the test opens the gate, then delivers the rest.
    Gated(Vec<Step>),
}

/// A child whose partitions stop in ways of their own: one dies or is parked, another stays parked.
///
/// A test of what a read does with a producer that dies has to be able to make exactly one producer
/// die, and to see that the others really were reading and really were stopped. [`Behaviour::Panics`]
/// is the death nothing in the read reaches, [`Behaviour::Parked`] is the peer that would wait for a
/// child that never answers, and the counters answer the two questions a timeout cannot: `polled`
/// says a partition was being read rather than skipped, and `dropped` says the read let the stream of
/// a parked partition go - which is what an aborted task does.
struct Fragile {
    behaviours: Vec<Behaviour>,
    switch: Arc<Switch>,
    /// Whether each partition has been polled once, which every partition waits for before it does
    /// what its behaviour says.
    ///
    /// [`Behaviour::Panics`] needs it, and needs it for the peer rather than for itself: a producer
    /// that dies on its very first poll can die before the task reading the other partition has been
    /// polled at all, and then the peer is aborted without ever having read anything - which is a read
    /// that never had a parked input to hang on. Waiting here is what makes the shape the test is
    /// about exist before the death happens.
    reading: Arc<FirstChunks>,
    polled: Vec<Arc<AtomicUsize>>,
    dropped: Vec<Arc<AtomicUsize>>,
    series_schema: Arc<SeriesSchema>,
    properties: Arc<PlanProperties>,
}

/// A [`Fragile`] child, and the two counters of every one of its partitions.
struct FragileChild {
    plan: Arc<dyn ExecutionPlan>,
    /// How many times each partition was polled.
    polled: Vec<Arc<AtomicUsize>>,
    /// How many of each partition's streams were dropped.
    dropped: Vec<Arc<AtomicUsize>>,
}

impl Fragile {
    /// The child over `behaviours`, one partition per behaviour, gated on `switch`.
    fn plan(behaviours: Vec<Behaviour>, switch: Arc<Switch>) -> FragileChild {
        let series_schema = schema();
        let frame = pack_evaluated_series(
            Box::pin(futures::stream::empty()),
            Arc::clone(&series_schema),
        )
        .expect("an evaluated schema declares a frame")
        .schema();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(frame),
            Partitioning::UnknownPartitioning(behaviours.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        let polled = (0..behaviours.len())
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect::<Vec<_>>();
        let dropped = (0..behaviours.len())
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect::<Vec<_>>();
        FragileChild {
            plan: Arc::new(Self {
                behaviours: behaviours.clone(),
                switch,
                reading: FirstChunks::new(behaviours.len()),
                polled: polled.clone(),
                dropped: dropped.clone(),
                series_schema,
                properties,
            }),
            polled,
            dropped,
        }
    }
}

impl fmt::Debug for Fragile {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Fragile")
            .field("partitions", &self.behaviours.len())
            .finish()
    }
}

impl DisplayAs for Fragile {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "Fragile: partitions={}", self.behaviours.len())
    }
}

impl ExecutionPlan for Fragile {
    fn name(&self) -> &str {
        "Fragile"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        assert!(children.is_empty(), "a fragile child has no children");
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let behaviour = self.behaviours.get(partition).cloned().ok_or_else(|| {
            DataFusionError::Execution(format!("fragile child has no partition {partition}"))
        })?;
        let steps = match &behaviour {
            Behaviour::Held(steps) | Behaviour::OneAtATime(steps) | Behaviour::Gated(steps) => {
                steps.clone()
            }
            Behaviour::Panics | Behaviour::Parked => Vec::new(),
        };
        pack_evaluated_series(
            Box::pin(FragileScript {
                partition,
                behaviour,
                steps: steps.into_iter(),
                switch: Arc::clone(&self.switch),
                reading: Arc::clone(&self.reading),
                polled: Arc::clone(&self.polled[partition]),
                dropped: Arc::clone(&self.dropped[partition]),
                stayed_pending: false,
            }),
            Arc::clone(&self.series_schema),
        )
    }
}

/// One partition of a [`Fragile`] child.
struct FragileScript {
    partition: usize,
    behaviour: Behaviour,
    steps: std::vec::IntoIter<Step>,
    switch: Arc<Switch>,
    reading: Arc<FirstChunks>,
    polled: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
    /// Whether a [`Behaviour::OneAtATime`] partition has already stayed pending since its last chunk.
    stayed_pending: bool,
}

impl Stream for FragileScript {
    type Item = Result<(Series, bool), BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        this.polled.fetch_add(1, Ordering::SeqCst);
        // Every partition is read once before any of them does what it is here for, so a partition
        // that dies cannot do so before its peer was read.
        this.reading.handed(this.partition);
        this.reading.wait(cx);
        if !this.reading.all() {
            return Poll::Pending;
        }
        match &this.behaviour {
            Behaviour::Held(_) => {
                this.switch.wait(cx);
                if !this.switch.is_open() {
                    return Poll::Pending;
                }
                Poll::Ready(this.steps.next().map(step_answer))
            }
            Behaviour::Gated(_) => {
                if !this.stayed_pending {
                    this.stayed_pending = true;
                    return Poll::Ready(this.steps.next().map(step_answer));
                }
                this.switch.wait(cx);
                if !this.switch.is_open() {
                    return Poll::Pending;
                }
                Poll::Ready(this.steps.next().map(step_answer))
            }
            // The child's own poll, which runs inside the task reading it: what a producer that dies
            // under a read does is exactly this, and nothing of the read is told about it.
            Behaviour::Panics => panic!("the fragile child partition panicked while being read"),
            // Parked on a source that never answers. No waker is registered because nothing can make
            // this partition answer: the only way it stops is the read aborting it.
            Behaviour::Parked => Poll::Pending,
            Behaviour::OneAtATime(_) => {
                if !this.stayed_pending {
                    // One pending poll between two chunks is what makes the frame boundary flush,
                    // and the wake is this stream's own: nothing outside it has to poll again for the
                    // next chunk to arrive.
                    this.stayed_pending = true;
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                this.stayed_pending = false;
                Poll::Ready(this.steps.next().map(step_answer))
            }
        }
    }
}

impl Drop for FragileScript {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

struct DropCounter(Arc<AtomicUsize>);

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// A task guard captured into an unpolled producer future still reports cancellation on drop.
///
/// Constructing `route_partition` synchronously constructs its captured `ReportIfUnreported` guard;
/// dropping the returned future before first poll therefore runs the failure bookkeeping. A parked
/// sibling then stops, all output sender clones close, and the output streams report the same error.
#[tokio::test]
async fn dropping_unpolled_route_future_ends_outputs_and_cancels_peer() {
    let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
    let ending = Arc::new(Mutex::new(Ending::default()));
    let inputs = Inputs::new(Arc::clone(&ending), 2, Weak::<SharedExecution>::new());
    let mut senders = Vec::new();
    let mut receivers = Vec::new();
    for _ in 0..2 {
        let (sender, receiver) = mpsc::unbounded_channel();
        senders.push(sender);
        receivers.push(Some(receiver));
    }
    let shared = Arc::new(SharedExecution {
        running: Mutex::new(Running {
            receivers,
            inputs: Arc::clone(&inputs),
            claimed: 0,
            remaining: 2,
        }),
        ending,
    });
    let series_schema = Arc::clone(&schema());
    let mut streams = (0..2)
        .map(|partition| {
            let (receiver, lease) =
                SharedExecution::claim(&shared, partition).expect("output claim");
            let frames = pack_evaluated_series(
                Box::pin(EvaluatedOutputStream {
                    inner: Some(Box::pin(RepartitionStream::new(
                        receiver,
                        lease,
                        Arc::clone(&schema()),
                        &(Arc::clone(&pool) as Arc<dyn MemoryPool>),
                        2,
                        SeriesOrdering::Contiguous,
                    ))),
                }),
                Arc::clone(&series_schema),
            )
            .expect("output frame builds");
            unpack_evaluated_series(frames, None).expect("evaluated output decodes")
        })
        .collect::<Vec<_>>();
    let peer_polled = Arc::new(AtomicUsize::new(0));
    let peer_dropped = Arc::new(AtomicUsize::new(0));
    let peer_poll_count = Arc::clone(&peer_polled);
    let peer_guard = DropCounter(Arc::clone(&peer_dropped));
    let peer_stream = futures::stream::poll_fn(move |_| {
        let _ = &peer_guard;
        peer_poll_count.fetch_add(1, Ordering::SeqCst);
        Poll::Pending
    });
    let peer = route_partition(
        RoutedInput::Evaluated(Box::pin(peer_stream)),
        1,
        Routing::new(2),
        senders.clone(),
        MemoryConsumer::new("parked-peer-test")
            .register(&(Arc::clone(&pool) as Arc<dyn MemoryPool>)),
        Arc::clone(&inputs),
        ReportIfUnreported {
            inputs: Arc::clone(&inputs),
            partition: 1,
            reported: false,
        },
    );
    let task = common_runtime::spawn_query(peer);
    inputs.attach(1, task);
    wait_until("parked sibling is actively holding output senders", || {
        peer_polled.load(Ordering::SeqCst) > 0
    })
    .await;
    let report = ReportIfUnreported {
        inputs: Arc::clone(&inputs),
        partition: 0,
        reported: false,
    };
    let unpolled = route_partition(
        RoutedInput::Evaluated(Box::pin(futures::stream::pending())),
        0,
        Routing::new(2),
        senders.clone(),
        MemoryConsumer::new("unpolled-route-test")
            .register(&(Arc::clone(&pool) as Arc<dyn MemoryPool>)),
        Arc::clone(&inputs),
        report,
    );
    drop(senders);
    drop(unpolled);

    let outputs = tokio::time::timeout(
        Duration::from_secs(10),
        futures::future::join_all(streams.iter_mut().map(until_end)),
    )
    .await
    .expect("unpolled producer drop closes each output");
    for (chunks, errors) in &outputs {
        assert!(chunks.is_empty());
        assert_eq!(1, errors.len());
        assert!(errors[0].contains("input partition 0 stopped without reporting an outcome"));
    }
    wait_until("parked sibling stream is dropped", || {
        peer_dropped.load(Ordering::SeqCst) == 1
    })
    .await;
    wait_until("producer tasks stopped", || producer_stopped(&shared)).await;
    wait_until("input memory released", || pool.reserved() == 0).await;
}

#[tokio::test]
async fn unpolled_route_drop_preserves_an_already_elected_failure() {
    let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
    const FIRST: &str = "the child failure was elected first";
    let ending = Arc::new(Mutex::new(Ending::default()));
    let inputs = Inputs::new(Arc::clone(&ending), 2, Weak::<SharedExecution>::new());
    let mut senders = Vec::new();
    let mut streams = Vec::new();
    let shared = {
        let mut receivers = Vec::new();
        for _ in 0..2 {
            let (sender, receiver) = mpsc::unbounded_channel();
            senders.push(sender);
            receivers.push(Some(receiver));
        }
        Arc::new(SharedExecution {
            running: Mutex::new(Running {
                receivers,
                inputs: Arc::clone(&inputs),
                claimed: 0,
                remaining: 2,
            }),
            ending,
        })
    };
    for partition in 0..2 {
        let (receiver, lease) = SharedExecution::claim(&shared, partition).expect("output claim");
        let frames = pack_evaluated_series(
            Box::pin(EvaluatedOutputStream {
                inner: Some(Box::pin(RepartitionStream::new(
                    receiver,
                    lease,
                    Arc::clone(&schema()),
                    &(Arc::clone(&pool) as Arc<dyn MemoryPool>),
                    2,
                    SeriesOrdering::Contiguous,
                ))),
            }),
            Arc::clone(&schema()),
        )
        .expect("output frame builds");
        streams.push(unpack_evaluated_series(frames, None).expect("evaluated output decodes"));
    }
    inputs.failed(1, &Arc::new(DataFusionError::Execution(FIRST.to_string())));
    let unpolled = route_partition(
        RoutedInput::Evaluated(Box::pin(futures::stream::pending())),
        0,
        Routing::new(2),
        senders.clone(),
        MemoryConsumer::new("unpolled-after-failure-test")
            .register(&(Arc::new(GreedyMemoryPool::new(1 << 20)) as Arc<dyn MemoryPool>)),
        Arc::clone(&inputs),
        ReportIfUnreported {
            inputs: Arc::clone(&inputs),
            partition: 0,
            reported: false,
        },
    );
    drop(senders);
    drop(unpolled);
    let outputs = tokio::time::timeout(
        Duration::from_secs(10),
        futures::future::join_all(streams.iter_mut().map(until_end)),
    )
    .await
    .expect("elected error closes both outputs");
    for (_, errors) in outputs {
        assert_eq!(1, errors.len());
        assert!(
            errors[0].contains(FIRST),
            "unexpected terminal error: {errors:?}"
        );
        assert!(!errors[0].contains("partition 0 stopped without reporting"));
    }
}

/// A continuously-ready decoded source is cooperatively budgeted while retaining a partial chain.
#[tokio::test]
async fn ready_chain_yields_after_a_charged_prefix_and_drop_releases_it() {
    const TOTAL: usize = 16_384;
    let count = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&count);
    let long_grid = Arc::new(
        EvalGrid::try_new(GridSpec::try_new(GRID_START, GRID_STEP, TOTAL as u64).unwrap()).unwrap(),
    );
    let long_schema = Arc::new(SeriesSchema {
        fields: Arc::from([FieldSpec {
            name: Arc::from("value"),
        }]),
        eval_grid: Some(Arc::clone(&long_grid)),
    });
    let source = futures::stream::poll_fn(move |_| {
        let position = seen.fetch_add(1, Ordering::SeqCst);
        Poll::Ready((position < TOTAL).then(|| {
            let time =
                TimeAxis::from_grid_spans(long_grid.clone(), vec![GridSpan::new(position, 1)])
                    .unwrap();
            let fields = SeriesFields::one(Samples::from_float(Float64Array::from(vec![
                position as f64,
            ])));
            let series =
                Series::try_new(&long_schema, labels("cooperative-prefix"), time, fields).unwrap();
            Ok((series, position + 1 == TOTAL))
        }))
    });
    let mut source: SendableEvaluatedSeriesStream = Box::pin(source);
    let pool = Arc::new(GreedyMemoryPool::new(usize::MAX));
    let reservation = MemoryConsumer::new("cooperative-prefix-test")
        .register(&(Arc::clone(&pool) as Arc<dyn MemoryPool>));
    let (sender, receiver) = mpsc::unbounded_channel();
    let routing = Routing::new(1);
    let senders = [sender];
    let mut read = Box::pin(read_evaluated_chains(
        &mut source,
        0,
        &routing,
        &senders,
        &reservation,
    ));
    let result = std::future::poll_fn(|cx| Poll::Ready(read.as_mut().poll(cx))).await;
    assert!(
        result.is_pending(),
        "one poll reaches the cooperative budget, not chain end"
    );
    let prefix = count.load(Ordering::SeqCst);
    assert!(
        prefix > 0 && prefix < TOTAL,
        "observed {prefix} of {TOTAL} chunks"
    );
    assert!(
        pool.reserved() > 0,
        "partial chain owns a memory reservation"
    );
    drop(read);
    assert_eq!(
        0,
        pool.reserved(),
        "dropping the read releases its partial chain"
    );
    drop(receiver);
}

#[tokio::test(flavor = "current_thread")]
async fn sibling_failure_aborts_budget_yielded_route_and_releases_prefix() {
    const TOTAL: usize = 16_384;
    let count = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&count);
    let guard = DropCounter(Arc::clone(&dropped));
    let long_grid = Arc::new(
        EvalGrid::try_new(GridSpec::try_new(GRID_START, GRID_STEP, TOTAL as u64).unwrap()).unwrap(),
    );
    let long_schema = Arc::new(SeriesSchema {
        fields: Arc::from([FieldSpec {
            name: Arc::from("value"),
        }]),
        eval_grid: Some(Arc::clone(&long_grid)),
    });
    let source = futures::stream::poll_fn(move |_| {
        let _ = &guard;
        let position = seen.fetch_add(1, Ordering::SeqCst);
        Poll::Ready((position < TOTAL).then(|| {
            let time =
                TimeAxis::from_grid_spans(long_grid.clone(), vec![GridSpan::new(position, 1)])
                    .unwrap();
            let fields = SeriesFields::one(Samples::from_float(Float64Array::from(vec![
                position as f64,
            ])));
            let series =
                Series::try_new(&long_schema, labels("aborted-prefix"), time, fields).unwrap();
            Ok((series, position + 1 == TOTAL))
        }))
    });
    let source: SendableEvaluatedSeriesStream = Box::pin(source);
    let pool = Arc::new(GreedyMemoryPool::new(usize::MAX));
    let ending = Arc::new(Mutex::new(Ending::default()));
    let inputs = Inputs::new(Arc::clone(&ending), 2, Weak::<SharedExecution>::new());
    let (sender, receiver) = mpsc::unbounded_channel();
    let shared = Arc::new(SharedExecution {
        running: Mutex::new(Running {
            receivers: vec![Some(receiver)],
            inputs: Arc::clone(&inputs),
            claimed: 0,
            remaining: 1,
        }),
        ending: Arc::clone(&ending),
    });
    let (receiver, lease) = SharedExecution::claim(&shared, 0).expect("claim output");
    let mut output: SendableEvaluatedSeriesStream = Box::pin(EvaluatedOutputStream {
        inner: Some(Box::pin(RepartitionStream::new(
            receiver,
            lease,
            Arc::clone(&schema()),
            &(Arc::clone(&pool) as Arc<dyn MemoryPool>),
            1,
            SeriesOrdering::Contiguous,
        ))),
    });
    let future = route_partition(
        RoutedInput::Evaluated(source),
        0,
        Routing::new(1),
        vec![sender],
        MemoryConsumer::new("aborted-prefix-test")
            .register(&(Arc::clone(&pool) as Arc<dyn MemoryPool>)),
        Arc::clone(&inputs),
        ReportIfUnreported {
            inputs: Arc::clone(&inputs),
            partition: 0,
            reported: false,
        },
    );
    let task = tokio::spawn(future);
    inputs.attach(0, task);
    wait_until("route yields after charging a strict chunk prefix", || {
        let chunks = count.load(Ordering::SeqCst);
        chunks > 0 && chunks < TOTAL && pool.reserved() > 0
    })
    .await;
    let prefix = count.load(Ordering::SeqCst);
    assert!(prefix < TOTAL, "failure arrives before all {TOTAL} chunks");
    inputs.failed(
        1,
        &Arc::new(DataFusionError::Execution("sibling failure".into())),
    );
    wait_until("aborted route drops its decoded source", || {
        dropped.load(Ordering::SeqCst) == 1
    })
    .await;
    assert_eq!(
        prefix,
        count.load(Ordering::SeqCst),
        "no chunk is read after cancellation"
    );
    assert_eq!(
        0,
        pool.reserved(),
        "cancellation releases the retained prefix"
    );
    let (chunks, errors) = until_end(&mut output).await;
    assert!(chunks.is_empty());
    assert_eq!(
        1,
        errors.len(),
        "one terminal failure precedes EOF: {errors:?}"
    );
    assert!(
        errors[0].contains("sibling failure"),
        "first failure is preserved: {errors:?}"
    );
}

/// A partition whose task panics ends every output with the read's error, and stops its peers.
///
/// Two inputs and one output. Partition 0's child panics inside its `poll_next`, which unwinds the
/// task reading it: no arm of [`route_partition`]'s ending match runs, so nothing is recorded and
/// nothing stops the peer. Partition 1's child never answers, keeping its sender alive and preventing
/// the output from observing an ending. The timeout makes the missing terminal error visible.
///
/// The panic happens on a task of the read, never on this test's, so the unwind is the runtime's to
/// catch and what the test sees is its effect: the output ends with the read's error instead of
/// waiting, the parked peer's child is dropped because its task was aborted, and nothing the read
/// reserved outlives it.
#[tokio::test]
async fn a_panicking_input_ends_every_output_and_stops_its_peers() {
    let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
    let context = task_context(&pool);
    let child = Fragile::plan(
        vec![Behaviour::Panics, Behaviour::Parked],
        Arc::new(Switch::default()),
    );
    let plan = node(child.plan, 1);
    let mut stream = output_of(&plan, 0, &context);
    let execution = execution(&plan);
    wait_until("the parked peer is reading its child", || {
        child.polled[1].load(Ordering::SeqCst) > 0
    })
    .await;

    let (chunks, errors) = tokio::time::timeout(Duration::from_secs(10), until_end(&mut stream))
        .await
        .unwrap_or_else(|_| {
            panic!(
                "the output never ended although one of its two inputs had died: the death recorded \
                 nothing, so the parked peer kept its sender alive"
            )
        });
    assert!(
        chunks.is_empty(),
        "no chunk was routed before the read stopped, so the output owes none"
    );
    assert_eq!(
        1,
        errors.len(),
        "the output reports the death of partition 0 once: {errors:?}"
    );
    assert!(
        errors[0]
            .contains("series repartition input partition 0 stopped without reporting an outcome"),
        "the output ended with {} rather than the death of its producer",
        errors[0]
    );
    // The peer did not go on waiting for a child that never answers: its task was aborted, which is
    // what the drop of the stream it was reading says.
    wait_until("the parked peer's child is dropped", || {
        child.dropped[1].load(Ordering::SeqCst) == 1
    })
    .await;
    wait_until("the read's tasks are gone", || producer_stopped(&execution)).await;
    wait_until("what the read reserved is released", || {
        pool.reserved() == 0
    })
    .await;
}

/// a partition whose task is aborted on its own ends every output, and stops its peers.
///
/// The same shape as [`a_panicking_input_ends_every_output_and_stops_its_peers`] with the other way a
/// task disappears: nothing panics, one task is aborted where it stands - as a cancelled query's task
/// is - and the future it was running is dropped with it. Both partitions stay parked on children that
/// never answer, so the only thing that can end this read is one of them dying and the read noticing:
/// the output would otherwise wait for two sources that never speak.
///
/// The abort reaches the handle of one partition only. [`Inputs::stop`] cannot state this: it stops
/// every task and records the read as stopped, so what its outputs report is the stop rather than one
/// producer that died while its peers were still running.
#[tokio::test]
async fn an_aborted_input_task_ends_every_output_and_stops_its_peers() {
    let child = Fragile::plan(
        vec![Behaviour::Parked, Behaviour::Parked],
        Arc::new(Switch::default()),
    );
    let plan = node(child.plan, 1);
    let context = SessionContext::default().task_ctx();
    let mut stream = output_of(&plan, 0, &context);
    let execution = execution(&plan);
    wait_until("both partitions are reading their children", || {
        child.polled[0].load(Ordering::SeqCst) > 0 && child.polled[1].load(Ordering::SeqCst) > 0
    })
    .await;

    let inputs = Arc::clone(
        &execution
            .running
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .inputs,
    );
    inputs.abort(0);

    let (chunks, errors) = tokio::time::timeout(Duration::from_secs(10), until_end(&mut stream))
        .await
        .unwrap_or_else(|_| {
            panic!(
                "the output never ended although one of its two inputs had been aborted: the abort \
                 recorded nothing, so the parked peer kept the queue open"
            )
        });
    assert!(
        chunks.is_empty(),
        "both partitions are parked, so nothing was routed before the read stopped"
    );
    assert_eq!(
        1,
        errors.len(),
        "the output reports the death of partition 0 once: {errors:?}"
    );
    assert!(
        errors[0]
            .contains("series repartition input partition 0 stopped without reporting an outcome"),
        "the output ended with {} rather than the death of its producer",
        errors[0]
    );
    wait_until("the other parked partition's child is dropped", || {
        child.dropped[1].load(Ordering::SeqCst) == 1
    })
    .await;
    wait_until("the read's tasks are gone", || producer_stopped(&execution)).await;
}

/// A dropped destination's oversized chain is discarded while a live destination continues.
#[tokio::test]
async fn dropped_output_discards_oversized_chain_and_keeps_live_output() {
    let doomed = labels_routed_to(0, 1).remove(0);
    let survivor = labels_routed_to(1, 1).remove(0);
    let many_chunks = (0..GRID_LEN as usize)
        .map(|position| {
            Step::chunk(chunk(
                doomed.clone(),
                position,
                1,
                position + 1 == GRID_LEN as usize,
            ))
        })
        .collect::<Vec<_>>();
    let many_chunk_count = many_chunks.len();
    let live_chunk = chunk(survivor.clone(), 0, 1, true);
    // Independent workspace allowance for the two live base chunks and queued-chain metadata.
    // Keep this finite and far below retaining every old whole-chain slice.
    let live_chunk_budget = operand_chunk_bytes(&live_chunk.0) * 2;
    let byte_budget = live_chunk_budget + 16 * 1024;
    assert!(
        operand_chunk_bytes(&chunk(doomed.clone(), 0, 1, false).0) * many_chunk_count > byte_budget,
        "discarding the oversized chain must be necessary under this finite budget"
    );
    let pool = Arc::new(GreedyMemoryPool::new(byte_budget));
    let context = task_context(&pool);
    let switch = Arc::new(Switch::default());
    let child = Fragile::plan(
        vec![Behaviour::Held({
            let mut steps = many_chunks;
            steps.push(Step::chunk(live_chunk));
            steps
        })],
        Arc::clone(&switch),
    );
    let plan = node(child.plan, 2);
    let dropped_output = output_of(&plan, 0, &context);
    let mut kept_output = output_of(&plan, 1, &context);
    let first_chunk_bytes = operand_chunk_bytes(&chunk(doomed.clone(), 0, 1, false).0);
    assert_eq!(0, pool.reserved(), "held child has not produced a chunk");
    drop(dropped_output);
    assert_eq!(
        0,
        pool.reserved(),
        "closed destination was dropped before first chunk"
    );
    switch.open();
    assert!(first_chunk_bytes > 0);
    wait_until(
        "held child processes discarded chain and live chain",
        || child.polled[0].load(Ordering::SeqCst) >= many_chunk_count + 2,
    )
    .await;

    let (chunks, errors) =
        tokio::time::timeout(Duration::from_secs(10), until_end(&mut kept_output))
            .await
            .expect("discarded oversized chain does not block the live destination");
    assert!(errors.is_empty(), "live output reports {errors:?}");
    assert_eq!(
        vec![named(&survivor)],
        chunks
            .iter()
            .map(|chunk| named(&chunk.0.metric))
            .collect::<Vec<_>>()
    );
    assert_eq!(1, chunks.len());
    wait_until("discarded and delivered chain charges are released", || {
        pool.reserved() == 0
    })
    .await;
}

#[tokio::test]
async fn discarded_open_chain_is_validated_before_live_chain_routes() {
    const INVALID: &str = "evaluated series positions are not increasing across chunks";
    let doomed = labels_routed_to(0, 1).remove(0);
    let first_chunk_bytes = operand_chunk_bytes(&chunk(doomed.clone(), 0, 1, false).0);
    let invalid = chunk(doomed.clone(), 0, 1, true);
    let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
    let context = task_context(&pool);
    let switch = Arc::new(Switch::default());
    let child = Fragile::plan(
        vec![Behaviour::Gated(vec![
            Step::chunk(chunk(doomed, 0, 1, false)),
            Step::chunk(invalid),
        ])],
        Arc::clone(&switch),
    );
    let plan = node(child.plan, 2);
    let dropped_output = output_of(&plan, 0, &context);
    let mut kept_output = output_of(&plan, 1, &context);
    wait_until("valid prefix is flushed and child parks at gate", || {
        pool.reserved() >= first_chunk_bytes && child.polled[0].load(Ordering::SeqCst) >= 2
    })
    .await;
    drop(dropped_output);
    wait_until(
        "discarding destination releases prefix while gate stays closed",
        || pool.reserved() == 0,
    )
    .await;
    switch.open();
    let (chunks, errors) =
        tokio::time::timeout(Duration::from_secs(10), until_end(&mut kept_output))
            .await
            .expect("malformed continuation fails the execution");
    assert!(
        chunks.is_empty(),
        "no incomplete chain was routed: {chunks:?}"
    );
    assert_eq!(
        1,
        errors.len(),
        "terminal error is reported exactly once: {errors:?}"
    );
    assert!(
        errors[0].contains(INVALID),
        "expected position validation error: {errors:?}"
    );
    assert_eq!(
        0,
        pool.reserved(),
        "failed discarded chain retains no memory"
    );
}

#[tokio::test]
async fn discarded_open_chain_eof_is_still_an_error() {
    let (sender, receiver) = mpsc::unbounded_channel();
    drop(receiver);
    let series = chunk(labels("unclosed"), 0, 1, false).0;
    let mut input: SendableEvaluatedSeriesStream =
        Box::pin(futures::stream::iter(vec![Ok((series, false))]));
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
    let reservation = MemoryConsumer::new("discarded-open-eof-test").register(&pool);
    let result =
        read_evaluated_chains(&mut input, 0, &Routing::new(1), &[sender], &reservation).await;
    assert!(
        matches!(result, Stop::Failed(_)),
        "open discarded chain EOF is not clean"
    );
    assert_eq!(0, pool.reserved());
}

/// A closed output is skipped while the sibling output continues to receive its chains.
#[tokio::test]
async fn closing_destination_releases_an_open_chain_before_child_resumes() {
    let doomed = labels_routed_to(0, 1).remove(0);
    let survivor = labels_routed_to(1, 1).remove(0);
    let switch = Arc::new(Switch::default());
    let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
    let context = task_context(&pool);
    let held_bytes = operand_chunk_bytes(&chunk(doomed.clone(), 0, 1, false).0);
    let child = Fragile::plan(
        vec![Behaviour::Gated(vec![
            Step::chunk(chunk(doomed.clone(), 0, 1, false)),
            Step::chunk(chunk(doomed, 1, 1, true)),
            Step::chunk(chunk(survivor.clone(), 4, 1, true)),
        ])],
        Arc::clone(&switch),
    );
    let plan = node(child.plan, 2);
    let dropped_output = output_of(&plan, 0, &context);
    let mut kept_output = output_of(&plan, 1, &context);
    wait_until("open chain is retained and child is gated", || {
        pool.reserved() >= held_bytes
    })
    .await;
    drop(dropped_output);
    wait_until(
        "open chain reservation is released while child remains gated",
        || pool.reserved() < held_bytes,
    )
    .await;
    switch.open();
    let (chunks, errors) =
        tokio::time::timeout(Duration::from_secs(10), until_end(&mut kept_output))
            .await
            .expect("the live output completes after gate opens");
    assert!(errors.is_empty());
    assert_eq!(
        vec![named(&survivor)],
        chunks
            .iter()
            .map(|chunk| named(&chunk.0.metric))
            .collect::<Vec<_>>()
    );
    assert_eq!(1, chunks.len());
    assert_eq!(0, pool.reserved());
}

#[tokio::test]
async fn dropping_an_output_leaves_surviving_outputs_running() {
    let switch = Arc::new(Switch::default());
    let doomed = labels_routed_to(0, 1).remove(0);
    let survivor = labels_routed_to(1, 1).remove(0);
    let child = Fragile::plan(
        vec![
            Behaviour::OneAtATime(vec![
                Step::chunk(chunk(doomed.clone(), 0, 1, true)),
                Step::chunk(chunk(survivor.clone(), 1, 1, true)),
            ]),
            Behaviour::Held(vec![]),
        ],
        Arc::clone(&switch),
    );
    let plan = node(child.plan, 2);
    let context = SessionContext::default().task_ctx();
    // Both outputs are claimed, so the one that goes away releases a claim on a read that still has
    // another output to serve instead of stopping it.
    let dropped_output = output_of(&plan, 0, &context);
    let mut kept_output = output_of(&plan, 1, &context);
    let execution = execution(&plan);

    // Dropped while its queue is still empty and its producer parked on the switch, so the chain held
    // for it is what finds it gone.
    drop(dropped_output);
    switch.open();

    let (chunks, errors) = tokio::time::timeout(
        Duration::from_secs(10),
        until_end(&mut kept_output),
    )
    .await
    .unwrap_or_else(|_| {
        panic!(
            "the output that remains claimed did not finish after its sibling output was dropped"
        )
    });
    assert_eq!(
        vec![named(&survivor)],
        chunks
            .iter()
            .map(|chunk| named(&chunk.0.metric))
            .collect::<Vec<_>>(),
        "the surviving output keeps its own chains after the sibling is dropped"
    );
    assert!(
        errors.is_empty(),
        "a dropped sibling is not a read failure: {errors:?}"
    );
    wait_until("the parked partition's child is dropped", || {
        child.dropped[1].load(Ordering::SeqCst) == 1
    })
    .await;
    wait_until("the read's tasks are gone", || producer_stopped(&execution)).await;
    assert!(matches!(execution.ending(), Ending::Finished));
}

#[tokio::test]
async fn sliced_chunk_memory_is_charged_by_rows_and_released_after_success_drop_and_failure() {
    let slices = sliced_chunks(labels("sliced"));
    let probe_context = task_context(&Arc::new(GreedyMemoryPool::new(usize::MAX)));
    let probe_child = Scripted::plan(vec![
        slices
            .iter()
            .cloned()
            .map(|(series, is_last)| Step::Chunk(series, is_last))
            .collect(),
    ]);
    let probe_stream = probe_child.execute(0, Arc::clone(&probe_context)).unwrap();
    let mut probe = unpack_evaluated_series(probe_stream, None).unwrap();
    let (decoded_slices, errors) = until_end(&mut probe).await;
    assert!(
        errors.is_empty(),
        "decoded slice probe succeeds: {errors:?}"
    );
    assert_eq!(2, decoded_slices.len());
    let slice_costs = decoded_slices
        .iter()
        .map(|(series, _)| operand_chunk_bytes(series))
        .collect::<Vec<_>>();
    let expected_bytes = slice_costs[0];
    assert_eq!(
        expected_bytes, slice_costs[1],
        "same-sized slices cost equally"
    );
    let first = &slices[0].0;
    assert!(
        series_bytes(first) > expected_bytes,
        "a sliced chunk charges its row payload rather than its backing Arrow allocation"
    );
    let success_budget = expected_bytes * 2 + 8 * 1024;
    let pool = Arc::new(GreedyMemoryPool::new(success_budget));
    let context = task_context(&pool);
    let plan = node(
        Scripted::plan(vec![
            slices
                .iter()
                .cloned()
                .map(|(s, l)| Step::Chunk(s, l))
                .collect(),
        ]),
        1,
    );
    let mut output = output_of(&plan, 0, &context);
    let (chunks, errors) = tokio::time::timeout(Duration::from_secs(10), until_end(&mut output))
        .await
        .unwrap();
    assert!(
        errors.is_empty(),
        "expected {expected_bytes} bytes per slice; got {errors:?}"
    );
    assert_eq!(2, chunks.len());
    assert_eq!(
        0,
        pool.reserved(),
        "successful delivery releases reservations"
    );

    let slices = sliced_chunks(labels("sliced-drop"));
    let pool = Arc::new(GreedyMemoryPool::new(expected_bytes * 3 + 8 * 1024));
    let context = task_context(&pool);
    let plan = node(
        Scripted::plan(vec![
            slices
                .iter()
                .cloned()
                .map(|(s, l)| Step::Chunk(s, l))
                .collect(),
        ]),
        1,
    );
    let mut output = output_of(&plan, 0, &context);
    let first = futures::future::poll_fn(|cx| output.as_mut().poll_next(cx))
        .await
        .expect("first slice arrives")
        .expect("first slice succeeds");
    drop(first);
    drop(output);
    drop(plan);
    wait_until("dropping the output releases slice memory", || {
        pool.reserved() == 0
    })
    .await;
    assert_eq!(
        0,
        pool.reserved(),
        "dropping public output releases all slice charges"
    );

    let slices = sliced_chunks(labels("sliced"));
    // This deliberately undersized pool refuses the first slice once its independently-accounted
    // routing workspace is included; the test covers terminal error and reservation release.
    let budget = expected_bytes * 2 - 1;
    assert!(expected_bytes <= budget);
    assert!(2 * expected_bytes > budget);
    assert!(
        series_bytes(&slices[0].0) + series_bytes(&slices[1].0) > 2 * expected_bytes,
        "full backing arrays cost more than the row-based charges"
    );
    let pool = Arc::new(GreedyMemoryPool::new(budget));
    let context = task_context(&pool);
    let plan = node(
        Scripted::plan(vec![
            slices
                .iter()
                .cloned()
                .map(|(s, l)| Step::Chunk(s, l))
                .collect(),
        ]),
        1,
    );
    let mut output = output_of(&plan, 0, &context);
    let (_, errors) = tokio::time::timeout(Duration::from_secs(10), until_end(&mut output))
        .await
        .unwrap();
    assert_eq!(
        1,
        errors.len(),
        "the second same-sized slice is refused while the first charge is retained"
    );
    assert_eq!(
        0,
        pool.reserved(),
        "the failed chain releases its slice reservation"
    );
}

#[tokio::test]
async fn internal_stream_releases_each_chunk_charge_only_when_polled() {
    let slices = sliced_chunks(labels("internal-lifetime"));
    let costs = slices
        .iter()
        .map(|(series, _)| operand_chunk_bytes(series))
        .collect::<Vec<_>>();
    assert_eq!(costs[0], costs[1]);
    let expected = costs.iter().sum::<usize>();
    let pool = Arc::new(GreedyMemoryPool::new(usize::MAX));
    let context = task_context(&pool);
    let child = Scripted::plan(vec![
        slices
            .into_iter()
            .map(|(series, is_last)| Step::Chunk(series, is_last))
            .collect(),
    ]);
    let shared =
        SharedExecution::start(&child, Routing::new(1), Arc::clone(&schema()), &context).unwrap();
    let (receiver, lease) = SharedExecution::claim(&shared, 0).expect("claim output");
    let mut stream = RepartitionStream::new(
        receiver,
        lease,
        Arc::clone(&schema()),
        &(Arc::clone(&pool) as Arc<dyn MemoryPool>),
        1,
        SeriesOrdering::Contiguous,
    );
    wait_until("internal producer finishes", || producer_stopped(&shared)).await;
    // Queue, EOF, and identity-group metadata are charged alongside the retained chunks; the
    // fixture payload is more than four times this allowance, so the bound still proves payload
    // retention rather than control-only accounting.
    const CONTROL_ALLOWANCE: usize = 256;
    assert!(
        expected > CONTROL_ALLOWANCE * 4,
        "the payload bound is materially larger than the control allowance"
    );
    assert!(
        (expected..=expected + CONTROL_ALLOWANCE).contains(&pool.reserved()),
        "retained bytes {} cover the payload bound {expected} and stay within the {CONTROL_ALLOWANCE}-byte control allowance",
        pool.reserved()
    );
    let merged = futures::future::poll_fn(|cx| Pin::new(&mut stream).poll_next(cx))
        .await
        .unwrap()
        .unwrap();
    let (merged_series, closed) = evaluated(merged);
    assert!(closed, "the logical owner closes once after global EOF");
    assert_eq!(
        4,
        merged_series.time.len(),
        "both slices retain their two positions in the merged logical owner"
    );
    assert!(
        pool.reserved() >= expected,
        "the internal handoff retains payload charges for the returned owner"
    );
    assert!(
        futures::future::poll_fn(|cx| Pin::new(&mut stream).poll_next(cx))
            .await
            .is_none(),
        "the logical owner has one stable end"
    );
    drop(merged_series);
    drop(stream);
    assert_eq!(
        0,
        pool.reserved(),
        "dropping handoff releases retained charges"
    );
}

#[tokio::test]
async fn public_repartition_interleaved_histogram_tables_stay_linear() {
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

    for table_count in [1usize, 2] {
        let metric = labels("interleaved-histograms");
        let mut inputs = Vec::new();
        let mut hist_positions = Vec::new();
        for table in 0..table_count {
            let positions = (0..128 / table_count)
                .map(|index| index * 2 * table_count + table * 2)
                .collect::<Vec<_>>();
            hist_positions.extend(positions.iter().copied());
            inputs.push(vec![Step::chunk(sparse_histogram_rows_chunk(
                metric.clone(),
                positions,
                128 / table_count,
                10.0 + table as f64,
            ))]);
        }
        let float_positions = (0..128).map(|index| index * 2 + 1).collect::<Vec<_>>();
        let float_values = float_positions
            .iter()
            .map(|position| Some(*position as f64))
            .collect::<Vec<_>>();
        let float_time = TimeAxis::from_grid_spans(
            schema().eval_grid.clone().unwrap(),
            float_positions
                .iter()
                .map(|position| GridSpan::new(*position, 1))
                .collect(),
        )
        .unwrap();
        let float_series = Series::try_new(
            &schema(),
            metric.clone(),
            float_time,
            SeriesFields::one(Samples::from_float(Float64Array::from(float_values))),
        )
        .unwrap();
        inputs.push(vec![Step::chunk((float_series, true))]);
        let child = Scripted::plan(inputs);
        let context = unbounded_context();
        let plan = node(child, 1);
        let mut frames = plan.execute(0, Arc::clone(&context)).unwrap();
        let mut batches = Vec::new();
        let mut referenced_histogram_rows = 0usize;
        while let Some(batch) = frames.try_next().await.unwrap() {
            let fields = batch
                .column(3)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            let payload = fields
                .column(0)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            let hist = payload
                .column(1)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            let tables = hist.column(1).as_any().downcast_ref::<ListArray>().unwrap();
            for row in 0..tables.len() {
                referenced_histogram_rows += tables.value_length(row) as usize;
            }
            batches.push(batch);
        }
        assert_eq!(128, referenced_histogram_rows, "table_count={table_count}");

        let frame_schema = batches[0].schema();
        let frames = Box::pin(RecordBatchStreamAdapter::new(
            frame_schema,
            futures::stream::iter(batches.into_iter().map(Ok)),
        ));
        let mut decoded = unpack_evaluated_series(frames, Some(schema())).unwrap();
        let mut chunks = Vec::new();
        while let Some(chunk) = decoded.try_next().await.unwrap() {
            chunks.push(chunk);
        }
        let mut next_position = 0;
        let mut final_count = 0;
        for (series, is_last) in chunks {
            assert_eq!(metric, series.metric, "full input identity survives");
            final_count += usize::from(is_last);
            assert_eq!(series.time.len(), series.fields.get(0).unwrap().len());
            for (position, row) in series.time.positions() {
                assert_eq!(
                    next_position, position,
                    "positions stay strictly increasing and complete"
                );
                if position % 2 == 0 {
                    let table = (position / 2) % table_count;
                    match series.fields.get(0).unwrap().sample(row) {
                        datatypes::series::samples::SampleRef::Histogram(histogram) => {
                            assert_eq!(10.0 + table as f64, histogram.sum);
                            assert_eq!(4.0, histogram.count);
                            assert_eq!(0.0, histogram.zero_threshold);
                            assert_eq!(0.0, histogram.zero_count);
                            assert_eq!(0, histogram.schema);
                            assert_eq!(CounterResetHint::Unknown, histogram.reset_hint);
                            assert_eq!(&[0.5, 1.5, 2.5, 3.5], histogram.custom_values);
                            assert_eq!(&[1.0, 2.0, 3.0, 4.0], histogram.positive.buckets);
                            assert_eq!(&[1.0, 2.0, 3.0, 4.0], histogram.negative.buckets);
                        }
                        _ => panic!("histogram at {position}"),
                    }
                } else {
                    match series.fields.get(0).unwrap().sample(row) {
                        datatypes::series::samples::SampleRef::Float(value) => {
                            assert_eq!(position as f64, value);
                        }
                        _ => panic!("float at {position}"),
                    }
                }
                next_position += 1;
            }
        }
        assert_eq!(256, next_position, "the full grid survives decoding");
        assert_eq!(1, final_count, "the merged identity closes exactly once");
        assert_eq!(128, hist_positions.len());
    }
}

#[tokio::test]
async fn packed_child_frame_decodes_all_256_closing_chunks() {
    let metric = labels("packed-child");
    let child = Scripted::plan(vec![
        (0..256)
            .map(|position| Step::chunk(chunk(metric.clone(), position, 1, position == 255)))
            .collect(),
    ]);
    let mut frames = child
        .execute(
            0,
            task_context(&Arc::new(GreedyMemoryPool::new(usize::MAX))),
        )
        .unwrap();
    let frame = frames
        .try_next()
        .await
        .unwrap()
        .expect("child packs all scripted chunks into one frame");
    assert_eq!(256, frame.num_rows(), "one child frame contains 256 chunks");
    assert!(frames.try_next().await.unwrap().is_none());

    let plan = node(child, 1);
    let pool = Arc::new(GreedyMemoryPool::new(usize::MAX));
    let context = task_context(&pool);
    let mut output = output_of(&plan, 0, &context);
    let (chunks, errors) = until_end(&mut output).await;
    assert!(
        errors.is_empty(),
        "decoded packed child succeeds: {errors:?}"
    );
    assert_eq!(256, chunks.len());
    for (position, (series, is_last)) in chunks.iter().enumerate() {
        assert_eq!(named(&metric), named(&series.metric));
        assert_eq!(Some(position), series.time.first_position());
        assert_eq!(1, series.time.len());
        assert_eq!(position == 255, *is_last);
        let Some(datatypes::series::samples::DenseSamplesRef::Float(values)) =
            series.fields.get(0).unwrap().segments()[0].dense()
        else {
            panic!("decoded packed chunk has float samples");
        };
        assert_eq!(position as f64, values.values()[0]);
    }
}

#[tokio::test]
async fn histogram_chain_slices_fit_a_budget_below_the_full_table_charge() {
    let metric = labels("histogram-sliced-chain");
    let steps = (0..256)
        .map(|position| {
            let (series, _) = histogram_chunk(metric.clone(), position);
            Step::Chunk(series, position == 255)
        })
        .collect::<Vec<_>>();
    let child = Scripted::plan(vec![steps]);
    let probe_context = task_context(&Arc::new(GreedyMemoryPool::new(usize::MAX)));
    let mut frames = child.execute(0, Arc::clone(&probe_context)).unwrap();
    let batch = frames
        .try_next()
        .await
        .unwrap()
        .expect("one histogram frame");
    assert_eq!(
        256,
        batch.num_rows(),
        "all histogram chunks share one frame"
    );
    assert!(frames.try_next().await.unwrap().is_none());
    drop(batch);
    let mut probe =
        unpack_evaluated_series(child.execute(0, Arc::clone(&probe_context)).unwrap(), None)
            .unwrap();
    let mut full_table_bytes = 0usize;
    let mut slice_bytes = 0usize;
    let mut base_bytes = 0usize;
    for _ in 0..256 {
        let (series, _) = futures::future::poll_fn(|cx| probe.as_mut().poll_next(cx))
            .await
            .unwrap()
            .unwrap();
        for segment in series.fields.iter().flat_map(|field| field.segments()) {
            if let Some(datatypes::series::samples::DenseSamplesRef::Histogram(histograms)) =
                segment.dense()
            {
                full_table_bytes =
                    full_table_bytes.max(histograms.batch().as_arrow().get_array_memory_size());
                slice_bytes += histogram_table_slice_bytes(histograms.batch()).unwrap();
            }
        }
        base_bytes += operand_chunk_bytes(&series);
    }
    // Each histogram row has 48 scalar bytes and 112 list-value bytes (4 custom f64 values,
    // 2 i32 offsets, 2 i32 lengths and 8 f64 buckets), independently of this accounting helper.
    const HISTOGRAM_ROW_PAYLOAD_BYTES: usize = 160;
    let independent_slice_lower_bound = HISTOGRAM_ROW_PAYLOAD_BYTES * 256;
    let independent_slice_upper_bound =
        256 * (std::mem::size_of::<datatypes::series::histogram::HistogramBatch>() + 168 + 7 * 8);
    assert!(
        (independent_slice_lower_bound..=independent_slice_upper_bound).contains(&slice_bytes),
        "measured slice charge {slice_bytes} is outside the independent bounds {independent_slice_lower_bound}..={independent_slice_upper_bound}"
    );
    let old_charge = full_table_bytes * 256;
    // Include independent bounded per-row/per-fragment bookkeeping workspace; the budget remains
    // below charging the full shared backing table once for every physical slice.
    let budget = base_bytes + independent_slice_upper_bound + 512 * 1024;
    assert!(
        budget < base_bytes + old_charge,
        "slice budget {budget} is below duplicated full-table charge {}",
        base_bytes + old_charge
    );
    assert!(
        budget > base_bytes + independent_slice_lower_bound,
        "finite budget must allow the base chunks and independently bounded histogram payload"
    );
    let pool = Arc::new(GreedyMemoryPool::new(budget));
    let context = task_context(&pool);
    let shared =
        SharedExecution::start(&child, Routing::new(1), Arc::clone(&schema()), &context).unwrap();
    let (receiver, lease) = SharedExecution::claim(&shared, 0).expect("claim output");
    let mut output = RepartitionStream::new(
        receiver,
        lease,
        Arc::clone(&schema()),
        &(Arc::clone(&pool) as Arc<dyn MemoryPool>),
        1,
        SeriesOrdering::Contiguous,
    );
    wait_until("histogram producer queues the whole chain", || {
        producer_stopped(&shared)
    })
    .await;
    assert!(
        pool.reserved() >= base_bytes + independent_slice_lower_bound,
        "queued charge {} must include chunk bases and the independently-derived histogram payload bound {}",
        pool.reserved(),
        base_bytes + independent_slice_lower_bound
    );
    let mut chunks = Vec::new();
    while let Some(item) = tokio::time::timeout(
        Duration::from_secs(20),
        futures::future::poll_fn(|cx| Pin::new(&mut output).poll_next(cx)),
    )
    .await
    .unwrap()
    {
        chunks.push(item.unwrap());
    }
    assert_eq!(
        1,
        chunks.len(),
        "one owner series spans all physical slices"
    );
    let (series, closed) = evaluated(chunks.remove(0));
    assert!(closed, "the complete owner closes once");
    assert_eq!(256, series.time.len(), "all histogram rows survive merge");
    assert!(matches!(shared.ending(), Ending::Finished));
    assert!(
        pool.reserved() >= base_bytes + independent_slice_lower_bound,
        "internal handoff retains payload and histogram slice charges"
    );
    drop(chunks);
    drop(output);
    assert_eq!(
        0,
        pool.reserved(),
        "dropping internal output releases every retained slice charge"
    );
}

#[tokio::test]
async fn histogram_table_memory_is_charged_and_released_with_retained_chain() {
    let metric = labels("histogram");
    let (series, is_last) = histogram_chunk(metric.clone(), 0);
    let measured_table = match series.fields.get(0).unwrap().segments()[0].dense() {
        Some(datatypes::series::samples::DenseSamplesRef::Histogram(histograms)) => {
            histograms.batch().as_arrow().get_array_memory_size()
        }
        _ => panic!("histogram sample expected"),
    };
    let child = Scripted::plan(vec![vec![Step::Chunk(series, is_last)]]);
    let pool = Arc::new(GreedyMemoryPool::new(usize::MAX));
    let context = task_context(&pool);
    let plan = node(child, 1);
    let mut output = output_of(&plan, 0, &context);
    let result = tokio::time::timeout(Duration::from_secs(10), until_end(&mut output))
        .await
        .expect("histogram output completes");
    assert!(result.1.is_empty());
    assert_eq!(0, pool.reserved(), "success releases queued reservation");
    assert!(measured_table > 0, "test table payload is non-empty");

    let (first, _) = histogram_chunk(metric.clone(), 0);
    let mut second = first.clone();
    second.time = TimeAxis::from_grid_spans(
        schema().eval_grid.clone().unwrap(),
        vec![GridSpan::new(1, 1)],
    )
    .expect("second histogram chunk advances on the grid");
    let pool = Arc::new(GreedyMemoryPool::new(usize::MAX));
    let context = task_context(&pool);
    let plan = node(
        Scripted::plan(vec![vec![
            Step::Chunk(first.clone(), false),
            Step::Chunk(second.clone(), true),
        ]]),
        1,
    );
    let mut output = output_of(&plan, 0, &context);
    let current_execution = execution(&plan);
    wait_until("producer queued both shared-table chunks", || {
        producer_stopped(&current_execution)
    })
    .await;
    let probe_child = Scripted::plan(vec![vec![
        Step::Chunk(first.clone(), false),
        Step::Chunk(second.clone(), true),
    ]]);
    let probe_stream = probe_child
        .execute(
            0,
            task_context(&Arc::new(GreedyMemoryPool::new(usize::MAX))),
        )
        .unwrap();
    let mut probe_output = unpack_evaluated_series(probe_stream, None).unwrap();
    let mut decoded_costs = Vec::new();
    for _ in 0..2 {
        let (series, _) = futures::future::poll_fn(|cx| probe_output.as_mut().poll_next(cx))
            .await
            .unwrap()
            .unwrap();
        let histogram_segments = series
            .fields
            .iter()
            .flat_map(|field| field.segments())
            .filter_map(|segment| match segment.dense() {
                Some(datatypes::series::samples::DenseSamplesRef::Histogram(histograms)) => {
                    Some(histograms)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(
            !histogram_segments.is_empty(),
            "decoded chunk contains histogram segments"
        );
        decoded_costs.push(
            operand_chunk_bytes(&series)
                + histogram_segments
                    .iter()
                    .map(|histograms| histogram_table_slice_bytes(histograms.batch()).unwrap())
                    .sum::<usize>(),
        );
    }
    let decoded_chunk_cost = decoded_costs[0];
    assert_eq!(decoded_chunk_cost, decoded_costs[1]);
    let expected = 2 * decoded_chunk_cost;
    drop(probe_output);
    assert!(
        pool.reserved() >= expected,
        "reservation includes both chunk payloads and independent shared-table slice charges"
    );
    let first = futures::future::poll_fn(|cx| output.as_mut().poll_next(cx))
        .await
        .unwrap()
        .unwrap();
    drop(first);
    drop(output);
    drop(plan);
    assert_eq!(
        0,
        pool.reserved(),
        "dropping internal output releases all shared-table reservations"
    );

    let (many, _) = histogram_rows_chunk(labels("histogram-many"), 0, 256);
    let probe_child = Scripted::plan(vec![vec![Step::Chunk(many.clone(), true)]]);
    let probe_context = task_context(&Arc::new(GreedyMemoryPool::new(usize::MAX)));
    let decoded = unpack_evaluated_series(
        probe_child.execute(0, Arc::clone(&probe_context)).unwrap(),
        None,
    );
    let mut probe = decoded.unwrap();
    let (decoded, _) = futures::future::poll_fn(|cx| probe.as_mut().poll_next(cx))
        .await
        .unwrap()
        .unwrap();
    let base = operand_chunk_bytes(&decoded);
    let table_cost = decoded
        .fields
        .iter()
        .flat_map(|field| field.segments())
        .map(|segment| match segment.dense() {
            Some(datatypes::series::samples::DenseSamplesRef::Histogram(histograms)) => {
                histogram_table_slice_bytes(histograms.batch()).unwrap()
            }
            _ => 0,
        })
        .sum::<usize>();
    let budget = base + table_cost - 1;
    let pool = Arc::new(GreedyMemoryPool::new(budget));
    let context = task_context(&pool);
    let plan = node(Scripted::plan(vec![vec![Step::Chunk(many, true)]]), 1);
    let mut output = output_of(&plan, 0, &context);
    let (_, errors) = tokio::time::timeout(Duration::from_secs(10), until_end(&mut output))
        .await
        .unwrap();
    assert_eq!(1, errors.len(), "underbudget histogram slice is refused");
    assert_eq!(0, pool.reserved(), "refusal releases slice charge");
}

/// a chain that outgrows the pool ends every output, cancels its peers and releases the memory.
///
/// This is what the whole-chain handover costs unless it is charged: a chain is buffered whole before
/// it is routed, so a child that keeps one series open and never closes it would grow the buffer
/// without bound. Here the chain is charged to the pool as it grows, and the pool is sized from the
/// chunks rather than from a number picked by hand.
///
/// The arithmetic is measured rather than assumed, and both sides of it matter. `per_chunk` is what
/// one chunk costs the read, measured through the same pack/unpack path.
/// The chain is `CHUNKS` of those, the pool holds `CHUNKS / 2` of them, so one chunk fits with room
/// to spare and the chain as a whole does not: the growth that is refused is the one that would have
/// made the buffer exceed the pool, at the `CHUNKS / 2 + 1`-th chunk. A change to what a chunk costs
/// moves the pool with it instead of silently making the refusal happen at the first chunk or not at
/// all - which is what a hard-coded byte count did here.
///
/// The child hands one chunk over per poll ([`Behaviour::OneAtATime`]) for a reason that is not
/// cosmetic: what a producer hands over in one poll reaches the read as one chunk, so back-to-back
/// chunks would coalesce into a single one and there would be no growing chain to refuse.
///
/// What is asserted is the whole of it: every output ends with the refusal rather than a clean end,
/// the partition parked on a child that never answers is cancelled, and the pool is back to nothing
/// once the read is over - the chain it refused to grow is not left charged for.
#[tokio::test]
async fn a_chain_that_outgrows_the_pool_ends_every_output_and_releases_it() {
    const CHUNKS: usize = 8;
    let shared = labels("web-1");
    // One open chain: every chunk continues the series and none of them closes it, so the read goes on
    // reading until the pool refuses to hold more of it.
    let steps = (0..CHUNKS)
        .map(|index| Step::chunk(chunk(shared.clone(), index, 1, false)))
        .collect::<Vec<_>>();
    // What one chunk of this chain costs, measured through the read's own path on an unbounded pool.
    // The probe closes its chain, so what it measures is a whole stream's worth of chunks rather than
    // the error a producer that never closes one leaves behind.
    let per_chunk = {
        let probe = Fragile::plan(
            vec![Behaviour::OneAtATime(vec![
                Step::chunk(chunk(shared.clone(), 0, 1, false)),
                Step::chunk(chunk(shared.clone(), 1, 1, true)),
            ])],
            Arc::new(Switch::default()),
        );
        let probe_context = unbounded_context();
        let mut probe_chunks = unpack_evaluated_series(
            probe
                .plan
                .execute(0, Arc::clone(&probe_context))
                .expect("the probe partition executes"),
            None,
        )
        .expect("the probe declares an evaluated frame");
        let (chunks, errors) =
            tokio::time::timeout(Duration::from_secs(10), until_end(&mut probe_chunks))
                .await
                .expect("the probe chain ends, so its stream ends");
        assert!(
            errors.is_empty(),
            "the probe chain is a whole one, so nothing about it fails: {errors:?}"
        );
        assert_eq!(
            2,
            chunks.len(),
            "one chunk per poll is what keeps the frame boundary from coalescing two chunks into one"
        );
        chunks
            .iter()
            .map(|(series, _)| operand_chunk_bytes(series))
            .max()
            .expect("the probe chain has chunks of its own")
    };
    let pool_size = per_chunk * (CHUNKS / 2);
    assert!(
        per_chunk >= 1 && pool_size < per_chunk * CHUNKS,
        "the pool has to hold at least one chunk and fewer than all of them, or this test refuses \
         nothing: {pool_size} bytes against {per_chunk} per chunk"
    );
    let pool = Arc::new(GreedyMemoryPool::new(pool_size));
    let context = task_context(&pool);
    let child = Fragile::plan(
        vec![Behaviour::OneAtATime(steps), Behaviour::Parked],
        Arc::new(Switch::default()),
    );
    let plan = node(child.plan, 2);

    let outputs = tokio::time::timeout(
        Duration::from_secs(10),
        every_output_until_end(&plan, &context),
    )
    .await
    .expect("a reservation that is refused ends the read instead of growing past the pool");
    for (partition, (chunks, errors)) in outputs.iter().enumerate() {
        assert!(
            chunks.is_empty(),
            "output partition {partition} was given the first {n} chunks of a chain that was never \
             routed",
            n = chunks.len()
        );
        assert_eq!(
            1,
            errors.len(),
            "output partition {partition} reports the refusal once: {errors:?}"
        );
        assert!(
            errors[0].contains("Failed to allocate additional"),
            "output partition {partition} ended with {} rather than the refusal",
            errors[0]
        );
    }
    wait_until("the parked partition's child is dropped", || {
        child.dropped[1].load(Ordering::SeqCst) == 1
    })
    .await;
    let execution = execution(&plan);
    wait_until("the read's tasks are gone", || producer_stopped(&execution)).await;
    wait_until("what the refused chain reserved is released", || {
        pool.reserved() == 0
    })
    .await;
}

#[tokio::test]
async fn raw_identity_publishes_once_every_input_contributed() {
    let switch = Arc::new(Switch::default());
    let child = RawScripted::plan(vec![
        RawPartition {
            chunks: vec![raw_chunk(
                "k",
                Some(raw_metric("a")),
                &[1, 3],
                vec![Some(1.0), Some(3.0)],
                true,
            )],
            gate: None,
            gate_chunks: false,
        },
        RawPartition {
            chunks: vec![raw_chunk(
                "k",
                Some(raw_metric("a")),
                &[2, 4],
                vec![Some(2.0), Some(4.0)],
                true,
            )],
            gate: Some(Arc::clone(&switch)),
            gate_chunks: false,
        },
    ]);
    let plan = raw_node(child, 1);
    let context = task_context(&Arc::new(GreedyMemoryPool::new(8 << 20)));
    let frames = plan.execute(0, context).expect("partition 0 exists");
    let mut stream = unpack_raw_series(frames, Some(raw_schema())).expect("the frame decodes");
    let mut chunks = Vec::new();
    while let Some(item) = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("published while EOF gate remains closed")
    {
        chunks.push(item.expect("raw merge succeeds"));
        if chunks.last().is_some_and(|chunk| chunk.is_last) {
            break;
        }
    }
    let timestamps: Vec<i64> = chunks
        .iter()
        .flat_map(|chunk| chunk.time.as_buffer().iter().copied())
        .collect();
    assert_eq!(timestamps, [1, 2, 3, 4]);
    assert!(chunks.last().is_some_and(|chunk| chunk.is_last));
    switch.open();
}

#[tokio::test]
async fn raw_identity_waits_for_an_input_that_has_not_contributed() {
    let switch = Arc::new(Switch::default());
    let child = RawScripted::plan(vec![
        RawPartition {
            chunks: vec![raw_chunk(
                "k",
                Some(raw_metric("a")),
                &[1, 3],
                vec![Some(1.0), Some(3.0)],
                true,
            )],
            gate: None,
            gate_chunks: false,
        },
        RawPartition {
            chunks: vec![raw_chunk(
                "k",
                Some(raw_metric("a")),
                &[2, 4],
                vec![Some(2.0), Some(4.0)],
                true,
            )],
            gate: Some(Arc::clone(&switch)),
            gate_chunks: true,
        },
    ]);
    let plan = raw_node(child, 1);
    let context = task_context(&Arc::new(GreedyMemoryPool::new(8 << 20)));
    let frames = plan.execute(0, context).expect("partition 0 exists");
    let mut stream = unpack_raw_series(frames, Some(raw_schema())).expect("the frame decodes");
    assert!(
        tokio::time::timeout(Duration::from_millis(200), stream.next())
            .await
            .is_err()
    );
    switch.open();
    let chunks: Vec<_> = stream.try_collect().await.expect("raw merge succeeds");
    let timestamps: Vec<i64> = chunks
        .iter()
        .flat_map(|chunk| chunk.time.as_buffer().iter().copied())
        .collect();
    assert_eq!(timestamps, [1, 2, 3, 4]);
    assert!(chunks.last().is_some_and(|chunk| chunk.is_last));
}

/// Chunks decoded from one frame batch share its Arrow buffers, so charging each chunk the whole
/// allocation would charge the batch's timestamps once per chunk.
///
/// A second input that stays open and contributes nothing holds every group unpublished, which is
/// what keeps the charges of the first input's chunks alive to be measured.
#[tokio::test]
async fn raw_chunks_of_one_batch_are_charged_their_own_slice() {
    const SERIES: usize = 64;
    const ROWS: usize = 64;
    let switch = Arc::new(Switch::default());
    let child = RawScripted::plan(vec![
        RawPartition {
            chunks: (0..SERIES)
                .map(|series| {
                    let times: Vec<i64> = (0..ROWS as i64)
                        .map(|row| series as i64 * 1000 + row)
                        .collect();
                    let values: Vec<Option<f64>> = (0..ROWS).map(|row| Some(row as f64)).collect();
                    raw_chunk(
                        &format!("k{series}"),
                        Some(raw_metric("a")),
                        &times,
                        values,
                        true,
                    )
                })
                .collect(),
            gate: None,
            gate_chunks: false,
        },
        RawPartition {
            chunks: Vec::new(),
            gate: Some(Arc::clone(&switch)),
            gate_chunks: false,
        },
    ]);
    let plan = raw_node(child, 1);
    let pool = Arc::new(GreedyMemoryPool::new(64 << 20));
    let context = task_context(&pool);
    let mut frames = plan
        .execute(0, Arc::clone(&context))
        .expect("partition 0 exists");
    // Everything the first input has is delivered; the second input has contributed nothing, so no
    // group is publishable and every chunk charge of the first input is still held.
    //
    // Wait for the first charge before sampling: under load the input task may not have run yet,
    // and a fixed number of idle ticks would then measure nothing.
    wait_until("the first chunk is charged", || pool.reserved() > 0).await;
    // Keep the largest charge seen while the rest of the partition is routed, so a charge that
    // grows per chunk is measured rather than missed.
    let mut peak = 0usize;
    let mut idle = 0usize;
    while idle < 100 {
        let state = futures::future::poll_fn(|cx| Poll::Ready(frames.as_mut().poll_next(cx))).await;
        peak = peak.max(pool.reserved());
        match state {
            Poll::Ready(Some(Ok(_))) => idle = 0,
            Poll::Ready(Some(Err(error))) => panic!("the raw read failed: {error}"),
            Poll::Ready(None) => panic!("the read ended before the gated input was opened"),
            Poll::Pending => idle += 1,
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    switch.open();
    drop(frames);
    // The timestamps of every chunk in the batch come from one buffer of at most SERIES * ROWS
    // values. Charging that buffer per chunk would retain about SERIES times its size.
    let batch_timestamps = SERIES * ROWS * std::mem::size_of::<i64>();
    assert!(
        peak < SERIES * batch_timestamps / 2,
        "peak {peak} must stay well below the {SERIES}x over-charge of {batch_timestamps} bytes"
    );
}

/// A histogram field is an index array naming rows of a shared table. A query over few distinct
/// histograms holds mostly indices, and those are charged alongside the table rather than instead
/// of it.
#[tokio::test]
async fn raw_histogram_index_arrays_are_charged() {
    const ROWS: usize = 4096;
    let side = BucketSideView {
        span_offsets: &[],
        span_lengths: &[],
        buckets: &[],
    };
    let mut builder = HistogramBatchBuilder::new();
    builder
        .push(HistogramView {
            schema: 1,
            zero_threshold: 0.5,
            sum: 1.0,
            count: 1.0,
            zero_count: 0.0,
            reset_hint: CounterResetHint::Unknown,
            start_timestamp: None,
            custom_values: &[],
            positive: side,
            negative: side,
        })
        .expect("one histogram row");
    // Every position names the same row, so the table stays small while the index array grows with
    // the number of samples.
    let samples = Samples::try_from_histogram(
        UInt32Array::from(vec![Some(0u32); ROWS]),
        builder.finish().expect("the table is built"),
    )
    .expect("one row is referenced by every position");
    let chunk = RawSeriesChunk::try_new(
        &raw_schema(),
        Buffer::from_slice_ref(b"k"),
        Some(raw_metric("a")),
        RawTimestamps::try_new(ScalarBuffer::from((0..ROWS as i64).collect::<Vec<_>>()))
            .expect("ascending timestamps"),
        SeriesFields::try_from_vec(vec![samples]).expect("one sample per timestamp"),
        true,
    )
    .expect("one series' samples are a legal raw chunk");
    let switch = Arc::new(Switch::default());
    let child = RawScripted::plan(vec![
        RawPartition {
            chunks: vec![chunk],
            gate: None,
            gate_chunks: false,
        },
        // An input that has contributed nothing keeps the group unpublished, which is what holds
        // the first input's charges alive to be measured.
        RawPartition {
            chunks: Vec::new(),
            gate: Some(Arc::clone(&switch)),
            gate_chunks: false,
        },
    ]);
    let plan = raw_node(child, 1);
    let pool = Arc::new(GreedyMemoryPool::new(64 << 20));
    let context = task_context(&pool);
    let frames = plan
        .execute(0, Arc::clone(&context))
        .expect("partition 0 exists");
    // The chunk is charged as the input task routes it, which happens without this stream being
    // polled; wait for the charge instead of sampling it for a fixed number of ticks.
    wait_until("the routed chunk is charged", || pool.reserved() > 0).await;
    let peak = pool.reserved();
    switch.open();
    drop(frames);
    let timestamps = ROWS * std::mem::size_of::<i64>();
    let indices = ROWS * std::mem::size_of::<u32>();
    assert!(
        peak >= timestamps + indices,
        "peak {peak} must cover the {timestamps} timestamp bytes and the {indices} index bytes"
    );
}

/// A key-then-time frame promises its keys arrive in order, so an identity that is complete while
/// a smaller one still waits must not be handed out first.
///
/// Input 0 delivers A then B and ends; input 1 delivers B and stays open. B is complete while A is
/// not, and publishing it there would put B before A.
#[tokio::test]
async fn a_key_then_time_frame_publishes_its_keys_in_order() {
    let switch = Arc::new(Switch::default());
    let child = RawScripted::plan_with_ordering(
        vec![
            RawPartition {
                chunks: vec![
                    raw_chunk("a", Some(raw_metric("a")), &[1], vec![Some(1.0)], true),
                    raw_chunk("b", Some(raw_metric("b")), &[1], vec![Some(2.0)], true),
                ],
                gate: None,
                gate_chunks: false,
            },
            RawPartition {
                chunks: vec![raw_chunk(
                    "b",
                    Some(raw_metric("b")),
                    &[2],
                    vec![Some(3.0)],
                    true,
                )],
                gate: Some(Arc::clone(&switch)),
                gate_chunks: false,
            },
        ],
        SeriesOrdering::KeyThenTime,
    );
    let plan = raw_node(child, 1);
    let pool = Arc::new(GreedyMemoryPool::new(8 << 20));
    let context = task_context(&pool);
    let frames = plan
        .execute(0, Arc::clone(&context))
        .expect("partition 0 exists");
    // Read through the frame's own decoder, which is where a key-order violation is refused.
    let mut stream = unpack_raw_series(frames, Some(raw_schema())).expect("the frame decodes");
    assert!(
        tokio::time::timeout(Duration::from_millis(200), stream.next())
            .await
            .is_err(),
        "b is complete but a is not, and a key-then-time frame cannot publish b first"
    );
    switch.open();
    let mut keys = Vec::new();
    let mut times = std::collections::BTreeMap::<Vec<u8>, Vec<i64>>::new();
    while let Some(next) = stream.next().await {
        let chunk = next.expect("the ordered identity arrives");
        keys.push(chunk.primary_key.to_vec());
        times
            .entry(chunk.primary_key.to_vec())
            .or_default()
            .extend(chunk.time.as_buffer().iter().copied());
    }
    // The frame promises its keys arrive in order, and the decoder restates that promise: it splits
    // one identity's rows across chunks, each closing exactly once.
    assert!(
        keys.windows(2).all(|pair| pair[0] <= pair[1]),
        "keys arrive in non-decreasing order: {keys:?}"
    );
    assert_eq!(
        std::collections::BTreeMap::from([(b"a".to_vec(), vec![1]), (b"b".to_vec(), vec![1, 2]),]),
        times,
        "each identity carries all of its inputs' samples"
    );
}

#[tokio::test]
async fn raw_identity_groups_by_primary_key_not_labels() {
    let child = RawScripted::plan(vec![
        RawPartition {
            chunks: vec![raw_chunk(
                "k1",
                Some(raw_metric("a")),
                &[1],
                vec![Some(1.0)],
                true,
            )],
            gate: None,
            gate_chunks: false,
        },
        RawPartition {
            chunks: vec![raw_chunk(
                "k2",
                Some(raw_metric("a")),
                &[2],
                vec![Some(2.0)],
                true,
            )],
            gate: None,
            gate_chunks: false,
        },
    ]);
    let plan = raw_node(child, 1);
    let pool = Arc::new(GreedyMemoryPool::new(8 << 20));
    let context = task_context(&pool);
    let (chunks, error) = drain_raw(&plan, &context).await;
    assert!(
        error.is_none(),
        "different keys with identical labels merge independently"
    );
    let keys: std::collections::HashSet<Vec<u8>> = chunks
        .iter()
        .map(|chunk| chunk.primary_key.as_slice().to_vec())
        .collect();
    assert_eq!(chunks.len(), 2, "one chunk is emitted per primary key");
    assert_eq!(keys, [b"k1".to_vec(), b"k2".to_vec()].into_iter().collect());
}

#[tokio::test]
async fn raw_pieces_of_one_key_must_agree_on_labels() {
    let child = RawScripted::plan(vec![
        RawPartition {
            chunks: vec![raw_chunk(
                "k",
                Some(raw_metric("a")),
                &[1],
                vec![Some(1.0)],
                true,
            )],
            gate: None,
            gate_chunks: false,
        },
        RawPartition {
            chunks: vec![raw_chunk(
                "k",
                Some(raw_metric("b")),
                &[2],
                vec![Some(2.0)],
                true,
            )],
            gate: None,
            gate_chunks: false,
        },
    ]);
    let plan = raw_node(child, 1);
    let pool = Arc::new(GreedyMemoryPool::new(8 << 20));
    let context = task_context(&pool);
    let (_, error) = drain_raw(&plan, &context).await;
    let error = error.expect("label disagreement is surfaced");
    assert!(
        error
            .to_string()
            .contains("disagree on their explicit labels")
    );
}

#[tokio::test]
async fn a_raw_merge_conflict_fails_the_execution() {
    let child = RawScripted::plan(vec![
        RawPartition {
            chunks: vec![raw_chunk(
                "k",
                Some(raw_metric("a")),
                &[1],
                vec![Some(1.0)],
                true,
            )],
            gate: None,
            gate_chunks: false,
        },
        RawPartition {
            chunks: vec![raw_chunk(
                "k",
                Some(raw_metric("a")),
                &[1],
                vec![Some(2.0)],
                true,
            )],
            gate: None,
            gate_chunks: false,
        },
    ]);
    let plan = raw_node(child, 1);
    let pool = Arc::new(GreedyMemoryPool::new(8 << 20));
    let context = task_context(&pool);
    let (_, error) = drain_raw(&plan, &context).await;
    let error = error.expect("conflicting duplicate samples fail the execution");
    assert!(
        error
            .to_string()
            .contains("duplicate raw timestamp has conflicting samples")
    );
}
