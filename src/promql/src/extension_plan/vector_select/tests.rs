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

use super::*;
use datafusion::arrow::array::{
    Array, Float64Array, StringArray, TimestampMillisecondArray, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream, collect,
};
use datafusion::prelude::SessionContext;
use datatypes::series::samples::{FieldSpec, Samples, Series, SeriesFields, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridSpan, GridSpec, TimeAxis};
use datatypes::series::{Label, SeriesLabels, SeriesOrdering};
use futures::StreamExt;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use crate::native::plan::{
    BoundInput, BoundInstant, BoundVectorOp, Grouping, InstantValue, NativeExpr, NativeNode,
    NativeTree, NativeType,
};

const GRID_START: i64 = 300_000;
const GRID_STEP: i64 = 60_000;
const GRID_LEN: usize = 8;
const TIME: &str = "time";
const VALUE: &str = "value";
const HOST: &str = "host";
const NAME: &str = "__name__";
const METRIC: &str = "requests";

fn grid() -> Arc<EvalGrid> {
    Arc::new(
        EvalGrid::try_new(GridSpec::try_new(GRID_START, GRID_STEP, GRID_LEN as u64).unwrap())
            .unwrap(),
    )
}
fn schema(raw: bool) -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: Arc::from([FieldSpec { name: VALUE.into() }]),
        eval_grid: (!raw).then(grid),
    })
}
fn output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(VALUE, DataType::Float64, true),
        Field::new(HOST, DataType::Utf8, true),
        Field::new(NAME, DataType::Utf8, true),
        Field::new(
            TIME,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]))
}
fn memory_exec(parts: Vec<Vec<RecordBatch>>) -> Arc<dyn ExecutionPlan> {
    Arc::new(DataSourceExec::new(Arc::new(
        MemorySourceConfig::try_new(
            &parts,
            crate::extension_plan::fused_series::provenance_frame_output_schema(&schema(false))
                .unwrap(),
            None,
        )
        .unwrap(),
    )))
}
fn chunk(
    host: &str,
    values: &[Option<f64>],
    start: usize,
    last: bool,
) -> crate::evaluated::EvaluatedChunk {
    let axis = TimeAxis::from_grid_spans(grid(), vec![GridSpan::new(start, values.len())]).unwrap();
    let series = Series {
        metric: SeriesLabels::try_new(
            Some(Arc::from(METRIC)),
            vec![Label {
                name: Arc::from(HOST),
                value: Arc::from(host),
            }],
        )
        .unwrap(),
        time: axis,
        fields: SeriesFields::one(Samples::from_float(Float64Array::from(values.to_vec()))),
    };
    crate::evaluated::EvaluatedChunk::try_new(series, last).unwrap()
}
fn frame_chunk(c: crate::evaluated::EvaluatedChunk, producer: u64, ordinal: u64) -> RecordBatch {
    let is_last = c.is_last;
    let close_host = c.series.metric.get(HOST).unwrap().to_string();
    let items = if is_last {
        vec![Ok::<_, common_error::ext::BoxedError>((c.series, true))]
    } else {
        let close = chunk(&close_host, &[], 0, true);
        vec![
            Ok::<_, common_error::ext::BoxedError>((c.series, false)),
            Ok((close.series, true)),
        ]
    };
    let mut stream = common_query::series_frame::pack_evaluated_series(
        Box::pin(futures::stream::iter(items)),
        schema(false),
    )
    .unwrap();
    let batch = futures::executor::block_on(stream.next()).unwrap().unwrap();
    let batch = if is_last { batch } else { batch.slice(0, 1) };
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(UInt64Array::from(vec![producer])));
    columns.push(Arc::new(UInt64Array::from(vec![ordinal])));
    RecordBatch::try_new(
        crate::extension_plan::fused_series::provenance_frame_output_schema(&schema(false))
            .unwrap(),
        columns,
    )
    .unwrap()
}
fn frame(host: &str, value: f64, producer: u64, ordinal: u64) -> RecordBatch {
    frame_chunk(
        chunk(host, &[Some(value); GRID_LEN], 0, true),
        producer,
        ordinal,
    )
}
fn suffix() -> NativeTree {
    let input = NativeType::Samples(schema(false));
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Vector {
            input: Box::new(NativeNode {
                expr: NativeExpr::Input(BoundInput {
                    source_index: 0,
                    output: input.clone(),
                    ordering: SeriesOrdering::Contiguous,
                }),
                output: input.clone(),
            }),
            op: BoundVectorOp::TopK {
                k: 2.0,
                top: true,
                group: Grouping::By(Arc::from([])),
            },
        },
        output: input,
    })
    .unwrap()
}
fn exec_with_input(input: Arc<dyn ExecutionPlan>) -> VectorSelectExec {
    VectorSelectExec::try_new(
        Arc::from("topk(2, x)"),
        input,
        Arc::new(suffix()),
        schema(false),
        output_schema(),
        TIME.into(),
        vec![HOST.into(), NAME.into()],
    )
    .unwrap()
}
fn exec(frames: Vec<RecordBatch>) -> VectorSelectExec {
    exec_with_input(memory_exec(vec![frames]))
}

struct TestInput {
    schema: SchemaRef,
    batch: Option<RecordBatch>,
    error_after: bool,
    dropped: Arc<AtomicBool>,
    props: Arc<PlanProperties>,
}
impl std::fmt::Debug for TestInput {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestInput").finish()
    }
}
impl DisplayAs for TestInput {
    fn fmt_as(&self, _: DisplayFormatType, _: &mut Formatter) -> std::fmt::Result {
        Ok(())
    }
}
impl ExecutionPlan for TestInput {
    fn name(&self) -> &str {
        "TestInput"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn execute(
        &self,
        _: usize,
        _: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        Ok(Box::pin(TestInputStream {
            schema: self.schema.clone(),
            batch: self.batch.clone(),
            error_after: self.error_after,
            dropped: Arc::clone(&self.dropped),
        }))
    }
    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
}
struct TestInputStream {
    schema: SchemaRef,
    batch: Option<RecordBatch>,
    error_after: bool,
    dropped: Arc<AtomicBool>,
}
impl RecordBatchStream for TestInputStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
impl futures::Stream for TestInputStream {
    type Item = DataFusionResult<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(batch) = self.batch.take() {
            return Poll::Ready(Some(Ok(batch)));
        }
        if std::mem::replace(&mut self.error_after, false) {
            return Poll::Ready(Some(Err(DataFusionError::Execution(
                "upstream test failure".into(),
            ))));
        }
        Poll::Ready(None)
    }
}
impl Drop for TestInputStream {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}
fn test_input(
    batch: Option<RecordBatch>,
    error_after: bool,
) -> (Arc<dyn ExecutionPlan>, Arc<AtomicBool>) {
    let schema =
        crate::extension_plan::fused_series::provenance_frame_output_schema(&schema(false))
            .unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let props = Arc::new(PlanProperties::new(
        EquivalenceProperties::new(schema.clone()),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Incremental,
        Boundedness::Bounded,
    ));
    (
        Arc::new(TestInput {
            schema,
            batch,
            error_after,
            dropped: Arc::clone(&dropped),
            props,
        }),
        dropped,
    )
}
#[derive(Debug, Clone, Copy)]
enum RejectAllocation {
    Frames,
    Occupancy,
}
#[derive(Debug)]
struct ControlledPool {
    inner: GreedyMemoryPool,
    reject: RejectAllocation,
    accepted_frames: AtomicBool,
}
impl std::fmt::Display for ControlledPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ControlledPool")
    }
}
impl MemoryPool for ControlledPool {
    fn name(&self) -> &str {
        "controlled vector-select pool"
    }
    fn register(&self, consumer: &datafusion::execution::memory_pool::MemoryConsumer) {
        self.inner.register(consumer);
    }
    fn unregister(&self, consumer: &datafusion::execution::memory_pool::MemoryConsumer) {
        self.inner.unregister(consumer);
    }
    fn grow(
        &self,
        reservation: &datafusion::execution::memory_pool::MemoryReservation,
        additional: usize,
    ) {
        self.inner.grow(reservation, additional);
    }
    fn shrink(
        &self,
        reservation: &datafusion::execution::memory_pool::MemoryReservation,
        shrink: usize,
    ) {
        self.inner.shrink(reservation, shrink);
    }
    fn try_grow(
        &self,
        reservation: &datafusion::execution::memory_pool::MemoryReservation,
        additional: usize,
    ) -> DataFusionResult<()> {
        let name = reservation.consumer().name();
        let is_frames = name.contains("VectorSelectExec frames");
        let is_occupancy = name.contains("VectorSelectExec occupied positions");
        let initial_frame_allocation = is_frames && reservation.size() == 0 && additional > 0;
        let reject = match self.reject {
            RejectAllocation::Frames => is_frames && reservation.size() > 0 && additional > 0,
            RejectAllocation::Occupancy => is_occupancy && additional > 0,
        };
        if reject {
            return Err(DataFusionError::ResourcesExhausted(format!(
                "controlled allocation rejection: {name}"
            )));
        }
        let result = self.inner.try_grow(reservation, additional);
        if result.is_ok() && initial_frame_allocation {
            self.accepted_frames.store(true, Ordering::SeqCst);
        }
        result
    }
    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
    fn memory_limit(&self) -> datafusion::execution::memory_pool::MemoryLimit {
        self.inner.memory_limit()
    }
}
fn controlled_pool(reject: RejectAllocation) -> Arc<ControlledPool> {
    Arc::new(ControlledPool {
        inner: GreedyMemoryPool::new(1 << 20),
        reject,
        accepted_frames: AtomicBool::new(false),
    })
}
fn context(pool: Arc<dyn MemoryPool>) -> Arc<TaskContext> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .build()
        .unwrap();
    Arc::new(TaskContext::default().with_runtime(Arc::new(runtime)))
}
async fn next(stream: &mut SendableRecordBatchStream) -> Option<DataFusionResult<RecordBatch>> {
    futures::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await
}

#[tokio::test]
async fn topk_overlap_is_rejected_before_ranking() {
    let error = collect(
        Arc::new(exec(vec![
            frame("same", 10.0, 0, 0),
            frame("same", 9.0, 1, 0),
        ])),
        SessionContext::default().task_ctx(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("both hold a value"), "{error}");
}
#[tokio::test]
async fn topk_loser_collision_is_rejected_but_control_is_valid() {
    // The colliding pair ranks below both distinct winners. Collision validation must therefore
    // cover losers too, rather than only the candidates retained by topk.
    let error = collect(
        Arc::new(exec(vec![
            frame("winner-a", 100.0, 0, 0),
            frame("winner-b", 90.0, 1, 0),
            frame("same-loser", 2.0, 2, 0),
            frame("same-loser", 1.0, 3, 0),
        ])),
        SessionContext::default().task_ctx(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("both hold a value"), "{error}");

    let control = collect(
        Arc::new(exec(vec![
            frame("winner-a", 100.0, 0, 0),
            frame("winner-b", 90.0, 1, 0),
            frame("loser-a", 2.0, 2, 0),
            frame("loser-b", 1.0, 3, 0),
        ])),
        SessionContext::default().task_ctx(),
    )
    .await
    .unwrap();
    assert_eq!(
        2 * GRID_LEN,
        control.iter().map(RecordBatch::num_rows).sum::<usize>()
    );
}
#[tokio::test]
async fn disjoint_fragments_and_null_samples_are_valid() {
    let first = frame_chunk(chunk("same", &[Some(10.0), None], 0, true), 0, 0);
    let second = frame_chunk(chunk("same", &[Some(1.0), Some(2.0)], 2, true), 1, 0);
    let batches = collect(
        Arc::new(exec(vec![first, second])),
        SessionContext::default().task_ctx(),
    )
    .await
    .unwrap();
    assert_eq!(3, batches.iter().map(RecordBatch::num_rows).sum::<usize>());
}
#[tokio::test]
async fn malformed_ordinal_unfinished_producer_and_upstream_error_end_once() {
    for (batches, expected) in [
        (
            vec![frame("a", 1.0, 0, 0), frame("b", 2.0, 0, 2)],
            "ordinal",
        ),
        (
            vec![
                frame_chunk(chunk("a", &[Some(1.0)], 0, false), 0, 0),
                frame("b", 2.0, 1, 0),
            ],
            "producer",
        ),
    ] {
        // Keep the rows in one input partition so producer/ordinal sequence is checked directly.
        let input = memory_exec(vec![batches]);
        let mut stream = exec_with_input(input)
            .execute(0, SessionContext::default().task_ctx())
            .unwrap();
        let error = next(&mut stream).await.unwrap().unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
        assert!(next(&mut stream).await.is_none());
    }
    let (input, dropped) = test_input(Some(frame("a", 1.0, 0, 0)), true);
    let mut stream = exec_with_input(input)
        .execute(0, SessionContext::default().task_ctx())
        .unwrap();
    let error = next(&mut stream).await.unwrap().unwrap_err();
    assert!(
        error.to_string().contains("upstream test failure"),
        "{error}"
    );
    assert!(next(&mut stream).await.is_none());
    assert!(dropped.load(Ordering::SeqCst));
}
#[tokio::test]
async fn targeted_allocation_failures_release_resources() {
    for reject in [RejectAllocation::Frames, RejectAllocation::Occupancy] {
        let plan = exec(vec![frame("a", 1.0, 0, 0)]);
        let pool = controlled_pool(reject);
        let mut stream = plan
            .execute(0, context(Arc::clone(&pool) as Arc<dyn MemoryPool>))
            .unwrap();
        let error = next(&mut stream).await.unwrap().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("controlled allocation rejection"),
            "{error}"
        );
        assert!(next(&mut stream).await.is_none());
        if matches!(reject, RejectAllocation::Frames) {
            assert!(pool.accepted_frames.load(Ordering::SeqCst));
        }
        assert_eq!(0, pool.reserved());
    }
}
#[tokio::test]
async fn constrained_pool_error_releases_reservations() {
    let plan = exec(vec![frame("a", 1.0, 0, 0)]);
    let pool = Arc::new(GreedyMemoryPool::new(1));
    let mut stream = plan
        .execute(0, context(Arc::clone(&pool) as Arc<dyn MemoryPool>))
        .unwrap();
    let error = next(&mut stream).await.unwrap().unwrap_err();
    assert!(
        error.to_string().to_lowercase().contains("memory"),
        "{error}"
    );
    assert!(next(&mut stream).await.is_none());
    drop(stream);
    assert_eq!(0, pool.reserved());
}
#[tokio::test]
async fn cancellation_after_feed_releases_occupancy_and_upstream() {
    let batch = frame("a", 1.0, 0, 0);
    let (input, dropped) = test_input(None, false);
    let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
    let task_ctx = context(Arc::clone(&pool) as Arc<dyn MemoryPool>);
    let suffix = Arc::new(suffix());
    let state =
        NativeExecutionState::try_new(&suffix, Arc::clone(&pool) as Arc<dyn MemoryPool>).unwrap();
    let mut stream = VectorSelectStream {
        input: Some(input.execute(0, task_ctx).unwrap()),
        batch: None,
        row: 0,
        decoded_batch: None,
        frame_schema: crate::extension_plan::fused_series::series_frame_output_schema(&schema(
            false,
        ))
        .unwrap(),
        held_batch_bytes: 0,
        producer: None,
        next_ordinal: 0,
        previous_closed: true,
        emitted: Emitted::new(),
        occupancy: datafusion::execution::memory_pool::MemoryConsumer::new(
            "VectorSelectExec occupied positions",
        )
        .register(&(pool.clone() as Arc<dyn MemoryPool>)),
        decoder: Some(
            datatypes::series::frame::SeriesFrameDecoder::try_new(
                &schema(false),
                FrameLabelMode::Explicit,
                None,
            )
            .unwrap(),
        ),
        executor: Some(NativeExecutor::new(state)),
        input_id: InputId::new(0),
        series_schema: Some(schema(false)),
        output_schema: output_schema(),
        classification: Arc::from([]),
        tags: Arc::from([]),
        frames: datafusion::execution::memory_pool::MemoryConsumer::new("VectorSelectExec frames")
            .register(&(pool.clone() as Arc<dyn MemoryPool>)),
        draining: false,
        done: false,
    };
    stream.hold(batch).unwrap();
    stream.feed(0).unwrap();
    assert!(stream.occupancy.size() > 0, "feed established occupancy");
    assert!(pool.reserved() > 0);
    assert!(
        !dropped.load(Ordering::SeqCst),
        "input remains attached before cancellation"
    );
    drop(stream);
    assert!(
        dropped.load(Ordering::SeqCst),
        "dropping the stream releases upstream"
    );
    assert_eq!(0, pool.reserved(), "cancellation releases all reservations");
}
#[tokio::test]
async fn eof_releases_decoder_and_drop_releases_pool() {
    let (input, _) = test_input(Some(frame("a", 1.0, 0, 0)), false);
    let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
    let task_ctx = context(Arc::clone(&pool) as Arc<dyn MemoryPool>);
    let suffix = Arc::new(suffix());
    let state =
        NativeExecutionState::try_new(&suffix, Arc::clone(&pool) as Arc<dyn MemoryPool>).unwrap();
    let mut ended = VectorSelectStream {
        input: Some(input.execute(0, Arc::clone(&task_ctx)).unwrap()),
        batch: None,
        row: 0,
        decoded_batch: None,
        frame_schema: crate::extension_plan::fused_series::series_frame_output_schema(&schema(
            false,
        ))
        .unwrap(),
        held_batch_bytes: 0,
        producer: None,
        next_ordinal: 0,
        previous_closed: true,
        emitted: Emitted::new(),
        occupancy: datafusion::execution::memory_pool::MemoryConsumer::new(
            "VectorSelectExec occupied positions",
        )
        .register(&(pool.clone() as Arc<dyn MemoryPool>)),
        decoder: Some(
            datatypes::series::frame::SeriesFrameDecoder::try_new(
                &schema(false),
                FrameLabelMode::Explicit,
                None,
            )
            .unwrap(),
        ),
        executor: Some(NativeExecutor::new(state)),
        input_id: InputId::new(0),
        series_schema: Some(schema(false)),
        output_schema: output_schema(),
        classification: Arc::from([
            OutputColumn::Field(0),
            OutputColumn::Tag(0),
            OutputColumn::Tag(1),
            OutputColumn::Time,
        ]),
        tags: Arc::from([HOST.to_string(), NAME.to_string()]),
        frames: datafusion::execution::memory_pool::MemoryConsumer::new("VectorSelectExec frames")
            .register(&(pool.clone() as Arc<dyn MemoryPool>)),
        draining: false,
        done: false,
    };
    while let Some(item) = futures::future::poll_fn(|cx| Pin::new(&mut ended).poll_next(cx)).await {
        item.unwrap();
    }
    assert_eq!(0, pool.reserved(), "EOF releases execution reservations");
    assert!(ended.done);
    assert!(ended.decoder.is_none());
    assert!(ended.series_schema.is_none());
    assert!(ended.executor.is_none());
    assert!(ended.input.is_none());
    assert!(ended.batch.is_none());
    assert!(ended.decoded_batch.is_none());
    drop(ended);
    assert_eq!(0, pool.reserved());

    let (input, dropped) = test_input(Some(frame("a", 1.0, 0, 0)), false);
    let stream = exec_with_input(input)
        .execute(0, SessionContext::default().task_ctx())
        .unwrap();
    drop(stream);
    assert!(
        dropped.load(Ordering::SeqCst),
        "dropping the stream releases upstream"
    );
}
/// The frame rows of `batches` in one batch, in the order they are given.
fn concat_frames(batches: &[RecordBatch]) -> RecordBatch {
    datafusion::arrow::compute::concat_batches(&batches[0].schema(), batches).unwrap()
}

/// The logical node [`suffix`] is the suffix of.
///
/// The prefix tree must read raw samples and produce evaluated ones, as the real compiled tree
/// does; an instant selection is the smallest such tree. Its physical input is the frames this test
/// builds, so the prefix is never executed here.
fn logical_select() -> VectorSelect {
    let raw = NativeType::Samples(schema(true));
    let evaluated = NativeType::Samples(schema(false));
    let prefix = NativeNode {
        expr: NativeExpr::Instant {
            input: Box::new(NativeNode {
                expr: NativeExpr::Input(BoundInput {
                    source_index: 0,
                    output: raw.clone(),
                    ordering: SeriesOrdering::Contiguous,
                }),
                output: raw,
            }),
            op: BoundInstant {
                lookback_ms: 300_000,
                offset_ms: 0,
                at_ms: None,
                value: InstantValue::Sample,
                output_grid: grid(),
            },
        },
        output: evaluated.clone(),
    };
    let full_tree = NativeTree::try_new(NativeNode {
        expr: NativeExpr::Vector {
            input: Box::new(prefix),
            op: BoundVectorOp::TopK {
                k: 2.0,
                top: true,
                group: Grouping::By(Arc::from([])),
            },
        },
        output: evaluated,
    })
    .unwrap();
    VectorSelect::try_new(
        Arc::from("topk(2, x)"),
        full_tree,
        FusedSeriesBinding {
            grid: grid(),
            metric_name: Some(Arc::from(METRIC)),
            time_index: TIME.into(),
            tags: vec![HOST.into(), NAME.into()],
        },
        datafusion_expr::LogicalPlanBuilder::empty(false)
            .build()
            .unwrap(),
        Arc::new(datafusion::common::DFSchema::try_from(output_schema()).unwrap()),
    )
    .unwrap()
}

/// `(host, time, value)` of every row the selection answered.
fn answered_rows(batches: &[RecordBatch]) -> Vec<(String, i64, f64)> {
    let mut rows = batches
        .iter()
        .flat_map(|batch| {
            let host = batch
                .column_by_name(HOST)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let time = batch
                .column_by_name(TIME)
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let value = batch
                .column_by_name(VALUE)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            (0..batch.num_rows())
                .map(|row| {
                    (
                        host.value(row).to_string(),
                        time.value(row),
                        value.value(row),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| (left.0.as_str(), left.1).cmp(&(right.0.as_str(), right.1)));
    rows
}

/// Frames that arrive interleaved across producers are put back in producer-then-ordinal order by
/// the sort [`VectorSelect::to_execution_plan`] places under the node.
///
/// Three producers hand over two chunks each, one chunk per producer per batch, so no producer's
/// series is contiguous in the input. The stream below this node refuses that: a producer may only
/// change once its last frame arrived, which the control below pins. Through `to_execution_plan` the
/// same frames are sorted first, each series' chunks are consecutive again, and the whole vector is
/// ranked.
#[tokio::test]
async fn interleaved_producers_are_ordered_before_the_selection() {
    // `a` and `c` outrank `b`, so `topk(2)` keeps two of the three series over the whole grid.
    let interleaved = [
        concat_frames(&[
            frame_chunk(chunk("a", &[Some(10.0); 4], 0, false), 0, 0),
            frame_chunk(chunk("b", &[Some(1.0); 4], 0, false), 1, 0),
            frame_chunk(chunk("c", &[Some(100.0); 4], 0, false), 2, 0),
        ]),
        concat_frames(&[
            frame_chunk(chunk("a", &[Some(10.0); 4], 4, true), 0, 1),
            frame_chunk(chunk("b", &[Some(1.0); 4], 4, true), 1, 1),
            frame_chunk(chunk("c", &[Some(100.0); 4], 4, true), 2, 1),
        ]),
    ];
    // Control: the same frames without the sort are refused rather than ranked from part of the
    // vector.
    let unsorted = collect(
        Arc::new(exec_with_input(memory_exec(vec![interleaved.to_vec()]))),
        SessionContext::default().task_ctx(),
    )
    .await
    .unwrap_err();
    assert!(unsorted.to_string().contains("producer"), "{unsorted}");
    // `to_execution_plan` sorts them into producer-then-ordinal order before the node reads them.
    let plan = logical_select()
        .to_execution_plan(memory_exec(vec![interleaved.to_vec()]))
        .unwrap();
    let batches = collect(plan, SessionContext::default().task_ctx())
        .await
        .unwrap();
    let rows = answered_rows(&batches);
    let mut expected = Vec::new();
    for (host, value) in [("a", 10.0), ("c", 100.0)] {
        for position in 0..GRID_LEN {
            expected.push((
                host.to_string(),
                GRID_START + position as i64 * GRID_STEP,
                value,
            ));
        }
    }
    expected.sort_by(|left, right| (left.0.as_str(), left.1).cmp(&(right.0.as_str(), right.1)));
    assert_eq!(
        rows, expected,
        "each producer's chunks must be read as one series again"
    );
}
#[test]
fn producer_ordinal_sort_rejects_wrong_helper_columns() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("wrong", DataType::UInt64, false),
        Field::new(NATIVE_FRAME_ORDINAL_COLUMN, DataType::UInt64, false),
    ]));
    assert!(frame_ordering(&schema).is_err());
}
