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

//! What this node has to get right: the two `EXPLAIN` lines it shows, the batches it runs end to
//! end, and what it refuses to run.
//!
//! The trees here are built node by node with the same bindings the PromQL compiler produces for
//! `rate(x[5m])`: the compiler itself lives in the `query` crate, which depends on this one, so a
//! test in this crate cannot call it.
//!
//! Kernel numerics are not the subject: `rate` is checked against a counter that advances one
//! second per second, so the value it has to produce is exactly 1 (or 2) at every grid point and
//! any position or finalization mistake shows up as a wrong, missing or duplicated row.

use std::collections::VecDeque;
use std::fmt::{self, Display, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll};

use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, StringArray, TimestampMillisecondArray,
};
use datafusion::arrow::buffer::{Buffer, ScalarBuffer};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ToDFSchema;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool, UnboundedMemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::{EmptyRelation, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricValue, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream, collect, displayable,
};
use datafusion::prelude::SessionContext;
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{FieldSpec, Samples, Series, SeriesFields, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridSpan, GridSpec, TimeAxis};
use datatypes::series::{Label, MetricName, SeriesOrdering};
use futures::{Stream, StreamExt, TryStreamExt};

use super::{FusedSeries, FusedSeriesBinding, OutputColumn, POLL_BUDGET, series_to_record_batch};
use crate::evaluated::EvaluatedChunk;
use crate::native::execute::{NativeExecutionState, NativeExecutor};
use crate::native::plan::{
    AggregateKind, AggregateStage, BoundAggregate, BoundInput, BoundWindow, Grouping, NativeExpr,
    NativeNode, NativeTree, NativeType,
};
use crate::range_eval::{RangeFunction, RangeWindow};

/// The query every tree here is compiled from.
const PROMQL: &str = "rate(x[5m])";
/// The range the statement's window selects, which is the `5m` of [`PROMQL`].
const RANGE_MS: i64 = 300_000;
/// The statement's grid: `rate(x[5m])` evaluated every minute from five minutes in.
const GRID_START: i64 = 300_000;
const GRID_STEP: i64 = 60_000;
const GRID_LEN: u64 = 5;
/// One raw sample per minute, from 0 to 9 minutes: enough to fill every window on the grid.
const SAMPLE_STEP: i64 = 60_000;
const SAMPLES: i64 = 10;
/// How many one-sample batches the quiet series below arrives in.
///
/// Two bounds hold at once: the series reaches the end of the last grid point's window (540s, so
/// indices `0..=540`), which is what lets it rate at 1 over the whole grid, and it arrives in more
/// batches than one poll's budget, which is what makes a poll that ignored the budget visible.
const QUIET_SAMPLES: i64 = 541;
/// One second between those samples, so the series rates at 1 once it is read to its end.
///
/// The step is a second and not a minute on purpose: at a minute the series would arrive in ten
/// batches, fewer than [`POLL_BUDGET`], and the budget the test above exists to check would never
/// be reached.
const QUIET_STEP: i64 = 1_000;

const TIME: &str = "time";
const VALUE: &str = "value";
const HOST: &str = "host";
const ZONE: &str = "zone";
const NAME: &str = "__name__";
const METRIC: &str = "http_requests_total";

fn grid() -> Arc<EvalGrid> {
    Arc::new(
        EvalGrid::try_new(GridSpec::try_new(GRID_START, GRID_STEP, GRID_LEN).unwrap()).unwrap(),
    )
}

/// The raw schema the tree's input is bound to: one float field called [`VALUE`].
fn raw_schema() -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: Arc::from([FieldSpec { name: VALUE.into() }]),
        eval_grid: None,
    })
}

/// The tree `rate(x[5m])` compiles to: raw samples folded by one window onto the grid.
fn rate_tree() -> NativeTree {
    let grid = grid();
    // One schema shared by the window's binding and its output, which is what `NativeTree`'s
    // validation compares by identity.
    let evaluated = NativeType::Samples(Arc::new(SeriesSchema {
        fields: raw_schema().fields.clone(),
        eval_grid: Some(Arc::clone(&grid)),
    }));
    let raw = NativeType::Samples(raw_schema());
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(NativeNode {
                expr: NativeExpr::Input(BoundInput {
                    output: raw.clone(),
                    ordering: SeriesOrdering::KeyThenTime,
                }),
                output: raw,
            }),
            op: BoundWindow {
                function: RangeFunction::Rate,
                window: RangeWindow {
                    range_ms: RANGE_MS,
                    offset_ms: 0,
                    anchor_ms: None,
                },
                // The input is raw samples, so it has no grid of its own.
                input_grid: None,
                output_grid: grid,
            },
        },
        output: evaluated,
    })
    .unwrap()
}

/// The `rate(x[5m])` tree with a `sum by (job)` over it, which the runtime does not execute.
fn aggregate_tree() -> NativeTree {
    let rate = rate_tree();
    let window = rate.root().clone();
    let grid = window
        .output
        .samples()
        .expect("a window produces samples")
        .eval_grid
        .clone()
        .expect("a window's output is evaluated on a grid");
    NativeTree::try_new(NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(window),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Complete,
            },
        },
        output: NativeType::Samples(Arc::new(SeriesSchema {
            fields: raw_schema().fields.clone(),
            eval_grid: Some(grid),
        })),
    })
    .expect("the bound tree is legal")
}

/// What the tree's input was compiled against, read off the tree itself.
fn binding(tree: &NativeTree) -> FusedSeriesBinding {
    FusedSeriesBinding {
        series_schema: raw_schema(),
        grid: tree
            .root()
            .output
            .samples()
            .expect("this tree produces samples")
            .eval_grid
            .clone()
            .expect("this tree's output is evaluated on a grid"),
        metric_name: Some(Arc::from(METRIC)),
        time_index: TIME.to_string(),
        tags: vec![HOST.to_string(), NAME.to_string()],
    }
}

/// The batches the input plan produces: the time, the field and the tag column.
fn input_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            TIME,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(VALUE, DataType::Float64, true),
        Field::new(HOST, DataType::Utf8, true),
    ]))
}

/// The columns this node produces: the field, the tags and the grid timestamps.
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

fn input_plan() -> LogicalPlan {
    LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: input_schema().to_dfschema_ref().unwrap(),
    })
}

/// A physical input producing `batches` in one partition, with `schema` as its schema.
fn memory_exec_with(schema: SchemaRef, batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
    Arc::new(DataSourceExec::new(Arc::new(
        MemorySourceConfig::try_new(&[batches], schema, None).unwrap(),
    )))
}

/// A physical input producing `batches` in one partition, on the input schema of these tests.
fn memory_exec(batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
    memory_exec_with(input_schema(), batches)
}

/// A physical input producing each of `partitions`' batches in a partition of its own.
fn memory_exec_partitioned(partitions: Vec<Vec<RecordBatch>>) -> Arc<dyn ExecutionPlan> {
    Arc::new(DataSourceExec::new(Arc::new(
        MemorySourceConfig::try_new(&partitions, input_schema(), None)
            .expect("every partition holds batches of the input schema"),
    )))
}

/// A task context whose memory pool is `pool`, so what the node charges can be read afterwards.
fn task_context(pool: &Arc<GreedyMemoryPool>) -> Arc<TaskContext> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(pool) as Arc<dyn MemoryPool>)
        .build()
        .expect("the runtime environment builds");
    Arc::new(TaskContext::default().with_runtime(Arc::new(runtime)))
}

/// A physical input whose one partition's stream `make` builds.
fn staged(
    schema: SchemaRef,
    make: impl Fn() -> SendableRecordBatchStream + Send + Sync + 'static,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(StagedPlan {
        properties: Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )),
        make: Box::new(make),
    })
}

/// A physical input, so a test can stage what a memory source cannot: a pending poll, an error
/// after some rows, and a stream whose drop it can see.
struct StagedPlan {
    make: Box<dyn Fn() -> SendableRecordBatchStream + Send + Sync>,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for StagedPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("StagedPlan").finish()
    }
}

impl DisplayAs for StagedPlan {
    fn fmt_as(&self, _t: DisplayFormatType, _f: &mut Formatter) -> fmt::Result {
        Ok(())
    }
}

impl ExecutionPlan for StagedPlan {
    fn name(&self) -> &str {
        "StagedPlan"
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
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        // A scan answers the partitions it declares and no others, which is what makes an index
        // past this plan's count an error the node above has to pass on.
        if partition >= self.properties.partitioning.partition_count() {
            return Err(DataFusionError::Execution(format!(
                "StagedPlan has no partition {partition}"
            )));
        }
        Ok((self.make)())
    }
}

/// The stream a [`StagedPlan`] hands over: `batches`, then an optional error, with a pending poll
/// and a drop a test can observe.
struct StagedStream {
    schema: SchemaRef,
    batches: VecDeque<RecordBatch>,
    handed: usize,
    /// Hand this many batches, then answer [`Poll::Pending`] once.
    pending_after: Option<usize>,
    /// The error to report once the batches are exhausted.
    error: Option<&'static str>,
    /// Set when this stream is dropped, so a test can see the input being released.
    dropped: Option<Arc<AtomicBool>>,
    /// Counts the batches this stream has handed over, so a test can see how far one poll read.
    handed_out: Option<Arc<AtomicUsize>>,
}

impl StagedStream {
    fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self {
            schema,
            batches: batches.into(),
            handed: 0,
            pending_after: None,
            error: None,
            dropped: None,
            handed_out: None,
        }
    }

    fn pending_after(mut self, batches: usize) -> Self {
        self.pending_after = Some(batches);
        self
    }

    fn error(mut self, message: &'static str) -> Self {
        self.error = Some(message);
        self
    }

    fn dropped(mut self, flag: &Arc<AtomicBool>) -> Self {
        self.dropped = Some(Arc::clone(flag));
        self
    }

    fn counted(mut self, counter: &Arc<AtomicUsize>) -> Self {
        self.handed_out = Some(Arc::clone(counter));
        self
    }
}

impl Stream for StagedStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        if this.pending_after == Some(this.handed) {
            this.pending_after = None;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        match this.batches.pop_front() {
            Some(batch) => {
                this.handed += 1;
                if let Some(counter) = &this.handed_out {
                    counter.fetch_add(1, Ordering::SeqCst);
                }
                Poll::Ready(Some(Ok(batch)))
            }
            None => match this.error.take() {
                Some(message) => {
                    Poll::Ready(Some(Err(DataFusionError::Execution(message.to_string()))))
                }
                None => Poll::Ready(None),
            },
        }
    }
}

impl RecordBatchStream for StagedStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Drop for StagedStream {
    fn drop(&mut self) {
        if let Some(dropped) = &self.dropped {
            dropped.store(true, Ordering::SeqCst);
        }
    }
}

fn fused(tree: NativeTree) -> FusedSeries {
    let binding = binding(&tree);
    FusedSeries::try_new(
        Arc::from(PROMQL),
        tree,
        binding,
        input_plan(),
        output_schema().to_dfschema_ref().unwrap(),
    )
    .expect("the binding matches the tree")
}

/// One batch of one series' `(timestamp, value)` rows.
fn series_batch(host: &str, rows: &[(i64, f64)]) -> RecordBatch {
    let times = rows.iter().map(|(time, _)| *time).collect::<Vec<_>>();
    let values = rows.iter().map(|(_, value)| *value).collect::<Vec<_>>();
    let hosts = rows.iter().map(|_| host).collect::<Vec<_>>();
    RecordBatch::try_new(
        input_schema(),
        vec![
            Arc::new(TimestampMillisecondArray::from(times)),
            Arc::new(Float64Array::from(values)),
            Arc::new(StringArray::from(hosts)),
        ],
    )
    .unwrap()
}

/// One series' samples: one per minute from 0, `scale` seconds per second.
fn series_samples(scale: f64) -> Vec<(i64, f64)> {
    (0..SAMPLES)
        .map(|index| {
            let time = index * SAMPLE_STEP;
            (time, scale * time as f64 / 1000.0)
        })
        .collect()
}

/// Two series, each split over two batches. A series' batches stay adjacent, which is the input
/// contract: a label set's rows are contiguous across the whole stream.
fn two_series_batches() -> Vec<RecordBatch> {
    let first = series_samples(1.0);
    let second = series_samples(2.0);
    vec![
        series_batch("web-1", &first[..3]),
        series_batch("web-1", &first[3..]),
        series_batch("web-2", &second[..3]),
        series_batch("web-2", &second[3..]),
    ]
}

/// One series split over three batches, so its chunks have to be stitched.
fn one_series_batches() -> Vec<RecordBatch> {
    let rows = series_samples(1.0);
    vec![
        series_batch("web-1", &rows[..4]),
        series_batch("web-1", &rows[4..7]),
        series_batch("web-1", &rows[7..]),
    ]
}

/// The rows of `batches`: timestamp, value, host and metric name.
type Row = (i64, Option<f64>, Option<String>, Option<String>);

fn rows(batches: &[RecordBatch]) -> Vec<Row> {
    let mut rows = Vec::new();
    for batch in batches {
        let time = batch
            .column_by_name(TIME)
            .expect("every batch has a time column")
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("the time column is a millisecond timestamp");
        let value = batch
            .column_by_name(VALUE)
            .expect("every batch has the field column")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("the field column is Float64");
        let host = batch
            .column_by_name(HOST)
            .expect("every batch has the tag column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("the tag column is Utf8");
        let name = batch
            .column_by_name(NAME)
            .expect("every batch has the metric name column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("the metric name column is Utf8");
        for row in 0..batch.num_rows() {
            rows.push((
                time.value(row),
                value.is_valid(row).then(|| value.value(row)),
                host.is_valid(row).then(|| host.value(row).to_string()),
                name.is_valid(row).then(|| name.value(row).to_string()),
            ));
        }
    }
    rows
}

/// The input schema of the collision below: the time, the field, `host` and `zone`.
fn zone_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            TIME,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(VALUE, DataType::Float64, true),
        Field::new(HOST, DataType::Utf8, true),
        Field::new(ZONE, DataType::Utf8, true),
    ]))
}

/// The two rows of the collision: one series, once with an empty `zone` and once with no `zone`.
fn zone_batches() -> Vec<RecordBatch> {
    let schema = zone_schema();
    let samples = series_samples(1.0);
    let times: Vec<_> = samples.iter().map(|(time, _)| *time).collect();
    let values: Vec<_> = samples.iter().map(|(_, value)| *value).collect();
    let hosts: Vec<_> = times.iter().map(|_| "web-1").collect();
    // The two rows differ only in the zone column: empty against null.
    let zones: [ArrayRef; 2] = [
        Arc::new(StringArray::from(vec![""; times.len()])),
        Arc::new(StringArray::from(vec![None::<&str>; times.len()])),
    ];
    zones
        .into_iter()
        .map(|zone| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(TimestampMillisecondArray::from(times.clone())),
                    Arc::new(Float64Array::from(values.clone())),
                    Arc::new(StringArray::from(hosts.clone())),
                    zone,
                ],
            )
            .expect("one batch of the two rows")
        })
        .collect()
}

/// `rate(x[5m])` over an input that carries `zone` as well as `host`, with both in the output.
fn zone_node() -> FusedSeries {
    let schema = zone_schema();
    let tree = rate_tree();
    let binding = FusedSeriesBinding {
        tags: vec![HOST.to_string(), ZONE.to_string(), NAME.to_string()],
        ..binding(&tree)
    };
    FusedSeries::try_new(
        Arc::from(PROMQL),
        tree,
        binding,
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::clone(&schema).to_dfschema_ref().unwrap(),
        }),
        zone_output_schema().to_dfschema_ref().unwrap(),
    )
    .expect("the binding matches the tree")
}

/// The output of [`zone_node`]: the field, `host`, `zone`, the dropped name and the grid times.
fn zone_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(VALUE, DataType::Float64, true),
        Field::new(HOST, DataType::Utf8, true),
        Field::new(ZONE, DataType::Utf8, true),
        Field::new(NAME, DataType::Utf8, true),
        Field::new(
            TIME,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]))
}

/// The canonical label set these tests collide on, as the runtime renders it in its refusal.
const CLOSED_LABEL_SET: &str =
    "the node at root already closed the label set [(\"host\", \"web-1\")]";

/// Two rows that are already one Prometheus identity, carried under two different raw keys, are
/// refused by the runtime that would have to answer both.
///
/// `{host="web-1", zone=""}` and `{host="web-1"}` are one identity to Prometheus: an empty label
/// value is no label, so the two rows are the same series and `rate` creates no collision here.
/// What the two *raw* keys differ in is the empty `zone` against the null one, which the scan
/// encodes differently and the converter therefore opens two input series for. `rate` drops the
/// metric name and the empty value, so the second raw series arrives on the canonical label set the
/// runtime has already closed; the refusal names that label set rather than answering it, because
/// answering would report one series twice.
#[tokio::test]
async fn differing_raw_keys_that_close_on_one_label_set_are_refused() {
    let exec = zone_node()
        .to_execution_plan(memory_exec_with(zone_schema(), zone_batches()))
        .expect("the runtime executes a rate tree");
    let error = collect(exec, SessionContext::default().task_ctx())
        .await
        .expect_err("the second raw key cannot be answered under the first one's label set");
    assert!(
        error.to_string().contains(CLOSED_LABEL_SET),
        "refused for another reason: {error}"
    );
}

/// A runtime error after buffered rows is reported, and the terminated stream releases its input.
///
/// The converter holds the batch it is reading, the chunk it is building and its own source, so a
/// stream that kept its input after the error would hold all three until the consumer drops it. The
/// collision above is the error: it happens on the second series, after the first one's rows have
/// been handed out.
#[tokio::test]
async fn a_runtime_error_after_rows_releases_the_input() {
    let dropped = Arc::new(AtomicBool::new(false));
    let schema = zone_schema();
    let batches = zone_batches();
    let source = staged(schema.clone(), {
        let dropped = Arc::clone(&dropped);
        move || Box::pin(StagedStream::new(schema.clone(), batches.clone()).dropped(&dropped))
    });
    let mut stream = zone_node()
        .to_execution_plan(source)
        .expect("the runtime executes a rate tree")
        .execute(0, SessionContext::default().task_ctx())
        .expect("the node executes");

    let mut answered = Vec::new();
    let error = loop {
        match stream.next().await {
            Some(Ok(batch)) => answered.push(batch),
            Some(Err(error)) => break error,
            None => panic!(
                "the collision was answered instead of refused: {:?}",
                rows(&answered)
            ),
        }
    };
    assert!(
        error.to_string().contains(CLOSED_LABEL_SET),
        "refused for another reason: {error}"
    );
    // The first series' rows were handed out before the error, and the input is gone although the
    // terminated stream is still alive here.
    assert!(
        !answered.is_empty(),
        "the first series is answered before the error"
    );
    assert!(
        dropped.load(Ordering::SeqCst),
        "the terminated stream still holds its input"
    );
}

/// Two series with *different* names collapse once `rate` drops the name.
///
/// This is the collision the canonical label set exists for, at the layer that sees it:
/// `http_requests_total{host="web-1"}` and `http_errors_total{host="web-1"}` reach the window as two
/// identities and leave it as `{host="web-1"}`. [`FusedSeries`] binds one constant metric name for
/// every series it reads, so this collision cannot be staged through it; the runtime is driven
/// directly, with the names on the raw chunks.
#[test]
fn two_differently_named_series_collide_once_rate_drops_the_name() {
    let tree = rate_tree();
    let mut execution = NativeExecutor::new(
        NativeExecutionState::try_new(
            &tree,
            Arc::new(UnboundedMemoryPool::default()),
            &ExecutionPlanMetricsSet::new(),
            0,
        )
        .expect("the runtime executes a rate tree"),
    );
    let samples = series_samples(1.0);
    execution
        .push_raw(raw_chunk(METRIC, &samples))
        .expect("the first series is a legal raw chunk");
    execution
        .next_chunk()
        .expect("the first series is answered")
        .expect("rate decides every grid point of the first series");
    execution
        .push_raw(raw_chunk("http_errors_total", &samples))
        .expect("the second series is a legal raw chunk");
    let error = execution
        .next_chunk()
        .expect_err("the second series lands on the label set the first one closed");
    assert!(
        error.to_string().contains(CLOSED_LABEL_SET),
        "refused for another reason: {error}"
    );
}

/// One raw chunk of the series `name{host="web-1"}`: one sample per minute, closing its series.
fn raw_chunk(name: &str, samples: &[(i64, f64)]) -> RawSeriesChunk {
    let times = samples.iter().map(|(time, _)| *time).collect::<Vec<_>>();
    let values = samples.iter().map(|(_, value)| *value).collect::<Vec<_>>();
    RawSeriesChunk::try_new(
        &raw_schema(),
        Buffer::from_slice_ref(name.as_bytes()),
        Some(
            MetricName::try_new(
                Some(Arc::from(name)),
                vec![Label {
                    name: Arc::from(HOST),
                    value: Arc::from("web-1"),
                }],
            )
            .expect("the labels of this test are legal"),
        ),
        RawTimestamps::try_new(ScalarBuffer::from(times)).expect("ascending timestamps"),
        SeriesFields::one(Samples::from_float(Float64Array::from(values))),
        true,
    )
    .expect("one series' samples are a legal raw chunk")
}

/// The grid timestamps of the statement, in order.
fn grid_timestamps() -> Vec<i64> {
    (0..GRID_LEN)
        .map(|index| GRID_START + index as i64 * GRID_STEP)
        .collect()
}

/// A waker a test can ask whether it was woken.
///
/// A poll that stops on its own budget has to wake itself, and the default waker a `poll!` uses
/// swallows that, so the tests that check it poll with this one.
#[derive(Default)]
struct CountingWake(AtomicBool);

impl futures::task::ArcWake for CountingWake {
    fn wake_by_ref(woken: &Arc<Self>) {
        woken.0.store(true, Ordering::SeqCst);
    }
}

impl CountingWake {
    fn woken(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

/// One series as many chunks that settle nothing: one sample per batch, all before the grid starts.
///
/// The counter advances one second per second, so `rate` answers 1 at every grid point once the
/// series is read to its end; until a sample reaches the grid's first point, a push decides
/// nothing and settles no chunk.
fn quiet_series_batches() -> Vec<RecordBatch> {
    (0..QUIET_SAMPLES)
        .map(|index| series_batch("web-1", &[(index * QUIET_STEP, index as f64)]))
        .collect()
}

/// Asserts that `rows` are `rates`' series, each over the whole grid in order.
fn assert_series_rates(rows: &[Row], rates: &[(f64, &str)]) {
    let expected = rates
        .iter()
        .flat_map(|(rate, host)| {
            grid_timestamps()
                .into_iter()
                .map(move |time| (time, *rate, *host))
        })
        .collect::<Vec<_>>();
    assert_eq!(expected.len(), rows.len(), "{rows:?}");
    for (row, (expected_time, expected_rate, expected_host)) in rows.iter().zip(expected) {
        let (time, value, host, _) = row;
        assert_eq!(&expected_time, time);
        assert_eq!(Some(expected_host), host.as_deref());
        let value = value.expect("rate has a value at every grid point");
        assert!(
            (expected_rate - value).abs() < 1e-9,
            "{expected_host} at {expected_time}: {value} is not {expected_rate}"
        );
    }
}

fn count(metrics: &MetricsSet, name: &str) -> usize {
    match metrics.sum_by_name(name) {
        Some(MetricValue::Count { count, .. }) => count.value(),
        other => panic!("no count named {name}: {other:?}"),
    }
}

/// Prints `node` the way the logical `EXPLAIN` prints it.
struct Explain<'a>(&'a FusedSeries);

impl Display for Explain<'_> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        self.0.fmt_for_explain(f)
    }
}

/// The logical node prints the query a reader wrote, not the tree it compiled to.
#[test]
fn fmt_for_explain_prints_the_promql() {
    assert_eq!(
        "FusedSeries: rate(x[5m])",
        Explain(&fused(rate_tree())).to_string()
    );
}

/// The physical node prints the tree it was built from, rendered by the tree's own display.
#[test]
fn fmt_as_prints_the_native_tree() {
    let exec = fused(rate_tree())
        .to_execution_plan(memory_exec(Vec::new()))
        .expect("the runtime executes a rate tree");
    let text = displayable(exec.as_ref()).indent(false).to_string();

    assert!(
        text.starts_with("FusedSeriesExec: rate(x[5m])"),
        "the node line names the query first: {text}"
    );
    // A distinctive line of the compiled tree: the window's bound range, offset, anchor and grid.
    assert!(
        text.contains("root Window rate [300000ms] offset 0 anchor - on grid 300000/60000x5"),
        "the node line carries the tree it runs: {text}"
    );
    assert!(
        text.contains("input Input raw samples"),
        "the node line carries the tree's source: {text}"
    );
}

/// `rate(x[5m])` runs end to end over two raw series: values, tags and the dropped metric name.
#[tokio::test]
async fn runs_rate_over_two_series() {
    let exec = fused(rate_tree())
        .to_execution_plan(memory_exec(two_series_batches()))
        .expect("the runtime executes a rate tree");
    let batches = collect(exec, SessionContext::default().task_ctx())
        .await
        .expect("the node runs");
    let rows = rows(&batches);

    // A counter advancing one second per second rates at 1, and at 2 for the series that advances
    // two: the value at each grid point says the right window was read for the right series.
    let expected = [(1.0, "web-1"), (2.0, "web-2")]
        .into_iter()
        .flat_map(|(rate, host)| {
            grid_timestamps()
                .into_iter()
                .map(move |time| (time, rate, host))
        })
        .collect::<Vec<_>>();
    assert_eq!(expected.len(), rows.len(), "{rows:?}");
    for (row, (expected_time, expected_rate, expected_host)) in rows.iter().zip(expected) {
        let (time, value, host, _) = row;
        assert_eq!(&expected_time, time);
        assert_eq!(Some(expected_host), host.as_deref());
        let value = value.expect("rate has a value at every grid point");
        assert!(
            (expected_rate - value).abs() < 1e-9,
            "{expected_host} at {expected_time}: {value} is not {expected_rate}"
        );
    }
    // `rate` drops `__name__`, so the tag column that reports it is empty for every row.
    assert!(
        rows.iter().all(|(_, _, _, name)| name.is_none()),
        "rate drops the metric name: {rows:?}"
    );
}

/// A series that spans several input chunks keeps its absolute grid positions and closes once.
#[tokio::test]
async fn a_series_spanning_chunks_keeps_absolute_positions_and_closes_once() {
    let exec = fused(rate_tree())
        .to_execution_plan(memory_exec(one_series_batches()))
        .expect("the runtime executes a rate tree");
    let batches = collect(Arc::clone(&exec), SessionContext::default().task_ctx())
        .await
        .expect("the node runs");
    let rows = rows(&batches);

    // Every grid position exactly once, in order: a chunk's range is never read as a row number.
    let times = rows.iter().map(|row| row.0).collect::<Vec<_>>();
    assert_eq!(grid_timestamps(), times);
    assert!(
        rows.iter().all(|(_, value, ..)| value.is_some()),
        "every grid point of this series has a value: {rows:?}"
    );

    // The source closed the series on exactly one chunk, and the window saw exactly that one
    // closing: one `is_last`, not one per input batch.
    let metrics = exec
        .metrics()
        .expect("the node reports its runtime's metrics");
    assert_eq!(1, count(&metrics, "native.root.input.completed_series"));
    assert_eq!(1, count(&metrics, "native.root.completed_series"));
}

/// A tree the runtime does not execute is refused when the execution plan is built.
#[tokio::test]
async fn a_tree_the_runtime_cannot_run_is_rejected_when_the_exec_is_built() {
    let node = fused(aggregate_tree());
    let error = node
        .to_execution_plan(memory_exec(Vec::new()))
        .expect_err("the runtime does not execute aggregate nodes");
    assert!(
        matches!(error, DataFusionError::NotImplemented(_)),
        "the runtime refuses the tree it cannot run: {error}"
    );
    assert!(error.to_string().contains("Aggregate"), "{error}");
}

/// A tree evaluated on another grid than the node declares is refused before it runs.
#[test]
fn a_tree_bound_to_another_grid_is_rejected() {
    let tree = rate_tree();
    let other_grid =
        Arc::new(EvalGrid::try_new(GridSpec::try_new(0, GRID_STEP, GRID_LEN).unwrap()).unwrap());
    let binding = FusedSeriesBinding {
        grid: other_grid,
        ..binding(&tree)
    };
    let error = FusedSeries::try_new(
        Arc::from(PROMQL),
        tree,
        binding,
        input_plan(),
        output_schema().to_dfschema_ref().unwrap(),
    )
    .expect_err("the tree is not evaluated on the declared grid");
    assert!(error.to_string().contains("grid"), "{error}");
}

/// A grid allocated separately from the tree's, with the same spec, is the tree's grid.
///
/// The comparison is by spec, which is the rule [`NativeTree`]'s own validation and the kernels
/// apply, and a chunk's timestamps come from its absolute grid positions rather than from the
/// allocation, so nothing downstream needs the `Arc` to be shared. This pairs with
/// [`a_tree_bound_to_another_grid_is_rejected`]: a different spec is still refused.
#[test]
fn a_separately_allocated_grid_with_an_equal_spec_is_accepted() {
    let tree = rate_tree();
    let equal = Arc::new(
        EvalGrid::try_new(GridSpec::try_new(GRID_START, GRID_STEP, GRID_LEN).unwrap()).unwrap(),
    );
    assert!(
        !Arc::ptr_eq(&equal, &binding(&tree).grid),
        "the two grids are separate allocations"
    );
    let binding = FusedSeriesBinding {
        grid: equal,
        ..binding(&tree)
    };
    FusedSeries::try_new(
        Arc::from(PROMQL),
        tree,
        binding,
        input_plan(),
        output_schema().to_dfschema_ref().unwrap(),
    )
    .expect("an equal grid spec is the same grid");
}

/// An input that ends without a series still runs the tree, and yields no rows.
#[tokio::test]
async fn an_empty_input_runs_the_tree_and_yields_no_rows() {
    let exec = fused(rate_tree())
        .to_execution_plan(memory_exec(Vec::new()))
        .expect("the runtime executes a rate tree");
    let batches = collect(Arc::clone(&exec), SessionContext::default().task_ctx())
        .await
        .expect("a tree with no series in it still closes");
    assert!(rows(&batches).is_empty(), "{:?}", rows(&batches));
    // The run closed on no series, so nothing was completed and nothing was decided.
    let metrics = exec
        .metrics()
        .expect("the node reports its runtime's metrics");
    assert_eq!(0, count(&metrics, "native.root.completed_series"));
    assert_eq!(0, count(&metrics, "native.root.input.completed_series"));
}

/// An upstream error after some rows is reported, and the terminated stream releases that input.
#[tokio::test]
async fn an_upstream_error_after_rows_is_reported() {
    let dropped = Arc::new(AtomicBool::new(false));
    let schema = input_schema();
    let batches = one_series_batches();
    let source = staged(schema.clone(), {
        let dropped = Arc::clone(&dropped);
        move || {
            Box::pin(
                StagedStream::new(schema.clone(), batches.clone())
                    .error("the source failed")
                    .dropped(&dropped),
            )
        }
    });
    let mut stream = fused(rate_tree())
        .to_execution_plan(source)
        .expect("the runtime executes a rate tree")
        .execute(0, SessionContext::default().task_ctx())
        .expect("the node executes");

    // The error is reported rather than swallowed by the rows the input already handed over.
    let mut answered = Vec::new();
    let error = loop {
        match stream.next().await {
            Some(Ok(batch)) => answered.push(batch),
            Some(Err(error)) => break error,
            None => panic!(
                "the input's error was swallowed after {:?}",
                rows(&answered)
            ),
        }
    };
    assert!(
        error.to_string().contains("the source failed"),
        "reported as another error: {error}"
    );
    // Still alive here: the release is the stream's, not the drop of a temporary.
    assert!(
        dropped.load(Ordering::SeqCst),
        "the terminated stream still holds its input"
    );
}

/// A pending poll loses nothing when the next-item future is dropped and the stream polled again.
#[tokio::test]
async fn a_pending_boundary_loses_no_rows() {
    let expected = rows(
        &collect(
            fused(rate_tree())
                .to_execution_plan(memory_exec(one_series_batches()))
                .expect("the runtime executes a rate tree"),
            SessionContext::default().task_ctx(),
        )
        .await
        .expect("the node runs"),
    );
    assert!(!expected.is_empty(), "the fixture decides some grid points");

    let schema = input_schema();
    let batches = one_series_batches();
    let source = staged(schema.clone(), move || {
        Box::pin(
            StagedStream::new(schema.clone(), batches.clone())
                // One batch is in the converter's hands when the stream reports pending.
                .pending_after(1),
        )
    });
    let mut stream = fused(rate_tree())
        .to_execution_plan(source)
        .expect("the runtime executes a rate tree")
        .execute(0, SessionContext::default().task_ctx())
        .expect("the node executes");

    // The future is dropped at the end of this block, as a consumer that gives up waiting does.
    {
        let next = stream.next();
        futures::pin_mut!(next);
        assert!(
            futures::poll!(next.as_mut()).is_pending(),
            "the input's pending poll reaches the consumer"
        );
    }
    let batches = stream
        .try_collect::<Vec<_>>()
        .await
        .expect("the stream resumes where the pending poll left it");
    assert_eq!(expected, rows(&batches));
}

/// A tag the input has no column for is refused when the plan is built, naming the column.
#[test]
fn a_missing_tag_column_is_rejected_when_the_plan_is_built() {
    let tree = rate_tree();
    let binding = FusedSeriesBinding {
        tags: vec![HOST.to_string(), ZONE.to_string(), NAME.to_string()],
        ..binding(&tree)
    };
    let error = FusedSeries::try_new(
        Arc::from(PROMQL),
        tree,
        binding,
        input_plan(),
        zone_output_schema().to_dfschema_ref().unwrap(),
    )
    .expect("the logical node does not see the input's columns")
    .to_execution_plan(memory_exec(Vec::new()))
    .expect_err("the input has no zone column");
    assert!(
        error.to_string().contains("no column \"zone\""),
        "refused for another reason: {error}"
    );
}

/// A tag column whose type the converter does not read as a label is refused when the plan is
/// built, naming the column.
#[test]
fn a_wrong_typed_tag_column_is_rejected_when_the_plan_is_built() {
    // `zone` is numeric here, which no label of a raw series can be read from.
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            TIME,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(VALUE, DataType::Float64, true),
        Field::new(HOST, DataType::Utf8, true),
        Field::new(ZONE, DataType::Float64, true),
    ]));
    let tree = rate_tree();
    let binding = FusedSeriesBinding {
        tags: vec![HOST.to_string(), ZONE.to_string(), NAME.to_string()],
        ..binding(&tree)
    };
    let error = FusedSeries::try_new(
        Arc::from(PROMQL),
        tree,
        binding,
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::clone(&schema).to_dfschema_ref().unwrap(),
        }),
        zone_output_schema().to_dfschema_ref().unwrap(),
    )
    .expect("the logical node does not see the input's column types")
    .to_execution_plan(memory_exec_with(schema, Vec::new()))
    .expect_err("a numeric column cannot carry a label");
    assert!(
        error.to_string().contains("tag \"zone\""),
        "refused without naming the column: {error}"
    );
}

/// An input column the binding does not map is refused when the plan is built, naming the column.
///
/// `zone` is part of these series' identity to the scan but not to the binding, so reading the
/// input would drop it: rows a Prometheus query keeps apart would be answered as one series with
/// the wrong labels.
#[test]
fn an_input_column_the_binding_does_not_map_is_rejected() {
    let tree = rate_tree();
    let binding = binding(&tree);
    let error = FusedSeries::try_new(
        Arc::from(PROMQL),
        tree,
        binding,
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: zone_schema().to_dfschema_ref().unwrap(),
        }),
        output_schema().to_dfschema_ref().unwrap(),
    )
    .expect("the logical node does not see the input's columns")
    .to_execution_plan(memory_exec_with(zone_schema(), Vec::new()))
    .expect_err("the binding does not account for the zone column");
    assert!(
        error.to_string().contains("column \"zone\""),
        "refused without naming the column: {error}"
    );
}

/// An input with two columns of one name is refused when the plan is built.
///
/// The coverage check reads names, so both `host` columns pass it as a role the binding maps, while
/// the mapping itself reads `index_of`, which answers with one index: the other column would be
/// part of these rows' identity that a raw series never carries, and rows a Prometheus query keeps
/// apart would be answered as one series with the wrong labels -- and no error. Nothing about the
/// two columns says which one was meant, so the schema is refused rather than one of them chosen.
#[test]
fn an_input_with_two_columns_of_one_name_is_rejected() {
    let duplicated = Arc::new(Schema::new(vec![
        Field::new(
            TIME,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(VALUE, DataType::Float64, true),
        Field::new(HOST, DataType::Utf8, true),
        // The second `host` column, which the name-based coverage check accepts just as well.
        Field::new(HOST, DataType::Utf8, true),
    ]));
    let error = fused(rate_tree())
        .to_execution_plan(memory_exec_with(duplicated, Vec::new()))
        .expect_err("the binding maps one of the two host columns");
    assert!(
        error
            .to_string()
            .contains("more than one column named \"host\""),
        "refused without naming the duplicate: {error}"
    );
}

/// One poll reads at most [`POLL_BUDGET`] chunks of input, and stopping there loses nothing.
///
/// `rate(x[5m])` decides nothing until a sample reaches the grid's first point, so a series can
/// arrive as many chunks that settle no batch at all: a poll that kept reading them would hold the
/// task's thread until the series ended, which is what stops other tasks on that thread and delays
/// the cancellation a consumer asked for. The first series here is [`QUIET_SAMPLES`] such chunks;
/// the second one covers the grid, so a poll that resumes properly still answers both.
#[tokio::test]
async fn one_poll_reads_at_most_the_budget_and_loses_nothing() {
    let mut batches = quiet_series_batches();
    batches.push(series_batch("web-2", &series_samples(2.0)));
    let total = batches.len();
    assert!(
        total > POLL_BUDGET,
        "the fixture has to outlast one poll: {total} batches"
    );

    let pulled = Arc::new(AtomicUsize::new(0));
    let woken = Arc::new(CountingWake::default());
    let schema = input_schema();
    let source = staged(schema.clone(), {
        let pulled = Arc::clone(&pulled);
        move || Box::pin(StagedStream::new(schema.clone(), batches.clone()).counted(&pulled))
    });
    let mut stream = fused(rate_tree())
        .to_execution_plan(source)
        .expect("the runtime executes a rate tree")
        .execute(0, SessionContext::default().task_ctx())
        .expect("the node executes");

    // The first poll: the quiet series settles nothing, so the budget is what ends this poll.
    let waker = futures::task::waker(Arc::clone(&woken));
    let mut context = Context::from_waker(&waker);
    assert!(
        stream.as_mut().poll_next(&mut context).is_pending(),
        "chunks that settle nothing cannot answer a poll"
    );
    let read = pulled.load(Ordering::SeqCst);
    assert!(read > 0, "the poll did read its input");
    // One step polls the converter once, and the converter reads at most two batches per poll: the
    // key it is holding and the first row of the next, which is what tells it the first key ended.
    // The bound is therefore two batches per step, and the budget is what it is measured in.
    assert!(
        read <= 2 * POLL_BUDGET + 1,
        "one poll read {read} of {total} batches, past its budget of {POLL_BUDGET} steps"
    );
    assert!(
        read < total,
        "one poll read the whole input ({read} batches): the budget did not stop it"
    );
    assert!(
        woken.woken(),
        "a poll that stopped on its budget has to wake itself"
    );

    // Nothing the budget stopped is lost: the stream resumes where the poll left it.
    let batches = stream
        .try_collect::<Vec<_>>()
        .await
        .expect("the stream resumes where the bounded poll left it");
    assert_series_rates(&rows(&batches), &[(1.0, "web-1"), (2.0, "web-2")]);
}

/// A batch that is ready is handed over before any batch behind it is read.
///
/// Each series below is one batch, and a series closes on the first batch of the next one, so the
/// first answer needs two batches and nothing more. A poll that read on would answer the same batch
/// only after the whole input, which is exactly what the budget above exists to prevent.
#[tokio::test]
async fn a_ready_batch_is_returned_before_later_input_is_read() {
    let batches = (0..20)
        .map(|index| series_batch(&format!("web-{index}"), &series_samples(1.0)))
        .collect::<Vec<_>>();
    let total = batches.len();
    let pulled = Arc::new(AtomicUsize::new(0));
    let schema = input_schema();
    let source = staged(schema.clone(), {
        let pulled = Arc::clone(&pulled);
        move || Box::pin(StagedStream::new(schema.clone(), batches.clone()).counted(&pulled))
    });
    let mut stream = fused(rate_tree())
        .to_execution_plan(source)
        .expect("the runtime executes a rate tree")
        .execute(0, SessionContext::default().task_ctx())
        .expect("the node executes");

    let first = stream
        .next()
        .await
        .expect("the first series is answered")
        .expect("the first series is answered without an error");
    let answered = rows(&[first]);
    assert_series_rates(&answered, &[(1.0, "web-0")]);
    // Two batches: the first series' own, and the one whose first key closes it.
    assert_eq!(
        2,
        pulled.load(Ordering::SeqCst),
        "the answer waited for input behind the series it reports"
    );
    assert!(total > 2, "the fixture has to outlast the first answer");
}

/// A chunk whose rows form several runs is converted row by row from the rows it carries.
///
/// A sparse result is one chunk holding two runs with a hole between them
/// ([`EvaluatedChunk::range`] answers `None` for it). Reading the hole as rows would invent grid
/// points this series holds no value at, and reading positions as compact rows would shift every
/// row after the hole onto the wrong timestamp: the batch has to hold exactly the carried rows, at
/// their absolute grid times, with nulls nowhere else.
#[test]
fn a_chunk_of_several_runs_converts_its_own_rows_only() {
    let grid = grid();
    let axis = TimeAxis::from_grid_spans(
        Arc::clone(&grid),
        // Grid positions 0..2 and 4..5: position 2 and 3 hold no row at all.
        vec![GridSpan::new(0, 2), GridSpan::new(4, 1)],
    )
    .expect("the spans are increasing and inside the grid");
    let chunk = EvaluatedChunk::try_new(
        Series {
            metric: MetricName::try_new(Some(Arc::from(METRIC)), Vec::new())
                .expect("one metric name is a legal identity"),
            time: axis,
            fields: SeriesFields::try_from_vec(vec![Samples::from_float(Float64Array::from(
                // One value per carried row, so the second run's value sits at grid position 4.
                vec![7.0, 8.0, 11.0],
            ))])
            .expect("one field is a legal field list"),
        },
        true,
    )
    .expect("a chunk on a grid");

    // The columns of `output_schema`, in its order: the field, the two tags, then the time.
    let batch = series_to_record_batch(
        &[
            OutputColumn::Field(0),
            OutputColumn::Tag(0),
            OutputColumn::Tag(1),
            OutputColumn::Time,
        ],
        &[HOST.to_string(), NAME.to_string()],
        &output_schema(),
        &chunk,
    )
    .expect("the chunk converts");

    let stamps = batch
        .column_by_name(TIME)
        .expect("the batch has its time column")
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .expect("the time column is a millisecond timestamp");
    let values = batch
        .column_by_name(VALUE)
        .expect("the batch has its field column")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("the field column is Float64");
    assert_eq!(3, batch.num_rows(), "only the carried rows are rows");
    let grid_stamps = grid.timestamps();
    assert_eq!(
        vec![grid_stamps[0], grid_stamps[1], grid_stamps[4]],
        stamps.values()[..3].to_vec(),
        "the carried rows keep their absolute grid positions"
    );
    assert_eq!(vec![7.0, 8.0, 11.0], values.values()[..3].to_vec());
}

/// Every message in `error`'s chain, so an error a layer above wrapped is still read.
fn error_chain(error: &DataFusionError) -> String {
    let mut message = error.to_string();
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        source = cause.source();
    }
    message
}

/// Two partitions, one series each, and both series leave the window on the same label set.
///
/// `rate` drops `__name__`, so `http_requests_total{host="web-1"}` reads as `{host="web-1"}` in
/// the output. `PerSeries` distribution keeps each of the two whole inside its own partition, so a
/// check that lived inside one execution would see one series each and report nothing: only state
/// shared by the partitions sees that the output vector holds the label set twice.
#[tokio::test]
async fn a_duplicate_label_set_across_partitions_is_refused() {
    let series = vec![series_batch("web-1", &series_samples(1.0))];
    let exec = fused(rate_tree())
        .to_execution_plan(memory_exec_partitioned(vec![series.clone(), series]))
        .expect("the runtime executes a rate tree");
    assert_eq!(
        2,
        exec.properties().output_partitioning().partition_count(),
        "this test needs one series in each of two partitions"
    );

    let error = collect(exec, SessionContext::default().task_ctx())
        .await
        .expect_err("the second partition lands on the label set the first one claimed");
    let message = error_chain(&error);
    assert!(
        message.contains("vector cannot contain metrics with the same labelset"),
        "refused for another reason: {message}"
    );
    assert!(
        message.contains("host=\"web-1\""),
        "the label set must be readable: {message}"
    );
}

/// The same label set on points that do not overlap is legal, and every point survives.
///
/// A rate needs two samples in its window, so the first series only has a value at the first grid
/// point and the second only at the last: the two share a final label set but no evaluation
/// timestamp. Prometheus merges such series into one; this node emits both batches and leaves the
/// merge to matrix assembly, so the check has to allow what it would otherwise refuse.
#[tokio::test]
async fn a_duplicate_label_set_on_disjoint_grid_points_is_emitted() {
    let first = vec![series_batch(
        "web-1",
        &[(60_000, 60.0), (GRID_START, GRID_START as f64 / 1_000.0)],
    )];
    let last = GRID_START + (GRID_LEN as i64 - 1) * GRID_STEP;
    let second = vec![series_batch(
        "web-1",
        &[
            (GRID_START, GRID_START as f64 / 1_000.0),
            (last, last as f64 / 1_000.0),
        ],
    )];
    let exec = fused(rate_tree())
        .to_execution_plan(memory_exec_partitioned(vec![first, second]))
        .expect("the runtime executes a rate tree");

    let batches = collect(exec, SessionContext::default().task_ctx())
        .await
        .expect("disjoint grid points are legal");
    assert_eq!(2, batches.len(), "each partition emits its own series");
    let mut rows = rows(&batches);
    rows.sort_by_key(|row| row.0);
    assert_eq!(
        vec![GRID_START, last],
        rows.iter().map(|row| row.0).collect::<Vec<_>>(),
        "both series must survive with the one point each holds"
    );
    for ((_, value, host, _), rate) in rows.iter().zip([1.0, 1.0]) {
        assert_eq!(Some("web-1"), host.as_deref());
        let value = value.expect("the point each series holds has a value");
        assert!((rate - value).abs() < 1e-9, "{value} is not {rate}");
    }
}

/// What a label set holds stays charged after the stream that first claimed it ends.
///
/// The map belongs to the plan: it outlives every partition and is released with the plan. Charging
/// each stream for what it inserted returned the bytes of a label set when *that* stream ended,
/// while the entry the next partition is checked against stayed in the map, so everything the map
/// held after the first partition was invisible to the pool. What is charged here is therefore read
/// between the partitions: it has to survive the first stream and grow no further when the second
/// partition lands on the label set the first one claimed.
#[tokio::test]
async fn a_label_set_stays_charged_after_its_first_stream_ends() {
    // The first series holds a value only at the first grid point and the second only at the last,
    // so the two share a final label set without sharing an evaluation timestamp: the second
    // partition is legal and has to be answered, and it has to find the first one's entry there.
    let first = vec![series_batch(
        "web-0",
        &[(60_000, 60.0), (GRID_START, GRID_START as f64 / 1_000.0)],
    )];
    let last = GRID_START + (GRID_LEN as i64 - 1) * GRID_STEP;
    let second = vec![series_batch(
        "web-0",
        &[
            (GRID_START, GRID_START as f64 / 1_000.0),
            (last, last as f64 / 1_000.0),
        ],
    )];
    let exec = fused(rate_tree())
        .to_execution_plan(memory_exec_partitioned(vec![first, second]))
        .expect("the runtime executes a rate tree");
    assert_eq!(
        2,
        exec.properties().output_partitioning().partition_count(),
        "this test needs one series in each of two partitions"
    );
    let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
    let context = task_context(&pool);

    // The first partition claims the label set, answers it and ends.
    let mut stream = exec
        .execute(0, Arc::clone(&context))
        .expect("the node executes");
    let mut answered = Vec::new();
    while let Some(batch) = stream.next().await {
        answered.push(batch.expect("the first partition runs"));
    }
    assert_eq!(1, answered.len(), "one series over one grid point");
    drop(stream);
    let charged = pool.reserved();
    assert!(
        charged > 0,
        "the first partition's label set is still in the plan's map, but nothing is charged for it"
    );

    // The second partition lands on that same label set on points the first one does not hold: it
    // is answered, and what it adds to the entry is already paid for.
    let mut stream = exec
        .execute(1, Arc::clone(&context))
        .expect("the node executes");
    let mut answered = Vec::new();
    while let Some(batch) = stream.next().await {
        answered.push(batch.expect("the second partition runs"));
    }
    assert_eq!(1, answered.len(), "disjoint points are legal");
    drop(stream);
    assert_eq!(
        charged,
        pool.reserved(),
        "a label set the map already holds is not charged a second time"
    );

    // The entries live as long as the plan, so dropping it is what gives their bytes back.
    drop(exec);
    assert_eq!(
        0,
        pool.reserved(),
        "the plan's drop releases what its label sets held"
    );
}

/// Each partition runs its own series and answers it, with no label set claimed twice.
#[tokio::test]
async fn every_partition_evaluates_its_own_series() {
    let partitions = [1.0, 2.0, 3.0]
        .into_iter()
        .enumerate()
        .map(|(index, scale)| {
            vec![series_batch(
                &format!("web-{index}"),
                &series_samples(scale),
            )]
        })
        .collect::<Vec<_>>();
    let exec = fused(rate_tree())
        .to_execution_plan(memory_exec_partitioned(partitions))
        .expect("the runtime executes a rate tree");
    assert_eq!(
        3,
        exec.properties().output_partitioning().partition_count(),
        "the node's partition count is its input's"
    );

    let batches = collect(exec, SessionContext::default().task_ctx())
        .await
        .expect("each partition runs its own tree");
    assert_eq!(3, batches.len(), "one batch per partition");
    let rows = rows(&batches);
    assert_eq!(
        3 * GRID_LEN as usize,
        rows.len(),
        "every partition reports its own series over the whole grid"
    );
    // Batches arrive in whatever order the partitions settle, so the series are read apart.
    for (index, scale) in [1.0, 2.0, 3.0].into_iter().enumerate() {
        let host = format!("web-{index}");
        let mut series = rows
            .iter()
            .filter(|row| row.2.as_deref() == Some(host.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        series.sort_by_key(|row| row.0);
        assert_eq!(GRID_LEN as usize, series.len(), "{host}: {series:?}");
        assert_series_rates(&series, &[(scale, host.as_str())]);
    }
}

/// A partition index the input does not have is refused rather than answered.
///
/// The node runs one evaluator per input partition and adds none of its own, so it forwards the
/// index to its input: the count it declares is the input's, and an index past that is the input's
/// refusal, not a series read from another partition.
#[tokio::test]
async fn a_partition_past_the_inputs_count_is_refused() {
    let schema = input_schema();
    let source = staged(schema.clone(), move || {
        Box::pin(StagedStream::new(schema.clone(), Vec::new()))
    });
    let exec = fused(rate_tree())
        .to_execution_plan(source)
        .expect("the runtime executes a rate tree");
    assert_eq!(
        1,
        exec.properties().output_partitioning().partition_count(),
        "the node's partition count is its input's"
    );
    exec.execute(0, SessionContext::default().task_ctx())
        .expect("the one partition the input has");
    let error = match exec.execute(1, SessionContext::default().task_ctx()) {
        Ok(_) => panic!("a partition the input does not have is answered"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("partition"),
        "refused for another reason: {error}"
    );
}
