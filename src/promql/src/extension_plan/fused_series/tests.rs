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
use std::sync::atomic::{AtomicBool, Ordering};
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
use datafusion::execution::memory_pool::UnboundedMemoryPool;
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
use datatypes::series::samples::{FieldSpec, Samples, SeriesFields, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridSpec};
use datatypes::series::{Label, MetricName, SeriesOrdering};
use futures::{Stream, StreamExt, TryStreamExt};

use super::{FusedSeries, FusedSeriesBinding};
use crate::native::execute::{NativeExecutionState, NativeExecutor};
use crate::native::plan::{
    AggregateKind, AggregateStage, BoundAggregate, BoundInput, BoundWindow, NativeExpr, NativeNode,
    NativeTree, NativeType,
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
                group: Arc::new(["job".into()]),
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
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
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
fn series_batch(host: &'static str, rows: &[(i64, f64)]) -> RecordBatch {
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
