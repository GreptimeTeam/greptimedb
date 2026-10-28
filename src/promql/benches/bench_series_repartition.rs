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

//! Benchmarks for `SeriesRepartitionExec`: what one execution of the node costs as the child's
//! partition count `N` and the node's output partition count `M` grow.
//!
//! One iteration is one execution, driven the way an executor drives it: a fresh node over a
//! scripted in-memory child, one consumer task per output partition all spawned before any is
//! awaited, every output drained to its end, and the decoded chunks dropped. What is timed is
//! therefore the node's own path - decoding each child frame, hashing one label set per chain,
//! buffering the chain, charging it to the task context's pool, one channel send per chain,
//! re-encoding per output - and nothing else: no scan, no I/O, no table cache. Dropping what a
//! consumer was handed is inside the timed section because the execution pays it.
//!
//! The matrix is the one that made the node's cost legible, on fixed workloads (see [`workload`]):
//! three shapes, each timed at every child-partition count in [`INPUTS`] crossed with every
//! output-partition count in [`OUTPUTS`]. That is 3 x 4 x 3 = 36 cells rather than the full 4 x 4 x 3:
//! criterion times every cell many times, and `M = 2` shows nothing the cells either side of it do not,
//! so it is the first thing left out to keep a full matrix runnable. A subset runs with criterion's
//! filter: `-- series_repartition/scenario-1` for one workload, or
//! `-- series_repartition/scenario-2/n8_m8` for one cell.
//!
//! What a number here means, and what it does not:
//!
//! - A cell reports the wall time of one execution and, as its throughput, the workload's rows
//!   (points) per second. Every cell prints its shape first - series, chunks, points - so chains per
//!   second follows by dividing, and a cell's workload is the same in every run of the matrix.
//! - Peak reservation is **not** a benchmark number: criterion times, and a memory high-water mark
//!   is not a duration. It is printed once per cell, from the untimed drain that gates the cell, and
//!   never inside the timed closure. The pool it is read from records the high-water mark where a
//!   reservation is granted rather than sampling what is reserved now, because a chain is held only
//!   as long as it takes to read it.
//! - Every cell is gated before it is timed, so a bench that measures the wrong thing fails loudly
//!   instead of reporting a number: the rows and series that went in are the rows and series that
//!   came out, and each output carries the series and rows the routing should have sent it.
//!
//! The child answers from memory, and the node's input tasks are `common_runtime::spawn_query` tasks
//! on the process-global query runtime, so the input side runs on that runtime's workers and not on
//! the one built here for the consumer tasks. Everything the child does is scripted, so a number
//! here is the node's cost and not a child's.
//!
//! ```text
//! cargo bench -p promql --bench bench_main                                     # the whole matrix
//! cargo bench -p promql --bench bench_main -- series_repartition/scenario-1     # one workload
//! cargo bench -p promql --bench bench_main -- series_repartition/scenario-2/n8_m8  # one cell
//! ```

use std::collections::BTreeSet;
use std::fmt::{self, Formatter};
use std::hash::{Hash, Hasher};
use std::hint::black_box;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use common_error::ext::BoxedError;
use common_query::series_frame::{
    SendableEvaluatedSeriesStream, pack_evaluated_series, unpack_evaluated_series,
};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group};
use datafusion::arrow::array::Float64Array;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::DataFusionError;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::context::TaskContext;
use datafusion::execution::memory_pool::{MemoryLimit, MemoryPool, MemoryReservation};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datatypes::series::samples::{FieldSpec, Samples, Series, SeriesFields, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridSpan, GridSpec, TimeAxis};
use datatypes::series::{Label, SeriesLabels};
use futures::Stream;
use promql::extension_plan::SeriesRepartitionExec;
use twox_hash::XxHash64;

/// The grid every frame of these benchmarks is addressed on: a value per second, long enough for a
/// whole long chain of [`LONG_CHUNKS`] chunks of [`LONG_POINTS`] points.
const GRID_START: i64 = 0;
const GRID_STEP: i64 = 1_000;
const GRID_LEN: u64 = 4_096;

/// The series of the two workloads built from short chains: thousands of them, one chunk each, which
/// is the shape a query over many metrics produces.
const MANY_SERIES: usize = 8_192;

/// The points of a short chain, so that a chain is one chunk and one complete series at once.
const SHORT_POINTS: usize = 3;

/// The series of the long-chain workload, and the chunks and points each chain is made of.
const LONG_SERIES: usize = 8;
const LONG_CHUNKS: usize = 64;
const LONG_POINTS: usize = 64;

/// The child partition counts every workload is timed at.
const INPUTS: [usize; 4] = [1, 2, 4, 8];

/// The output partition counts every workload is timed at. See the module docs for why `2` is not
/// one of them.
const OUTPUTS: [usize; 3] = [1, 4, 8];

/// The workload numbers, in the order the module docs describe them.
const SCENARIOS: [usize; 3] = [1, 2, 3];

/// The worker threads of the runtime the consumer tasks run on. The node's own input tasks run on
/// the process-global query runtime instead, and that worker count is not this benchmark's to pick.
const CONSUMER_WORKERS: usize = 8;

/// What the pool of a drain may hand out. Far above anything these workloads reserve on purpose: a
/// cell that met it would be a cell whose read failed, since the node refuses rather than buffer
/// past the pool.
const POOL_LIMIT: usize = 1 << 30;

/// The seed behind every routing decision of the node.
///
/// A copy of the node's private `ROUTING_SEED`, because a workload has to know where its series will
/// land before it can be built: the skewed workload is skewed only if nine tenths of its series
/// route to one output, and the long-chain workload gives each chain an owner of its own. The copy is
/// checked rather than trusted - [`Cell::gate`] asserts that every output carried exactly the series
/// and rows this copy predicted, so a changed seed or a changed canonicalization fails the bench
/// instead of silently timing a different workload.
const ROUTING_SEED: u64 = 0x517c_c1b7_2722_0a95;

/// A frame schema of one field on a grid long enough for every workload below.
fn schema() -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: Arc::from([FieldSpec {
            name: Arc::from("value"),
        }]),
        eval_grid: Some(Arc::new(
            EvalGrid::try_new(
                GridSpec::try_new(GRID_START, GRID_STEP, GRID_LEN).expect("a grid of this length"),
            )
            .expect("a grid of this specification"),
        )),
    })
}

/// The labels of the `cpu` metric on one host, which is one series.
fn labels(host: &str) -> SeriesLabels {
    SeriesLabels::try_new(
        Some(Arc::from("cpu")),
        vec![Label {
            name: Arc::from("host"),
            value: Arc::from(host),
        }],
    )
    .expect("the labels of these workloads are legal")
}

/// The `index`-th candidate series of a workload: a label set of its own.
fn candidate(index: usize) -> SeriesLabels {
    labels(&format!("perf-{index}"))
}

/// One evaluated chunk: `len` rows from grid position `start`, and whether it closes its series.
fn chunk_on(
    schema: &Arc<SeriesSchema>,
    metric: SeriesLabels,
    start: usize,
    len: usize,
    is_last: bool,
) -> Step {
    let spans = if len == 0 {
        Vec::new()
    } else {
        vec![GridSpan::new(start, len)]
    };
    let time = TimeAxis::from_grid_spans(Arc::clone(schema.eval_grid.as_ref().unwrap()), spans)
        .expect("one run on the grid");
    let values = (0..len)
        .map(|index| Some((start + index) as f64))
        .collect::<Vec<_>>();
    let fields = SeriesFields::one(Samples::from_float(Float64Array::from(values)));
    (
        Series::try_new(schema, metric, time, fields).expect("a chunk of this schema"),
        is_last,
    )
}

/// The canonical label set of `metric`: what the node puts in its hash, and what makes two chunks
/// pieces of one series.
///
/// The metric name when it is not empty, then every tag whose value is not empty - Prometheus' view,
/// which is the one the node routes by. It is ordered, so two of them that are equal are the same
/// series and can be counted as one.
fn canonical(metric: &SeriesLabels) -> Vec<(Arc<str>, Arc<str>)> {
    metric
        .name()
        .filter(|name| !name.is_empty())
        .map(|name| (Arc::from("__name__"), Arc::from(name)))
        .into_iter()
        .chain(
            metric
                .tags()
                .iter()
                .filter(|tag| !tag.value.is_empty())
                .map(|tag| (tag.name.clone(), tag.value.clone())),
        )
        .collect()
}

/// The output partition a canonical label set belongs on, as the node decides it.
///
/// The node's routing is private, so this is the same rule written out: hash the canonical label set
/// with the node's seeded hasher, and reduce it modulo the output count. [`Cell::gate`] checks it
/// against the node's own decisions on every cell, which is what makes copying it safe.
fn routes_to(metric: &SeriesLabels, outputs: usize) -> usize {
    let mut hasher = XxHash64::with_seed(ROUTING_SEED);
    canonical(metric).hash(&mut hasher);
    (hasher.finish() % outputs as u64) as usize
}

/// The `found`-th candidate whose label set routes to `want`, and the cursor to look on from.
///
/// Found rather than assumed, so a workload's premise - this series lands on that output - holds by
/// construction whatever the seed or the canonicalization becomes.
fn candidate_routing_to(want: usize, outputs: usize, cursor: &mut usize) -> SeriesLabels {
    loop {
        let metric = candidate(*cursor);
        *cursor += 1;
        if routes_to(&metric, outputs) == want {
            return metric;
        }
    }
}

/// One measured workload: the steps of each input partition, and what they add up to.
///
/// `chunks` is what went in rather than what has to come back: a packed frame flushes at a fixed row
/// bound, so a chunk that straddles a flush is read back as two pieces of itself. Rows and series
/// survive that and are what the gate is on.
struct Workload {
    steps: Vec<Vec<Step>>,
    chunks: usize,
    rows: usize,
    series: usize,
    /// How many of the series and how many of their rows the routing sends to each output.
    per_output: Vec<usize>,
    rows_per_output: Vec<usize>,
}

/// The workload of `scenario` split over `inputs` partitions, routing into `outputs`.
///
/// The series and chunk counts are fixed by the scenario rather than by either partition count, so
/// every cell of one scenario moves the same data and only its shape differs. A chain is never split
/// across two input partitions: which chunks of a series an input carries is the child's business,
/// and two inputs holding two halves of one series would be a child this node refuses to compose
/// with.
fn workload(scenario: usize, inputs: usize, outputs: usize) -> Workload {
    assert!(
        inputs > 0 && outputs > 0,
        "a partition count is at least one"
    );
    let schema = schema();
    let mut steps = vec![Vec::new(); inputs];
    let mut per_output = vec![0usize; outputs];
    let mut rows_per_output = vec![0usize; outputs];
    let mut chunks = 0usize;
    let mut rows = 0usize;
    let mut series = 0usize;

    match scenario {
        // Many independent series: one short chain each, spread over the outputs by the hash and over
        // the inputs round robin.
        1 => {
            for index in 0..MANY_SERIES {
                let metric = candidate(index);
                let output = routes_to(&metric, outputs);
                per_output[output] += 1;
                rows_per_output[output] += SHORT_POINTS;
                steps[index % inputs].push(chunk_on(&schema, metric, 0, SHORT_POINTS, true));
                chunks += 1;
                rows += SHORT_POINTS;
            }
            series = MANY_SERIES;
        }
        // Few long chains on different owners: [`LONG_SERIES`] chains of [`LONG_CHUNKS`] chunks, each
        // routed to an output of its own as far as there are outputs to spread them over.
        2 => {
            let mut cursor = 0usize;
            for index in 0..LONG_SERIES {
                let output = index % outputs;
                let metric = candidate_routing_to(output, outputs, &mut cursor);
                per_output[output] += 1;
                rows_per_output[output] += LONG_CHUNKS * LONG_POINTS;
                for chunk_index in 0..LONG_CHUNKS {
                    let start = chunk_index * LONG_POINTS;
                    let is_last = chunk_index + 1 == LONG_CHUNKS;
                    steps[index % inputs].push(chunk_on(
                        &schema,
                        metric.clone(),
                        start,
                        LONG_POINTS,
                        is_last,
                    ));
                    chunks += 1;
                    rows += LONG_POINTS;
                }
                series += 1;
            }
        }
        // Skewed owners: nine tenths of the short chains hash to one output and the rest to another,
        // which is the case that leaves every input parked on one output's queue.
        3 => {
            let heavy = MANY_SERIES / 10 * 9;
            // One output makes every series heavy by construction, and that is the degenerate cell
            // of this scenario rather than a second shape.
            let mut heavy_left = if outputs == 1 { MANY_SERIES } else { heavy };
            let mut light_left = if outputs == 1 { 0 } else { MANY_SERIES - heavy };
            let mut cursor = 0usize;
            while series < MANY_SERIES {
                let metric = candidate(cursor);
                cursor += 1;
                let output = routes_to(&metric, outputs);
                let wanted = if output == 0 {
                    &mut heavy_left
                } else if output == 1 {
                    &mut light_left
                } else {
                    continue;
                };
                if *wanted == 0 {
                    continue;
                }
                *wanted -= 1;
                per_output[output] += 1;
                rows_per_output[output] += SHORT_POINTS;
                steps[series % inputs].push(chunk_on(&schema, metric, 0, SHORT_POINTS, true));
                chunks += 1;
                rows += SHORT_POINTS;
                series += 1;
            }
            assert_eq!(
                (0, 0),
                (heavy_left, light_left),
                "the skewed workload placed every series it meant to"
            );
        }
        other => panic!("no scenario {other}"),
    }

    Workload {
        steps,
        chunks,
        rows,
        series,
        per_output,
        rows_per_output,
    }
}

/// One step of a scripted child partition: a chunk and whether it closes its series.
type Step = (Series, bool);

/// The steps of one partition, as the frame stream that hands them over.
struct Script {
    steps: std::vec::IntoIter<Step>,
}

impl Stream for Script {
    type Item = Result<Step, BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.steps.next().map(Ok))
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
    fn plan(series_schema: Arc<SeriesSchema>, steps: Vec<Vec<Step>>) -> Arc<dyn ExecutionPlan> {
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

/// A pool that keeps the peak of what it has reserved, so a cell can report it afterwards.
///
/// DataFusion's own pools report what is reserved now and not the high-water mark, and sampling that
/// number from another task can miss the instant a chain was held. Recording it where a reservation
/// is granted cannot: `grow` and `try_grow` are the only two ways the total goes up, and both record
/// what they reached.
#[derive(Debug)]
struct PeakPool {
    limit: usize,
    /// What is reserved now, and the highest it has been.
    used: AtomicUsize,
    peak: AtomicUsize,
}

impl PeakPool {
    /// A pool that hands out up to `limit` bytes.
    fn new(limit: usize) -> Self {
        Self {
            limit,
            used: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }

    /// The highest total this pool has had reserved.
    fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    /// Records `total` as the total now reserved, keeping the high-water mark.
    fn record(&self, total: usize) {
        self.peak.fetch_max(total, Ordering::Relaxed);
    }
}

impl MemoryPool for PeakPool {
    fn name(&self) -> &str {
        "peak-recording"
    }

    fn grow(&self, _reservation: &MemoryReservation, additional: usize) {
        let total = self.used.fetch_add(additional, Ordering::Relaxed) + additional;
        self.record(total);
    }

    fn shrink(&self, _reservation: &MemoryReservation, shrink: usize) {
        self.used.fetch_sub(shrink, Ordering::Relaxed);
    }

    fn try_grow(
        &self,
        _reservation: &MemoryReservation,
        additional: usize,
    ) -> DataFusionResult<()> {
        let mut used = self.used.load(Ordering::Relaxed);
        loop {
            let total = used + additional;
            if total > self.limit {
                return Err(DataFusionError::ResourcesExhausted(format!(
                    "peak-recording pool refused {additional} bytes with {used} of {} reserved",
                    self.limit
                )));
            }
            match self
                .used
                .compare_exchange_weak(used, total, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => {
                    self.record(total);
                    return Ok(());
                }
                Err(actual) => used = actual,
            }
        }
    }

    fn reserved(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.limit)
    }
}

impl fmt::Display for PeakPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}(used: {}, peak: {}, limit: {})",
            self.name(),
            self.reserved(),
            self.peak(),
            self.limit
        )
    }
}

/// A task context whose memory pool is `pool`, so what the node charges can be read afterwards.
fn peak_context(pool: Arc<dyn MemoryPool>) -> Arc<TaskContext> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .build()
        .expect("the runtime environment builds");
    Arc::new(TaskContext::default().with_runtime(Arc::new(runtime)))
}

/// What one output partition handed over: its chunks, and the errors it ended with.
type DrainedOutput = (Vec<Step>, Vec<String>);

/// Reads `stream` to its end, keeping what it handed over and how it failed.
async fn until_end(stream: &mut SendableEvaluatedSeriesStream) -> DrainedOutput {
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

/// Drains every output of `plan` the way an executor does: one task per partition, all of them
/// spawned before any is awaited.
///
/// Several streams polled from one task would measure this benchmark's own scheduler rather than the
/// node: the node suspends a producer on an output nobody is draining, so the outputs have to be
/// read at the same time to be read at all.
fn drain_apart(
    runtime: &tokio::runtime::Runtime,
    plan: &Arc<dyn ExecutionPlan>,
    outputs: usize,
    context: &Arc<TaskContext>,
) -> Vec<DrainedOutput> {
    runtime.block_on(async {
        let mut tasks = Vec::with_capacity(outputs);
        for partition in 0..outputs {
            let mut stream = unpack_evaluated_series(
                plan.execute(partition, Arc::clone(context))
                    .expect("the output executes"),
                None,
            )
            .expect("the output declares an evaluated frame");
            tasks.push(tokio::spawn(async move { until_end(&mut stream).await }));
        }
        let mut drained = Vec::with_capacity(tasks.len());
        for task in tasks {
            drained.push(task.await.expect("an output task ends"));
        }
        drained
    })
}

/// What came out of one drain, counted the way the gate reads it.
struct Counted {
    /// The distinct series each output carried, and the rows it carried.
    series: Vec<usize>,
    rows: Vec<usize>,
}

impl Counted {
    /// Counts what every output of `drained` handed over, and fails if one of them failed.
    ///
    /// Counting is the gate's business and not the timed section's: this is called on the untimed
    /// drain that decides whether the cell may be timed at all.
    fn of(drained: &[DrainedOutput]) -> Self {
        let mut series = Vec::with_capacity(drained.len());
        let mut rows = Vec::with_capacity(drained.len());
        for (partition, (chunks, errors)) in drained.iter().enumerate() {
            assert!(
                errors.is_empty(),
                "output partition {partition} failed with {errors:?}"
            );
            // Identity is the canonical label set, not its rendering and not the metric name: every
            // short chain of these workloads is `cpu`, and only its labels tell them apart.
            let distinct = chunks
                .iter()
                .map(|(chunk, _)| canonical(&chunk.metric))
                .collect::<BTreeSet<_>>()
                .len();
            series.push(distinct);
            rows.push(
                chunks
                    .iter()
                    .map(|(chunk, _)| chunk.time.len())
                    .sum::<usize>(),
            );
        }
        Self { series, rows }
    }
}

/// One cell of the matrix: a workload at one (child partitions, output partitions) pair.
struct Cell {
    scenario: usize,
    inputs: usize,
    outputs: usize,
    workload: Workload,
}

impl Cell {
    fn new(scenario: usize, inputs: usize, outputs: usize) -> Self {
        Self {
            scenario,
            inputs,
            outputs,
            workload: workload(scenario, inputs, outputs),
        }
    }

    /// A fresh child plan over this cell's workload, one partition per list of steps.
    fn plan(&self) -> Arc<dyn ExecutionPlan> {
        Scripted::plan(schema(), self.workload.steps.clone())
    }

    /// One execution of the node over `child`: the timed section of this cell, less its counting.
    fn drain(
        &self,
        runtime: &tokio::runtime::Runtime,
        child: &Arc<dyn ExecutionPlan>,
        context: &Arc<TaskContext>,
    ) -> Vec<DrainedOutput> {
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            SeriesRepartitionExec::try_new(Arc::clone(child), self.outputs)
                .expect("the node is built"),
        );
        drain_apart(runtime, &plan, self.outputs, context)
    }

    /// Fails unless a drain carried this cell's workload back, which is what has to hold before the
    /// cell's time means anything.
    ///
    /// Anything but the workload back - a lost series, a duplicated row, a chain on the wrong output -
    /// is a cell that would report the cost of something other than the workload it claims, so it
    /// panics here rather than in the table.
    fn gate(&self, drained: &[DrainedOutput]) {
        let counted = Counted::of(drained);
        assert_eq!(
            (self.workload.series, self.workload.rows),
            (
                counted.series.iter().sum::<usize>(),
                counted.rows.iter().sum::<usize>()
            ),
            "scenario {} at {}x{} did not carry its workload back: series and rows out say {} \
             series and {} rows came back",
            self.scenario,
            self.inputs,
            self.outputs,
            counted.series.iter().sum::<usize>(),
            counted.rows.iter().sum::<usize>(),
        );
        assert_eq!(
            self.workload.per_output, counted.series,
            "scenario {} at {}x{} routed its series somewhere other than this bench predicted: the \
             routing this bench copies from the node no longer matches it",
            self.scenario, self.inputs, self.outputs,
        );
        assert_eq!(
            self.workload.rows_per_output, counted.rows,
            "scenario {} at {}x{} carried its rows on outputs other than this bench predicted",
            self.scenario, self.inputs, self.outputs,
        );
    }

    /// The cell's shape and the memory high-water mark of its gate drain, printed once.
    ///
    /// One line per cell, outside the timed section: the shape is what a throughput has to be read
    /// against, and the reservation is the pool's own bookkeeping rather than a duration.
    fn shape_line(&self, peak: usize) -> String {
        format!(
            "series_repartition scenario={} n={} m={} series={} chunks={} points={} \
             peak_pool_kib={:.1} \
             (peak pool reservation over the gate drain, outside the timed section; not a bench \
             number)",
            self.scenario,
            self.inputs,
            self.outputs,
            self.workload.series,
            self.workload.chunks,
            self.workload.rows,
            peak as f64 / 1024.0,
        )
    }
}

/// Prints one line of the matrix, uncaptured: a benchmark's only account of what it timed.
#[allow(clippy::print_stdout)]
fn report(line: &str) {
    println!("{line}");
}

/// The rows (points) of `scenario`'s workload.
///
/// Fixed by the scenario rather than by either partition count. It is what a cell reports as its
/// throughput, and it is known before the cell's workload is built, which is what lets a cell be built
/// inside its own benchmark closure - and so be skipped, workload and gate drain both, when criterion's
/// filter does not select it. The cell that is built asserts this equals its own row count, so the two
/// cannot drift apart into a throughput that describes a workload nobody timed.
fn rows_of(scenario: usize) -> usize {
    match scenario {
        1 | 3 => MANY_SERIES * SHORT_POINTS,
        2 => LONG_SERIES * LONG_CHUNKS * LONG_POINTS,
        other => panic!("no scenario {other}"),
    }
}

/// Times one execution of `SeriesRepartitionExec` per cell of the workload and partition matrix.
fn bench_series_repartition(c: &mut Criterion) {
    // One runtime for the whole matrix: it is the only worker count this benchmark picks, and the
    // node's input tasks do not run on it.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(CONSUMER_WORKERS)
        .enable_all()
        .build()
        .expect("a runtime for the consumer tasks");
    let mut group = c.benchmark_group("series_repartition");
    for scenario in SCENARIOS {
        group.throughput(Throughput::Elements(rows_of(scenario) as u64));
        for inputs in INPUTS {
            for outputs in OUTPUTS {
                let reported = std::sync::Once::new();
                group.bench_function(
                    BenchmarkId::new(
                        format!("scenario-{scenario}"),
                        format!("n{inputs}_m{outputs}"),
                    ),
                    |b| {
                        // Built here and not outside the closure so that criterion's filter reaches
                        // it: a run of one cell does not pay to build the other 35.
                        let cell = Cell::new(scenario, inputs, outputs);
                        assert_eq!(
                            rows_of(scenario),
                            cell.workload.rows,
                            "scenario {scenario}'s throughput has to be its workload's rows"
                        );
                        let pool = Arc::new(PeakPool::new(POOL_LIMIT));
                        let context = peak_context(Arc::clone(&pool) as Arc<dyn MemoryPool>);
                        let child = cell.plan();
                        // One drain before the timed ones: it gates the cell, and the global runtime
                        // the node's input tasks run on is built on first use, so the first drain of
                        // the matrix is not a measurement of that build.
                        cell.gate(&cell.drain(&runtime, &child, &context));
                        // Criterion calls this closure once per sample, so the line is said once and
                        // not once per sample: one shape line per cell.
                        reported.call_once(|| report(&cell.shape_line(pool.peak())));
                        b.iter(|| {
                            // The decoded chunks are dropped inside the timed section: dropping what
                            // a consumer was handed is work the execution pays.
                            black_box(cell.drain(&runtime, &child, &context).len());
                        });
                    },
                );
            }
        }
    }
    group.finish();
}

criterion_group!(benches, bench_series_repartition);
