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

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Waker;
use std::time::Duration;

use datafusion::arrow::array::Float64Array;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool, UnboundedMemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::prelude::SessionContext;
use datatypes::series::Label;
use datatypes::series::samples::{FieldSpec, Samples, SeriesFields};
use datatypes::series::time::{EvalGrid, GridSpan, GridSpec, TimeAxis};
use futures::TryStreamExt;

use super::*;
use crate::extension_plan::test_util::prepare_test_data;
use crate::grid_series::render_label_set;

/// The grid every frame of this module is addressed on: a value per second, sixteen of them.
const GRID_START: i64 = 0;
const GRID_STEP: i64 = 1_000;
const GRID_LEN: u64 = 16;

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

/// Every output partition of `plan`, decoded, and the error it ended with if it ended with one.
///
/// All partitions are read at once, which is what an executor does: this node suspends the producer
/// on an output that is not being drained, so reading one partition to its end before starting the
/// next would test the backpressure rather than the routing.
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

/// The summaries of an output, in an order the input tasks do not decide.
///
/// Every input is read by a task of its own, so which of two inputs delivers a chain first is not
/// fixed - and no test may depend on it. What a test still states is that each chain arrives whole and
/// in order ([`assert_chains_are_whole`]), and that the set of chunks is exactly the set it expects;
/// this is the comparison for a chunk list that came from more than one source.
fn unordered(
    summaries: &[(String, Option<usize>, usize, bool)],
) -> Vec<(String, Option<usize>, usize, bool)> {
    let mut sorted = summaries.to_vec();
    sorted.sort();
    sorted
}

/// Asserts that every chain `chunks` hands over is whole.
///
/// A chunk that leaves a series open must be followed by the same series' next chunk: another
/// series' chunk in between is what a merge downstream cannot repair, since it would have to hold
/// two open series to read either. Ending with a chain open fails too, because that means the
/// finality of a chunk was lost on the way.
fn assert_chains_are_whole(chunks: &[(Series, bool)]) {
    let mut open: Option<String> = None;
    for (series, is_last) in chunks {
        if let Some(open) = &open {
            assert_eq!(
                open,
                &named(&series.metric),
                "another series' chunk interrupted an open chain"
            );
        }
        open = (!is_last).then(|| named(&series.metric));
    }
    assert!(open.is_none(), "the output ended with a chain open");
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

    let piece = named(&shared);
    let other_piece = named(&other);
    let shared_output = routing.output(&shared);
    assert_eq!(
        unordered(&[
            (piece.clone(), Some(0), 2, false),
            (piece.clone(), None, 0, true),
            (piece.clone(), Some(8), 2, true),
        ]),
        unordered(&outputs[shared_output]),
        "both pieces of one label set, and the closing chunk between them, land on one output"
    );
    assert_eq!(
        vec![(other_piece, Some(0), 1, true)],
        outputs[routing.output(&other)],
        "another label set lands where its own labels hash to"
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
            Step::chunk(chunk(shared.clone(), 8, 2, false)),
            Step::chunk(chunk(shared.clone(), 0, 0, true)),
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

    let piece = named(&shared);
    assert_eq!(
        unordered(&[
            (piece.clone(), Some(0), 2, false),
            (piece.clone(), Some(2), 2, true),
            (piece.clone(), Some(8), 2, false),
            // The closing chunk of the second chain carries no position of its own.
            (piece, None, 0, true),
        ]),
        unordered(&summaries(&chunks)),
        "both chains reach the one output whole, and the empty closing chunk is still a chunk"
    );
}

/// T3, first half: an output nobody reads suspends the input task once its queue is full, and the
/// chains it already routed are neither lost nor delivered twice once that output is drained.
///
/// What happens, explicitly: the input's task queues [`OUTPUT_CAPACITY`] chains to the stalled output
/// and then waits on that output's send. Every other output stops receiving from that input at that
/// point - one task reads this one input, and it does not run ahead of the slowest output - so an
/// input has head-of-line blocking behind its own chains, and no scheduler to route around it. What it
/// does not have is a share of that wait with another input: each one waits on its own. It is not a
/// deadlock: the task is suspended on a channel, the rest of the query continues, and draining the
/// stalled output lets the read go on.
#[tokio::test]
async fn a_stalled_output_suspends_the_producer_without_losing_chunks() {
    let routing = Routing::new(2);
    let shared = labels("web-1");
    let other = labels_elsewhere(&routing, &shared);
    let stalled = routing.output(&shared);
    let moving = routing.output(&other);
    // More chains than one queue can hold, so the producer has to stop mid-read rather than finish.
    let chains = OUTPUT_CAPACITY + 2;
    let mut steps = (0..chains)
        .map(|index| Step::chunk(chunk(shared.clone(), index, 1, true)))
        .collect::<Vec<_>>();
    steps.push(Step::chunk(chunk(other.clone(), 0, 1, true)));
    let plan: Arc<dyn ExecutionPlan> = node(Scripted::plan(vec![steps]), 2);
    let context = SessionContext::default().task_ctx();

    let mut moving_stream = output(&plan, moving, &context);
    let stalled_stream = output(&plan, stalled, &context);
    // The other output is read first, and the read is already blocked on the stalled one: nothing it
    // would have been given can arrive, which is the head-of-line blocking this test states.
    assert!(
        tokio::time::timeout(Duration::from_millis(200), moving_stream.next())
            .await
            .is_err(),
        "the stalled output did not hold the producer back"
    );

    // Draining the stalled output lets the read finish, and it loses nothing on the way.
    let stalled_chunks = tokio::time::timeout(
        Duration::from_secs(10),
        stalled_stream.try_collect::<Vec<_>>(),
    )
    .await
    .expect("draining the stalled output releases the producer")
    .expect("the stalled output decodes");
    assert_eq!(chains, stalled_chunks.len());
    assert_chains_are_whole(&stalled_chunks);
    for (index, (series, is_last)) in stalled_chunks.iter().enumerate() {
        assert_eq!(named(&shared), named(&series.metric));
        assert_eq!(Some(index), series.time.first_position(), "in read order");
        assert!(is_last, "every chain of this test closes");
    }

    let mut moving_chunks = Vec::new();
    while let Some(chunk) = tokio::time::timeout(Duration::from_secs(10), moving_stream.next())
        .await
        .expect("the other output still receives what was routed to it")
    {
        moving_chunks.push(chunk.expect("the other output decodes"));
    }
    assert_eq!(
        vec![(named(&other), Some(0), 1, true)],
        summaries(&moving_chunks),
        "the chunk queued behind the stalled output arrives once it is unstuck, exactly once"
    );
}

/// T3, second half: what the producer queues is charged to the execution's pool, and dropping the
/// execution releases it.
#[tokio::test]
async fn dropping_the_execution_releases_what_the_producer_reserved() {
    let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
    let context = task_context(&pool);
    let shared = labels("web-1");
    let child = Scripted::plan(vec![vec![
        Step::chunk(chunk(shared.clone(), 0, 1, true)),
        Step::chunk(chunk(shared.clone(), 1, 1, true)),
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
    assert!(held >= 3 * std::mem::size_of::<Series>(), "{held}");

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

/// T4: a child that fails leaves every output with a terminal error rather than a clean end.
#[tokio::test]
async fn a_child_failure_reaches_every_output_as_an_error() {
    let shared = labels("web-1");
    let other = labels("elsewhere");
    let child = Scripted::plan(vec![
        vec![Step::chunk(chunk(shared.clone(), 0, 2, false)), Step::Error],
        vec![Step::chunk(chunk(other.clone(), 0, 1, true)), Step::Error],
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
    // The read rotates at every chain boundary, not at every input end: the first chain closes on
    // partition 0, so the next chain read is partition 1's, and partition 0's remaining chain comes
    // after it. Both label sets still reach the one output, whole.
    assert_eq!(
        unordered(&[
            (named(&shared), Some(0), 2, false),
            (named(&shared), Some(2), 2, true),
            (named(&shared), Some(8), 2, true),
            (named(&other), Some(0), 1, false),
            (named(&other), None, 0, true),
        ]),
        unordered(&summaries(&chunks)),
        "every chunk of both label sets arrives, each chain in one piece"
    );
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
#[derive(Clone, Default)]
struct GatedPartition {
    before: Vec<Step>,
    after: Vec<Step>,
}

impl GatedPartition {
    /// A partition that hands over `steps` only once the switch is opened.
    fn held(steps: Vec<Step>) -> Self {
        Self {
            before: Vec::new(),
            after: steps,
        }
    }

    /// A partition that hands over `steps` as soon as it is polled.
    fn open(steps: Vec<Step>) -> Self {
        Self {
            before: steps,
            after: Vec::new(),
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
        pack_evaluated_series(
            Box::pin(GatedScript {
                before: steps.before.into_iter(),
                after: steps.after.into_iter(),
                switch: Arc::clone(&self.switch),
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

/// T-A1: the read drives each input partition on a task of its own.
///
/// The child records the task that first polls each of its partitions, and what is asserted is that
/// the two partitions were polled by two distinct tasks, neither of them the consumer's. That is the
/// property `Forwarders::start` provides - one task per child partition - and it is what a partition
/// count on its own cannot say: one task polling every partition in turn declares the same count.
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
    assert_eq!(
        vec![
            (named(&shared), Some(0), 1, true),
            (named(&shared), Some(4), 1, true),
        ],
        summaries(&chunks),
        "both partitions' chunks arrive, each from the task that read it"
    );
}

/// T-B1: the streams own the read, so dropping the plan does not truncate them.
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

/// T-B2: when every output is dropped the read stops, even though the plan is still alive.
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
    let steps = (0..OUTPUT_CAPACITY * 3)
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

/// T-B3: a child failure is a terminal item on every output, and the end comes after it.
///
/// The second half matters as much as the first: an output that reports the failure and then answers
/// again would be a stream whose error was not terminal, and one that answers with a chunk before the
/// error is already covered by the contents. What must never happen is a clean end in place of the
/// failure, so the error is required on every output.
#[tokio::test]
async fn a_child_failure_is_terminal_on_every_output() {
    let shared = labels("web-1");
    let child = Scripted::plan(vec![
        vec![Step::chunk(chunk(shared.clone(), 0, 2, false)), Step::Error],
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

/// T-B4: a read that stops without finishing is not reported as the end of the answer.
///
/// The read is aborted through the tasks it runs on, which are reachable from this module because the
/// tests are a child of the module that defines it - so this needs no hook in the production code, and
/// the stop is the one the read stops itself with. The child hands over one chunk and then waits on a
/// switch nothing opens, so the read cannot have finished and recorded an outcome when it is aborted:
/// what an output sees after that is a queue closing on a read with no recorded ending, which must be
/// reported as the read stopping unexpectedly rather than as the end of the series.
#[tokio::test]
async fn an_aborted_producer_is_not_reported_as_a_clean_end() {
    let switch = Arc::new(Switch::default());
    let dropped = Arc::new(AtomicUsize::new(0));
    let shared = labels("web-1");
    let child = Gated::plan(
        vec![GatedPartition {
            before: vec![Step::chunk(chunk(shared.clone(), 0, 1, true))],
            after: vec![Step::chunk(chunk(shared.clone(), 1, 1, true))],
        }],
        switch,
        Arc::clone(&dropped),
    );
    let plan = node(child, 1);
    let context = SessionContext::default().task_ctx();
    let mut stream = unpack_evaluated_series(
        plan.execute(0, Arc::clone(&context))
            .expect("the output executes"),
        None,
    )
    .expect("the output declares an evaluated frame");

    let first = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .expect("the read starts")
        .expect("a chunk arrives")
        .expect("the chunk decodes");
    assert_eq!(
        (named(&shared), Some(0), 1, true),
        summary(&first),
        "the chunk the child handed over before the gate"
    );

    let execution = execution(&plan);
    // Aborted directly, while its outputs are still being read: what an output does after that is
    // the whole point of this test. This aborts every input task and records nothing, which is what a
    // read stopped from outside looks like.
    execution.running.lock().unwrap().inputs.stop();

    let tail = tokio::time::timeout(Duration::from_secs(10), stream.try_collect::<Vec<_>>())
        .await
        .expect("an aborted read still ends the outputs it left behind");
    let error = tail.expect_err(
        "the aborted read reported a clean end, so the chunks it never routed look like chunks that \
         do not exist",
    );
    assert!(
        error.to_string().contains("producer stopped unexpectedly"),
        "the output ended with {error}"
    );
}

/// T-C1: an output that ends takes nothing else down with it when another output was never executed.
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

/// T-C2: the output claimed last still receives its whole share after a claimed sibling ended.
///
/// Both outputs' shares fit one queue, so the read ends on its own instead of suspending on anything:
/// what is asserted is what a late claim gets, not the backpressure. Partition 0 is read to its end
/// before partition 1 is even executed, which releases partition 0's lease; a read that treated that
/// as the end would drop partition 1's queue and the chunks queued there - the answer would be
/// truncated rather than late, and nothing on partition 1 would arrive at all.
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
    // One chain per label set, in a row, both of them closed: what reaches each output is a run of
    // complete chains, so nothing here depends on how the read interleaves two open ones.
    let chains = OUTPUT_CAPACITY - 1;
    let steps = (0..chains)
        .map(|index| Step::chunk(chunk(shared.clone(), index, 1, true)))
        .chain((0..chains).map(|index| Step::chunk(chunk(other.clone(), index, 1, true))))
        .collect::<Vec<_>>();
    let plan: Arc<dyn ExecutionPlan> = node(Scripted::plan(vec![steps]), 2);
    let context = SessionContext::default().task_ctx();

    let first = output(&plan, ended_first, &context);
    let first_chunks = tokio::time::timeout(Duration::from_secs(10), first.try_collect::<Vec<_>>())
        .await
        .expect("the first output reaches its end before the other is executed")
        .expect("the first output ends without a failure");
    assert_eq!(
        (0..chains)
            .map(|index| (named(&shared), Some(index), 1, true))
            .collect::<Vec<_>>(),
        summaries(&first_chunks),
        "the output executed first hands over every chain of its label set"
    );

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
    assert_eq!(
        (0..chains)
            .map(|index| (named(&other), Some(index), 1, true))
            .collect::<Vec<_>>(),
        summaries(&second_chunks),
        "the output executed last still receives every chain of its label set, in order"
    );
}

/// T-C3: an output nobody claimed keeps the read running, not merely claimable.
///
/// More chains than one queue holds are routed to the output nobody has asked for, so the read cannot
/// finish: it is parked on that output's queue. The claimed output delivered everything routed to it
/// and is then dropped, which gives up every lease the read has - and that must not be taken for
/// "nothing can take a chunk any more", because the output that has never been claimed is exactly the
/// one that will. A read that stopped here would abort a producer that is correctly waiting, and the
/// chunks it holds would be lost; the last part of the test is that it is not merely alive but that
/// every chunk it queued arrives once the output is claimed.
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
    let chains = OUTPUT_CAPACITY + 2;
    let steps = std::iter::once(Step::chunk(chunk(shared.clone(), 0, 1, true)))
        .chain((0..chains).map(|index| Step::chunk(chunk(other.clone(), index, 1, true))))
        .collect::<Vec<_>>();
    let plan = node(Scripted::plan(vec![steps]), 2);
    let context = SessionContext::default().task_ctx();
    let mut first = unpack_evaluated_series(
        plan.execute(read_first, Arc::clone(&context))
            .expect("the output that is read first executes"),
        None,
    )
    .expect("the output declares an evaluated frame");
    let execution = execution(&plan);

    let chunk_read = tokio::time::timeout(Duration::from_secs(10), first.next())
        .await
        .expect("the first chunk of the read arrives")
        .expect("the output hands over a chunk")
        .expect("the chunk decodes");
    assert_eq!(
        (named(&shared), Some(0), 1, true),
        summary(&chunk_read),
        "the chain routed to the output that is read first"
    );
    assert!(
        !producer_stopped(&execution),
        "the read finished although nothing could have delivered the chains it never routed"
    );

    // Everything routed to this output was handed over, so the output is done being read - and the
    // only output that was ever claimed is gone with it.
    drop(first);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !producer_stopped(&execution),
        "the producer was aborted although one of its outputs had never been claimed: the chunks \
         queued for that output are what a later execute still comes for"
    );

    // And the read was not merely alive: claiming the output nobody had asked for gets all of it.
    let second = unpack_evaluated_series(
        plan.execute(claimed_later, Arc::clone(&context))
            .unwrap_or_else(|error| panic!("the unclaimed output is claimable: {error}")),
        None,
    )
    .expect("the unclaimed output declares an evaluated frame");
    let second_chunks =
        tokio::time::timeout(Duration::from_secs(10), second.try_collect::<Vec<_>>())
            .await
            .expect("claiming the output unblocks the read it was parked on")
            .expect("the output executed last ends without a failure");
    assert_eq!(
        (0..chains)
            .map(|index| (named(&other), Some(index), 1, true))
            .collect::<Vec<_>>(),
        summaries(&second_chunks),
        "every chain the parked read had queued arrives once the output is claimed"
    );
    wait_until("the read that was parked finishes", || {
        producer_stopped(&execution)
    })
    .await;
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

/// T-D1: two of these nodes composed do not deadlock while every output of the outer one is drained.
///
/// The child produces `SERIES` single-point chains, each with a label set of its own, every one of
/// them hashing to output 1 of a two-output node - so the inner node's output 0 is empty and output 1
/// carries all of them. Composed as `outer(inner(child))`, the outer node's input 0 is the inner
/// node's empty output, and what the read does with that is what this test states.
///
/// The volume is chosen against the buffers in the path, not at random. Each output holds
/// [`OUTPUT_CAPACITY`] chains and each input task holds the one chain it is assembling, so the whole
/// composition absorbs a handful of chunks: measured against the router this test was written for,
/// ten was what that path took and eleven already stopped. `SERIES` is two orders of magnitude above
/// that, so no amount of buffering makes this test pass by swallowing the child whole, and a read that
/// is merely slow is not what a failure here would be.
///
/// Both outputs of the outer node are read at once and each is drained to its end, so nothing here
/// can be blamed on an output nobody drained - which is the one requirement this node states of its
/// caller. The count that comes out is asserted against the count that went in, so a passing run is a
/// run that lost nothing.
///
/// What it caught is why the router is per input now: with one task routing every input, the outer
/// read parked on its input 0 - the inner node's empty output, which does not close until the inner
/// read finishes - so it routed nothing at all and both outputs stayed empty, while the inner read
/// filled the outer node's input 1 after about ten chunks and parked on its output 1. Measured: 0 of
/// 4096 series delivered, both outputs empty, timeout at 10 s. Giving the outer node's input 0 a
/// single series of its own changed nothing, which is what says the wait was the problem rather than
/// the volume; giving output 0 thousands of series of its own instead finished the same composition in
/// under a second. With one task per input there is no such wait: the outer node's task 0 is parked on
/// the inner node's empty output by itself, and its task 1 keeps routing everything the inner node
/// sends on output 1, which is what lets the inner node finish and close output 0.
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

/// T-E1: two sources whose chains belong on one output land there whole, never inside one another.
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
                before: vec![Step::chunk(chunk(held.clone(), 0, 2, false))],
                after: vec![Step::chunk(chunk(held.clone(), 2, 2, true))],
            },
            // The other source's whole chain, handed over as soon as it is polled.
            GatedPartition::open(vec![
                Step::chunk(chunk(closed.clone(), 0, 2, false)),
                Step::chunk(chunk(closed.clone(), 2, 2, true)),
            ]),
        ],
        Arc::clone(&switch),
        Arc::new(AtomicUsize::new(0)),
    );
    let plan: Arc<dyn ExecutionPlan> = node(child, 2);
    let context = SessionContext::default().task_ctx();
    let mut routed = output(&plan, 0, &context);
    let other = output(&plan, 1, &context);

    // The chain of the source that closed arrives while the other source is still open - and in one
    // piece, since a partial chain is never put on an output in the first place.
    let mut chunks = Vec::new();
    while chunks.len() < 2 {
        let chunk = tokio::time::timeout(Duration::from_secs(10), routed.next())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "the closed chain of one source did not arrive while the other source's chain \
                     was still open: {} chunks arrived",
                    chunks.len()
                )
            })
            .expect("the output is not over while a source is still reading")
            .expect("the chunk decodes");
        chunks.push(chunk);
    }
    assert_chains_are_whole(&chunks);
    assert_eq!(
        vec![
            (named(&closed), Some(0), 2, false),
            (named(&closed), Some(2), 2, true),
        ],
        summaries(&chunks),
        "the source whose chain closed is on the output whole, before the open chain's first chunk"
    );

    // Opening the gate closes the held chain: it follows on the same output, and everything the
    // output carried over the test is a run of whole chains.
    switch.open();
    chunks.extend(
        tokio::time::timeout(Duration::from_secs(10), routed.try_collect::<Vec<_>>())
            .await
            .expect("the held chain arrives once the gate opens")
            .expect("the output decodes"),
    );
    assert_chains_are_whole(&chunks);
    assert_eq!(
        vec![
            (named(&closed), Some(0), 2, false),
            (named(&closed), Some(2), 2, true),
            (named(&held), Some(0), 2, false),
            (named(&held), Some(2), 2, true),
        ],
        summaries(&chunks),
        "both sources' chains are on one output, each of them in one piece"
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

/// T-E2: a closing chunk that carries no rows is delivered, and is not mistaken for the end of the
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
            Step::chunk(chunk(other.clone(), 0, 1, true)),
        ],
        // The other source hands over a chain with positions, and closes it with an empty chunk.
        vec![
            Step::chunk(chunk(shared.clone(), 0, 0, true)),
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

    assert_eq!(
        vec![
            // The chunk with no rows of its own, then the chunk of the stream that is still going.
            (named(&shared), None, 0, true),
            (named(&shared), Some(4), 2, false),
            (named(&shared), None, 0, true),
        ],
        summaries(&outputs[routing.output(&shared)]),
        "an empty closing chunk is handed over, and the chunk after it is not swallowed"
    );
    assert_eq!(
        vec![
            (named(&other), None, 0, true),
            (named(&other), Some(0), 1, true),
        ],
        summaries(&outputs[routing.output(&other)]),
        "the same holds for a source whose whole first chain is one empty closing chunk"
    );
}

/// T-E3: while one owner's chain is still open, the other owner's output still delivers.
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
            // A long chain, gated after its first chunk.
            GatedPartition {
                before: vec![Step::chunk(chunk(held.clone(), 0, 2, false))],
                after: vec![
                    Step::chunk(chunk(held.clone(), 2, 2, false)),
                    Step::chunk(chunk(held.clone(), 4, 2, false)),
                    Step::chunk(chunk(held.clone(), 6, 2, true)),
                ],
            },
            // A chain of its own, handed over as soon as it is polled.
            GatedPartition::open(vec![
                Step::chunk(chunk(free.clone(), 0, 2, false)),
                Step::chunk(chunk(free.clone(), 2, 2, true)),
            ]),
        ],
        Arc::clone(&switch),
        Arc::new(AtomicUsize::new(0)),
    );
    let plan: Arc<dyn ExecutionPlan> = node(child, 2);
    let context = SessionContext::default().task_ctx();
    let mut held_stream = output(&plan, held_output, &context);
    let mut free_stream = output(&plan, free_output, &context);

    // The other owner's output delivers while the gated chain is still open, and what it delivers is
    // its whole chain.
    let mut delivered = Vec::new();
    while delivered.len() < 2 {
        let chunk = tokio::time::timeout(Duration::from_secs(10), free_stream.next())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "the other owner's output delivered {} of its 2 chunks while one chain was open",
                    delivered.len()
                )
            })
            .expect("the other output is not over")
            .expect("the chunk decodes");
        delivered.push(chunk);
    }
    assert_chains_are_whole(&delivered);
    assert_eq!(
        vec![
            (named(&free), Some(0), 2, false),
            (named(&free), Some(2), 2, true),
        ],
        summaries(&delivered),
        "the free chain is delivered in full while the other owner's chain is still open"
    );

    // The open chain is still waiting for its own source, so nothing of it has been delivered: a
    // partial chain does not sit on an output waiting to be completed.
    assert!(
        tokio::time::timeout(Duration::from_millis(200), held_stream.next())
            .await
            .is_err(),
        "the held chain was delivered while its source was still open"
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
    let rest = tokio::time::timeout(Duration::from_secs(10), free_stream.try_collect::<Vec<_>>())
        .await
        .expect("the other output ends")
        .expect("the other output decodes");
    assert!(
        rest.is_empty(),
        "the other output carried its whole chain already"
    );
}

/// T-E4: a partition that fails mid-stream ends every output with that error, none of them cleanly.
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

/// T-E5: with two input partitions, each is read on a task of its own, and neither on the consumer's.
///
/// The mirror of [`every_input_partition_is_read_on_a_task_of_its_own`] over a node with two outputs -
/// the shape the per-input router is for, where each partition's chains are routed by the task that
/// read them. What the child records is the task that first polled each of its partitions, so one id
/// for both, or the consumer's own, is the shape a node with a reader shared by all its inputs has.
#[tokio::test]
async fn both_input_partitions_are_read_on_tasks_of_their_own_and_route_their_own_chunks() {
    let routing = Routing::new(2);
    let shared = labels("web-1");
    let other = labels_elsewhere(&routing, &shared);
    let (child, polled) = Recording::plan(vec![
        vec![Step::chunk(chunk(shared.clone(), 0, 1, true))],
        vec![Step::chunk(chunk(other.clone(), 4, 1, true))],
    ]);
    let plan: Arc<dyn ExecutionPlan> = node(child, 2);
    // Taken before anything is read: the task that asks for an output is the one a partition must
    // not be read on.
    let consumer = tokio::task::try_id();
    let outputs = tokio::time::timeout(
        Duration::from_secs(10),
        drain(&plan, &SessionContext::default().task_ctx()),
    )
    .await
    .expect("both partitions of the child answer, so every output ends");

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
        "the two partitions were polled by two different tasks, so each input was read - and routed - \
         on a task of its own: {tasks:?}"
    );
    assert!(
        tasks.iter().all(|task| Some(*task) != consumer),
        "neither partition ran on the consumer's task: {tasks:?} against {consumer:?}"
    );

    assert_eq!(
        vec![(named(&shared), Some(0), 1, true)],
        summaries(
            outputs[routing.output(&shared)]
                .as_ref()
                .expect("the output of the first label set decodes")
        ),
        "the first input's chain lands on the output its labels hash to"
    );
    assert_eq!(
        vec![(named(&other), Some(4), 1, true)],
        summaries(
            outputs[routing.output(&other)]
                .as_ref()
                .expect("the output of the second label set decodes")
        ),
        "and the second input's chain on its own"
    );
}

/// T-E6: two inputs make their own routing decisions, and neither waits for the other to make one.
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
/// This is the gate T-E6 is built on: progress through one partition's chain requires the other
/// partition to have progressed too, which is only possible if both are read at the same time. It is a
/// rendezvous between inputs, not a stopwatch: nothing about it depends on two threads running at
/// once.
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
