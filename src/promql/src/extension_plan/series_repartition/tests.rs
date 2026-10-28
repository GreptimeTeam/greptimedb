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
//! Two properties need an input that behaves in time rather than only in sequence, so they are tested
//! with children that hold: a partition that will not answer until another one has been polled, and
//! partitions that stay silent until the test opens a [`Switch`]. The first is how the read's
//! partition parallelism is checked - a read that drives every partition from one task cannot make
//! progress past it - and the second is how the streams' ownership of the read is checked, by making
//! the input produce nothing before the plan is dropped.
//!
//! The child here is scripted rather than planned: each of its partitions answers a list of steps,
//! so a test says which series a chunk belongs to, where on the grid it sits and whether it closes
//! the series instead of relying on where a batch boundary happens to fall. The frames are packed and
//! unpacked by the same helpers the real path uses, so an assertion about an output stream is an
//! assertion about what a consumer of this node decodes.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Waker;
use std::time::{Duration, Instant};

use datafusion::arrow::array::Float64Array;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
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
        vec![
            (piece.clone(), Some(0), 2, false),
            (piece.clone(), None, 0, true),
            (piece.clone(), Some(8), 2, true),
        ],
        outputs[shared_output],
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
        vec![
            (piece.clone(), Some(0), 2, false),
            (piece.clone(), Some(2), 2, true),
            (piece.clone(), Some(8), 2, false),
            // The closing chunk of the second chain carries no position of its own.
            (piece, None, 0, true),
        ],
        summaries(&chunks),
        "the first chain is handed over before the second one starts, and the empty closing chunk \
         is still a chunk"
    );
}

/// T3, first half: an output nobody reads suspends the producer once its queue is full, and the
/// chunks it already read are neither lost nor delivered twice once that output is drained.
///
/// What happens, explicitly: the producer queues [`OUTPUT_CAPACITY`] chunks to the stalled output and
/// then waits on that output's send. Every other output stops receiving at that point - the read is
/// one task, and it does not run ahead of the slowest output - so this node has head-of-line
/// blocking by design and no scheduler to route around it. It is not a deadlock: the task is
/// suspended on a channel, the rest of the query continues, and draining the stalled output lets the
/// read go on.
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
        vec![
            (named(&shared), Some(0), 2, false),
            (named(&shared), Some(2), 2, true),
            (named(&shared), Some(8), 2, true),
            (named(&other), Some(0), 1, false),
            (named(&other), None, 0, true),
        ],
        summaries(&chunks),
        "every chunk of both label sets arrives, in the order the read reached them"
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

/// Where a [`Blocking`] child's partitions were polled, and what that showed about the read.
#[derive(Clone)]
struct BlockingProbe {
    /// How many times each partition's stream was polled.
    polls: Arc<Vec<AtomicUsize>>,
    /// How many partitions are inside `poll_next` right now; more than one is two tasks reading at
    /// once, since one task polls one partition at a time.
    inside: Arc<AtomicUsize>,
    /// Whether two partitions were ever in flight at the same time.
    overlapped: Arc<AtomicBool>,
    /// Whether partition 0 waited for another partition and never saw one polled.
    timed_out: Arc<AtomicBool>,
}

impl BlockingProbe {
    fn new(partitions: usize) -> Self {
        Self {
            polls: Arc::new((0..partitions).map(|_| AtomicUsize::new(0)).collect()),
            inside: Arc::new(AtomicUsize::new(0)),
            overlapped: Arc::new(AtomicBool::new(false)),
            timed_out: Arc::new(AtomicBool::new(false)),
        }
    }

    fn polls(&self, partition: usize) -> usize {
        self.polls[partition].load(Ordering::SeqCst)
    }
}

/// How long partition 0 of a [`Blocking`] child waits before giving up on being read in parallel.
const GATE: Duration = Duration::from_secs(5);

/// A child whose partition 0 will not answer until another partition has been polled.
///
/// A poll in flight is invisible to a task that never gets to run, so the gate is a real one:
/// partition 0 stays inside `poll_next` - holding its thread - until another partition's `poll_next`
/// has run, which a read that drives every partition from one task can never arrange.
struct Blocking {
    steps: Vec<Vec<Step>>,
    probe: BlockingProbe,
    series_schema: Arc<SeriesSchema>,
    properties: Arc<PlanProperties>,
}

impl Blocking {
    /// The child over `steps`, one partition per list, and the record of how it was polled.
    fn plan(steps: Vec<Vec<Step>>) -> (Arc<dyn ExecutionPlan>, BlockingProbe) {
        let probe = BlockingProbe::new(steps.len());
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
            probe: probe.clone(),
            series_schema,
            properties,
        });
        (child, probe)
    }
}

impl fmt::Debug for Blocking {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Blocking")
            .field("partitions", &self.steps.len())
            .finish()
    }
}

impl DisplayAs for Blocking {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "Blocking: partitions={}", self.steps.len())
    }
}

impl ExecutionPlan for Blocking {
    fn name(&self) -> &str {
        "Blocking"
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
        assert!(children.is_empty(), "a blocking child has no children");
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let steps = self.steps.get(partition).cloned().ok_or_else(|| {
            DataFusionError::Execution(format!("blocking child has no partition {partition}"))
        })?;
        pack_evaluated_series(
            Box::pin(BlockingScript {
                partition,
                steps: steps.into_iter(),
                probe: self.probe.clone(),
                waited: false,
            }),
            Arc::clone(&self.series_schema),
        )
    }
}

/// One partition of a [`Blocking`] child.
struct BlockingScript {
    partition: usize,
    steps: std::vec::IntoIter<Step>,
    probe: BlockingProbe,
    /// Whether this partition already waited at the gate.
    waited: bool,
}

impl Stream for BlockingScript {
    type Item = Result<(Series, bool), BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        // Counted before the gate, so a partition that is inside its poll is visible as such to any
        // other partition polling at the same time on another task.
        let inside = this.probe.inside.fetch_add(1, Ordering::SeqCst) + 1;
        if this.partition > 0 && inside > 1 {
            this.probe.overlapped.store(true, Ordering::SeqCst);
        }
        if this.partition == 0 && !this.waited {
            this.waited = true;
            let deadline = Instant::now() + GATE;
            while !(1..this.probe.polls.len()).any(|partition| this.probe.polls(partition) > 0) {
                if Instant::now() >= deadline {
                    this.probe.timed_out.store(true, Ordering::SeqCst);
                    break;
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        this.probe.polls[this.partition].fetch_add(1, Ordering::SeqCst);
        this.probe.inside.fetch_sub(1, Ordering::SeqCst);
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

/// Whether the task routing the child has stopped.
fn producer_stopped(execution: &SharedExecution) -> bool {
    execution.running.lock().unwrap().producer.is_finished()
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
/// What is asserted, and why it is the property rather than a hint of it: partition 0 of the child
/// does not answer until another partition has been polled, and it waits by holding its thread
/// inside `poll_next`, not by returning `Poll::Pending`. A single task polls one future at a time, so
/// no arrangement of awaits inside one task can get past that gate - it only opens if partition 1's
/// `poll_next` really ran while partition 0 was still inside its own, which is two tasks on two
/// threads. `overlapped` records that the child saw exactly that, and `timed_out` records a gate that
/// was never opened, so a read that went back to polling everything from one task fails here instead
/// of merely being slow.
#[tokio::test]
async fn every_input_partition_is_read_on_a_task_of_its_own() {
    let shared = labels("web-1");
    let (child, probe) = Blocking::plan(vec![
        vec![Step::chunk(chunk(shared.clone(), 0, 1, true))],
        vec![Step::chunk(chunk(shared.clone(), 4, 1, true))],
    ]);
    let plan: Arc<dyn ExecutionPlan> = node(child, 1);
    let outputs = tokio::time::timeout(
        Duration::from_secs(10),
        drain(&plan, &SessionContext::default().task_ctx()),
    )
    .await
    .expect("a read that drives each partition separately gets past the gate");

    assert!(
        !probe.timed_out.load(Ordering::SeqCst),
        "partition 0 waited for another partition to be polled and none was: the read drove the \
         partitions one after another on one task"
    );
    assert!(
        probe.overlapped.load(Ordering::SeqCst),
        "two partitions were never polled at the same time"
    );
    // At least once each: the frame codec in front of the child polls one more time to see the end.
    assert!(probe.polls(0) >= 1, "partition 0 was never polled");
    assert!(probe.polls(1) >= 1, "partition 1 was never polled");

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
        "both partitions' chunks arrive, so the gate opened on real concurrency"
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
/// The read is aborted through its own task handle, which is reachable from this module because the
/// tests are a child of the module that defines it - so this needs no hook in the production code. The
/// child hands over one chunk and then waits on a switch nothing opens, so the read cannot have
/// finished and recorded an outcome when it is aborted: what an output sees after that is a queue
/// closing on a read with no recorded ending, which must be reported as the read stopping
/// unexpectedly rather than as the end of the series.
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
    // the whole point of this test.
    execution.running.lock().unwrap().producer.abort();

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
