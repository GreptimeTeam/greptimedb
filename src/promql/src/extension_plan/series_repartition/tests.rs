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
//! The child here is scripted rather than planned: each of its partitions answers a list of steps,
//! so a test says which series a chunk belongs to, where on the grid it sits and whether it closes
//! the series instead of relying on where a batch boundary happens to fall. The frames are packed and
//! unpacked by the same helpers the real path uses, so an assertion about an output stream is an
//! assertion about what a consumer of this node decodes.

use std::time::Duration;

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

/// The steps of one partition, as the frame stream that hands them over.
struct Script {
    steps: std::vec::IntoIter<Step>,
}

impl Stream for Script {
    type Item = Result<(Series, bool), BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.steps.next() {
            None => Poll::Ready(None),
            Some(Step::Chunk(series, is_last)) => Poll::Ready(Some(Ok((series, is_last)))),
            Some(Step::Error) => Poll::Ready(Some(Err(boxed(DataFusionError::Execution(
                "scripted child failure".to_string(),
            ))))),
        }
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
