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

//! What the runtime has to get right: each layer's grid, per-node accounting, rejection and
//! release.

use std::collections::VecDeque;
use std::sync::Arc;

use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricValue};
use datatypes::arrow::array::{Float64Array, UInt32Array};
use datatypes::arrow::buffer::{Buffer, ScalarBuffer};
use datatypes::series::histogram::{
    BucketSideView, CounterResetHint, HistogramBatch, HistogramBatchBuilder, HistogramView,
};
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{
    FieldSpec, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{EvalGrid, GridRange, GridSpan, GridSpec, TimeAxis};
use datatypes::series::{Label, MetricName, SeriesOrdering};

use super::{
    ChargedAllocations, Emitted, History, NativeChunk, NativeExecutionState, NativeExecutor,
    NodeBuffer, NodeRuntime, QueuedCharge, Run, label_bytes, map_chunk, raw_bytes,
};
use crate::evaluated::EvaluatedChunk;
use crate::grid_series::{LabelSet, label_set};
use crate::map::MathOp;
use crate::native::metrics::{NodeMetric, NodeMetrics, NodePath};
use crate::native::plan::{
    AggregateKind, AggregateStage, BoundAggregate, BoundInput, BoundInstant, BoundMap, BoundScalar,
    BoundWindow, Grouping, NativeExpr, NativeNode, NativeTree, NativeType,
};
use crate::range_eval::{RangeEvaluator, RangeFunction, RangeWindow};
use crate::vector_match::{BinaryMatching, BinaryOp, Cardinality, MatchingLabels};

fn grid(start: i64, step: i64, len: u64) -> Arc<EvalGrid> {
    Arc::new(EvalGrid::try_new(GridSpec::try_new(start, step, len).unwrap()).unwrap())
}

fn fields() -> Arc<[FieldSpec]> {
    field_count(1)
}

/// `count` float fields named `v0`, `v1`, ...
fn field_count(count: usize) -> Arc<[FieldSpec]> {
    (0..count)
        .map(|index| FieldSpec {
            name: format!("v{index}").into(),
        })
        .collect()
}

fn raw_schema() -> Arc<SeriesSchema> {
    raw_schema_of(fields())
}

fn raw_schema_of(fields: Arc<[FieldSpec]>) -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields,
        eval_grid: None,
    })
}

fn evaluated_schema(grid: Arc<EvalGrid>) -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: fields(),
        eval_grid: Some(grid),
    })
}

fn evaluated(grid: Arc<EvalGrid>) -> NativeType {
    NativeType::Samples(evaluated_schema(grid))
}

fn metric() -> MetricName {
    MetricName::try_new(Some("requests".into()), Vec::new()).unwrap()
}

/// The raw source every supported tree starts from.
fn input() -> NativeNode {
    let output = NativeType::Samples(raw_schema());
    NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: output.clone(),
            ordering: SeriesOrdering::KeyThenTime,
        }),
        output,
    }
}

/// The same source over `count` float fields, which one input chunk has to carry all of.
fn raw_input_of(count: usize) -> NativeNode {
    let output = NativeType::Samples(raw_schema_of(field_count(count)));
    NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: output.clone(),
            ordering: SeriesOrdering::KeyThenTime,
        }),
        output,
    }
}

/// A source whose chunks are already evaluated, such as the inner result of a subquery.
fn evaluated_input(grid: Arc<EvalGrid>) -> NativeNode {
    evaluated_input_with_ordering(grid, SeriesOrdering::KeyThenTime)
}

/// A source of evaluated chunks that guarantees `ordering`.
fn evaluated_input_with_ordering(grid: Arc<EvalGrid>, ordering: SeriesOrdering) -> NativeNode {
    let output = evaluated(grid);
    NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: output.clone(),
            ordering,
        }),
        output,
    }
}

fn window(inner: NativeNode, output_grid: Arc<EvalGrid>) -> NativeNode {
    window_with(
        inner,
        RangeFunction::LastOverTime,
        300_000,
        None,
        output_grid,
    )
}

fn window_with(
    inner: NativeNode,
    function: RangeFunction,
    range_ms: i64,
    input_grid: Option<Arc<EvalGrid>>,
    output_grid: Arc<EvalGrid>,
) -> NativeNode {
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
                input_grid,
                output_grid: Arc::clone(&output_grid),
            },
        },
        output: evaluated(output_grid),
    }
}

/// A 1 ms `last_over_time` window over `count` float fields, whose output carries them all.
///
/// Every other window here is over `fields`; this one is what a test of per-field retention needs.
fn window_over_fields(count: usize, output_grid: Arc<EvalGrid>) -> NativeNode {
    let fields = field_count(count);
    NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(raw_input_of(count)),
            op: BoundWindow {
                function: RangeFunction::LastOverTime,
                window: RangeWindow {
                    range_ms: 1,
                    offset_ms: 0,
                    anchor_ms: None,
                },
                input_grid: None,
                output_grid: Arc::clone(&output_grid),
            },
        },
        output: NativeType::Samples(Arc::new(SeriesSchema {
            fields,
            eval_grid: Some(output_grid),
        })),
    }
}

fn map(inner: NativeNode, ops: Vec<MathOp>) -> NativeNode {
    let output = inner.output.grid().expect("a map needs a grid").clone();
    NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(inner),
            op: BoundMap { ops: ops.into() },
        },
        output: evaluated(output),
    }
}

fn scalar(value: f64, grid: Arc<EvalGrid>) -> NativeNode {
    NativeNode {
        expr: NativeExpr::Scalar(BoundScalar { value }),
        output: NativeType::Scalar(grid),
    }
}

fn raw_chunk(
    schema: &SeriesSchema,
    timestamps: &[i64],
    values: &[f64],
    is_last: bool,
) -> RawSeriesChunk {
    raw_chunk_of(schema, b"series-a", metric(), timestamps, values, is_last)
}

/// One raw chunk of the series named by `key` and `metric`.
fn raw_chunk_of(
    schema: &SeriesSchema,
    key: &[u8],
    metric: MetricName,
    timestamps: &[i64],
    values: &[f64],
    is_last: bool,
) -> RawSeriesChunk {
    RawSeriesChunk::try_new(
        schema,
        Buffer::from_slice_ref(key),
        Some(metric),
        RawTimestamps::try_new(ScalarBuffer::from(timestamps.to_vec())).unwrap(),
        SeriesFields::one(Samples::from_float(Float64Array::from(values.to_vec()))),
        is_last,
    )
    .unwrap()
}

/// One raw chunk of `series-a` whose every field carries `values`, so one input can be pushed to a
/// schema of more than one field.
fn raw_chunk_repeated(
    schema: &SeriesSchema,
    timestamps: &[i64],
    values: &[f64],
    is_last: bool,
) -> RawSeriesChunk {
    let fields = SeriesFields::try_from_vec(
        (0..schema.fields.len())
            .map(|_| Samples::from_float(Float64Array::from(values.to_vec())))
            .collect(),
    )
    .unwrap();
    RawSeriesChunk::try_new(
        schema,
        Buffer::from_slice_ref(b"series-a"),
        Some(metric()),
        RawTimestamps::try_new(ScalarBuffer::from(timestamps.to_vec())).unwrap(),
        fields,
        is_last,
    )
    .unwrap()
}

/// Pushes one native histogram whose positive side is one span of `buckets` buckets of one count
/// into `builder`, and returns the index it was filed under.
fn push_histogram(builder: &mut HistogramBatchBuilder, buckets: i32) -> u32 {
    let offsets = [0];
    let lengths = [buckets];
    let counts = vec![1.0; usize::try_from(buckets).unwrap()];
    builder
        .push(HistogramView {
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count: f64::from(buckets),
            sum: f64::from(buckets),
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
        .unwrap()
}

/// One raw chunk of `series-a` whose one field holds a `buckets`-wide native histogram at each of
/// `timestamps`.
fn raw_histogram_chunk(
    schema: &SeriesSchema,
    timestamps: &[i64],
    buckets: i32,
    is_last: bool,
) -> RawSeriesChunk {
    let mut builder = HistogramBatchBuilder::new();
    let indices: UInt32Array = timestamps
        .iter()
        .map(|_| Some(push_histogram(&mut builder, buckets)))
        .collect();
    let samples = Samples::try_from_histogram(indices, builder.finish().unwrap()).unwrap();
    RawSeriesChunk::try_new(
        schema,
        Buffer::from_slice_ref(b"series-a"),
        Some(metric()),
        RawTimestamps::try_new(ScalarBuffer::from(timestamps.to_vec())).unwrap(),
        SeriesFields::one(samples),
        is_last,
    )
    .unwrap()
}

/// One raw chunk of `series-a` whose one field holds the histogram at `index` of `table`.
///
/// Two chunks built this way share one `Arc<HistogramBatch>`, which is what makes the allocation
/// one table for accounting even though each chunk carries its own indices into it.
fn raw_shared_histogram_chunk(
    schema: &SeriesSchema,
    table: &Arc<HistogramBatch>,
    index: u32,
    timestamp: i64,
    is_last: bool,
) -> RawSeriesChunk {
    let samples =
        Samples::try_from_histogram(UInt32Array::from(vec![Some(index)]), Arc::clone(table))
            .unwrap();
    RawSeriesChunk::try_new(
        schema,
        Buffer::from_slice_ref(b"series-a"),
        Some(metric()),
        RawTimestamps::try_new(ScalarBuffer::from(vec![timestamp])).unwrap(),
        SeriesFields::one(samples),
        is_last,
    )
    .unwrap()
}

/// A source node's runtime: the queue, its accounting and the reservation the two are held for.
fn source_runtime(pool: &Arc<dyn MemoryPool>) -> NodeRuntime {
    NodeRuntime {
        buffer: NodeBuffer::new(
            &NodePath::root().input(),
            Arc::new(NodeMetrics::default()),
            pool,
        ),
        queued: VecDeque::new(),
        queued_charge: QueuedCharge::default(),
        window: None,
        map: None,
        emitted: None,
    }
}

/// One evaluated chunk of an input series: `values` at `start` and onwards on `schema`'s grid.
fn evaluated_chunk(
    schema: &SeriesSchema,
    start: usize,
    values: Vec<f64>,
    is_last: bool,
) -> EvaluatedChunk {
    evaluated_chunk_of(
        schema,
        metric(),
        start,
        values.into_iter().map(Some).collect(),
        is_last,
    )
}

/// One evaluated chunk of the series `metric`, `values` at `start` and onwards on `schema`'s grid.
///
/// A `None` value is a position the series decided to hold no sample, which is what an inner
/// evaluated result carries where it had nothing to report.
fn evaluated_chunk_of(
    schema: &SeriesSchema,
    metric: MetricName,
    start: usize,
    values: Vec<Option<f64>>,
    is_last: bool,
) -> EvaluatedChunk {
    let samples = Samples::from_float(Float64Array::from(values));
    let range =
        GridRange::try_new(schema.eval_grid.clone().unwrap(), start, samples.len()).unwrap();
    let series = Series {
        metric,
        time: TimeAxis::Grid(range),
        fields: SeriesFields::try_from_vec(vec![samples]).unwrap(),
    };
    EvaluatedChunk::try_new(series, is_last).unwrap()
}

/// The series `name{job="job"}`, so two series can share the label a map leaves behind.
fn named_metric(name: &str, job: &str) -> MetricName {
    MetricName::try_new(
        Some(name.into()),
        vec![Label {
            name: "job".into(),
            value: job.into(),
        }],
    )
    .unwrap()
}

/// `requests{host="web-1"}` with `zone` either present and empty or absent.
///
/// The two are distinct series to a scan — an empty tag value is a present label, an absent one is
/// not — and the same label set to everything that drops empty-valued labels, as Prometheus does.
fn host_metric(zone: Option<&str>) -> MetricName {
    let mut tags = vec![Label {
        name: "host".into(),
        value: "web-1".into(),
    }];
    if let Some(zone) = zone {
        tags.push(Label {
            name: "zone".into(),
            value: zone.into(),
        });
    }
    MetricName::try_new(Some("requests".into()), tags).unwrap()
}

fn pool() -> Arc<dyn MemoryPool> {
    Arc::new(GreedyMemoryPool::new(1 << 20))
}

/// A pool with a fixed budget, so a reservation that grows with the input fails here.
fn bounded_pool(bytes: usize) -> Arc<dyn MemoryPool> {
    Arc::new(GreedyMemoryPool::new(bytes))
}

fn executor(
    tree: &NativeTree,
    pool: Arc<dyn MemoryPool>,
    set: &ExecutionPlanMetricsSet,
) -> NativeExecutor {
    NativeExecutor::new(NativeExecutionState::try_new(tree, pool, set, 0).unwrap())
}

/// Pulls until the tree has nothing settled, then closes it.
fn run(executor: &mut NativeExecutor) -> Vec<EvaluatedChunk> {
    let mut out = Vec::new();
    while let Some(chunk) = executor.next_chunk().unwrap() {
        out.push(chunk);
    }
    out.extend(executor.finish().unwrap());
    out
}

fn sample_values(chunk: &EvaluatedChunk) -> Vec<Option<f64>> {
    let field = chunk
        .series
        .fields
        .get(0)
        .expect("these series have one field");
    field
        .iter()
        .map(|sample| match sample {
            SampleRef::Float(value) => Some(value),
            _ => None,
        })
        .collect()
}

fn series_start(chunk: &EvaluatedChunk) -> usize {
    chunk
        .series
        .time
        .first_position()
        // An empty chunk carries no row, but where it sits is still what it addresses: a closing
        // chunk that holds nothing is handed over exactly so the consumer learns it closed.
        .or_else(|| chunk.series.time.as_grid_range().map(GridRange::start))
        .expect("every chunk in these tests is addressed on a grid")
}

/// The absolute start and the values of every chunk, which is what addressing means here.
fn addressed(chunks: &[EvaluatedChunk]) -> Vec<(usize, Vec<Option<f64>>)> {
    chunks
        .iter()
        .map(|chunk| (series_start(chunk), sample_values(chunk)))
        .collect()
}

/// Every decided position of a chunk stream with its value, in absolute grid order.
///
/// Two streams that decided the same positions hold the same result however they were split into
/// chunks, which is what "the same series, delivered differently" means.
///
/// The positions come from the axis, not from the chunk's start plus a row number: a chunk whose
/// rows form several runs carries them at absolute positions that are not adjacent, so adding the
/// row to the start would name positions the chunk does not hold.
fn decided(chunks: &[EvaluatedChunk]) -> Vec<(usize, Option<f64>)> {
    chunks
        .iter()
        .flat_map(|chunk| {
            let values = sample_values(chunk);
            chunk
                .series
                .time
                .positions()
                .map(|(position, row)| {
                    (
                        position,
                        *values.get(row).expect("one value per carried row"),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn count(set: &ExecutionPlanMetricsSet, path: &NodePath, metric: &str) -> usize {
    let name = path.metric_name(metric);
    match set.clone_inner().sum_by_name(&name) {
        Some(MetricValue::Count { count, .. }) => count.value(),
        other => panic!("no count named {name}: {other:?}"),
    }
}

fn gauge(set: &ExecutionPlanMetricsSet, name: &str) -> usize {
    match set.clone_inner().sum_by_name(name) {
        Some(MetricValue::Gauge { gauge, .. }) => gauge.value(),
        other => panic!("no gauge named {name}: {other:?}"),
    }
}

/// A window over raw input evaluates onto its own output grid, and its values are the evaluator's.
///
/// The grid is compared by allocation identity, so a window that evaluated on a copy of its
/// output grid — the same timestamps, a different grid — fails here even though every value and
/// every timestamp would still look right.
#[test]
fn a_window_over_raw_input_evaluates_onto_its_own_grid() {
    let out_grid = grid(0, 60_000, 4);
    let tree = NativeTree::try_new(window(input(), Arc::clone(&out_grid))).unwrap();
    let schema = raw_schema();
    let chunk = raw_chunk(
        &schema,
        &[0, 60_000, 120_000, 180_000, 240_000],
        &[1.0, 2.0, 3.0, 4.0, 5.0],
        true,
    );

    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    execution.push_raw(chunk.clone()).unwrap();
    let out = run(&mut execution);

    // The same samples, evaluated by the evaluator this runtime wraps.
    let mut evaluator = RangeEvaluator::try_new(
        &schema,
        Arc::clone(&out_grid),
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::LastOverTime,
    )
    .unwrap();
    let expected: Vec<(usize, Vec<Option<f64>>)> = evaluator
        .push(&chunk)
        .unwrap()
        .iter()
        .map(|produced| (series_start(produced), sample_values(produced)))
        .collect();
    evaluator.finish().unwrap();

    assert_eq!(out.len(), 1, "one closed series, one chunk");
    for chunk in &out {
        assert!(
            Arc::ptr_eq(chunk.series.time.grid().expect("a grid chunk"), &out_grid),
            "the chunk is addressed on a grid that is not the window's output grid"
        );
        assert_eq!(
            chunk.series.time.grid().expect("a grid chunk").spec(),
            out_grid.spec()
        );
        assert!(chunk.is_last);
    }
    assert_eq!(
        addressed(&out),
        vec![(0, vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0)])]
    );
    assert_eq!(addressed(&out), expected);
}

/// `Window -> Map -> Window` keeps the inner grid inside and the outer grid outside.
///
/// The inner window runs on a 60 s grid and the outer one on a 90 s grid offset by 30 s, so a
/// layer that resampled onto its consumer's grid would change the addressing of every chunk and
/// the values of the last one.
#[test]
fn a_window_map_window_chain_keeps_each_layer_on_its_own_grid() {
    let inner_grid = grid(0, 60_000, 5);
    let outer_grid = grid(30_000, 90_000, 3);
    let inner = || {
        window_with(
            input(),
            RangeFunction::LastOverTime,
            120_000,
            None,
            Arc::clone(&inner_grid),
        )
    };
    let mapped = || map(inner(), vec![MathOp::Abs]);
    let chain = || {
        window_with(
            map(inner(), vec![MathOp::Abs]),
            RangeFunction::LastOverTime,
            240_000,
            Some(Arc::clone(&inner_grid)),
            Arc::clone(&outer_grid),
        )
    };
    let schema = raw_schema();
    let chunk = raw_chunk(
        &schema,
        &[0, 60_000, 120_000, 180_000, 240_000],
        &[-1.0, -2.0, -3.0, -4.0, -5.0],
        true,
    );

    // Down to the map, on its own: the map's chunks stay addressed on the inner window's grid.
    let map_tree = NativeTree::try_new(mapped()).unwrap();
    let mut map_execution = executor(&map_tree, pool(), &ExecutionPlanMetricsSet::new());
    map_execution.push_raw(chunk.clone()).unwrap();
    let mapped_chunks = run(&mut map_execution);
    assert_eq!(mapped_chunks.len(), 1);
    for chunk in &mapped_chunks {
        assert!(
            Arc::ptr_eq(chunk.series.time.grid().expect("a grid chunk"), &inner_grid),
            "the map handed over a chunk that is not addressed on the inner grid"
        );
    }
    assert_eq!(mapped_chunks[0].series.time.len(), 5);
    assert_eq!(
        addressed(&mapped_chunks),
        vec![(
            0,
            vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0), Some(5.0)]
        )]
    );

    // The whole chain: only the outer window's grid leaves the tree.
    let chain_tree = NativeTree::try_new(chain()).unwrap();
    let mut chain_execution = executor(&chain_tree, pool(), &ExecutionPlanMetricsSet::new());
    chain_execution.push_raw(chunk.clone()).unwrap();
    let out = run(&mut chain_execution);
    assert_eq!(out.len(), 1);
    for chunk in &out {
        assert!(
            Arc::ptr_eq(chunk.series.time.grid().expect("a grid chunk"), &outer_grid),
            "the chain handed over a chunk that is not addressed on the outer grid"
        );
    }
    assert_eq!(out[0].series.time.len(), 3);
    assert_eq!(
        addressed(&out),
        vec![(0, vec![Some(1.0), Some(3.0), Some(4.0)])]
    );

    // The same chain driven by hand, one kernel at a time.
    let mut inner_evaluator = RangeEvaluator::try_new(
        &schema,
        Arc::clone(&inner_grid),
        RangeWindow {
            range_ms: 120_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::LastOverTime,
    )
    .unwrap();
    let inner_series = inner_evaluator.push(&chunk).unwrap().unwrap();
    inner_evaluator.finish().unwrap();
    let map_schema = evaluated_schema(Arc::clone(&inner_grid));
    let mapped_chunk = map_chunk(&map_schema, &inner_series, &[MathOp::Abs])
        .unwrap()
        .expect("`abs` maps every position, so the chunk keeps its rows");
    assert!(
        Arc::ptr_eq(
            mapped_chunk.series.time.grid().expect("a grid chunk"),
            &inner_grid
        ),
        "the map kernel moved the inner grid"
    );
    let mut outer_evaluator = RangeEvaluator::try_new(
        &map_schema,
        Arc::clone(&outer_grid),
        RangeWindow {
            range_ms: 240_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::LastOverTime,
    )
    .unwrap();
    let outer_series = outer_evaluator
        .push_series(&mapped_chunk.series, mapped_chunk.is_last)
        .unwrap()
        .unwrap();
    outer_evaluator.finish().unwrap();
    assert_eq!(
        addressed(&out),
        vec![(series_start(&outer_series), sample_values(&outer_series))]
    );
}

/// A window over an evaluated input reports a decision before its input series has ended.
///
/// The inner series arrives in two pieces and the window is pulled after the first one, so the
/// outer grid point whose window that piece settled has to come out then. A layer that collected
/// the pieces into a whole series first could not produce anything here: its first answer would
/// only exist once the piece carrying `is_last` had arrived, and the bytes of the whole series
/// would be held until then.
#[test]
fn a_downstream_window_emits_before_its_input_series_ends() {
    // 60 s inner grid, 120 s outer grid, so the inner series settles outer points as it arrives.
    let inner_grid = grid(0, 60_000, 5);
    let outer_grid = grid(0, 120_000, 3);
    let tree = NativeTree::try_new(window_with(
        evaluated_input(Arc::clone(&inner_grid)),
        RangeFunction::LastOverTime,
        240_000,
        Some(Arc::clone(&inner_grid)),
        Arc::clone(&outer_grid),
    ))
    .unwrap();
    let schema = evaluated_schema(Arc::clone(&inner_grid));

    // The first piece covers inner positions 0..3; the series is still open after it.
    let head = evaluated_chunk(&schema, 0, vec![1.0, 2.0, 3.0], false);
    let tail = evaluated_chunk(&schema, 3, vec![4.0, 5.0], true);

    let set = ExecutionPlanMetricsSet::new();
    let mut execution = executor(&tree, pool(), &set);
    execution.push_evaluated(head).unwrap();

    let mut early = Vec::new();
    while let Some(chunk) = execution.next_chunk().unwrap() {
        early.push(chunk);
    }
    assert!(
        !early.is_empty(),
        "the outer window buffered its input: nothing came out before the series ended"
    );
    // The piece settled outer points 0 and 1: their windows end inside it, at the inner values
    // 1.0 and 3.0.
    assert_eq!(
        decided(&early),
        vec![(0, Some(1.0)), (1, Some(3.0))],
        "the outer window did not report the points its input had already settled"
    );
    for chunk in &early {
        assert!(
            !chunk.is_last,
            "the series cannot close before its last piece"
        );
    }
    // The open outer series is still accounted for: the pieces handed over are what it holds.
    assert_eq!(
        count(&set, &NodePath::root(), NodeMetric::COMPLETED_SERIES),
        0,
        "a series closed before its last piece arrived"
    );

    // The closing piece: the rest of the outer grid, and the one chunk that closes the series.
    execution.push_evaluated(tail).unwrap();
    let mut out = early;
    while let Some(chunk) = execution.next_chunk().unwrap() {
        out.push(chunk);
    }
    out.extend(execution.finish().unwrap());
    assert_eq!(
        decided(&out),
        vec![(0, Some(1.0)), (1, Some(3.0)), (2, Some(5.0)),]
    );
    assert_eq!(out.iter().filter(|chunk| chunk.is_last).count(), 1);
    assert!(out.last().unwrap().is_last);
    assert_eq!(
        count(&set, &NodePath::root(), NodeMetric::COMPLETED_SERIES),
        1,
        "the outer series closed once per piece instead of once in total"
    );

    // The same chain with the whole series in one piece answers exactly the same.
    let whole_tree = NativeTree::try_new(window_with(
        evaluated_input(Arc::clone(&inner_grid)),
        RangeFunction::LastOverTime,
        240_000,
        Some(Arc::clone(&inner_grid)),
        Arc::clone(&outer_grid),
    ))
    .unwrap();
    let mut whole = executor(&whole_tree, pool(), &ExecutionPlanMetricsSet::new());
    whole
        .push_evaluated(evaluated_chunk(
            &schema,
            0,
            vec![1.0, 2.0, 3.0, 4.0, 5.0],
            true,
        ))
        .unwrap();
    let whole = run(&mut whole);
    assert_eq!(decided(&out), decided(&whole));
    for chunk in &whole {
        assert!(
            Arc::ptr_eq(chunk.series.time.grid().expect("a grid chunk"), &outer_grid),
            "the chain handed over a chunk that is not addressed on the outer grid"
        );
    }
}

/// Every node kind the runtime does not execute is rejected, naming the kind and its position.
#[test]
fn an_unimplemented_node_is_reported_not_panicked() {
    // One grid shared by the nodes that validate by allocation identity.
    let statement = || grid(0, 120_000, 2);

    let scalar_tree = scalar(1.0, statement());
    let instant_tree = {
        let out = statement();
        map(
            NativeNode {
                expr: NativeExpr::Instant {
                    input: Box::new(input()),
                    op: BoundInstant {
                        lookback_ms: 300_000,
                        offset_ms: 0,
                        output_grid: Arc::clone(&out),
                    },
                },
                output: NativeType::Samples(evaluated_schema(out)),
            },
            vec![MathOp::Abs],
        )
    };
    let aggregate_tree = {
        let out = statement();
        NativeNode {
            expr: NativeExpr::Aggregate {
                input: Box::new(window(input(), Arc::clone(&out))),
                op: BoundAggregate {
                    kind: AggregateKind::Sum,
                    group: Grouping::By(Arc::new(["job".into()])),
                    stage: AggregateStage::Complete,
                },
            },
            output: NativeType::Samples(evaluated_schema(out)),
        }
    };
    let binary_tree = {
        let out = statement();
        NativeNode {
            expr: NativeExpr::Binary {
                left: Box::new(window(input(), Arc::clone(&out))),
                right: Box::new(scalar(1.0, Arc::clone(&out))),
                matching: BinaryMatching {
                    op: BinaryOp::Add,
                    return_bool: false,
                    labels: MatchingLabels::Ignoring(Vec::new()),
                    cardinality: Cardinality::OneToOne,
                },
            },
            output: NativeType::Samples(evaluated_schema(out)),
        }
    };

    let cases = [
        ("Scalar", "root", scalar_tree),
        ("Instant", "root.input", instant_tree),
        ("Aggregate", "root", aggregate_tree),
        ("Binary", "root", binary_tree),
    ];
    for (kind, path, root) in cases {
        // Legality stays `validate`'s: the plan accepts these trees, the runtime does not run them.
        let tree = NativeTree::try_new(root).expect("the bound tree is legal");
        let error = match NativeExecutionState::try_new(
            &tree,
            pool(),
            &ExecutionPlanMetricsSet::new(),
            0,
        ) {
            Ok(_) => panic!("the runtime accepted a {kind} node"),
            Err(error) => error,
        };
        assert!(
            matches!(error, DataFusionError::NotImplemented(_)),
            "{kind}: {error}"
        );
        let message = error.to_string();
        assert!(message.contains(kind), "{kind}: {message}");
        assert!(message.contains(&format!("at {path}")), "{message}");
    }
}

/// Each node reports its own chunks, grid points and closed series, at its own position.
///
/// The two windows decide different traffic — the inner one four grid points of its own grid, the
/// outer one two of its own — so a single total for the tree cannot produce both numbers. They take
/// and hand over the same number of chunks, which is what streaming means here: a node passes on one
/// chunk per chunk it took in, instead of holding its input until a whole series is available.
#[test]
fn each_node_records_its_own_metrics() {
    let inner_grid = grid(0, 60_000, 4);
    let outer_grid = grid(0, 120_000, 2);
    let root = window_with(
        window_with(
            input(),
            RangeFunction::LastOverTime,
            300_000,
            None,
            Arc::clone(&inner_grid),
        ),
        RangeFunction::LastOverTime,
        300_000,
        Some(inner_grid),
        outer_grid,
    );
    let tree = NativeTree::try_new(root).unwrap();
    let set = ExecutionPlanMetricsSet::new();
    let mut execution = executor(&tree, pool(), &set);
    let schema = raw_schema();
    execution
        .push_raw(raw_chunk(&schema, &[0, 60_000], &[1.0, 2.0], false))
        .unwrap();
    execution
        .push_raw(raw_chunk(&schema, &[120_000], &[3.0], false))
        .unwrap();
    execution
        .push_raw(raw_chunk(&schema, &[180_000], &[4.0], true))
        .unwrap();
    let out = run(&mut execution);
    // The outer window reports each point as the inner one settles it: one chunk per inner chunk,
    // and an empty chunk that closes the series.
    assert_eq!(
        addressed(&out),
        vec![(0, vec![Some(1.0)]), (1, vec![Some(3.0)]), (2, Vec::new()),]
    );
    assert!(out.last().unwrap().is_last);

    let outer = NodePath::root();
    let inner = outer.input();
    let source = inner.input();

    // Three source chunks reach the inner window, and the chunks it hands over reach the outer one
    // one for one, because neither window buffers.
    assert_eq!(count(&set, &source, NodeMetric::INPUT_BATCHES), 3);
    assert_eq!(count(&set, &source, NodeMetric::OUTPUT_BATCHES), 3);
    assert_eq!(count(&set, &inner, NodeMetric::INPUT_BATCHES), 3);
    assert_eq!(count(&set, &outer, NodeMetric::INPUT_BATCHES), 3);
    assert_eq!(count(&set, &inner, NodeMetric::OUTPUT_BATCHES), 3);
    assert_eq!(count(&set, &outer, NodeMetric::OUTPUT_BATCHES), 3);
    // The grid points each window decided, which is its own output range: four on the inner grid
    // and two on the outer one, which is the figure that tells the two nodes apart.
    assert_eq!(count(&set, &inner, NodeMetric::EVALUATED_GRID_POINTS), 4);
    assert_eq!(count(&set, &outer, NodeMetric::EVALUATED_GRID_POINTS), 2);
    // The one series closes once, on the chunk carrying `is_last`.
    assert_eq!(count(&set, &source, NodeMetric::COMPLETED_SERIES), 1);
    assert_eq!(count(&set, &inner, NodeMetric::COMPLETED_SERIES), 1);
    assert_eq!(count(&set, &outer, NodeMetric::COMPLETED_SERIES), 1);
    // Per node, not one total for the tree.
    assert_ne!(
        count(&set, &inner, NodeMetric::EVALUATED_GRID_POINTS),
        count(&set, &outer, NodeMetric::EVALUATED_GRID_POINTS)
    );
}

/// An error returns the reservation while the execution is still alive, and a drop returns it too.
#[test]
fn an_error_releases_the_reservation() {
    let tree = NativeTree::try_new(window(input(), grid(0, 60_000, 4))).unwrap();
    let pool = pool();
    let set = ExecutionPlanMetricsSet::new();
    let schema = raw_schema();

    let mut execution = executor(&tree, Arc::clone(&pool), &set);
    assert_eq!(pool.reserved(), 0);
    assert_eq!(
        execution.state().nodes_holding_windows(),
        1,
        "a window that has not been released is what the drop below has to give back"
    );
    execution
        .push_raw(raw_chunk(&schema, &[0, 60_000], &[1.0, 2.0], false))
        .unwrap();
    assert!(
        pool.reserved() > 0,
        "the queued chunk has to be reserved for"
    );
    // The chunk settles the first grid points, so it comes out; the series it belongs to has not
    // ended, so the node still holds what it buffered for it.
    let settled = execution.next_chunk().unwrap();
    assert!(
        settled.is_some_and(|chunk| !chunk.is_last),
        "a chunk that does not close its series was reported as closing it"
    );
    let held = pool.reserved();
    assert!(held > 0, "the open series has to be reserved for");

    // A second chunk of the same series whose timestamps do not increase: the evaluator rejects it
    // after the node already held input for it.
    execution
        .push_raw(raw_chunk(&schema, &[0, 60_000], &[3.0, 4.0], true))
        .unwrap();
    let error = execution.next_chunk().unwrap_err();
    assert!(error.to_string().contains("not increasing"), "{error}");
    // Everything went back to the pool while the execution is alive: waiting for the drop would
    // keep the query's bytes in use for as long as the caller holds the executor.
    assert_eq!(pool.reserved(), 0);
    assert_eq!(execution.state().reserved_bytes(), 0);
    // The numbers above only say the reservation was returned. The state the bytes were taken for
    // has to be gone too, or the executor keeps the evaluator's sample buffers alive while the pool
    // reports them as free: the window is dropped, not only its accounting.
    assert_eq!(
        execution.state().nodes_holding_windows(),
        0,
        "the stopped execution still holds a window's samples while its reservation reads zero"
    );
    let current = NodePath::root().metric_name(NodeMetric::CURRENT_RESERVED_BYTES);
    assert_eq!(gauge(&set, &current), 0);
    let peak = NodePath::root().partitioned_metric_name(NodeMetric::PEAK_RESERVED_BYTES, 0);
    assert!(gauge(&set, &peak) >= held);
    // A stopped execution is not resumed.
    let again = execution.next_chunk().unwrap_err();
    assert!(
        again.to_string().contains("stopped after an earlier error"),
        "{again}"
    );

    // Dropping an execution that never finished returns its bytes too.
    let mut unfinished = executor(&tree, Arc::clone(&pool), &set);
    unfinished
        .push_raw(raw_chunk(&schema, &[0], &[1.0], false))
        .unwrap();
    let _ = unfinished.next_chunk().unwrap();
    assert!(
        pool.reserved() > 0,
        "the open series has to be reserved for"
    );
    drop(unfinished);
    assert_eq!(pool.reserved(), 0);
}

/// A map that drops the metric name cannot hand two series on under one label set.
///
/// `a{job="x"}` and `b{job="x"}` arrive whole, so each says it is the last chunk of its own series,
/// and `abs` drops the name, so both become `{job="x"}`. A window above the map reads chunks as
/// they arrive, so it would take them for two series and answer once per series; the only answer
/// the data has is the merged series, which this layer does not hold. The map refuses the second
/// one before the window above evaluates it.
#[test]
fn two_series_onto_one_label_set_after_a_map_are_rejected() {
    let inner_grid = grid(0, 60_000, 2);
    let outer_grid = grid(60_000, 60_000, 1);
    let tree = || {
        NativeTree::try_new(window_with(
            map(evaluated_input(Arc::clone(&inner_grid)), vec![MathOp::Abs]),
            RangeFunction::AvgOverTime,
            120_000,
            Some(Arc::clone(&inner_grid)),
            Arc::clone(&outer_grid),
        ))
        .unwrap()
    };
    let schema = evaluated_schema(Arc::clone(&inner_grid));

    // The first series closes, so its window answer comes out on its own.
    let mut execution = executor(&tree(), pool(), &ExecutionPlanMetricsSet::new());
    execution
        .push_evaluated(evaluated_chunk_of(
            &schema,
            named_metric("a", "x"),
            0,
            vec![Some(1.0), None],
            true,
        ))
        .unwrap();
    let first = execution
        .next_chunk()
        .unwrap()
        .expect("the first series settles the outer point");
    assert_eq!(sample_values(&first), vec![Some(1.0)]);
    assert!(first.is_last);

    // The second one maps onto the label set the first one closed. Answering would produce a second
    // chunk with the same labels at the same point.
    execution
        .push_evaluated(evaluated_chunk_of(
            &schema,
            named_metric("b", "x"),
            0,
            vec![None, Some(3.0)],
            true,
        ))
        .unwrap();
    let error = match execution.next_chunk() {
        Ok(None) => panic!("the second series settled nothing at all"),
        Ok(Some(chunk)) => panic!(
            "the map handed a second series on at the label set the first one closed: {:?}",
            decided(&[chunk])
        ),
        Err(error) => error,
    };
    let message = error.to_string();
    assert!(
        message.contains("job"),
        "the rejection does not name the label set: {message}"
    );
    assert!(
        message.contains("closed"),
        "the rejection does not say the label set was already closed: {message}"
    );

    // What the rejection protects: one series carrying both samples averages to 2 at that point,
    // which is the answer neither of the two separate series can produce.
    let mut merged = executor(&tree(), pool(), &ExecutionPlanMetricsSet::new());
    merged
        .push_evaluated(evaluated_chunk_of(
            &schema,
            named_metric("c", "x"),
            0,
            vec![Some(1.0), Some(3.0)],
            true,
        ))
        .unwrap();
    let answer = merged
        .next_chunk()
        .unwrap()
        .expect("the merged series settles the outer point");
    assert_eq!(sample_values(&answer), vec![Some(2.0)]);
}

/// A window that drops empty-valued labels cannot hand two series on under one label set either.
///
/// `{host="web-1", zone=""}` and `{host="web-1"}` are two series to the scan: an empty tag value is
/// a present label, an absent one is not. `rate` drops the metric name and the empty-valued labels,
/// so both become `{host="web-1"}` and each covers the same grid; answering would hand the whole
/// grid out twice under the same labels and the same timestamps. No map is involved, so the
/// rejection has to be the node's own — a check at the root would be too late, because the window
/// above would already have evaluated the two as two series.
#[test]
fn two_series_onto_one_label_set_after_a_window_are_rejected() {
    let out_grid = grid(0, 60_000, 5);
    let tree = NativeTree::try_new(window_with(
        input(),
        RangeFunction::Rate,
        300_000,
        None,
        Arc::clone(&out_grid),
    ))
    .unwrap();
    let schema = raw_schema();
    let counter = [0, 60_000, 120_000, 180_000, 240_000];

    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    execution
        .push_raw(raw_chunk_of(
            &schema,
            b"series-a",
            host_metric(Some("")),
            &counter,
            &[1.0, 2.0, 3.0, 4.0, 5.0],
            true,
        ))
        .unwrap();
    let first = execution
        .next_chunk()
        .unwrap()
        .expect("the first series settles the grid");
    assert!(!first.series.time.is_empty());
    assert!(first.is_last);

    // The second series is a different one to the scan and the same one to `rate`.
    execution
        .push_raw(raw_chunk_of(
            &schema,
            b"series-b",
            host_metric(None),
            &counter,
            &[6.0, 7.0, 8.0, 9.0, 10.0],
            true,
        ))
        .unwrap();
    let error = match execution.next_chunk() {
        Ok(None) => panic!("the second series settled nothing at all"),
        Ok(Some(chunk)) => panic!(
            "a second series left the window under the first one's label set: {:?}",
            decided(&[chunk])
        ),
        Err(error) => error,
    };
    let message = error.to_string();
    assert!(message.contains("host"), "{message}");
    assert!(message.contains("web-1"), "{message}");
    assert!(message.contains("closed"), "{message}");

    // Two series carrying different hosts stay distinct, which is what a rate over a label that
    // survives the rewrite looks like.
    let mut distinct = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    distinct
        .push_raw(raw_chunk_of(
            &schema,
            b"series-a",
            host_metric(Some("")),
            &counter,
            &[1.0, 2.0, 3.0, 4.0, 5.0],
            true,
        ))
        .unwrap();
    distinct
        .push_raw(raw_chunk_of(
            &schema,
            b"series-b",
            MetricName::try_new(
                Some("requests".into()),
                vec![Label {
                    name: "host".into(),
                    value: "web-2".into(),
                }],
            )
            .unwrap(),
            &counter,
            &[6.0, 7.0, 8.0, 9.0, 10.0],
            true,
        ))
        .unwrap();
    let out = run(&mut distinct);
    assert_eq!(out.len(), 2, "two series over two hosts are two series");
}

/// A chunk that holds a value where the same field already holds one is rejected.
///
/// Neither piece closes its series, so this is not a closed label set: the refusal is about the
/// positions themselves. `v0` would carry two values at position 0 under one label set, which no
/// merge can resolve, so the node refuses rather than picking one.
#[test]
fn two_series_holding_a_value_at_one_position_are_rejected() {
    let map_grid = grid(0, 60_000, 2);
    let tree = NativeTree::try_new(map(
        evaluated_input(Arc::clone(&map_grid)),
        vec![MathOp::Abs],
    ))
    .unwrap();
    let schema = evaluated_schema(Arc::clone(&map_grid));

    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    for (name, values) in [
        ("a", vec![Some(-1.0), Some(-2.0)]),
        ("b", vec![Some(-3.0), Some(-4.0)]),
    ] {
        execution
            .push_evaluated(evaluated_chunk_of(
                &schema,
                named_metric(name, "x"),
                0,
                values,
                false,
            ))
            .unwrap();
    }
    // The first piece is fine on its own: nothing has occupied any position yet.
    let first = execution
        .next_chunk()
        .unwrap()
        .expect("the first piece is handed on as it arrives");
    assert_eq!(sample_values(&first), vec![Some(1.0), Some(2.0)]);
    // The second one puts a second value of the same field at position 0.
    let error = match execution.next_chunk() {
        Ok(None) => panic!("the map produced no chunk at all"),
        Ok(Some(chunk)) => panic!(
            "a second series holding a value at the same position was handed on: {:?}",
            decided(&[chunk])
        ),
        Err(error) => error,
    };
    let message = error.to_string();
    assert!(message.contains("job"), "{message}");
    assert!(message.contains("field 0"), "{message}");
    assert!(message.contains("position 0"), "{message}");
}

/// Two closed series that hold no position of the same field in common still cannot be handed on:
/// merging across a closed series is not implemented by this streaming evaluator, so the pair is
/// refused rather than answered wrongly.
///
/// Neither piece holds a value where the other does — each has a missing value where the other holds
/// one — so the two do merge in Prometheus, and [`crate::grid_series::merge_same_labelset`] accepts
/// exactly this. What refuses it is the label set having closed on the piece carrying `is_last`: the
/// evaluator above has already closed its per-series state for it, so the node cannot hold the first
/// series back until it knows whether a second one follows. The refusal has to say so; a test that
/// passed both pieces as open chunks would be proving a merge the runtime never performs.
#[test]
fn a_merge_across_a_closed_series_is_not_implemented() {
    let map_grid = grid(0, 60_000, 2);
    let tree = NativeTree::try_new(map(
        evaluated_input(Arc::clone(&map_grid)),
        vec![MathOp::Abs],
    ))
    .unwrap();
    let schema = evaluated_schema(Arc::clone(&map_grid));

    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    // Each source series is closed properly: the pieces hold disjoint positions of field 0 and both
    // end their series.
    for (name, values) in [("a", vec![Some(-1.0), None]), ("b", vec![None, Some(-3.0)])] {
        execution
            .push_evaluated(evaluated_chunk_of(
                &schema,
                named_metric(name, "x"),
                0,
                values,
                true,
            ))
            .unwrap();
    }
    let first = execution
        .next_chunk()
        .unwrap()
        .expect("the first closed series is handed on before the second one arrives");
    assert_eq!(sample_values(&first), vec![Some(1.0), None]);
    // The second one would merge with it, which this runtime does not do across a closed series.
    let error = match execution.next_chunk() {
        Ok(None) => panic!("the map produced no chunk for the closed label set"),
        Ok(Some(chunk)) => panic!(
            "a merge across a closed series was handed on instead of refused: {:?}",
            decided(&[chunk])
        ),
        Err(error) => error,
    };
    let message = error.to_string();
    assert!(message.contains("job"), "{message}");
    assert!(
        message.contains("already closed the label set"),
        "{message}"
    );
}

/// Two open chunks that share a label set are accepted where no position of a field collides.
///
/// `a{job="x"}` and `b{job="x"}` are two series to the input and one label set to `abs`, which
/// drops the name. The source is interleaved, so the second one is pushed while the first is still
/// open, and neither says it is the last chunk of its series. The two occupy disjoint positions of
/// field 0 — a value at 0 and a value at 3 — so the label-set check hands both on.
///
/// This is acceptance by that check, and it is not a merge: this layer passes every chunk through
/// as it arrives and keeps no sample state that would combine two series. Merging two series under
/// one label set is [`crate::grid_series::merge_same_labelset`]'s job, and nothing here performs
/// it.
#[test]
fn two_open_series_that_share_a_label_set_are_accepted_when_no_position_collides() {
    let map_grid = grid(0, 60_000, 4);
    let tree = NativeTree::try_new(map(
        evaluated_input_with_ordering(Arc::clone(&map_grid), SeriesOrdering::Interleaved),
        vec![MathOp::Abs],
    ))
    .expect("a map-only tree is legal over an interleaved input");
    let schema = evaluated_schema(Arc::clone(&map_grid));

    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    execution
        .push_evaluated(evaluated_chunk_of(
            &schema,
            named_metric("a", "x"),
            0,
            vec![Some(-1.0), None],
            false,
        ))
        .unwrap();
    let first = execution
        .next_chunk()
        .unwrap()
        .expect("the first open chunk is handed on as it arrives");
    assert_eq!(sample_values(&first), vec![Some(1.0), None]);
    assert_eq!(series_start(&first), 0);

    // Positions 2 and 3 of field 0, holding a value at 3 only: no position of the field is
    // occupied twice, so nothing the check looks at is claimed by both series.
    execution
        .push_evaluated(evaluated_chunk_of(
            &schema,
            named_metric("b", "x"),
            2,
            vec![None, Some(-3.0)],
            false,
        ))
        .unwrap();
    let second = execution
        .next_chunk()
        .unwrap_or_else(|error| panic!("the second open chunk was refused: {error}"))
        .expect("the second open chunk is handed on too");
    assert_eq!(sample_values(&second), vec![None, Some(3.0)]);
    assert_eq!(series_start(&second), 2);
    assert!(
        execution.next_chunk().unwrap().is_none(),
        "the map handed on more than the two chunks it was given"
    );
}

/// One series arriving in pieces is not two series: a chunk that continues where its label set
/// ended is handed on.
///
/// The map is passed twice for one label set, and the window above it reports the whole series once
/// the piece carrying `is_last` arrives.
#[test]
fn a_series_that_continues_where_it_ended_is_not_a_second_series() {
    let inner_grid = grid(0, 60_000, 4);
    let outer_grid = grid(180_000, 60_000, 1);
    let tree = NativeTree::try_new(window_with(
        map(evaluated_input(Arc::clone(&inner_grid)), vec![MathOp::Abs]),
        RangeFunction::AvgOverTime,
        360_000,
        Some(Arc::clone(&inner_grid)),
        Arc::clone(&outer_grid),
    ))
    .unwrap();
    let schema = evaluated_schema(Arc::clone(&inner_grid));

    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    execution
        .push_evaluated(evaluated_chunk_of(
            &schema,
            named_metric("a", "x"),
            0,
            vec![Some(-1.0), None],
            false,
        ))
        .unwrap();
    while let Some(chunk) = execution.next_chunk().unwrap() {
        assert!(
            !chunk.is_last,
            "the series cannot close before its last chunk"
        );
    }
    execution
        .push_evaluated(evaluated_chunk_of(
            &schema,
            named_metric("a", "x"),
            2,
            vec![None, Some(-3.0)],
            true,
        ))
        .unwrap();
    let out = run(&mut execution);
    assert_eq!(
        decided(&out),
        vec![(0, Some(2.0))],
        "the two chunks of one series are one series to the window above the map"
    );
    assert!(out.last().unwrap().is_last);
}

/// A window reserves what it retains, not what it has consumed.
///
/// A long series is fed one sample per chunk to a pool with a fixed budget, pulling and dropping
/// the result of every chunk and closing only on the last one. The same one-sample-per-chunk input
/// leaves the evaluator's storage at the same size however many chunks it arrives in, so the
/// reservation reaches the same figure whether the data is split into 64 chunks or into 1024.
///
/// What the reservation has to be is not left to a constant: after every pull it is compared with
/// what the same window's evaluator says it retains and what the label sets it handed out hold. A
/// figure that merely did not grow with the chunk count would fail that, as would one that let the
/// emitted table go.
#[test]
fn a_window_reserves_what_it_retains_not_what_it_consumed() {
    // A 1 ms window on a 1 ms grid, so every chunk's sample is out of the window of the next one.
    let tree = || {
        NativeTree::try_new(window_with(
            input(),
            RangeFunction::LastOverTime,
            1,
            None,
            grid(0, 1, 1024),
        ))
        .unwrap()
    };
    let schema = raw_schema();

    let peak = |count: usize| {
        let pool = bounded_pool(32 << 10);
        let mut execution = executor(&tree(), Arc::clone(&pool), &ExecutionPlanMetricsSet::new());
        let mut peak = 0;
        for index in 0..count {
            execution
                .push_raw(raw_chunk(
                    &schema,
                    &[index as i64],
                    &[index as f64],
                    index + 1 == count,
                ))
                .unwrap_or_else(|error| {
                    panic!("chunk {index} of the {count}-chunk series did not fit: {error}")
                });
            // The output of every chunk is dropped: a consumer reading the series as it arrives
            // never holds it, and neither may the node.
            let pulled = execution.next_chunk().unwrap_or_else(|error| {
                panic!("chunk {index} of the {count}-chunk series did not fit: {error}")
            });
            drop(pulled);
            let held = execution.state().reserved_bytes();
            let (retained, emitted) = execution.state().window_charge(&NodePath::root());
            assert_eq!(
                held,
                retained.saturating_add(emitted),
                "chunk {index} of {count} holds {held} bytes, but the window retains {retained} \
                 and handed out {emitted}"
            );
            assert!(
                held <= (4 << 10),
                "chunk {index} of {count} holds {held} bytes of a 1 ms window over one sample"
            );
            peak = peak.max(held).max(pool.reserved());
        }
        peak
    };
    let short = peak(64);
    let long = peak(1024);
    assert_eq!(
        short, long,
        "the reservation grew with the number of chunks the same series arrived in"
    );
}

/// A window's retention is per field, as the evaluator's storage is.
///
/// The same one-sample-per-chunk input goes to a window over one float field, over two and over
/// three, and the reservation is compared with what each window retains after every pull. Every
/// field costs its own timestamps, values and row slot, so the figure grows by the same amount per
/// field; a figure counting one timestamp and one float per row would not. The widths are compared
/// by their increments rather than as a multiple of one: the identity of the open series is part of
/// the reservation and is counted once however many fields the series carries.
#[test]
fn a_window_retains_the_samples_of_every_field() {
    // The most the window ever retained, over the same input and the same short window.
    let peak = |width: usize| -> usize {
        let tree = NativeTree::try_new(window_over_fields(width, grid(0, 1, 1024))).unwrap();
        let schema = raw_schema_of(field_count(width));
        let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
        let (count, mut peak) = (8, 0);
        for index in 0..count {
            execution
                .push_raw(raw_chunk_repeated(
                    &schema,
                    &[index as i64],
                    &[index as f64],
                    index + 1 == count,
                ))
                .unwrap();
            drop(execution.next_chunk().unwrap());
            let (retained, emitted) = execution.state().window_charge(&NodePath::root());
            let held = execution.state().reserved_bytes();
            assert_eq!(
                held,
                retained.saturating_add(emitted),
                "{width} field(s), chunk {index} of {count}: {held} bytes reserved, but the \
                 window retains {retained} and handed out {emitted}"
            );
            peak = peak.max(retained);
        }
        peak
    };
    let (one, two, three) = (peak(1), peak(2), peak(3));
    assert!(
        two > one,
        "a window over two fields retained {two} bytes, no more than the {one} bytes a window over \
         one field retains"
    );
    assert_eq!(
        two - one,
        three - two,
        "a third field costs {} bytes where a second cost {}: the retention is not one field's \
         storage per field",
        three - two,
        two - one
    );
}

/// A chunk that produces nothing still leaves the table this node emitted into in its hands.
///
/// The first chunk decided both grid points and the window handed them out, so the table holds a
/// label set and its runs. The second chunk decides nothing — every point was already decided — so
/// the window yields no chunk, but it has given none of that history back: the reservation has to
/// keep covering the table, not drop it.
#[test]
fn a_chunk_with_no_output_keeps_charging_the_emitted_table() {
    let tree = NativeTree::try_new(window_with(
        input(),
        RangeFunction::LastOverTime,
        1,
        None,
        grid(0, 60_000, 2),
    ))
    .unwrap();
    let schema = raw_schema();
    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());

    execution
        .push_raw(raw_chunk(&schema, &[0, 60_000], &[1.0, 2.0], false))
        .unwrap();
    let first = execution
        .next_chunk()
        .unwrap()
        .expect("the first chunk decided both points");
    assert_eq!(sample_values(&first), vec![Some(1.0), Some(2.0)]);
    drop(first);
    let (retained, emitted) = execution.state().window_charge(&NodePath::root());
    assert!(
        emitted > 0,
        "the window handed out a position, which it has to hold"
    );
    assert_eq!(
        execution.state().reserved_bytes(),
        retained.saturating_add(emitted),
        "the reservation does not cover the {emitted} bytes the window handed out"
    );

    // Both points are decided, so this chunk decides nothing and the window yields no chunk.
    execution
        .push_raw(raw_chunk(&schema, &[120_000], &[3.0], false))
        .unwrap();
    assert!(
        execution.next_chunk().unwrap().is_none(),
        "a chunk after every point was decided produced a chunk"
    );
    let (retained, emitted) = execution.state().window_charge(&NodePath::root());
    assert!(
        emitted > 0,
        "the window's emitted table emptied itself between chunks"
    );
    assert_eq!(
        execution.state().reserved_bytes(),
        retained.saturating_add(emitted),
        "a chunk with no output released the {emitted} bytes the emitted table still holds"
    );
}

/// A window cannot run over an interleaved input, and the runtime says so before reading a chunk.
#[test]
fn a_window_over_an_interleaved_input_is_rejected() {
    let inner_grid = grid(0, 60_000, 2);
    let outer_grid = grid(0, 120_000, 1);
    let tree = NativeTree::try_new(window_with(
        evaluated_input_with_ordering(Arc::clone(&inner_grid), SeriesOrdering::Interleaved),
        RangeFunction::LastOverTime,
        120_000,
        Some(Arc::clone(&inner_grid)),
        Arc::clone(&outer_grid),
    ));
    // A window directly over an interleaved source is rejected when the tree is bound: the
    // binding is where the ordering is known, and refusing there fails before a chunk is read.
    let error = tree.expect_err("a window over an interleaved input must not bind");
    let message = error.to_string();
    assert!(message.contains("interleaved"), "{message}");
    assert!(message.contains("window"), "{message}");

    // A map between them is not caught by the binding, which only looks at a window's own input:
    // the map passes chunks through in the order they arrive, so the window still reads an
    // interleaved stream. That is what this runtime checks for itself.
    let tree = NativeTree::try_new(window_with(
        map(
            evaluated_input_with_ordering(Arc::clone(&inner_grid), SeriesOrdering::Interleaved),
            vec![MathOp::Abs],
        ),
        RangeFunction::LastOverTime,
        120_000,
        Some(Arc::clone(&inner_grid)),
        Arc::clone(&outer_grid),
    ))
    .expect("the binding accepts a window over a map");
    let error =
        match NativeExecutionState::try_new(&tree, pool(), &ExecutionPlanMetricsSet::new(), 0) {
            Ok(_) => panic!("the runtime accepted an interleaved input under a window"),
            Err(error) => error,
        };
    assert!(
        matches!(error, DataFusionError::NotImplemented(_)),
        "{error}"
    );
    let message = error.to_string();
    assert!(message.contains("Interleaved"), "{message}");
    assert!(message.contains("at root "), "{message}");
    assert!(message.contains("root.input"), "{message}");

    // A map needs no ordering: it holds one chunk at a time, so the series of an interleaved stream
    // never have to be contiguous. `A1, B1, A2, B2` runs here as it arrives.
    let map_grid = grid(0, 60_000, 4);
    let tree = NativeTree::try_new(map(
        evaluated_input_with_ordering(Arc::clone(&map_grid), SeriesOrdering::Interleaved),
        vec![MathOp::Abs],
    ))
    .expect("a map-only tree is legal over an interleaved input");
    let schema = evaluated_schema(Arc::clone(&map_grid));
    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    for (name, job, start, values, is_last) in [
        ("a", "x", 0, vec![Some(-1.0), Some(-2.0)], false),
        ("b", "y", 0, vec![Some(-3.0), Some(-4.0)], false),
        ("a", "x", 2, vec![Some(-5.0), Some(-6.0)], true),
        ("b", "y", 2, vec![Some(-7.0), Some(-8.0)], true),
    ] {
        execution
            .push_evaluated(evaluated_chunk_of(
                &schema,
                named_metric(name, job),
                start,
                values,
                is_last,
            ))
            .unwrap();
    }
    assert_eq!(
        addressed(&run(&mut execution)),
        vec![
            (0, vec![Some(1.0), Some(2.0)]),
            (0, vec![Some(3.0), Some(4.0)]),
            (2, vec![Some(5.0), Some(6.0)]),
            (2, vec![Some(7.0), Some(8.0)]),
        ]
    );
}

/// The charge for the label sets a node has handed out does not depend on how it is read.
///
/// The table only grows, so the figure is folded in as each entry changes rather than walked after
/// every output. Folding it wrong would show up as a running total that drifts from the same figure
/// recounted in full, so both are compared after every chunk: distinct label sets add entries, and
/// later chunks of one set grow its run vectors and the table's own capacity.
#[test]
fn the_emitted_charge_is_the_same_folded_in_as_walked() {
    let grid = grid(0, 60_000, 4);
    let tree =
        NativeTree::try_new(map(evaluated_input(Arc::clone(&grid)), vec![MathOp::Abs])).unwrap();
    let schema = evaluated_schema(Arc::clone(&grid));
    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());

    let chunks = [
        ("a", "x", 0, vec![Some(-1.0), None, Some(-2.0), None], false),
        ("a", "x", 0, vec![None, Some(-3.0), None, None], true),
        ("b", "y", 0, vec![None, None, None, Some(-4.0)], true),
        (
            "c",
            "z",
            0,
            vec![Some(-5.0), Some(-6.0), Some(-7.0), Some(-8.0)],
            true,
        ),
    ];
    for (name, job, start, values, is_last) in chunks {
        execution
            .push_evaluated(evaluated_chunk_of(
                &schema,
                named_metric(name, job),
                start,
                values,
                is_last,
            ))
            .unwrap();
        while execution.next_chunk().unwrap().is_some() {}
        let (folded, walked) = execution.state().emitted_charge_pair(&NodePath::root());
        assert!(folded > 0, "the node handed out nothing to charge for");
        assert_eq!(
            folded, walked,
            "the folded charge of {folded} bytes is not the {walked} bytes the table holds"
        );
    }
}

/// A series that produced values under a label set, followed by one that produced none under the
/// same label set, is not a collision: the second contributes no sample, so the answer is just the
/// first one's values.
///
/// This is the other order of [`two_series_with_no_result_do_not_collide`], and the one that used to
/// fail: by the time the empty series arrives the label set is already closed, and refusing it
/// failed an execution whose answer was never in doubt.
#[test]
fn a_series_with_no_result_after_a_closed_one_is_passed_over() {
    let map_grid = grid(0, 60_000, 1);
    let tree = NativeTree::try_new(map(
        evaluated_input(Arc::clone(&map_grid)),
        vec![MathOp::Abs],
    ))
    .unwrap();
    let schema = evaluated_schema(Arc::clone(&map_grid));
    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());

    // `abs` drops the name, so both series arrive under the same label set. The first holds a
    // value; the second holds none at all and has to be passed over rather than refused.
    for (name, values) in [("a", vec![Some(-7.0)]), ("b", vec![None])] {
        execution
            .push_evaluated(evaluated_chunk_of(
                &schema,
                named_metric(name, "x"),
                0,
                values,
                true,
            ))
            .unwrap();
    }

    let mut handed = Vec::new();
    loop {
        match execution.next_chunk() {
            Ok(Some(chunk)) => handed.push(decided(&[chunk])),
            Ok(None) => break,
            Err(error) => panic!("an empty series after a closed one was refused: {error}"),
        }
    }
    assert!(
        handed
            .iter()
            .flatten()
            .any(|(_, value)| *value == Some(7.0)),
        "the first series' value is missing: {handed:?}"
    );
}

/// Two series that each produce no value at all are not a collision: both answer empty, and the
/// node hands nothing on rather than refusing the second one.
///
/// `rate` over a five-minute window with one sample in it has no result, in the single-point and
/// the batch kernel alike. Both series then arrive as a closing chunk of missing values under the
/// same label set -- `rate` drops the name -- but neither holds a sample, so there is nothing to
/// report twice and nothing to merge.
#[test]
fn two_series_with_no_result_do_not_collide() {
    let out = grid(0, 60_000, 1);
    let tree = NativeTree::try_new(window_with(
        input(),
        RangeFunction::Rate,
        300_000,
        None,
        Arc::clone(&out),
    ))
    .unwrap();
    let schema = raw_schema();
    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());

    // One sample each: rate over a 5m window with a single sample has no result.
    for name in ["a", "b"] {
        execution
            .push_raw(raw_chunk_of(
                &schema,
                name.as_bytes(),
                MetricName::try_new(Some(name.into()), vec![]).unwrap(),
                &[0],
                &[1.0],
                true,
            ))
            .unwrap();
    }
    let mut handed = Vec::new();
    loop {
        match execution.next_chunk() {
            Ok(Some(chunk)) => handed.push(decided(&[chunk])),
            Ok(None) => break,
            Err(error) => panic!("two empty series were refused: {error}"),
        }
    }
    assert!(
        handed.iter().flatten().all(|(_, value)| value.is_none()),
        "a series with no result handed a value on: {handed:?}"
    );
}

/// A chunk of `metric` whose rows are the `spans` of `schema`'s grid, with `values` row by row.
///
/// This is what a producer that decided only some positions hands over: the rows are absolute grid
/// positions and need not be adjacent, so a consumer that reads them as `start + row` names
/// positions the chunk does not hold.
fn sparse_chunk(
    schema: &SeriesSchema,
    metric: MetricName,
    spans: &[(usize, usize)],
    values: Vec<f64>,
    is_last: bool,
) -> EvaluatedChunk {
    let samples = Samples::from_float(Float64Array::from(values));
    let time = TimeAxis::from_grid_spans(
        Arc::clone(schema.eval_grid.as_ref().unwrap()),
        spans
            .iter()
            .map(|&(start, len)| GridSpan::new(start, len))
            .collect(),
    )
    .unwrap();
    let series = Series {
        metric,
        time,
        fields: SeriesFields::try_from_vec(vec![samples]).unwrap(),
    };
    EvaluatedChunk::try_new(series, is_last).unwrap()
}

#[test]
fn a_sparse_chunk_is_read_at_the_positions_it_holds_through_window_map_window() {
    // An inner series that decided positions 0, 1 and 5, 6 of an eight-point grid and left the
    // positions between them empty: two runs, not six rows with holes.
    let inner_grid = grid(0, 60_000, 8);
    let middle_grid = grid(0, 60_000, 8);
    let outer_grid = grid(0, 120_000, 4);
    let tree = NativeTree::try_new(window_with(
        map(
            window_with(
                evaluated_input(Arc::clone(&inner_grid)),
                RangeFunction::LastOverTime,
                // One step exactly, so the window is the identity: each point reads the sample at
                // its own position and nothing else.
                60_000,
                Some(Arc::clone(&inner_grid)),
                Arc::clone(&middle_grid),
            ),
            vec![MathOp::Abs],
        ),
        RangeFunction::LastOverTime,
        240_000,
        Some(Arc::clone(&middle_grid)),
        Arc::clone(&outer_grid),
    ))
    .unwrap();
    let schema = evaluated_schema(Arc::clone(&inner_grid));
    let chunk = sparse_chunk(
        &schema,
        metric(),
        &[(0, 2), (5, 2)],
        vec![1.0, 2.0, 3.0, 4.0],
        true,
    );

    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    execution.push_evaluated(chunk).unwrap();
    let out = run(&mut execution);

    // The outer window's points: every one of them has a sample in its range, so every one holds
    // the last value its window saw. 4.0 sits at 360 s, inside the window that ends there, which
    // is why the sparse tail is not lost between the runs.
    assert_eq!(
        decided(&out),
        vec![
            (0, Some(1.0)),
            (1, Some(2.0)),
            (2, Some(2.0)),
            (3, Some(4.0)),
        ]
    );
    assert_eq!(out.iter().filter(|chunk| chunk.is_last).count(), 1);
}

#[test]
fn a_sparse_chunk_occupies_only_the_positions_it_holds() {
    let map_grid = grid(0, 60_000, 8);
    let tree = NativeTree::try_new(map(
        evaluated_input_with_ordering(Arc::clone(&map_grid), SeriesOrdering::Interleaved),
        vec![MathOp::Abs],
    ))
    .expect("a map-only tree is legal over an interleaved input");
    let schema = evaluated_schema(Arc::clone(&map_grid));

    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    // Series a holds positions 0, 1 and 5, 6: two runs with a hole in the middle, not one run
    // covering the hole.
    execution
        .push_evaluated(sparse_chunk(
            &schema,
            named_metric("a", "x"),
            &[(0, 2), (5, 2)],
            vec![-1.0, -2.0, -3.0, -4.0],
            false,
        ))
        .unwrap();
    execution
        .next_chunk()
        .unwrap()
        .expect("the first open chunk is handed on as it arrives");

    // Position 2 sits in the hole, so series b occupies it and collides with nothing a holds.
    execution
        .push_evaluated(sparse_chunk(
            &schema,
            named_metric("b", "x"),
            &[(2, 1)],
            vec![-5.0],
            true,
        ))
        .unwrap();
    execution
        .next_chunk()
        .unwrap_or_else(|error| panic!("the series in the hole was refused: {error}"))
        .expect("the second series is handed on");
}

/// What an operation that empties every position hands on: nothing while the series is open, and
/// one empty chunk when it closes.
///
/// The two are not interchangeable. A non-closing empty chunk is rejected by a downstream evaluator
/// (an empty delivery inside a series decides nothing), and dropping the closing one would leave
/// the consumer unable to tell "nothing more here" from "no more input", so the series would never
/// close and `finish` would report an input that ended mid-series.
#[test]
fn an_emptying_operation_hands_on_nothing_until_it_closes_the_series() {
    let inner_grid = grid(0, 60_000, 4);
    let outer_grid = grid(0, 120_000, 2);
    // `clamp` with `max < min` empties every field of every chunk it sees.
    let emptying = vec![MathOp::Clamp { min: 1.0, max: 0.0 }];
    let tree = NativeTree::try_new(window_with(
        map(evaluated_input(Arc::clone(&inner_grid)), emptying.clone()),
        RangeFunction::LastOverTime,
        120_000,
        Some(Arc::clone(&inner_grid)),
        Arc::clone(&outer_grid),
    ))
    .unwrap();
    let schema = evaluated_schema(Arc::clone(&inner_grid));

    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    // An open chunk that maps to nothing yields nothing at all: it is not handed to the window,
    // which would refuse an empty delivery inside a series.
    execution
        .push_evaluated(evaluated_chunk(&schema, 0, vec![5.0, 6.0], false))
        .unwrap();
    assert!(
        execution.next_chunk().unwrap().is_none(),
        "an emptying map handed on a chunk for a series still open"
    );

    // The closing chunk still closes: one empty chunk reaches the window and closes the series.
    execution
        .push_evaluated(evaluated_chunk(&schema, 2, vec![7.0, 8.0], true))
        .unwrap();
    let mut out = Vec::new();
    while let Some(chunk) = execution.next_chunk().unwrap() {
        out.push(chunk);
    }
    out.extend(execution.finish().expect("the window closed its series"));

    assert_eq!(
        out.len(),
        1,
        "an emptying series handed on more than the one chunk that closes it"
    );
    assert!(
        out.iter().all(|chunk| chunk.series.time.is_empty()),
        "the outer window produced a value for a series that held none"
    );
    assert_eq!(
        out.iter().filter(|chunk| chunk.is_last).count(),
        1,
        "the series closed a number of times other than once"
    );
    assert!(out.last().unwrap().is_last);
}

/// A window over native histograms reserves what it retains, payload included, and the label-set
/// ledger it charges beside that agrees with a full walk of the table.
///
/// The chunk accounting walks the samples a chunk holds instead of charging one float per field and
/// row, so a histogram field -- which holds no float at all -- reaches both figures as the table its
/// indices point into. Neither may drift from what the node says it holds: the reservation is the
/// retained bytes plus the emitted ones, and the running total of the emitted table is the same
/// figure recounted in full.
#[test]
fn a_window_over_histograms_reserves_what_it_retains() {
    let out = grid(0, 10_000, 2);
    let tree = NativeTree::try_new(window_with(
        input(),
        RangeFunction::LastOverTime,
        20_000,
        None,
        Arc::clone(&out),
    ))
    .unwrap();
    let schema = raw_schema();
    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());

    for (timestamp, is_last) in [(0, false), (10_000, true)] {
        execution
            .push_raw(raw_histogram_chunk(&schema, &[timestamp], 2, is_last))
            .unwrap();
        while execution.next_chunk().unwrap().is_some() {}
        let (retained, emitted) = execution.state().window_charge(&NodePath::root());
        assert!(
            emitted > 0,
            "the window handed nothing out, so the ledger below charges for nothing"
        );
        assert_eq!(
            execution.state().reserved_bytes(),
            retained.saturating_add(emitted),
            "the reservation is not what the window retains ({retained} bytes) plus what it handed \
             out ({emitted} bytes)",
        );
        let (folded, walked) = execution.state().emitted_charge_pair(&NodePath::root());
        assert_eq!(
            folded, walked,
            "the folded charge of {folded} bytes is not the {walked} bytes the table holds",
        );
    }
}

/// One histogram table is charged once, however many chunks of the series reference it.
///
/// The chunks of one series can share the immutable table their histogram indices point into, and a
/// node holding both of them holds one table: the source charges its queue as one accounting pass,
/// which counts the table the first time it meets it and the address the `Arc` holds is what makes
/// two clones one table.
#[test]
fn a_histogram_table_is_charged_once_per_pass() {
    let schema = raw_schema();
    let mut builder = HistogramBatchBuilder::new();
    let index = push_histogram(&mut builder, 2);
    let table = builder.finish().unwrap();
    let chunk = |timestamp: i64, is_last: bool| {
        let samples =
            Samples::try_from_histogram(UInt32Array::from(vec![Some(index)]), Arc::clone(&table))
                .unwrap();
        RawSeriesChunk::try_new(
            &schema,
            Buffer::from_slice_ref(b"series-a"),
            Some(metric()),
            RawTimestamps::try_new(ScalarBuffer::from(vec![timestamp])).unwrap(),
            SeriesFields::one(samples),
            is_last,
        )
        .unwrap()
    };
    let (first, second) = (chunk(0, false), chunk(10_000, true));

    let mut charged = ChargedAllocations::default();
    let queue = raw_bytes(&first, &mut charged).saturating_add(raw_bytes(&second, &mut charged));
    let alone = raw_bytes(&first, &mut ChargedAllocations::default())
        .saturating_add(raw_bytes(&second, &mut ChargedAllocations::default()));
    assert!(
        queue < alone,
        "a queue of two chunks sharing one table is charged {queue} bytes, no less than the \
         {alone} bytes they are charged apart: the table was counted twice",
    );

    // What the source reserves is exactly that one pass: it holds its queue and nothing else, and
    // nothing has been pulled yet.
    let tree = NativeTree::try_new(window(input(), grid(0, 10_000, 1))).unwrap();
    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    execution.push_raw(first).unwrap();
    execution.push_raw(second).unwrap();
    assert_eq!(
        execution.state().reserved_bytes(),
        queue,
        "the queue of two chunks sharing one table was not charged as one pass",
    );
}

/// A queue of two chunks that share one table charges it once, and the running total is what one
/// pass over the queue counts.
///
/// The figure the node reserves while both chunks are queued is the incremental one: the table is
/// met by the first chunk that references it and is not charged again by the second. Charging it
/// per chunk fails the record check, and recounting the queue per enqueue fails the count of
/// chunks walked in the test below.
#[test]
fn a_queued_allocation_is_charged_once() {
    let pool = pool();
    let mut source = source_runtime(&pool);
    let schema = raw_schema();
    let mut builder = HistogramBatchBuilder::new();
    let index = push_histogram(&mut builder, 2);
    let table = builder.finish().unwrap();
    let chunk = |timestamp: i64, is_last: bool| {
        raw_shared_histogram_chunk(&schema, &table, index, timestamp, is_last)
    };
    let (first, second) = (chunk(0, false), chunk(10_000, true));

    // What the two chunks cost charged apart, and what the second one costs on its own: the
    // difference between the two is what the shared table is worth.
    let apart = raw_bytes(&first, &mut ChargedAllocations::default())
        .saturating_add(raw_bytes(&second, &mut ChargedAllocations::default()));
    let second_alone = raw_bytes(&second, &mut ChargedAllocations::default());

    source.enqueue(NativeChunk::Raw(first)).unwrap();
    let one = source.queued_charge.bytes;
    assert_eq!(
        one,
        source.queued_bytes_recomputed(),
        "one queued chunk is not charged what one pass over it counts"
    );
    assert_eq!(
        source.buffer.held, one,
        "the reservation is not the queue's figure"
    );
    assert_eq!(pool.reserved(), one);

    source.enqueue(NativeChunk::Raw(second)).unwrap();
    let two = source.queued_charge.bytes;
    let counted = *source
        .queued_charge
        .tables
        .get(&(Arc::as_ptr(&table) as usize))
        .expect("the queue did not count the table both chunks index into");
    assert_eq!(
        counted.chunks, 2,
        "two queued chunks reference the table, but the queue counts {}",
        counted.chunks
    );
    assert_eq!(
        two,
        source.queued_bytes_recomputed(),
        "the running total is not what one pass over the queue counts"
    );
    assert_eq!(
        two - one,
        second_alone - counted.bytes,
        "the chunk that entered charged the shared table a second time"
    );
    assert!(
        two < apart,
        "a queue of two chunks sharing one table is charged {two} bytes, no less than the {apart} \
         bytes they are charged apart: the table was counted twice",
    );
    assert_eq!(source.buffer.held, two);
    assert_eq!(pool.reserved(), two);
}

/// A shared table is given back with the last queued chunk that references it, not the first.
///
/// A leak here shows as a reservation that never returns: the first pop takes one reference away
/// and the table stays charged, the second takes the last one and the table goes with it.
#[test]
fn a_queued_allocation_is_given_back_with_its_last_chunk() {
    let pool = pool();
    let mut source = source_runtime(&pool);
    let schema = raw_schema();
    let mut builder = HistogramBatchBuilder::new();
    let index = push_histogram(&mut builder, 2);
    let table = builder.finish().unwrap();
    let address = Arc::as_ptr(&table) as usize;
    let chunk = |timestamp: i64, is_last: bool| {
        raw_shared_histogram_chunk(&schema, &table, index, timestamp, is_last)
    };

    source.enqueue(NativeChunk::Raw(chunk(0, false))).unwrap();
    source
        .enqueue(NativeChunk::Raw(chunk(10_000, true)))
        .unwrap();
    assert_eq!(source.queued_charge.tables[&address].chunks, 2);

    let first = source
        .pop_source()
        .unwrap()
        .expect("the queue held two chunks");
    assert!(!first.is_last());
    assert_eq!(
        source.queued_charge.tables[&address].chunks, 1,
        "the chunk left in the queue still references the table, so it is still charged"
    );
    assert!(
        source.queued_charge.bytes > 0,
        "the queue still holds a chunk, but charges for nothing"
    );
    assert_eq!(
        source.queued_charge.bytes,
        source.queued_bytes_recomputed(),
        "the pop did not leave the running total at one pass over the queue"
    );
    assert_eq!(source.buffer.held, source.queued_charge.bytes);

    source
        .pop_source()
        .unwrap()
        .expect("the queue held two chunks");
    assert!(
        !source.queued_charge.tables.contains_key(&address),
        "the last chunk referencing the table left, but the queue still counts it"
    );
    assert!(
        source.queued_charge.buffers.is_empty(),
        "a buffer outlived the chunks that referenced it"
    );
    assert_eq!(
        source.queued_charge.bytes, 0,
        "the queue is empty, but it is still charged for"
    );
    assert_eq!(
        source.buffer.held, 0,
        "the reservation did not come back with the queue"
    );
    assert_eq!(pool.reserved(), 0);
    assert!(source.pop_source().unwrap().is_none());
}

/// Queueing a chunk accounts for that chunk alone, however deep the queue is.
///
/// The figure is the number of chunks the accounting walked, not wall time: `N` enqueues have to
/// walk `N` chunks and `N` pops another `N`, whatever the queue held. Recounting the queue per
/// operation -- the defect this refuses -- walks `N * (N + 1) / 2` chunks for the enqueues alone.
#[test]
fn queueing_a_chunk_walks_one_chunk() {
    let pool = pool();
    let mut source = source_runtime(&pool);
    let schema = raw_schema();
    let chunks = 64;
    for index in 0..chunks {
        let timestamp = i64::try_from(index).unwrap() * 10_000;
        let chunk = raw_chunk(&schema, &[timestamp], &[1.0], index + 1 == chunks);
        source.enqueue(NativeChunk::Raw(chunk)).unwrap();
    }
    assert_eq!(
        source.queued_charge.walked, chunks,
        "queueing {chunks} chunks walked {} chunk(s): the figure is recounted from the queue",
        source.queued_charge.walked,
    );
    assert_eq!(
        source.queued_charge.bytes,
        source.queued_bytes_recomputed(),
        "the running total is not what one pass over the queue counts"
    );

    let mut popped = 0;
    while source.pop_source().unwrap().is_some() {
        popped += 1;
    }
    assert_eq!(popped, chunks, "the queue did not hand back every chunk");
    assert_eq!(
        source.queued_charge.walked,
        2 * chunks,
        "popping {chunks} chunks walked {} chunk(s): the figure is recounted from the queue",
        source.queued_charge.walked,
    );
    assert_eq!(source.queued_charge.bytes, 0);
}

/// A mixed sequence of pushes and pulls leaves the queue's figure equal to one pass over what is
/// left in it, and the reservation equal to that figure.
///
/// The chunks mix ones that share a table with ones that share nothing, so an allocation is
/// referenced by one, two or no queued chunk as the sequence runs. Both the node's own reservation
/// and the pool's are checked, so a total that drifts from the queue is caught by the figure the
/// memory pool actually holds.
#[test]
fn a_queue_figure_survives_a_mixed_push_and_pull_sequence() {
    let pool = pool();
    let mut source = source_runtime(&pool);
    let schema = raw_schema();
    let mut builder = HistogramBatchBuilder::new();
    let index = push_histogram(&mut builder, 2);
    let table = builder.finish().unwrap();

    for round in 0..6i64 {
        for step in 0..2 {
            let chunk =
                raw_shared_histogram_chunk(&schema, &table, index, round * 10_000 + step, false);
            source.enqueue(NativeChunk::Raw(chunk)).unwrap();
        }
        let chunk = raw_chunk(&schema, &[round * 10_000 + 2], &[1.0], false);
        source.enqueue(NativeChunk::Raw(chunk)).unwrap();
        let folded = source.queued_charge.bytes;
        assert_eq!(
            folded,
            source.queued_bytes_recomputed(),
            "round {round}: the running total is not one pass over the queue"
        );
        assert_eq!(source.buffer.held, folded);
        assert_eq!(pool.reserved(), folded);

        for _ in 0..2 {
            if source.pop_source().unwrap().is_none() {
                break;
            }
        }
        let folded = source.queued_charge.bytes;
        assert_eq!(
            folded,
            source.queued_bytes_recomputed(),
            "round {round}: the running total is not one pass over what is left"
        );
        assert_eq!(source.buffer.held, folded);
        assert_eq!(pool.reserved(), folded);
    }

    while source.pop_source().unwrap().is_some() {}
    assert_eq!(source.queued_charge.bytes, 0);
    assert!(
        source.queued_charge.tables.is_empty(),
        "a table outlived the chunks that referenced it"
    );
    assert!(
        source.queued_charge.buffers.is_empty(),
        "a buffer outlived the chunks that referenced it"
    );
    assert_eq!(source.buffer.held, 0);
    assert_eq!(pool.reserved(), 0);
}

/// What a source reserves through the executor is the figure one pass over its queue counts.
#[test]
fn the_executor_reserves_what_one_pass_over_the_queue_counts() {
    let tree = NativeTree::try_new(window(input(), grid(0, 10_000, 4))).unwrap();
    let schema = raw_schema();
    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    for step in 0..3 {
        let chunk = raw_chunk(&schema, &[step * 10_000], &[1.0], false);
        execution.push_raw(chunk).unwrap();
    }
    let (folded, walked) = execution
        .state()
        .queued_charge_pair(&NodePath::root().input());
    assert_eq!(
        folded, walked,
        "the executor's queue is not charged what one pass over it counts"
    );
    assert_eq!(
        execution.state().reserved_bytes(),
        folded,
        "the source's reservation is not its queue's figure"
    );
}

/// Releasing a node drops the queue's counts with the queue and returns the reservation once.
///
/// A count left behind would be folded onto by the next chunk the node queues, and the reservation
/// shown as returned while the counts still hold the bytes is the leak this refuses.
#[test]
fn releasing_a_node_drops_the_queue_and_its_counts() {
    let pool = pool();
    let mut source = source_runtime(&pool);
    let schema = raw_schema();
    let mut builder = HistogramBatchBuilder::new();
    let index = push_histogram(&mut builder, 2);
    let table = builder.finish().unwrap();
    source
        .enqueue(NativeChunk::Raw(raw_shared_histogram_chunk(
            &schema, &table, index, 0, false,
        )))
        .unwrap();
    source
        .enqueue(NativeChunk::Raw(raw_shared_histogram_chunk(
            &schema, &table, index, 10_000, true,
        )))
        .unwrap();
    assert_eq!(pool.reserved(), source.queued_charge.bytes);

    source.release();
    assert!(source.queued.is_empty(), "the queue outlived its release");
    assert!(
        source.queued_charge.tables.is_empty(),
        "the queue's table counts outlived the queue"
    );
    assert!(
        source.queued_charge.buffers.is_empty(),
        "the queue's buffer counts outlived the queue"
    );
    assert_eq!(source.queued_charge.bytes, 0);
    assert_eq!(source.buffer.held, 0);
    assert_eq!(pool.reserved(), 0, "the reservation was not returned");
    assert!(source.pop_source().unwrap().is_none());
}

/// One entry's figure is the heap it owns, and its `History` header is charged once, by its slot.
///
/// The `History` of a label set lives in the map's slot, so a `size_of::<History>()` term in the
/// entry's own figure billed one header twice: once here and once in the capacity term beside it.
/// The two assertions below are the definition of the whole figure -- the labels, the vectors and
/// their runs, and one label-set slot plus one history slot per entry -- so either header counted
/// in the wrong place fails them.
#[test]
fn a_history_entry_is_charged_once_for_its_header() {
    let grid = grid(0, 10_000, 2);
    let schema = evaluated_schema(Arc::clone(&grid));
    let metric = named_metric("requests", "job");
    let labels = label_set(&metric);
    // Not the closing chunk: a closed label set drops its runs, and the runs are what the entry's
    // own figure is.
    let chunk = evaluated_chunk_of(&schema, metric, 0, vec![Some(1.0), Some(2.0)], false);

    let mut emitted = Emitted::default();
    emitted.occupy(&NodePath::root(), &chunk).unwrap();

    let history = emitted
        .sets
        .get(&labels)
        .expect("the chunk recorded its label set");
    let entry = label_bytes(&labels)
        .saturating_add(history.fields.capacity() * std::mem::size_of::<Vec<Run>>())
        .saturating_add(
            history
                .fields
                .iter()
                .map(|runs| runs.capacity() * std::mem::size_of::<Run>())
                .sum::<usize>(),
        );
    assert!(
        history.fields.iter().any(|runs| !runs.is_empty()),
        "the entry kept no run, so it has no heap of its own to charge for"
    );
    assert_eq!(
        emitted.entries,
        entry,
        "the entry's own bytes are not its labels and runs alone: a `History` header in the figure \
         would add the {} bytes its slot already charges",
        std::mem::size_of::<History>(),
    );
    assert_eq!(
        emitted.bytes(),
        entry.saturating_add(
            emitted.sets.capacity()
                * (std::mem::size_of::<LabelSet>() + std::mem::size_of::<History>())
        ),
        "the table's capacity is where a label-set slot and one `History` per entry are charged",
    );
}
