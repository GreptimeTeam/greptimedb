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
    Emitted, NativeChunk, NativeExecutionState, NativeExecutor, NodeBuffer, NodeRuntime,
    absorb_rebuilds, evaluated_bytes, map_chunk, raw_bytes,
};
use crate::evaluated::EvaluatedChunk;
use crate::grid_series::merge_same_labelset;
use crate::map::MathOp;
use crate::native::metrics::{NodeMetric, NodeMetrics, NodePath};
use crate::native::plan::{
    AggregateKind, AggregateStage, BoundAggregate, BoundInput, BoundInstant, BoundMap, BoundScalar,
    BoundWindow, Grouping, NativeExpr, NativeNode, NativeTree, NativeType,
};
use crate::range_eval::{RangeEvaluator, RangeFunction, RangeWindow};
use crate::vector_match::{BinaryMatching, BinaryOp, Cardinality, FillValues, MatchingLabels};

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

/// A source node's runtime: the queue and the reservation it is held for.
fn source_runtime(pool: &Arc<dyn MemoryPool>) -> NodeRuntime {
    NodeRuntime {
        buffer: NodeBuffer::new(
            &NodePath::root().input(),
            Arc::new(NodeMetrics::default()),
            pool,
        ),
        queued: VecDeque::new(),
        window: None,
        map: None,
        emitted: None,
        merge: None,
        open_source_series: false,
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
                    fill: FillValues::default(),
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
/// outer one two of its own — so a single total for the tree cannot produce both numbers. The inner
/// window reads the source as it arrives and hands over one chunk per chunk it took in; the outer
/// one collects those chunks, because the inner one keeps the metric name and its output can hold
/// two source series under one label set, and reports the one merged series its input ended with.
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
    // The outer window reports the two points of its grid once its input ended, in the one chunk of
    // the series it merged from the inner window's pieces.
    assert_eq!(addressed(&out), vec![(0, vec![Some(1.0), Some(3.0)])]);
    assert!(out.last().unwrap().is_last);

    let outer = NodePath::root();
    let inner = outer.input();
    let source = inner.input();

    // Three source chunks reach the inner window, which hands one chunk on per chunk it took in.
    // The outer window takes those three pieces in and reports the one series they merge into.
    assert_eq!(count(&set, &source, NodeMetric::INPUT_BATCHES), 3);
    assert_eq!(count(&set, &source, NodeMetric::OUTPUT_BATCHES), 3);
    assert_eq!(count(&set, &inner, NodeMetric::INPUT_BATCHES), 3);
    assert_eq!(count(&set, &outer, NodeMetric::INPUT_BATCHES), 3);
    assert_eq!(count(&set, &inner, NodeMetric::OUTPUT_BATCHES), 3);
    assert_eq!(count(&set, &outer, NodeMetric::OUTPUT_BATCHES), 1);
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

/// A chunk the pool cannot admit fails the execution and gives back everything it already held.
///
/// The window holds the first chunk's samples, so the second one is refused against a reservation
/// that is already standing: the failure has to hand that reservation back too, rather than leaving
/// the pool full for as long as the caller keeps the stopped executor alive.
#[test]
fn a_pool_that_cannot_admit_a_chunk_releases_what_it_held() {
    let tree = NativeTree::try_new(window(input(), grid(0, 60_000, 4))).unwrap();
    let schema = raw_schema();
    let set = ExecutionPlanMetricsSet::new();
    // Room for the first chunk and what the window keeps of it, not for the second one.
    let pool = bounded_pool(16 << 10);
    let mut execution = executor(&tree, Arc::clone(&pool), &set);

    execution
        .push_raw(raw_chunk(&schema, &[0], &[1.0], false))
        .unwrap();
    drop(execution.next_chunk().unwrap());
    let held = pool.reserved();
    assert!(held > 0, "the open series has to be reserved for");

    // A hundred thousand samples are hundreds of kilobytes whatever the pool already holds.
    let timestamps: Vec<i64> = (0..100_000).collect();
    let values = vec![1.0; 100_000];
    let error = execution
        .push_raw(raw_chunk(&schema, &timestamps, &values, true))
        .expect_err("a chunk larger than the pool was admitted");
    assert!(
        matches!(error, DataFusionError::ResourcesExhausted(_)),
        "{error}"
    );

    // The chunk never entered the queue, and everything the execution held went back to the pool
    // while the executor is still alive.
    assert_eq!(execution.state().reserved_bytes(), 0);
    assert_eq!(pool.reserved(), 0);
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
}

/// The malformed streams a streaming window refuses are refused by the collector too.
///
/// The window below the map collects the whole fragment stream and hands it to
/// [`crate::grid_series::merge_same_labelset`], which sorts the rows by position and groups them by
/// label set. That is what makes two closed series that share a label set one series, and it is also
/// what would hide a stream that never was one series: runs that went backwards inside a series, and
/// an identity that changed inside a series, both come out as a plausible merged series with a
/// peaceful answer. A window that read the same stream as it arrived refuses it on the chunk that
/// breaks it, and so does the collector, before the chunk is kept.
///
/// The stream below is a single series per case and the answer it would have produced is the one
/// nothing asked for: `3` for the first (the merge would sort 1 and 2 onto positions 0 and 1), and
/// two answers for the second.
#[test]
fn a_collector_refuses_the_malformed_streams_a_streaming_window_refuses() {
    let inner_grid = grid(0, 60_000, 2);
    let outer_grid = grid(60_000, 60_000, 1);
    let schema = evaluated_schema(Arc::clone(&inner_grid));
    let tree = || {
        NativeTree::try_new(window_with(
            map(evaluated_input(Arc::clone(&inner_grid)), vec![MathOp::Abs]),
            RangeFunction::SumOverTime,
            120_000,
            Some(Arc::clone(&inner_grid)),
            Arc::clone(&outer_grid),
        ))
        .unwrap()
    };

    let cases = [
        (
            "runs going backwards inside one series",
            evaluated_chunk_of(&schema, named_metric("a", "x"), 1, vec![Some(1.0)], false),
            evaluated_chunk_of(&schema, named_metric("a", "x"), 0, vec![Some(2.0)], true),
            "not increasing",
        ),
        (
            "an identity changing inside one series",
            evaluated_chunk_of(&schema, named_metric("a", "x"), 0, vec![Some(1.0)], false),
            evaluated_chunk_of(&schema, named_metric("b", "y"), 1, vec![Some(2.0)], true),
            "changed identity",
        ),
    ];
    for (what, first, second, phrase) in cases {
        let pool = pool();
        let mut execution = executor(&tree(), Arc::clone(&pool), &ExecutionPlanMetricsSet::new());
        // The chunk that only opens the series is what a stream hands over first, and it is legal:
        // the collector keeps it and reports nothing.
        execution.push_evaluated(first).unwrap();
        assert!(
            execution.next_chunk().unwrap().is_none(),
            "{what}: the collector reported a piece before its input ended"
        );
        execution.push_evaluated(second).unwrap();
        let error = match execution.next_chunk() {
            Ok(None) => panic!("{what} was collected as if it were one series"),
            Ok(Some(chunk)) => panic!("{what} reached the evaluator as {:?}", decided(&[chunk])),
            Err(error) => error,
        };
        let message = error.to_string();
        assert!(
            message.contains(phrase),
            "{what} was refused for another reason: {message}"
        );
        // The failure released everything the chain held while the executor is still alive.
        assert_eq!(execution.state().reserved_bytes(), 0);
        assert_eq!(pool.reserved(), 0);
        assert_eq!(
            execution.state().nodes_holding_windows(),
            0,
            "{what}: the stopped execution still holds a window's samples"
        );
        // A stopped execution is not resumed.
        let again = execution.next_chunk().unwrap_err();
        assert!(
            again.to_string().contains("stopped after an earlier error"),
            "{again}"
        );
    }
}

/// The streams a streaming window accepts are accepted by the collector too.
///
/// One series arriving in chunks that move forward is what a scan hands over, and the chunk that
/// closes a series may carry no row at all. Both are legal to a streaming window, so both are legal
/// to the collector, which merges them into the one series they are and answers over it. The values
/// are the ones the audit's counterexamples would have produced had they been legal: the two forward
/// chunks hold 1 and 3 and sum to 4, and the close that carries no row leaves the 1 it follows.
#[test]
fn a_collector_accepts_forward_runs_and_a_close_without_a_row() {
    let inner_grid = grid(0, 60_000, 2);
    let outer_grid = grid(60_000, 60_000, 1);
    let schema = evaluated_schema(Arc::clone(&inner_grid));
    let tree = || {
        NativeTree::try_new(window_with(
            map(evaluated_input(Arc::clone(&inner_grid)), vec![MathOp::Abs]),
            RangeFunction::SumOverTime,
            120_000,
            Some(Arc::clone(&inner_grid)),
            Arc::clone(&outer_grid),
        ))
        .unwrap()
    };

    let cases = [
        (
            "two chunks moving forward",
            vec![
                evaluated_chunk_of(&schema, named_metric("a", "x"), 0, vec![Some(-1.0)], false),
                evaluated_chunk_of(&schema, named_metric("a", "x"), 1, vec![Some(-3.0)], true),
            ],
            vec![(0, Some(4.0))],
        ),
        (
            "a close that carries no row",
            vec![
                evaluated_chunk_of(&schema, named_metric("a", "x"), 0, vec![Some(-1.0)], false),
                evaluated_chunk_of(&schema, named_metric("a", "x"), 2, Vec::new(), true),
            ],
            vec![(0, Some(1.0))],
        ),
    ];
    for (what, chunks, expected) in cases {
        let mut execution = executor(&tree(), pool(), &ExecutionPlanMetricsSet::new());
        for chunk in chunks {
            execution.push_evaluated(chunk).unwrap();
            assert!(
                execution.next_chunk().unwrap().is_none(),
                "{what}: the collector reported a piece before its input ended"
            );
        }
        let out = execution
            .finish()
            .unwrap_or_else(|error| panic!("{what} was refused: {error}"));
        assert_eq!(decided(&out), expected, "{what}");
        assert_eq!(
            out.iter().filter(|chunk| chunk.is_last).count(),
            1,
            "{what}: the one merged series closed more than once"
        );
    }
}

/// An input that ends inside a series is reported by a window that collects its input too.
///
/// The collector holds the fragments until the input ends and closes every series it merges with a
/// synthetic `is_last`, so a series the caller never closed would look exactly like one that closed
/// afterwards: the merge would answer it and the barrier would hide the truncation. Nothing is
/// merged here, because the executor refuses the truncation before the flush runs. The check is the
/// input node's own, which follows what it handed over, and it is the only one needed: every closing
/// chunk the caller sent has already reached the collector, which wrote down the series it left open.
///
/// A root map reports the same truncation, and
/// [`an_input_that_ends_mid_series_is_reported_without_a_window`] is that case on its own; this one
/// is the barrier's, where the flush would have run before the report if the check came later.
#[test]
fn an_input_that_ends_inside_a_collected_series_is_reported() {
    let inner_grid = grid(0, 60_000, 2);
    let outer_grid = grid(60_000, 60_000, 1);
    let tree = NativeTree::try_new(window_with(
        map(evaluated_input(Arc::clone(&inner_grid)), vec![MathOp::Abs]),
        RangeFunction::SumOverTime,
        120_000,
        Some(Arc::clone(&inner_grid)),
        Arc::clone(&outer_grid),
    ))
    .unwrap();
    let schema = evaluated_schema(Arc::clone(&inner_grid));
    let pool = pool();
    let set = ExecutionPlanMetricsSet::new();
    let mut execution = executor(&tree, Arc::clone(&pool), &set);

    // One piece of a series that never says it ended: the collector keeps it and reports nothing.
    execution
        .push_evaluated(evaluated_chunk_of(
            &schema,
            named_metric("a", "x"),
            0,
            vec![Some(-1.0)],
            false,
        ))
        .unwrap();
    assert!(
        execution
            .next_chunk()
            .expect("the fragment is legal")
            .is_none(),
        "the collector reported a piece before its input ended"
    );
    assert!(
        execution.state().reserved_bytes() > 0,
        "the collected fragment is not charged for"
    );

    let error = execution
        .finish()
        .expect_err("an input that ended inside a collected series was accepted");
    assert!(
        error
            .to_string()
            .contains("ended before the last chunk of a series"),
        "the truncation was reported as {error}"
    );
    // Nothing was merged and nothing was answered, and everything went back while the executor is
    // alive.
    assert_eq!(execution.state().reserved_bytes(), 0);
    assert_eq!(pool.reserved(), 0);
    assert_eq!(
        execution.state().nodes_holding_windows(),
        0,
        "the stopped execution still holds a window's samples"
    );
    // The window's own gauge, which is the node that collected the fragment, reads zero.
    let current = NodePath::root().metric_name(NodeMetric::CURRENT_RESERVED_BYTES);
    assert_eq!(gauge(&set, &current), 0);
    // A stopped execution is not resumed.
    let again = execution.next_chunk().unwrap_err();
    assert!(
        again.to_string().contains("stopped after an earlier error"),
        "{again}"
    );
}

/// Two series that a map turns into one label set are merged before the window above reads them.
///
/// `a{job="x"}` and `b{job="x"}` arrive whole, so each says it is the last chunk of its own source
/// series, and `abs` drops the name, so both become `{job="x"}`. The two hold their values at
/// different positions, so they are one series with a hole each and the answer is the window over
/// the merged series: `sum_over_time` sees 1 and 3 and answers 4, `count_over_time` answers 2. A
/// window that read the two as two series would answer twice, once per source series, and close
/// twice.
///
/// The window above the map is the node that merges: its input can hold two series under one label
/// set, so it collects the whole input, merges each label set once with
/// [`crate::grid_series::merge_same_labelset`], and only then evaluates. Nothing comes out of it
/// while the pieces arrive, which is the price of merging; what comes out is one series.
#[test]
fn two_series_onto_one_label_set_after_a_map_are_merged_into_one_series() {
    let inner_grid = grid(0, 60_000, 2);
    let outer_grid = grid(60_000, 60_000, 1);
    let schema = evaluated_schema(Arc::clone(&inner_grid));
    // One map and two maps: the label set is rewritten at the first one either way, and the second
    // only rewrites it again, so both shapes are one affected window over one rewritten set.
    let shapes = |function: RangeFunction| {
        [
            (
                "a map",
                NativeTree::try_new(window_with(
                    map(evaluated_input(Arc::clone(&inner_grid)), vec![MathOp::Abs]),
                    function,
                    120_000,
                    Some(Arc::clone(&inner_grid)),
                    Arc::clone(&outer_grid),
                ))
                .unwrap(),
            ),
            (
                "two maps",
                NativeTree::try_new(window_with(
                    map(
                        map(evaluated_input(Arc::clone(&inner_grid)), vec![MathOp::Abs]),
                        vec![MathOp::Abs],
                    ),
                    function,
                    120_000,
                    Some(Arc::clone(&inner_grid)),
                    Arc::clone(&outer_grid),
                ))
                .unwrap(),
            ),
        ]
    };

    for (function, expected) in [
        // The two values at inner positions 0 and 1 both fall in the one window of the outer grid.
        (RangeFunction::SumOverTime, 4.0),
        (RangeFunction::CountOverTime, 2.0),
    ] {
        for (shape, tree) in shapes(function) {
            let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
            // Each source series is closed properly, and each holds its value where the other holds
            // none, so the pair is one logical series and one answer.
            execution
                .push_evaluated(evaluated_chunk_of(
                    &schema,
                    named_metric("a", "x"),
                    0,
                    vec![Some(1.0), None],
                    true,
                ))
                .unwrap();
            execution
                .push_evaluated(evaluated_chunk_of(
                    &schema,
                    named_metric("b", "x"),
                    0,
                    vec![None, Some(3.0)],
                    true,
                ))
                .unwrap();
            // Nothing settles while the pieces arrive: the merged series is what the window evaluates,
            // and it does not exist before the input ends.
            assert!(
                execution.next_chunk().unwrap().is_none(),
                "{shape} with {function:?} reported a piece it had not merged yet"
            );
            let out = execution.finish().expect("the merged series evaluates");
            assert_eq!(
                decided(&out),
                vec![(0, Some(expected))],
                "{shape} with {function:?} did not answer over the merged series"
            );
            assert_eq!(
                out.iter().filter(|chunk| chunk.is_last).count(),
                1,
                "{shape} with {function:?} closed the one merged series more than once"
            );
        }
    }
}

/// Two closed series that hold a value at the same position are still refused under one label set.
///
/// The pair below maps into one label set and both hold a value at position 0 of the field, which
/// no merge resolves and Prometheus rejects for two series whose label sets became equal. The
/// refusal is the map's own, before the window above evaluates anything, and it names the position
/// rather than a closed label set: a closed source series is not a closed label set.
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
    // The first series closed, and the window above collects it rather than reporting it.
    assert!(
        execution.next_chunk().unwrap().is_none(),
        "the window above the map reported a piece before its input ended"
    );
    execution
        .push_evaluated(evaluated_chunk_of(
            &schema,
            named_metric("b", "x"),
            0,
            vec![Some(3.0), Some(4.0)],
            true,
        ))
        .unwrap();
    let error = match execution.next_chunk() {
        Ok(None) => panic!("the second series was accepted at a position the first one holds"),
        Ok(Some(chunk)) => panic!(
            "two values at one position were handed on: {:?}",
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
        message.contains("position 0"),
        "the rejection does not name the shared position: {message}"
    );
}

/// An empty tag and an absent one are two source series and one canonical label set.
///
/// `requests{host="web-1", zone=""}` and `requests{host="web-1"}` are distinct series to a scan: an
/// empty tag value is a present label, an absent one is not. `last_over_time` keeps the metric name
/// and every tag, so the inner window hands both on as two pieces, each closing its own source
/// series, and the two pieces carry different metrics. The outer window is the node that merges
/// them: a consumer compares label sets without the empty value, so the two are one series there.
/// Merging first is what makes the outer `sum_over_time` answer 4 at the second point -- the two
/// pieces hold 1 at the first point and 3 at the second -- where evaluating each piece on its own
/// would answer 3.
#[test]
fn an_empty_tag_and_an_absent_one_are_one_label_set_to_the_window_above() {
    let inner_grid = grid(0, 60_000, 2);
    let outer_grid = grid(0, 60_000, 2);
    let inner = || {
        // A one-millisecond window on a 60 s grid, so every point reads only the sample at its own
        // timestamp and each source series decides one point.
        window_with(
            input(),
            RangeFunction::LastOverTime,
            1,
            None,
            Arc::clone(&inner_grid),
        )
    };
    let chain = || {
        window_with(
            inner(),
            RangeFunction::SumOverTime,
            120_000,
            Some(Arc::clone(&inner_grid)),
            Arc::clone(&outer_grid),
        )
    };
    let schema = raw_schema();
    let push_both = |execution: &mut NativeExecutor| {
        execution
            .push_raw(raw_chunk_of(
                &schema,
                b"series-a",
                host_metric(Some("")),
                &[0],
                &[1.0],
                true,
            ))
            .unwrap();
        execution
            .push_raw(raw_chunk_of(
                &schema,
                b"series-b",
                host_metric(None),
                &[60_000],
                &[3.0],
                true,
            ))
            .unwrap();
    };

    // The inner window alone hands both series on, and they are two series: the empty tag survives
    // `last_over_time`, which keeps the name and every tag.
    let inner_tree = NativeTree::try_new(inner()).unwrap();
    let mut inner_execution = executor(&inner_tree, pool(), &ExecutionPlanMetricsSet::new());
    push_both(&mut inner_execution);
    let pieces = run(&mut inner_execution);
    assert_eq!(
        pieces.len(),
        2,
        "the inner window did not hand both series on"
    );
    assert_ne!(
        pieces[0].series.metric, pieces[1].series.metric,
        "the two source series were not two series to `last_over_time`"
    );

    // The chain merges them before the outer window reads them.
    let tree = NativeTree::try_new(chain()).unwrap();
    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    push_both(&mut execution);
    let out = run(&mut execution);
    assert_eq!(
        decided(&out),
        vec![(0, Some(1.0)), (1, Some(4.0))],
        "the outer window did not answer over the merged label set"
    );
    assert_eq!(
        out.iter().filter(|chunk| chunk.is_last).count(),
        1,
        "the merged label set closed more than once"
    );
}

/// A window that collects its whole input gives every byte back when the execution closes.
///
/// The fragments it holds, the workspace it merges them with and the chunks it queued while
/// flushing are all charged while they live, so the pool never admits memory the node still owns.
/// The close releases all of them, and the figures read zero while the executor is still alive.
#[test]
fn a_window_that_merges_its_input_releases_what_it_collected() {
    let inner_grid = grid(0, 60_000, 2);
    let outer_grid = grid(60_000, 60_000, 1);
    let tree = NativeTree::try_new(window_with(
        map(evaluated_input(Arc::clone(&inner_grid)), vec![MathOp::Abs]),
        RangeFunction::SumOverTime,
        120_000,
        Some(Arc::clone(&inner_grid)),
        Arc::clone(&outer_grid),
    ))
    .unwrap();
    let schema = evaluated_schema(Arc::clone(&inner_grid));
    let set = ExecutionPlanMetricsSet::new();
    let pool = pool();
    let mut execution = executor(&tree, Arc::clone(&pool), &set);
    for (name, values) in [("a", vec![Some(1.0), None]), ("b", vec![None, Some(3.0)])] {
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
    // Pulling takes both fragments out of the queue and into the window, which charges them.
    assert!(
        execution.next_chunk().unwrap().is_none(),
        "the collecting window reported a piece"
    );
    let held = execution.state().reserved_bytes();
    let window_held = execution.state().runtimes[0].buffer.reservation.size();
    assert!(held > 0, "the collected input is not charged for");
    assert_eq!(
        held,
        pool.reserved(),
        "the state and the pool disagree on what the collected input holds"
    );

    let out = execution.finish().expect("the merged series evaluates");
    assert_eq!(decided(&out), vec![(0, Some(4.0))]);
    // The fragments, the workspace, the queue and the evaluator are all gone.
    assert_eq!(execution.state().reserved_bytes(), 0);
    assert_eq!(pool.reserved(), 0);
    assert_eq!(execution.state().nodes_holding_windows(), 0);
    let current = NodePath::root().metric_name(NodeMetric::CURRENT_RESERVED_BYTES);
    assert_eq!(gauge(&set, &current), 0);
    let peak = NodePath::root().partitioned_metric_name(NodeMetric::PEAK_RESERVED_BYTES, 0);
    assert!(
        gauge(&set, &peak) >= window_held,
        "the peak does not cover the input the window collected"
    );
}

/// A pool that cannot admit what a collecting chain holds fails the execution and gives everything
/// back.
///
/// The window collects its whole input, so the fragment it keeps costs more than the chunk it was
/// made from: the pool below holds room for the chunk and no more, so one of the charges the chain
/// makes fails. Whichever it is, the failure hands every reservation back while the executor is
/// still alive, and the evaluators and the collected fragments go with it.
#[test]
fn a_pool_that_cannot_admit_a_collected_fragment_releases_what_it_held() {
    let inner_grid = grid(0, 60_000, 2);
    let outer_grid = grid(60_000, 60_000, 1);
    let tree = NativeTree::try_new(window_with(
        map(evaluated_input(Arc::clone(&inner_grid)), vec![MathOp::Abs]),
        RangeFunction::SumOverTime,
        120_000,
        Some(Arc::clone(&inner_grid)),
        Arc::clone(&outer_grid),
    ))
    .unwrap();
    let schema = evaluated_schema(Arc::clone(&inner_grid));
    let chunk = evaluated_chunk_of(
        &schema,
        named_metric("a", "x"),
        0,
        vec![Some(1.0), None],
        true,
    );
    // Room for the chunk the source queue holds, which is what the pool admits first, and nothing
    // for the label sets the map records or the fragment the window collects.
    let pool = bounded_pool(evaluated_bytes(&chunk) + 1);
    let mut execution = executor(&tree, Arc::clone(&pool), &ExecutionPlanMetricsSet::new());
    execution
        .push_evaluated(chunk)
        .expect("the source queue holds the chunk");

    let error = execution
        .next_chunk()
        .expect_err("the chain was admitted by a pool that cannot hold what it makes of the chunk");
    assert!(
        matches!(error, DataFusionError::ResourcesExhausted(_)),
        "{error}"
    );
    assert_eq!(execution.state().reserved_bytes(), 0);
    assert_eq!(pool.reserved(), 0);
    assert_eq!(
        execution.state().nodes_holding_windows(),
        0,
        "the stopped execution still holds a window's samples"
    );
}

/// A merge whose workspace the pool cannot admit stops the execution and gives everything back.
///
/// The fragments are admitted one by one as they arrive, so their charge is not what fails here:
/// what fails is the workspace the merge needs, taken in `finish` before the helper runs. The first
/// run measures what the collected input holds where the pool admits it; the second is given that
/// much and a little over -- enough for every fragment, not for the workspace, which charges at
/// least one `(position, member, row)` triple and one position per row. The failure releases the
/// fragments, the evaluators and the queues while the executor is still alive.
#[test]
fn a_pool_that_cannot_admit_the_merge_workspace_releases_what_it_held() {
    let inner_grid = grid(0, 60_000, 2);
    let outer_grid = grid(60_000, 60_000, 1);
    let tree = || {
        NativeTree::try_new(window_with(
            map(evaluated_input(Arc::clone(&inner_grid)), vec![MathOp::Abs]),
            RangeFunction::SumOverTime,
            120_000,
            Some(Arc::clone(&inner_grid)),
            Arc::clone(&outer_grid),
        ))
        .unwrap()
    };
    let schema = evaluated_schema(Arc::clone(&inner_grid));
    // Two closed source series, each holding its value where the other holds none, so the fragments
    // do merge and the merge is what needs the workspace.
    let push_both = |execution: &mut NativeExecutor| {
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
    };

    // What the chain holds once its input is collected, measured where the pool admits it.
    let mut reference = executor(&tree(), pool(), &ExecutionPlanMetricsSet::new());
    push_both(&mut reference);
    assert!(
        reference.next_chunk().unwrap().is_none(),
        "the collecting window reported a piece"
    );
    let measured = reference.state().reserved_bytes();
    assert!(measured > 0, "the collected input is not charged for");
    drop(reference);

    // Room for exactly what the collected input holds, and not for the merge's workspace.
    let pool = bounded_pool(measured + 64);
    let mut execution = executor(&tree(), Arc::clone(&pool), &ExecutionPlanMetricsSet::new());
    push_both(&mut execution);
    assert!(
        execution.next_chunk().unwrap().is_none(),
        "the collecting window reported a piece"
    );
    let collected = execution.state().reserved_bytes();
    assert_eq!(
        collected, measured,
        "the collected input was charged differently the second time, so the pool is not sized for it"
    );
    assert_eq!(collected, pool.reserved());

    let error = execution
        .finish()
        .expect_err("the merge ran without its workspace admitted");
    assert!(
        matches!(error, DataFusionError::ResourcesExhausted(_)),
        "{error}"
    );
    // Everything the chain held went back while the executor is alive: the fragments, the
    // evaluator and both queues.
    assert_eq!(execution.state().reserved_bytes(), 0);
    assert_eq!(pool.reserved(), 0);
    assert_eq!(
        execution.state().nodes_holding_windows(),
        0,
        "the stopped execution still holds a window's samples"
    );
    // A stopped execution is not resumed.
    let again = execution.next_chunk().unwrap_err();
    assert!(
        again.to_string().contains("stopped after an earlier error"),
        "{again}"
    );
}

/// A window that drops empty-valued labels cannot hand two series on under one label set either.
///
/// `{host="web-1", zone=""}` and `{host="web-1"}` are two series to the scan: an empty tag value is
/// a present label, an absent one is not. `rate` drops the metric name and the empty-valued labels,
/// so both become `{host="web-1"}` and each covers the same grid; answering would hand the whole
/// grid out twice under the same labels and the same timestamps. No map is involved, so the
/// rejection has to be the node's own — a check at the root would be too late, because the window
/// above would already have evaluated the two as two series.
///
/// The two cover the same positions of the same field, which is the collision no merge resolves:
/// the refusal names the position, not a closed label set, because closing a source series says
/// nothing about the label set it shared with another one. `rate` needs two samples, so neither
/// series decides position 0 -- it holds the single sample at its own timestamp -- and both decide
/// positions 1 to 4. The first position both occupy is therefore 1.
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
    // The decided positions of the first series, which are the ones the second one has to share.
    assert_eq!(
        first.series.time.runs().count(),
        1,
        "the first series decided more than one run"
    );
    assert_eq!(
        first.series.time.first_position(),
        Some(1),
        "`rate` decided a position the single sample at 0 cannot settle"
    );
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
    assert!(
        message.contains("position 1"),
        "the rejection does not name the first position both series hold: {message}"
    );

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
/// Neither piece closes its series, and neither needs to: the refusal is about the positions
/// themselves. `v0` would carry two values at position 0 under one label set, which no merge can
/// resolve, so the node refuses rather than picking one.
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

/// Two closed series under one label set are two pieces of one logical series.
///
/// `a{job="x"}` and `b{job="x"}` are two series to the input and one label set to `abs`, which drops
/// the name. Each holds a value where the other holds none, and each says it is the last chunk of
/// its own source series — which is what `is_last` means here: the source series ended, not the
/// label set. The map hands both pieces on, and merging the two pieces with
/// [`crate::grid_series::merge_same_labelset`] gives the one series the data has, which is what a
/// caller holding the root's pieces does.
#[test]
fn a_merge_across_two_closed_series_is_the_root_piece_stream() {
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
    // Both pieces come out of the root, one per source series, and neither is refused.
    let mut pieces = Vec::new();
    loop {
        match execution.next_chunk() {
            Ok(Some(chunk)) => pieces.push(chunk),
            Ok(None) => break,
            Err(error) => panic!("a closed source series was refused: {error}"),
        }
    }
    pieces.extend(
        execution
            .finish()
            .expect("the map holds no collected input"),
    );
    assert_eq!(pieces.len(), 2, "the root did not hand over both pieces");
    assert!(
        pieces.iter().all(|piece| piece.is_last),
        "a piece of a closed source series did not close it"
    );

    // The one series the two pieces are, merged the way a consumer of the root's pieces merges
    // them: positions 0 and 1 of field 0, holding 1 and 3.
    let merged = merge_same_labelset(
        &evaluated_schema(Arc::clone(&map_grid)),
        pieces.into_iter().map(|piece| piece.series).collect(),
    )
    .expect("the two pieces hold disjoint positions");
    assert_eq!(
        merged.len(),
        1,
        "the two pieces did not merge into one logical series"
    );
    assert_eq!(
        merged[0].metric,
        {
            let mut mapped = named_metric("a", "x");
            mapped.remove("__name__");
            mapped
        },
        "the merged series does not carry the label set both pieces produced"
    );
    assert_eq!(
        merged[0]
            .fields
            .get(0)
            .expect("one field")
            .iter()
            .map(|sample| match sample {
                SampleRef::Float(value) => Some(value),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![Some(1.0), Some(3.0)],
        "the merged series does not hold both pieces' values at their positions"
    );
}

/// Two open chunks that share a label set are accepted where no position of a field collides.
///
/// `a{job="x"}` and `b{job="x"}` are two series to the input and one label set to `abs`, which
/// drops the name. Neither piece says it is the last chunk of its series, so the second one arrives
/// while the first is still open. The two occupy disjoint positions of field 0 — a value at 0 and a
/// value at 3 — so the label-set check hands both on.
///
/// The ordering is contiguous because that is the only ordering this runtime executes: the stream
/// below is not one a scan produces, since it never closes `a`, and what is checked here is the
/// occupancy the check reads rather than a delivery order.
///
/// This is acceptance by that check, and it is not a merge: this layer passes every chunk through
/// as it arrives and keeps no sample state that would combine two series. Merging two series under
/// one label set is [`crate::grid_series::merge_same_labelset`]'s job, and nothing here performs
/// it.
#[test]
fn two_open_series_that_share_a_label_set_are_accepted_when_no_position_collides() {
    let map_grid = grid(0, 60_000, 4);
    let tree = NativeTree::try_new(map(
        evaluated_input_with_ordering(Arc::clone(&map_grid), SeriesOrdering::Contiguous),
        vec![MathOp::Abs],
    ))
    .expect("a map-only tree is legal over a contiguous input");
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

/// A `NaN` is a value and a missing sample is not, so only the `NaN` occupies its position.
///
/// `abs` drops the metric name, so the two series below are one label set to the map, which is where
/// the two are read against each other. A position holding no sample is a position the series never
/// claimed, and another series may fill it; a position holding a `NaN` is decided and holds a value,
/// like any other, so a second value there is the collision no merge resolves.
#[test]
fn a_nan_occupies_its_position_where_a_missing_sample_does_not() {
    let map_grid = grid(0, 60_000, 1);
    let tree = || {
        NativeTree::try_new(map(
            evaluated_input_with_ordering(Arc::clone(&map_grid), SeriesOrdering::Contiguous),
            vec![MathOp::Abs],
        ))
        .unwrap()
    };
    let schema = evaluated_schema(Arc::clone(&map_grid));

    // The first series' sample at position 0, the second series' sample there, and whether the two
    // can share the label set `abs` leaves them.
    let cases = [
        (vec![Some(f64::NAN)], vec![Some(1.0)], false),
        (vec![Some(1.0)], vec![Some(f64::NAN)], false),
        (vec![Some(f64::NAN)], vec![None], true),
        (vec![Some(1.0)], vec![None], true),
        (vec![None], vec![Some(f64::NAN)], true),
    ];
    // Both chunks are open and neither needs to close: what the check reads is which positions a
    // chunk occupies, and that does not depend on `is_last`.
    let drain = |execution: &mut NativeExecutor| -> Result<(), DataFusionError> {
        while execution.next_chunk()?.is_some() {}
        Ok(())
    };
    for (first, second, accepted) in cases {
        let mut execution = executor(&tree(), pool(), &ExecutionPlanMetricsSet::new());
        execution
            .push_evaluated(evaluated_chunk_of(
                &schema,
                named_metric("a", "x"),
                0,
                first.clone(),
                false,
            ))
            .unwrap();
        drain(&mut execution).expect("the first open chunk is handed on");
        execution
            .push_evaluated(evaluated_chunk_of(
                &schema,
                named_metric("b", "x"),
                0,
                second.clone(),
                false,
            ))
            .unwrap();
        let outcome = drain(&mut execution);
        assert_eq!(
            outcome.is_ok(),
            accepted,
            "{first:?} and {second:?} were {}: {:?}",
            if accepted { "refused" } else { "handed on" },
            outcome.err(),
        );
    }
}

/// One series arriving in pieces is not two series: the pieces are merged into the one series they
/// are, and the window above reports it once.
///
/// The map is passed twice for one label set, so the window above collects both pieces and merges
/// them with [`crate::grid_series::merge_same_labelset`] before it evaluates: the two pieces hold
/// one position each, the window across them needs both, and answering per piece would close the
/// first series in the evaluator before the second arrived.
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
    // Nothing comes out of the collecting window before its input ended, and the piece here does
    // not end it.
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
/// A long series is fed one sample per chunk to a pool with a fixed budget, pulling and dropping the
/// result of every chunk and closing only on the last one. The same one-sample-per-chunk input
/// leaves the evaluator's storage at the same size however many chunks it arrives in, so the peak
/// reservation is the same whether the data is split into 64 chunks or into 1024, and it stays a
/// small constant rather than the running total of what arrived. A figure charged per chunk
/// consumed would grow with the chunk count and exhaust the budget the first chunks fit in.
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
            drop(execution.next_chunk().unwrap_or_else(|error| {
                panic!("chunk {index} of the {count}-chunk series did not fit: {error}")
            }));
            let held = execution.state().reserved_bytes();
            assert_eq!(
                held,
                pool.reserved(),
                "chunk {index} of {count}: the state and the pool disagree on what is held"
            );
            assert!(
                held <= (8 << 10),
                "chunk {index} of {count} holds {held} bytes of a 1 ms window over one sample"
            );
            peak = peak.max(held);
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
/// three. Every field costs its own timestamps, values and row slot, so the reservation a window
/// reaches grows with the width of the series; a figure counting one timestamp and one float per
/// row, or one field per series, would not.
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
            peak = peak.max(execution.state().reserved_bytes());
        }
        peak
    };
    let (one, two, three) = (peak(1), peak(2), peak(3));
    assert!(
        two > one,
        "a window over two fields reserved {two} bytes, no more than the {one} bytes a window over \
         one field reserves"
    );
    assert!(
        three > two,
        "a window over three fields reserved {three} bytes, no more than the {two} bytes a window \
         over two fields reserves"
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
    let pool = pool();
    let mut execution = executor(&tree, Arc::clone(&pool), &ExecutionPlanMetricsSet::new());

    execution
        .push_raw(raw_chunk(&schema, &[0, 60_000], &[1.0, 2.0], false))
        .unwrap();
    let first = execution
        .next_chunk()
        .unwrap()
        .expect("the first chunk decided both points");
    assert_eq!(sample_values(&first), vec![Some(1.0), Some(2.0)]);
    drop(first);
    let after_output = execution.state().reserved_bytes();
    assert!(
        after_output > 0,
        "the window handed out a position, which it has to hold"
    );
    assert_eq!(after_output, pool.reserved());

    // Both points are decided, so this chunk decides nothing and the window yields no chunk.
    execution
        .push_raw(raw_chunk(&schema, &[120_000], &[3.0], false))
        .unwrap();
    assert!(
        execution.next_chunk().unwrap().is_none(),
        "a chunk after every point was decided produced a chunk"
    );
    assert!(
        execution.state().reserved_bytes() > 0,
        "a chunk with no output released the bytes the emitted table still holds"
    );
    assert_eq!(execution.state().reserved_bytes(), pool.reserved());
}

/// A window cannot run over an interleaved input, and the runtime says so before reading a chunk.
///
/// The binding refuses a window whose own input declares the ordering, and the runtime refuses the
/// ordering itself -- which is what catches a window over a map over the source, where the binding
/// only sees the map. A tree with no window above the source is refused by the same check, since
/// every node here reads one series at a time; [`an_interleaved_input_is_refused_without_a_window`]
/// is that case on its own.
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
    // A rejected tree is a compiler defect rather than a query error, so the binding reports it as
    // `Internal`; what this pins is that the refusal names the ordering it came from rather than
    // the node kind, since the same check refuses every input that declares it.
    assert!(matches!(error, DataFusionError::Internal(_)), "{error}");
    let message = error.to_string();
    assert!(message.contains("interleaved"), "{message}");

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
    assert!(message.contains("root.input"), "{message}");
}

/// An interleaved input is refused with no window above it to notice it.
///
/// A map over an input has no evaluator, so nothing in the tree reads one series to its `is_last`.
/// The sequence the input node cannot tell from a whole one is `a{job="x"}` open, `b{job="y"}`
/// closed, and the end of the input: the flag that reports a series left open would say the last
/// chunk closed its series and `finish` would accept an input whose first series never closed. The
/// runtime therefore refuses the ordering when the state is built, before a chunk is read, instead
/// of reporting a truncation it cannot see.
#[test]
fn an_interleaved_input_is_refused_without_a_window() {
    let map_grid = grid(0, 60_000, 2);
    let tree = NativeTree::try_new(map(
        evaluated_input_with_ordering(Arc::clone(&map_grid), SeriesOrdering::Interleaved),
        vec![MathOp::Abs],
    ))
    .expect("the binding accepts a map over an interleaved input");
    let error =
        match NativeExecutionState::try_new(&tree, pool(), &ExecutionPlanMetricsSet::new(), 0) {
            Ok(_) => panic!("the runtime accepted an interleaved input with no window above it"),
            Err(error) => error,
        };
    assert!(
        matches!(error, DataFusionError::NotImplemented(_)),
        "{error}"
    );
    let message = error.to_string();
    assert!(message.contains("Interleaved"), "{message}");
    assert!(message.contains("root.input"), "{message}");
}

/// A series that produced values under a label set, followed by one that produced none under the
/// same label set, is not a collision: the second contributes no sample, so the answer is just the
/// first one's values.
///
/// This is the other order of [`two_series_with_no_result_do_not_collide`]: a series that produced
/// values under a label set, followed by one that produced none. The empty one occupies no
/// position, so it records nothing and changes nothing, and the answer is the first one's values.
///
/// The first series closed, which closes the source series and not the label set: the empty series
/// is a later one that also answers empty, and
/// [`an_empty_closing_chunk_of_another_series_does_not_close_a_label_set`] is its mirror image.
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

/// An empty closing chunk of one series does not close a label set another series is still filling.
///
/// `abs` drops the metric name, so `a{job="x"}` and `b{job="x"}` are one label set to the map. `b`
/// holds no row at all: its closing chunk closes a series that contributed nothing, so it must not
/// close the set `a` opened, or `a`'s continuation would be refused for a label set that `a` itself
/// never closed. Two series back to back are what contiguous input hands over, so this is a stream
/// the runtime executes rather than an ordering it refuses.
#[test]
fn an_empty_closing_chunk_of_another_series_does_not_close_a_label_set() {
    let map_grid = grid(0, 60_000, 2);
    let tree = NativeTree::try_new(map(
        evaluated_input(Arc::clone(&map_grid)),
        vec![MathOp::Abs],
    ))
    .unwrap();
    let schema = evaluated_schema(Arc::clone(&map_grid));
    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());

    // `a` opens the label set and has not closed it when `b` closes an empty series onto it, and the
    // chunk that closes `a` itself comes last.
    let chunks = [
        evaluated_chunk_of(&schema, named_metric("a", "x"), 0, vec![Some(-1.0)], false),
        evaluated_chunk_of(&schema, named_metric("b", "x"), 0, vec![], true),
        evaluated_chunk_of(&schema, named_metric("a", "x"), 1, vec![Some(-2.0)], true),
    ];
    let steps = chunks.len();
    let mut handed = Vec::new();
    for (step, chunk) in chunks.into_iter().enumerate() {
        execution.push_evaluated(chunk).unwrap();
        while let Some(chunk) = execution
            .next_chunk()
            .unwrap_or_else(|error| panic!("step {step} was refused: {error}"))
        {
            handed.push(chunk);
        }
    }
    assert_eq!(
        handed.len(),
        steps,
        "the map did not consume all {steps} chunks it was given: {:?}",
        decided(&handed)
    );
    let last = handed.last().expect("the map handed nothing on");
    assert!(last.is_last, "the final output does not close its series");
    // `abs` drops the metric name, so the label set the output closes is `{job="x"}`.
    let mut mapped = named_metric("a", "x");
    mapped.remove("__name__");
    assert_eq!(
        last.series.metric, mapped,
        "the final output closes a label set other than the one `abs` leaves of `a{{job=\"x\"}}`"
    );
    assert_eq!(
        (series_start(last), sample_values(last)),
        (1, vec![Some(2.0)]),
        "the value `a` held at position 1 did not survive"
    );
    assert!(
        execution.finish().unwrap().is_empty(),
        "the map still held a chunk when the input ended"
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
        evaluated_input(Arc::clone(&map_grid)),
        vec![MathOp::Abs],
    ))
    .unwrap();
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

/// A tree with no window reports an input that ended mid-series as a truncated input.
///
/// A window's evaluator is what knows a series has not closed: it holds the series until the chunk
/// carrying `is_last`, so closing the execution over what is left reports the input that never
/// delivered it. A tree of an input alone, or of a map over one, has no evaluator at all, so
/// without the input node saying so the series would reach the consumer open and never close. The
/// input node is the only place that sees every chunk the caller queued, so the truncation is
/// reported against it, for a tree of one node and for one with a map above it alike.
#[test]
fn an_input_that_ends_mid_series_is_reported_without_a_window() {
    let map_grid = grid(0, 60_000, 2);
    let schema = evaluated_schema(Arc::clone(&map_grid));
    let shapes = [
        // The input alone, and the same input under a map: neither has a window above it.
        ("an input", evaluated_input(Arc::clone(&map_grid))),
        (
            "a map over an input",
            map(evaluated_input(Arc::clone(&map_grid)), vec![MathOp::Abs]),
        ),
    ];
    for (what, root) in shapes {
        let tree = NativeTree::try_new(root).unwrap();
        let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
        // A chunk that does not close its series: the chunk carrying `is_last` never arrives.
        execution
            .push_evaluated(evaluated_chunk(&schema, 0, vec![-1.0, -2.0], false))
            .unwrap();
        let handed = execution
            .next_chunk()
            .unwrap()
            .expect("a chunk of a series still open is handed on as it arrives");
        assert!(
            !handed.is_last,
            "{what} handed on a chunk that closed a series it never closed"
        );
        let error = execution
            .finish()
            .expect_err("an input that ended mid-series was accepted");
        assert!(
            error
                .to_string()
                .contains("ended before the last chunk of a series"),
            "{what} reported {error} instead of a truncated input"
        );
    }
}

/// A window over native histograms reserves what it retains, payload included.
///
/// A chunk is estimated from the samples it holds rather than from one float per field and row, so a
/// histogram field -- which holds no float at all -- reaches the figure as the table its indices point
/// into. A window over such a series therefore reserves for the payload it keeps, and a one-sample
/// histogram chunk is worth more than a one-sample float chunk of the same shape.
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
    let pool = pool();
    let mut execution = executor(&tree, Arc::clone(&pool), &ExecutionPlanMetricsSet::new());

    for (timestamp, is_last) in [(0, false), (10_000, true)] {
        execution
            .push_raw(raw_histogram_chunk(&schema, &[timestamp], 2, is_last))
            .unwrap();
        while execution.next_chunk().unwrap().is_some() {}
        assert_eq!(
            execution.state().reserved_bytes(),
            pool.reserved(),
            "the state and the pool disagree on what the window holds"
        );
        assert!(
            pool.reserved() > 0,
            "a window over a histogram series reserves for nothing"
        );
    }

    // The table the indices point into is what the field costs, so the same one-sample chunk is
    // worth more as a histogram than as a float.
    let histogram = raw_histogram_chunk(&schema, &[0], 2, true);
    let float = raw_chunk(&schema, &[0], &[1.0], true);
    assert!(
        raw_bytes(&histogram) > raw_bytes(&float),
        "a histogram chunk is estimated at {} bytes, no more than the {} bytes of a float chunk of \
         the same shape",
        raw_bytes(&histogram),
        raw_bytes(&float),
    );
}

/// A source reserves the estimate of every queued chunk, and gives each one back as it leaves.
///
/// The figure is symmetric: what a chunk added when it entered is what comes out when it leaves, so a
/// queue that empties returns its reservation to the pool exactly. Two chunks of one series may share
/// one immutable histogram table, and each of them is charged for it: the estimate is per chunk, which
/// costs a bounded over-count of a shared table rather than a pass over the queue per operation.
#[test]
fn a_source_charges_its_queue_and_gives_it_back() {
    let pool = pool();
    let mut source = source_runtime(&pool);
    let schema = raw_schema();
    let mut builder = HistogramBatchBuilder::new();
    let index = push_histogram(&mut builder, 2);
    let table = builder.finish().unwrap();
    let first = raw_shared_histogram_chunk(&schema, &table, index, 0, false);
    let second = raw_shared_histogram_chunk(&schema, &table, index, 10_000, true);
    let (first_bytes, second_bytes) = (raw_bytes(&first), raw_bytes(&second));

    source.enqueue(NativeChunk::Raw(first)).unwrap();
    assert_eq!(source.buffer.reservation.size(), first_bytes);
    assert_eq!(pool.reserved(), first_bytes);

    source.enqueue(NativeChunk::Raw(second)).unwrap();
    assert_eq!(
        source.buffer.reservation.size(),
        first_bytes + second_bytes,
        "the queue is not the sum of the estimates it holds"
    );
    assert_eq!(pool.reserved(), first_bytes + second_bytes);

    let popped = source
        .pop_source()
        .unwrap()
        .expect("the queue held two chunks");
    assert!(!popped.is_last());
    // One table, two queued chunks, two shares of its estimate: what leaves is what the chunk that
    // left was charged, and the other chunk's share stays.
    assert_eq!(
        source.buffer.reservation.size(),
        second_bytes,
        "the chunk that left did not give its estimate back"
    );
    assert_eq!(pool.reserved(), second_bytes);

    source
        .pop_source()
        .unwrap()
        .expect("the queue held two chunks");
    assert_eq!(source.buffer.reservation.size(), 0);
    assert_eq!(
        pool.reserved(),
        0,
        "the queue emptied, but the pool holds its bytes"
    );
    assert!(source.pop_source().unwrap().is_none());
}

/// Releasing a node drops its queue and returns the reservation once.
///
/// A node stopped with chunks still queued holds nothing afterwards: the queue goes, and the bytes it
/// was reserved for go back to the pool rather than staying held for as long as the caller keeps the
/// stopped executor alive.
#[test]
fn releasing_a_node_drops_its_queue() {
    let pool = pool();
    let mut source = source_runtime(&pool);
    let schema = raw_schema();
    let mut builder = HistogramBatchBuilder::new();
    let index = push_histogram(&mut builder, 2);
    let table = builder.finish().unwrap();
    for timestamp in [0, 10_000] {
        source
            .enqueue(NativeChunk::Raw(raw_shared_histogram_chunk(
                &schema, &table, index, timestamp, false,
            )))
            .unwrap();
    }
    assert!(pool.reserved() > 0);

    source.release();
    assert!(source.queued.is_empty(), "the queue outlived its release");
    assert_eq!(source.buffer.reservation.size(), 0);
    assert_eq!(pool.reserved(), 0, "the reservation was not returned");
    assert!(source.pop_source().unwrap().is_none());
}

/// A chunk whose axis and fields disagree is refused, not read past the end of a field.
///
/// [`Series`] keeps its fields public, so a chunk built by struct literal -- by a caller of this
/// crate as much as by a mistake here -- can carry fewer samples than the axis places rows. The
/// label-set ledger reads every row the axis names, so it is where such a chunk would be read out of
/// bounds; it reports the misalignment against the node that was handed it instead.
#[test]
fn a_series_whose_axis_and_fields_disagree_is_rejected() {
    let grid = grid(0, 60_000, 4);
    // Two samples placed on a four-row axis: the third row the axis names has nothing to read.
    let samples = Samples::from_float(Float64Array::from(vec![1.0, 2.0]));
    let time = TimeAxis::Grid(GridRange::try_new(Arc::clone(&grid), 0, 4).unwrap());
    let series = Series {
        metric: named_metric("requests", "job"),
        time,
        fields: SeriesFields::try_from_vec(vec![samples]).unwrap(),
    };
    let chunk = EvaluatedChunk::try_new(series, false).expect("the axis is addressed on a grid");

    let mut emitted = Emitted::default();
    let error = emitted
        .occupy(&NodePath::root(), &chunk)
        .expect_err("a field with fewer samples than the axis places rows was accepted");
    let message = error.to_string();
    assert!(
        message.contains("misaligned")
            && message.contains("4 rows")
            && message.contains("2 samples"),
        "the rejection does not name what the axis places against what the field holds: {message}"
    );
}

/// A sparse series that only ever moves forward is appended to its history, not merged into it again
/// chunk after chunk.
///
/// One legal series arrives as eight chunks of one position each, every position a hole apart from
/// the last: no label set is reached twice, no position of a field is occupied twice, and this is
/// what a scan produces as its positions advance. The stored runs still grow by one per chunk, so
/// rebuilding the array after each chunk moves `K(K+1)/2` runs to hold `K` rows of real data, which
/// is the cost this asserts away.
///
/// The chunks go through [`super::Emitted::occupy`], the helper the runtime records an output with,
/// so what is checked is the history that helper keeps. Three shapes are pushed: chunks landing past
/// every stored run, which have to be appended to; a chunk starting where the last stored run ends,
/// which has to be joined to it rather than left beside it; and chunks landing before what is
/// stored, which cannot be appended to and have to go through the full merge while still leaving the
/// history sorted and disjoint.
#[test]
fn a_sparse_series_that_moves_forward_is_appended_to_its_history() {
    let map_grid = grid(0, 60_000, 21);
    let schema = evaluated_schema(Arc::clone(&map_grid));
    let path = NodePath::root();
    let mut emitted = Emitted::default();

    // One row at `position`, which is one run of one position: the same series every time, since a
    // map dropping the name leaves them all one label set.
    let push = |emitted: &mut Emitted, position: usize| {
        let chunk = sparse_chunk(
            &schema,
            named_metric("a", "x"),
            &[(position, 1)],
            vec![-1.0],
            false,
        );
        emitted
            .occupy(&path, &chunk)
            .unwrap_or_else(|error| panic!("position {position} was refused: {error}"));
    };
    // The runs stored under the one label set, which is what `absorb` keeps in position order.
    let stored = |emitted: &Emitted| -> Vec<(usize, usize)> {
        emitted
            .sets
            .values()
            .next()
            .expect("the chunk was recorded under one label set")
            .fields
            .first()
            .expect("the series has one field")
            .iter()
            .map(|run| (run.start, run.end))
            .collect()
    };
    let ordered = |runs: &[(usize, usize)]| {
        runs.iter().all(|(start, end)| start < end)
            && runs.windows(2).all(|pair| pair[0].1 < pair[1].0)
    };

    // Eight chunks, one position each, a hole between them.
    let positions = [4, 6, 8, 10, 12, 14, 16, 18];
    let before = absorb_rebuilds();
    for position in positions {
        push(&mut emitted, position);
    }
    assert_eq!(
        stored(&emitted),
        vec![
            (4, 5),
            (6, 7),
            (8, 9),
            (10, 11),
            (12, 13),
            (14, 15),
            (16, 17),
            (18, 19),
        ],
        "a sparse series was not stored as the one run each of its chunks occupies"
    );
    assert_eq!(
        absorb_rebuilds() - before,
        0,
        "the {} chunks of a series moving forward rebuilt its history instead of appending to it",
        positions.len()
    );

    // A chunk that starts where the last stored run ends touches it, and the two are one run: the
    // fast path appends, and the boundary is the part it has to join.
    let before = absorb_rebuilds();
    push(&mut emitted, 19);
    assert_eq!(
        stored(&emitted),
        vec![
            (4, 5),
            (6, 7),
            (8, 9),
            (10, 11),
            (12, 13),
            (14, 15),
            (16, 17),
            (18, 20),
        ],
        "a chunk touching the last stored run was left beside it rather than joined to it"
    );
    assert_eq!(
        absorb_rebuilds() - before,
        0,
        "a chunk touching the last stored run was merged rather than appended to it"
    );

    // A chunk landing in a hole before the last stored run cannot be appended to: it joins the runs
    // on both sides of it, so it goes through the full merge, which has to join them all the same.
    let before = absorb_rebuilds();
    push(&mut emitted, 5);
    assert_eq!(
        stored(&emitted),
        vec![
            (4, 7),
            (8, 9),
            (10, 11),
            (12, 13),
            (14, 15),
            (16, 17),
            (18, 20),
        ],
        "a chunk in a hole did not join the runs on either side of it"
    );
    assert_eq!(
        absorb_rebuilds() - before,
        1,
        "a chunk landing before the last stored run was appended rather than merged"
    );

    // A chunk before every stored run is the other shape the fast path cannot take.
    let before = absorb_rebuilds();
    push(&mut emitted, 1);
    let runs = stored(&emitted);
    assert_eq!(
        runs,
        vec![
            (1, 2),
            (4, 7),
            (8, 9),
            (10, 11),
            (12, 13),
            (14, 15),
            (16, 17),
            (18, 20),
        ],
        "a chunk before every stored run did not land in front of them"
    );
    assert_eq!(
        absorb_rebuilds() - before,
        1,
        "a chunk landing before every stored run was appended rather than merged"
    );
    assert!(
        ordered(&runs),
        "the merged history is not sorted, disjoint and joined: {runs:?}"
    );
}
