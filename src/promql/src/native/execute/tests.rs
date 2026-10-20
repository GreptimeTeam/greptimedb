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
    BinaryRuntime, ChargedAllocations, Emitted, History, NativeChunk, NativeExecutionState,
    NativeExecutor, NodeBuffer, NodeRuntime, QueuedCharge, Run, absorb_rebuilds, fields_bytes,
    label_bytes, map_chunk, raw_bytes,
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
    raw_input_with_ordering(SeriesOrdering::KeyThenTime)
}

/// The same raw source, declaring `ordering`.
fn raw_input_with_ordering(ordering: SeriesOrdering) -> NativeNode {
    let output = NativeType::Samples(raw_schema());
    NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: output.clone(),
            ordering,
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

/// A bare selector over `inner`: the last sample at or before each grid point, read within
/// `lookback_ms` and at `offset_ms`.
///
/// The selection reports one value per input field, so its layout is the input's, just as the
/// compiler builds it.
fn instant(
    inner: NativeNode,
    lookback_ms: i64,
    offset_ms: i64,
    output_grid: Arc<EvalGrid>,
) -> NativeNode {
    let fields = inner
        .output
        .samples()
        .expect("a selection reads samples")
        .fields
        .clone();
    NativeNode {
        expr: NativeExpr::Instant {
            input: Box::new(inner),
            op: BoundInstant {
                lookback_ms,
                offset_ms,
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

/// One raw chunk of `series-a` whose fields carry `columns`, one column per field.
///
/// A `None` is a position that field holds no sample at, which is what a partially filled field
/// looks like at the source.
fn raw_chunk_columns(
    schema: &SeriesSchema,
    timestamps: &[i64],
    columns: Vec<Vec<Option<f64>>>,
    is_last: bool,
) -> RawSeriesChunk {
    let fields = SeriesFields::try_from_vec(
        columns
            .into_iter()
            .map(|values| Samples::from_float(Float64Array::from(values)))
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

/// One raw chunk of `series-a` whose one field holds a four-bucket native histogram at each of
/// `timestamps`, each carrying `start_timestamp` as the payload it was read from reports it.
fn raw_histogram_chunk_with_start(
    schema: &SeriesSchema,
    timestamps: &[i64],
    start_timestamp: Option<i64>,
    is_last: bool,
) -> RawSeriesChunk {
    let mut builder = HistogramBatchBuilder::new();
    let offsets = [0];
    let lengths = [4];
    let counts = vec![1.0; 4];
    let indices: UInt32Array = timestamps
        .iter()
        .map(|_| {
            Some(
                builder
                    .push(HistogramView {
                        schema: 0,
                        zero_threshold: 0.0,
                        zero_count: 0.0,
                        count: 4.0,
                        sum: 4.0,
                        reset_hint: CounterResetHint::Unknown,
                        start_timestamp,
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
                    .unwrap(),
            )
        })
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
        instant: None,
        window: None,
        map: None,
        binary: None,
        source_schema: None,
        emitted: None,
        open_source_series: false,
    }
}

/// A binary node's runtime: both operands bound to `schema` and the reservation they are held for.
fn binary_runtime(pool: &Arc<dyn MemoryPool>, schema: &Arc<SeriesSchema>) -> NodeRuntime {
    NodeRuntime {
        buffer: NodeBuffer::new(&NodePath::root(), Arc::new(NodeMetrics::default()), pool),
        queued: VecDeque::new(),
        queued_charge: QueuedCharge::default(),
        instant: None,
        window: None,
        map: None,
        binary: Some(BinaryRuntime::new(
            Arc::new(binary_matching(BinaryOp::Add)),
            [Arc::clone(schema), Arc::clone(schema)],
        )),
        source_schema: None,
        emitted: None,
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
    evaluated_array_chunk_of(schema, metric, start, Float64Array::from(values), is_last)
}

/// One evaluated chunk of the series `metric` whose one field holds `values`, addressed at `start`.
///
/// The array is passed in rather than built here so that two chunks can hold sibling slices of one
/// Arrow allocation, which is what a source handing over the chunks of one batch does.
fn evaluated_array_chunk_of(
    schema: &SeriesSchema,
    metric: MetricName,
    start: usize,
    values: Float64Array,
    is_last: bool,
) -> EvaluatedChunk {
    let samples = Samples::from_float(values);
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
    field_values(chunk, 0)
}

/// The values field `index` carries, one per row the chunk holds.
fn field_values(chunk: &EvaluatedChunk, index: usize) -> Vec<Option<f64>> {
    let field = chunk
        .series
        .fields
        .get(index)
        .unwrap_or_else(|| panic!("the chunk carries no field {index}"));
    field
        .iter()
        .map(|sample| match sample {
            SampleRef::Float(value) => Some(value),
            _ => None,
        })
        .collect()
}

/// The start timestamp of the histogram at every position of a chunk, in grid order.
///
/// It is read off the output's own histogram views rather than off a converted value, because the
/// figure under test is the one a later reader sees.
fn histogram_starts(chunk: &EvaluatedChunk) -> Vec<Option<i64>> {
    let mut cursor = chunk.series.fields.as_slice()[0].cursor();
    chunk
        .series
        .time
        .positions()
        .filter_map(
            |(_, row)| match cursor.at(row).expect("the axis and the fields are aligned") {
                SampleRef::Histogram(view) => Some(view.start_timestamp),
                _ => None,
            },
        )
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

/// A selection over an interleaved input is refused when the execution is built, not run.
///
/// The binding accepts this tree: the plan's `Instant` arm has no ordering check yet -- #9785 adds
/// one to `validate`, so that half of the refusal arrives in a separate change. The tree can
/// therefore be *built* here and cannot be *executed*, which is exactly the split this runtime
/// keeps: an instant selection reads one series to its `is_last` and holds no cross-series state,
/// but the input node's flag -- and every node above it -- assumes the chunks of a series arrive
/// together, so an input declaring `Interleaved` is refused before a chunk is read. The shape with
/// a map between the input and the selection is
/// [`an_interleaved_input_is_refused_under_a_map_over_a_selection`].
#[test]
fn an_interleaved_input_is_refused_under_a_selection() {
    let out_grid = grid(0, 60_000, 2);
    let tree = NativeTree::try_new(instant(
        raw_input_with_ordering(SeriesOrdering::Interleaved),
        60_000,
        0,
        Arc::clone(&out_grid),
    ))
    .expect("the binding accepts a selection over an interleaved input");
    let error =
        match NativeExecutionState::try_new(&tree, pool(), &ExecutionPlanMetricsSet::new(), 0) {
            Ok(_) => panic!("the runtime accepted an interleaved input under a selection"),
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

/// The same refusal with a map between the input and the selection.
///
/// A map passes chunks through in the order they arrive, so the selection still reads an
/// interleaved stream even though the node below it is a map: the ordering is a property of the
/// input, and the runtime records it there rather than at the node that reads it.
#[test]
fn an_interleaved_input_is_refused_under_a_map_over_a_selection() {
    let out_grid = grid(0, 60_000, 2);
    let tree = NativeTree::try_new(map(
        instant(
            raw_input_with_ordering(SeriesOrdering::Interleaved),
            60_000,
            0,
            Arc::clone(&out_grid),
        ),
        vec![MathOp::Abs],
    ))
    .expect("the binding accepts a map over a selection over an interleaved input");
    let error =
        match NativeExecutionState::try_new(&tree, pool(), &ExecutionPlanMetricsSet::new(), 0) {
            Ok(_) => {
                panic!("the runtime accepted an interleaved input under a map and a selection")
            }
            Err(error) => error,
        };
    assert!(
        matches!(error, DataFusionError::NotImplemented(_)),
        "{error}"
    );
    let message = error.to_string();
    assert!(message.contains("Interleaved"), "{message}");
    assert!(message.contains("root.input.input"), "{message}");
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
///
/// The set is closed here, which is what makes the empty series a later one that also answers
/// empty. Its mirror image -- an empty closing chunk that arrives while the set is still open -- is
/// [`an_empty_closing_chunk_of_another_series_does_not_close_a_label_set`], where the same series
/// is not a harmless extra but would take the close away from the series that filled the set.
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
        .occupy(&NodePath::root(), Some(&chunk.series.metric), &chunk)
        .expect_err("a field with fewer samples than the axis places rows was accepted");
    let message = error.to_string();
    assert!(
        message.contains("misaligned")
            && message.contains("4 rows")
            && message.contains("2 samples"),
        "the rejection does not name what the axis places against what the field holds: {message}"
    );
}

/// One entry's figure is the heap it owns, and its `History` header is charged once, by its slot.
///
/// The `History` of a label set lives in the map's slot, so a `size_of::<History>()` term in the
/// entry's own figure billed one header twice: once here and once in the capacity term beside it.
/// The two assertions below are the definition of the whole figure -- the labels, the vectors and
/// their runs, the owner's name and tags, and one label-set slot plus one history slot per entry --
/// so either header counted in the wrong place fails them.
#[test]
fn a_history_entry_is_charged_once_for_its_header() {
    let grid = grid(0, 10_000, 2);
    let schema = evaluated_schema(Arc::clone(&grid));
    let metric = named_metric("requests", "job");
    let labels = label_set(&metric);
    // Not the closing chunk: a closed label set drops its runs, and the runs are what the entry's
    // own figure is.
    let chunk = evaluated_chunk_of(
        &schema,
        metric.clone(),
        0,
        vec![Some(1.0), Some(2.0)],
        false,
    );

    let mut emitted = Emitted::default();
    emitted
        .occupy(&NodePath::root(), Some(&metric), &chunk)
        .unwrap();

    let history = emitted
        .sets
        .get(&labels)
        .expect("the chunk recorded its label set");
    // The owner the entry keeps is the series that filled it, so its name and each tag cost the
    // same bytes the label set of the chunk did.
    let owner = metric.name().map_or(0, |name| name.len())
        + std::mem::size_of_val(metric.tags())
        + metric
            .tags()
            .iter()
            .map(|tag| tag.name.len() + tag.value.len())
            .sum::<usize>();
    let entry = label_bytes(&labels)
        .saturating_add(history.fields.capacity() * std::mem::size_of::<Vec<Run>>())
        .saturating_add(
            history
                .fields
                .iter()
                .map(|runs| runs.capacity() * std::mem::size_of::<Run>())
                .sum::<usize>(),
        )
        .saturating_add(owner);
    assert_eq!(
        history.owner.as_ref(),
        Some(&metric),
        "the entry does not name the series that filled it"
    );
    assert!(
        history.fields.iter().any(|runs| !runs.is_empty()),
        "the entry kept no run, so it has no heap of its own to charge for"
    );
    assert_eq!(
        emitted.entries,
        entry,
        "the entry's own bytes are not its labels, its runs and its owner: a `History` header in \
         the figure would add the {} bytes its slot already charges",
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
            .occupy(&path, Some(&chunk.series.metric), &chunk)
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

/// A bare selector stamps the last sample at or before each grid point onto its own grid.
///
/// The chunk is addressed on the node's output grid, not on the source's timestamps; a grid point
/// may select a row an earlier point already did (the point after the last sample repeats it); and
/// the same samples delivered as one chunk and as three answer the same, because the selection reads
/// a whole axis rather than a chunk.
#[test]
fn a_bare_selector_selects_instants_onto_its_own_grid() {
    let out_grid = grid(0, 60_000, 4);
    let tree = NativeTree::try_new(instant(input(), 120_000, 0, Arc::clone(&out_grid))).unwrap();
    let schema = raw_schema();
    let whole = raw_chunk(
        &schema,
        &[0, 30_000, 90_000, 120_000, 200_000],
        &[1.0, 2.0, 3.0, 4.0, 5.0],
        true,
    );
    // The same series as three chunks, so nothing about it is decided by a chunk boundary.
    let split = [
        raw_chunk(&schema, &[0, 30_000], &[1.0, 2.0], false),
        raw_chunk(&schema, &[90_000], &[3.0], false),
        raw_chunk(&schema, &[120_000, 200_000], &[4.0, 5.0], true),
    ];

    let mut once = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    once.push_raw(whole).unwrap();
    let out = run(&mut once);

    let mut piecewise = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    let mut in_pieces = Vec::new();
    for chunk in split {
        let closing = chunk.is_last;
        piecewise.push_raw(chunk).unwrap();
        // Until the series ends nothing is decided for it, so only the closing chunk reports.
        let pulled = piecewise.next_chunk().unwrap();
        assert_eq!(
            pulled.is_some(),
            closing,
            "a series reported a chunk before its last one arrived"
        );
        in_pieces.extend(pulled);
    }
    in_pieces.extend(piecewise.finish().unwrap());

    assert_eq!(out.len(), 1, "one series is one chunk, however it arrived");
    let chunk = &out[0];
    assert!(chunk.is_last, "the only chunk of a series closes it");
    assert!(
        Arc::ptr_eq(chunk.series.time.grid().expect("a grid chunk"), &out_grid),
        "the chunk is addressed on a grid that is not the selection's output grid"
    );
    // 0 -> 0, 60s -> 30s, 120s -> 120s, 180s -> 120s: the row at 90s is never the last one before a
    // grid point, and the point past the last sample repeats the row before it.
    assert_eq!(
        decided(&out),
        vec![
            (0, Some(1.0)),
            (1, Some(2.0)),
            (2, Some(4.0)),
            (3, Some(4.0)),
        ]
    );
    // A selection keeps the labels it read, the metric name included: unlike a range function it
    // does not rewrite what the selector named.
    assert_eq!(chunk.series.metric, metric());
    assert_eq!(
        decided(&in_pieces),
        decided(&out),
        "the same series split into chunks answered differently"
    );
}

/// The lookback edge is exclusive: a sample exactly `lookback_ms` before a grid point is not
/// selected, one millisecond later is.
///
/// This pins what the selection kernel does rather than what Prometheus' documentation suggests: a
/// sample at the point itself is always selected, and the window is `(point - lookback, point]`.
#[test]
fn a_sample_at_the_lookback_edge_is_not_selected() {
    // The statement's grid is the single point 1000ms, and the lookback is 200ms.
    let out_grid = grid(1000, 100, 1);
    let schema = raw_schema();
    let selected = |timestamp: i64| {
        let tree = NativeTree::try_new(instant(input(), 200, 0, Arc::clone(&out_grid))).unwrap();
        let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
        execution
            .push_raw(raw_chunk(&schema, &[timestamp], &[1.0], true))
            .unwrap();
        run(&mut execution)
    };

    // 800 is exactly `1000 - 200`, so the edge itself selects nothing.
    let edge = selected(800);
    assert_eq!(edge.len(), 1, "the series still closes on one chunk");
    assert!(edge[0].is_last);
    assert!(
        decided(&edge).is_empty(),
        "a sample exactly at the lookback edge was selected: {:?}",
        decided(&edge)
    );
    // One millisecond inside the window is selected; a sample after the grid point never is.
    assert_eq!(decided(&selected(801)), vec![(0, Some(1.0))]);
    assert!(decided(&selected(1001)).is_empty());
}

/// A positive offset reads earlier samples, and a negative one later samples.
#[test]
fn an_offset_reads_a_different_sample_at_each_grid_point() {
    let out_grid = grid(0, 60_000, 2);
    let schema = raw_schema();
    let values = |offset_ms: i64| {
        let tree = NativeTree::try_new(instant(input(), 600_000, offset_ms, Arc::clone(&out_grid)))
            .unwrap();
        let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
        execution
            .push_raw(raw_chunk(
                &schema,
                &[-60_000, 0, 60_000],
                &[1.0, 2.0, 3.0],
                true,
            ))
            .unwrap();
        decided(&run(&mut execution))
    };

    assert_eq!(
        values(0),
        vec![(0, Some(2.0)), (1, Some(3.0))],
        "without an offset a point reads the sample at or before it"
    );
    assert_eq!(
        values(60_000),
        vec![(0, Some(1.0)), (1, Some(2.0))],
        "a minute of offset did not read the samples a minute earlier"
    );
    // A negative offset reads samples the statement's own range does not reach: both points read
    // the sample one minute after them.
    assert_eq!(
        values(-60_000),
        vec![(0, Some(3.0)), (1, Some(3.0))],
        "a negative offset did not read the samples a minute later"
    );
}

/// A series that selects nothing still closes, exactly once, on a chunk that holds no row.
#[test]
fn a_series_with_no_selectable_sample_closes_once() {
    // Every sample carries the stale marker, which suppresses the selection at every point.
    let stale = f64::from_bits(common_query::prometheus::PROMETHEUS_STALE_NAN_BITS);
    let out_grid = grid(0, 60_000, 3);
    let tree = NativeTree::try_new(instant(input(), 300_000, 0, Arc::clone(&out_grid))).unwrap();
    let set = ExecutionPlanMetricsSet::new();
    let mut execution = executor(&tree, pool(), &set);
    let schema = raw_schema();
    execution
        .push_raw(raw_chunk(&schema, &[0, 60_000], &[stale, stale], true))
        .unwrap();
    let out = run(&mut execution);

    assert_eq!(out.len(), 1, "the series has to close exactly once");
    let chunk = &out[0];
    assert!(chunk.is_last);
    assert!(
        chunk.series.time.is_empty(),
        "the closing chunk of a series that selected nothing holds a row"
    );
    assert_eq!(
        series_start(chunk),
        0,
        "the empty chunk is not addressed on the grid"
    );
    assert!(
        Arc::ptr_eq(chunk.series.time.grid().expect("a grid chunk"), &out_grid),
        "the empty chunk is addressed on a grid that is not the selection's output grid"
    );
    assert_eq!(
        chunk.series.metric,
        metric(),
        "the series lost its identity"
    );
    assert_eq!(
        count(&set, &NodePath::root(), NodeMetric::COMPLETED_SERIES),
        1,
        "the series did not close exactly once"
    );
}

/// A row one field fills and another misses stays one row, with a null where the field misses it.
#[test]
fn a_selected_row_keeps_the_fields_that_miss_it_null() {
    let out_grid = grid(0, 60_000, 2);
    let tree =
        NativeTree::try_new(instant(raw_input_of(2), 60_000, 0, Arc::clone(&out_grid))).unwrap();
    let schema = raw_schema_of(field_count(2));
    // Each field holds a value at one of the two rows and none at the other.
    let chunk = raw_chunk_columns(
        &schema,
        &[0, 60_000],
        vec![vec![Some(1.0), None], vec![None, Some(4.0)]],
        true,
    );
    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    execution.push_raw(chunk).unwrap();
    let out = run(&mut execution);

    assert_eq!(out.len(), 1);
    let chunk = &out[0];
    assert_eq!(
        chunk.series.time.len(),
        2,
        "a row no field of which holds a value was written down"
    );
    assert_eq!(field_values(chunk, 0), vec![Some(1.0), None]);
    assert_eq!(field_values(chunk, 1), vec![None, Some(4.0)]);
    assert_eq!(
        chunk
            .series
            .time
            .positions()
            .map(|(position, _)| position)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
}

/// A grid position nobody fills is not a row, and a hole does not close the series early.
///
/// The samples lie at the first and the last point of the grid with nothing in between, and the
/// lookback is too short to carry either one across: two rows, two runs, and no row per position
/// between them.
#[test]
fn a_position_no_field_fills_is_not_a_row() {
    let out_grid = grid(0, 60_000, 5);
    let tree = NativeTree::try_new(instant(input(), 1_000, 0, Arc::clone(&out_grid))).unwrap();
    let schema = raw_schema();
    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    execution
        .push_raw(raw_chunk(&schema, &[0], &[1.0], false))
        .unwrap();
    // The hole between the two samples decides nothing, and it does not end the series.
    assert!(
        execution.next_chunk().unwrap().is_none(),
        "a hole in the grid closed the series"
    );
    assert!(
        execution.state().reserved_bytes() > 0,
        "the open series is not reserved for"
    );
    execution
        .push_raw(raw_chunk(&schema, &[240_000], &[5.0], true))
        .unwrap();
    let out = run(&mut execution);

    assert_eq!(out.len(), 1);
    let chunk = &out[0];
    assert!(chunk.is_last);
    assert_eq!(
        chunk
            .series
            .time
            .positions()
            .map(|(position, _)| position)
            .collect::<Vec<_>>(),
        vec![0, 4],
        "the positions nobody fills were written down as rows"
    );
    assert_eq!(decided(&out), vec![(0, Some(1.0)), (4, Some(5.0))]);
    assert!(
        chunk.range().is_none(),
        "a chunk of two runs of positions was addressed as one range"
    );
}

/// An instant selection selects: the histogram it picks keeps its start timestamp.
///
/// The sample is read, converted into the histogram type the builder writes back, and written out
/// again, and that type used to have no start timestamp to write: a sample carrying
/// `start_timestamp = Some(1500)` at 2000ms came out `None`. It matters downstream -- a reader
/// detects a counter reset from a start timestamp that falls inside the range it reads -- and it is
/// the difference between selecting a sample and computing a value.
///
/// One source sample is selected by three grid points here, which is the case a per-row conversion
/// can get wrong twice: applied once and forgotten on the second use, or applied to a value that no
/// longer carries the timestamp.
#[test]
fn a_selected_histogram_keeps_its_start_timestamp() {
    let out_grid = grid(2000, 1, 3);
    let tree = NativeTree::try_new(instant(input(), 60_000, 0, Arc::clone(&out_grid))).unwrap();
    let schema = raw_schema();
    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    execution
        .push_raw(raw_histogram_chunk_with_start(
            &schema,
            &[2000],
            Some(1500),
            true,
        ))
        .unwrap();
    let out = run(&mut execution);

    assert_eq!(out.len(), 1);
    assert_eq!(
        histogram_starts(&out[0]),
        vec![Some(1500), Some(1500), Some(1500)],
        "the selected sample lost the start timestamp it was read with"
    );
}

/// Two series that differ only in an empty-valued label are two series, and both are answered.
///
/// The label set the runtime's occupancy table compares drops empty values, so a node that recorded
/// what it handed out under one would refuse the second series. A selection keeps the labels it
/// read, empty values included, so it keeps them apart and records nothing.
#[test]
fn two_series_differing_only_in_an_empty_label_are_both_answered() {
    let out_grid = grid(0, 60_000, 1);
    let tree = NativeTree::try_new(instant(input(), 300_000, 0, Arc::clone(&out_grid))).unwrap();
    let schema = raw_schema();
    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    // `zone=""` against an absent `zone`: one label set to everything that drops empty values.
    execution
        .push_raw(raw_chunk_of(
            &schema,
            b"a",
            host_metric(Some("")),
            &[0],
            &[1.0],
            true,
        ))
        .unwrap();
    execution
        .push_raw(raw_chunk_of(
            &schema,
            b"b",
            host_metric(None),
            &[0],
            &[2.0],
            true,
        ))
        .unwrap();
    let out = run(&mut execution);

    assert_eq!(out.len(), 2, "one of the two series was not answered");
    assert_eq!(out[0].series.metric, host_metric(Some("")));
    assert_eq!(out[1].series.metric, host_metric(None));
    assert_eq!(decided(&out[..1]), vec![(0, Some(1.0))]);
    assert_eq!(decided(&out[1..]), vec![(0, Some(2.0))]);
}

/// What a selection reserves is the series it has buffered, folded chunk by chunk.
///
/// The node holds a whole series, so its reservation grows with the chunks that series arrived in
/// and goes back when the series is answered. The figure is the one the queue of a source keeps: an
/// allocation two chunks share is counted once, which is what one pass over the buffer counts.
///
/// What this checks is the buffer alone. The workspace the gather of that buffer allocates -- the
/// axis, the selection's two arrays and the rows the builder keeps -- is reserved for while it is
/// live and is not part of this figure, so this test cannot see it, however many bytes it is:
/// [`an_instant_selection_reserves_the_workspace_it_gathers`] is the one that does.
#[test]
fn an_instant_selection_reserves_the_series_it_buffers() {
    let out_grid = grid(0, 60_000, 4);
    let tree = NativeTree::try_new(instant(input(), 300_000, 0, Arc::clone(&out_grid))).unwrap();
    let schema = raw_schema();
    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    let mut previous = 0;
    for timestamps in [vec![0, 60_000], vec![120_000], vec![180_000]] {
        let values = vec![1.0; timestamps.len()];
        execution
            .push_raw(raw_chunk(&schema, &timestamps, &values, false))
            .unwrap();
        assert!(
            execution.next_chunk().unwrap().is_none(),
            "a series was reported before its last chunk arrived"
        );
        let held = execution.state().reserved_bytes();
        let (folded, walked) = execution.state().instant_charge_pair(&NodePath::root());
        assert_eq!(
            folded, walked,
            "the reservation is not what one pass over the buffered series counts"
        );
        assert_eq!(
            held, folded,
            "the buffered series is not what is reserved for"
        );
        assert!(
            held > previous,
            "the chunk was not reserved for: {held} bytes held after {previous}"
        );
        previous = held;
    }
    execution
        .push_raw(raw_chunk(&schema, &[240_000], &[2.0], true))
        .unwrap();
    let out = run(&mut execution);
    assert_eq!(out.len(), 1);
    assert_eq!(
        execution.state().reserved_bytes(),
        0,
        "the series that was answered is still reserved for"
    );
}

/// The workspace the gather allocates is reserved for before it exists.
///
/// A selection decides its whole grid at once, so its workspace is proportional to the points of
/// the grid rather than to the samples of the series: the axis it concatenates, the source row and
/// evaluation timestamp it reports per point, and the rows the builder keeps. One sample selected
/// by a million points is the review's case: the two selection arrays alone are 16 MB and the built
/// rows are more, while the series the node holds is one sample of a few hundred bytes.
///
/// The pool here holds 8 KiB, which is enough for the buffered chunk and nothing near the
/// workspace, so the gather is refused instead of being performed. Nothing else refuses a million
/// points, so this is the reservation and not a point limit answering: without it the gather simply
/// runs.
#[test]
fn an_instant_selection_reserves_the_workspace_it_gathers() {
    let out_grid = grid(0, 1, 1_000_000);
    let tree = NativeTree::try_new(instant(input(), 1_000_000, 0, out_grid)).unwrap();
    let pool = bounded_pool(8 * 1024);
    let mut execution = executor(&tree, Arc::clone(&pool), &ExecutionPlanMetricsSet::new());
    let schema = raw_schema();
    execution
        .push_raw(raw_chunk(&schema, &[0], &[1.0], true))
        .unwrap();

    let error = execution.next_chunk().unwrap_err();
    assert!(
        matches!(error, DataFusionError::ResourcesExhausted(_)),
        "the gather's workspace was taken without asking the pool: {error}"
    );
    assert!(
        !error.to_string().contains("exceed limit"),
        "a point limit answered in the pool's place: {error}"
    );
    assert_eq!(
        pool.reserved(),
        0,
        "the stopped execution still holds bytes"
    );
    assert_eq!(
        execution.state().reserved_bytes(),
        0,
        "the workspace of the refused gather is still reserved for"
    );
    assert_eq!(
        execution.state().nodes_holding_instants(),
        0,
        "the stopped execution still holds the buffered series"
    );
    // A refused gather stops the execution rather than leaving it to be resumed.
    let again = execution.next_chunk().unwrap_err();
    assert!(
        again.to_string().contains("stopped after an earlier error"),
        "{again}"
    );
}

/// A grid longer than any point limit is charged for, and the pool is what refuses it.
///
/// `gather_bytes` used to return zero for a grid this long, so that a point limit inside the
/// kernel stayed the error reported and the workspace of a long gather was never reserved. The
/// estimate now applies to every gather, so the pool is asked before the kernel is and its refusal
/// is the one the caller sees — a bound in bytes, on the path that decides the points, rather than
/// a count that says nothing about what they cost.
#[test]
fn an_oversized_selection_is_refused_by_the_pool() {
    // One million and two points over two samples: longer than the short circuit used to allow,
    // so the estimate has to cover it rather than return zero.
    let out_grid = grid(0, 1, 1_000_002);
    let tree = NativeTree::try_new(instant(input(), 1_000_000, 0, out_grid)).unwrap();
    let pool = bounded_pool(8 * 1024);
    let mut execution = executor(&tree, Arc::clone(&pool), &ExecutionPlanMetricsSet::new());
    let schema = raw_schema();
    execution
        .push_raw(raw_chunk(&schema, &[0, 1_000_001], &[1.0, 2.0], true))
        .unwrap();

    let error = execution.next_chunk().unwrap_err();
    assert!(
        matches!(error, DataFusionError::ResourcesExhausted(_)),
        "the gather's workspace was taken without asking the pool: {error}"
    );
    assert!(
        !error.to_string().contains("exceed limit"),
        "a point limit answered in the pool's place: {error}"
    );
    // Nothing is dropped silently: the execution stops and gives its reservation back.
    assert_eq!(pool.reserved(), 0);
    assert_eq!(execution.state().reserved_bytes(), 0);
    assert_eq!(execution.state().nodes_holding_instants(), 0);
    let again = execution.next_chunk().unwrap_err();
    assert!(
        again.to_string().contains("stopped after an earlier error"),
        "{again}"
    );
}

/// An input that ends inside a series is an error, not an empty answer.
#[test]
fn an_input_that_ends_inside_a_series_is_reported() {
    let tree = NativeTree::try_new(instant(input(), 300_000, 0, grid(0, 60_000, 2))).unwrap();
    let pool = pool();
    let mut execution = executor(&tree, Arc::clone(&pool), &ExecutionPlanMetricsSet::new());
    let schema = raw_schema();
    execution
        .push_raw(raw_chunk(&schema, &[0], &[1.0], false))
        .unwrap();
    assert!(execution.next_chunk().unwrap().is_none());

    let error = execution.finish().unwrap_err();
    assert!(
        error.to_string().contains("ended before the last chunk"),
        "{error}"
    );
    assert_eq!(pool.reserved(), 0);
    assert_eq!(execution.state().reserved_bytes(), 0);
    assert_eq!(
        execution.state().nodes_holding_instants(),
        0,
        "the stopped execution still holds the buffered series"
    );
}

/// A selection combines one series into one axis, so a chunk of another series is refused.
///
/// The buffered chunks are read as one axis, so a chunk of a second series has nowhere to go: it
/// would be selected as though its samples were the open series'.
#[test]
fn a_chunk_of_another_series_is_refused() {
    let tree = NativeTree::try_new(instant(input(), 300_000, 0, grid(0, 60_000, 2))).unwrap();
    let schema = raw_schema();
    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    execution
        .push_raw(raw_chunk(&schema, &[0], &[1.0], false))
        .unwrap();
    assert!(execution.next_chunk().unwrap().is_none());
    // The second chunk carries another series' key.
    execution
        .push_raw(raw_chunk_of(
            &schema,
            b"series-b",
            metric(),
            &[60_000],
            &[2.0],
            true,
        ))
        .unwrap();

    let error = execution.next_chunk().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("a chunk of another series arrived"),
        "{error}"
    );
    assert_eq!(
        execution.state().reserved_bytes(),
        0,
        "the refused series is still reserved for"
    );
}

// ---------------------------------------------------------------------------
// Vector–vector binary operations, driven through `NativeExecutor`.
// ---------------------------------------------------------------------------

/// The matching most tests here start from: no label list, one-to-one, no `bool`.
fn binary_matching(op: BinaryOp) -> BinaryMatching {
    BinaryMatching {
        op,
        return_bool: false,
        labels: MatchingLabels::Ignoring(Vec::new()),
        cardinality: Cardinality::OneToOne,
    }
}

/// A binary node over `left` and `right`, bound the way the compiler binds it: the left side's
/// fields paired by position, on the grid both operands are evaluated on.
fn binary_of(left: NativeNode, right: NativeNode, matching: BinaryMatching) -> NativeNode {
    let left_schema = left.output.samples().expect("binary reads samples").clone();
    let right_schema = right
        .output
        .samples()
        .expect("binary reads samples")
        .clone();
    let fields = field_count(left_schema.fields.len().min(right_schema.fields.len()));
    NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(left),
            right: Box::new(right),
            matching,
        },
        output: NativeType::Samples(Arc::new(SeriesSchema {
            fields,
            eval_grid: Some(
                left_schema
                    .eval_grid
                    .clone()
                    .expect("binary reads evaluated samples"),
            ),
        })),
    }
}

/// A source of evaluated chunks over `count` float fields.
fn evaluated_input_of(count: usize, grid: Arc<EvalGrid>) -> NativeNode {
    let output = NativeType::Samples(Arc::new(SeriesSchema {
        fields: field_count(count),
        eval_grid: Some(grid),
    }));
    NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: output.clone(),
            ordering: SeriesOrdering::KeyThenTime,
        }),
        output,
    }
}

/// One evaluated chunk of `metric` whose fields carry `columns`, one column per field.
fn evaluated_columns(
    schema: &SeriesSchema,
    metric: MetricName,
    start: usize,
    columns: Vec<Vec<Option<f64>>>,
    is_last: bool,
) -> EvaluatedChunk {
    let rows = columns.first().map_or(0, Vec::len);
    evaluated_on(schema, metric, start, rows, columns, is_last)
}

/// One evaluated chunk of `metric` addressed on `rows` positions from `start`, carrying `columns`.
///
/// The axis and the columns are given independently so that a chunk whose two disagree can be built:
/// [`Series`] keeps its fields public, so nothing between a caller and a kernel refuses such a chunk
/// on its way in.
fn evaluated_on(
    schema: &SeriesSchema,
    metric: MetricName,
    start: usize,
    rows: usize,
    columns: Vec<Vec<Option<f64>>>,
    is_last: bool,
) -> EvaluatedChunk {
    let fields = SeriesFields::try_from_vec(
        columns
            .into_iter()
            .map(|values| Samples::from_float(Float64Array::from(values)))
            .collect(),
    )
    .unwrap();
    let range = GridRange::try_new(schema.eval_grid.clone().unwrap(), start, rows).unwrap();
    EvaluatedChunk::try_new(
        Series {
            metric,
            time: TimeAxis::Grid(range),
            fields,
        },
        is_last,
    )
    .unwrap()
}

/// One evaluated chunk of `metric` whose one field holds a `buckets`-wide histogram at each of
/// `len` positions from `start`.
fn evaluated_histograms(
    schema: &SeriesSchema,
    metric: MetricName,
    start: usize,
    buckets: i32,
    len: usize,
    is_last: bool,
) -> EvaluatedChunk {
    let mut builder = HistogramBatchBuilder::new();
    let indices: UInt32Array = (0..len)
        .map(|_| Some(push_histogram(&mut builder, buckets)))
        .collect();
    let samples = Samples::try_from_histogram(indices, builder.finish().unwrap()).unwrap();
    let range = GridRange::try_new(schema.eval_grid.clone().unwrap(), start, len).unwrap();
    EvaluatedChunk::try_new(
        Series {
            metric,
            time: TimeAxis::Grid(range),
            fields: SeriesFields::one(samples),
        },
        is_last,
    )
    .unwrap()
}

/// The `count` of the histogram at every carried row of a chunk.
fn histogram_counts(chunk: &EvaluatedChunk) -> Vec<f64> {
    let mut cursor = chunk.series.fields.as_slice()[0].cursor();
    chunk
        .series
        .time
        .positions()
        .filter_map(
            |(_, row)| match cursor.at(row).expect("the axis and the fields align") {
                SampleRef::Histogram(view) => Some(view.count),
                _ => None,
            },
        )
        .collect()
}

/// A source bound in label mode: `name{job="api",pod="a"}` and the other series here.
fn pod_metric(name: &str, job: &str, pod: &str) -> MetricName {
    MetricName::try_new(
        Some(name.into()),
        vec![
            Label {
                name: "job".into(),
                value: job.into(),
            },
            Label {
                name: "pod".into(),
                value: pod.into(),
            },
        ],
    )
    .unwrap()
}

/// The one side of a `group_left` match: `one{job=...,instance=...}`.
fn one_metric(job: &str, instance: &str) -> MetricName {
    MetricName::try_new(
        Some("one".into()),
        vec![
            Label {
                name: "job".into(),
                value: job.into(),
            },
            Label {
                name: "instance".into(),
                value: instance.into(),
            },
        ],
    )
    .unwrap()
}

/// A tree of two evaluated sources over `points` points under a binary node, with the two paths a
/// caller queues at.
fn binary_tree(op: BinaryOp, points: u64) -> (NativeTree, Arc<EvalGrid>, NodePath, NodePath) {
    let grid = grid(0, 60_000, points);
    let root = binary_of(
        evaluated_input_of(1, Arc::clone(&grid)),
        evaluated_input_of(1, Arc::clone(&grid)),
        binary_matching(op),
    );
    (
        NativeTree::try_new(root).unwrap(),
        grid,
        NodePath::root().left(),
        NodePath::root().right(),
    )
}

/// Two evaluated sources under a binary node add the series they match, point by point.
#[test]
fn a_binary_of_two_inputs_adds_the_matched_series() {
    let (tree, grid, left, right) = binary_tree(BinaryOp::Add, 4);
    let schema = evaluated_schema(grid);
    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    execution
        .push_evaluated_at(
            &left,
            evaluated_chunk(&schema, 0, vec![1.0, 2.0, 3.0, 4.0], true),
        )
        .unwrap();
    execution
        .push_evaluated_at(
            &right,
            evaluated_chunk(&schema, 0, vec![10.0, 20.0, 30.0, 40.0], true),
        )
        .unwrap();
    assert_eq!(
        addressed(&run(&mut execution)),
        vec![(0, vec![Some(11.0), Some(22.0), Some(33.0), Some(44.0)])]
    );
}

/// Neither the order the sides arrive in nor the chunk boundaries of one side change the result.
///
/// A binary node matches whole series, so the two deliveries below are the same vector: one splits
/// the right side over two chunks, the other queues the right side first. `is_last` is the only
/// thing that ends a series, so a chunk boundary carries no meaning of its own.
#[test]
fn a_binary_result_does_not_depend_on_arrival_order_or_chunk_boundaries() {
    let (tree, grid, left, right) = binary_tree(BinaryOp::Sub, 4);
    let schema = evaluated_schema(Arc::clone(&grid));
    let mut split = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    split
        .push_evaluated_at(
            &left,
            evaluated_chunk(&schema, 0, vec![1.0, 2.0, 3.0, 4.0], true),
        )
        .unwrap();
    split
        .push_evaluated_at(&right, evaluated_chunk(&schema, 0, vec![10.0, 20.0], false))
        .unwrap();
    split
        .push_evaluated_at(&right, evaluated_chunk(&schema, 2, vec![30.0, 40.0], true))
        .unwrap();
    let split = decided(&run(&mut split));

    let mut reversed = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    reversed
        .push_evaluated_at(&right, evaluated_chunk(&schema, 0, vec![10.0, 20.0], false))
        .unwrap();
    reversed
        .push_evaluated_at(
            &left,
            evaluated_chunk(&schema, 0, vec![1.0, 2.0, 3.0, 4.0], true),
        )
        .unwrap();
    reversed
        .push_evaluated_at(&right, evaluated_chunk(&schema, 2, vec![30.0, 40.0], true))
        .unwrap();
    let reversed = decided(&run(&mut reversed));

    let expected = vec![
        (0, Some(-9.0)),
        (1, Some(-18.0)),
        (2, Some(-27.0)),
        (3, Some(-36.0)),
    ];
    assert_eq!(split, expected);
    assert_eq!(reversed, expected);
}

/// Two series that arrive in separate chunks and hold a value at the same point are refused.
///
/// Each chunk is a series of its own, closed by its own `is_last`, and the two claim one label set:
/// Prometheus reports the duplicate at the first point both are present at, which no chunk boundary
/// can resolve. Two such series whose positions do not meet are accepted instead, as
/// `a_sparse_binary_keeps_the_shared_positions` shows.
#[test]
fn a_cross_chunk_duplicate_on_one_side_is_refused() {
    let (tree, grid, left, right) = binary_tree(BinaryOp::Add, 4);
    let schema = evaluated_schema(grid);
    let pool = pool();
    let mut execution = executor(&tree, Arc::clone(&pool), &ExecutionPlanMetricsSet::new());
    execution
        .push_evaluated_at(&left, evaluated_chunk(&schema, 0, vec![1.0, 2.0], true))
        .unwrap();
    // The same label set twice on the right, each delivered in the chunk that closes it, and both
    // holding values at positions 0 and 1.
    execution
        .push_evaluated_at(&right, evaluated_chunk(&schema, 0, vec![10.0, 20.0], true))
        .unwrap();
    execution
        .push_evaluated_at(&right, evaluated_chunk(&schema, 0, vec![30.0, 40.0], true))
        .unwrap();

    let error = execution.finish().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("found duplicate series for the match group"),
        "{error}"
    );
    assert_eq!(pool.reserved(), 0, "the refused operands are still held");
    assert_eq!(execution.state().reserved_bytes(), 0);
}

/// An operand that closes without a row contributes nothing, and pairing it is not an error.
#[test]
fn an_empty_side_yields_no_series() {
    let (tree, grid, left, right) = binary_tree(BinaryOp::Add, 2);
    let schema = evaluated_schema(grid);
    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    // A closing chunk with no row: the series ends before its first point.
    execution
        .push_evaluated_at(
            &left,
            evaluated_chunk_of(&schema, metric(), 0, Vec::new(), true),
        )
        .unwrap();
    execution
        .push_evaluated_at(&right, evaluated_chunk(&schema, 0, vec![1.0, 2.0], true))
        .unwrap();
    assert!(run(&mut execution).is_empty());
}

/// An empty closing chunk after rows is not the series going backwards.
#[test]
fn an_empty_closing_chunk_does_not_rewind_a_series() {
    let (tree, grid, left, right) = binary_tree(BinaryOp::Add, 2);
    let schema = evaluated_schema(grid);
    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    execution
        .push_evaluated_at(&left, evaluated_chunk(&schema, 0, vec![1.0], false))
        .unwrap();
    // The chunk that closes the series holds no row, so where it addresses says nothing.
    execution
        .push_evaluated_at(
            &left,
            evaluated_chunk_of(&schema, metric(), 0, Vec::new(), true),
        )
        .unwrap();
    execution
        .push_evaluated_at(&right, evaluated_chunk(&schema, 0, vec![10.0, 20.0], true))
        .unwrap();
    assert_eq!(decided(&run(&mut execution)), vec![(0, Some(11.0))]);
}

/// Two sparse operands meet at the positions both carry.
#[test]
fn a_sparse_binary_keeps_the_shared_positions() {
    let (tree, grid, left, right) = binary_tree(BinaryOp::Add, 4);
    let schema = evaluated_schema(grid);
    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    // The left side decides positions 0 and 2 only, in two chunks.
    execution
        .push_evaluated_at(&left, evaluated_chunk(&schema, 0, vec![1.0], false))
        .unwrap();
    execution
        .push_evaluated_at(&left, evaluated_chunk(&schema, 2, vec![3.0], true))
        .unwrap();
    execution
        .push_evaluated_at(
            &right,
            evaluated_chunk(&schema, 0, vec![10.0, 20.0, 30.0, 40.0], true),
        )
        .unwrap();
    assert_eq!(
        decided(&run(&mut execution)),
        vec![(0, Some(11.0)), (2, Some(33.0))]
    );
}

/// Fields are paired by position, and a field that misses a position keeps it null.
#[test]
fn a_binary_pairs_the_fields_of_each_operand() {
    let grid = grid(0, 60_000, 2);
    let root = binary_of(
        evaluated_input_of(2, Arc::clone(&grid)),
        evaluated_input_of(2, Arc::clone(&grid)),
        binary_matching(BinaryOp::Add),
    );
    let tree = NativeTree::try_new(root).unwrap();
    let schema = Arc::new(SeriesSchema {
        fields: field_count(2),
        eval_grid: Some(grid),
    });
    let set = ExecutionPlanMetricsSet::new();
    let mut execution = executor(&tree, pool(), &set);
    execution
        .push_evaluated_at(
            &NodePath::root().left(),
            evaluated_columns(
                &schema,
                metric(),
                0,
                vec![vec![Some(1.0), None], vec![Some(5.0), Some(6.0)]],
                true,
            ),
        )
        .unwrap();
    execution
        .push_evaluated_at(
            &NodePath::root().right(),
            evaluated_columns(
                &schema,
                metric(),
                0,
                vec![vec![Some(10.0), Some(20.0)], vec![Some(7.0), Some(8.0)]],
                true,
            ),
        )
        .unwrap();
    let out = run(&mut execution);
    assert_eq!(out.len(), 1);
    // Position 1 holds no left value of field 0, so that field keeps it null while field 1 has one.
    assert_eq!(field_values(&out[0], 0), vec![Some(11.0), None]);
    assert_eq!(field_values(&out[0], 1), vec![Some(12.0), Some(14.0)]);
    // The result decided two positions and holds three values: the two positions are what the node
    // evaluated, and a value is what a field sample holds, which is one fewer for the null.
    let root_path = NodePath::root();
    assert_eq!(
        count(&set, &root_path, NodeMetric::EVALUATED_GRID_POINTS),
        2
    );
    assert_eq!(count(&set, &root_path, NodeMetric::OUTPUT_VALUES), 3);
}

/// A comparison without `bool` filters the points it does not hold at; with `bool` it reports both.
#[test]
fn a_comparison_filters_and_a_bool_comparison_reports_both_outcomes() {
    let grid = grid(0, 60_000, 4);
    let schema = evaluated_schema(Arc::clone(&grid));
    for (return_bool, expected) in [
        (false, vec![(2, Some(3.0)), (3, Some(4.0))]),
        (
            true,
            vec![
                (0, Some(0.0)),
                (1, Some(0.0)),
                (2, Some(1.0)),
                (3, Some(1.0)),
            ],
        ),
    ] {
        let matching = BinaryMatching {
            op: BinaryOp::Gt,
            return_bool,
            labels: MatchingLabels::Ignoring(Vec::new()),
            cardinality: Cardinality::OneToOne,
        };
        let root = binary_of(
            evaluated_input_of(1, Arc::clone(&grid)),
            evaluated_input_of(1, Arc::clone(&grid)),
            matching,
        );
        let tree = NativeTree::try_new(root).unwrap();
        let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
        execution
            .push_evaluated_at(
                &NodePath::root().left(),
                evaluated_chunk(&schema, 0, vec![1.0, 2.0, 3.0, 4.0], true),
            )
            .unwrap();
        execution
            .push_evaluated_at(
                &NodePath::root().right(),
                evaluated_chunk(&schema, 0, vec![2.0, 2.0, 2.0, 2.0], true),
            )
            .unwrap();
        assert_eq!(
            decided(&run(&mut execution)),
            expected,
            "bool={return_bool}"
        );
    }
}

/// `group_left(instance)` keeps every many-side series and copies the label from its match.
#[test]
fn a_group_modifier_copies_the_one_side_label() {
    let grid = grid(0, 60_000, 2);
    let matching = BinaryMatching {
        op: BinaryOp::Add,
        return_bool: false,
        labels: MatchingLabels::On(vec![Arc::from("job")]),
        cardinality: Cardinality::ManyToOne(vec![Arc::from("instance")]),
    };
    let root = binary_of(
        evaluated_input_of(1, Arc::clone(&grid)),
        evaluated_input_of(1, Arc::clone(&grid)),
        matching,
    );
    let tree = NativeTree::try_new(root).unwrap();
    let schema = evaluated_schema(Arc::clone(&grid));
    let (left, right) = (NodePath::root().left(), NodePath::root().right());
    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    for (metric, values) in [
        (pod_metric("a", "api", "p1"), vec![Some(1.0), Some(2.0)]),
        (pod_metric("b", "db", "p2"), vec![Some(3.0), Some(4.0)]),
    ] {
        execution
            .push_evaluated_at(&left, evaluated_chunk_of(&schema, metric, 0, values, true))
            .unwrap();
    }
    for (metric, values) in [
        (one_metric("api", "i1"), vec![Some(10.0), Some(20.0)]),
        (one_metric("db", "i2"), vec![Some(100.0), Some(200.0)]),
    ] {
        execution
            .push_evaluated_at(&right, evaluated_chunk_of(&schema, metric, 0, values, true))
            .unwrap();
    }

    // The result is compared as label sets, so the two series are sorted by the labels they carry:
    // the value lists of `f64` have no total order, and none is needed here.
    let mut results: Vec<(Vec<(String, String)>, Vec<Option<f64>>)> = run(&mut execution)
        .iter()
        .map(|chunk| {
            let mut tags: Vec<(String, String)> = chunk
                .series
                .metric
                .tags()
                .iter()
                .map(|tag| (tag.name.to_string(), tag.value.to_string()))
                .collect();
            tags.sort();
            (tags, sample_values(chunk))
        })
        .collect();
    results.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(
        results,
        vec![
            (
                vec![
                    ("instance".to_string(), "i1".to_string()),
                    ("job".to_string(), "api".to_string()),
                    ("pod".to_string(), "p1".to_string()),
                ],
                vec![Some(11.0), Some(22.0)],
            ),
            (
                vec![
                    ("instance".to_string(), "i2".to_string()),
                    ("job".to_string(), "db".to_string()),
                    ("pod".to_string(), "p2".to_string()),
                ],
                vec![Some(103.0), Some(204.0)],
            ),
        ]
    );
}

/// Two native histograms are added bucket by bucket.
#[test]
fn a_binary_adds_the_native_histograms_of_both_sides() {
    let (tree, grid, left, right) = binary_tree(BinaryOp::Add, 2);
    let schema = evaluated_schema(grid);
    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    execution
        .push_evaluated_at(
            &left,
            evaluated_histograms(&schema, metric(), 0, 4, 2, true),
        )
        .unwrap();
    execution
        .push_evaluated_at(
            &right,
            evaluated_histograms(&schema, metric(), 0, 4, 2, true),
        )
        .unwrap();
    let out = run(&mut execution);
    assert_eq!(out.len(), 1);
    // Each side holds four buckets of one count at each point, so the sum counts eight.
    assert_eq!(histogram_counts(&out[0]), vec![8.0, 8.0]);
}

/// Nothing is decided before the input ends; `finish` is what hands the result over.
#[test]
fn a_binary_yields_nothing_until_the_input_finishes() {
    let (tree, grid, left, right) = binary_tree(BinaryOp::Add, 2);
    let schema = evaluated_schema(grid);
    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    execution
        .push_evaluated_at(&left, evaluated_chunk(&schema, 0, vec![1.0, 2.0], true))
        .unwrap();
    execution
        .push_evaluated_at(&right, evaluated_chunk(&schema, 0, vec![10.0, 20.0], true))
        .unwrap();
    assert!(
        execution.next_chunk().unwrap().is_none(),
        "a binary node answered before its input ended"
    );
    assert_eq!(
        addressed(&execution.finish().unwrap()),
        vec![(0, vec![Some(11.0), Some(22.0)])]
    );
}

/// A truncated operand is refused before the kernel is handed anything.
///
/// The right side also carries a duplicate series, which the kernel would report: the truncation is
/// what comes back, so the kernel never ran over half a vector.
#[test]
fn a_truncated_operand_is_refused_before_the_kernel_runs() {
    let (tree, grid, left, right) = binary_tree(BinaryOp::Add, 4);
    let schema = evaluated_schema(grid);
    let pool = pool();
    let mut execution = executor(&tree, Arc::clone(&pool), &ExecutionPlanMetricsSet::new());
    // The left side holds a duplicate the kernel would report if it were reached.
    execution
        .push_evaluated_at(&left, evaluated_chunk(&schema, 0, vec![1.0, 2.0], true))
        .unwrap();
    execution
        .push_evaluated_at(&left, evaluated_chunk(&schema, 0, vec![3.0, 4.0], true))
        .unwrap();
    // The right side never hands over the chunk that closes its series.
    execution
        .push_evaluated_at(&right, evaluated_chunk(&schema, 0, vec![10.0], false))
        .unwrap();

    let error = execution.finish().unwrap_err();
    assert!(
        error.to_string().contains("ended before the last chunk"),
        "{error}"
    );
    assert_eq!(pool.reserved(), 0);
    assert_eq!(execution.state().nodes_holding_binary(), 0);
}

/// A workspace the pool cannot hold is refused, and what the node retained is given back with it.
#[test]
fn an_oversized_binary_workspace_is_refused_by_the_pool() {
    let points = 100_000;
    let (tree, grid, left, right) = binary_tree(BinaryOp::Add, points);
    let schema = evaluated_schema(grid);
    // Enough for the two one-row operands, far less than the presence arrays the kernel builds.
    let pool = bounded_pool(60_000);
    let mut execution = executor(&tree, Arc::clone(&pool), &ExecutionPlanMetricsSet::new());
    execution
        .push_evaluated_at(&left, evaluated_chunk(&schema, 0, vec![1.0], true))
        .unwrap();
    execution
        .push_evaluated_at(&right, evaluated_chunk(&schema, 0, vec![10.0], true))
        .unwrap();
    let error = execution.finish().unwrap_err();
    assert!(
        matches!(error, DataFusionError::ResourcesExhausted(_)),
        "a point limit answered in the pool's place: {error}"
    );
    assert_eq!(pool.reserved(), 0);
    assert_eq!(execution.state().reserved_bytes(), 0);
    assert_eq!(execution.state().nodes_holding_binary(), 0);
}

/// A pool that cannot hold both operands refuses the second one, and the first is given back.
///
/// Both chunks sit in their input's queue until the input ends, so the reservation covers them at
/// once: the second one is what the pool refuses, and stopping the execution returns what the first
/// one held.
#[test]
fn an_oversized_binary_operand_is_refused_by_the_pool() {
    let (tree, grid, left, right) = binary_tree(BinaryOp::Add, 100_000);
    let schema = evaluated_schema(grid);
    let pool = bounded_pool(60_000);
    let mut execution = executor(&tree, Arc::clone(&pool), &ExecutionPlanMetricsSet::new());
    execution
        .push_evaluated_at(&left, evaluated_chunk(&schema, 0, vec![1.0], true))
        .unwrap();
    let values: Vec<f64> = (0..50_000).map(f64::from).collect();
    let error = execution
        .push_evaluated_at(&right, evaluated_chunk(&schema, 0, values, true))
        .unwrap_err();
    assert!(
        matches!(error, DataFusionError::ResourcesExhausted(_)),
        "an operand limit answered in the pool's place: {error}"
    );
    assert_eq!(pool.reserved(), 0);
    assert_eq!(execution.state().reserved_bytes(), 0);
}

/// Two chunks of one series that share one Arrow allocation are charged for it once, and the pool
/// is asked for the increment rather than for the chunk.
///
/// A source hands over sibling slices of one batch, so both chunks reference one allocation: the
/// second costs the operand the descriptor that refers to it and nothing else. The budget here is
/// exactly what the operand holds, so a chunk reserved for on top of the allocation it shares would
/// be refused even though the node never holds more than it reserved.
#[test]
fn a_binary_operand_is_charged_the_increment_of_a_shared_allocation() {
    let grid = grid(0, 60_000, 40_000);
    let schema = evaluated_schema(Arc::clone(&grid));
    let batch = Float64Array::from((0..40_000).map(f64::from).collect::<Vec<_>>());
    let first = evaluated_array_chunk_of(&schema, metric(), 0, batch.slice(0, 20_000), false);
    let second =
        evaluated_array_chunk_of(&schema, metric(), 20_000, batch.slice(20_000, 20_000), true);

    // What one chunk costs the operand on its own: the descriptor and the allocation, because a
    // pass over one chunk does not know what the operand already holds.
    let alone = |chunk: &EvaluatedChunk| {
        let mut charged = ChargedAllocations::default();
        std::mem::size_of::<Series>() + fields_bytes(&chunk.series.fields, &mut charged)
    };
    let (first_alone, second_alone) = (alone(&first), alone(&second));
    // What both cost together: the one allocation they are slices of, and a descriptor each.
    let mut charged = ChargedAllocations::default();
    let held = [&first, &second]
        .iter()
        .map(|chunk| {
            std::mem::size_of::<Series>() + fields_bytes(&chunk.series.fields, &mut charged)
        })
        .fold(0usize, usize::saturating_add);
    assert!(
        first_alone + second_alone > held,
        "the two chunks are not slices of one allocation: {first_alone} and {second_alone} bytes \
         add up to no more than the {held} they are charged together, so nothing would be charged \
         twice"
    );

    // The budget is the figure the operand holds, which a chunk reserved for on top of the
    // allocation it shares would not fit into.
    let pool = bounded_pool(held);
    let mut runtime = binary_runtime(&pool, &schema);
    let before = runtime.buffer.held;
    runtime
        .retain_binary(&NodePath::root(), 0, NativeChunk::Evaluated(first))
        .unwrap();
    assert_eq!(
        runtime.buffer.held - before,
        first_alone,
        "the first chunk of an allocation does not cost the descriptor and the allocation"
    );
    let before = runtime.buffer.held;
    runtime
        .retain_binary(&NodePath::root(), 0, NativeChunk::Evaluated(second))
        .unwrap();
    assert_eq!(
        runtime.buffer.held - before,
        std::mem::size_of::<Series>(),
        "a chunk of the allocation the operand already retains cost more than its descriptor"
    );
    assert_eq!(
        runtime.buffer.held, held,
        "the operand does not reserve what it holds"
    );
    assert_eq!(pool.reserved(), held);

    // An allocation the operand does not hold yet is still charged: the increment is the descriptor
    // and the new allocation, not the descriptor alone.
    let own = evaluated_chunk(&schema, 0, vec![1.0, 2.0], true);
    assert_eq!(
        runtime
            .binary
            .as_ref()
            .unwrap()
            .pending(&NodePath::root(), 0, &own)
            .unwrap(),
        alone(&own),
        "a chunk of an allocation the operand does not hold is not charged for it"
    );
}

/// The executor's binary node never reserves a chunk on top of the allocation it shares: the peak
/// of its own reservation is the operand it retains, not that plus the chunk it was handed.
///
/// The source queue holds one chunk while the node retains the other, so what is read here is the
/// binary node's own figure rather than the pool's: the overlap of the two nodes is a charge of the
/// input and says nothing about what the operand costs.
#[test]
fn a_binary_beneath_the_executor_does_not_reserve_a_shared_chunk_on_top() {
    let (tree, grid, left, right) = binary_tree(BinaryOp::Add, 4_000);
    let schema = evaluated_schema(Arc::clone(&grid));
    let batch = Float64Array::from(vec![1.0; 4_000]);
    let first = evaluated_array_chunk_of(&schema, metric(), 0, batch.slice(0, 2_000), false);
    let second =
        evaluated_array_chunk_of(&schema, metric(), 2_000, batch.slice(2_000, 2_000), true);
    // What the left operand holds once both chunks are retained, counted the way its ledger counts:
    // the one allocation the chunks are slices of, and one descriptor per chunk.
    let mut charged = ChargedAllocations::default();
    let retained = [&first, &second]
        .iter()
        .map(|chunk| {
            std::mem::size_of::<Series>() + fields_bytes(&chunk.series.fields, &mut charged)
        })
        .fold(0usize, usize::saturating_add);

    let set = ExecutionPlanMetricsSet::new();
    let mut execution = executor(&tree, bounded_pool(1 << 22), &set);
    execution.push_evaluated_at(&left, first).unwrap();
    execution.push_evaluated_at(&left, second).unwrap();
    // Nothing is queued on the right, so the pull retains both chunks of the left operand and
    // nothing else: the kernel has not run and the node holds what it retains.
    assert!(execution.next_chunk().unwrap().is_none());
    let peak = NodePath::root().partitioned_metric_name(NodeMetric::PEAK_RESERVED_BYTES, 0);
    assert_eq!(
        gauge(&set, &peak),
        retained,
        "the binary node reserved the chunk it was handed on top of the allocation it retains"
    );

    // The right operand matches the label set, so the operation answers every point of the grid.
    execution
        .push_evaluated_at(&right, evaluated_chunk(&schema, 0, vec![2.0; 4_000], true))
        .unwrap();
    assert_eq!(
        addressed(&run(&mut execution)),
        vec![(0, vec![Some(3.0); 4_000])]
    );
}

/// A kernel error and dropping the execution both give back everything the node reserved.
#[test]
fn a_kernel_error_and_a_drop_release_the_binary_reservation() {
    let (tree, grid, left, right) = binary_tree(BinaryOp::Add, 2);
    let schema = evaluated_schema(grid);
    let pool = pool();
    let mut execution = executor(&tree, Arc::clone(&pool), &ExecutionPlanMetricsSet::new());
    execution
        .push_evaluated_at(&left, evaluated_chunk(&schema, 0, vec![1.0, 2.0], true))
        .unwrap();
    // The same label set twice on the right, both holding a value at position 0.
    execution
        .push_evaluated_at(&right, evaluated_chunk(&schema, 0, vec![1.0], true))
        .unwrap();
    execution
        .push_evaluated_at(&right, evaluated_chunk(&schema, 0, vec![2.0], true))
        .unwrap();
    let error = execution.finish().unwrap_err();
    assert!(error.to_string().contains("duplicate series"), "{error}");
    assert_eq!(pool.reserved(), 0, "the kernel error kept its reservation");
    assert_eq!(execution.state().reserved_bytes(), 0);

    // A dropped execution returns its reservation without being told to.
    let mut dropped = executor(&tree, Arc::clone(&pool), &ExecutionPlanMetricsSet::new());
    dropped
        .push_evaluated_at(&left, evaluated_chunk(&schema, 0, vec![1.0], true))
        .unwrap();
    assert!(pool.reserved() > 0, "the operand reserved nothing");
    drop(dropped);
    assert_eq!(pool.reserved(), 0);
}

/// A queued chunk of a two-input tree has to name the input it belongs to.
#[test]
fn a_queued_chunk_names_its_input() {
    let (tree, grid, left, _right) = binary_tree(BinaryOp::Add, 2);
    let schema = evaluated_schema(grid);
    let chunk = evaluated_chunk(&schema, 0, vec![1.0, 2.0], true);

    // Unnamed: two inputs make "the input" ambiguous, so nothing is queued at either.
    let mut ambiguous = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    let error = ambiguous.push_evaluated(chunk.clone()).unwrap_err();
    assert!(error.to_string().contains("has 2 inputs"), "{error}");

    // A path that names the binary node rather than an input.
    let mut wrong_node = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    let error = wrong_node
        .push_evaluated_at(&NodePath::root(), chunk.clone())
        .unwrap_err();
    assert!(error.to_string().contains("only an input node"), "{error}");

    // A path that names no node at all.
    let mut missing = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    let error = missing
        .push_evaluated_at(&NodePath::root().right().right(), chunk)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("has no node at root.right.right"),
        "{error}"
    );

    // An input bound to evaluated samples refuses raw ones, naming the input it was queued at.
    let mut fresh = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    let error = fresh
        .push_raw_at(&left, raw_chunk(&raw_schema(), &[0], &[1.0], true))
        .unwrap_err();
    assert!(
        error.to_string().contains(&format!(
            "the native source at {left} is bound to an evaluated series"
        )),
        "{error}"
    );
    assert_eq!(left, NodePath::root().left());
}

/// A map above a binary, and a window above a binary, pull its result through their own loops.
#[test]
fn a_binary_beneath_a_map_and_a_window_is_pulled_recursively() {
    // A map over the binary result: the subtraction is mapped to its magnitude.
    let inner_grid = grid(0, 60_000, 2);
    let schema = evaluated_schema(Arc::clone(&inner_grid));
    // The map and the window below are the outermost nodes, so the binary node is one step down
    // from the root and its two inputs one step below that: `root.input.left` and
    // `root.input.right`, not `root.left` and `root.right`.
    let (left, right) = (
        NodePath::root().input().left(),
        NodePath::root().input().right(),
    );
    let root = binary_of(
        evaluated_input_of(1, Arc::clone(&inner_grid)),
        evaluated_input_of(1, Arc::clone(&inner_grid)),
        binary_matching(BinaryOp::Sub),
    );
    let mapped = NativeTree::try_new(map(root, vec![MathOp::Abs])).unwrap();
    let mut execution = executor(&mapped, pool(), &ExecutionPlanMetricsSet::new());
    execution
        .push_evaluated_at(&left, evaluated_chunk(&schema, 0, vec![1.0, 2.0], true))
        .unwrap();
    execution
        .push_evaluated_at(&right, evaluated_chunk(&schema, 0, vec![4.0, 5.0], true))
        .unwrap();
    assert_eq!(
        decided(&run(&mut execution)),
        vec![(0, Some(3.0)), (1, Some(3.0))]
    );

    // A window over the binary result, evaluated on its own outer grid.
    let outer = grid(0, 60_000, 2);
    let root = binary_of(
        evaluated_input_of(1, Arc::clone(&inner_grid)),
        evaluated_input_of(1, Arc::clone(&inner_grid)),
        binary_matching(BinaryOp::Add),
    );
    let windowed = NativeTree::try_new(window_with(
        root,
        RangeFunction::LastOverTime,
        120_000,
        Some(Arc::clone(&inner_grid)),
        Arc::clone(&outer),
    ))
    .unwrap();
    let mut execution = executor(&windowed, pool(), &ExecutionPlanMetricsSet::new());
    execution
        .push_evaluated_at(&left, evaluated_chunk(&schema, 0, vec![1.0, 2.0], true))
        .unwrap();
    execution
        .push_evaluated_at(&right, evaluated_chunk(&schema, 0, vec![10.0, 20.0], true))
        .unwrap();
    // The outer window decides each point from the binary result, on its own grid.
    assert_eq!(
        decided(&run(&mut execution)),
        vec![(0, Some(11.0)), (1, Some(22.0))]
    );
}

/// A binary over two windows reads what each window decided, and over two selections what each
/// selection picked.
#[test]
fn a_binary_over_two_windows_and_selections_is_executed() {
    let inner = grid(0, 60_000, 2);
    let schema = raw_schema();

    let root = binary_of(
        window_with(
            input(),
            RangeFunction::LastOverTime,
            60_000,
            None,
            Arc::clone(&inner),
        ),
        window_with(
            input(),
            RangeFunction::LastOverTime,
            60_000,
            None,
            Arc::clone(&inner),
        ),
        binary_matching(BinaryOp::Add),
    );
    let tree = NativeTree::try_new(root).unwrap();
    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    for (path, key, values) in [
        (
            NodePath::root().left().input(),
            b"series-a".as_slice(),
            [1.0, 2.0],
        ),
        (
            NodePath::root().right().input(),
            b"series-b".as_slice(),
            [10.0, 20.0],
        ),
    ] {
        execution
            .push_raw_at(
                &path,
                raw_chunk_of(&schema, key, metric(), &[0, 60_000], &values, true),
            )
            .unwrap();
    }
    assert_eq!(
        decided(&run(&mut execution)),
        vec![(0, Some(11.0)), (1, Some(22.0))]
    );

    // The same two grid points, selected from raw samples on each side.
    let root = binary_of(
        instant(input(), 300_000, 0, Arc::clone(&inner)),
        instant(input(), 300_000, 0, Arc::clone(&inner)),
        binary_matching(BinaryOp::Add),
    );
    let tree = NativeTree::try_new(root).unwrap();
    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    for (path, key, value) in [
        (NodePath::root().left().input(), b"series-a".as_slice(), 1.0),
        (
            NodePath::root().right().input(),
            b"series-b".as_slice(),
            10.0,
        ),
    ] {
        execution
            .push_raw_at(
                &path,
                raw_chunk_of(&schema, key, metric(), &[0], &[value], true),
            )
            .unwrap();
    }
    // The one sample of each side is the last at or before both grid points.
    assert_eq!(
        decided(&run(&mut execution)),
        vec![(0, Some(11.0)), (1, Some(11.0))]
    );
}

/// The chain Prometheus evaluates as `abs(instant - last_over_time)`, with the raw samples of each
/// side and the literal rows it decides.
///
/// The left side is a selection reading back 1001 ms and the right side a 1000 ms window, on a grid
/// of 1000 ms steps: the two report different samples of the two raw series, which is what the
/// subtraction then pairs.
#[test]
fn a_binary_of_a_selection_and_a_window_matches_prometheus() {
    let out = grid(0, 1_000, 4);
    let left = instant(input(), 1_001, 0, Arc::clone(&out));
    let right = window_with(
        input(),
        RangeFunction::LastOverTime,
        1_000,
        None,
        Arc::clone(&out),
    );
    let tree = NativeTree::try_new(map(
        binary_of(left, right, binary_matching(BinaryOp::Sub)),
        vec![MathOp::Abs],
    ))
    .unwrap();
    let schema = raw_schema();
    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    execution
        .push_raw_at(
            &NodePath::root().input().left().input(),
            raw_chunk_of(
                &schema,
                b"left_raw",
                named_metric("left_raw", "x"),
                &[0, 500, 2_000],
                &[-2.0, -4.0, -8.0],
                true,
            ),
        )
        .unwrap();
    execution
        .push_raw_at(
            &NodePath::root().input().right().input(),
            raw_chunk_of(
                &schema,
                b"right_raw",
                named_metric("right_raw", "x"),
                &[0, 1_000, 2_000, 3_000],
                &[1.0, 3.0, 2.0, 4.0],
                true,
            ),
        )
        .unwrap();
    assert_eq!(
        decided(&run(&mut execution)),
        vec![
            (0, Some(3.0)),
            (1, Some(7.0)),
            (2, Some(10.0)),
            (3, Some(12.0)),
        ]
    );
}

/// An operand chunk whose series changes before it ends is refused, naming the side and the node.
///
/// A binary node matches whole series, so the pairs of a series that is still open cannot be
/// computed from a chunk of another one: the two are reported instead of being paired as though the
/// second were a continuation of the first.
#[test]
fn a_chunk_of_another_series_on_one_operand_is_refused() {
    let (tree, grid, left, right) = binary_tree(BinaryOp::Add, 4);
    let schema = evaluated_schema(grid);
    let pool = pool();
    let mut execution = executor(&tree, Arc::clone(&pool), &ExecutionPlanMetricsSet::new());
    execution
        .push_evaluated_at(
            &right,
            evaluated_chunk_of(&schema, named_metric("api", "x"), 0, vec![Some(1.0)], false),
        )
        .unwrap();
    // The chunk that arrived while `api` is open names another series.
    execution
        .push_evaluated_at(&left, evaluated_chunk(&schema, 0, vec![1.0], true))
        .unwrap();
    assert!(execution.next_chunk().unwrap().is_none());

    execution
        .push_evaluated_at(
            &right,
            evaluated_chunk_of(&schema, named_metric("db", "x"), 1, vec![Some(2.0)], true),
        )
        .unwrap();
    let error = execution.next_chunk().unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("the binary node at root")
            && message.contains("its right operand")
            && message.contains("before"),
        "{message}"
    );
    // The execution stopped rather than handing on the pairs it had, and it holds nothing.
    assert_eq!(pool.reserved(), 0);
    assert_eq!(execution.state().reserved_bytes(), 0);
    assert_eq!(execution.state().nodes_holding_binary(), 0);
    let again = execution.next_chunk().unwrap_err();
    assert!(
        again.to_string().contains("stopped after an earlier error"),
        "{again}"
    );
    // No partial answer surfaced through `finish` either.
    assert!(execution.finish().is_err());
}

/// An operand chunk that starts behind the end of the open series is refused, values or not.
///
/// The overlap is read off the axes, not off the samples they carry: a chunk whose every sample is
/// missing still occupies the positions its axis places, so a second chunk that reaches back into
/// them is a rewind rather than a continuation.
#[test]
fn an_operand_chunk_that_rewinds_is_refused_with_or_without_values() {
    let (tree, grid, left, _right) = binary_tree(BinaryOp::Add, 4);
    let schema = evaluated_schema(grid);
    let pool = pool();
    let mut execution = executor(&tree, Arc::clone(&pool), &ExecutionPlanMetricsSet::new());
    // Two positions that hold no value at all, and a series that did not close on them.
    execution
        .push_evaluated_at(
            &left,
            evaluated_chunk_of(&schema, metric(), 0, vec![None, None], false),
        )
        .unwrap();
    assert!(execution.next_chunk().unwrap().is_none());
    // Position 1 is one of the two the series already placed.
    execution
        .push_evaluated_at(&left, evaluated_chunk(&schema, 1, vec![1.0], true))
        .unwrap();

    let error = execution.next_chunk().unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("do not continue each other")
            && message.contains("the left operand of the binary node at root")
            && message.contains("after the series already reached 2"),
        "{message}"
    );
    assert_eq!(pool.reserved(), 0);
    assert_eq!(execution.state().nodes_holding_binary(), 0);
}

/// A chunk that does not match the layout its operand is bound to is refused before the kernel.
///
/// Each case is a chunk a caller can build: [`Series`] keeps its fields public, so an axis and the
/// fields beside it can disagree, and a chunk can be addressed on a grid no operand was bound to.
#[test]
fn a_chunk_that_does_not_match_its_operand_is_refused_before_the_kernel() {
    let (tree, out_grid, left, _right) = binary_tree(BinaryOp::Add, 4);
    let pool = pool();
    // What each operand of `binary_tree` is bound to: one float field on the tree's own grid.
    let schema = evaluated_schema(Arc::clone(&out_grid));
    let two_fields = Arc::new(SeriesSchema {
        fields: field_count(2),
        eval_grid: Some(Arc::clone(&out_grid)),
    });
    let elsewhere = grid(1, 60_000, 4);

    let cases: Vec<(&str, EvaluatedChunk)> = vec![
        (
            // An axis on another grid: one step from the one the operand is bound to.
            "the other grid",
            evaluated_on(
                &evaluated_schema(Arc::clone(&elsewhere)),
                metric(),
                0,
                2,
                vec![vec![Some(1.0), Some(2.0)]],
                true,
            ),
        ),
        (
            // Two fields where the operand is bound to one.
            "the field count",
            evaluated_on(
                &two_fields,
                metric(),
                0,
                2,
                vec![vec![Some(1.0), Some(2.0)], vec![Some(3.0), Some(4.0)]],
                true,
            ),
        ),
        (
            // Two rows of axis, one sample of the one field.
            "the field length",
            evaluated_on(&schema, metric(), 0, 2, vec![vec![Some(1.0)]], true),
        ),
    ];
    for (case, chunk) in cases {
        let mut execution = executor(&tree, Arc::clone(&pool), &ExecutionPlanMetricsSet::new());
        execution.push_evaluated_at(&left, chunk).unwrap();
        let error = execution.next_chunk().unwrap_err();
        assert!(
            error.to_string().contains("rejected its left operand"),
            "{case}: {error}"
        );
        assert_eq!(
            pool.reserved(),
            0,
            "{case}: the refused chunk is still held"
        );
        assert_eq!(execution.state().reserved_bytes(), 0, "{case}");
        assert_eq!(execution.state().nodes_holding_binary(), 0, "{case}");
    }
}

/// A series cannot be built with no field at all, which is what keeps a zero-field check honest.
///
/// The validator's field-count check is not skipped by an empty field list because an empty one
/// cannot exist: [`SeriesFields`] refuses it, so every chunk any caller can build carries at least
/// one field. The boundary is the constructor's, and it is recorded here rather than loosened there.
#[test]
fn a_series_cannot_carry_no_fields() {
    let error =
        SeriesFields::try_from_vec(Vec::new()).expect_err("a series with no field was accepted");
    assert!(error.to_string().contains("must not be empty"), "{error}");
    // The one-field and many-field shapes a chunk can have are not empty either.
    let one = SeriesFields::one(Samples::from_float(Float64Array::from(vec![1.0])));
    assert!(!one.is_empty());
    assert_eq!(one.len(), 1);
}

/// A binary node with no chunk at all on either side answers nothing, and holds nothing.
///
/// The input ending without a chunk is a stream that held no series, not a tree that never ran: the
/// kernel is asked about two empty vectors and reports none, so the answer is the empty one.
#[test]
fn an_absent_operand_yields_no_series() {
    let (tree, grid, _left, right) = binary_tree(BinaryOp::Add, 2);
    let schema = evaluated_schema(grid);
    let pool = pool();

    // Neither side queued a chunk.
    let mut empty = executor(&tree, Arc::clone(&pool), &ExecutionPlanMetricsSet::new());
    assert!(empty.finish().unwrap().is_empty());
    assert_eq!(pool.reserved(), 0);
    assert_eq!(empty.state().nodes_holding_binary(), 0);

    // Only the right side ran, and it closed: nothing matches a series that is not there.
    let mut one_side = executor(&tree, Arc::clone(&pool), &ExecutionPlanMetricsSet::new());
    one_side
        .push_evaluated_at(&right, evaluated_chunk(&schema, 0, vec![1.0, 2.0], true))
        .unwrap();
    assert!(one_side.finish().unwrap().is_empty());
    assert_eq!(pool.reserved(), 0);
    assert_eq!(one_side.state().nodes_holding_binary(), 0);
}

/// A binary node counts the series it closes on its output, and does not count an operand chunk
/// that already closed its input series.
#[test]
fn a_binary_counts_each_closed_result_once() {
    let (tree, grid, left, right) = binary_tree(BinaryOp::Add, 2);
    let schema = evaluated_schema(grid);
    let set = ExecutionPlanMetricsSet::new();
    let mut execution = executor(&tree, pool(), &set);
    execution
        .push_evaluated_at(&left, evaluated_chunk(&schema, 0, vec![1.0, 2.0], true))
        .unwrap();
    execution
        .push_evaluated_at(&right, evaluated_chunk(&schema, 0, vec![10.0, 20.0], true))
        .unwrap();
    let out = run(&mut execution);
    assert_eq!(out.len(), 1);
    assert!(out[0].is_last, "a result series is handed over closed");
    let root = NodePath::root();
    // One matched result closed, not one per operand chunk and not one per handover of the result.
    assert_eq!(count(&set, &root, NodeMetric::COMPLETED_SERIES), 1);
    assert_eq!(count(&set, &root, NodeMetric::INPUT_BATCHES), 2);
    assert_eq!(count(&set, &root, NodeMetric::OUTPUT_BATCHES), 1);
}
