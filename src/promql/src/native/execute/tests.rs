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
use datatypes::arrow::array::{Array, Float64Array, UInt32Array};
use datatypes::arrow::buffer::{Buffer, ScalarBuffer};
use datatypes::series::histogram::{
    BucketSideView, CounterResetHint, HistogramBatch, HistogramBatchBuilder, HistogramView,
};
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{
    DenseSamplesRef, FieldSpec, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{EvalGrid, GridRange, GridSpan, GridSpec, TimeAxis};
use datatypes::series::{Label, MetricName, SeriesOrdering};

use super::{
    BinaryRuntime, Emitted, NativeChunk, NativeExecutionState, NativeExecutor, NodeBuffer,
    NodeRuntime, absorb_rebuilds, evaluated_bytes, grid_end_ms, map_chunk, operand_chunk_bytes,
    raw_bytes,
};
use crate::evaluated::EvaluatedChunk;
use crate::grid_series::merge_same_labelset;
use crate::histogram_eval::NativeHistogramFunction;
use crate::instant_eval::select_instants;
use crate::map::MathOp;
use crate::map_label::LabelRewrite;
use crate::native::metrics::{NodeMetric, NodeMetrics, NodePath};
use crate::native::plan::{
    AggregateKind, AggregateStage, BoundAggregate, BoundInput, BoundInstant, BoundMap, BoundScalar,
    BoundWindow, Grouping, MapOp, NativeExpr, NativeNode, NativeTree, NativeType,
};
use crate::range_eval::{RangeEvaluator, RangeFunction, RangeWindow};
use crate::vector_match::{
    BinaryMatching, BinaryOp, Cardinality, FillValues, MatchingLabels, reset_visited_pairs,
    visited_pairs,
};

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
    map_ops(inner, ops.into_iter().map(MapOp::Math).collect())
}

fn map_ops(inner: NativeNode, ops: Vec<MapOp>) -> NativeNode {
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

/// A source node's runtime: the queue and the reservation it is held for.
fn source_runtime(pool: &Arc<dyn MemoryPool>) -> NodeRuntime {
    NodeRuntime {
        buffer: NodeBuffer::new(
            &NodePath::root().input(),
            Arc::new(NodeMetrics::default()),
            pool,
        ),
        queued: VecDeque::new(),
        instant: None,
        window: None,
        map: None,
        binary: None,
        source_schema: None,
        emitted: None,
        merge: None,
        open_source_series: false,
    }
}

/// A binary node's runtime: both operands bound to `schema` and the reservation they are held for.
fn binary_runtime(pool: &Arc<dyn MemoryPool>, schema: &Arc<SeriesSchema>) -> NodeRuntime {
    NodeRuntime {
        buffer: NodeBuffer::new(&NodePath::root(), Arc::new(NodeMetrics::default()), pool),
        queued: VecDeque::new(),
        instant: None,
        window: None,
        map: None,
        binary: Some(BinaryRuntime::new(
            Arc::new(binary_matching(BinaryOp::Add)),
            [Arc::clone(schema), Arc::clone(schema)],
        )),
        source_schema: None,
        emitted: None,
        merge: None,
        open_source_series: false,
    }
}

/// What a binary node holds once it retains `left` and `right`, and what assembling the retained
/// chunks into the series the kernel matches adds on top.
///
/// Both are read from a runtime of its own, so a pool can be sized from them: the first is what the
/// node reserves for the operands it retains and the second its own bound, and neither is a figure
/// the pool reports.
fn binary_charge(
    schema: &Arc<SeriesSchema>,
    left: &EvaluatedChunk,
    right: &EvaluatedChunk,
) -> (usize, usize) {
    let mut runtime = binary_runtime(&pool(), schema);
    runtime
        .retain_binary(&NodePath::root(), 0, NativeChunk::Evaluated(left.clone()))
        .unwrap();
    runtime
        .retain_binary(&NodePath::root(), 1, NativeChunk::Evaluated(right.clone()))
        .unwrap();
    let retained = runtime.buffer.reservation.size();
    let assembly = runtime
        .binary
        .as_ref()
        .expect("the runtime holds a binary node")
        .assembly_bytes()
        .expect("the assembly bound does not overflow");
    (retained, assembly)
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

/// One evaluated chunk of the series `metric` whose one field holds `indices` into `table`,
/// addressed at `start` on `schema`'s grid.
///
/// Two chunks built with one `table` are two slices of one allocation; two built with tables of the
/// same shape hold two allocations of the same size.
fn evaluated_histogram_chunk_of(
    schema: &SeriesSchema,
    metric: MetricName,
    start: usize,
    indices: Vec<Option<u32>>,
    table: &Arc<HistogramBatch>,
    is_last: bool,
) -> EvaluatedChunk {
    let samples =
        Samples::try_from_histogram(UInt32Array::from(indices), Arc::clone(table)).unwrap();
    let range =
        GridRange::try_new(schema.eval_grid.clone().unwrap(), start, samples.len()).unwrap();
    let series = Series {
        metric,
        time: TimeAxis::Grid(range),
        fields: SeriesFields::try_from_vec(vec![samples]).unwrap(),
    };
    EvaluatedChunk::try_new(series, is_last).unwrap()
}

/// One histogram table of `histograms` histograms of `buckets` buckets each.
fn histogram_table(histograms: usize, buckets: i32) -> Arc<HistogramBatch> {
    let mut builder = HistogramBatchBuilder::new();
    for _ in 0..histograms {
        push_histogram(&mut builder, buckets);
    }
    builder.finish().unwrap()
}

/// What `table` holds: the batch struct and the arrays its columns view.
///
/// Written out here rather than read from the runtime, so a test can size its expectations from the
/// table it built instead of from the figure it is checking.
fn histogram_table_bytes(table: &Arc<HistogramBatch>) -> usize {
    std::mem::size_of::<HistogramBatch>() + table.as_arrow().get_array_memory_size()
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
    let mapped_chunk = map_chunk(&map_schema, &inner_series, &[MapOp::Math(MathOp::Abs)])
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
                    fill: FillValues::default(),
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

/// A selection over an interleaved input is refused when the tree is bound, before it can run.
///
/// The binding is where the ordering is known, and the plan's `Instant` arm refuses an input that
/// declares `Interleaved` for the reason the window arm does: a selection's consumer collects one
/// series' chunks into one buffer, so it reads a series to its last chunk before the next. Nothing
/// about an interleaved input is therefore left to the runtime along a selection, and the tree
/// never reaches [`NativeExecutionState::try_new`]. The ordering check there stays for the shape
/// the binding cannot see through: it only looks at a window's own input, so a window over a map
/// over the source still reads an interleaved stream, which is
/// [`a_window_over_an_interleaved_input_is_rejected`]. The shape with a map above the selection is
/// [`an_interleaved_input_is_refused_under_a_map_over_a_selection`].
#[test]
fn an_interleaved_input_is_refused_under_a_selection() {
    let out_grid = grid(0, 60_000, 2);
    let error = NativeTree::try_new(instant(
        raw_input_with_ordering(SeriesOrdering::Interleaved),
        60_000,
        0,
        Arc::clone(&out_grid),
    ))
    .expect_err("a selection over an interleaved input must not bind");
    // A rejected tree is a compiler defect rather than a query error, so the binding reports it as
    // `Internal`; what this pins is that the refusal names the ordering it came from, since the
    // same check refuses a window over the same input.
    assert!(matches!(error, DataFusionError::Internal(_)), "{error}");
    let message = error.to_string();
    assert!(
        message.contains("interleaved chunks cannot be fed to it"),
        "{message}"
    );
}

/// The same refusal with a map above the selection.
///
/// The tree here is `Input -> Instant -> Map`: the map sits above the selection, and the selection
/// reads the interleaved input directly. A map between the input and the selection is not a tree
/// this compiler can build at all -- a `Map` produces an evaluated vector while an instant
/// selection requires raw samples, and the plan's `Instant` arm rejects evaluated values -- so what
/// this test pins is the recursion: `validate` descends through the outer map to the selection
/// below it, whose own arm then refuses the input it reads, and the tree is refused at the same
/// point as [`an_interleaved_input_is_refused_under_a_selection`].
#[test]
fn an_interleaved_input_is_refused_under_a_map_over_a_selection() {
    let out_grid = grid(0, 60_000, 2);
    let error = NativeTree::try_new(map(
        instant(
            raw_input_with_ordering(SeriesOrdering::Interleaved),
            60_000,
            0,
            Arc::clone(&out_grid),
        ),
        vec![MathOp::Abs],
    ))
    .expect_err("a map over a selection over an interleaved input must not bind");
    assert!(matches!(error, DataFusionError::Internal(_)), "{error}");
    let message = error.to_string();
    assert!(
        message.contains("interleaved chunks cannot be fed to it"),
        "{message}"
    );
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

/// What a selection reserves is the series it has buffered, measured chunk by chunk.
///
/// The node holds a whole series, so its reservation grows with the chunks that series arrived in
/// and goes back when the series is answered. Each chunk is measured once, with the same estimate a
/// queued chunk is charged with: an allocation two chunks share is counted once per chunk, which is
/// the over-count a figure that keeps no state across chunks accepts.
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
        let chunk = raw_chunk(&schema, &timestamps, &values, false);
        let bytes = raw_bytes(&chunk);
        execution.push_raw(chunk).unwrap();
        assert!(
            execution.next_chunk().unwrap().is_none(),
            "a series was reported before its last chunk arrived"
        );
        let held = execution.state().reserved_bytes();
        assert_eq!(
            held,
            previous + bytes,
            "the buffered series is not what is reserved for: {held} bytes held after {previous} \
             and a chunk of {bytes}"
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
/// A selection decides the points of the grid it can be selected at, so its workspace is
/// proportional to those points rather than to the samples of the series: the axis it concatenates,
/// the source row and evaluation timestamp it reports per point, and the rows the builder keeps.
/// The lookback here reaches from the sample to the last point, so this series is decididable at
/// every one of the million points -- the clipped count equals the grid's -- and the two selection
/// arrays alone are 16 MB while the built rows are more, against a series the node holds of one
/// sample of a few hundred bytes. [`a_selection_reserves_the_points_it_decides`] is the other side
/// of the same figure: a series whose samples cover only the start of a long grid reserves for the
/// few points it can decide and is admitted by a pool this one is refused by.
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

/// A selection reserves the points it can decide, not the points the grid plans.
///
/// The review's shape: a seven-day grid at a one-minute step is 10,081 points, and one sample at
/// the start of the series read with a five-minute lookback is decided at five of them. The
/// grid-scaled terms of `InstantRuntime::gather_bytes` at the planned grid -- the selection's two
/// arrays and the doubled builder columns -- are about 504 KB, which the 64 KiB pool here is far
/// below; the handful of points really decided need a few hundred bytes, so this gather has to be
/// admitted. What it hands out is the rows `select_instants` reports for the same series, which is
/// the count the reservation has to agree with.
///
/// The second half is the other direction: the same grid, the same pool, and a series that spans
/// the grid with a lookback long enough to cover it, so all 10,081 points can be decided. The
/// figure then exceeds the pool and the gather is refused, which is what keeps this from being a
/// reservation of nothing.
#[test]
fn a_selection_reserves_the_points_it_decides() {
    let out_grid = grid(0, 60_000, 10_081);
    let schema = raw_schema();
    let budget = 64 << 10;
    // The two grid-scaled terms of `gather_bytes` at the planned grid: 10,081 points x 16 bytes for
    // the selection's arrays, and 10,081 points x (8 + 8 + 1) x 2 for the doubled builder columns.
    let full_grid = 10_081 * (8 + 8) + 10_081 * (8 + 8 + 1) * 2;
    assert!(
        full_grid > budget,
        "the pool is not below what the full grid would be reserved: {full_grid} bytes"
    );

    // One sample at the start, read within five minutes: the kernel reports 0s..240s, and the
    // candidate bound it sizes its arrays with is 0s..300s.
    let tree = NativeTree::try_new(instant(input(), 300_000, 0, Arc::clone(&out_grid))).unwrap();
    let chunk = raw_chunk(&schema, &[0], &[1.0], true);
    let (_, timestamps) = select_instants(
        &chunk.time,
        &chunk.fields,
        out_grid.spec().start_ms..=grid_end_ms(out_grid.spec()),
        out_grid.spec().step_ms,
        300_000,
        0,
    )
    .unwrap();
    assert_eq!(
        timestamps.len(),
        5,
        "the clipping kernel decides another count"
    );
    let mut execution = executor(&tree, bounded_pool(budget), &ExecutionPlanMetricsSet::new());
    execution.push_raw(chunk).unwrap();
    let out = run(&mut execution);
    assert_eq!(out.len(), 1);
    let expected: Vec<(usize, Option<f64>)> = timestamps
        .iter()
        .map(|timestamp| {
            (
                usize::try_from((timestamp - out_grid.spec().start_ms) / out_grid.spec().step_ms)
                    .unwrap(),
                Some(1.0),
            )
        })
        .collect();
    assert_eq!(
        decided(&out),
        expected,
        "the gather handed out another set of points than the clipping decides"
    );

    // The same series spans the grid with a lookback that covers it, so every point can be decided
    // and the workspace no longer fits the same pool.
    let tree =
        NativeTree::try_new(instant(input(), 604_800_001, 0, Arc::clone(&out_grid))).unwrap();
    let pool = bounded_pool(budget);
    let mut execution = executor(&tree, Arc::clone(&pool), &ExecutionPlanMetricsSet::new());
    execution
        .push_raw(raw_chunk(&schema, &[0, 604_800_000], &[1.0, 2.0], true))
        .unwrap();
    let error = execution.next_chunk().unwrap_err();
    assert!(
        matches!(error, DataFusionError::ResourcesExhausted(_)),
        "a series covering the grid was gathered without asking the pool: {error}"
    );
    assert_eq!(pool.reserved(), 0);
    assert_eq!(execution.state().reserved_bytes(), 0);
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
///
/// The selection holds the series and gathers it only on the chunk that closes it, so nothing is
/// answered for it. The node that notices is the input: the chunk it handed over did not say
/// `is_last`, and the tree's close reports that before any collected input is merged.
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
        fill: FillValues::default(),
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
            fill: FillValues::default(),
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
        fill: FillValues::default(),
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
    let mut results: Vec<_> = run(&mut execution)
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

/// A binary operand reserves each chunk it retains before it retains it, and holds exactly what
/// those chunks hold on their own.
///
/// The figure of a retained chunk is what its rows occupy rather than the allocation they are slices
/// of, so a series delivered in sibling slices of one batch is charged the batch's data once instead
/// of once per chunk. What is refused here is a figure that disagrees with what the operand holds --
/// a chunk reserved for on top of what the operand already keeps would be refused by a pool sized to
/// the total, and one reserved for less would leave the pool holding less than the node does.
#[test]
fn a_binary_operand_reserves_what_it_retains() {
    let grid = grid(0, 60_000, 40_000);
    let schema = evaluated_schema(Arc::clone(&grid));
    let batch = Float64Array::from((0..40_000).map(f64::from).collect::<Vec<_>>());
    let first = evaluated_array_chunk_of(&schema, metric(), 0, batch.slice(0, 20_000), false);
    let second =
        evaluated_array_chunk_of(&schema, metric(), 20_000, batch.slice(20_000, 20_000), true);

    // What each chunk holds on its own, and the entry the closing chunk records in the operand's
    // boundary list -- the list is storage the operand keeps, and it is charged one entry per closed
    // series as it is written.
    let boundaries = std::mem::size_of::<usize>();
    let (first_alone, second_alone) = (
        operand_chunk_bytes(&first.series),
        operand_chunk_bytes(&second.series),
    );
    let held = first_alone + second_alone + boundaries;

    // The budget is the figure the operand ends up holding, so a chunk reserved for on top of what
    // the operand keeps would be refused and one reserved for less would leave the pool holding less
    // than the node does.
    let pool = bounded_pool(held);
    let mut runtime = binary_runtime(&pool, &schema);
    let before = runtime.buffer.reservation.size();
    runtime
        .retain_binary(&NodePath::root(), 0, NativeChunk::Evaluated(first))
        .unwrap();
    assert_eq!(
        runtime.buffer.reservation.size() - before,
        first_alone,
        "the first chunk of the operand does not cost what it is estimated at"
    );
    let before = runtime.buffer.reservation.size();
    runtime
        .retain_binary(&NodePath::root(), 0, NativeChunk::Evaluated(second))
        .unwrap();
    assert_eq!(
        runtime.buffer.reservation.size() - before,
        second_alone + boundaries,
        "the closing chunk of the operand does not cost what it is estimated at, and the end it \
         records"
    );
    assert_eq!(
        runtime.buffer.reservation.size(),
        held,
        "the operand does not reserve what it holds"
    );
    assert_eq!(pool.reserved(), held);

    // The figure a caller reserves for before retaining a chunk is the one the operand grows by: the
    // estimate of the series it carries and the one entry the chunk's end records.
    let own = evaluated_chunk(&schema, 0, vec![1.0, 2.0], true);
    assert_eq!(
        runtime
            .binary
            .as_ref()
            .unwrap()
            .pending(&NodePath::root(), 0, &own)
            .unwrap(),
        operand_chunk_bytes(&own.series) + std::mem::size_of::<usize>(),
        "the figure reserved for a chunk is not what the operand grows by"
    );

    // Assembling the two chunks keeps the allocation they are slices of: the series the kernel reads
    // is made of their descriptors, and no sample is copied into it.
    let assembled = runtime
        .binary
        .as_ref()
        .unwrap()
        .assemble(&NodePath::root())
        .unwrap();
    assert_eq!(
        assembled[0].len(),
        1,
        "two chunks of one series are one series"
    );
    let field = &assembled[0][0].fields.as_slice()[0];
    assert_eq!(
        field.segments().len(),
        2,
        "the assembled field is not the chunks it is built from"
    );
    for segment in field.segments() {
        let Some(DenseSamplesRef::Float(values)) = segment.dense() else {
            panic!("a float chunk assembles into float segments");
        };
        assert_eq!(
            values.values().inner().data_ptr(),
            batch.values().inner().data_ptr(),
            "the assembly copied samples instead of sharing the allocation they are slices of"
        );
    }
}

/// Two chunks that are first to reference a histogram table each are billed a table each.
///
/// A histogram field's samples are a column of indices into a table, and the table is the bulk of
/// what such a chunk costs: the indices are four bytes a row where the table is thousands of bytes
/// of buckets. Two chunks of the same shape hold two tables, so each costs the other, and each
/// costs at least the table it holds -- a figure that charged the column of indices alone would
/// leave both tables outside everything the node reserves.
#[test]
fn a_binary_operand_bills_each_histogram_table_it_retains() {
    const BUCKETS: i32 = 4_000;
    let grid = grid(0, 60_000, 2);
    let schema = evaluated_schema(Arc::clone(&grid));
    let table = histogram_table(2, BUCKETS);
    let other = histogram_table(2, BUCKETS);
    let bytes = histogram_table_bytes(&table);
    assert_eq!(
        bytes,
        histogram_table_bytes(&other),
        "the two tables are not the same shape, so the two chunks are not comparable"
    );
    assert!(
        bytes > 32 * 1024,
        "a table of {bytes} bytes is no bigger than the descriptors of a one-row chunk, so nothing \
         here would tell a billed table from an unbilled one"
    );
    let first = evaluated_histogram_chunk_of(&schema, metric(), 0, vec![Some(0)], &table, true);
    let second = evaluated_histogram_chunk_of(&schema, metric(), 1, vec![Some(1)], &other, true);

    let pool = pool();
    let mut runtime = binary_runtime(&pool, &schema);
    let before = runtime.buffer.reservation.size();
    runtime
        .retain_binary(&NodePath::root(), 0, NativeChunk::Evaluated(first))
        .unwrap();
    let first_cost = runtime.buffer.reservation.size() - before;
    let before = runtime.buffer.reservation.size();
    runtime
        .retain_binary(&NodePath::root(), 0, NativeChunk::Evaluated(second))
        .unwrap();
    let second_cost = runtime.buffer.reservation.size() - before;

    assert_eq!(
        first_cost, second_cost,
        "two chunks of one shape and one table each cost different figures: {first_cost} and \
         {second_cost}"
    );
    for (name, cost) in [("first", first_cost), ("second", second_cost)] {
        assert!(
            cost >= bytes,
            "the {name} chunk costs {cost} bytes, less than the {bytes} its histogram table holds"
        );
        assert!(
            cost < 2 * bytes,
            "the {name} chunk costs {cost} bytes, more than the {bytes} of its table and the rest \
             of a one-row chunk"
        );
    }
    assert_eq!(
        runtime.buffer.reservation.size(),
        first_cost + second_cost,
        "the operand does not reserve what the two chunks it retains cost"
    );
    assert_eq!(pool.reserved(), runtime.buffer.reservation.size());
}

/// Two chunks that index one histogram table are billed that table once.
///
/// A table is one allocation however many chunks of a series index into it, so the second chunk is
/// charged the descriptor, the index and the end it records, and not the table again. The two
/// chunks here are the same shape, so what separates their figures is the table: the pair costs one
/// table plus both chunks' own descriptors, not one table per chunk and not no table at all.
#[test]
fn a_binary_operand_bills_a_shared_histogram_table_once() {
    const BUCKETS: i32 = 4_000;
    let grid = grid(0, 60_000, 2);
    let schema = evaluated_schema(Arc::clone(&grid));
    let table = histogram_table(2, BUCKETS);
    let bytes = histogram_table_bytes(&table);
    assert!(
        bytes > 32 * 1024,
        "a table of {bytes} bytes is no bigger than the descriptors of a one-row chunk, so nothing \
         here would tell a shared table from a second one"
    );
    let first = evaluated_histogram_chunk_of(&schema, metric(), 0, vec![Some(0)], &table, true);
    let second = evaluated_histogram_chunk_of(&schema, metric(), 1, vec![Some(1)], &table, true);

    let pool = pool();
    let mut runtime = binary_runtime(&pool, &schema);
    let before = runtime.buffer.reservation.size();
    runtime
        .retain_binary(&NodePath::root(), 0, NativeChunk::Evaluated(first))
        .unwrap();
    let first_cost = runtime.buffer.reservation.size() - before;
    let before = runtime.buffer.reservation.size();
    runtime
        .retain_binary(&NodePath::root(), 0, NativeChunk::Evaluated(second))
        .unwrap();
    let second_cost = runtime.buffer.reservation.size() - before;

    assert!(
        first_cost >= bytes,
        "the chunk that first references the table costs {first_cost} bytes, less than the {bytes} \
         the table holds"
    );
    // The two chunks are the same shape, so the second one is charged its own descriptors and not
    // the table: it costs a small fraction of what the table holds, where a second table would cost
    // the table again.
    assert!(
        second_cost * 8 < bytes,
        "the second slice of one table costs {second_cost} bytes, comparable to the {bytes} the \
         table itself holds"
    );
    // What the pair costs past the second chunk is the table the first one billed, and no more than
    // the few bytes that remembering it takes.
    let difference = first_cost.saturating_sub(second_cost);
    assert!(
        difference >= bytes,
        "the pair costs {difference} bytes more than its second chunk, less than the {bytes} of the \
         one table between them"
    );
    assert!(
        difference <= bytes + 64,
        "the pair costs {difference} bytes more than its second chunk, past the {bytes} of the one \
         table between them: the table is billed twice"
    );
    assert!(
        runtime.buffer.reservation.size() < bytes + 2 * second_cost + 64,
        "the operand charges the one table of its two chunks twice: {} bytes for {} bytes of table",
        runtime.buffer.reservation.size(),
        bytes
    );
    assert_eq!(pool.reserved(), runtime.buffer.reservation.size());
}

/// A pool with room for one histogram table admits one chunk of a table, refuses a second chunk
/// holding another table, and admits a second slice of the table it already holds.
///
/// This is the figure the reservation is made for: a chunk that is the first to reference a table
/// costs that table, so two of them do not fit in room for one, while a chunk that indexes a table
/// the operand already holds costs its own slice and does. The room beside the first chunk is half a
/// table -- far more than the descriptors of a second chunk and far less than a second table -- and
/// the refused chunk leaves the operand where it was.
#[test]
fn a_pool_that_holds_one_histogram_table_refuses_a_second() {
    const BUCKETS: i32 = 4_000;
    let grid = grid(0, 60_000, 2);
    let schema = evaluated_schema(Arc::clone(&grid));
    let table = histogram_table(2, BUCKETS);
    let other = histogram_table(2, BUCKETS);
    let bytes = histogram_table_bytes(&table);
    assert!(
        bytes > 32 * 1024,
        "a table of {bytes} bytes is no bigger than the descriptors of a one-row chunk, so the room \
         beside one table would hold a second"
    );
    let first = evaluated_histogram_chunk_of(&schema, metric(), 0, vec![Some(0)], &table, true);
    let second_own =
        evaluated_histogram_chunk_of(&schema, metric(), 0, vec![Some(0)], &other, true);
    let second_shared =
        evaluated_histogram_chunk_of(&schema, metric(), 1, vec![Some(1)], &table, true);

    // What the operand holds once it retains one chunk of a table, measured where the pool admits
    // anything; the pool below is sized from it and half a table.
    let mut probe = binary_runtime(&pool(), &schema);
    probe
        .retain_binary(&NodePath::root(), 0, NativeChunk::Evaluated(first.clone()))
        .unwrap();
    let one_chunk = probe.buffer.reservation.size();
    let budget = one_chunk + bytes / 2;

    // A second chunk holding a table of its own does not fit: the pool refuses it and the operand
    // keeps what it had.
    let mut refused = binary_runtime(&bounded_pool(budget), &schema);
    refused
        .retain_binary(&NodePath::root(), 0, NativeChunk::Evaluated(first.clone()))
        .unwrap();
    let error = refused
        .retain_binary(&NodePath::root(), 0, NativeChunk::Evaluated(second_own))
        .unwrap_err();
    assert!(
        matches!(error, DataFusionError::ResourcesExhausted(_)),
        "a second chunk holding a table of its own was admitted beside the first: {error}"
    );
    assert_eq!(
        refused.buffer.reservation.size(),
        one_chunk,
        "the refused chunk moved what the operand reserves"
    );

    // The second slice of the one table the operand holds does fit.
    let mut shared = binary_runtime(&bounded_pool(budget), &schema);
    shared
        .retain_binary(&NodePath::root(), 0, NativeChunk::Evaluated(first))
        .unwrap();
    shared
        .retain_binary(&NodePath::root(), 0, NativeChunk::Evaluated(second_shared))
        .unwrap();
    assert!(
        shared.buffer.reservation.size() > one_chunk,
        "the second slice of the table was admitted without being charged its own descriptors"
    );
    assert!(
        shared.buffer.reservation.size() <= budget,
        "the operand reserves past the pool it was admitted in"
    );
}

/// The executor's binary node holds no more than the operands it retains: the peak of its own
/// reservation is what every retained chunk holds on its own, not that plus a chunk it was handed
/// again.
///
/// The source queue holds one chunk while the node retains the other, so what is read here is the
/// binary node's own figure rather than the pool's: the overlap of the two nodes is a charge of the
/// input and says nothing about what the operand costs.
#[test]
fn a_binary_beneath_the_executor_reserves_what_it_retains() {
    let (tree, grid, left, right) = binary_tree(BinaryOp::Add, 4_000);
    let schema = evaluated_schema(Arc::clone(&grid));
    let batch = Float64Array::from(vec![1.0; 4_000]);
    let first = evaluated_array_chunk_of(&schema, metric(), 0, batch.slice(0, 2_000), false);
    let second =
        evaluated_array_chunk_of(&schema, metric(), 2_000, batch.slice(2_000, 2_000), true);
    // What the left operand holds once both chunks are retained: what each chunk holds on its own,
    // and the boundary list the closing chunk adds one entry to.
    let retained = std::mem::size_of::<usize>()
        + [&first, &second]
            .iter()
            .map(|chunk| operand_chunk_bytes(&chunk.series))
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
        "the binary node reserved more than the operands it retains"
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

/// A query rooted at a binary node streams its answer after the input ends, one chunk per pull.
///
/// The kernel needs both whole vectors before it answers anything, so nothing is settled until the
/// caller ends the input. From there the answer is not handed over as one collected value: each pull
/// takes the next chunk out of the node that holds it and gives back what that chunk was charged,
/// and the pull that finds nothing left is where the execution closes. A caller that stops pulling
/// early therefore still holds nothing it did not ask for, and the pool is empty when the last
/// `None` arrives.
#[test]
fn an_ended_input_streams_the_binary_answer_one_chunk_at_a_time() {
    let (tree, grid, left, right) = binary_tree(BinaryOp::Add, 4);
    let schema = evaluated_schema(Arc::clone(&grid));
    let pool = bounded_pool(1 << 22);
    let mut execution = executor(&tree, Arc::clone(&pool), &ExecutionPlanMetricsSet::new());
    execution
        .push_evaluated_at(&left, evaluated_chunk(&schema, 0, vec![1.0; 4], true))
        .unwrap();
    execution
        .push_evaluated_at(&right, evaluated_chunk(&schema, 0, vec![2.0; 4], true))
        .unwrap();
    // The operands are whole now, but the kernel has not run: the caller has not ended the input.
    assert!(execution.next_chunk().unwrap().is_none());

    execution
        .end_of_input()
        .expect("a closed input with two whole operands ends cleanly");
    // Ending the input runs the kernel and leaves its answer in the node that computed it, charged
    // there until it is pulled; it does not collect the answer into a value of its own.
    let mut chunks = Vec::new();
    while let Some(chunk) = execution.next_chunk().unwrap() {
        chunks.push(chunk);
    }
    assert_eq!(addressed(&chunks), vec![(0, vec![Some(3.0); 4])]);
    // The last pull closed the execution and returned everything it reserved.
    assert_eq!(execution.state().reserved_bytes(), 0);
    assert_eq!(pool.reserved(), 0);
    // A finished execution is not pulled again.
    let again = execution.next_chunk().unwrap_err();
    assert!(again.to_string().contains("already finished"), "{again}");
}

/// `finish` still answers the whole result in one value for a caller that wants it that way.
#[test]
fn finish_collects_what_the_streaming_contract_would_have_pulled() {
    let (tree, grid, left, right) = binary_tree(BinaryOp::Add, 4);
    let schema = evaluated_schema(Arc::clone(&grid));
    let pool = bounded_pool(1 << 22);
    let mut execution = executor(&tree, Arc::clone(&pool), &ExecutionPlanMetricsSet::new());
    execution
        .push_evaluated_at(&left, evaluated_chunk(&schema, 0, vec![1.0; 4], true))
        .unwrap();
    execution
        .push_evaluated_at(&right, evaluated_chunk(&schema, 0, vec![2.0; 4], true))
        .unwrap();
    let chunks = execution.finish().expect("the whole answer is collected");
    assert_eq!(addressed(&chunks), vec![(0, vec![Some(3.0); 4])]);
    assert_eq!(execution.state().reserved_bytes(), 0);
    assert_eq!(pool.reserved(), 0);
}

/// A binary node reads an operand a collecting window below it produces only after that window
/// flushes.
///
/// `Add(LastOverTime(Abs(a)), b)` puts a collecting window under a binary node: the window is above
/// a map, so it collects its whole input and answers at the end of it, while the binary node needs
/// both operands whole before it decides a point. Ending the input therefore has to finish the
/// subtree below each operand before the binary node computes -- flushing the window first and
/// letting the pull that follows carry its answer up. A binary node that computed on the empty
/// left operand would mark itself computed and answer nothing, and the value the window produced
/// afterwards would never be read.
#[test]
fn a_binary_reads_an_operand_a_collecting_window_flushes() {
    let grid = grid(0, 60_000, 1);
    let schema = evaluated_schema(Arc::clone(&grid));
    // The left operand is a window over a map: the map rewrites the label set, so the window
    // collects its input and answers at the end of it. The right operand is a plain source.
    let root = binary_of(
        window_with(
            map(evaluated_input(Arc::clone(&grid)), vec![MathOp::Abs]),
            RangeFunction::LastOverTime,
            60_000,
            Some(Arc::clone(&grid)),
            Arc::clone(&grid),
        ),
        evaluated_input(Arc::clone(&grid)),
        binary_matching(BinaryOp::Add),
    );
    let tree = NativeTree::try_new(root).expect("the shape is legal");
    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    // One point on each side, in one window: the left is -1, which `abs` makes 1, and the right is
    // 10, so the sum is 11.
    execution
        .push_evaluated_at(
            &NodePath::root().left().input().input(),
            evaluated_chunk_of(&schema, metric(), 0, vec![Some(-1.0)], true),
        )
        .unwrap();
    execution
        .push_evaluated_at(
            &NodePath::root().right(),
            evaluated_chunk_of(&schema, metric(), 0, vec![Some(10.0)], true),
        )
        .unwrap();

    assert_eq!(
        decided(&run(&mut execution)),
        vec![(0, Some(11.0))],
        "the binary node computed before the window below it answered"
    );
}

/// A series that arrives in ten thousand one-point chunks is one candidate for the kernel, not ten
/// thousand.
///
/// The kernel matches whole series, so the chunks of a logical series are assembled into the series
/// it reads before it runs: the pairing is over candidates, and a chunk that was a candidate of its
/// own would pair every chunk of one side with every chunk of the other. The same series delivered
/// whole must answer the same result, so the count of pairs the kernel actually visited is what
/// separates the two deliveries rather than their outcome.
#[test]
fn a_series_of_many_chunks_is_one_candidate_for_the_kernel() {
    let points = 10_000usize;
    let (tree, grid, left, right) = binary_tree(BinaryOp::Add, points as u64);
    let schema = evaluated_schema(grid);
    let batch = Float64Array::from((0..points).map(|point| point as f64).collect::<Vec<_>>());
    // One point per chunk, and only the last of them closes the series: the boundaries of the
    // delivery say nothing about the series it carries.
    let per_point: Vec<EvaluatedChunk> = (0..points)
        .map(|position| {
            evaluated_array_chunk_of(
                &schema,
                metric(),
                position,
                batch.slice(position, 1),
                position + 1 == points,
            )
        })
        .collect();

    reset_visited_pairs();
    let mut split = executor(
        &tree,
        bounded_pool(1 << 26),
        &ExecutionPlanMetricsSet::new(),
    );
    for (position, chunk) in per_point.iter().enumerate() {
        split.push_evaluated_at(&left, chunk.clone()).unwrap();
        split.push_evaluated_at(&right, chunk.clone()).unwrap();
        assert!(
            split.next_chunk().unwrap().is_none(),
            "the binary node answered at position {position} before its input ended"
        );
    }
    let split = split.finish().unwrap();
    assert_eq!(
        visited_pairs(),
        1,
        "the chunks of one series were paired instead of the one series they make up"
    );
    assert_eq!(split.len(), 1, "one matched label set answers one chunk");

    reset_visited_pairs();
    let mut whole = executor(
        &tree,
        bounded_pool(1 << 26),
        &ExecutionPlanMetricsSet::new(),
    );
    let chunk = evaluated_array_chunk_of(&schema, metric(), 0, batch.clone(), true);
    whole.push_evaluated_at(&left, chunk.clone()).unwrap();
    whole.push_evaluated_at(&right, chunk).unwrap();
    assert!(whole.next_chunk().unwrap().is_none());
    let whole = whole.finish().unwrap();
    assert_eq!(
        visited_pairs(),
        1,
        "a series delivered whole is one candidate as well"
    );
    assert_eq!(whole.len(), 1, "one matched label set answers one chunk");

    assert_eq!(
        decided(&split),
        decided(&whole),
        "the delivery of a series changed the result"
    );
    let expected: Vec<(usize, Option<f64>)> = (0..points)
        .map(|position| (position, Some(2.0 * position as f64)))
        .collect();
    assert_eq!(decided(&split), expected);
}

/// The assembly of the operands into the series the kernel matches is reserved before it allocates.
///
/// Assembling one series per closed series is metadata on top of the chunks the node retains -- the
/// descriptors the chunks' fields are collected into and the runs the axis is built from -- and the
/// pool is asked for that figure before it exists. A pool a byte short of it refuses the assembly,
/// and nothing of it is left charged: the node holds what it retained while the runtime is still
/// alive.
#[test]
fn an_assembly_the_pool_cannot_hold_is_refused_before_it_allocates() {
    let grid = grid(0, 60_000, 2);
    let schema = evaluated_schema(Arc::clone(&grid));
    let left = evaluated_chunk(&schema, 0, vec![1.0, 2.0], true);
    let right = evaluated_chunk(&schema, 0, vec![10.0, 20.0], true);
    let (retained, assembly) = binary_charge(&schema, &left, &right);
    assert!(
        assembly > 0,
        "assembling the operands into series is not charged for"
    );

    // One byte short of the assembly: the assembly is refused rather than run over the budget.
    let pool = bounded_pool(retained + assembly - 1);
    let mut runtime = binary_runtime(&pool, &schema);
    runtime
        .retain_binary(&NodePath::root(), 0, NativeChunk::Evaluated(left.clone()))
        .unwrap();
    runtime
        .retain_binary(&NodePath::root(), 1, NativeChunk::Evaluated(right.clone()))
        .unwrap();
    assert_eq!(runtime.buffer.reservation.size(), retained);
    let error = runtime.compute_binary(&NodePath::root()).unwrap_err();
    assert!(
        matches!(error, DataFusionError::ResourcesExhausted(_)),
        "the assembly was not reserved before it allocated: {error}"
    );
    // The assembly never ran, so its charge is gone: what the node holds is what it retained, and
    // the pool holds no more than that while the runtime is still alive.
    assert_eq!(runtime.buffer.reservation.size(), retained);
    assert_eq!(pool.reserved(), retained);
    assert_eq!(
        runtime.binary.as_ref().unwrap().retained_bytes(),
        retained,
        "the refused assembly changed what the operand retains"
    );

    // The figure covers the assembly itself: with exactly that much more, the series are built and
    // hold the rows the chunks carried.
    let pool = bounded_pool(retained + assembly);
    let mut runtime = binary_runtime(&pool, &schema);
    runtime
        .retain_binary(&NodePath::root(), 0, NativeChunk::Evaluated(left))
        .unwrap();
    runtime
        .retain_binary(&NodePath::root(), 1, NativeChunk::Evaluated(right))
        .unwrap();
    let assembled = runtime
        .binary
        .as_ref()
        .unwrap()
        .assemble(&NodePath::root())
        .unwrap();
    assert_eq!(assembled[0].len(), 1, "one closed series is one series");
    assert_eq!(assembled[1].len(), 1, "one closed series is one series");
    let built = EvaluatedChunk::try_new(assembled[0][0].clone(), true).unwrap();
    assert_eq!(sample_values(&built), vec![Some(1.0), Some(2.0)]);
}

/// An assembly the executor's pool cannot hold stops it and leaves it holding nothing.
///
/// The pool is one byte short of what the node retains plus what assembling the retained chunks
/// allocates, so both operands are retained and the assembly is what the pool refuses. What a caller
/// sees afterwards is a stopped execution that holds nothing: the operand chunks are dropped and the
/// reservation is back at zero while the executor is still alive.
#[test]
fn an_assembly_the_executors_pool_cannot_hold_stops_it() {
    let (tree, grid, left, right) = binary_tree(BinaryOp::Add, 2);
    let schema = evaluated_schema(grid);
    let (left_chunk, right_chunk) = (
        evaluated_chunk(&schema, 0, vec![1.0, 2.0], true),
        evaluated_chunk(&schema, 0, vec![10.0, 20.0], true),
    );
    let (retained, assembly) = binary_charge(&schema, &left_chunk, &right_chunk);
    let pool = bounded_pool(retained + assembly - 1);
    let set = ExecutionPlanMetricsSet::new();
    let mut execution = executor(&tree, Arc::clone(&pool), &set);
    execution.push_evaluated_at(&left, left_chunk).unwrap();
    execution.push_evaluated_at(&right, right_chunk).unwrap();
    // Both operands are retained before anything is assembled, and nothing is answered: the refusal
    // below is the assembly's and not a chunk's.
    assert!(execution.next_chunk().unwrap().is_none());
    assert_eq!(
        pool.reserved(),
        retained,
        "the operands are not what the pool holds"
    );
    assert_eq!(
        gauge(
            &set,
            &NodePath::root().partitioned_metric_name(NodeMetric::PEAK_RESERVED_BYTES, 0)
        ),
        retained,
        "the binary node reserved past what it retains"
    );

    let error = execution.finish().unwrap_err();
    assert!(
        matches!(error, DataFusionError::ResourcesExhausted(_)),
        "the assembly was not reserved before it allocated: {error}"
    );
    assert_eq!(
        pool.reserved(),
        0,
        "the stopped execution kept a reservation"
    );
    assert_eq!(execution.state().reserved_bytes(), 0);
    assert_eq!(
        execution.state().nodes_holding_binary(),
        0,
        "the stopped execution kept the operands it retained"
    );
    let stopped = execution.next_chunk().unwrap_err();
    assert!(
        stopped
            .to_string()
            .contains("stopped after an earlier error"),
        "a stopped execution answered again: {stopped}"
    );
}

/// The assembly bound covers the vector a field's descriptors are flattened into, on the shape that
/// grows it the most.
///
/// One chunk's field can itself hold 1024 segments. Beside a second chunk's one, [`Samples::try_from_chunks`]
/// copies all 1025 descriptors into a vector that doubled to 2048 slots, and then into the
/// `Arc<[Samples]>` it returns, so 3073 slots are live at once: two slots per segment -- 2052 here --
/// would reserve less than the assembly allocates. The segments are dense slices of one histogram
/// table, which the assembled field keeps sharing rather than copying.
#[test]
fn an_assembly_bound_covers_the_capacity_its_flatten_grows_to() {
    let (segments, chunks) = (1025usize, 2usize);
    let grid = grid(0, 60_000, segments as u64);
    let schema = evaluated_schema(Arc::clone(&grid));
    // One table every segment indexes into, so the assembly has one allocation to share.
    let mut builder = HistogramBatchBuilder::new();
    for _ in 0..segments {
        push_histogram(&mut builder, 4);
    }
    let table = builder.finish().unwrap();
    let one = |index: usize| {
        let index = u32::try_from(index).unwrap();
        Samples::try_from_histogram(UInt32Array::from(vec![Some(index)]), Arc::clone(&table))
            .unwrap()
    };
    // The first chunk's field is itself chunked -- 1024 one-sample segments -- and the second holds
    // one, so the two chunks of the one logical series hold 1025 segments in all.
    let nested = Samples::try_from_chunks((0..1024).map(one).collect::<Vec<_>>()).unwrap();
    assert_eq!(
        nested.segments().len(),
        1024,
        "the first field is not nested"
    );
    let chunk_of = |start: usize, len: usize, samples: Samples, is_last: bool| {
        EvaluatedChunk::try_new(
            Series {
                metric: metric(),
                time: TimeAxis::Grid(GridRange::try_new(Arc::clone(&grid), start, len).unwrap()),
                fields: SeriesFields::one(samples),
            },
            is_last,
        )
        .unwrap()
    };
    let first = chunk_of(0, 1024, nested, false);
    let second = chunk_of(1024, 1, one(1024), true);

    // What the flatten the assembly runs holds at its peak, measured with the operations it makes:
    // a chunked field contributes its segments and a dense one contributes itself.
    let mut flat: Vec<Samples> = Vec::new();
    for chunk in [&first, &second] {
        let field = &chunk.series.fields.as_slice()[0];
        if field.segments().len() > 1 {
            flat.extend(field.segments().iter().cloned());
        } else {
            flat.push(field.clone());
        }
    }
    // The `Arc<[Samples]>` holds one slot per descriptor while the vector they were copied out of is
    // still alive, so the peak is that vector's capacity plus the descriptors it held.
    let peak = flat.capacity() + flat.len();
    assert_eq!(
        flat.len(),
        segments,
        "the flatten did not meet every segment"
    );
    assert!(
        flat.capacity() > flat.len(),
        "the flatten grew to no more than its descriptors, so this shape proves nothing"
    );
    // The bound the assembly is reserved for: one descriptor per chunk per field, three per segment,
    // and the four slots a first push allocates.
    let allowed = chunks + 3 * segments + 4;
    assert!(
        peak <= allowed,
        "the flatten of {segments} segments in {chunks} chunks holds {peak} slots at its peak, past \
         the {allowed} the assembly bound allows"
    );

    let pool = pool();
    let mut runtime = binary_runtime(&pool, &schema);
    for chunk in [first, second.clone()] {
        runtime
            .retain_binary(&NodePath::root(), 0, NativeChunk::Evaluated(chunk))
            .unwrap();
    }
    runtime
        .retain_binary(&NodePath::root(), 1, NativeChunk::Evaluated(second))
        .unwrap();
    let assembly = runtime.binary.as_ref().unwrap().assembly_bytes().unwrap();
    assert!(
        peak * std::mem::size_of::<Samples>() <= assembly,
        "the {assembly} bytes reserved for the assembly do not cover the {peak} slots its flatten \
         holds"
    );

    // The assembled series keeps the segments and the one table they index into.
    let assembled = runtime
        .binary
        .as_ref()
        .unwrap()
        .assemble(&NodePath::root())
        .unwrap();
    assert_eq!(assembled[0].len(), 1, "one closed series is one series");
    assert_eq!(assembled[0][0].time.len(), segments);
    let field = &assembled[0][0].fields.as_slice()[0];
    assert_eq!(field.segments().len(), segments);
    for segment in field.segments() {
        let Some(DenseSamplesRef::Histogram(histograms)) = segment.dense() else {
            panic!("a histogram chunk assembles into histogram segments");
        };
        assert!(
            Arc::ptr_eq(histograms.batch(), &table),
            "the assembly copied the histogram table instead of sharing it"
        );
    }
    let built = EvaluatedChunk::try_new(assembled[0][0].clone(), true).unwrap();
    assert_eq!(histogram_counts(&built), vec![4.0; segments]);
}

/// A mixed operand delivered in two chunks over a sparse axis assembles into one series that keeps
/// its holes, both its fields and the allocations they are slices of.
///
/// Field 0 holds native histograms and field 1 floats, both chunks index one table, and the
/// positions they decide are 0, 2 and 3 of a four-point grid. The assembly keeps one descriptor per
/// segment instead of copying a sample, so the assembled series reads the same table and the same
/// float buffers the chunks read, and the position neither chunk decided stays a hole. The kernel
/// then answers that series as one candidate, which is what the result below is computed from.
#[test]
fn a_mixed_nested_operand_assembles_into_one_series_with_its_holes() {
    let grid = grid(0, 60_000, 4);
    let schema = Arc::new(SeriesSchema {
        fields: field_count(2),
        eval_grid: Some(Arc::clone(&grid)),
    });
    // One table both chunks index into.
    let mut builder = HistogramBatchBuilder::new();
    for _ in 0..2 {
        push_histogram(&mut builder, 4);
    }
    let table = builder.finish().unwrap();
    let chunk_of = |spans: &[(usize, usize)],
                    indices: Vec<Option<u32>>,
                    floats: Vec<Option<f64>>,
                    is_last: bool| {
        let histograms =
            Samples::try_from_histogram(UInt32Array::from(indices), Arc::clone(&table)).unwrap();
        let time = TimeAxis::from_grid_spans(
            Arc::clone(&grid),
            spans
                .iter()
                .map(|&(start, len)| GridSpan::new(start, len))
                .collect(),
        )
        .unwrap();
        EvaluatedChunk::try_new(
            Series {
                metric: metric(),
                time,
                fields: SeriesFields::try_from_vec(vec![
                    histograms,
                    Samples::from_float(Float64Array::from(floats)),
                ])
                .unwrap(),
            },
            is_last,
        )
        .unwrap()
    };
    let (left_a, left_b) = (
        chunk_of(
            &[(0, 1), (2, 1)],
            vec![Some(0), Some(0)],
            vec![Some(1.0), None],
            false,
        ),
        chunk_of(&[(3, 1)], vec![Some(1)], vec![Some(4.0)], true),
    );
    let (right_a, right_b) = (
        chunk_of(
            &[(0, 1), (2, 1)],
            vec![Some(1), Some(1)],
            vec![Some(10.0), Some(30.0)],
            false,
        ),
        chunk_of(&[(3, 1)], vec![Some(0)], vec![Some(40.0)], true),
    );

    // The two chunks are collected into the one series the kernel matches.
    let pool = pool();
    let mut runtime = binary_runtime(&pool, &schema);
    for chunk in [&left_a, &left_b] {
        runtime
            .retain_binary(&NodePath::root(), 0, NativeChunk::Evaluated(chunk.clone()))
            .unwrap();
    }
    let assembled = runtime
        .binary
        .as_ref()
        .unwrap()
        .assemble(&NodePath::root())
        .unwrap();
    assert_eq!(
        assembled[0].len(),
        1,
        "two chunks of one series are not one series"
    );
    let series = &assembled[0][0];
    assert_eq!(series.time.len(), 3);
    assert_eq!(series.time.first_position(), Some(0));
    assert_eq!(series.time.last_position(), Some(3));
    assert_eq!(
        series.time.runs().count(),
        3,
        "the assembly filled the hole neither chunk decided"
    );
    let histograms = &series.fields.as_slice()[0];
    assert_eq!(
        histograms.segments().len(),
        2,
        "the chunks' histograms were not collected"
    );
    for segment in histograms.segments() {
        let Some(DenseSamplesRef::Histogram(samples)) = segment.dense() else {
            panic!("a histogram chunk assembles into histogram segments");
        };
        assert!(
            Arc::ptr_eq(samples.batch(), &table),
            "the assembly copied a histogram table instead of sharing it"
        );
    }
    let floats = &series.fields.as_slice()[1];
    assert_eq!(
        floats.segments().len(),
        2,
        "the chunks' floats were not collected"
    );
    for (segment, chunk) in floats.segments().iter().zip([&left_a, &left_b]) {
        let (Some(DenseSamplesRef::Float(values)), Some(DenseSamplesRef::Float(source))) =
            (segment.dense(), chunk.series.fields.as_slice()[1].dense())
        else {
            panic!("a float chunk assembles into float segments");
        };
        assert_eq!(
            values.values().inner().data_ptr(),
            source.values().inner().data_ptr(),
            "the assembly copied floats instead of sharing the buffer they are slices of"
        );
    }

    // The kernel reads the assembled series, over the two fields and the positions it holds.
    let root = binary_of(
        evaluated_input_of(2, Arc::clone(&grid)),
        evaluated_input_of(2, Arc::clone(&grid)),
        binary_matching(BinaryOp::Add),
    );
    let tree = NativeTree::try_new(root).unwrap();
    let (left, right) = (NodePath::root().left(), NodePath::root().right());
    let mut execution = executor(&tree, Arc::clone(&pool), &ExecutionPlanMetricsSet::new());
    for chunk in [left_a, left_b] {
        execution.push_evaluated_at(&left, chunk).unwrap();
    }
    for chunk in [right_a, right_b] {
        execution.push_evaluated_at(&right, chunk).unwrap();
    }
    let out = run(&mut execution);
    assert_eq!(out.len(), 1, "one matched label set answers one chunk");
    assert_eq!(
        field_values(&out[0], 1),
        vec![Some(11.0), None, Some(44.0)],
        "the floats of the two chunks are not added at the positions they hold"
    );
    assert_eq!(
        histogram_counts(&out[0]),
        vec![8.0, 8.0, 8.0],
        "the histograms of the two chunks are not added at the positions they hold"
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

/// A legitimate empty result over a wide operand finishes in a 4 MiB pool and holds nothing after.
///
/// A match group of one member collects no points in the duplicate scan, so charging a point per
/// row of an operand reserves memory the kernel never allocates.
#[test]
fn an_empty_result_of_a_wide_operand_fits_its_pool() {
    let points = 10_000;
    let (tree, grid, left, right) = binary_tree(BinaryOp::Add, points as u64);
    let schema = evaluated_schema(grid);
    let pool = bounded_pool(4 << 20);
    let mut execution = executor(&tree, Arc::clone(&pool), &ExecutionPlanMetricsSet::new());
    let series = |job: &str| pod_metric("requests", job, "p");
    execution
        .push_evaluated_at(
            &left,
            evaluated_chunk_of(&schema, series("left"), 0, vec![Some(1.0); points], true),
        )
        .unwrap();
    assert!(execution.next_chunk().unwrap().is_none());
    for index in 0..32 {
        execution
            .push_evaluated_at(
                &right,
                evaluated_chunk_of(
                    &schema,
                    series(&format!("r{index:02}")),
                    0,
                    vec![Some(2.0); points],
                    true,
                ),
            )
            .unwrap();
        assert!(execution.next_chunk().unwrap().is_none());
    }

    let finished = execution.finish();
    assert!(
        finished.is_ok(),
        "an empty result over {points} points does not fit a 4 MiB pool: {:?}",
        finished.as_ref().err()
    );
    assert!(
        finished.unwrap().is_empty(),
        "nothing matches, so nothing is answered"
    );
    assert_eq!(pool.reserved(), 0);
    assert_eq!(execution.state().reserved_bytes(), 0);
}

/// A legitimate wide query finishes in a 32 MiB pool, keeping every field's sum and nothing else.
///
/// A thousand points pair all sixty-four fields of both operands, so the workspace is the rows
/// filed and the tables beside them, not a figure far past either.
#[test]
fn a_dense_wide_result_fits_a_32_mib_pool() {
    let (points, width) = (1_000usize, 64usize);
    let grid = grid(0, 60_000, points as u64);
    let (left_node, right_node) = (
        evaluated_input_of(width, Arc::clone(&grid)),
        evaluated_input_of(width, Arc::clone(&grid)),
    );
    let schema = left_node
        .output
        .samples()
        .expect("a source of evaluated samples")
        .clone();
    let root = binary_of(left_node, right_node, binary_matching(BinaryOp::Add));
    let tree = NativeTree::try_new(root).unwrap();
    let pool = bounded_pool(32 << 20);
    let mut execution = executor(&tree, Arc::clone(&pool), &ExecutionPlanMetricsSet::new());
    let columns = |value: f64| -> Vec<Vec<Option<f64>>> {
        (0..width).map(|_| vec![Some(value); points]).collect()
    };
    let (left, right) = (NodePath::root().left(), NodePath::root().right());
    execution
        .push_evaluated_at(
            &left,
            evaluated_columns(&schema, metric(), 0, columns(1.0), true),
        )
        .unwrap();
    execution
        .push_evaluated_at(
            &right,
            evaluated_columns(&schema, metric(), 0, columns(2.0), true),
        )
        .unwrap();
    assert!(execution.next_chunk().unwrap().is_none());

    let finished = execution.finish();
    assert!(
        finished.is_ok(),
        "{width} fields over {points} points do not fit a 32 MiB pool: {:?}",
        finished.as_ref().err()
    );
    let out = finished.unwrap();
    assert_eq!(out.len(), 1, "one matched label set answers one chunk");
    let chunk = &out[0];
    assert!(chunk.is_last, "a result series is handed over closed");
    assert_eq!(chunk.series.time.len(), points);
    assert_eq!(series_start(chunk), 0);
    assert_eq!(chunk.series.fields.len(), width);
    for field in 0..width {
        assert_eq!(
            field_values(chunk, field),
            vec![Some(3.0); points],
            "field {field}"
        );
    }
    assert_eq!(pool.reserved(), 0);
    assert_eq!(execution.state().reserved_bytes(), 0);
}

/// An all-missing match group of two right series finishes in a 4 MiB pool and answers nothing.
///
/// No member carries a sample, so the duplicate scan needs no point buffer.
#[test]
fn an_all_missing_match_group_fits_its_pool() {
    let points = 100_000;
    let (tree, grid, left, right) = binary_tree(BinaryOp::Add, points as u64);
    let schema = evaluated_schema(grid);
    let pool = bounded_pool(4 << 20);
    let mut execution = executor(&tree, Arc::clone(&pool), &ExecutionPlanMetricsSet::new());
    execution
        .push_evaluated_at(
            &left,
            evaluated_chunk_of(&schema, metric(), 0, vec![Some(1.0); points], true),
        )
        .unwrap();
    assert!(execution.next_chunk().unwrap().is_none());
    // Both right chunks close the one label set, so its match group has two members.
    for _ in 0..2 {
        execution
            .push_evaluated_at(
                &right,
                evaluated_chunk_of(&schema, metric(), 0, vec![None; points], true),
            )
            .unwrap();
        assert!(execution.next_chunk().unwrap().is_none());
    }

    let finished = execution.finish();
    assert!(
        finished.is_ok(),
        "an all-missing match group over {points} points does not fit a 4 MiB pool: {:?}",
        finished.as_ref().err()
    );
    assert!(
        finished.unwrap().is_empty(),
        "no point holds a value on both sides, so nothing is answered"
    );
    assert_eq!(pool.reserved(), 0);
    assert_eq!(execution.state().reserved_bytes(), 0);
}

/// `label_replace` runs in a map: each series is relabelled as it passes, and two series it gives
/// one label set collide where both hold a value, as Prometheus' same-labelset check refuses.
#[test]
fn a_label_rewrite_relabels_series_and_refuses_a_shared_point() {
    let grid = grid(0, 60_000, 2);
    let schema = evaluated_schema(Arc::clone(&grid));
    let rewrite = LabelRewrite::replace("job", "all", "job", ".*").unwrap();
    let tree = NativeTree::try_new(map_ops(
        evaluated_input(Arc::clone(&grid)),
        vec![MapOp::Label(Arc::new(rewrite))],
    ))
    .unwrap();
    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    execution
        .push_evaluated(evaluated_chunk_of(
            &schema,
            named_metric("a", "x"),
            0,
            vec![Some(1.0), Some(2.0)],
            true,
        ))
        .unwrap();
    let first = execution
        .next_chunk()
        .unwrap()
        .expect("the relabelled series");
    assert_eq!(first.series.metric, named_metric("a", "all"));
    execution
        .push_evaluated(evaluated_chunk_of(
            &schema,
            named_metric("a", "y"),
            1,
            vec![Some(3.0)],
            true,
        ))
        .unwrap();
    let error = execution.next_chunk().unwrap_err().to_string();
    assert!(
        error.contains("both hold a value of field 0 at position 1"),
        "{error}"
    );
}

/// A chunk that math empties still closes its series, under the labels the whole chain would have
/// given it: the label changes after the emptying step apply, and math drops the name.
#[test]
fn an_emptied_closing_chunk_carries_the_labels_of_the_whole_chain() {
    let grid = grid(0, 60_000, 2);
    let schema = evaluated_schema(Arc::clone(&grid));
    let replace = LabelRewrite::replace("job", "y", "job", "x").unwrap();
    let join = LabelRewrite::join("both", "-", &["__name__", "job"]).unwrap();
    let tree = NativeTree::try_new(map_ops(
        evaluated_input(Arc::clone(&grid)),
        vec![
            MapOp::Label(Arc::new(replace)),
            MapOp::Math(MathOp::Clamp { min: 2.0, max: 1.0 }),
            MapOp::Label(Arc::new(join)),
        ],
    ))
    .unwrap();
    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    execution
        .push_evaluated(evaluated_chunk_of(
            &schema,
            named_metric("a", "x"),
            0,
            vec![Some(1.0), Some(2.0)],
            true,
        ))
        .unwrap();
    let chunks = run(&mut execution);
    let [closing] = chunks.as_slice() else {
        panic!("expected one closing chunk, got {}", chunks.len());
    };
    assert!(closing.is_last);
    assert_eq!(closing.series.time.len(), 0);
    // The name is gone before `label_join` reads it, and `job` was rewritten before the clamp.
    assert_eq!(closing.series.metric.name(), None);
    assert_eq!(closing.series.metric.get("both"), Some("-y"));
}

/// A histogram function maps only the positions native histograms hold, dropping the float one
/// between them, and drops the name.
#[test]
fn a_histogram_function_maps_the_histogram_positions() {
    let grid = grid(0, 60_000, 3);
    let mut builder = HistogramBatchBuilder::new();
    let first = push_histogram(&mut builder, 2);
    let second = push_histogram(&mut builder, 5);
    let table = builder.finish().unwrap();
    let histogram = |index| {
        Samples::try_from_histogram(UInt32Array::from(vec![index]), Arc::clone(&table)).unwrap()
    };
    let field = Samples::try_from_chunks(vec![
        histogram(first),
        Samples::from_float(Float64Array::from(vec![7.0])),
        histogram(second),
    ])
    .unwrap();
    let series = Series {
        metric: named_metric("a", "x"),
        time: TimeAxis::Grid(GridRange::try_new(Arc::clone(&grid), 0, 3).unwrap()),
        fields: SeriesFields::try_from_vec(vec![field]).unwrap(),
    };
    let tree = NativeTree::try_new(map_ops(
        evaluated_input(Arc::clone(&grid)),
        vec![MapOp::Histogram(NativeHistogramFunction::Count)],
    ))
    .unwrap();
    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    execution
        .push_evaluated(EvaluatedChunk::try_new(series, true).unwrap())
        .unwrap();
    let chunks = run(&mut execution);
    let [chunk] = chunks.as_slice() else {
        panic!("expected one chunk, got {}", chunks.len());
    };
    assert_eq!(chunk.series.metric.name(), None);
    assert_eq!(chunk.series.metric.get("job"), Some("x"));
    let positions = chunk
        .series
        .time
        .positions()
        .map(|(position, _)| position)
        .collect::<Vec<_>>();
    assert_eq!(positions, vec![0, 2]);
    assert_eq!(sample_values(chunk), vec![Some(2.0), Some(5.0)]);
}

/// A histogram function over a closing float chunk empties it, and the closing chunk is named as
/// Prometheus' `simpleHistogramFunc` names its output: without `__name__` but with `__type__`,
/// which math functions drop.
#[test]
fn a_histogram_function_names_an_emptied_closing_chunk_like_its_kernel() {
    let grid = grid(0, 60_000, 2);
    let schema = evaluated_schema(Arc::clone(&grid));
    let metric = MetricName::try_new(
        Some("a".into()),
        vec![Label {
            name: "__type__".into(),
            value: "histogram".into(),
        }],
    )
    .unwrap();
    let tree = NativeTree::try_new(map_ops(
        evaluated_input(Arc::clone(&grid)),
        vec![MapOp::Histogram(NativeHistogramFunction::Sum)],
    ))
    .unwrap();
    let mut execution = executor(&tree, pool(), &ExecutionPlanMetricsSet::new());
    execution
        .push_evaluated(evaluated_chunk_of(
            &schema,
            metric,
            0,
            vec![Some(1.0), Some(2.0)],
            true,
        ))
        .unwrap();
    let chunks = run(&mut execution);
    let [closing] = chunks.as_slice() else {
        panic!("expected one closing chunk, got {}", chunks.len());
    };
    assert!(closing.is_last);
    assert_eq!(closing.series.time.len(), 0);
    assert_eq!(closing.series.metric.name(), None);
    assert_eq!(closing.series.metric.get("__type__"), Some("histogram"));
}

/// A label change reads no value, so a map of label changes alone hands histogram samples and
/// holes over as they came, where a math function would drop them.
#[test]
fn a_label_only_map_keeps_histogram_samples() {
    let grid = grid(0, 60_000, 3);
    let schema = evaluated_schema(Arc::clone(&grid));
    let table = histogram_table(2, 3);
    let chunk = evaluated_histogram_chunk_of(
        &schema,
        named_metric("a", "x"),
        0,
        vec![Some(0), None, Some(1)],
        &table,
        true,
    );
    let rewrite = LabelRewrite::replace("job", "y", "job", "x").unwrap();
    let mapped = map_chunk(&schema, &chunk, &[MapOp::Label(Arc::new(rewrite))])
        .unwrap()
        .unwrap();
    assert_eq!(mapped.series.metric.get("job"), Some("y"));
    assert_eq!(mapped.series.time.len(), 3);
    let samples = mapped.series.fields.as_slice()[0]
        .iter()
        .collect::<Vec<_>>();
    assert!(matches!(
        samples.as_slice(),
        [
            SampleRef::Histogram(_),
            SampleRef::Missing,
            SampleRef::Histogram(_)
        ]
    ));
}
