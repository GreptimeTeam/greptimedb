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

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::{Arc, Mutex};

use common_query::prometheus::PROMETHEUS_STALE_NAN_BITS;
use datafusion::arrow::array::{ArrayRef, Float64Array, TimestampMillisecondArray, UInt32Array};
use datafusion::arrow::buffer::{Buffer, ScalarBuffer};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ColumnarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::ScalarFunctionArgs;
use datatypes::series::histogram::{
    BucketSideView, CounterResetHint, HistogramBatchBuilder, HistogramView,
};
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{
    FieldSpec, HistogramSamples, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{EvalGrid, GridRange, GridSpec, TimeAxis};
use datatypes::series::{Label, MetricName};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::{
    CounterReset, FloatWindow, RangeEvaluator, RangeFunction, RangeWindow, WindowKernel,
    subquery_grid,
};
use crate::evaluated::EvaluatedChunk;
use crate::functions::{
    Changes, CountOverTime, IDelta, LastOverTime, MaxOverTime, MinOverTime, PresentOverTime, Resets,
};
use crate::grid_series::reference::{
    Dense, GoldenCase, OwnedHistogram, assert_histogram_close, assert_series_eq,
    assert_series_eq_any_nan, close, expand, golden_cases, labels_of, metric_of, mixed_series,
    random_dense, special_value,
};
use crate::grid_series::{GridSample, GridSeriesBuilder};
use crate::histogram_eval::float_histogram::FloatHistogram;
use crate::map::{MathOp, map_chunk};
use crate::range_array::RangeArray;
use crate::select::{LabelMatcher, MatchOp, absent, absent_labels};

/// One raw row: source timestamp and a value per field, `None` when missing.
type Row = (i64, Vec<Option<f64>>);

fn stale() -> f64 {
    f64::from_bits(PROMETHEUS_STALE_NAN_BITS)
}

/// Encodes the exact window membership, so any boundary or alignment error changes the value.
fn probe(window: FloatWindow<'_>) -> Option<f64> {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    let mut mix = |value: u64| hash = (hash ^ value).wrapping_mul(0x0100_0000_01b3);
    mix(window.eval_ts as u64);
    mix(window.range_ms as u64);
    for (timestamp, value) in window.timestamps.iter().zip(window.values) {
        mix(*timestamp as u64);
        mix(value.to_bits());
    }
    // The reference builds these by scanning the window, so hashing them turns every random
    // differential test into a check of the evaluator's reset index as well.
    for reset in window.counter_resets {
        mix(reset.previous_value.to_bits());
    }
    Some((hash >> 11) as f64)
}

/// The resets of a window by scanning it, the way the kernels used to before the index.
fn scanned_resets(values: &[f64]) -> Vec<CounterReset> {
    (1..values.len())
        .filter(|&index| values[index] < values[index - 1])
        .map(|index| CounterReset {
            position: index as u64,
            previous_value: values[index - 1],
        })
        .collect()
}

fn raw_schema(fields: usize) -> SeriesSchema {
    SeriesSchema {
        fields: (0..fields)
            .map(|index| FieldSpec {
                name: Arc::from(format!("f{index}")),
            })
            .collect::<Vec<_>>()
            .into(),
        eval_grid: None,
    }
}

fn grid(start_ms: i64, step_ms: i64, len: u64) -> Arc<EvalGrid> {
    Arc::new(EvalGrid::try_new(GridSpec::try_new(start_ms, step_ms, len).unwrap()).unwrap())
}

fn metric(host: u64) -> MetricName {
    MetricName::try_new(
        Some(Arc::from("http_requests")),
        vec![Label {
            name: Arc::from("host"),
            value: Arc::from(format!("h{host}")),
        }],
    )
    .unwrap()
}

fn probe_evaluator(fields: usize, grid: Arc<EvalGrid>, window: RangeWindow) -> RangeEvaluator {
    let kernel: WindowKernel = Box::new(probe);
    RangeEvaluator::with_kernel(&raw_schema(fields), grid, window, kernel, true, true).unwrap()
}

/// A probe evaluator whose anchored result is evaluated at every point, like `predict_linear`.
fn per_point_probe_evaluator(
    fields: usize,
    grid: Arc<EvalGrid>,
    window: RangeWindow,
) -> RangeEvaluator {
    let kernel: WindowKernel = Box::new(probe);
    RangeEvaluator::with_kernel(&raw_schema(fields), grid, window, kernel, false, true).unwrap()
}

/// The metric of a result of `function` over the series of `metric(host)`.
fn output_metric(function: RangeFunction, host: u64) -> MetricName {
    let mut metric = metric(host);
    if function.drops_metric_name() {
        metric.remove("__name__");
    }
    metric
}

/// Builds a chunk whose timestamp and value arrays are slices at a nonzero offset.
fn chunk(rows: &[Row], fields: usize, host: u64, is_last: bool) -> RawSeriesChunk {
    let padding = 3;
    let timestamps: Vec<_> = (0..padding)
        .map(|i| i64::MIN + i)
        .chain(rows.iter().map(|(timestamp, _)| *timestamp))
        .collect();
    let time =
        RawTimestamps::try_new(ScalarBuffer::from(timestamps).slice(padding as usize, rows.len()))
            .unwrap();
    let samples = (0..fields)
        .map(|field| {
            let values: Vec<_> = std::iter::repeat_n(Some(-1.0), padding as usize)
                .chain(rows.iter().map(|(_, values)| values[field]))
                .collect();
            Samples::from_float(Float64Array::from(values).slice(padding as usize, rows.len()))
        })
        .collect();
    RawSeriesChunk::try_new(
        &raw_schema(fields),
        Buffer::from(host.to_le_bytes().to_vec()),
        Some(metric(host)),
        time,
        SeriesFields::try_from_vec(samples).unwrap(),
        is_last,
    )
    .unwrap()
}

/// Splits `rows` into chunks at random points; some fields are stored as chunked samples.
fn random_chunks(rng: &mut StdRng, rows: &[Row], fields: usize, host: u64) -> Vec<RawSeriesChunk> {
    let mut chunks = Vec::new();
    let mut start = 0;
    while start < rows.len() {
        let end = rng.random_range(start + 1..=rows.len());
        let mut piece = chunk(&rows[start..end], fields, host, end == rows.len());
        if end - start > 1 && rng.random_bool(0.3) {
            let split = rng.random_range(1..end - start);
            let samples = piece
                .fields
                .iter()
                .map(|field| {
                    Samples::try_from_chunks(vec![
                        field.slice(0, split).unwrap(),
                        field.slice(split, end - start - split).unwrap(),
                    ])
                    .unwrap()
                })
                .collect();
            piece.fields = SeriesFields::try_from_vec(samples).unwrap();
        }
        chunks.push(piece);
        start = end;
    }
    chunks
}

/// Whether every sample of every field is missing.
fn all_missing(series: &Series) -> bool {
    series.fields.iter().all(|field| {
        (0..field.len()).all(|index| matches!(field.sample(index), SampleRef::Missing))
    })
}

/// Pushes every chunk of one series, keeping what the evaluator hands out on the way.
fn collect_chunks(
    evaluator: &mut RangeEvaluator,
    pieces: &[RawSeriesChunk],
) -> Vec<EvaluatedChunk> {
    pieces
        .iter()
        .flat_map(|piece| evaluator.push(piece).unwrap())
        .collect()
}

/// Folds the chunks of one series back into one series, the way a consumer that keeps every value
/// would, and checks the chunk protocol: absolute ranges, never going backwards, all of the same
/// series, and one closing chunk last.
///
/// The range check is explicit rather than left to the builder, whose own ordering check only sees
/// the rows holding a value: a closing chunk with no rows must still start at or after the end of
/// the previous chunk, and an empty chunk at a wrong position is exactly what would pass otherwise.
fn fold_chunks(chunks: &[EvaluatedChunk], schema: &Arc<SeriesSchema>) -> Option<Series> {
    let metric = chunks
        .first()
        .expect("a series always hands out its closing chunk")
        .series
        .metric
        .clone();
    let mut builder = GridSeriesBuilder::try_new(schema.clone()).unwrap();
    let mut closing = false;
    let mut previous_end = 0;
    for chunk in chunks {
        assert!(!closing, "a chunk follows the closing chunk");
        closing = chunk.is_last;
        assert_eq!(chunk.series.metric, metric, "a chunk changed the series");
        let range = chunk.range();
        assert!(
            range.start() >= previous_end,
            "chunk range {} goes back before the previous end {previous_end}",
            range.start()
        );
        previous_end = range.start() + range.len();
        let mut fields: Vec<_> = chunk
            .series
            .fields
            .iter()
            .map(|field| field.iter())
            .collect();
        for row in 0..range.len() {
            let values: Vec<_> = fields
                .iter_mut()
                .map(
                    |field| match field.next().expect("the range length is checked") {
                        SampleRef::Missing => None,
                        SampleRef::Float(value) => Some(GridSample::Float(value)),
                        SampleRef::Histogram(view) => Some(GridSample::Histogram(Box::new(
                            FloatHistogram::try_from_view(&view).unwrap(),
                        ))),
                    },
                )
                .collect();
            // Positions only increase across chunks, so an overlap is an error here.
            builder.push_samples(range.start() + row, &values).unwrap();
        }
    }
    assert!(closing, "the series never closed");
    builder.finish(metric).unwrap()
}

fn random_rows(rng: &mut StdRng, fields: usize) -> Vec<Row> {
    let missing = if rng.random_bool(0.3) { 0.0 } else { 0.15 };
    let counter = rng.random_bool(0.5);
    let mut timestamp = rng.random_range(-40_000..20_000);
    let mut totals = vec![rng.random_range(0.0..100.0); fields];
    (0..rng.random_range(1..60))
        .map(|_| {
            timestamp += if rng.random_bool(0.1) {
                rng.random_range(5_000..40_000)
            } else {
                rng.random_range(1..3_000)
            };
            let values = totals
                .iter_mut()
                .map(|total| {
                    if rng.random_bool(missing) {
                        None
                    } else if rng.random_bool(0.05) {
                        Some(stale())
                    } else if counter {
                        // A counter that occasionally resets.
                        *total = if rng.random_bool(0.05) {
                            rng.random_range(0.0..10.0)
                        } else {
                            *total + rng.random_range(0.0..50.0)
                        };
                        Some(*total)
                    } else {
                        Some(special_value(rng))
                    }
                })
                .collect();
            (timestamp, values)
        })
        .collect()
}

/// Rows on the evaluation timeline, without stale rows when `drop_stale`, and for each grid
/// point the range of those rows in its window, found by scanning all rows independently of
/// chunking and cursors.
fn reference_windows(
    rows: &[Row],
    grid: &EvalGrid,
    window: RangeWindow,
    drop_stale: bool,
) -> (Vec<Row>, Vec<Range<usize>>) {
    let kept: Vec<_> = rows
        .iter()
        .filter(|(_, values)| {
            !drop_stale
                || !values
                    .iter()
                    .flatten()
                    .any(|value| value.to_bits() == PROMETHEUS_STALE_NAN_BITS)
        })
        .map(|(timestamp, values)| (timestamp + fold_offset(grid, window), values.clone()))
        .collect();
    let windows = windows_of(&kept, grid, window);
    (kept, windows)
}

/// The offset samples are shifted by: with `@`, Prometheus rewrites the offset so that the
/// anchored window is the window of the first evaluation point.
fn fold_offset(grid: &EvalGrid, window: RangeWindow) -> i64 {
    match window.anchor_ms {
        Some(anchor) => grid.spec().start_ms - (anchor - window.offset_ms),
        None => window.offset_ms,
    }
}

/// The end of the window each grid point reads, on the evaluation timeline.
fn window_end(grid: &EvalGrid, window: RangeWindow, eval_ts: i64) -> i64 {
    match window.anchor_ms {
        Some(_) => grid.spec().start_ms,
        None => eval_ts,
    }
}

/// Rows of `rows`, already on the evaluation timeline, inside each grid point's window.
fn windows_of(rows: &[Row], grid: &EvalGrid, window: RangeWindow) -> Vec<Range<usize>> {
    grid.timestamps()
        .iter()
        .map(|&eval_ts| {
            let end = window_end(grid, window, eval_ts);
            let inside: Vec<_> = (0..rows.len())
                .filter(|&row| rows[row].0 > end - window.range_ms && rows[row].0 <= end)
                .collect();
            inside
                .first()
                .map_or(0..0, |&first| first..first + inside.len())
        })
        .collect()
}

/// Applies the probe to the valid samples of every reference window. With `@`, a step-invariant
/// probe is evaluated at the first point and others at each point.
fn reference(
    rows: &[Row],
    grid: &EvalGrid,
    window: RangeWindow,
    fields: usize,
    drop_stale: bool,
    step_invariant: bool,
) -> Dense {
    let (kept, windows) = reference_windows(rows, grid, window, drop_stale);
    grid.timestamps()
        .iter()
        .zip(windows)
        .map(|(&eval_ts, rows)| {
            (0..fields)
                .map(|field| {
                    let (timestamps, values): (Vec<_>, Vec<_>) = kept[rows.clone()]
                        .iter()
                        .filter_map(|(timestamp, row)| Some((*timestamp, row[field]?)))
                        .unzip();
                    let counter_resets = scanned_resets(&values);
                    (!timestamps.is_empty()).then(|| {
                        probe(FloatWindow {
                            timestamps: &timestamps,
                            values: &values,
                            counter_resets: &counter_resets,
                            eval_ts: if step_invariant {
                                window_end(grid, window, eval_ts)
                            } else {
                                eval_ts
                            },
                            range_ms: window.range_ms,
                        })
                        .unwrap()
                    })
                })
                .collect()
        })
        .collect()
}

/// Evaluates every reference window with the ordinary range UDF, on the row layout
/// `RangeManipulate` produces: shifted timestamps and nullable values per field. Only functions
/// whose UDF agrees with Prometheus are checked this way; the others are checked against
/// Prometheus itself in `range_functions_match_prometheus`.
fn udf_reference(
    function: RangeFunction,
    rows: &[Row],
    grid: &EvalGrid,
    window: RangeWindow,
    fields: usize,
    drop_stale: bool,
) -> Dense {
    let (kept, windows) = reference_windows(rows, grid, window, drop_stale);
    let ranges: Vec<_> = windows
        .iter()
        .map(|rows| (rows.start as u32, rows.len() as u32))
        .collect();
    let timestamps: ArrayRef = Arc::new(TimestampMillisecondArray::from_iter_values(
        kept.iter().map(|(timestamp, _)| *timestamp),
    ));
    let udf = match function {
        RangeFunction::Irate => IDelta::<true>::scalar_udf(),
        RangeFunction::Idelta => IDelta::<false>::scalar_udf(),
        RangeFunction::MinOverTime => MinOverTime::scalar_udf(),
        RangeFunction::MaxOverTime => MaxOverTime::scalar_udf(),
        RangeFunction::CountOverTime => CountOverTime::scalar_udf(),
        RangeFunction::LastOverTime => LastOverTime::scalar_udf(),
        RangeFunction::PresentOverTime => PresentOverTime::scalar_udf(),
        RangeFunction::Resets => Resets::scalar_udf(),
        RangeFunction::Changes => Changes::scalar_udf(),
        other => unreachable!("{other:?} is checked against Prometheus"),
    };
    let mut dense = vec![vec![None; fields]; ranges.len()];
    for field in 0..fields {
        let values: ArrayRef = Arc::new(Float64Array::from_iter(
            kept.iter().map(|(_, values)| values[field]),
        ));
        let args = vec![
            ColumnarValue::Array(Arc::new(
                RangeArray::from_ranges(timestamps.clone(), ranges.iter().copied())
                    .unwrap()
                    .into_dict(),
            )),
            ColumnarValue::Array(Arc::new(
                RangeArray::from_ranges(values, ranges.iter().copied())
                    .unwrap()
                    .into_dict(),
            )),
        ];
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(index, arg)| Arc::new(Field::new(format!("a{index}"), arg.data_type(), true)))
            .collect();
        let output = udf
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields,
                number_rows: ranges.len(),
                return_field: Arc::new(Field::new("r", DataType::Float64, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap()
            .into_array(ranges.len())
            .unwrap();
        let output = output.as_any().downcast_ref::<Float64Array>().unwrap();
        for (row, value) in dense.iter_mut().zip(output.iter()) {
            row[field] = value;
        }
    }
    dense
}

/// Random grid, range and offset, with ranges below, at and above the step.
fn random_window(rng: &mut StdRng) -> (Arc<EvalGrid>, RangeWindow) {
    let step = rng.random_range(1..5_000);
    let grid = grid(
        rng.random_range(-30_000..10_000),
        step,
        rng.random_range(1..80),
    );
    let range_ms = match rng.random_range(0..5) {
        0 => 1,
        1 => step,
        2 => step + 1,
        3 => 2 * step + 7,
        _ => rng.random_range(1..30_000),
    };
    let offset_ms = match rng.random_range(0..3) {
        0 => 0,
        _ => rng.random_range(-20_000..20_000),
    };
    let anchor_ms = rng
        .random_bool(0.25)
        .then(|| rng.random_range(-40_000..60_000));
    (
        grid,
        RangeWindow {
            range_ms,
            offset_ms,
            anchor_ms,
        },
    )
}

#[test]
fn evaluator_matches_brute_force_reference() {
    for seed in 0..500 {
        let mut rng = StdRng::seed_from_u64(seed);
        let fields = rng.random_range(1..=3);
        let (grid, window) = random_window(&mut rng);
        let step_invariant = rng.random_bool(0.5);
        // One evaluator serves several series, so state must not leak between them.
        let mut evaluator = if step_invariant {
            probe_evaluator(fields, grid.clone(), window)
        } else {
            per_point_probe_evaluator(fields, grid.clone(), window)
        };
        for host in 0..3 {
            let rows = random_rows(&mut rng, fields);
            let expected = reference(&rows, &grid, window, fields, true, step_invariant);
            let chunks = collect_chunks(
                &mut evaluator,
                &random_chunks(&mut rng, &rows, fields, host),
            );
            let result = fold_chunks(&chunks, evaluator.output_schema());
            assert_series_eq(result.as_ref(), &expected, &metric(host));
        }
        evaluator.finish().unwrap();
    }
}

/// Functions whose range UDF agrees with Prometheus.
fn udf_functions() -> Vec<RangeFunction> {
    vec![
        RangeFunction::Irate,
        RangeFunction::Idelta,
        RangeFunction::MinOverTime,
        RangeFunction::MaxOverTime,
        RangeFunction::CountOverTime,
        RangeFunction::LastOverTime,
        RangeFunction::PresentOverTime,
        RangeFunction::Resets,
        RangeFunction::Changes,
    ]
}

#[test]
fn functions_match_range_udfs() {
    for function in udf_functions() {
        for seed in 0..300 {
            let mut rng = StdRng::seed_from_u64(seed);
            let fields = rng.random_range(1..=3);
            let (grid, window) = random_window(&mut rng);
            let mut evaluator =
                RangeEvaluator::try_new(&raw_schema(fields), grid.clone(), window, function)
                    .unwrap();
            for host in 0..3 {
                let rows = random_rows(&mut rng, fields);
                let expected = udf_reference(function, &rows, &grid, window, fields, true);
                let chunks = collect_chunks(
                    &mut evaluator,
                    &random_chunks(&mut rng, &rows, fields, host),
                );
                let result = fold_chunks(&chunks, evaluator.output_schema());
                assert_series_eq(result.as_ref(), &expected, &output_metric(function, host));
            }
        }
    }
}

/// A chunk of entirely present float columns is appended in bulk, and that is the path the test
/// asserts: the per-sample loop produces the same output, so comparing values alone could not
/// tell whether the bulk path ran.
#[test]
fn an_ordinary_float_chunk_is_appended_in_bulk() {
    let rows: Vec<Row> = (0..5)
        .map(|index| (index as i64 * 10_000, vec![Some(index as f64), Some(1.0)]))
        .collect();
    let mut evaluator = probe_evaluator(
        2,
        grid(0, 10_000, 5),
        RangeWindow {
            range_ms: 20_000,
            offset_ms: 0,
            anchor_ms: None,
        },
    );
    assert!(evaluator.push(&chunk(&rows, 2, 0, true)).unwrap().is_some());
    assert_eq!(evaluator.bulk_ingest_count(), 1);
}

/// A field holding native histogram samples keeps its windows on the histogram path, where the
/// mixed-window rules decide the result. Letting such a chunk take the bulk path would drop the
/// histograms and read the floats alone, so the chunk has to fall back to the per-sample loop.
#[test]
fn a_chunk_with_histograms_is_not_appended_in_bulk() {
    let histogram = "0/0/0/4014000000000000/4014000000000000/1/0_1/4014000000000000///";
    let mut builder = HistogramBatchBuilder::new();
    let index = builder
        .push(OwnedHistogram::parse(histogram).view())
        .unwrap();
    let histograms = HistogramSamples::try_new(
        UInt32Array::from(vec![None, Some(index), None]),
        builder.finish().unwrap(),
    )
    .unwrap();
    let samples = Samples::try_from_mixed(
        Float64Array::from(vec![Some(1.0), None, Some(2.0)]),
        histograms,
    )
    .unwrap();
    let chunk = RawSeriesChunk::try_new(
        &raw_schema(1),
        Buffer::from(0u64.to_le_bytes().to_vec()),
        Some(metric(0)),
        RawTimestamps::try_new(ScalarBuffer::from(vec![0, 10_000, 20_000])).unwrap(),
        SeriesFields::try_from_vec(vec![samples]).unwrap(),
        true,
    )
    .unwrap();
    let mut evaluator = RangeEvaluator::try_new(
        &raw_schema(1),
        grid(0, 10_000, 3),
        RangeWindow {
            range_ms: 20_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::PresentOverTime,
    )
    .unwrap();
    evaluator.push(&chunk).unwrap();
    assert_eq!(evaluator.bulk_ingest_count(), 0);
}

/// `rate` over a float-only series takes the batch path. The per-window path produces the same
/// output, so a test comparing values alone could not tell which one ran.
#[test]
fn rate_over_floats_is_evaluated_in_batches() {
    let rows: Vec<Row> = (0..20)
        .map(|index| (index as i64 * 10_000, vec![Some(index as f64)]))
        .collect();
    let mut evaluator = RangeEvaluator::try_new(
        &raw_schema(1),
        grid(0, 10_000, 20),
        RangeWindow {
            range_ms: 30_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
    )
    .unwrap();
    evaluator.push(&chunk(&rows, 1, 0, true)).unwrap();
    assert!(
        evaluator.batch_call_count() > 0,
        "rate over floats must reach the batch kernel"
    );
}

/// A batch spanning more grid points than `EVALUATION_BATCH` flushes in the middle of one
/// evaluation, and each field's windows come from that field's own samples.
///
/// Both properties are invisible to value comparisons over small single-field inputs: every field
/// sharing field 0's spans, or the mid-evaluation flush never running, would still pass the
/// golden cases (at most 20 points, one field). The comparison is against the per-window path,
/// which is the same kernel without batching, so a disagreement is the batching's fault alone.
#[test]
fn a_batch_spanning_fields_and_the_flush_threshold_matches_the_per_window_path() {
    let points = 600;
    let fields = 3;
    let grid = grid(0, 10_000, points);
    let window = RangeWindow {
        range_ms: 30_000,
        offset_ms: 0,
        anchor_ms: None,
    };
    // Counter-like values, each field resetting at a different point, growing at a different
    // rate, and missing at a different interval. The gaps are what make the fields' windows
    // differ: a field reading another field's windows would then get a different number, which a
    // shared dense layout would hide.
    let mut totals = [0.0f64; 3];
    let rows: Vec<Row> = (0..points)
        .map(|index| {
            let values = (0..fields)
                .map(|field| {
                    let step = [1.0, 2.0, 0.5][field];
                    totals[field] = if index == 100 * (field as u64 + 1) {
                        step
                    } else {
                        totals[field] + step
                    };
                    // Field 0 keeps every sample; the others drop some, at different intervals.
                    let gap = [0, 7, 5][field];
                    (gap == 0 || index % gap != 0).then_some(totals[field])
                })
                .collect();
            (index as i64 * 10_000, values)
        })
        .collect();

    let mut batch = RangeEvaluator::try_new(
        &raw_schema(fields),
        grid.clone(),
        window,
        RangeFunction::Rate,
    )
    .unwrap();
    let batched = batch
        .push(&chunk(&rows, fields, 0, true))
        .unwrap()
        .expect("the series completes");
    assert!(
        batch.batch_call_count() > 1,
        "the batch has to flush before the end of the series, not only on its last point"
    );

    let mut per_window = RangeEvaluator::with_kernel(
        &raw_schema(fields),
        grid,
        window,
        RangeFunction::Rate.kernel(0),
        true,
        true,
    )
    .unwrap();
    let expected = per_window
        .push(&chunk(&rows, fields, 0, true))
        .unwrap()
        .expect("the series completes");
    assert_eq!(per_window.batch_call_count(), 0);

    // Fold the chunk back into one series first: a chunk's range reports the positions its push
    // decided, which can include leading and trailing positions holding no value, while the
    // comparison is against the trimmed series the per-window path produced.
    let schema = batch.output_schema();
    let batched = fold_chunks(std::slice::from_ref(&batched), schema);
    let expected = expand(&expected.series);
    assert_series_eq(
        batched.as_ref(),
        &expected,
        &output_metric(RangeFunction::Rate, 0),
    );
}

/// A window holding a native histogram sample keeps its grid point on the per-window path, since
/// the batch kernel reads the floats alone and cannot reproduce the mixed-window rules.
///
/// The series is laid out so every grid point's window contains the histogram: one point fewer
/// than the first test, so no point can fall before it.
#[test]
fn a_histogram_window_keeps_rate_on_the_per_window_path() {
    let histogram = "0/0/0/4014000000000000/4014000000000000/1/0_1/4014000000000000///";
    let mut builder = HistogramBatchBuilder::new();
    let index = builder
        .push(OwnedHistogram::parse(histogram).view())
        .unwrap();
    let histograms = HistogramSamples::try_new(
        UInt32Array::from(vec![Some(index), None, None]),
        builder.finish().unwrap(),
    )
    .unwrap();
    let samples = Samples::try_from_mixed(
        Float64Array::from(vec![None, Some(1.0), Some(3.0)]),
        histograms,
    )
    .unwrap();
    let chunk = RawSeriesChunk::try_new(
        &raw_schema(1),
        Buffer::from(0u64.to_le_bytes().to_vec()),
        Some(metric(0)),
        RawTimestamps::try_new(ScalarBuffer::from(vec![0, 10_000, 20_000])).unwrap(),
        SeriesFields::try_from_vec(vec![samples]).unwrap(),
        true,
    )
    .unwrap();
    let mut evaluator = RangeEvaluator::try_new(
        &raw_schema(1),
        grid(0, 10_000, 3),
        RangeWindow {
            range_ms: 30_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
    )
    .unwrap();
    evaluator.push(&chunk).unwrap();
    assert_eq!(
        evaluator.batch_call_count(),
        0,
        "every window holds the histogram, so none may reach the batch kernel"
    );
}

/// A function without a batch form never reaches the batch kernel.
#[test]
fn a_function_without_a_batch_form_stays_on_the_per_window_path() {
    let rows: Vec<Row> = (0..10)
        .map(|index| (index as i64 * 10_000, vec![Some(index as f64)]))
        .collect();
    let mut evaluator = RangeEvaluator::try_new(
        &raw_schema(1),
        grid(0, 10_000, 10),
        RangeWindow {
            range_ms: 30_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::SumOverTime,
    )
    .unwrap();
    evaluator.push(&chunk(&rows, 1, 0, true)).unwrap();
    assert_eq!(evaluator.batch_call_count(), 0);
}

#[test]
fn empty_input_and_series_without_results() {
    let window = RangeWindow {
        range_ms: 10,
        offset_ms: 0,
        anchor_ms: None,
    };
    probe_evaluator(1, grid(0, 10, 4), window).finish().unwrap();

    // A series whose samples all miss the grid still closes, and its closing chunk says the whole
    // grid was decided: every point is a settled missing value, not a point still waiting.
    let mut evaluator = probe_evaluator(1, grid(0, 10, 4), window);
    let after_grid = chunk(&[(31, vec![Some(1.0)])], 1, 0, true);
    let closed = evaluator
        .push(&after_grid)
        .unwrap()
        .expect("a series closes");
    assert!(closed.is_last);
    assert_eq!(closed.range().start(), 0);
    assert_eq!(closed.range().len(), 4);
    assert!(all_missing(&closed.series));
    // A series whose only sample is stale does the same: the row is dropped, the points behind it
    // are decided.
    let only_stale = chunk(&[(5, vec![Some(stale())])], 1, 1, true);
    let closed = evaluator
        .push(&only_stale)
        .unwrap()
        .expect("a series closes");
    assert!(closed.is_last);
    assert_eq!((closed.range().start(), closed.range().len()), (0, 4));
    assert!(all_missing(&closed.series));
    evaluator.finish().unwrap();
}

/// A push that completes nothing must not hand out a chunk.
///
/// `evaluate` opens the builder's span at the first position it has not handed out yet, even when
/// this push settled no position, so finishing unconditionally yields a zero-length series rather
/// than none. Handing that out would report progress the push did not make.
#[test]
fn a_push_that_completes_nothing_hands_out_no_chunk() {
    let window = RangeWindow {
        range_ms: 10,
        offset_ms: 0,
        anchor_ms: None,
    };
    let mut evaluator = probe_evaluator(1, grid(10, 10, 3), window);
    // The only sample sits inside the first window, so the first point cannot be settled yet: no
    // position is completed and the push has nothing to report.
    let partial = chunk(&[(5, vec![Some(1.0)])], 1, 0, false);
    assert!(
        evaluator.push(&partial).unwrap().is_none(),
        "a push that completed no position must not hand out a chunk"
    );

    // The closing push still hands one out, and it reports every position of the grid.
    let closed = evaluator
        .push(&chunk(&[(100, vec![Some(2.0)])], 1, 0, true))
        .unwrap()
        .expect("a series closes");
    assert!(closed.is_last);
    assert_eq!((closed.range().start(), closed.range().len()), (0, 3));
    evaluator.finish().unwrap();
}

/// A series whose last sample is behind the end of the grid still closes, on a chunk holding no
/// positions: the remaining points have no values, and only the closing chunk tells the consumer
/// that the series is over rather than still waiting for input.
#[test]
fn a_series_that_ends_early_still_closes() {
    let window = RangeWindow {
        range_ms: 10,
        offset_ms: 0,
        anchor_ms: None,
    };
    let mut evaluator = probe_evaluator(1, grid(0, 10, 5), window);
    let first = chunk(&[(0, vec![Some(1.0)]), (10, vec![Some(2.0)])], 1, 0, false);
    let handed = evaluator
        .push(&first)
        .unwrap()
        .expect("the windows of the first two points are complete");
    assert!(!handed.is_last);
    assert_eq!((handed.range().start(), handed.range().len()), (0, 2));

    // The last sample lies past every remaining window, so points 2 to 4 are decided to hold no
    // value. The closing chunk reports them rather than trimming them away.
    let last = chunk(&[(45, vec![Some(3.0)])], 1, 0, true);
    let closed = evaluator.push(&last).unwrap().expect("a series closes");
    assert!(closed.is_last);
    assert_eq!((closed.range().start(), closed.range().len()), (2, 3));
    assert!(all_missing(&closed.series));
}

/// A chunk that decides only missing positions still has to be handed out before the series ends.
///
/// Without it a consumer cannot tell that those positions are settled: it would keep their
/// windows open and wait for input that has already been shown to contribute nothing. The values
/// are all missing, so only the span itself carries the progress.
#[test]
fn a_chunk_of_decided_missing_positions_is_handed_out_before_the_series_ends() {
    // `rate` over a one-second window on grid 0,1,2,3,4s.
    let window = RangeWindow {
        range_ms: 1_000,
        offset_ms: 0,
        anchor_ms: None,
    };
    let mut evaluator = RangeEvaluator::try_new(
        &raw_schema(1),
        grid(0, 1_000, 5),
        window,
        RangeFunction::Rate,
    )
    .unwrap();

    // A: samples at -0.5s and 0s decide the point at 0s.
    let a = chunk(
        &[(-500, vec![Some(1.0)]), (0, vec![Some(2.0)])],
        1,
        0,
        false,
    );
    let first = evaluator.push(&a).unwrap().expect("point 0 is decided");
    assert!(!first.is_last);
    assert_eq!((first.range().start(), first.range().len()), (0, 1));

    // B: the sample at 2s decides that 1s has no window content and 2s has a single sample, so
    // neither yields a rate. Neither point can change once a sample at 4s arrives: that sample
    // lies past both windows.
    let b = chunk(&[(2_000, vec![Some(3.0)])], 1, 0, false);
    let second = evaluator
        .push(&b)
        .unwrap()
        .expect("points 1 and 2 are decided, missing or not");
    assert!(!second.is_last, "the series has not ended");
    assert_eq!((second.range().start(), second.range().len()), (1, 2));
    assert!(all_missing(&second.series));

    // C closes the series and reports the rest.
    let c = chunk(&[(4_000, vec![Some(4.0)])], 1, 0, true);
    let last = evaluator.push(&c).unwrap().expect("a series closes");
    assert!(last.is_last);
    assert_eq!((last.range().start(), last.range().len()), (3, 2));

    // Folding the three chunks reproduces the whole grid, so the missing spans cost nothing.
    let chunks = [first, second, last];
    let folded = fold_chunks(&chunks, evaluator.output_schema()).expect("the series completed");
    assert_eq!(folded.metric, output_metric(RangeFunction::Rate, 0));
}

/// The closing chunk of a series is labelled like the chunks before it, including the metadata
/// labels a function drops: `rate` removes `__name__`, and a consumer that keyed series by label
/// set would see the same series split in two if the closing chunk kept it.
#[test]
fn the_closing_chunk_is_labelled_like_the_chunks_before_it() {
    let window = RangeWindow {
        range_ms: 20,
        offset_ms: 0,
        anchor_ms: None,
    };
    let mut evaluator =
        RangeEvaluator::try_new(&raw_schema(1), grid(0, 10, 6), window, RangeFunction::Rate)
            .unwrap();
    let first = chunk(&[(10, vec![Some(1.0)]), (20, vec![Some(3.0)])], 1, 0, false);
    let handed = evaluator
        .push(&first)
        .unwrap()
        .expect("the windows of the first point are complete");
    assert!(!handed.is_last);
    assert_eq!(handed.series.metric.name(), None);

    // The last sample is past every remaining window, so the closing chunk carries decided
    // missing positions and must still be labelled the same.
    let last = chunk(&[(45, vec![Some(5.0)])], 1, 0, true);
    let closed = evaluator.push(&last).unwrap().expect("a series closes");
    assert!(closed.is_last);
    assert!(!closed.range().is_empty());
    assert!(all_missing(&closed.series));
    assert_eq!(closed.series.metric, handed.series.metric);
    assert_eq!(closed.series.metric.name(), None);
}

#[test]
fn rejects_invalid_parameters() {
    let zero_range = RangeWindow {
        range_ms: 0,
        offset_ms: 0,
        anchor_ms: None,
    };
    assert!(matches!(
        RangeEvaluator::try_new(
            &raw_schema(1),
            grid(0, 10, 4),
            zero_range,
            RangeFunction::Rate
        ),
        Err(DataFusionError::Plan(_))
    ));

    let far_offset = RangeWindow {
        range_ms: 10,
        offset_ms: i64::MAX - 20,
        anchor_ms: None,
    };
    assert!(matches!(
        RangeEvaluator::try_new(
            &raw_schema(1),
            grid(0, 10, 4),
            far_offset,
            RangeFunction::PredictLinear(0.0)
        ),
        Err(DataFusionError::Plan(_))
    ));

    let unrepresentable_anchor = RangeWindow {
        range_ms: 10,
        offset_ms: 1,
        anchor_ms: Some(i64::MIN),
    };
    assert!(matches!(
        RangeEvaluator::try_new(
            &raw_schema(1),
            grid(0, 10, 4),
            unrepresentable_anchor,
            RangeFunction::Rate
        ),
        Err(DataFusionError::Plan(_))
    ));
}

fn execution_error<T: std::fmt::Debug>(result: Result<T, DataFusionError>) {
    assert!(
        matches!(result, Err(DataFusionError::Execution(_))),
        "{result:?}"
    );
}

#[test]
fn rejects_invalid_chunk_sequences() {
    let window = RangeWindow {
        range_ms: 10,
        offset_ms: 0,
        anchor_ms: None,
    };
    let new = || probe_evaluator(1, grid(0, 10, 4), window);
    let row = |timestamp| (timestamp, vec![Some(1.0)]);

    let mut unlabeled = chunk(&[row(1)], 1, 0, true);
    unlabeled.metric = None;
    execution_error(new().push(&unlabeled));

    let mut evaluator = new();
    evaluator.push(&chunk(&[row(1)], 1, 0, false)).unwrap();
    execution_error(evaluator.push(&chunk(&[row(2)], 1, 1, true)));

    let mut evaluator = new();
    evaluator.push(&chunk(&[row(1)], 1, 0, false)).unwrap();
    let mut relabeled = chunk(&[row(2)], 1, 0, true);
    relabeled.metric = Some(metric(1));
    execution_error(evaluator.push(&relabeled));

    let mut evaluator = new();
    evaluator.push(&chunk(&[row(5)], 1, 0, false)).unwrap();
    execution_error(evaluator.push(&chunk(&[row(5)], 1, 0, true)));

    let mut evaluator = new();
    let mut extra_field = chunk(&[row(1)], 1, 0, true);
    extra_field.fields = chunk(&[(1, vec![Some(1.0), Some(2.0)])], 2, 0, true).fields;
    execution_error(evaluator.push(&extra_field));

    let mut evaluator = new();
    evaluator.push(&chunk(&[row(1)], 1, 0, false)).unwrap();
    execution_error(evaluator.finish());
}

#[test]
fn offset_overflow_fails_unless_the_sample_is_stale() {
    let window = RangeWindow {
        range_ms: 10,
        offset_ms: 10,
        anchor_ms: None,
    };
    let late = i64::MAX - 5;
    let mut evaluator = probe_evaluator(1, grid(0, 10, 4), window);
    assert!(matches!(
        evaluator.push(&chunk(&[(late, vec![Some(1.0)])], 1, 0, true)),
        Err(DataFusionError::Execution(_))
    ));

    let mut evaluator = probe_evaluator(1, grid(0, 10, 4), window);
    let rows = [(1, vec![Some(1.0)]), (late, vec![Some(stale())])];
    let series = evaluator.push(&chunk(&rows, 1, 0, true)).unwrap();
    assert!(series.is_some());
}

#[test]
fn histogram_samples_need_a_function_and_drop_stale_markers() {
    let empty = BucketSideView {
        span_offsets: &[],
        span_lengths: &[],
        buckets: &[],
    };
    let mut builder = HistogramBatchBuilder::new();
    for sum in [1.0, stale()] {
        builder
            .push(HistogramView {
                schema: 0,
                zero_threshold: 0.0,
                sum,
                count: 1.0,
                zero_count: 1.0,
                reset_hint: CounterResetHint::Unknown,
                start_timestamp: None,
                custom_values: &[],
                positive: empty,
                negative: empty,
            })
            .unwrap();
    }
    let histograms =
        Samples::try_from_histogram(UInt32Array::from(vec![0, 1]), builder.finish().unwrap())
            .unwrap();
    let mut piece = chunk(&[(1, vec![Some(1.0)]), (2, vec![Some(1.0)])], 1, 0, true);
    piece.fields = SeriesFields::one(histograms);

    let window = RangeWindow {
        range_ms: 10,
        offset_ms: 0,
        anchor_ms: None,
    };
    assert!(matches!(
        probe_evaluator(1, grid(0, 10, 4), window).push(&piece),
        Err(DataFusionError::NotImplemented(_))
    ));
    // A histogram whose sum is the stale marker ends the series like a stale float.
    let mut evaluator = RangeEvaluator::try_new(
        &raw_schema(1),
        grid(2, 10, 1),
        window,
        RangeFunction::CountOverTime,
    )
    .unwrap();
    let series = evaluator.push(&piece).unwrap().unwrap().series;
    assert!(matches!(
        series.fields.as_slice()[0].sample(0),
        SampleRef::Float(count) if count == 1.0
    ));
}

#[test]
fn retains_only_samples_later_windows_need() {
    let window = RangeWindow {
        range_ms: 50,
        offset_ms: 0,
        anchor_ms: None,
    };
    let mut evaluator = probe_evaluator(1, grid(0, 10, 1_000), window);
    let rows: Vec<Row> = (0..10_000).map(|i| (i, vec![Some(1.0)])).collect();
    for (index, piece) in rows.chunks(100).enumerate() {
        evaluator
            .push(&chunk(piece, 1, 0, (index + 1) * 100 == rows.len()))
            .unwrap();
        // At most one window plus one grid step of samples survives each chunk.
        assert!(evaluator.fields[0].timestamps.len() <= 60);
    }
}

/// A counter whose every window contains resets, so the index holds entries the way a real
/// counter does, still releases them as the samples they reach back to are dropped.
#[test]
fn reset_index_is_released_with_the_samples_it_reaches_back_to() {
    let window = RangeWindow {
        range_ms: 50,
        offset_ms: 0,
        anchor_ms: None,
    };
    let mut evaluator = probe_evaluator(1, grid(0, 10, 1_000), window);
    // A sawtooth that resets every 10 samples.
    let rows: Vec<Row> = (0..10_000)
        .map(|i| (i, vec![Some((i % 10) as f64)]))
        .collect();
    for (index, piece) in rows.chunks(100).enumerate() {
        evaluator
            .push(&chunk(piece, 1, 0, (index + 1) * 100 == rows.len()))
            .unwrap();
        // Resets are released with the samples behind the next window, so they stay bounded by
        // the same window plus one grid step the samples do, one reset per 10 samples.
        assert!(
            evaluator.fields[0].resets.len() <= 6,
            "{} resets after {} chunks: the index kept history no window can reach",
            evaluator.fields[0].resets.len(),
            index + 1
        );
    }
}

/// The reset index, not a rescan of the window, decides which samples a window corrects. The
/// samples are laid out so each rule is visible point by point: a reset is the first sample of a
/// later chunk, a reset's predecessor sits exactly on a window's left edge and must not count,
/// and a reset enters a window whole. The reported resets are compared instead of the function's
/// output so a wrong position cannot hide inside a float's last bits.
#[test]
fn counter_resets_follow_the_window_and_survive_chunking() {
    // A counter that drops at index 3 (20 -> 5) and again at index 6 (25 -> 7), one sample per
    // 10 s.
    let counter = [0.0, 10.0, 20.0, 5.0, 15.0, 25.0, 7.0, 17.0, 27.0, 37.0];
    // A field that only rises, so its windows never correct anything.
    let rising = [0.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0];
    let rows: Vec<Row> = (0..10)
        .map(|index| {
            (
                index as i64 * 10_000,
                vec![Some(counter[index]), Some(rising[index])],
            )
        })
        .collect();
    let grid = grid(30_000, 10_000, 4);
    let window = RangeWindow {
        range_ms: 20_000,
        offset_ms: 0,
        anchor_ms: None,
    };
    // Every chunk starts on a sample an earlier window corrected, so the index has to reach
    // across the chunk boundary.
    let pieces = [(0, 3), (3, 6), (6, 10)];

    let recorded = Arc::new(Mutex::new(Vec::new()));
    let mut evaluator = {
        let sink = recorded.clone();
        let record = move |window: FloatWindow<'_>| {
            sink.lock().unwrap().push((
                window.timestamps.to_vec(),
                window
                    .counter_resets
                    .iter()
                    .map(|reset| reset.previous_value)
                    .collect::<Vec<_>>(),
            ));
            Some(window.values[0])
        };
        let mut evaluator = RangeEvaluator::with_kernel(
            &raw_schema(2),
            grid.clone(),
            window,
            Box::new(record),
            true,
            true,
        )
        .unwrap();
        for (start, end) in pieces {
            let last = end == rows.len();
            if let Some(handed) = evaluator
                .push(&chunk(&rows[start..end], 2, 0, last))
                .unwrap()
            {
                assert_eq!(
                    handed.is_last, last,
                    "only the last chunk closes the series"
                );
            }
        }
        evaluator
    };

    // Windows (10 s, 30 s], (20 s, 40 s], (30 s, 50 s] and (40 s, 60 s].
    // The first holds 20 and 5, so the reset at index 3 corrects it. The second's left edge is
    // the sample before that reset, which a left-open window excludes, so the same reset must not
    // count there. The fourth holds 25 and 7, corrected by the reset at index 6. The second field
    // never corrects anything.
    let expected = [
        (vec![20_000, 30_000], vec![20.0], vec![]),
        (vec![30_000, 40_000], vec![], vec![]),
        (vec![40_000, 50_000], vec![], vec![]),
        (vec![50_000, 60_000], vec![25.0], vec![]),
    ];
    let first = recorded.lock().unwrap().clone();
    assert_eq!(first.len(), 8, "four grid points, two fields");
    for (index, (pair, (timestamps, counter_resets, rising_resets))) in
        first.chunks(2).zip(expected.iter().cloned()).enumerate()
    {
        assert_eq!(
            pair[0],
            (timestamps.clone(), counter_resets),
            "field 0 at grid point {index}"
        );
        assert_eq!(
            pair[1],
            (timestamps, rising_resets),
            "field 1 at grid point {index}"
        );
    }

    // The same samples pushed as a second series through the same evaluator: no buffer, cursor or
    // index state of the first series may survive, so the second series reports exactly the same
    // windows and resets.
    recorded.lock().unwrap().clear();
    for (start, end) in pieces {
        let closed = end == rows.len();
        if let Some(chunk) = evaluator
            .push(&chunk(&rows[start..end], 2, 1, closed))
            .unwrap()
        {
            assert_eq!(
                chunk.is_last, closed,
                "only the last chunk closes the series"
            );
        }
    }
    assert_eq!(*recorded.lock().unwrap(), recorded_of(&expected));
    evaluator.finish().unwrap();
}

/// The per-call recordings `counter_resets_follow_the_window_and_survive_chunking` expects, in
/// kernel-call order: field 0 then field 1 for each grid point.
fn recorded_of(expected: &[(Vec<i64>, Vec<f64>, Vec<f64>)]) -> Vec<(Vec<i64>, Vec<f64>)> {
    expected
        .iter()
        .flat_map(|(timestamps, counter, rising)| {
            [
                (timestamps.clone(), counter.clone()),
                (timestamps.clone(), rising.clone()),
            ]
        })
        .collect()
}

/// Missing samples shift which neighbours a reset sits between, and the shift differs per
/// field. The resets each window reports are checked position by position instead of through a
/// function's output, so a field paying for another field's resets shows up directly.
#[test]
fn nulls_change_the_reset_positions_of_each_field_independently() {
    let rows: Vec<Row> = vec![
        (0, vec![Some(0.0), Some(0.0)]),
        (10_000, vec![Some(20.0), Some(20.0)]),
        (20_000, vec![None, Some(15.0)]),
        (30_000, vec![Some(5.0), Some(5.0)]),
        (40_000, vec![Some(5.0), Some(5.0)]),
    ];
    let grid = grid(0, 10_000, 5);
    // Every window is wider than the series, so each grid point sees every sample up to it.
    let window = RangeWindow {
        range_ms: 100_000,
        offset_ms: 0,
        anchor_ms: None,
    };

    // One entry per kernel call, in field order within each grid point.
    let reported = Arc::new(Mutex::new(Vec::new()));
    {
        let sink = reported.clone();
        let record = move |window: FloatWindow<'_>| {
            sink.lock().unwrap().push(
                window
                    .counter_resets
                    .iter()
                    .map(|reset| reset.previous_value)
                    .collect::<Vec<_>>(),
            );
            Some(window.values[0])
        };
        let mut evaluator =
            RangeEvaluator::with_kernel(&raw_schema(2), grid, window, Box::new(record), true, true)
                .unwrap();
        for (start, end) in [(0, 2), (2, 5)] {
            evaluator
                .push(&chunk(&rows[start..end], 2, 0, end == rows.len()))
                .unwrap();
        }
        evaluator.finish().unwrap();
    }

    // Field 0 never sees the missing 15, so 20 is followed directly by 5 and it resets once.
    // Field 1 keeps the 15 and resets twice: 20 -> 15 and 15 -> 5. Both fields report their own
    // resets even though they share a window and a chunk.
    let reported = reported.lock().unwrap().clone();
    assert_eq!(reported.len(), 10, "five grid points, two fields");
    let expected = [
        (vec![], vec![]),
        (vec![], vec![]),
        (vec![], vec![20.0]),
        (vec![20.0], vec![20.0, 15.0]),
        (vec![20.0], vec![20.0, 15.0]),
    ];
    for (index, (pair, (field0, field1))) in reported.chunks(2).zip(expected).enumerate() {
        assert_eq!(pair[0], field0, "field 0 at grid point {index}");
        assert_eq!(pair[1], field1, "field 1 at grid point {index}");
    }
}

/// A random evaluated series on `grid`; values may carry the stale bit pattern, which computed
/// input keeps as an ordinary NaN.
fn random_evaluated(
    rng: &mut StdRng,
    grid: &Arc<EvalGrid>,
    fields: usize,
    host: u64,
) -> (Option<Series>, Vec<Row>) {
    let mut dense = random_dense(rng, grid.timestamps().len(), fields);
    for value in dense.iter_mut().flatten().flatten() {
        if rng.random_bool(0.05) {
            *value = stale();
        }
    }
    let schema = Arc::new(SeriesSchema {
        fields: raw_schema(fields).fields,
        eval_grid: Some(grid.clone()),
    });
    let mut builder = GridSeriesBuilder::try_new(schema).unwrap();
    for (index, row) in dense.iter().enumerate() {
        builder.push(index, row).unwrap();
    }
    let rows = grid
        .timestamps()
        .iter()
        .zip(&dense)
        .filter(|(_, row)| row.iter().any(Option::is_some))
        .map(|(timestamp, row)| (*timestamp, row.clone()))
        .collect();
    (builder.finish(metric(host)).unwrap(), rows)
}

#[test]
fn subquery_series_match_references() {
    for seed in 0..300 {
        let mut rng = StdRng::seed_from_u64(seed);
        let fields = rng.random_range(1..=3);
        let (outer, window) = random_window(&mut rng);
        let step = rng.random_range(1..4_000);
        let Some(inner) = subquery_grid(outer.spec(), window, step).unwrap() else {
            continue;
        };
        let inner = Arc::new(EvalGrid::try_new(inner).unwrap());
        let schema = SeriesSchema {
            fields: raw_schema(fields).fields,
            eval_grid: Some(inner.clone()),
        };
        let (Some(series), rows) = random_evaluated(&mut rng, &inner, fields, seed) else {
            continue;
        };
        let mut probe = RangeEvaluator::with_kernel(
            &schema,
            outer.clone(),
            window,
            Box::new(probe),
            true,
            true,
        )
        .unwrap();
        let expected = reference(&rows, &outer, window, fields, false, true);
        let whole = EvaluatedChunk::try_new(series, true).unwrap();
        let evaluated = probe
            .push_evaluated(&whole)
            .unwrap()
            .expect("the input closes");
        assert!(evaluated.is_last);
        let result = fold_chunks(std::slice::from_ref(&evaluated), probe.output_schema());
        assert_series_eq(result.as_ref(), &expected, &metric(seed));
        for function in udf_functions() {
            let mut evaluator =
                RangeEvaluator::try_new(&schema, outer.clone(), window, function).unwrap();
            let expected = udf_reference(function, &rows, &outer, window, fields, false);
            let evaluated = evaluator
                .push_evaluated(&whole)
                .unwrap()
                .expect("the input closes");
            let result = fold_chunks(std::slice::from_ref(&evaluated), evaluator.output_schema());
            assert_series_eq(result.as_ref(), &expected, &output_metric(function, seed));
        }
    }
}

#[test]
fn subquery_grid_covers_every_step_multiple_in_the_windows() {
    for seed in 0..2_000 {
        let mut rng = StdRng::seed_from_u64(seed);
        let (outer, window) = random_window(&mut rng);
        let spec = outer.spec();
        let step = rng.random_range(1..4_000);
        let fold = fold_offset(&outer, window);
        let last_eval = match window.anchor_ms {
            Some(_) => spec.start_ms,
            None => *outer.timestamps().last().unwrap(),
        };
        let lower = spec.start_ms - fold - window.range_ms;
        let expected: Vec<i64> = ((lower.div_euclid(step) - 1) * step..=last_eval - fold)
            .step_by(step as usize)
            .filter(|point| *point > lower && point.rem_euclid(step) == 0)
            .collect();
        let actual = subquery_grid(spec, window, step)
            .unwrap()
            .map(|grid| EvalGrid::try_new(grid).unwrap().timestamps().to_vec())
            .unwrap_or_default();
        assert_eq!(actual, expected, "seed {seed}");
    }
}

#[test]
fn rejects_input_of_the_other_kind() {
    let window = RangeWindow {
        range_ms: 10,
        offset_ms: 0,
        anchor_ms: None,
    };
    let evaluated = SeriesSchema {
        fields: raw_schema(1).fields,
        eval_grid: Some(grid(0, 10, 4)),
    };
    let mut evaluator =
        RangeEvaluator::try_new(&evaluated, grid(0, 10, 4), window, RangeFunction::Rate).unwrap();
    execution_error(evaluator.push(&chunk(&[(1, vec![Some(1.0)])], 1, 0, true)));

    let mut builder = GridSeriesBuilder::try_new(Arc::new(evaluated)).unwrap();
    builder.push(0, &[Some(1.0)]).unwrap();
    let series = builder.finish(metric(0)).unwrap().unwrap();
    let chunk = EvaluatedChunk::try_new(series, true).unwrap();
    execution_error(probe_evaluator(1, grid(0, 10, 4), window).push_evaluated(&chunk));
}

/// The range function a golden fixture names, with its constant arguments.
fn range_function(name: &str, params: &[f64]) -> RangeFunction {
    match name {
        "rate" => RangeFunction::Rate,
        "increase" => RangeFunction::Increase,
        "delta" => RangeFunction::Delta,
        "irate" => RangeFunction::Irate,
        "idelta" => RangeFunction::Idelta,
        "avg_over_time" => RangeFunction::AvgOverTime,
        "min_over_time" => RangeFunction::MinOverTime,
        "max_over_time" => RangeFunction::MaxOverTime,
        "sum_over_time" => RangeFunction::SumOverTime,
        "count_over_time" => RangeFunction::CountOverTime,
        "last_over_time" => RangeFunction::LastOverTime,
        "present_over_time" => RangeFunction::PresentOverTime,
        "stddev_over_time" => RangeFunction::StddevOverTime,
        "stdvar_over_time" => RangeFunction::StdvarOverTime,
        "quantile_over_time" => RangeFunction::QuantileOverTime(params[0]),
        "resets" => RangeFunction::Resets,
        "changes" => RangeFunction::Changes,
        "deriv" => RangeFunction::Deriv,
        "predict_linear" => RangeFunction::PredictLinear(params[0]),
        "double_exponential_smoothing" => RangeFunction::DoubleExponentialSmoothing {
            smoothing: params[0],
            trend: params[1],
        },
        "first_over_time" => RangeFunction::FirstOverTime,
        "mad_over_time" => RangeFunction::MadOverTime,
        "ts_of_first_over_time" => RangeFunction::TsOfFirstOverTime,
        "ts_of_last_over_time" => RangeFunction::TsOfLastOverTime,
        "ts_of_min_over_time" => RangeFunction::TsOfMinOverTime,
        "ts_of_max_over_time" => RangeFunction::TsOfMaxOverTime,
        other => panic!("unknown function {other}"),
    }
}

/// Range functions evaluated by the Prometheus v3.15.0 engine over random single series; see
/// the header of `prometheus_cases.txt`.
#[test]
fn range_functions_match_prometheus() {
    let parse_points = |points: &str| -> Vec<(i64, f64)> {
        points
            .split_whitespace()
            .map(|point| {
                let (timestamp, bits) = point.split_once(':').unwrap();
                (
                    timestamp.parse().unwrap(),
                    f64::from_bits(u64::from_str_radix(bits, 16).unwrap()),
                )
            })
            .collect()
    };
    let mut checked = 0;
    let fixtures = [
        include_str!("prometheus_cases.txt"),
        include_str!("experimental_cases.txt"),
    ];
    for line in fixtures.iter().flat_map(|fixture| fixture.lines()) {
        if line.starts_with('#') {
            continue;
        }
        let mut parts = line.split(';');
        let header: Vec<_> = parts.next().unwrap().split_whitespace().collect();
        let samples = parse_points(parts.next().unwrap());
        let expected_points = parse_points(parts.next().unwrap());
        let params: Vec<f64> = match header[1] {
            "-" => Vec::new(),
            params => params.split(',').map(|p| p.parse().unwrap()).collect(),
        };
        let function = range_function(header[0], &params);
        let numbers: Vec<i64> = header[2..].iter().map(|n| n.parse().unwrap()).collect();
        let anchor_ms = numbers.get(5).copied();
        let [range_ms, offset_ms, start_ms, step_ms, points] = numbers[..5] else {
            panic!("bad header {line}");
        };
        let grid = grid(start_ms, step_ms, points as u64);
        let window = RangeWindow {
            range_ms,
            offset_ms,
            anchor_ms,
        };
        let mut expected = vec![vec![None]; points as usize];
        for (timestamp, value) in &expected_points {
            expected[((timestamp - start_ms) / step_ms) as usize][0] = Some(*value);
        }
        let rows: Vec<Row> = samples
            .iter()
            .map(|(timestamp, value)| (*timestamp, vec![Some(*value)]))
            .collect();
        let mut evaluator =
            RangeEvaluator::try_new(&raw_schema(1), grid, window, function).unwrap();
        let split = rows.len() / 2;
        let pieces = if split > 0 {
            vec![
                chunk(&rows[..split], 1, 0, false),
                chunk(&rows[split..], 1, 0, true),
            ]
        } else {
            vec![chunk(&rows, 1, 0, true)]
        };
        let chunks = collect_chunks(&mut evaluator, &pieces);
        let result = fold_chunks(&chunks, evaluator.output_schema());
        assert_series_eq_any_nan(
            result.as_ref(),
            &expected,
            &output_metric(function, 0),
            line,
        );
        checked += expected_points.len();
    }
    assert!(checked > 2_000, "{checked} points checked");
}

#[test]
fn rejects_double_exponential_smoothing_factors_outside_the_unit_interval() {
    let window = RangeWindow {
        range_ms: 10,
        offset_ms: 0,
        anchor_ms: None,
    };
    for (smoothing, trend) in [(0.0, 0.5), (1.0, 0.5), (0.5, 0.0), (0.5, 1.5)] {
        let evaluator = || {
            RangeEvaluator::try_new(
                &raw_schema(1),
                grid(0, 10, 4),
                window,
                RangeFunction::DoubleExponentialSmoothing { smoothing, trend },
            )
            .unwrap()
        };
        // Like Prometheus, only a window holding a sample checks the factors. The sample lies past
        // every window, so the result is missing everywhere rather than an error.
        let outside = chunk(&[(100, vec![Some(1.0)])], 1, 0, true);
        let closed = evaluator()
            .push(&outside)
            .unwrap()
            .expect("a series closes");
        assert!(all_missing(&closed.series));
        evaluator().finish().unwrap();
        let inside = chunk(&[(5, vec![Some(1.0)])], 1, 0, true);
        assert!(matches!(
            evaluator().push(&inside),
            Err(DataFusionError::Execution(_))
        ));
    }
}

/// Range functions drop the metric name and the other metadata labels, except
/// `last_over_time` and `first_over_time`, as Prometheus does.
#[test]
fn range_functions_drop_metadata_labels() {
    let labels = |names: &[&str]| -> Vec<Label> {
        names
            .iter()
            .map(|name| Label {
                name: Arc::from(*name),
                value: Arc::from("v"),
            })
            .collect()
    };
    let input = MetricName::try_new(
        Some(Arc::from("m")),
        labels(&["__type__", "__unit__", "host"]),
    )
    .unwrap();
    let window = RangeWindow {
        range_ms: 10_000,
        offset_ms: 0,
        anchor_ms: None,
    };
    let rows: Vec<Row> = vec![(1_000, vec![Some(1.0)]), (2_000, vec![Some(2.0)])];
    let nameless = MetricName::try_new(None, labels(&["host"])).unwrap();
    for (function, expected) in [
        (RangeFunction::Rate, nameless.clone()),
        (RangeFunction::MadOverTime, nameless.clone()),
        (RangeFunction::TsOfMinOverTime, nameless.clone()),
        (RangeFunction::TsOfMaxOverTime, nameless.clone()),
        (RangeFunction::TsOfFirstOverTime, nameless.clone()),
        (RangeFunction::TsOfLastOverTime, nameless),
        (RangeFunction::LastOverTime, input.clone()),
        (RangeFunction::FirstOverTime, input.clone()),
    ] {
        let mut chunk = chunk(&rows, 1, 0, true);
        chunk.metric = Some(input.clone());
        let mut evaluator =
            RangeEvaluator::try_new(&raw_schema(1), grid(2_000, 1_000, 1), window, function)
                .unwrap();
        let series = evaluator.push(&chunk).unwrap().unwrap().series;
        assert_eq!(series.metric, expected, "{function:?}");
    }
}

/// A leading NaN is replaced by the next sample in `ts_of_min/max_over_time`, so the timestamp
/// moves past it even though no comparison with NaN holds.
#[test]
fn ts_of_extremum_moves_past_a_leading_nan() {
    let window = RangeWindow {
        range_ms: 10_000,
        offset_ms: 0,
        anchor_ms: None,
    };
    let rows: Vec<Row> = [(1_000, f64::NAN), (2_000, 5.0), (3_000, 7.0)]
        .map(|(ts, value)| (ts, vec![Some(value)]))
        .to_vec();
    for (function, expected) in [
        (RangeFunction::TsOfMinOverTime, 2.0),
        (RangeFunction::TsOfMaxOverTime, 3.0),
    ] {
        let mut evaluator =
            RangeEvaluator::try_new(&raw_schema(1), grid(3_000, 1_000, 1), window, function)
                .unwrap();
        let series = evaluator
            .push(&chunk(&rows, 1, 0, true))
            .unwrap()
            .unwrap()
            .series;
        assert_series_eq(
            Some(&series),
            &vec![vec![Some(expected)]],
            &output_metric(function, 0),
        );
    }
}

/// Samples behind the next window are dropped while waiting for it, so a long gap between grid
/// points keeps at most one chunk.
#[test]
fn waiting_for_a_point_drops_samples_behind_its_window() {
    let window = RangeWindow {
        range_ms: 10,
        offset_ms: 0,
        anchor_ms: None,
    };
    let mut evaluator = probe_evaluator(1, grid(0, 100_000, 2), window);
    for chunk_index in 0..900 {
        let rows: Vec<Row> = (0..100)
            .map(|row| (1 + chunk_index * 100 + row, vec![Some(1.0)]))
            .collect();
        // The second point stays undecided until its window can be read, so a chunk here may
        // report progress on the first point but never on the second.
        if let Some(handed) = evaluator.push(&chunk(&rows, 1, 0, false)).unwrap() {
            assert!(!handed.is_last);
            assert!(handed.range().start() + handed.range().len() <= 1);
        }
        assert!(evaluator.fields[0].timestamps.len() <= 100);
    }
    let last = chunk(&[(100_000, vec![Some(1.0)])], 1, 0, true);
    let closed = evaluator.push(&last).unwrap().expect("a series closes");
    assert!(closed.is_last);
}

/// Prometheus 3.15 reports `ts_of_last_over_time` as the max of the last float timestamp and
/// a histogram timestamp that defaults to 0, so a window of negative timestamps reports 0;
/// `ts_of_first_over_time` takes the min and keeps them.
#[test]
fn ts_of_last_over_time_reports_zero_for_negative_timestamps() {
    let window = RangeWindow {
        range_ms: 2_000,
        offset_ms: 0,
        anchor_ms: None,
    };
    let rows: Vec<Row> = vec![(-1_000, vec![Some(7.0)])];
    for (function, expected) in [
        (RangeFunction::TsOfLastOverTime, 0.0),
        (RangeFunction::TsOfFirstOverTime, -1.0),
    ] {
        let mut evaluator =
            RangeEvaluator::try_new(&raw_schema(1), grid(0, 1_000, 1), window, function).unwrap();
        let series = evaluator
            .push(&chunk(&rows, 1, 0, true))
            .unwrap()
            .unwrap()
            .series;
        assert_series_eq(
            Some(&series),
            &vec![vec![Some(expected)]],
            &output_metric(function, 0),
        );
    }
}

/// `quantile_over_time` sorts the window as Go's `sort.Sort` does, so the zero a rank lands on,
/// and the sign of the result, match Prometheus over windows longer than its insertion sort.
#[test]
fn quantile_picks_the_signed_zero_go_picks() {
    let window = RangeWindow {
        range_ms: 20_000,
        offset_ms: 0,
        anchor_ms: None,
    };
    let (z, n) = (0.0, -0.0);
    let values = [
        z,
        n,
        n,
        z,
        1.0,
        n,
        z,
        z,
        n,
        f64::NAN,
        n,
        z,
        -1.0,
        z,
        n,
        n,
        z,
        2.0,
        n,
        z,
    ];
    let rows: Vec<Row> = values
        .iter()
        .enumerate()
        .map(|(i, &value)| (i as i64 * 1_000, vec![Some(value)]))
        .collect();
    // Go's `sort.Sort` on amd64 gives a negative quantile for q = k / 19 only at these k.
    let go_negative = |k: i32| matches!(k, 1 | 8 | 14);
    for k in 1..=19 {
        let mut evaluator = RangeEvaluator::try_new(
            &raw_schema(1),
            grid(19_000, 1_000, 1),
            window,
            RangeFunction::QuantileOverTime(f64::from(k) / 19.0),
        )
        .unwrap();
        let series = evaluator
            .push(&chunk(&rows, 1, 0, true))
            .unwrap()
            .unwrap()
            .series;
        let SampleRef::Float(value) = series.fields.as_slice()[0].sample(0) else {
            panic!("expected a float");
        };
        assert_eq!(value.is_sign_negative(), go_negative(k), "k = {k}: {value}");
    }
}

/// Chunks after the last grid point are only validated, so nothing accumulates and the outcome
/// does not depend on where the series is split.
#[test]
fn samples_after_the_last_window_are_validated_but_not_retained() {
    let window = RangeWindow {
        range_ms: 10,
        offset_ms: 0,
        anchor_ms: None,
    };
    let mut evaluator = probe_evaluator(1, grid(0, 10, 2), window);
    for chunk_index in 0..900 {
        let rows: Vec<Row> = (0..100)
            .map(|row| (1 + chunk_index * 100 + row, vec![Some(1.0)]))
            .collect();
        evaluator.push(&chunk(&rows, 1, 0, false)).unwrap();
        assert!(evaluator.fields[0].timestamps.len() <= 100);
    }

    let window = RangeWindow {
        range_ms: 10,
        offset_ms: 10,
        anchor_ms: None,
    };
    let late = i64::MAX - 5;
    let mut evaluator = probe_evaluator(1, grid(0, 10, 4), window);
    evaluator
        .push(&chunk(
            &[(15, vec![Some(1.0)]), (20, vec![Some(1.0)])],
            1,
            0,
            false,
        ))
        .unwrap();
    assert!(matches!(
        evaluator.push(&chunk(&[(late, vec![Some(1.0)])], 1, 0, true)),
        Err(DataFusionError::Execution(_))
    ));
}

/// An anchored `predict_linear` regresses the one anchored window around each grid point, as
/// Prometheus does with `enh.Ts`; over `y = t` that predicts `T` whatever the offset.
#[test]
fn anchored_predict_linear_regresses_around_each_point() {
    let rows: Vec<Row> = (0..=60)
        .step_by(5)
        .map(|ts| (ts, vec![Some(ts as f64)]))
        .collect();
    for offset_ms in [0, 10] {
        let window = RangeWindow {
            range_ms: 30,
            offset_ms,
            anchor_ms: Some(50),
        };
        let mut evaluator = RangeEvaluator::try_new(
            &raw_schema(1),
            grid(100, 10, 3),
            window,
            RangeFunction::PredictLinear(0.0),
        )
        .unwrap();
        let series = evaluator
            .push(&chunk(&rows, 1, 0, true))
            .unwrap()
            .unwrap()
            .series;
        let values: Vec<_> = (0..3)
            .map(|index| match series.fields.as_slice()[0].sample(index) {
                SampleRef::Float(value) => value,
                _ => panic!("expected a float"),
            })
            .collect();
        for (value, expected) in values.into_iter().zip([100.0, 110.0, 120.0]) {
            assert!(
                (value - expected).abs() < 1e-9,
                "offset {offset_ms}: {value}"
            );
        }
    }
}

#[test]
fn rejects_a_subquery_grid_longer_than_u64() {
    let outer = GridSpec::try_new(i64::MIN, i64::MAX, 3).unwrap();
    let window = RangeWindow {
        range_ms: 7,
        offset_ms: -6,
        anchor_ms: None,
    };
    assert!(matches!(
        subquery_grid(outer, window, 1),
        Err(DataFusionError::Plan(_))
    ));
}

/// An anchored window that ends before the series starts skips past the one point it reads;
/// later chunks must not pile up either.
#[test]
fn anchored_window_before_the_series_retains_nothing() {
    let window = RangeWindow {
        range_ms: 10,
        offset_ms: 0,
        anchor_ms: Some(5),
    };
    let mut evaluator = probe_evaluator(1, grid(100, 10, 3), window);
    for chunk_index in 0..100 {
        let rows: Vec<Row> = (0..100)
            .map(|row| (50 + chunk_index * 100 + row, vec![Some(1.0)]))
            .collect();
        evaluator.push(&chunk(&rows, 1, 0, false)).unwrap();
        assert!(evaluator.fields[0].timestamps.len() <= 100);
    }
}

/// `absent_over_time(v[r])` is `absent` over the series of `present_over_time(v[r])`, with the
/// labels derived from the selector's matchers. Checked against 120 cases from the Prometheus
/// v3.15.0 engine (`absent_over_time_cases.txt`, promql-compat `cmd/narthex-golden-vector
/// -absent-over-time`); samples sit on grid points.
#[test]
fn absent_over_time_is_absent_over_present_over_time() {
    let text = include_str!("absent_over_time_cases.txt");
    let mut checked = 0;
    for case in text.split("end\n").filter(|case| case.contains("case ")) {
        let lines: Vec<&str> = case.lines().filter(|line| !line.starts_with('#')).collect();
        let spec: Vec<&str> = lines[0].split_whitespace().collect();
        let range_ms: i64 = spec[2].parse().unwrap();
        let matchers: Vec<LabelMatcher> = spec[3]
            .split(',')
            .filter(|matcher| *matcher != "-")
            .map(|matcher| {
                let (name, op, value) = match matcher.split_once("!=") {
                    Some((name, value)) => (name, MatchOp::NotEqual, value),
                    None => {
                        let (name, value) = matcher.split_once('=').unwrap();
                        (name, MatchOp::Equal, value)
                    }
                };
                let value = value.trim_matches('"');
                LabelMatcher {
                    name: Arc::from(name),
                    op,
                    value: Arc::from(value),
                    matches_empty: (op == MatchOp::Equal) == value.is_empty(),
                }
            })
            .collect();
        let grid_spec: Vec<i64> = lines[1]
            .split_whitespace()
            .skip(1)
            .map(|field| field.parse().unwrap())
            .collect();
        let grid = grid(grid_spec[0], grid_spec[1], grid_spec[2] as u64);
        let window = RangeWindow {
            range_ms,
            offset_ms: 0,
            anchor_ms: None,
        };
        let mut present = Vec::new();
        let mut expected = None;
        for (host, line) in lines[2..].iter().enumerate() {
            let (kind, rest) = line.split_once(' ').unwrap();
            let (labels, points) = rest.split_once(" ;").unwrap();
            let points: Vec<(i64, f64)> = points
                .split_whitespace()
                .map(|point| {
                    let (index, bits) = point.split_once(':').unwrap();
                    let ts = grid_spec[0] + index.parse::<i64>().unwrap() * grid_spec[1];
                    (ts, f64::from_bits(u64::from_str_radix(bits, 16).unwrap()))
                })
                .collect();
            if kind == "result" {
                let mut dense = vec![vec![None]; grid_spec[2] as usize];
                for (ts, value) in points {
                    dense[((ts - grid_spec[0]) / grid_spec[1]) as usize][0] = Some(value);
                }
                let labels = labels
                    .split(',')
                    .filter(|pair| *pair != "-")
                    .map(|pair| {
                        let (name, value) = pair.split_once('=').unwrap();
                        (name.to_string(), value.to_string())
                    })
                    .collect();
                expected = Some((dense, metric_of(&labels)));
                continue;
            }
            let selected = matchers.iter().all(|matcher| {
                let value = labels
                    .split(',')
                    .find_map(|pair| pair.strip_prefix(&format!("{}=", matcher.name)))
                    .unwrap_or("");
                (value == &*matcher.value) == (matcher.op == MatchOp::Equal)
            });
            if !selected || points.is_empty() {
                continue;
            }
            let rows: Vec<Row> = points
                .into_iter()
                .map(|(ts, value)| (ts, vec![Some(value)]))
                .collect();
            let mut evaluator = RangeEvaluator::try_new(
                &raw_schema(1),
                grid.clone(),
                window,
                RangeFunction::PresentOverTime,
            )
            .unwrap();
            let closed = evaluator
                .push(&chunk(&rows, 1, host as u64, true))
                .unwrap()
                .expect("the series closes");
            assert!(closed.is_last);
            present.push(closed.series);
        }
        let schema = SeriesSchema {
            fields: raw_schema(1).fields,
            eval_grid: Some(grid.clone()),
        };
        let actual = absent(&schema, &present, absent_labels(&matchers).unwrap()).unwrap();
        let (expected, labels) = expected.unwrap_or_else(|| {
            let none = MetricName::try_new(None, Vec::new()).unwrap();
            (vec![vec![None]; grid_spec[2] as usize], none)
        });
        assert_series_eq(actual.as_ref(), &expected, &labels);
        checked += 1;
    }
    assert_eq!(checked, 120);
}

/// One series of a golden case as raw chunks of float and histogram samples: a single chunk, or
/// with `split` one chunk per sample.
fn mixed_chunks(case: &GoldenCase, index: usize, split: bool) -> Vec<RawSeriesChunk> {
    let (labels, floats) = &case.inputs[index];
    let texts = &case.histograms[index];
    let points: Vec<usize> = floats
        .keys()
        .chain(texts.keys())
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let groups: Vec<&[usize]> = if split {
        points.chunks(1).collect()
    } else {
        vec![&points[..]]
    };
    let spec = case.grid.spec();
    let count = groups.len();
    groups
        .into_iter()
        .filter(|group| !group.is_empty())
        .enumerate()
        .map(|(position, group)| {
            let timestamps: Vec<i64> = group
                .iter()
                .map(|&point| spec.start_ms + point as i64 * spec.step_ms)
                .collect();
            let values: Float64Array = group
                .iter()
                .map(|point| floats.get(point).map(|bits| f64::from_bits(*bits)))
                .collect();
            let mut builder = HistogramBatchBuilder::new();
            let indices: UInt32Array = group
                .iter()
                .map(|point| {
                    let text = texts.get(point)?;
                    Some(builder.push(OwnedHistogram::parse(text).view()).unwrap())
                })
                .collect();
            let histograms = HistogramSamples::try_new(indices, builder.finish().unwrap()).unwrap();
            RawSeriesChunk::try_new(
                &raw_schema(1),
                Buffer::from((index as u64).to_le_bytes().to_vec()),
                Some(metric_of(labels)),
                RawTimestamps::try_new(ScalarBuffer::from(timestamps)).unwrap(),
                SeriesFields::try_from_vec(vec![
                    Samples::try_from_mixed(values, histograms).unwrap(),
                ])
                .unwrap(),
                position + 1 == count,
            )
            .unwrap()
        })
        .collect()
}

/// Range functions over series mixing floats and native histograms, evaluated by the Prometheus
/// v3.15.0 engine (`histogram_cases.txt`, promql-compat `cmd/narthex-golden-native -range`).
/// Values compare within the promql-compat tolerance; histograms bucket by bucket.
#[test]
fn histogram_range_functions_match_prometheus() {
    let (mut floats_checked, mut histograms_checked) = (0, 0);
    for case in golden_cases(include_str!("histogram_cases.txt")) {
        let name = case.spec[0].as_str();
        let function = range_function(name, &[0.5, 0.5]);
        let function = match function {
            RangeFunction::PredictLinear(_) => RangeFunction::PredictLinear(10.0),
            other => other,
        };
        let epsilon = match function {
            RangeFunction::Deriv
            | RangeFunction::PredictLinear(_)
            | RangeFunction::StddevOverTime
            | RangeFunction::StdvarOverTime => 1e-6,
            _ => 1e-10,
        };
        let window = RangeWindow {
            range_ms: case.spec[1].parse().unwrap(),
            offset_ms: case.spec[2].parse().unwrap(),
            anchor_ms: None,
        };
        // One chunk per sample also checks that histograms survive across chunks.
        for split in [false, true] {
            let mut actual = BTreeMap::new();
            for index in 0..case.inputs.len() {
                let mut evaluator =
                    RangeEvaluator::try_new(&raw_schema(1), case.grid.clone(), window, function)
                        .unwrap();
                let chunks = collect_chunks(&mut evaluator, &mixed_chunks(&case, index, split));
                if let Some(series) = fold_chunks(&chunks, evaluator.output_schema()) {
                    actual.insert(labels_of(&series.metric), series);
                }
            }
            let expected = case.outcome.as_ref().unwrap();
            let context = format!("{:?} split {split}", case.spec);
            assert_eq!(
                actual.keys().collect::<Vec<_>>(),
                expected.keys().collect::<Vec<_>>(),
                "{context}"
            );
            for (labels, series) in &actual {
                let TimeAxis::Grid(range) = &series.time else {
                    panic!("expected a grid series");
                };
                let floats = &expected[labels];
                let no_histograms = BTreeMap::new();
                let texts = case.result_histograms.get(labels).unwrap_or(&no_histograms);
                let mut points = 0;
                for (row, sample) in series.fields.as_slice()[0].iter().enumerate() {
                    let point = range.start() + row;
                    match sample {
                        SampleRef::Missing => continue,
                        SampleRef::Float(value) => {
                            let want = f64::from_bits(floats[&point]);
                            assert!(
                                close(value, want, epsilon),
                                "{context} at {point}: {value} vs {want}"
                            );
                            floats_checked += 1;
                        }
                        SampleRef::Histogram(view) => {
                            let got = FloatHistogram::try_from_view(&view).unwrap();
                            let want = FloatHistogram::try_from_view(
                                &OwnedHistogram::parse(&texts[&point]).view(),
                            )
                            .unwrap();
                            assert_histogram_close(
                                &got,
                                &want,
                                epsilon,
                                &format!("{context} at {point}"),
                            );
                            histograms_checked += 1;
                        }
                    }
                    points += 1;
                }
                assert_eq!(points, floats.len() + texts.len(), "{context}");
            }
        }
    }
    assert!(
        floats_checked > 500 && histograms_checked > 150,
        "{floats_checked} floats, {histograms_checked} histograms"
    );
}

/// Prometheus clears explicit counter reset hints of subquery results, which describe the raw
/// neighbours of a sample: the same `CounterReset` histogram repeated at two evaluated points
/// is no reset.
#[test]
fn evaluated_histograms_drop_counter_reset_hints() {
    let inner = grid(0, 10, 2);
    let schema = Arc::new(SeriesSchema {
        fields: raw_schema(1).fields,
        eval_grid: Some(inner),
    });
    let reset = "0/0/0/4014000000000000/4014000000000000/1/0_1/4014000000000000///";
    let histograms = BTreeMap::from([(0, reset.to_string()), (1, reset.to_string())]);
    let labels = BTreeMap::from([("job".to_string(), "x".to_string())]);
    let series = mixed_series(&schema, &labels, &BTreeMap::new(), &histograms).unwrap();
    let window = RangeWindow {
        range_ms: 30,
        offset_ms: 0,
        anchor_ms: None,
    };
    let mut evaluator =
        RangeEvaluator::try_new(&schema, grid(10, 10, 1), window, RangeFunction::Resets).unwrap();
    let chunk = EvaluatedChunk::try_new(series, true).unwrap();
    let result = evaluator.push_evaluated(&chunk).unwrap().unwrap().series;
    assert!(matches!(
        result.fields.as_slice()[0].sample(0),
        SampleRef::Float(resets) if resets == 0.0
    ));
}

/// A window ending at its evaluation point, `range_ms` long: what `last_over_time` reads,
/// `(E - range_ms, E]`.
fn last_window(range_ms: i64) -> RangeWindow {
    RangeWindow {
        range_ms,
        offset_ms: 0,
        anchor_ms: None,
    }
}

/// The raw chunks of the consumer fixtures: one sample per inner grid point, parted so that the
/// inner layer hands the series out in three chunks.
fn consumer_chunks() -> Vec<RawSeriesChunk> {
    let rows: [Row; 5] = [
        (0, vec![Some(4.0)]),
        (10, vec![Some(5.0)]),
        (20, vec![Some(6.0)]),
        (30, vec![Some(7.0)]),
        (40, vec![Some(8.0)]),
    ];
    vec![
        chunk(&rows[..1], 1, 0, false),
        chunk(&rows[1..3], 1, 0, false),
        chunk(&rows[3..], 1, 0, true),
    ]
}

/// The inner layer of the consumer fixtures: `last_over_time` over the 0/10 grid, so every grid
/// point reads the sample at its own timestamp.
fn inner_layer() -> RangeEvaluator {
    RangeEvaluator::try_new(
        &raw_schema(1),
        grid(0, 10, 5),
        last_window(10),
        RangeFunction::LastOverTime,
    )
    .unwrap()
}

/// A consumer of evaluated series, windowing them onto `grid`.
fn evaluated_layer(
    input: &Arc<SeriesSchema>,
    grid: Arc<EvalGrid>,
    range_ms: i64,
) -> RangeEvaluator {
    RangeEvaluator::try_new(
        input,
        grid,
        last_window(range_ms),
        RangeFunction::LastOverTime,
    )
    .unwrap()
}

/// The grid a consumer of the consumer fixtures is asked for: 5/20, a different start and step
/// from the 0/10 grid the series it reads is indexed on.
fn outer_grid() -> Arc<EvalGrid> {
    grid(5, 20, 3)
}

/// A consumer of an evaluated series on the [`outer_grid`].
fn outer_layer(input: &Arc<SeriesSchema>) -> RangeEvaluator {
    evaluated_layer(input, outer_grid(), 10)
}

/// One evaluated chunk of `schema`, beginning at grid position `start`.
fn evaluated_chunk(
    schema: &SeriesSchema,
    start: usize,
    values: Vec<Option<f64>>,
    is_last: bool,
) -> EvaluatedChunk {
    let grid = schema
        .eval_grid
        .clone()
        .expect("an evaluated chunk is addressed on a grid");
    let range = GridRange::try_new(grid, start, values.len()).unwrap();
    let fields =
        SeriesFields::try_from_vec(vec![Samples::from_float(Float64Array::from(values))]).unwrap();
    let series = Series::try_new(schema, metric(0), TimeAxis::Grid(range), fields).unwrap();
    EvaluatedChunk::try_new(series, is_last).unwrap()
}

/// Pushes `chunks` into a fresh consumer on the 5/20 grid and returns what it handed out together
/// with the series those chunks fold into.
fn consume(
    input: &Arc<SeriesSchema>,
    chunks: &[EvaluatedChunk],
) -> (Vec<EvaluatedChunk>, Option<Series>) {
    let mut consumer = outer_layer(input);
    let schema = consumer.output_schema().clone();
    let handed_out: Vec<_> = chunks
        .iter()
        .flat_map(|chunk| consumer.push_evaluated(chunk).unwrap())
        .collect();
    consumer.finish().expect("the series closed");
    let series = fold_chunks(&handed_out, &schema);
    (handed_out, series)
}

/// The chunks of one evaluated series pushed one at a time and the whole series pushed as one
/// chunk reach the same consumer result, on a grid the series is not indexed on.
///
/// This is chunking invariance, not an independent oracle: both sides run the same outer layer,
/// so it is the properties the other tests of this module name that pin the protocol itself.
#[test]
fn a_series_pushed_in_chunks_matches_the_whole_series() {
    let mut inner = inner_layer();
    let schema = inner.output_schema().clone();
    let inner_chunks = collect_chunks(&mut inner, &consumer_chunks());
    inner.finish().expect("the inner series closed");
    assert!(
        inner_chunks.len() > 1,
        "the fixture hands the inner series out as one chunk"
    );

    let (handed_out, chunked) = consume(&schema, &inner_chunks);

    // The whole inner series as one chunk that closes it, the way a producer that never chunked
    // would hand it over.
    let whole = fold_chunks(&inner_chunks, &schema).expect("the fixture inner series holds values");
    let whole = EvaluatedChunk::try_new(whole, true).unwrap();
    let (_, at_once) = consume(&schema, std::slice::from_ref(&whole));

    assert!(
        handed_out.len() > 1,
        "the chunked path handed out one chunk, exactly like the whole-series path"
    );
    assert_series_eq(
        chunked.as_ref(),
        &expand(at_once.as_ref().expect("both paths produce a value")),
        &metric(0),
    );
}

/// A chunk leaving the outer layer is addressed on the outer grid, and the starts of those chunks
/// strictly increase: no row number of the inner grid leaks into the outer addressing.
#[test]
fn a_chunk_indexed_on_the_inner_grid_is_absolute_on_the_outer_one() {
    let mut inner = inner_layer();
    let schema = inner.output_schema().clone();
    let inner_grid = schema.eval_grid.clone().unwrap();
    let inner_chunks = collect_chunks(&mut inner, &consumer_chunks());
    inner.finish().expect("the inner series closed");

    assert_ne!(
        inner_grid.spec(),
        outer_grid().spec(),
        "the fixture evaluates both layers on one grid"
    );
    let mut consumer = outer_layer(&schema);
    let mut starts = Vec::new();
    for chunk in &inner_chunks {
        let Some(handed_out) = consumer.push_evaluated(chunk).unwrap() else {
            continue;
        };
        assert_eq!(
            handed_out.range().grid().spec(),
            outer_grid().spec(),
            "a chunk is addressed on a grid other than the one its layer was asked for"
        );
        starts.push(handed_out.range().start());
    }
    consumer.finish().expect("the outer series closed");

    assert!(
        starts.len() > 1,
        "one chunk makes the strict monotonicity below free"
    );
    assert!(
        starts.windows(2).all(|pair| pair[0] < pair[1]),
        "the outer chunk starts {starts:?} do not strictly increase"
    );
}

/// A point is decided by the chunk after which no sample can reach its window, so it leaves with
/// that chunk: the outer point at 5 ms is settled by the inner chunk covering position 0, and it
/// must not wait for the chunk that follows it.
///
/// The grids differ on purpose -- inner 0/10, outer 5/20 -- so the first outer point reads the
/// first inner sample and no other, and the next sample that can arrive sits at inner 10 ms, five
/// steps past the end of its window.
#[test]
fn a_point_no_later_sample_can_reach_leaves_with_the_chunk_that_settles_it() {
    let raw = consumer_chunks();
    let mut inner = inner_layer();
    let schema = inner.output_schema().clone();
    let mut consumer = outer_layer(&schema);

    // The sample at 0 ms is the whole of inner position 0, so this push hands the consumer the
    // inner chunk covering that position alone.
    let first = inner
        .push(&raw[0])
        .unwrap()
        .expect("the sample at 0 ms settles the inner point at 0 ms");
    assert_eq!(
        (first.range().start(), first.range().len()),
        (0, 1),
        "the inner fixture hands out a chunk other than the one covering position 0 alone"
    );
    assert!(!first.is_last);

    let settled = consumer
        .push_evaluated(&first)
        .unwrap()
        .expect("nothing after inner position 0 can reach the window of the outer point at 5 ms");
    assert_eq!(
        (settled.range().start(), settled.range().len()),
        (0, 1),
        "the outer point the first inner chunk settles waited for the next one"
    );
    assert_eq!(
        expand(&settled.series),
        vec![vec![Some(4.0)], vec![None], vec![None]],
        "the outer point at 5 ms does not report the value of the inner sample at 0 ms alone"
    );

    // The rest of the series still streams through both layers, and the whole series is the same
    // as the one handed out one chunk at a time.
    let mut handed_out = vec![settled];
    for piece in &raw[1..] {
        let inner_chunk = inner.push(piece).unwrap();
        if let Some(inner_chunk) = inner_chunk {
            handed_out.extend(consumer.push_evaluated(&inner_chunk).unwrap());
        }
    }
    inner.finish().expect("the inner series closed");
    let outer_schema = consumer.output_schema().clone();
    consumer.finish().expect("the outer series closed");

    let series = fold_chunks(&handed_out, &outer_schema);
    assert_series_eq(
        series.as_ref(),
        &vec![vec![Some(4.0)], vec![Some(6.0)], vec![Some(8.0)]],
        &metric(0),
    );
}

/// A non-final chunk that reaches the end of the input grid decides everything that is left: no
/// sample can follow it, so the outer points behind it are handed out at once instead of waiting
/// for the chunk that closes the series.
#[test]
fn a_non_final_chunk_ending_the_input_grid_decides_the_rest() {
    let inner_grid = grid(0, 10, 5);
    let schema = Arc::new(SeriesSchema {
        fields: raw_schema(1).fields,
        eval_grid: Some(inner_grid),
    });
    // One chunk carrying the whole input grid, without closing the series.
    let whole = evaluated_chunk(
        &schema,
        0,
        vec![Some(4.0), Some(5.0), Some(6.0), Some(7.0), Some(8.0)],
        false,
    );
    let mut consumer = outer_layer(&schema);
    let decided = consumer
        .push_evaluated(&whole)
        .unwrap()
        .expect("the last input position leaves no outer point waiting for a sample");
    assert!(
        !decided.is_last,
        "the chunk closes a series its input does not close"
    );
    assert_eq!(
        (decided.range().start(), decided.range().len()),
        (0, 3),
        "the outer points behind the last inner position waited for a closing chunk"
    );
    assert_eq!(
        expand(&decided.series),
        vec![vec![Some(4.0)], vec![Some(6.0)], vec![Some(8.0)]],
        "the outer points do not read the inner samples their windows hold"
    );

    // Only the empty chunk that closes the series is left, and it decides nothing new.
    let closing = evaluated_chunk(&schema, 5, vec![], true);
    let closing = consumer
        .push_evaluated(&closing)
        .unwrap()
        .expect("a series closes");
    assert!(closing.is_last);
    assert!(
        closing.range().is_empty(),
        "the closing chunk decided {} position(s) the chunk before it already decided",
        closing.range().len()
    );
    consumer.finish().expect("the outer series closed");
}

/// A map chain that yields no value still carries progress: the chunk keeps the range the inner
/// layer decided, and its consumer advances to its end rather than waiting for a value.
#[test]
fn a_map_chain_yielding_no_value_keeps_progress_and_the_closing_chunk() {
    // `clamp` with `max < min` is the one kernel that yields no series in Prometheus, so it is how
    // this layer drops every value of a chunk. Histogram and mixed samples are not mapped away
    // here: `map_math` rejects them with `NotImplemented`, which is a boundary above this layer.
    let ops = [MathOp::Clamp { min: 1.0, max: 0.0 }];
    let mut inner = inner_layer();
    let schema = inner.output_schema().clone();
    let inner_chunks = collect_chunks(&mut inner, &consumer_chunks());
    inner.finish().expect("the inner series closed");
    let mapped: Vec<_> = inner_chunks
        .iter()
        .map(|chunk| map_chunk(&schema, chunk, &ops).unwrap())
        .collect();

    // The chain drops the values, not the positions: a consumer needs the positions to advance.
    for (input, mapped) in inner_chunks.iter().zip(&mapped) {
        assert!(all_missing(&mapped.series), "the chain left a value behind");
        assert_eq!(
            (mapped.range().start(), mapped.range().len()),
            (input.range().start(), input.range().len()),
            "the chain shrank the range the inner layer decided"
        );
        assert_eq!(
            mapped.is_last, input.is_last,
            "the chain dropped the closing chunk"
        );
    }

    // A consumer on the inner grid: it reports the settled positions as decided, and only a series
    // that advanced can take the next chunk, which begins where the previous one ended.
    let mut consumer = evaluated_layer(&schema, grid(0, 10, 5), 10);
    let mut handed_out = Vec::new();
    for chunk in &mapped {
        handed_out.extend(consumer.push_evaluated(chunk).unwrap());
    }
    consumer.finish().expect("the series closed");

    let pushed: Vec<_> = mapped
        .iter()
        .map(|chunk| (chunk.range().start(), chunk.range().len()))
        .collect();
    let decided: Vec<_> = handed_out
        .iter()
        .map(|chunk| (chunk.range().start(), chunk.range().len()))
        .collect();
    assert!(
        handed_out.len() > 1,
        "the closing chunk is the only one that came back"
    );
    assert_eq!(
        decided, pushed,
        "the consumer did not report the positions the mapped chunks decided"
    );
    assert!(
        handed_out.iter().all(|chunk| all_missing(&chunk.series)),
        "a consumer of a chunk without values reported a value"
    );
    assert!(
        handed_out.last().expect("a series closes").is_last,
        "the closing chunk is lost"
    );
}

/// The chunks of one series are handed over once, in order: a chunk starting behind the end of the
/// previous one is rejected, and the error names both positions.
#[test]
fn an_out_of_order_chunk_is_rejected() {
    let mut inner = inner_layer();
    let schema = inner.output_schema().clone();
    let inner_chunks = collect_chunks(&mut inner, &consumer_chunks());
    inner.finish().expect("the inner series closed");

    let mut consumer = evaluated_layer(&schema, grid(0, 10, 5), 10);
    // The first two chunks cover 0..1 and 1..3, so the series has reached position 3.
    for chunk in &inner_chunks[..2] {
        consumer.push_evaluated(chunk).unwrap();
    }
    let overlapping = evaluated_chunk(&schema, 2, vec![Some(1.0)], false);
    let error = consumer.push_evaluated(&overlapping).unwrap_err();
    assert!(
        matches!(error, DataFusionError::Execution(_)),
        "not an execution error: {error}"
    );
    let message = error.to_string();
    assert!(
        message.contains("2 does not continue from 3"),
        "the message does not name both positions: {message}"
    );
}

/// A producer that trims its output starts at its first row holding a value. The consumer accepts
/// that series and reports the positions before it as decided, so the undelivered prefix is not
/// left waiting for input.
#[test]
fn a_series_may_begin_past_the_first_position_of_its_grid() {
    let grid = grid(0, 10, 4);
    let schema = Arc::new(SeriesSchema {
        fields: raw_schema(1).fields,
        eval_grid: Some(grid.clone()),
    });
    let window = last_window(10);
    // The same evaluated series, once with the leading position trimmed and once with it carried
    // as an explicit missing value.
    let trimmed = evaluated_chunk(&schema, 1, vec![Some(2.0), Some(3.0), Some(4.0)], true);
    assert_eq!(
        trimmed.range().start(),
        1,
        "the fixture series does not begin past position 0"
    );
    let explicit = evaluated_chunk(
        &schema,
        0,
        vec![None, Some(2.0), Some(3.0), Some(4.0)],
        true,
    );

    let mut from_trimmed =
        RangeEvaluator::try_new(&schema, grid.clone(), window, RangeFunction::LastOverTime)
            .unwrap();
    let accepted = from_trimmed
        .push_evaluated(&trimmed)
        .expect("a first chunk may begin past the start of its grid")
        .expect("the last chunk closes the series");
    from_trimmed.finish().expect("the series closed");

    let mut from_explicit =
        RangeEvaluator::try_new(&schema, grid.clone(), window, RangeFunction::LastOverTime)
            .unwrap();
    let carried = from_explicit
        .push_evaluated(&explicit)
        .expect("the input closes the series")
        .expect("the last chunk closes the series");
    from_explicit.finish().expect("the series closed");

    // Dense rows rather than `assert_series_eq`, which also demands the range be trimmed to the
    // rows holding a value: keeping the undelivered prefix in the range is the point here.
    assert_eq!(
        expand(&accepted.series),
        expand(&carried.series),
        "the trimmed series evaluates differently from the one carrying the missing value"
    );
    assert_eq!(
        (accepted.range().start(), accepted.range().len()),
        (0, 4),
        "the undelivered prefix is not reported as a decided position"
    );
}

/// Composed layers: an inner evaluator hands its chunks to a map chain and on to an outer
/// evaluator, so the chunk protocol is exercised end to end instead of on one side alone.
mod composition {
    use super::*;

    /// The kernels of the composed map chain: `abs` rewrites every value, so the layer between the
    /// two windows is one the outer layer cannot do without.
    const OPS: [MathOp; 1] = [MathOp::Abs];

    /// A composed `Window -> Map -> Window` pipeline over one raw series.
    ///
    /// The inner layer windows raw samples onto the 0/10 grid, the map chain rewrites what it
    /// hands out, and the outer layer windows those chunks onto the 5/20 grid: a different start
    /// and step, so the outer positions (5, 25, 45) are no inner timestamps (0, 10, 20, 30, 40).
    struct Pipeline {
        inner: RangeEvaluator,
        outer: RangeEvaluator,
        /// The outer grid and window, kept so the reference can build its own outer layer.
        grid: Arc<EvalGrid>,
        window: RangeWindow,
        /// Every chunk each layer handed out, in the order it was handed out.
        inner_chunks: Vec<EvaluatedChunk>,
        mapped_chunks: Vec<EvaluatedChunk>,
        outer_chunks: Vec<EvaluatedChunk>,
    }

    /// The raw chunks of the composed fixtures: a sample at the first inner point, a push whose
    /// samples fall into the 5 ms gaps the inner windows leave between grid points, a sample at
    /// the third point, and a closing chunk whose sample lies past the fourth.
    fn composed_raw() -> Vec<RawSeriesChunk> {
        let rows: [Row; 5] = [
            (0, vec![Some(1.0)]),
            (3, vec![Some(2.0)]),
            (12, vec![Some(3.0)]),
            (20, vec![Some(4.0)]),
            (40, vec![Some(5.0)]),
        ];
        vec![
            chunk(&rows[..1], 1, 0, false),
            chunk(&rows[1..3], 1, 0, false),
            chunk(&rows[3..4], 1, 0, false),
            chunk(&rows[4..], 1, 0, true),
        ]
    }

    /// The labels a mapped series carries: the map chain drops the metadata labels, `__name__`
    /// included, and the fixture metric carries no other one.
    fn mapped_metric(host: u64) -> MetricName {
        let mut metric = metric(host);
        metric.remove("__name__");
        metric
    }

    impl Pipeline {
        fn new() -> Self {
            let inner = RangeEvaluator::try_new(
                &raw_schema(1),
                grid(0, 10, 5),
                last_window(5),
                RangeFunction::LastOverTime,
            )
            .unwrap();
            let grid = outer_grid();
            let window = last_window(10);
            let outer = RangeEvaluator::try_new(
                inner.output_schema(),
                grid.clone(),
                window,
                RangeFunction::LastOverTime,
            )
            .unwrap();
            Self {
                inner,
                outer,
                grid,
                window,
                inner_chunks: Vec::new(),
                mapped_chunks: Vec::new(),
                outer_chunks: Vec::new(),
            }
        }

        /// Pushes one raw chunk through all three layers, keeping what each of them handed out.
        fn push(&mut self, raw: &RawSeriesChunk) {
            let inner = self.inner.push(raw).unwrap();
            let mapped = match &inner {
                Some(chunk) => Some(map_chunk(self.inner.output_schema(), chunk, &OPS).unwrap()),
                None => None,
            };
            let outer = match &mapped {
                Some(chunk) => self.outer.push_evaluated(chunk).unwrap(),
                None => None,
            };
            self.inner_chunks.extend(inner);
            self.mapped_chunks.extend(mapped);
            self.outer_chunks.extend(outer);
        }

        /// The whole inner series, folded from the chunks the inner layer handed out.
        fn inner_series(&self) -> Option<Series> {
            fold_chunks(&self.inner_chunks, self.inner.output_schema())
        }

        /// How far the outer layer has reported progress: the end of the last position any of its
        /// chunks decided.
        fn outer_extent(&self) -> usize {
            self.outer_chunks
                .iter()
                .map(|chunk| chunk.range().start() + chunk.range().len())
                .max()
                .unwrap_or(0)
        }

        /// The reference: the whole inner series pushed through the map chain and one outer layer
        /// at once, as if neither the inner layer nor the chain had chunked it.
        fn reference(&self, inner_series: Series) -> Option<Series> {
            let whole = EvaluatedChunk::try_new(inner_series, true).unwrap();
            let mapped = map_chunk(self.inner.output_schema(), &whole, &OPS).unwrap();
            let mut outer = RangeEvaluator::try_new(
                self.inner.output_schema(),
                self.grid.clone(),
                self.window,
                RangeFunction::LastOverTime,
            )
            .unwrap();
            let chunk = outer
                .push_evaluated(&mapped)
                .unwrap()
                .expect("the input closes the series");
            let result = fold_chunks(std::slice::from_ref(&chunk), outer.output_schema());
            outer.finish().expect("the reference series closed");
            result
        }

        /// Checks that both layers saw their series close.
        fn finish(self) {
            self.inner.finish().expect("the inner series closed");
            self.outer.finish().expect("the outer series closed");
        }
    }

    /// An empty map chain is the identity, and it has to be the identity on every chunk of one
    /// series: the valued chunk and the zero-length closing one must carry the same labels, or
    /// the consumer of the series rejects it as two.
    ///
    /// `last_over_time` keeps `__name__` -- it is one of the two range functions that do -- so this
    /// series carries the metadata label a map chain rewrites, and the identity below is not
    /// vacuous. Before `map_chunk` treated a chain without kernels as the identity it dropped the
    /// metadata labels from the zero-length chunk alone, so the chunk that closed the series was
    /// labelled differently from the chunk before it.
    ///
    /// No plan reaches this state: a `Map` is only built around at least one kernel, and
    /// `map_chunk` is the only caller-side contract here. The empty slice is therefore pinned as
    /// what `map_chunk` documents it to be -- the identity -- rather than as a case a planner
    /// produces. Rejecting the empty slice instead would drop this test and nothing else.
    #[test]
    fn an_empty_map_chain_keeps_the_chunk_identity() {
        let window = RangeWindow {
            range_ms: 10,
            offset_ms: 0,
            anchor_ms: None,
        };
        let inner_grid = grid(0, 10, 2);
        let mut inner = RangeEvaluator::try_new(
            &raw_schema(1),
            inner_grid.clone(),
            window,
            RangeFunction::LastOverTime,
        )
        .unwrap();
        let schema = inner.output_schema().clone();

        // The sample at 10 ms settles both points of the two-point grid, and the point at 10 ms
        // reads it, so this push hands out the whole grid with a value in it.
        let first = chunk(&[(10, vec![Some(2.0)])], 1, 0, false);
        let valued = inner
            .push(&first)
            .unwrap()
            .expect("the windows of both points are complete");
        assert!(!valued.is_last);
        assert_eq!(
            (valued.range().start(), valued.range().len()),
            (0, inner_grid.timestamps().len()),
            "the fixture does not decide the whole grid before the last chunk"
        );

        // The last sample lies past every window, so the closing chunk decides nothing new and
        // reaches a consumer with no positions at all.
        let last = chunk(&[(100, vec![Some(3.0)])], 1, 0, true);
        let closing = inner.push(&last).unwrap().expect("a series closes");
        assert!(closing.is_last);
        assert!(
            closing.range().is_empty(),
            "the closing chunk decided a position: {} values",
            closing.range().len()
        );
        inner.finish().expect("the inner series closed");

        // Without a metadata label in the fixture both assertions below would hold for free.
        assert_eq!(valued.series.metric.name(), Some("http_requests"));
        assert_eq!(closing.series.metric.name(), Some("http_requests"));

        // An empty chain maps nothing, so each chunk comes back exactly as it went in.
        let mapped_valued = map_chunk(&schema, &valued, &[]).unwrap();
        assert_eq!(
            mapped_valued.series.metric, valued.series.metric,
            "an empty chain rewrote the valued chunk's labels"
        );
        let mapped_closing = map_chunk(&schema, &closing, &[]).unwrap();
        assert_eq!(
            mapped_closing.series.metric, closing.series.metric,
            "an empty chain rewrote the closing chunk's labels"
        );

        // Both chunks are one series to the consumer: the valued one opens it, the closing one is
        // accepted after it, and the series ends.
        let mut downstream =
            RangeEvaluator::try_new(&schema, inner_grid, window, RangeFunction::LastOverTime)
                .unwrap();
        downstream
            .push_evaluated(&mapped_valued)
            .expect("the valued chunk opens the series");
        let closed = downstream
            .push_evaluated(&mapped_closing)
            .expect("the closing chunk carries the labels of the chunk before it")
            .expect("a series closes");
        assert!(closed.is_last);
        downstream.finish().expect("the series closed");
    }

    /// A chain with a kernel rewrites the labels of every chunk it maps, and it has to rewrite them
    /// the same way on the zero-length closing chunk as on the chunk before it.
    ///
    /// A kernel drops the metadata labels and, like Prometheus' `simpleFloatFunc`, every label whose
    /// value is empty -- an ordinary label such as `empty=""` goes too. The closing chunk carries no
    /// position to map, so a chain that rewrote only its metadata labels would leave `empty=""` on
    /// it while the chunk before it lost the label, and a consumer of the series would reject the
    /// two chunks as different series.
    #[test]
    fn a_map_chain_relabels_the_closing_chunk_like_the_chunk_before_it() {
        let grid = grid(0, 10, 3);
        let schema = Arc::new(SeriesSchema {
            fields: vec![FieldSpec {
                name: Arc::from("f0"),
            }]
            .into(),
            eval_grid: Some(grid.clone()),
        });
        // A label with an empty value, which Prometheus treats as absent, next to a label that
        // survives the chain. Without the empty one both assertions below would hold for free.
        let labelled = MetricName::try_new(
            Some(Arc::from("http_requests")),
            vec![
                Label {
                    name: Arc::from("host"),
                    value: Arc::from("h0"),
                },
                Label {
                    name: Arc::from("empty"),
                    value: Arc::from(""),
                },
            ],
        )
        .unwrap();
        let with_metric = |start: usize, values: Vec<Option<f64>>, is_last: bool| {
            let range = GridRange::try_new(grid.clone(), start, values.len()).unwrap();
            let fields =
                SeriesFields::try_from_vec(vec![Samples::from_float(Float64Array::from(values))])
                    .unwrap();
            let series =
                Series::try_new(&schema, labelled.clone(), TimeAxis::Grid(range), fields).unwrap();
            EvaluatedChunk::try_new(series, is_last).unwrap()
        };

        // The valued chunk fills the whole grid, so the closing one decides nothing new and reaches
        // a consumer with no positions at all.
        let valued = with_metric(0, vec![Some(-1.0), Some(-2.0), Some(-3.0)], false);
        let closing = with_metric(3, vec![], true);

        let ops = [MathOp::Abs];
        let mapped_valued = map_chunk(&schema, &valued, &ops).unwrap();
        let mapped_closing = map_chunk(&schema, &closing, &ops).unwrap();

        let names: Vec<_> = mapped_valued
            .series
            .metric
            .tags()
            .iter()
            .map(|label| label.name.to_string())
            .collect();
        assert_eq!(
            names,
            vec!["host".to_string()],
            "the kernel did not drop the name, the metadata labels and the empty-valued label"
        );
        assert_eq!(
            mapped_closing.series.metric, mapped_valued.series.metric,
            "the closing chunk carries labels the chunk before it does not"
        );

        // Both chunks are one series to the consumer.
        let mut downstream = outer_layer(&schema);
        downstream
            .push_evaluated(&mapped_valued)
            .expect("the valued chunk opens the series");
        downstream
            .push_evaluated(&mapped_closing)
            .expect("the closing chunk carries the labels of the chunk before it")
            .expect("a series closes");
        downstream.finish().expect("the series closed");
    }

    /// A prefix reaches the outer layer before the inner series ends: the composed pipeline streams
    /// instead of buffering a whole series behind the map chain.
    #[test]
    fn a_prefix_reaches_the_outer_layer_before_the_inner_series_ends() {
        let raw = composed_raw();
        let (open, closing) = raw.split_at(raw.len() - 1);
        assert!(
            open.iter().all(|chunk| !chunk.is_last),
            "the fixture closes the inner series before its last raw chunk"
        );
        let mut pipeline = Pipeline::new();
        for raw in open {
            pipeline.push(raw);
        }
        // The only chunk that closes the inner series has not been pushed, so what the outer layer
        // has heard so far is its prefix.
        assert!(
            !pipeline.outer_chunks.is_empty(),
            "the outer layer only hears from the series once it has ended"
        );
        for raw in closing {
            pipeline.push(raw);
        }
        let chunked = fold_chunks(&pipeline.outer_chunks, pipeline.outer.output_schema());

        // The reference differs in chunking alone: the same whole inner series, mapped and pushed
        // in one chunk. Every comparison against it in this module is such an invariance check,
        // not an independent oracle.
        let reference = pipeline.reference(
            pipeline
                .inner_series()
                .expect("the inner series holds values"),
        );
        assert_series_eq(
            chunked.as_ref(),
            &expand(reference.as_ref().expect("the reference holds values")),
            &mapped_metric(0),
        );
        pipeline.finish();
    }

    /// A chunk that settles inner positions without a value still advances the outer window: what
    /// crosses both layers is progress, not values.
    #[test]
    fn an_all_null_middle_chunk_still_advances_the_outer_window() {
        // This fixture is its own: the sample of the middle push lands at 25 ms, past both inner
        // windows it crosses, so the inner layer settles the positions at 10 and 20 ms without a
        // value while its own sample belongs to no window at all. The next sample the input can
        // still deliver then sits at 30 ms -- past the window of the outer point at 25 ms, and so
        // past the point this push makes final. `composed_raw` cannot show this: its push lands at
        // 12 ms, which still leaves a sample at 20 ms possible, and the outer window at 25 ms
        // (15..25) could still gain it, so nothing there is decided yet.
        let raw = [
            chunk(&[(0, vec![Some(1.0)])], 1, 0, false),
            chunk(&[(3, vec![Some(2.0)]), (25, vec![Some(3.0)])], 1, 0, false),
            chunk(&[(40, vec![Some(8.0)])], 1, 0, true),
        ];
        let mut pipeline = Pipeline::new();
        pipeline.push(&raw[0]);

        // The outer layer already reported the point at 5 ms: nothing after inner position 0 can
        // reach its window.
        let before = pipeline.outer_extent();
        assert_eq!(
            before, 1,
            "the fixture does not advance the outer layer on its first push"
        );
        pipeline.push(&raw[1]);
        let decided = pipeline
            .inner_chunks
            .last()
            .expect("the push decides a position");
        assert!(
            !decided.range().is_empty(),
            "the push decided no position, so it can advance nothing"
        );
        assert!(
            all_missing(&decided.series),
            "the middle push was expected to decide a position without a value"
        );
        let after = pipeline.outer_extent();
        assert!(
            after > before,
            "the outer layer did not advance over the position without a value ({before} -> {after})"
        );

        for raw in &raw[2..] {
            pipeline.push(raw);
        }
        let chunked = fold_chunks(&pipeline.outer_chunks, pipeline.outer.output_schema());
        let reference = pipeline.reference(
            pipeline
                .inner_series()
                .expect("the inner series holds values"),
        );
        assert_series_eq(
            chunked.as_ref(),
            &expand(reference.as_ref().expect("the reference holds values")),
            &mapped_metric(0),
        );
        pipeline.finish();
    }

    /// Each layer indexes its own grid: a chunk leaving a layer is addressed on that layer's grid,
    /// the starts within a layer increase, and no outer position is an inner timestamp.
    #[test]
    fn each_layer_indexes_its_own_grid() {
        let mut pipeline = Pipeline::new();
        for raw in composed_raw() {
            pipeline.push(&raw);
        }
        let inner_grid = pipeline.inner.output_schema().eval_grid.clone().unwrap();
        assert_ne!(
            inner_grid.spec(),
            outer_grid().spec(),
            "the fixture evaluates both layers on one grid"
        );
        for (layer, chunks, spec) in [
            ("inner", &pipeline.inner_chunks, inner_grid.spec()),
            ("mapped", &pipeline.mapped_chunks, inner_grid.spec()),
            ("outer", &pipeline.outer_chunks, outer_grid().spec()),
        ] {
            assert!(
                chunks.len() > 1,
                "the {layer} layer hands its series out as one chunk"
            );
            for chunk in chunks {
                assert_eq!(
                    chunk.range().grid().spec(),
                    spec,
                    "a chunk of the {layer} layer is addressed on another grid"
                );
            }
            let starts: Vec<_> = chunks.iter().map(|chunk| chunk.range().start()).collect();
            assert!(
                starts.windows(2).all(|pair| pair[0] < pair[1]),
                "the chunk starts {starts:?} of the {layer} layer do not increase"
            );
        }
        for chunk in &pipeline.outer_chunks {
            for &timestamp in &chunk.range().timestamps() {
                assert!(
                    !inner_grid.timestamps().contains(&timestamp),
                    "the outer layer used the inner timestamp {timestamp}"
                );
            }
        }
        pipeline.finish();
    }
}
