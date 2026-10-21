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
use datafusion::arrow::array::builder::NullBufferBuilder;
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
    DenseSamplesRef, FieldSpec, HistogramSamples, SampleRef, Samples, Series, SeriesFields,
    SeriesSchema,
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
    Dense, GoldenCase, Labels, OwnedHistogram, Points, assert_histogram_close, assert_pieces_eq,
    assert_series_eq, assert_series_eq_any_nan, close, expand, golden_cases, labels_of, metric_of,
    mixed_series, random_dense, series_of, sparse_series, special_value,
};
use crate::grid_series::{GridSample, GridSeriesBuilder};
use crate::histogram_eval::float_histogram::FloatHistogram;
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

/// The absolute grid position `series` starts at.
fn grid_start(series: &Series) -> usize {
    series
        .time
        .first_position()
        .unwrap_or_else(|| panic!("expected a grid series, got {:?}", series.time))
}

/// How many rows of `series` hold a value in any field.
fn rows_with_a_value(series: &Series) -> usize {
    if series.time.grid().is_none() {
        panic!("expected a grid series, got {:?}", series.time);
    }
    // The axis names the rows; a value is counted where a field holds one at that row.
    let mut cursors: Vec<_> = series.fields.iter().map(|field| field.cursor()).collect();
    series
        .time
        .positions()
        .filter(|&(_, row)| {
            cursors.iter_mut().any(|field| {
                !matches!(
                    field.at(row).expect("the axis and the fields are aligned"),
                    SampleRef::Missing
                )
            })
        })
        .count()
}

/// Whether every sample of every field is missing.
fn all_missing(series: &Series) -> bool {
    series.fields.iter().all(|field| {
        (0..field.len()).all(|index| matches!(field.sample(index), SampleRef::Missing))
    })
}

/// Pushes every chunk of one series, keeping what the evaluator hands out on the way.
///
/// A push hands out at most one chunk, which carries every run of positions it decided.
fn collect_chunks(
    evaluator: &mut RangeEvaluator,
    pieces: &[RawSeriesChunk],
) -> Vec<EvaluatedChunk> {
    let mut chunks = Vec::new();
    for piece in pieces {
        chunks.extend(evaluator.push(piece).unwrap());
    }
    chunks
}

/// Ingests one complete evaluated series and returns every chunk it produced.
fn evaluate_series(evaluator: &mut RangeEvaluator, series: &Series) -> Vec<EvaluatedChunk> {
    let chunks: Vec<_> = evaluator
        .push_series(series, true)
        .unwrap()
        .into_iter()
        .collect();
    chunks
}

/// Folds the chunks of one series into one dense series, the way a consumer that keeps every value
/// would, and checks the chunk protocol: absolute ranges, never going backwards, all of the same
/// series, and one closing chunk last.
///
/// The result is deliberately dense -- the gaps between chunks become null rows -- because a value
/// comparison wants one series over the whole span. Row counts must never be read off this: doing
/// so would hide exactly the allocation this change removes.
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
    let width = chunks[0].series.fields.len();
    let grid_len = schema.eval_grid.as_ref().unwrap().timestamps().len();
    let mut rows: Vec<Option<Vec<Option<GridSample>>>> = (0..grid_len).map(|_| None).collect();
    let mut closing = false;
    let mut previous_end = 0;
    for chunk in chunks {
        assert!(!closing, "a chunk follows the closing chunk");
        closing = chunk.is_last;
        assert_eq!(chunk.series.metric, metric, "a chunk changed the series");
        // The chunk says which absolute positions it holds: a chunk of several runs must not be
        // read as the envelope from its first to its last one.
        let mut first = None;
        let mut rows_run: Vec<(usize, usize)> = Vec::new();
        for (position, row) in chunk.series.time.positions() {
            first.get_or_insert(position);
            rows_run.push((position, row));
        }
        if let Some(first) = first {
            assert!(
                first >= previous_end,
                "chunk range {first} goes back before the previous end {previous_end}"
            );
            let last = rows_run
                .last()
                .map(|(position, _)| *position)
                .unwrap_or(first);
            previous_end = last + 1;
        }
        // One forward cursor per field, driven by the rows the chunk carries.
        let mut fields: Vec<_> = chunk
            .series
            .fields
            .iter()
            .map(|field| field.cursor())
            .collect();
        for (position, row) in rows_run {
            let values: Vec<Option<GridSample>> = fields
                .iter_mut()
                .map(|field| {
                    match field
                        .at(row)
                        .expect("the chunk rows are aligned with its fields")
                    {
                        SampleRef::Missing => None,
                        SampleRef::Float(value) => Some(GridSample::Float(value)),
                        SampleRef::Histogram(view) => Some(GridSample::Histogram(Box::new(
                            FloatHistogram::try_from_view(&view).unwrap(),
                        ))),
                    }
                })
                .collect();
            // Positions only increase across chunks, so an overlap is an error here.
            if values.iter().any(|value| value.is_some()) {
                assert!(
                    rows[position].is_none(),
                    "two chunks hold a value at position {position}"
                );
                rows[position] = Some(values);
            }
        }
    }
    assert!(closing, "the series never closed");
    assert_eq!(
        width,
        schema.fields.len(),
        "the fold changed the field count"
    );
    dense_series(&rows, schema.clone(), metric)
}

/// Builds one series covering every row of `rows`, with a null where no chunk held a value.
///
/// This is deliberately not the builder: the builder hands out one piece per run of valued
/// positions, and a value comparison wants a single dense series. Row counts must never be read
/// off this -- doing so would hide exactly the allocation this change removes.
fn dense_series(
    rows: &[Option<Vec<Option<GridSample>>>],
    schema: Arc<SeriesSchema>,
    metric: MetricName,
) -> Option<Series> {
    let start = rows.iter().position(Option::is_some)?;
    let end = rows.iter().rposition(Option::is_some)? + 1;
    let rows = &rows[start..end];
    let fields = (0..schema.fields.len())
        .map(|field| {
            let mut values = Vec::with_capacity(rows.len());
            let mut nulls = NullBufferBuilder::new(rows.len());
            let mut histograms: Option<(HistogramBatchBuilder, Vec<Option<u32>>)> = None;
            for row in rows {
                match row.as_ref().and_then(|row| row[field].as_ref()) {
                    Some(GridSample::Float(value)) => {
                        values.push(*value);
                        nulls.append(true);
                        if let Some((_, indices)) = &mut histograms {
                            indices.push(None);
                        }
                    }
                    Some(GridSample::Histogram(h)) => {
                        values.push(0.0);
                        nulls.append(false);
                        let (batch, indices) = histograms
                            .get_or_insert_with(|| (HistogramBatchBuilder::new(), Vec::new()));
                        while indices.len() < values.len() - 1 {
                            indices.push(None);
                        }
                        indices.push(Some(h.push_to(batch).unwrap()));
                    }
                    None => {
                        values.push(0.0);
                        nulls.append(false);
                        if let Some((_, indices)) = &mut histograms {
                            indices.push(None);
                        }
                    }
                }
            }
            let floats = Float64Array::new(values.into(), nulls.finish());
            match histograms {
                None => Samples::from_float(floats),
                Some((batch, indices)) => {
                    let histograms = HistogramSamples::try_new(
                        UInt32Array::from(indices),
                        batch.finish().unwrap(),
                    )
                    .unwrap();
                    Samples::try_from_mixed(floats, histograms).unwrap()
                }
            }
        })
        .collect::<Vec<_>>();
    let grid = schema.eval_grid.clone().expect("the fold is on a grid");
    let time = GridRange::try_new(grid, start, rows.len()).unwrap();
    let fields = SeriesFields::try_from_vec(fields).unwrap();
    Some(Series::try_new(&schema, metric, TimeAxis::Grid(time), fields).unwrap())
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

/// One raw chunk of one field holding a single native histogram sample at `ts`, whose positive side
/// is one span of `buckets` buckets of one count each.
fn histogram_chunk(schema: &SeriesSchema, buckets: i32, ts: i64) -> RawSeriesChunk {
    let offsets = [0];
    let lengths = [buckets];
    let counts = vec![1.0; usize::try_from(buckets).unwrap()];
    let mut builder = HistogramBatchBuilder::new();
    let index = builder
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
        .unwrap();
    let samples = Samples::try_from_histogram(
        UInt32Array::from(vec![Some(index)]),
        builder.finish().unwrap(),
    )
    .unwrap();
    RawSeriesChunk::try_new(
        schema,
        Buffer::from(0u64.to_le_bytes().to_vec()),
        Some(metric(0)),
        RawTimestamps::try_new(ScalarBuffer::from(vec![ts])).unwrap(),
        SeriesFields::try_from_vec(vec![samples]).unwrap(),
        false,
    )
    .unwrap()
}

/// A window retaining a native histogram is charged for the payload it owns, not only the slot.
///
/// The histogram sample below stays ahead of the second grid point's window, so the evaluator keeps
/// it, and the two runs differ in nothing but the number of buckets on the positive side: 4 and
/// 4096. Those buckets are `(index, count)` pairs on the heap, which no `size_of` of the struct
/// shows -- about 64 KiB for the wider one. The property is that this payload dominates the
/// estimate, not that it is counted to the byte: the wider histogram alone has to be reported as at
/// least half the physical width of its buckets, and the narrow one has to stay far below that. A
/// figure counting slots alone would report one number for both.
#[test]
fn a_retained_histogram_is_charged_for_its_buckets() {
    let retained = |buckets: i32| -> usize {
        let schema = raw_schema(1);
        let mut evaluator = RangeEvaluator::try_new(
            &schema,
            grid(0, 10_000, 2),
            // Far longer than the grid: the second point's window still reaches the sample, so the
            // push retains it instead of advancing past it.
            RangeWindow {
                range_ms: 600_000,
                offset_ms: 0,
                anchor_ms: None,
            },
            // A function that counts the histogram, so what the row holds is a float: the two
            // figures then differ in the payload of the retained sample alone, not in what the row
            // boxes beside it.
            RangeFunction::CountOverTime,
        )
        .unwrap();
        evaluator
            .push(&histogram_chunk(&schema, buckets, 0))
            .unwrap();
        evaluator.retained_bytes()
    };
    let (few, many) = (4, 4096);
    let physical = usize::try_from(many).unwrap() * std::mem::size_of::<(i32, f64)>();
    let (narrow, wide) = (retained(few), retained(many));
    assert!(
        wide >= physical / 2,
        "a window retaining one histogram of {many} buckets reports {wide} bytes, not at least half \
         the {physical} bytes its buckets occupy",
    );
    assert!(
        narrow < wide / 2,
        "a window retaining one histogram of {few} buckets reports {narrow} bytes, not far below \
         the {wide} bytes it reports for {many} buckets: the bucket payload is not what the \
         estimate follows",
    );
}

/// The payload a retained histogram is charged for is released with the sample.
///
/// The histogram below is most of what the evaluator holds, and it stays inside the second grid
/// point's window: a push that only reaches the first point retains it. The chunk at 30 s is beyond
/// the window of every later point, so the evaluation that push triggers drops the histogram while
/// the series is still open. The estimate must come back down by its payload: one that charges at
/// admission and never releases it would keep reporting the history of every histogram the series
/// carried. The closing chunk then drains what is left and ends the series, so the estimate falls
/// back to the field and row buffers' capacity, with no series identity and no histogram payload.
#[test]
fn an_evicted_histogram_releases_its_payload() {
    let schema = raw_schema(1);
    let buckets = 4096;
    let payload = usize::try_from(buckets).unwrap() * std::mem::size_of::<(i32, f64)>();
    let mut evaluator = RangeEvaluator::try_new(
        &schema,
        grid(0, 10_000, 3),
        // 20 s of window over 10 s of step: the histogram at 0 is inside the window of the second
        // point but outside the third's, so the second push evicts it.
        RangeWindow {
            range_ms: 20_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::CountOverTime,
    )
    .unwrap();
    evaluator
        .push(&histogram_chunk(&schema, buckets, 0))
        .unwrap();
    let retained = evaluator.retained_bytes();
    assert!(
        retained >= payload,
        "a window retaining one histogram of {buckets} buckets reports {retained} bytes, less than \
         the {payload} bytes of buckets it holds",
    );

    // The float chunk keeps the series open, so only the window drops the histogram.
    evaluator
        .push(&chunk(&[(30_000, vec![Some(1.0)])], 1, 0, false))
        .unwrap();
    let evicted = evaluator.retained_bytes();
    assert!(
        retained.saturating_sub(evicted) >= payload / 2,
        "dropping one histogram of {payload} bytes left the estimate at {evicted} bytes, down from \
         {retained}: the payload it owned is still counted",
    );

    evaluator
        .push(&chunk(&[(40_000, vec![Some(2.0)])], 1, 0, true))
        .unwrap();
    let closed = evaluator.retained_bytes();
    assert!(
        closed < payload / 2,
        "a closed series still reports {closed} bytes, of which the {payload} bytes of bucket \
         payload were released with their samples",
    );
}

/// A fixed-size window over a long series of histograms reports a fixed size.
///
/// Each push adds one histogram and the next point's window drops the one before it, so the
/// payload charged at admission has to come back at eviction. A counter that only grew would make
/// the estimate follow the length of the series instead of the window over it.
#[test]
fn a_fixed_window_over_histograms_keeps_the_estimate_bounded() {
    let peak_retained = |chunks: i64| -> usize {
        let schema = raw_schema(1);
        let mut evaluator = RangeEvaluator::try_new(
            &schema,
            grid(0, 1, 1024),
            // One millisecond of window over a one millisecond step: the next point evicts the
            // sample of this one.
            RangeWindow {
                range_ms: 1,
                offset_ms: 0,
                anchor_ms: None,
            },
            RangeFunction::CountOverTime,
        )
        .unwrap();
        let mut peak = 0;
        for ts in 0..chunks {
            // Left open: the window, not the close, is what releases the sample.
            evaluator.push(&histogram_chunk(&schema, 64, ts)).unwrap();
            peak = peak.max(evaluator.retained_bytes());
        }
        peak
    };
    assert_eq!(
        peak_retained(8),
        peak_retained(64),
        "a fixed-size window over a series of histograms grew with the length of the series"
    );
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
    // grid was decided: every point is a settled missing value, not a point still waiting. A
    // settled point that holds no value is reported by the close itself, not by a row.
    let mut evaluator = probe_evaluator(1, grid(0, 10, 4), window);
    let after_grid = chunk(&[(31, vec![Some(1.0)])], 1, 0, true);
    let closed = evaluator
        .push(&after_grid)
        .unwrap()
        .expect("a series closes");
    assert!(closed.is_last);
    assert_eq!(chunk_run(&closed).start(), 0);
    assert_eq!(
        chunk_run(&closed).len(),
        0,
        "no point of the grid holds a value"
    );
    assert!(all_missing(&closed.series));
    // A series whose only sample is stale does the same: the row is dropped, the points behind it
    // are decided.
    let only_stale = chunk(&[(5, vec![Some(stale())])], 1, 1, true);
    let closed = evaluator
        .push(&only_stale)
        .unwrap()
        .expect("a series closes");
    assert!(closed.is_last);
    assert_eq!(
        (chunk_run(&closed).start(), chunk_run(&closed).len()),
        (0, 0)
    );
    assert!(all_missing(&closed.series));
    evaluator.finish().unwrap();
}

/// A push that completes nothing must not hand out a chunk.
///
/// A position that settles to no value is not a row, so a push that settled only such positions
/// has no run to hand out. Emitting an empty chunk instead would report progress the push did not
/// make, and a consumer that keys on chunks would see a decision that never happened.
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

    // The closing push hands out the one position of the grid that holds a value, and that same
    // chunk is the close: nothing follows it. The two positions without a value cost no row.
    let mut chunks = Vec::new();
    chunks.extend(
        evaluator
            .push(&chunk(&[(100, vec![Some(2.0)])], 1, 0, true))
            .unwrap(),
    );
    assert_eq!(
        chunks.len(),
        1,
        "one valued position, and it closes the series"
    );
    assert_eq!(
        (chunk_run(&chunks[0]).start(), chunk_run(&chunks[0]).len()),
        (0, 1)
    );
    assert!(chunks[0].is_last);
    let folded = fold_chunks(&chunks, evaluator.output_schema()).expect("the series completed");
    assert_eq!(
        rows_with_a_value(&folded),
        1,
        "only the first grid point holds a value"
    );
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
    assert_eq!(
        (chunk_run(&handed).start(), chunk_run(&handed).len()),
        (0, 2)
    );

    // The last sample lies past every remaining window, so points 2 to 4 are decided to hold no
    // value. They cost no row: the close is what reports that they are settled, and it starts
    // where the last handed-out run ended.
    let last = chunk(&[(45, vec![Some(3.0)])], 1, 0, true);
    let closed = evaluator.push(&last).unwrap().expect("a series closes");
    assert!(closed.is_last);
    assert_eq!(
        (chunk_run(&closed).start(), chunk_run(&closed).len()),
        (2, 0)
    );
    assert!(all_missing(&closed.series));
    assert_eq!(rows_with_a_value(&closed.series), 0);
}

/// A stretch of decided positions holding no value costs no row, and does not delay the close.
///
/// The positions are settled as soon as the sampler pushes past them, but settling them allocates
/// nothing: a consumer learns they hold no value from the run that follows them, or from the
/// close when nothing follows.
#[test]
fn decided_missing_positions_cost_no_rows_and_do_not_delay_the_close() {
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
    assert_eq!((chunk_run(&first).start(), chunk_run(&first).len()), (0, 1));

    // B settles 1s and 2s: neither yields a rate, and neither point can change once a sample at
    // 4s arrives, since that sample lies past both windows. Nothing is handed out, because a
    // position holding no value is not a row -- and the series has not ended, so there is no
    // close to report yet.
    let b = chunk(&[(2_000, vec![Some(3.0)])], 1, 0, false);
    assert!(
        evaluator.push(&b).unwrap().is_none(),
        "settled positions holding no value are not rows"
    );

    // C closes the series. 3s and 4s hold no rate either, so the close carries the progress: it
    // starts where the last row ended and holds none.
    let c = chunk(&[(4_000, vec![Some(4.0)])], 1, 0, true);
    let mut chunks = vec![first];
    chunks.extend(evaluator.push(&c).unwrap());
    let last = chunks.last().expect("a series always closes");
    assert!(last.is_last);
    assert_eq!((chunk_run(last).start(), chunk_run(last).len()), (1, 0));

    // Folding the chunks reproduces the one point that holds a value; the empty stretch cost no
    // row at any point along the way.
    let folded = fold_chunks(&chunks, evaluator.output_schema()).expect("the series completed");
    assert_eq!(folded.metric, output_metric(RangeFunction::Rate, 0));
    assert_eq!((grid_start(&folded), rows_with_a_value(&folded)), (0, 1));
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

    // The last sample is past every remaining window, so the close reports the settled positions
    // that hold no value, and it must be labelled the same as the chunk before it.
    let last = chunk(&[(45, vec![Some(5.0)])], 1, 0, true);
    let mut chunks = vec![handed];
    chunks.extend(evaluator.push(&last).unwrap());
    let closed = chunks.last().expect("a series always closes");
    assert!(closed.is_last);
    assert_eq!(
        chunk_run(closed).len(),
        0,
        "the settled positions hold no value"
    );
    assert!(all_missing(&closed.series));
    assert_eq!(closed.series.metric, chunks[0].series.metric);
    assert_eq!(closed.series.metric.name(), None);
}

/// The single run a chunk holds, for the chunks that are known to cover one.
fn chunk_run(chunk: &EvaluatedChunk) -> &GridRange {
    chunk
        .range()
        .expect("this chunk was expected to cover a single run")
}

/// The `(start, len)` of every run of every chunk handed out, in order.
///
/// One chunk may carry several runs, separated by stretches holding no value; each is reported on
/// its own, so a test comparing chunk boundaries always sees the positions that hold a value. A
/// chunk holding no row at all reports the position it closes at, with a length of zero.
fn chunk_ranges(chunks: &[EvaluatedChunk]) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    for chunk in chunks {
        ranges.extend(run_ranges_of(chunk));
    }
    ranges
}

/// The `(start, len)` of every run of one chunk, or the empty range it closes at.
fn run_ranges_of(chunk: &EvaluatedChunk) -> Vec<(usize, usize)> {
    let runs: Vec<(usize, usize)> = chunk
        .series
        .time
        .runs()
        .map(|run| (run.position, run.len))
        .collect();
    if !runs.is_empty() {
        return runs;
    }
    chunk
        .range()
        .map(|range| vec![(range.start(), 0)])
        .unwrap_or_default()
}

/// The rows handed out over every chunk of one series.
fn total_rows(chunks: &[EvaluatedChunk]) -> usize {
    chunks.iter().map(|chunk| chunk.series.time.len()).sum()
}

/// A one-millisecond window, so the value at a grid point is the sample sitting exactly on it.
fn pinpoint(window: RangeWindow) -> RangeEvaluator {
    probe_evaluator(1, grid(0, 1, 1_000_000), window)
}

fn one_ms() -> RangeWindow {
    RangeWindow {
        range_ms: 1,
        offset_ms: 0,
        anchor_ms: None,
    }
}

/// Two samples a million positions apart cost two rows: a position holding no value is not a
/// row, so the empty stretch between them is never materialised.
#[test]
fn a_huge_gap_costs_one_row_per_value() {
    let mut evaluator = pinpoint(one_ms());
    let rows = [(10, vec![Some(1.0)]), (900_000, vec![Some(2.0)])];
    let chunks = collect_chunks(&mut evaluator, &[chunk(&rows, 1, 0, true)]);

    assert_eq!(
        chunk_ranges(&chunks),
        vec![(10, 1), (900_000, 1)],
        "one run per value, each over its own position"
    );
    assert_eq!(chunks.len(), 1, "one push decides both values");
    assert_eq!(total_rows(&chunks), 2, "two values, two rows");
    for (start, len) in chunk_ranges(&chunks) {
        let end = start + len;
        assert!(
            end <= 11 || start >= 900_000,
            "run {start}..{end} reaches into the empty stretch"
        );
        assert_ne!((start, end), (0, 1_000_000), "no run spans the whole grid");
    }
    assert!(chunks.last().unwrap().is_last);
    evaluator.finish().unwrap();
}

/// A series whose first value comes late is addressed at that position: nothing is allocated for
/// the positions before it.
#[test]
fn a_very_late_first_value_allocates_nothing_before_it() {
    let mut evaluator = pinpoint(one_ms());
    let chunks = collect_chunks(
        &mut evaluator,
        &[chunk(&[(999_000, vec![Some(1.0)])], 1, 0, true)],
    );

    assert_eq!(chunk_ranges(&chunks), vec![(999_000, 1)]);
    assert_eq!(total_rows(&chunks), 1);
    assert!(chunks[0].is_last);
    evaluator.finish().unwrap();
}

/// A series whose last value comes early closes on the value it has: the positions after it are
/// settled and hold nothing, which the close reports without a row for any of them.
#[test]
fn a_very_early_last_value_closes_without_rows_for_the_rest() {
    let mut evaluator = pinpoint(one_ms());
    let first = chunk(&[(0, vec![Some(1.0)])], 1, 0, false);
    let mut chunks = vec![evaluator.push(&first).unwrap().expect("point 0 is decided")];
    assert!(!chunks[0].is_last);
    chunks.extend(
        evaluator
            .push(&chunk(&[(5, vec![Some(2.0)])], 1, 0, true))
            .unwrap(),
    );

    assert_eq!(chunk_ranges(&chunks), vec![(0, 1), (5, 1)]);
    assert_eq!(
        total_rows(&chunks),
        2,
        "the two positions holding a value, and no others"
    );
    assert!(chunks.last().unwrap().is_last);
    evaluator.finish().unwrap();
}

/// A push that advances over a long blank stretch hands out nothing for it, and a later push still
/// yields its own value: skipping the stretch loses no sample.
#[test]
fn a_blank_stretch_hands_out_nothing_and_loses_the_next_value() {
    let mut evaluator = pinpoint(one_ms());
    let mut chunks = Vec::new();
    chunks.extend(
        evaluator
            .push(&chunk(&[(10, vec![Some(1.0)])], 1, 0, false))
            .unwrap(),
    );
    assert_eq!(
        chunk_ranges(&chunks),
        vec![(10, 1)],
        "only the position holding a value is handed out"
    );
    assert!(!chunks[0].is_last);

    chunks.extend(
        evaluator
            .push(&chunk(&[(900_000, vec![Some(2.0)])], 1, 0, true))
            .unwrap(),
    );
    assert_eq!(chunk_ranges(&chunks), vec![(10, 1), (900_000, 1)]);
    assert_eq!(total_rows(&chunks), 2);
    assert!(chunks.last().unwrap().is_last);

    let folded = fold_chunks(&chunks, evaluator.output_schema()).expect("the series completed");
    assert_eq!(
        rows_with_a_value(&folded),
        2,
        "the later value reached the consumer"
    );
    evaluator.finish().unwrap();
}

/// A series with no value anywhere still closes, exactly once, on a chunk holding no rows.
#[test]
fn a_series_without_any_result_closes_once() {
    // The only sample lies before the grid, so no window contains it.
    let mut evaluator = probe_evaluator(1, grid(5, 1, 1_000_000), one_ms());
    let mut chunks = Vec::new();
    chunks.extend(
        evaluator
            .push(&chunk(&[(0, vec![Some(1.0)])], 1, 0, true))
            .unwrap(),
    );

    assert_eq!(chunks.len(), 1, "a series closes exactly once");
    assert!(chunks[0].is_last);
    assert_eq!(chunk_ranges(&chunks), vec![(0, 0)]);
    assert_eq!(total_rows(&chunks), 0);
    evaluator.finish().unwrap();
}

/// A valued prefix followed by an all-empty remainder still closes, and the remainder costs no
/// row.
#[test]
fn a_valued_prefix_and_an_empty_remainder_still_close() {
    let mut evaluator = pinpoint(one_ms());
    let mut chunks = vec![
        evaluator
            .push(&chunk(&[(3, vec![Some(1.0)])], 1, 0, false))
            .unwrap()
            .expect("point 3 is decided"),
    ];
    assert_eq!(chunk_ranges(&chunks), vec![(3, 1)]);
    assert!(!chunks[0].is_last);

    // The last row is stale, so it is dropped and the remainder holds no value at all.
    let last = chunk(&[(900_000, vec![Some(stale())])], 1, 0, true);
    chunks.extend(evaluator.push(&last).unwrap());

    assert_eq!(chunk_ranges(&chunks), vec![(3, 1), (4, 0)]);
    assert_eq!(total_rows(&chunks), 1, "the remainder holds no rows");
    assert!(chunks.last().unwrap().is_last);
    evaluator.finish().unwrap();
}

/// Folding the chunks of a sparse series gives the same series as folding the same samples with a
/// row for every position holding no value: a missing position and an uncovered one mean the same
/// thing to a consumer.
#[test]
fn a_sparse_series_folds_like_the_same_data_with_explicit_nulls() {
    let grid = grid(0, 1_000, 40);
    let window = RangeWindow {
        range_ms: 3_000,
        offset_ms: 0,
        anchor_ms: None,
    };
    let rows = [
        (0, vec![Some(1.0)]),
        (1_000, vec![Some(2.0)]),
        (2_000, vec![Some(3.0)]),
        (20_000, vec![Some(4.0)]),
        (21_000, vec![Some(5.0)]),
        (39_000, vec![Some(6.0)]),
    ];
    let mut evaluator = probe_evaluator(1, grid.clone(), window);
    let chunks = collect_chunks(&mut evaluator, &[chunk(&rows, 1, 0, true)]);
    let folded = fold_chunks(&chunks, evaluator.output_schema()).expect("the series completed");

    let expected = reference(&rows, &grid, window, 1, true, true);
    let spread = expected.iter().filter(|row| row[0].is_some()).count();
    assert_eq!(
        total_rows(&chunks),
        spread,
        "a row per position holding a value, and none for the rest"
    );
    assert_eq!(
        expand(&folded),
        expected,
        "the sparse result equals the one written as null rows"
    );
    evaluator.finish().unwrap();
}

/// The runs of one evaluated series share the evaluator's retained samples: a window reaching
/// back across the gap to an earlier run reads both.
///
/// This is what makes a sparse inner result usable. Closing the series after its first run, or
/// promoting each run to a series of its own, would leave the window at 10ms reading 3 alone
/// instead of 2 + 3.
#[test]
fn a_window_reads_across_the_gap_between_runs_of_one_series() {
    let inner = grid(0, 1, 11);
    let schema = Arc::new(SeriesSchema {
        fields: raw_schema(1).fields,
        eval_grid: Some(inner),
    });
    // One point at 10ms; the window reaches back 11ms, which takes in the point at 0ms.
    let window = RangeWindow {
        range_ms: 11,
        offset_ms: 0,
        anchor_ms: None,
    };
    let mut evaluator =
        RangeEvaluator::try_new(&schema, grid(10, 1, 1), window, RangeFunction::SumOverTime)
            .unwrap();
    let labels = BTreeMap::from([("job".to_string(), "x".to_string())]);
    let run = |point: usize, value: f64| {
        mixed_series(
            &schema,
            &labels,
            &Points::from([(point, value.to_bits())]),
            &BTreeMap::new(),
        )
        .unwrap()
    };

    let mut chunks = Vec::new();
    chunks.extend(evaluator.push_series(&run(0, 2.0), false).unwrap());
    assert!(
        chunks.is_empty(),
        "the run at 0ms decides no point of the outer grid"
    );
    chunks.extend(evaluator.push_series(&run(10, 3.0), true).unwrap());

    let closed = chunks.last().expect("the series closes");
    assert!(closed.is_last);
    assert!(matches!(
        closed.series.fields.as_slice()[0].sample(0),
        SampleRef::Float(sum) if sum == 5.0
    ));
    evaluator.finish().unwrap();
}

/// The samples decide the rows, not the input chunking: the same logical data split at different
/// boundaries folds to the same series over the same number of rows.
#[test]
fn raw_chunk_boundaries_do_not_change_the_rows() {
    let grid = grid(0, 1_000, 40);
    let window = RangeWindow {
        range_ms: 2_000,
        offset_ms: 0,
        anchor_ms: None,
    };
    let rows = [
        (0, vec![Some(1.0)]),
        (1_000, vec![Some(2.0)]),
        (10_000, vec![Some(3.0)]),
        (11_000, vec![Some(4.0)]),
        (30_000, vec![Some(5.0)]),
    ];
    let expected = reference(&rows, &grid, window, 1, true, true);
    let expected_rows = expected.iter().filter(|row| row[0].is_some()).count();
    let splits: [&[usize]; 4] = [&[], &[2], &[2, 4], &[1, 2, 3, 4]];

    let mut outcomes = Vec::new();
    for split in splits {
        let mut chunks = Vec::new();
        let mut start = 0;
        let mut evaluator = probe_evaluator(1, grid.clone(), window);
        for end in split.iter().copied().chain(std::iter::once(rows.len())) {
            let piece = chunk(&rows[start..end], 1, 0, end == rows.len());
            chunks.extend(evaluator.push(&piece).unwrap());
            start = end;
        }
        let folded = fold_chunks(&chunks, evaluator.output_schema()).expect("the series completed");
        assert_eq!(expand(&folded), expected, "split {split:?} differs");
        // No run ever spans a position holding no value, whatever the chunking: the rows of a run
        // are consecutive grid positions, and the positions between two runs are the empty ones.
        for (start, len) in chunk_ranges(&chunks) {
            assert!(
                len <= 3,
                "split {split:?} handed out {len} rows at once from {start} over a series with no three-value run"
            );
        }
        outcomes.push(total_rows(&chunks));
        evaluator.finish().unwrap();
    }
    // Which chunk a run lands in follows the input boundaries, since a push has to hand its
    // decided positions out before the next one changes them. What must not depend on chunking is
    // the number of rows: an empty position never costs one.
    assert!(
        outcomes.windows(2).all(|pair| pair[0] == pair[1]),
        "chunking changed the rows handed out: {outcomes:?}"
    );
    assert_eq!(outcomes, vec![expected_rows; outcomes.len()]);
}

/// Values and nulls alternating: one row per value and one chunk per value, which is what this
/// design costs for data that never holds two consecutive positions.
#[test]
fn alternating_values_and_nulls_hand_out_one_row_per_value() {
    let mut evaluator = probe_evaluator(1, grid(0, 1, 10), one_ms());
    let rows: Vec<Row> = (0..10)
        .map(|index| {
            let value = (index % 2 == 0).then_some(index as f64 + 1.0);
            (index, vec![value])
        })
        .collect();
    let chunks = collect_chunks(&mut evaluator, &[chunk(&rows, 1, 0, true)]);

    assert_eq!(
        chunk_ranges(&chunks),
        vec![(0, 1), (2, 1), (4, 1), (6, 1), (8, 1)],
        "one run per value: five values, five runs"
    );
    assert_eq!(
        chunks.len(),
        1,
        "one push decides the whole series, so it arrives as one chunk"
    );
    assert_eq!(total_rows(&chunks), 5);
    assert!(chunks.last().unwrap().is_last);
    assert_eq!(chunks.iter().filter(|chunk| chunk.is_last).count(), 1);
    evaluator.finish().unwrap();
}

/// Values and nulls alternating over 200,000 positions: 100,000 valued positions cost 100,000
/// rows, not one per position, and each value is its own run.
///
/// The run count is the accepted cost of this design; the row count is what must not regress.
#[test]
fn interleaved_values_over_a_long_grid_cost_one_row_per_value() {
    const GRID: u64 = 200_000;
    let mut evaluator = probe_evaluator(1, grid(0, 1, GRID), one_ms());
    let rows: Vec<Row> = (0..GRID)
        .map(|index| {
            let value = (index % 2 == 0).then_some(index as f64);
            (index as i64, vec![value])
        })
        .collect();
    let chunks = collect_chunks(&mut evaluator, &[chunk(&rows, 1, 0, true)]);

    assert_eq!(
        total_rows(&chunks),
        (GRID / 2) as usize,
        "one row per valued position, never one per position"
    );
    assert_eq!(
        chunks.len(),
        1,
        "the whole series is decided by one push, so it arrives as one chunk"
    );
    assert!(chunks.last().unwrap().is_last);
    assert_eq!(chunks.iter().filter(|chunk| chunk.is_last).count(), 1);
    let runs: Vec<(usize, usize)> = chunks[0]
        .series
        .time
        .runs()
        .map(|run| (run.position, run.len))
        .collect();
    assert_eq!(
        runs.len(),
        (GRID / 2) as usize,
        "every valued position is its own run"
    );
    for (start, len) in runs {
        assert_eq!(len, 1, "every valued position is its own run");
        assert_eq!(start % 2, 0, "only even positions hold a value");
    }
    evaluator.finish().unwrap();
}

/// An anchored selector broadcasts one value over every grid point, so every position holds one:
/// the result is a single contiguous piece, not one piece per position.
#[test]
fn an_anchored_replay_stays_one_contiguous_piece() {
    let window = RangeWindow {
        range_ms: 10,
        offset_ms: 0,
        anchor_ms: Some(5),
    };
    for step_invariant in [true, false] {
        let mut evaluator = if step_invariant {
            // `Replay::Once`: the first point's result holds at every point.
            probe_evaluator(1, grid(0, 10, 100), window)
        } else {
            // `Replay::EveryPoint`: the first point's window is evaluated at every point.
            per_point_probe_evaluator(1, grid(0, 10, 100), window)
        };
        let chunks = collect_chunks(
            &mut evaluator,
            &[chunk(&[(0, vec![Some(1.0)])], 1, 0, true)],
        );
        assert_eq!(
            chunk_ranges(&chunks),
            vec![(0, 100)],
            "every position holds a value, so the whole grid is one piece              (step_invariant={step_invariant})"
        );
        assert_eq!(total_rows(&chunks), 100);
        assert!(chunks[0].is_last);
        evaluator.finish().unwrap();
    }
}

/// Fields of one run stay row-aligned: a field with no value at a position another field fills
/// keeps a null row there. That null is the layout, not a bridged gap.
#[test]
fn a_run_keeps_its_fields_aligned_and_nulls_only_the_missing_one() {
    let mut evaluator = probe_evaluator(2, grid(0, 1, 6), one_ms());
    let rows = [
        (0, vec![Some(1.0), None]),
        (1, vec![None, Some(2.0)]),
        (2, vec![Some(3.0), Some(4.0)]),
    ];
    let chunks = collect_chunks(&mut evaluator, &[chunk(&rows, 2, 0, true)]);

    assert_eq!(
        chunk_ranges(&chunks),
        vec![(0, 3)],
        "three positions, one run"
    );
    let series = &chunks[0].series;
    let observed: Vec<Vec<bool>> = (0..3)
        .map(|row| {
            series
                .fields
                .iter()
                .map(|field| !matches!(field.sample(row), SampleRef::Missing))
                .collect()
        })
        .collect();
    assert_eq!(
        observed,
        vec![vec![true, false], vec![false, true], vec![true, true]],
        "both fields carry a row at every position of the run"
    );
    assert!(chunks[0].is_last);
    evaluator.finish().unwrap();
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
/// input keeps as an ordinary NaN. The rows hold holes, so the series carries one run per stretch
/// of positions holding a value.
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

/// The rows `offset..offset + len` of `series` as a series of its own, on the same axis.
fn rows_of(schema: &Arc<SeriesSchema>, series: &Series, offset: usize, len: usize) -> Series {
    let fields = series
        .fields
        .iter()
        .map(|field| {
            field
                .slice(offset, len)
                .expect("the rows lie inside the field")
        })
        .collect::<Vec<_>>();
    Series::try_new(
        schema,
        series.metric.clone(),
        series
            .time
            .slice(offset, len)
            .expect("the rows lie inside the axis"),
        SeriesFields::try_from_vec(fields).expect("one field per schema field"),
    )
    .expect("the slice matches the schema")
}

/// Delivers one evaluated series the way a stream does -- a prefix now, the closing rows later --
/// and drains the chunks after each delivery.
fn deliver_as_a_stream(
    evaluator: &mut RangeEvaluator,
    schema: &Arc<SeriesSchema>,
    series: &Series,
    rng: &mut StdRng,
) -> Vec<EvaluatedChunk> {
    let len = series.time.len();
    let cut = if len < 2 {
        len
    } else {
        rng.random_range(1..len)
    };
    let mut chunks = Vec::new();
    if cut == len {
        if let Some(chunk) = evaluator.push_series(series, true).unwrap() {
            chunks.push(chunk);
        }
        return chunks;
    }
    if let Some(chunk) = evaluator
        .push_series(&rows_of(schema, series, 0, cut), false)
        .unwrap()
    {
        chunks.push(chunk);
    }
    if let Some(chunk) = evaluator
        .push_series(&rows_of(schema, series, cut, len - cut), true)
        .unwrap()
    {
        chunks.push(chunk);
    }
    chunks
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
        let schema = Arc::new(SeriesSchema {
            fields: raw_schema(fields).fields,
            eval_grid: Some(inner.clone()),
        });
        let (series, rows) = random_evaluated(&mut rng, &inner, fields, seed);
        // Every row of this draw holds no value, so there is no input series to deliver.
        let Some(series) = series else {
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
        // The same logical series arrives as a prefix and its closing rows, as a stream delivers
        // it: the runs of both deliveries are folded together for the comparison.
        let evaluated = deliver_as_a_stream(&mut probe, &schema, &series, &mut rng);
        assert!(evaluated.last().expect("the input closes").is_last);
        let result = fold_chunks(&evaluated, probe.output_schema());
        assert_series_eq(result.as_ref(), &expected, &metric(seed));
        for function in udf_functions() {
            let mut evaluator =
                RangeEvaluator::try_new(&schema, outer.clone(), window, function).unwrap();
            let expected = udf_reference(function, &rows, &outer, window, fields, false);
            let evaluated = deliver_as_a_stream(&mut evaluator, &schema, &series, &mut rng);
            let result = fold_chunks(&evaluated, evaluator.output_schema());
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
    let series = builder
        .finish(metric(0))
        .unwrap()
        .expect("one contiguous run");
    execution_error(probe_evaluator(1, grid(0, 10, 4), window).push_series(&series, true));
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
            assert!(chunk_run(&handed).start() + chunk_run(&handed).len() <= 1);
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
            let input = chunk(&rows, 1, host as u64, true);
            let chunks = collect_chunks(&mut evaluator, std::slice::from_ref(&input));
            let folded =
                fold_chunks(&chunks, evaluator.output_schema()).expect("every input series closes");
            present.push(folded);
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
        // `absent` over single-field inputs writes a value at every point where it is absent, so
        // the one series, expanded over the grid, gives the dense expectation.
        assert_pieces_eq(actual.as_slice(), &expected, &labels);
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
                if series.time.grid().is_none() {
                    panic!("expected a grid series, got {:?}", series.time);
                }
                let floats = &expected[labels];
                let no_histograms = BTreeMap::new();
                let texts = case.result_histograms.get(labels).unwrap_or(&no_histograms);
                let mut points = 0;
                // The axis says where each row sits, so a result of several runs compares right.
                let mut cursor = series.fields.as_slice()[0].cursor();
                for (point, row) in series.time.positions() {
                    let sample = cursor.at(row).expect("the axis and the fields are aligned");
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
    let mut chunks = evaluate_series(&mut evaluator, &series);
    assert_eq!(chunks.len(), 1);
    let result = chunks.pop().unwrap().series;
    assert!(matches!(
        result.fields.as_slice()[0].sample(0),
        SampleRef::Float(resets) if resets == 0.0
    ));
}

/// A series of `grid` holding no position: the shape of an empty closing chunk.
fn empty_series(
    schema: &Arc<SeriesSchema>,
    grid: &Arc<EvalGrid>,
    metric: MetricName,
    at: usize,
) -> Series {
    let fields = SeriesFields::try_from_vec(vec![Samples::from_float(Float64Array::from(Vec::<
        f64,
    >::new(
    )))])
    .unwrap();
    Series::try_new(
        schema,
        metric,
        TimeAxis::Grid(GridRange::try_new(grid.clone(), at, 0).unwrap()),
        fields,
    )
    .unwrap()
}

/// The evaluated-input schema of `grid`: the shape an upstream evaluator's chunks declare.
fn evaluated_schema(grid: &Arc<EvalGrid>) -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: raw_schema(1).fields.clone(),
        eval_grid: Some(grid.clone()),
    })
}

/// A delivery holding no position decides nothing, so inside a series it is refused: it cannot be
/// told from a stall, and the caller would wait for a result that is not coming. The same empty
/// delivery carrying `is_last` is the series' close, and the identity initialises and closes.
#[test]
fn an_empty_delivery_closes_only_when_it_is_last() {
    let outer = grid(0, 10, 4);
    let schema = evaluated_schema(&outer);
    let empty = empty_series(&schema, &outer, metric(0), 0);

    let mut mid_series =
        RangeEvaluator::try_new(&schema, outer.clone(), one_ms(), RangeFunction::SumOverTime)
            .unwrap();
    execution_error(mid_series.push_series(&empty, false));

    // The identity never produced a value, so the only chunk it hands out is the empty close.
    let mut evaluator =
        RangeEvaluator::try_new(&schema, outer.clone(), one_ms(), RangeFunction::SumOverTime)
            .unwrap();
    let closed = evaluator
        .push_series(&empty, true)
        .unwrap()
        .expect("the closing chunk");
    assert!(closed.is_last);
    assert!(closed.series.time.is_empty(), "nothing was decided");
    assert_eq!(
        closed.series.metric,
        output_metric(RangeFunction::SumOverTime, 0)
    );
    evaluator.finish().unwrap();
}

/// A valued prefix followed by an empty closing chunk gives the same result as the same data
/// delivered in one piece: composing streams must not change the answer.
#[test]
fn a_valued_prefix_composes_with_an_empty_closing_chunk() {
    let outer = grid(0, 1_000, 6);
    let schema = evaluated_schema(&outer);
    let window = RangeWindow {
        range_ms: 2_000,
        offset_ms: 0,
        anchor_ms: None,
    };
    let points = Points::from([
        (0, f64::to_bits(1.0)),
        (1, f64::to_bits(2.0)),
        (3, f64::to_bits(4.0)),
    ]);
    let labels = Labels::from([("job".to_string(), "x".to_string())]);
    let valued = series_of(&schema, &labels, &points).expect("three rows");

    // One piece: the whole series closes on its own delivery.
    let mut whole =
        RangeEvaluator::try_new(&schema, outer.clone(), window, RangeFunction::SumOverTime)
            .unwrap();
    let one_piece = evaluate_series(&mut whole, &valued);
    let expected = fold_chunks(&one_piece, whole.output_schema()).expect("the series completed");
    whole.finish().unwrap();

    // Composed: the valued rows as a prefix, then the empty chunk that closes the series.
    let mut composed =
        RangeEvaluator::try_new(&schema, outer.clone(), window, RangeFunction::SumOverTime)
            .unwrap();
    let mut chunks = Vec::new();
    chunks.extend(composed.push_series(&valued, false).unwrap());
    assert!(
        total_rows(&chunks) > 0,
        "the valued prefix already decided some points"
    );
    assert!(
        !chunks.last().expect("a chunk").is_last,
        "the prefix does not close the series"
    );
    // The empty close keeps the identity of the series it closes.
    let empty = empty_series(&schema, &outer, metric_of(&labels), 6);
    chunks.extend(composed.push_series(&empty, true).unwrap());
    let actual = fold_chunks(&chunks, composed.output_schema()).expect("the series completed");
    composed.finish().unwrap();
    assert_eq!(
        expand(&actual),
        expand(&expected),
        "the composed pipeline disagrees with one piece"
    );
}

/// The empty closing chunk an evaluator produces is legal input for the next one: a pipeline that
/// composes two evaluators must not reject its own output.
#[test]
fn an_empty_chunk_from_one_evaluator_closes_the_next() {
    let outer = grid(0, 1_000, 4);
    // A sample far past the grid closes a series whose every window stays empty, so the only chunk
    // the evaluator hands out is the empty close.
    let rows = [(100_000, vec![Some(1.0)])];
    let mut upstream = probe_evaluator(1, outer.clone(), one_ms());
    let produced = collect_chunks(&mut upstream, &[chunk(&rows, 1, 0, true)]);
    upstream.finish().unwrap();
    let closed = produced.last().expect("the series closes");
    assert!(closed.is_last);
    assert!(
        closed.series.time.is_empty(),
        "the closing chunk carries no row"
    );

    let schema = evaluated_schema(&outer);
    let mut downstream =
        RangeEvaluator::try_new(&schema, outer.clone(), one_ms(), RangeFunction::SumOverTime)
            .unwrap();
    let chunk = downstream
        .push_series(&closed.series, true)
        .unwrap()
        .expect("the downstream series still closes");
    assert!(chunk.is_last);
    assert!(chunk.series.time.is_empty());
    downstream.finish().unwrap();
}

// ---------------------------------------------------------------------------------------------
// Acceptance: a fragmented result, end to end through the evaluator.
//
// These tests pin the observable shape of a sparse result as well as its values: one series per
// logical output, one row per position that holds a value, and an axis that names where those rows
// sit. The dense matrices they compare against are built by this module's reference and are never
// fed back into the evaluator.
// ---------------------------------------------------------------------------------------------

/// A window of one step over a grid of `step_ms`: the point at `t` reads the sample sitting
/// exactly on `t`, so a kernel that reports its last sample reports the input value unchanged.
fn one_step_window(step_ms: i64) -> RangeWindow {
    RangeWindow {
        range_ms: step_ms,
        offset_ms: 0,
        anchor_ms: None,
    }
}

/// A: a million-position grid holding two values, one a long way into the grid.
///
/// The span between them costs neither a row nor a field sample: the axis carries two rows and
/// names two runs, and the shared grid -- one timestamp buffer for every result of the query -- is
/// what turns a row into an instant.
#[test]
fn a_million_position_grid_with_two_values_is_one_series_of_two_rows() {
    const SPAN: u64 = 1_000_000;
    let grid = grid(0, 1_000, SPAN);
    // `last_over_time` over a one-step window reports the sample sitting on the point.
    let mut evaluator = RangeEvaluator::try_new(
        &raw_schema(1),
        grid.clone(),
        one_step_window(1_000),
        RangeFunction::LastOverTime,
    )
    .unwrap();
    let rows = [(10_000, vec![Some(1.0)]), (900_000_000, vec![Some(2.0)])];
    let chunks = collect_chunks(&mut evaluator, &[chunk(&rows, 1, 0, true)]);

    // One series, not a piece per run: a push hands its decided rows over in one `Series`.
    assert_eq!(
        chunks.len(),
        1,
        "one push decides the whole series, so it arrives as one chunk, got {}",
        chunks.len()
    );
    let closing = &chunks[0];
    assert!(
        closing.is_last,
        "a one-push series closes on its only chunk"
    );
    let series = &closing.series;
    assert_eq!(
        run_ranges_of(closing),
        vec![(10, 1), (900_000, 1)],
        "two values a million positions apart are two runs"
    );
    assert_eq!(
        series.time.len(),
        2,
        "the axis carries two rows, not the span"
    );
    assert_eq!(
        series.fields.len(),
        1,
        "the evaluator kept its input's field"
    );
    let field = &series.fields.as_slice()[0];
    assert_eq!(field.len(), 2, "the field holds one sample per carried row");
    let values = match field.dense() {
        Some(DenseSamplesRef::Float(values)) => values,
        _ => panic!("the field is one dense float array"),
    };
    assert_eq!(
        values.len(),
        2,
        "the value buffer is sized by the values, not by the span"
    );
    assert_eq!(values.values().as_ref(), &[1.0, 2.0]);

    // What the axis does carry is the shared grid: every result of the query addresses rows on it.
    assert_eq!(
        series
            .time
            .grid()
            .expect("a grid series")
            .timestamps()
            .len(),
        SPAN as usize,
        "the grid really holds a million positions"
    );
    assert_eq!(
        series.time.timestamps().len(),
        2,
        "the axis hands out two timestamps, not one per grid position"
    );
    assert_eq!(series.time.timestamp(0), Some(10_000));
    assert_eq!(series.time.timestamp(1), Some(900_000_000));

    // The reference: the result expands to a million slots, and exactly the two valued positions
    // carry anything.
    let expanded = expand(series);
    assert_eq!(expanded.len(), SPAN as usize);
    assert_eq!(
        expanded.iter().filter(|row| row[0].is_some()).count(),
        2,
        "only the two pushed positions hold a value"
    );
    assert_eq!(expanded[10], vec![Some(1.0)]);
    assert_eq!(expanded[900_000], vec![Some(2.0)]);
    evaluator.finish().unwrap();
}

/// B: every position valued, so the result is one contiguous run and keeps the fast path: the
/// axis is a `TimeAxis::Grid`, not a span list of one element.
#[test]
fn a_contiguous_result_still_takes_the_single_range_fast_path() {
    const POINTS: u64 = 8;
    let grid = grid(0, 1_000, POINTS);
    let mut evaluator = RangeEvaluator::try_new(
        &raw_schema(1),
        grid.clone(),
        one_step_window(1_000),
        RangeFunction::LastOverTime,
    )
    .unwrap();
    let rows: Vec<Row> = (0..POINTS)
        .map(|index| (index as i64 * 1_000, vec![Some(index as f64 + 1.0)]))
        .collect();
    let chunks = collect_chunks(&mut evaluator, &[chunk(&rows, 1, 0, true)]);

    assert_eq!(chunks.len(), 1);
    let series = &chunks[0].series;
    let range = series
        .time
        .as_grid_range()
        .unwrap_or_else(|| panic!("a contiguous result is a grid range, got {:?}", series.time));
    assert_eq!(
        (range.start(), range.len()),
        (0, POINTS as usize),
        "the range starts at the first valued position and covers every position"
    );
    assert!(series.time.is_contiguous());
    assert_eq!(series.time.len(), POINTS as usize);
    evaluator.finish().unwrap();
}

/// B: alternating valued and missing positions: one series whose axis names one run per value, and
/// one delivery for the whole series rather than one per run.
///
/// The measured numbers are 8 runs for 16 positions, handed out by 1 chunk.
#[test]
fn alternating_values_and_holes_arrive_in_one_delivery_over_many_runs() {
    const POINTS: usize = 16;
    let grid = grid(0, 1_000, POINTS as u64);
    let mut evaluator = RangeEvaluator::try_new(
        &raw_schema(1),
        grid.clone(),
        one_step_window(1_000),
        RangeFunction::LastOverTime,
    )
    .unwrap();
    // Every even position holds a value, every odd one holds none.
    let rows: Vec<Row> = (0..POINTS)
        .map(|index| {
            let value = (index % 2 == 0).then_some(index as f64);
            (index as i64 * 1_000, vec![value])
        })
        .collect();
    let chunks = collect_chunks(&mut evaluator, &[chunk(&rows, 1, 0, true)]);

    assert_eq!(chunks.len(), 1, "one push decides the whole series");
    let series = &chunks[0].series;
    let runs = run_ranges_of(&chunks[0]);
    let expected: Vec<(usize, usize)> = (0..POINTS).step_by(2).map(|index| (index, 1)).collect();
    assert_eq!(
        runs, expected,
        "every valued position is its own run, at its own absolute position"
    );
    assert_eq!(runs.len(), POINTS / 2, "8 runs over 16 positions");
    assert!(
        chunks.len() * 4 <= runs.len(),
        "deliveries ({}) must stay far below the runs ({}): one delivery per run would be the \
         envelope strategy this change removes",
        chunks.len(),
        runs.len()
    );
    assert_eq!(series.time.len(), POINTS / 2, "one row per value");
    assert_eq!(chunks.iter().filter(|chunk| chunk.is_last).count(), 1);
    evaluator.finish().unwrap();
}

/// B: the same positions delivered as one series with holes and as several pushes of the same
/// logical series give the same dense result.
///
/// A stream hands a series over in pieces, so where the piece boundaries fall must not decide the
/// answer: only the axis does.
#[test]
fn a_series_with_holes_and_the_same_positions_in_several_pushes_expand_alike() {
    let outer = grid(0, 1_000, 16);
    let schema = evaluated_schema(&outer);
    let window = one_step_window(1_000);
    let labels = Labels::from([("job".to_string(), "x".to_string())]);
    let points = Points::from([
        (0, f64::to_bits(1.0)),
        (1, f64::to_bits(2.0)),
        (3, f64::to_bits(4.0)),
        (8, f64::to_bits(8.0)),
        (9, f64::to_bits(16.0)),
        (15, f64::to_bits(32.0)),
    ]);

    // One series, holes as explicit nulls between the valued positions.
    let holed = series_of(&schema, &labels, &points).expect("six positions hold a value");
    assert_eq!(
        holed.time.len(),
        16,
        "the fixture spans the whole grid with nulls"
    );
    let mut one_push =
        RangeEvaluator::try_new(&schema, outer.clone(), window, RangeFunction::LastOverTime)
            .unwrap();
    let whole = fold_chunks(
        &evaluate_series(&mut one_push, &holed),
        one_push.output_schema(),
    )
    .expect("the series completed");
    one_push.finish().unwrap();
    let expected = expand(&whole);
    assert_eq!(
        expected.iter().filter(|row| row[0].is_some()).count(),
        6,
        "the one-piece result carries the six values"
    );

    // The same positions as a fragmented axis, delivered in three pushes of the one series.
    let sparse = sparse_series(&schema, &labels, &points).expect("six positions hold a value");
    assert_eq!(
        sparse.time.len(),
        6,
        "the fragmented fixture carries six rows"
    );
    assert_eq!(
        sparse.time.runs().count(),
        4,
        "positions 0-1, 3, 8-9 and 15 are four runs"
    );
    let cuts = [1, 3];
    let mut delivered =
        RangeEvaluator::try_new(&schema, outer.clone(), window, RangeFunction::LastOverTime)
            .unwrap();
    let mut chunks: Vec<EvaluatedChunk> = Vec::new();
    let mut start = 0;
    for end in cuts
        .iter()
        .copied()
        .chain(std::iter::once(sparse.time.len()))
    {
        let piece = rows_of(&schema, &sparse, start, end - start);
        chunks.extend(
            delivered
                .push_series(&piece, end == sparse.time.len())
                .unwrap(),
        );
        start = end;
    }
    assert!(
        chunks.len() >= 2,
        "the pushes hand out more than one chunk, got {}",
        chunks.len()
    );
    assert!(chunks.last().expect("a closing chunk").is_last);
    let several = fold_chunks(&chunks, delivered.output_schema()).expect("the series completed");
    delivered.finish().unwrap();

    assert_eq!(
        expand(&several),
        expected,
        "where a push ends changed the answer"
    );
}

/// E (5): the input chunk boundaries do not change the evaluated rows.
///
/// Runs compare only across the variants whose settled axis is genuinely the same: a push settles
/// the points it decides, so the same logical data split differently may hand a run over as two
/// adjacent spans. What must not depend on the split is the set of carried positions, which is
/// what the expanded matrix compares.
#[test]
fn input_chunk_boundaries_do_not_change_the_evaluated_rows() {
    let grid = grid(0, 1_000, 12);
    let window = one_step_window(1_000);
    let rows = [
        (0, vec![Some(1.0)]),
        (1_000, vec![Some(2.0)]),
        (3_000, vec![Some(4.0)]),
        (4_000, vec![Some(8.0)]),
        (10_000, vec![Some(16.0)]),
    ];
    let splits: [&[usize]; 3] = [&[], &[2], &[1, 3, 4]];
    let mut expanded = Vec::new();
    let mut covered = Vec::new();
    for split in splits {
        let mut evaluator = RangeEvaluator::try_new(
            &raw_schema(1),
            grid.clone(),
            window,
            RangeFunction::LastOverTime,
        )
        .unwrap();
        let mut chunks = Vec::new();
        let mut start = 0;
        for end in split.iter().copied().chain(std::iter::once(rows.len())) {
            let piece = chunk(&rows[start..end], 1, 0, end == rows.len());
            chunks.extend(evaluator.push(&piece).unwrap());
            start = end;
        }
        let folded = fold_chunks(&chunks, evaluator.output_schema()).expect("the series completed");
        // The split decides how many chunks a run is handed out in, never which positions hold a
        // value: compare those, and the dense matrix, not the raw span count.
        let mut positions: Vec<usize> = chunk_ranges(&chunks)
            .into_iter()
            .filter(|(_, len)| *len > 0)
            .flat_map(|(start, len)| start..start + len)
            .collect();
        positions.sort_unstable();
        covered.push(positions);
        expanded.push(expand(&folded));
        evaluator.finish().unwrap();
    }
    assert_eq!(covered[0], vec![0, 1, 3, 4, 10], "split {:?}", splits[0]);
    assert_eq!(
        covered[1], covered[0],
        "split {:?} moved the valued positions",
        splits[1]
    );
    assert_eq!(
        covered[2], covered[0],
        "split {:?} moved the valued positions",
        splits[2]
    );
    assert_eq!(expanded[1], expanded[0], "split {:?}", splits[1]);
    assert_eq!(expanded[2], expanded[0], "split {:?}", splits[2]);
    // The one-piece variant's axis is the maximal-run one, so its runs are pinned exactly.
    assert_eq!(
        expanded[0]
            .iter()
            .enumerate()
            .filter(|(_, row)| row[0].is_some())
            .map(|(index, _)| index)
            .collect::<Vec<_>>(),
        vec![0, 1, 3, 4, 10]
    );
}

/// G: a series hands its rows out before it ends.
///
/// The chunks of a prefix arrive with `is_last` still false, and the positions they carry are a
/// prefix of the positions the whole series ends up holding: a consumer can act on them without
/// waiting for the last chunk of the input.
#[test]
fn a_prefix_hands_out_its_rows_before_the_series_ends() {
    let grid = grid(0, 1_000, 12);
    let mut evaluator = RangeEvaluator::try_new(
        &raw_schema(1),
        grid.clone(),
        one_step_window(1_000),
        RangeFunction::LastOverTime,
    )
    .unwrap();

    let first = evaluator
        .push(&chunk(
            &[(0, vec![Some(1.0)]), (1_000, vec![Some(2.0)])],
            1,
            0,
            false,
        ))
        .unwrap()
        .expect("the first push decides positions 0 and 1");
    assert!(
        !first.is_last,
        "the series keeps going, so the prefix chunk does not close it"
    );
    let mut chunks = vec![first];
    assert_eq!(chunks.len(), 1, "one push hands out one chunk");
    assert_eq!(
        run_ranges_of(&chunks[0]),
        vec![(0, 2)],
        "the prefix carries exactly the positions it decided"
    );

    // The closing push: positions 2..11 are decided, and only position 3 holds a value.
    chunks.extend(
        evaluator
            .push(&chunk(&[(3_000, vec![Some(4.0)])], 1, 0, true))
            .unwrap(),
    );
    let closing = chunks.last().expect("a closing chunk");
    assert!(closing.is_last, "exactly one chunk closes the series");
    assert_eq!(
        closing.range().map(|range| (range.start(), range.len())),
        Some((3, 1))
    );

    // The one-piece form of the same input: the prefix must be a prefix of it.
    let mut one_piece = RangeEvaluator::try_new(
        &raw_schema(1),
        grid.clone(),
        one_step_window(1_000),
        RangeFunction::LastOverTime,
    )
    .unwrap();
    let whole = one_piece
        .push(&chunk(
            &[
                (0, vec![Some(1.0)]),
                (1_000, vec![Some(2.0)]),
                (3_000, vec![Some(4.0)]),
            ],
            1,
            0,
            true,
        ))
        .unwrap()
        .expect("the one push decides the whole series");
    assert!(whole.is_last);
    let folded = fold_chunks(&chunks, one_piece.output_schema()).expect("the series completed");
    assert_eq!(
        expand(&folded),
        expand(&whole.series),
        "the streamed series differs from the one-piece one"
    );

    let prefix_positions: Vec<usize> = chunks[0]
        .series
        .time
        .positions()
        .map(|(position, _)| position)
        .collect();
    let whole_positions: Vec<usize> = whole
        .series
        .time
        .positions()
        .map(|(position, _)| position)
        .collect();
    assert_eq!(
        prefix_positions,
        vec![0, 1],
        "the prefix holds the first rows"
    );
    assert_eq!(
        &whole_positions[..prefix_positions.len()],
        prefix_positions.as_slice(),
        "the prefix carries the first positions of the whole series"
    );
    assert_eq!(whole_positions, vec![0, 1, 3]);
    one_piece.finish().unwrap();
    evaluator.finish().unwrap();
}

/// Sums of field 0 per window, by scanning every row for every grid point: the dense reference of
/// `sum_over_time`, independent of chunking, cursors and the retained-sample state.
fn dense_sum_over_time(rows: &[Row], grid: &EvalGrid, window: RangeWindow) -> Dense {
    assert_eq!(window.offset_ms, 0, "this reference folds no offset");
    grid.timestamps()
        .iter()
        .map(|&eval_ts| {
            let inside: Vec<f64> = rows
                .iter()
                .filter(|(timestamp, _)| {
                    *timestamp > eval_ts - window.range_ms && *timestamp <= eval_ts
                })
                .filter_map(|(_, values)| values[0])
                .collect();
            vec![(!inside.is_empty()).then(|| inside.iter().sum())]
        })
        .collect()
}

/// G: a window at a later run reads the samples of an earlier run of the same series.
///
/// The two runs arrive in two deliveries, and the single outer point's window reaches back across
/// the hole to the first delivery's sample. A run-local read would report 3 + 4 = 7; the window
/// holds 2 + 3 + 4 = 9.
#[test]
fn a_window_at_a_later_run_reads_the_earlier_run() {
    let inner = grid(0, 1_000, 20);
    let schema = Arc::new(SeriesSchema {
        fields: raw_schema(1).fields.clone(),
        eval_grid: Some(inner.clone()),
    });
    let window = RangeWindow {
        range_ms: 12_000,
        offset_ms: 0,
        anchor_ms: None,
    };
    let outer = grid(11_000, 1_000, 1);
    let mut evaluator =
        RangeEvaluator::try_new(&schema, outer.clone(), window, RangeFunction::SumOverTime)
            .unwrap();
    let labels = Labels::from([("job".to_string(), "x".to_string())]);
    let no_histograms = std::collections::BTreeMap::new();
    let early = mixed_series(
        &schema,
        &labels,
        &Points::from([(0, f64::to_bits(2.0))]),
        &no_histograms,
    )
    .expect("position 0 holds a value");
    let late = mixed_series(
        &schema,
        &labels,
        &Points::from([(10, f64::to_bits(3.0)), (11, f64::to_bits(4.0))]),
        &no_histograms,
    )
    .expect("positions 10 and 11 hold values");
    assert_eq!(early.time.runs().count(), 1);
    assert_eq!(late.time.runs().count(), 1);

    let mut chunks = evaluator
        .push_series(&early, false)
        .unwrap()
        .into_iter()
        .collect::<Vec<_>>();
    assert!(
        chunks.is_empty(),
        "the first run alone decides no point of the one-point outer grid"
    );
    chunks.extend(evaluator.push_series(&late, true).unwrap());

    let closing = chunks.last().expect("the series closes");
    assert!(closing.is_last, "exactly one chunk closes the series");
    assert_eq!(
        closing.series.time.len(),
        1,
        "the outer grid holds one point"
    );
    let value = match closing.series.fields.as_slice()[0].sample(0) {
        SampleRef::Float(value) => value,
        SampleRef::Histogram(_) => panic!("expected one float, got a histogram sample"),
        SampleRef::Missing => panic!("expected one float, got a missing sample"),
    };
    assert_eq!(
        value, 9.0,
        "the window reads 2 + 3 + 4 across the hole; a run-local read would report 7"
    );
    let rows = [
        (0, vec![Some(2.0)]),
        (10_000, vec![Some(3.0)]),
        (11_000, vec![Some(4.0)]),
    ];
    let expected = dense_sum_over_time(&rows, &outer, window);
    assert_eq!(expand(&closing.series), expected);
    evaluator.finish().unwrap();
}

/// G: a fragmented prefix closed by an empty chunk equals the one-piece result.
///
/// The prefix carries several runs, so it is a genuinely sparse delivery; the empty chunk only
/// ends the series. Delivering the same rows in one piece must give the same values.
#[test]
fn a_fragmented_prefix_closed_by_an_empty_chunk_equals_one_piece() {
    let outer = grid(0, 1_000, 8);
    let schema = evaluated_schema(&outer);
    let window = one_step_window(1_000);
    let labels = Labels::from([("job".to_string(), "x".to_string())]);
    let points = Points::from([
        (0, f64::to_bits(1.0)),
        (1, f64::to_bits(2.0)),
        (3, f64::to_bits(4.0)),
        (6, f64::to_bits(8.0)),
    ]);
    let sparse = sparse_series(&schema, &labels, &points).expect("four positions hold a value");
    assert_eq!(sparse.time.len(), 4);
    assert_eq!(
        sparse.time.runs().count(),
        3,
        "positions 0-1, 3 and 6 are three runs"
    );

    // One piece: the whole sparse series in one delivery.
    let mut one_piece =
        RangeEvaluator::try_new(&schema, outer.clone(), window, RangeFunction::LastOverTime)
            .unwrap();
    let whole = fold_chunks(
        &evaluate_series(&mut one_piece, &sparse),
        one_piece.output_schema(),
    )
    .expect("the series completed");
    one_piece.finish().unwrap();

    // A prefix, then the empty chunk that closes the series.
    let mut composed =
        RangeEvaluator::try_new(&schema, outer.clone(), window, RangeFunction::LastOverTime)
            .unwrap();
    let mut chunks = composed
        .push_series(&sparse, false)
        .unwrap()
        .into_iter()
        .collect::<Vec<_>>();
    assert!(
        !chunks.is_empty(),
        "the valued prefix decides some points before the series ends"
    );
    assert!(
        !chunks.last().expect("a prefix chunk").is_last,
        "the prefix does not close the series"
    );
    assert_eq!(
        chunk_ranges(&chunks),
        vec![(0, 2), (3, 1), (6, 1)],
        "the prefix keeps the runs of the rows it decided"
    );
    let empty = empty_series(&schema, &outer, metric_of(&labels), 8);
    chunks.extend(composed.push_series(&empty, true).unwrap());
    let closing = chunks.last().expect("the series closes");
    assert!(closing.is_last);
    assert!(
        closing.series.time.is_empty(),
        "the empty chunk only closes the series"
    );
    assert_eq!(
        chunks.iter().filter(|chunk| chunk.is_last).count(),
        1,
        "a series closes exactly once"
    );
    let streamed = fold_chunks(&chunks, composed.output_schema()).expect("the series completed");
    assert_eq!(
        expand(&streamed),
        expand(&whole),
        "the composed series disagrees with the one-piece one"
    );
    assert_eq!(
        expand(&streamed)
            .iter()
            .filter(|row| row[0].is_some())
            .count(),
        4,
        "the four values survived the composition"
    );
    composed.finish().unwrap();
}

/// G: a series none of whose samples falls into any window produces nothing, so the only chunk it
/// hands out is the empty closing one.
#[test]
fn a_series_outside_every_window_closes_on_an_empty_chunk() {
    let inner = grid(0, 1_000, 20);
    let schema = Arc::new(SeriesSchema {
        fields: raw_schema(1).fields.clone(),
        eval_grid: Some(inner.clone()),
    });
    // The one outer point sits at 0ms and its window looks back one step, so a sample at 15ms
    // decides nothing.
    let outer = grid(0, 1_000, 1);
    let mut evaluator = RangeEvaluator::try_new(
        &schema,
        outer.clone(),
        one_step_window(1_000),
        RangeFunction::SumOverTime,
    )
    .unwrap();
    let labels = Labels::from([("job".to_string(), "x".to_string())]);
    let input = mixed_series(
        &schema,
        &labels,
        &Points::from([(15, f64::to_bits(7.0))]),
        &std::collections::BTreeMap::new(),
    )
    .expect("position 15 holds a value");
    let chunks = evaluate_series(&mut evaluator, &input);

    assert_eq!(chunks.len(), 1, "the identity hands out one chunk");
    let closing = &chunks[0];
    assert!(closing.is_last, "the one chunk closes the series");
    assert!(
        closing.series.time.is_empty(),
        "nothing was decided, so the chunk carries no row: {:?}",
        closing.series.time
    );
    assert_eq!(chunks.iter().filter(|chunk| chunk.is_last).count(), 1);
    evaluator.finish().unwrap();
}

/// G: two evaluators composed -- the first one's chunks fed into the second as its input -- agree
/// with the dense reference of the same two stages, and with handing the first stage's result to
/// the second in one piece.
#[test]
fn two_composed_evaluators_agree_with_the_dense_reference() {
    let inner = grid(0, 1_000, 24);
    let outer = grid(0, 1_000, 24);
    let inner_window = RangeWindow {
        range_ms: 3_000,
        offset_ms: 0,
        anchor_ms: None,
    };
    let outer_window = RangeWindow {
        range_ms: 2_000,
        offset_ms: 0,
        anchor_ms: None,
    };
    let rows = vec![
        (0, vec![Some(1.0)]),
        (1_000, vec![Some(2.0)]),
        (10_000, vec![Some(5.0)]),
        (11_000, vec![Some(7.0)]),
        (20_000, vec![Some(11.0)]),
    ];

    // Stage one, delivered as three raw chunks so that it streams rather than evaluating at once.
    let mut stage_one = RangeEvaluator::try_new(
        &raw_schema(1),
        inner.clone(),
        inner_window,
        RangeFunction::SumOverTime,
    )
    .unwrap();
    let mut upstream: Vec<EvaluatedChunk> = Vec::new();
    let mut start = 0;
    for end in [2, 4, rows.len()] {
        let piece = chunk(&rows[start..end], 1, 0, end == rows.len());
        upstream.extend(stage_one.push(&piece).unwrap());
        start = end;
    }
    let first = fold_chunks(&upstream, stage_one.output_schema()).expect("stage one completed");
    let dense_one = dense_sum_over_time(&rows, &inner, inner_window);
    assert_eq!(
        expand(&first),
        dense_one,
        "stage one disagrees with its reference"
    );
    stage_one.finish().unwrap();
    assert!(
        upstream.len() >= 2,
        "stage one handed its rows out in several chunks, got {}",
        upstream.len()
    );

    // Stage two composed: every chunk of stage one is a delivery, the last one closing.
    let input_schema = evaluated_schema(&inner);
    let mut composed = RangeEvaluator::try_new(
        &input_schema,
        outer.clone(),
        outer_window,
        RangeFunction::SumOverTime,
    )
    .unwrap();
    let last_index = upstream.len() - 1;
    let mut chunks: Vec<EvaluatedChunk> = Vec::new();
    for (index, chunk) in upstream.iter().enumerate() {
        chunks.extend(
            composed
                .push_series(&chunk.series, index == last_index)
                .unwrap(),
        );
    }
    let streamed = fold_chunks(&chunks, composed.output_schema()).expect("the series completed");
    composed.finish().unwrap();

    // The one-piece form: stage one's result handed to a fresh stage two in a single delivery.
    let mut one_piece = RangeEvaluator::try_new(
        &input_schema,
        outer.clone(),
        outer_window,
        RangeFunction::SumOverTime,
    )
    .unwrap();
    let single = fold_chunks(
        &evaluate_series(&mut one_piece, &first),
        one_piece.output_schema(),
    )
    .expect("the series completed");
    one_piece.finish().unwrap();

    // The dense reference of both stages: stage one's rows, summed again around each outer point.
    let stage_one_rows: Vec<Row> = dense_one
        .iter()
        .enumerate()
        .filter_map(|(index, row)| {
            row[0].map(|value| (inner.timestamps()[index], vec![Some(value)]))
        })
        .collect();
    let expected = dense_sum_over_time(&stage_one_rows, &outer, outer_window);
    assert_eq!(
        expand(&streamed),
        expected,
        "the composed pipeline disagrees with the dense reference"
    );
    assert_eq!(
        expand(&streamed),
        expand(&single),
        "chunking stage one changed the composed answer"
    );
    assert_eq!(
        expand(&streamed)
            .iter()
            .filter(|row| row[0].is_some())
            .count(),
        expected.iter().filter(|row| row[0].is_some()).count(),
        "the composition lost or invented a value"
    );
}
