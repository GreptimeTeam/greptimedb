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

use std::ops::Range;
use std::sync::Arc;

use common_query::prometheus::PROMETHEUS_STALE_NAN_BITS;
use datafusion::arrow::array::{
    ArrayRef, Float64Array, Int64Array, TimestampMillisecondArray, UInt32Array,
};
use datafusion::arrow::buffer::{Buffer, ScalarBuffer};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ColumnarValue;
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::ScalarFunctionArgs;
use datatypes::series::histogram::{
    BucketSideView, CounterResetHint, HistogramBatchBuilder, HistogramView,
};
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{FieldSpec, Samples, Series, SeriesFields, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridSpec};
use datatypes::series::{Label, MetricName};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::{FloatWindow, RangeEvaluator, RangeFunction, RangeWindow, WindowKernel, subquery_grid};
use crate::functions::{
    AvgOverTime, Changes, CountOverTime, Delta, Deriv, DoubleExponentialSmoothing, IDelta,
    Increase, LastOverTime, MaxOverTime, MinOverTime, PredictLinear, PresentOverTime,
    QuantileOverTime, Rate, Resets, StddevOverTime, StdvarOverTime, SumOverTime,
};
use crate::grid_series::GridSeriesBuilder;
use crate::grid_series::reference::{Dense, assert_series_eq, random_dense, special_value};
use crate::range_array::RangeArray;

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
    Some((hash >> 11) as f64)
}

fn sum(window: FloatWindow<'_>) -> Option<f64> {
    Some(window.values.iter().sum())
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
    RangeEvaluator::with_kernel(&raw_schema(fields), grid, window, kernel, true).unwrap()
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

/// Applies the probe to the valid samples of every reference window.
fn reference(
    rows: &[Row],
    grid: &EvalGrid,
    window: RangeWindow,
    fields: usize,
    drop_stale: bool,
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
                    (!timestamps.is_empty()).then(|| {
                        probe(FloatWindow {
                            timestamps: &timestamps,
                            values: &values,
                            eval_ts: window_end(grid, window, eval_ts),
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
/// `RangeManipulate` produces: shifted timestamps and nullable values per field.
///
/// Some UDFs depend on where null slots sit, which the evaluator does not keep, so they get only
/// the field's valid rows: Arrow's lane-wise sum in `sum_over_time` and `avg_over_time`, the
/// first slot's timestamp in `deriv`, and the slot payloads `double_exponential_smoothing` reads.
/// `double_exponential_smoothing` also returns NaN for an empty window, where the evaluator
/// follows Prometheus and leaves the point missing.
fn udf_reference(
    function: RangeFunction,
    rows: &[Row],
    grid: &EvalGrid,
    window: RangeWindow,
    fields: usize,
    drop_stale: bool,
) -> Dense {
    let (kept, _) = reference_windows(rows, grid, window, drop_stale);
    let mut dense = vec![vec![None; fields]; grid.timestamps().len()];
    for field in 0..fields {
        let rows: Vec<_> = if matches!(
            function,
            RangeFunction::SumOverTime
                | RangeFunction::AvgOverTime
                | RangeFunction::Deriv
                | RangeFunction::DoubleExponentialSmoothing { .. }
        ) {
            kept.iter()
                .filter(|(_, values)| values[field].is_some())
                .cloned()
                .collect()
        } else {
            kept.clone()
        };
        let ranges: Vec<_> = windows_of(&rows, grid, window)
            .iter()
            .map(|rows| (rows.start as u32, rows.len() as u32))
            .collect();
        let timestamps: ArrayRef = Arc::new(TimestampMillisecondArray::from_iter_values(
            rows.iter().map(|(timestamp, _)| *timestamp),
        ));
        let values: ArrayRef = Arc::new(Float64Array::from_iter(
            rows.iter().map(|(_, values)| values[field]),
        ));
        let mut args = vec![
            ColumnarValue::Array(Arc::new(
                RangeArray::from_ranges(timestamps, ranges.iter().copied())
                    .unwrap()
                    .into_dict(),
            )),
            ColumnarValue::Array(Arc::new(
                RangeArray::from_ranges(values, ranges.iter().copied())
                    .unwrap()
                    .into_dict(),
            )),
        ];
        let udf = match function {
            RangeFunction::Rate => Rate::scalar_udf(),
            RangeFunction::Increase => Increase::scalar_udf(),
            RangeFunction::Delta => Delta::scalar_udf(),
            RangeFunction::Irate => IDelta::<true>::scalar_udf(),
            RangeFunction::Idelta => IDelta::<false>::scalar_udf(),
            RangeFunction::AvgOverTime => AvgOverTime::scalar_udf(),
            RangeFunction::MinOverTime => MinOverTime::scalar_udf(),
            RangeFunction::MaxOverTime => MaxOverTime::scalar_udf(),
            RangeFunction::SumOverTime => SumOverTime::scalar_udf(),
            RangeFunction::CountOverTime => CountOverTime::scalar_udf(),
            RangeFunction::LastOverTime => LastOverTime::scalar_udf(),
            RangeFunction::PresentOverTime => PresentOverTime::scalar_udf(),
            RangeFunction::StddevOverTime => StddevOverTime::scalar_udf(),
            RangeFunction::StdvarOverTime => StdvarOverTime::scalar_udf(),
            RangeFunction::QuantileOverTime(_) => QuantileOverTime::scalar_udf(),
            RangeFunction::Resets => Resets::scalar_udf(),
            RangeFunction::Changes => Changes::scalar_udf(),
            RangeFunction::Deriv => Deriv::scalar_udf(),
            RangeFunction::PredictLinear(_) => PredictLinear::scalar_udf(),
            RangeFunction::DoubleExponentialSmoothing { .. } => {
                DoubleExponentialSmoothing::scalar_udf()
            }
        };
        match function {
            RangeFunction::Rate | RangeFunction::Increase | RangeFunction::Delta => {
                // An anchored call is evaluated once, at the start, then replayed.
                args.push(ColumnarValue::Array(Arc::new(
                    TimestampMillisecondArray::from_iter_values(
                        grid.timestamps()
                            .iter()
                            .map(|&eval_ts| window_end(grid, window, eval_ts)),
                    ),
                )));
                args.push(ColumnarValue::Array(Arc::new(Int64Array::from(vec![
                    window.range_ms,
                ]))));
            }
            RangeFunction::QuantileOverTime(quantile) => {
                args.push(ColumnarValue::Scalar(ScalarValue::Float64(Some(quantile))));
            }
            RangeFunction::PredictLinear(horizon) => {
                // The planner passes the evaluation time plus the fold offset.
                args.push(ColumnarValue::Scalar(ScalarValue::Int64(Some(horizon))));
                args.push(ColumnarValue::Array(Arc::new(
                    TimestampMillisecondArray::from_iter_values(
                        grid.timestamps()
                            .iter()
                            .map(|eval_ts| eval_ts + fold_offset(grid, window)),
                    ),
                )));
            }
            RangeFunction::DoubleExponentialSmoothing { smoothing, trend } => {
                args.push(ColumnarValue::Scalar(ScalarValue::Float64(Some(smoothing))));
                args.push(ColumnarValue::Scalar(ScalarValue::Float64(Some(trend))));
            }
            _ => {}
        }
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
        for ((row, value), (_, len)) in dense.iter_mut().zip(output.iter()).zip(&ranges) {
            row[field] = value.filter(|_| *len > 0);
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
        // One evaluator serves several series, so state must not leak between them.
        let mut evaluator = probe_evaluator(fields, grid.clone(), window);
        for host in 0..3 {
            let rows = random_rows(&mut rng, fields);
            let expected = reference(&rows, &grid, window, fields, true);
            let mut result = None;
            for piece in random_chunks(&mut rng, &rows, fields, host) {
                assert!(
                    result.is_none(),
                    "seed {seed}: result before the last chunk"
                );
                result = evaluator.push(&piece).unwrap();
            }
            assert_series_eq(result.as_ref(), &expected, &metric(host));
        }
        evaluator.finish().unwrap();
    }
}

/// Every function, with parameters inside, at and outside their valid ranges.
fn all_functions() -> Vec<RangeFunction> {
    vec![
        RangeFunction::Rate,
        RangeFunction::Increase,
        RangeFunction::Delta,
        RangeFunction::Irate,
        RangeFunction::Idelta,
        RangeFunction::AvgOverTime,
        RangeFunction::MinOverTime,
        RangeFunction::MaxOverTime,
        RangeFunction::SumOverTime,
        RangeFunction::CountOverTime,
        RangeFunction::LastOverTime,
        RangeFunction::PresentOverTime,
        RangeFunction::StddevOverTime,
        RangeFunction::StdvarOverTime,
        RangeFunction::QuantileOverTime(-0.5),
        RangeFunction::QuantileOverTime(0.0),
        RangeFunction::QuantileOverTime(0.9),
        RangeFunction::QuantileOverTime(1.0),
        RangeFunction::QuantileOverTime(1.5),
        RangeFunction::QuantileOverTime(f64::NAN),
        RangeFunction::Resets,
        RangeFunction::Changes,
        RangeFunction::Deriv,
        RangeFunction::PredictLinear(-600),
        RangeFunction::PredictLinear(0),
        RangeFunction::PredictLinear(3_600),
        RangeFunction::DoubleExponentialSmoothing {
            smoothing: 0.5,
            trend: 0.1,
        },
        RangeFunction::DoubleExponentialSmoothing {
            smoothing: 0.9,
            trend: 0.6,
        },
        RangeFunction::DoubleExponentialSmoothing {
            smoothing: 1.5,
            trend: 0.1,
        },
        RangeFunction::DoubleExponentialSmoothing {
            smoothing: 0.5,
            trend: -0.1,
        },
        RangeFunction::DoubleExponentialSmoothing {
            smoothing: f64::NAN,
            trend: 0.1,
        },
    ]
}

#[test]
fn functions_match_range_udfs() {
    for function in all_functions() {
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
                let mut result = None;
                for piece in random_chunks(&mut rng, &rows, fields, host) {
                    result = evaluator.push(&piece).unwrap();
                }
                assert_series_eq(result.as_ref(), &expected, &metric(host));
            }
        }
    }
}

#[test]
fn overlapping_window_spans_chunks() {
    let window = RangeWindow {
        range_ms: 15,
        offset_ms: 0,
        anchor_ms: None,
    };
    let kernel: WindowKernel = Box::new(sum);
    let mut evaluator =
        RangeEvaluator::with_kernel(&raw_schema(1), grid(10, 10, 3), window, kernel, true).unwrap();
    let first = chunk(&[(5, vec![Some(1.0)]), (12, vec![Some(2.0)])], 1, 0, false);
    let second = chunk(&[(18, vec![Some(4.0)]), (25, vec![Some(8.0)])], 1, 0, true);
    assert!(evaluator.push(&first).unwrap().is_none());
    let series = evaluator.push(&second).unwrap();
    // Windows (-5, 10], (5, 20] and (15, 30].
    let expected = vec![vec![Some(1.0)], vec![Some(6.0)], vec![Some(12.0)]];
    assert_series_eq(series.as_ref(), &expected, &metric(0));
}

#[test]
fn empty_input_and_series_without_results() {
    let window = RangeWindow {
        range_ms: 10,
        offset_ms: 0,
        anchor_ms: None,
    };
    probe_evaluator(1, grid(0, 10, 4), window).finish().unwrap();

    let mut evaluator = probe_evaluator(1, grid(0, 10, 4), window);
    let after_grid = chunk(&[(31, vec![Some(1.0)])], 1, 0, true);
    assert!(evaluator.push(&after_grid).unwrap().is_none());
    let only_stale = chunk(&[(5, vec![Some(stale())])], 1, 1, true);
    assert!(evaluator.push(&only_stale).unwrap().is_none());
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
            RangeFunction::PredictLinear(0)
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
fn rejects_histogram_samples() {
    let empty = BucketSideView {
        span_offsets: &[],
        span_lengths: &[],
        buckets: &[],
    };
    let mut builder = HistogramBatchBuilder::new();
    builder
        .push(HistogramView {
            schema: 0,
            zero_threshold: 0.0,
            sum: 1.0,
            count: 1.0,
            zero_count: 1.0,
            reset_hint: CounterResetHint::Unknown,
            start_timestamp: None,
            custom_values: &[],
            positive: empty,
            negative: empty,
        })
        .unwrap();
    let histograms =
        Samples::try_from_histogram(UInt32Array::from(vec![0]), builder.finish().unwrap()).unwrap();
    let mut piece = chunk(&[(1, vec![Some(1.0)])], 1, 0, true);
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
        let mut probe =
            RangeEvaluator::with_kernel(&schema, outer.clone(), window, Box::new(probe), true)
                .unwrap();
        let expected = reference(&rows, &outer, window, fields, false);
        assert_series_eq(
            probe.push_series(&series).unwrap().as_ref(),
            &expected,
            &metric(seed),
        );
        for function in all_functions() {
            let mut evaluator =
                RangeEvaluator::try_new(&schema, outer.clone(), window, function).unwrap();
            let expected = udf_reference(function, &rows, &outer, window, fields, false);
            assert_series_eq(
                evaluator.push_series(&series).unwrap().as_ref(),
                &expected,
                &metric(seed),
            );
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
    execution_error(probe_evaluator(1, grid(0, 10, 4), window).push_series(&series));
}
