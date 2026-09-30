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

//! Benchmarks for PromQL range functions.

use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, criterion_group};
use datafusion::arrow::array::{Array, Float64Array, TimestampMillisecondArray};
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::physical_plan::ColumnarValue;
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::ScalarFunctionArgs;
use datatypes::arrow::datatypes::{DataType, Field};
use promql::extension_plan::RangeManipulateStream;
use promql::functions::{
    AbsentOverTime, Changes, CountOverTime, Delta, DoubleExponentialSmoothing, IDelta, Increase,
    LastOverTime, MaxOverTime, MinOverTime, PredictLinear, PresentOverTime, QuantileOverTime, Rate,
    Resets, SumOverTime,
};
use promql::range_array::RangeArray;

/// A `window_step` below `window_size` makes consecutive windows overlap, which is the normal
/// PromQL range query shape.
fn build_sliding_ranges(
    num_points: usize,
    window_size: u32,
    window_step: usize,
    values: Vec<f64>,
    eval_offset_ms: i64,
) -> (RangeArray, RangeArray, Arc<TimestampMillisecondArray>) {
    let step_ms = 1000i64;
    let timestamps: Vec<i64> = (0..num_points as i64).map(|i| (i + 1) * step_ms).collect();

    let ts_array = Arc::new(TimestampMillisecondArray::from(timestamps.clone()));
    let val_array = Arc::new(Float64Array::from(values));

    let num_windows = if num_points >= window_size as usize {
        num_points - window_size as usize + 1
    } else {
        0
    };

    let offsets: Vec<usize> = (0..num_windows).step_by(window_step).collect();
    let ranges: Vec<(u32, u32)> = offsets.iter().map(|&i| (i as u32, window_size)).collect();

    let eval_ts: Vec<i64> = offsets
        .iter()
        .map(|&i| timestamps[i + window_size as usize - 1] + eval_offset_ms)
        .collect();
    let eval_ts_array = Arc::new(TimestampMillisecondArray::from(eval_ts));

    let ts_range = RangeArray::from_ranges(ts_array, ranges.clone()).unwrap();
    let val_range = RangeArray::from_ranges(val_array, ranges).unwrap();

    (ts_range, val_range, eval_ts_array)
}

fn build_monotonic_counter_values(num_points: usize) -> Vec<f64> {
    let mut current = 0.0;
    (0..num_points)
        .map(|i| {
            current += 1.0 + (i % 7) as f64 * 0.25;
            current
        })
        .collect()
}

fn build_resetting_counter_values(num_points: usize) -> Vec<f64> {
    let mut current = 0.0;
    (0..num_points)
        .map(|i| {
            if i > 0 && i % 37 == 0 {
                current = 1.0;
            } else {
                current += 1.0 + (i % 5) as f64 * 0.5;
            }
            current
        })
        .collect()
}

fn build_gauge_values(num_points: usize) -> Vec<f64> {
    (0..num_points)
        .map(|i| ((i % 29) as f64 - 14.0) * 1.25 + (i % 3) as f64 * 0.1)
        .collect()
}

fn build_default_values(num_points: usize) -> Vec<f64> {
    (0..num_points).map(|i| i as f64 * 1.5 + 0.1).collect()
}

fn build_changing_values(num_points: usize) -> Vec<f64> {
    (0..num_points)
        .map(|i| match i % 48 {
            0..=7 => 42.0,
            8..=15 => 43.5,
            16..=17 => f64::NAN,
            18..=35 => 41.0,
            _ => 44.0,
        })
        .collect()
}

fn make_extrapolated_rate_input(
    num_points: usize,
    window_size: u32,
    window_step: usize,
    values: Vec<f64>,
    eval_offset_ms: i64,
) -> Vec<ColumnarValue> {
    let (ts_range, val_range, eval_ts) =
        build_sliding_ranges(num_points, window_size, window_step, values, eval_offset_ms);
    let range_length = window_size as i64 * 1000;
    vec![
        ColumnarValue::Array(Arc::new(ts_range.into_dict())),
        ColumnarValue::Array(Arc::new(val_range.into_dict())),
        ColumnarValue::Array(eval_ts),
        ColumnarValue::Scalar(ScalarValue::Int64(Some(range_length))),
    ]
}

fn make_delta_rate_comparison_input(
    series_count: usize,
    hours: usize,
    sample_step_seconds: usize,
    query_step_seconds: usize,
    window_seconds: usize,
) -> (Vec<ColumnarValue>, Vec<ColumnarValue>) {
    let points_per_series = hours * 60 * 60 / sample_step_seconds;
    let window_points = window_seconds / sample_step_seconds;
    let query_stride = query_step_seconds / sample_step_seconds;
    let mut timestamps = Vec::with_capacity(series_count * points_per_series);
    let mut deltas = Vec::with_capacity(timestamps.capacity());
    let mut cumulative = Vec::with_capacity(timestamps.capacity());
    let mut ranges = Vec::new();
    let mut eval_timestamps = Vec::new();

    for _ in 0..series_count {
        let offset = timestamps.len();
        let mut total = 0.0;
        for point in 0..points_per_series {
            let delta = 1.0 + (point % 7) as f64 * 0.25;
            total += delta;
            timestamps.push((point as i64 + 1) * sample_step_seconds as i64 * 1_000);
            deltas.push(delta);
            cumulative.push(total);
        }
        for end in (window_points - 1..points_per_series).step_by(query_stride) {
            ranges.push((
                (offset + end + 1 - window_points) as u32,
                window_points as u32,
            ));
            eval_timestamps.push(timestamps[offset + end] + 500);
        }
    }

    let timestamps = Arc::new(TimestampMillisecondArray::from(timestamps));
    let delta_timestamp_ranges =
        RangeArray::from_ranges(timestamps.clone(), ranges.clone()).unwrap();
    let cumulative_timestamp_ranges = RangeArray::from_ranges(timestamps, ranges.clone()).unwrap();
    let delta_ranges =
        RangeArray::from_ranges(Arc::new(Float64Array::from(deltas)), ranges.clone()).unwrap();
    let cumulative_ranges =
        RangeArray::from_ranges(Arc::new(Float64Array::from(cumulative)), ranges).unwrap();
    let delta = vec![
        ColumnarValue::Array(Arc::new(delta_timestamp_ranges.into_dict())),
        ColumnarValue::Array(Arc::new(delta_ranges.into_dict())),
    ];
    let cumulative = vec![
        ColumnarValue::Array(Arc::new(cumulative_timestamp_ranges.into_dict())),
        ColumnarValue::Array(Arc::new(cumulative_ranges.into_dict())),
        ColumnarValue::Array(Arc::new(TimestampMillisecondArray::from(eval_timestamps))),
        ColumnarValue::Scalar(ScalarValue::Int64(Some(window_seconds as i64 * 1_000))),
    ];
    (delta, cumulative)
}

fn make_idelta_input(num_points: usize, window_size: u32) -> Vec<ColumnarValue> {
    let (ts_range, val_range, _) = build_sliding_ranges(
        num_points,
        window_size,
        1,
        build_default_values(num_points),
        0,
    );
    vec![
        ColumnarValue::Array(Arc::new(ts_range.into_dict())),
        ColumnarValue::Array(Arc::new(val_range.into_dict())),
    ]
}

fn make_edge_count_input(
    num_points: usize,
    window_size: u32,
    values: Vec<f64>,
) -> Vec<ColumnarValue> {
    let (ts_range, val_range, _) = build_sliding_ranges(num_points, window_size, 1, values, 0);
    vec![
        ColumnarValue::Array(Arc::new(ts_range.into_dict())),
        ColumnarValue::Array(Arc::new(val_range.into_dict())),
    ]
}

fn make_edge_count_input_with_ranges(
    values: Vec<f64>,
    ranges: Vec<(u32, u32)>,
) -> Vec<ColumnarValue> {
    let timestamps = Arc::new(TimestampMillisecondArray::from_iter_values(
        (0..values.len()).map(|index| index as i64 * 1_000),
    ));
    let values = Arc::new(Float64Array::from(values));
    let timestamp_ranges = RangeArray::from_ranges(timestamps, ranges.clone()).unwrap();
    let value_ranges = RangeArray::from_ranges(values, ranges).unwrap();
    vec![
        ColumnarValue::Array(Arc::new(timestamp_ranges.into_dict())),
        ColumnarValue::Array(Arc::new(value_ranges.into_dict())),
    ]
}

fn make_extrema_input_with_ranges(values: Vec<f64>, ranges: Vec<(u32, u32)>) -> Vec<ColumnarValue> {
    let timestamps = Arc::new(TimestampMillisecondArray::from_iter_values(
        (0..values.len()).map(|index| index as i64 * 1_000),
    ));
    let values = Arc::new(Float64Array::from(values));
    let timestamp_ranges = RangeArray::from_ranges(timestamps, ranges.clone()).unwrap();
    let value_ranges = RangeArray::from_ranges(values, ranges).unwrap();
    vec![
        ColumnarValue::Array(Arc::new(timestamp_ranges.into_dict())),
        ColumnarValue::Array(Arc::new(value_ranges.into_dict())),
    ]
}

fn make_quantile_input(num_points: usize, window_size: u32) -> Vec<ColumnarValue> {
    let (ts_range, val_range, _) = build_sliding_ranges(
        num_points,
        window_size,
        1,
        build_default_values(num_points),
        0,
    );
    vec![
        ColumnarValue::Array(Arc::new(ts_range.into_dict())),
        ColumnarValue::Array(Arc::new(val_range.into_dict())),
        ColumnarValue::Scalar(ScalarValue::Float64(Some(0.9))),
    ]
}

fn make_predict_linear_input(num_points: usize, window_size: u32) -> Vec<ColumnarValue> {
    let (ts_range, val_range, _) = build_sliding_ranges(
        num_points,
        window_size,
        1,
        build_default_values(num_points),
        0,
    );
    vec![
        ColumnarValue::Array(Arc::new(ts_range.into_dict())),
        ColumnarValue::Array(Arc::new(val_range.into_dict())),
        // predict 60s into the future
        ColumnarValue::Scalar(ScalarValue::Int64(Some(60))),
    ]
}

fn make_double_exponential_smoothing_input(
    num_points: usize,
    window_size: u32,
    window_step: usize,
) -> Vec<ColumnarValue> {
    let (ts_range, val_range, _) = build_sliding_ranges(
        num_points,
        window_size,
        window_step,
        build_gauge_values(num_points),
        0,
    );
    vec![
        ColumnarValue::Array(Arc::new(ts_range.into_dict())),
        ColumnarValue::Array(Arc::new(val_range.into_dict())),
        ColumnarValue::Scalar(ScalarValue::Float64(Some(0.5))),
        ColumnarValue::Scalar(ScalarValue::Float64(Some(0.1))),
    ]
}

struct PreparedUdfCall {
    args: Vec<ColumnarValue>,
    arg_fields: Vec<Arc<Field>>,
    number_rows: usize,
    return_field: Arc<Field>,
    config_options: Arc<ConfigOptions>,
}

impl PreparedUdfCall {
    fn new(args: Vec<ColumnarValue>) -> Self {
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(i, c)| Arc::new(Field::new(format!("c{i}"), c.data_type(), true)))
            .collect();
        let number_rows = args
            .iter()
            .find_map(|c| match c {
                ColumnarValue::Array(a) => Some(a.len()),
                _ => None,
            })
            .unwrap_or(1);
        Self {
            args,
            arg_fields,
            number_rows,
            return_field: Arc::new(Field::new("out", DataType::Float64, true)),
            config_options: Arc::new(ConfigOptions::default()),
        }
    }
}

fn invoke_prepared_output(
    udf: &datafusion::logical_expr::ScalarUDF,
    prepared: &PreparedUdfCall,
) -> ColumnarValue {
    udf.invoke_with_args(ScalarFunctionArgs {
        args: prepared.args.clone(),
        arg_fields: prepared.arg_fields.clone(),
        number_rows: prepared.number_rows,
        return_field: prepared.return_field.clone(),
        config_options: prepared.config_options.clone(),
    })
    .unwrap()
}

fn invoke_prepared(udf: &datafusion::logical_expr::ScalarUDF, prepared: &PreparedUdfCall) {
    let _ = invoke_prepared_output(udf, prepared);
}

fn edge_count_oracle(
    values: &[f64],
    ranges: &[(u32, u32)],
    predicate: impl Fn(f64, f64) -> bool,
) -> Vec<Option<f64>> {
    ranges
        .iter()
        .map(|(offset, length)| {
            let window = &values[*offset as usize..(*offset + *length) as usize];
            if window.is_empty() {
                None
            } else if window.len() == 1 {
                Some(0.0)
            } else {
                Some(
                    window
                        .windows(2)
                        .filter(|pair| predicate(pair[0], pair[1]))
                        .count() as f64,
                )
            }
        })
        .collect()
}

fn assert_edge_count_output(
    udf: &datafusion::logical_expr::ScalarUDF,
    prepared: &PreparedUdfCall,
    expected: &[Option<f64>],
) {
    let output = invoke_prepared_output(udf, prepared);
    let ColumnarValue::Array(output) = output else {
        panic!("edge-count range UDF must return an array");
    };
    let output = output.as_any().downcast_ref::<Float64Array>().unwrap();
    let actual = output.iter().collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

fn bench_presence_range_functions(c: &mut Criterion) {
    let mut group = c.benchmark_group("presence_range_fn");
    let values = build_default_values(4_096);
    let overlapping = PreparedUdfCall::new(make_edge_count_input(4_096, 20, values.clone()));
    let low_coverage_ranges = vec![
        (0, 4),
        (512, 4),
        (1_024, 4),
        (1_536, 4),
        (2_048, 4),
        (2_560, 4),
        (3_584, 4),
        (4_092, 4),
    ];
    let low_coverage = PreparedUdfCall::new(make_edge_count_input_with_ranges(
        values,
        low_coverage_ranges,
    ));
    let udfs = [
        ("count_over_time", CountOverTime::scalar_udf()),
        ("last_over_time", LastOverTime::scalar_udf()),
        ("present_over_time", PresentOverTime::scalar_udf()),
        ("absent_over_time", AbsentOverTime::scalar_udf()),
    ];

    for (name, udf) in &udfs {
        group.bench_with_input(BenchmarkId::new(*name, "N4096_overlap_w20"), &(), |b, _| {
            b.iter(|| invoke_prepared(udf, &overlapping))
        });
        group.bench_with_input(BenchmarkId::new(*name, "N4096_windows8_w4"), &(), |b, _| {
            b.iter(|| invoke_prepared(udf, &low_coverage))
        });
    }

    group.finish();
}

fn extrema_oracle(
    values: &[f64],
    ranges: &[(u32, u32)],
    is_better: impl Fn(f64, f64) -> bool,
) -> Vec<Option<f64>> {
    ranges
        .iter()
        .map(|(offset, length)| {
            let window = &values[*offset as usize..(*offset + *length) as usize];
            let mut extrema = *window.first()?;
            for value in &window[1..] {
                if is_better(*value, extrema) || extrema.is_nan() {
                    extrema = *value;
                }
            }
            Some(extrema)
        })
        .collect()
}

fn assert_extrema_output(
    udf: &datafusion::logical_expr::ScalarUDF,
    prepared: &PreparedUdfCall,
    expected: &[Option<f64>],
) {
    let output = invoke_prepared_output(udf, prepared);
    let ColumnarValue::Array(output) = output else {
        panic!("extrema range UDF must return an array");
    };
    let output = output.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(output.len(), expected.len());
    for (actual, expected) in output.iter().zip(expected) {
        match (actual, expected) {
            (Some(actual), Some(expected)) => assert_eq!(actual.to_bits(), expected.to_bits()),
            (None, None) => {}
            (actual, expected) => panic!("expected {expected:?}, got {actual:?}"),
        }
    }
}

fn bench_range_functions(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_fn");

    // Benchmark parameters: (total_points, window_size)
    let params: &[(usize, u32)] = &[
        (1_000, 10),   // small series, small window
        (10_000, 10),  // large series, small window
        (10_000, 60),  // large series, typical 1-min window at 1s step
        (10_000, 360), // large series, wide 6-min window
    ];

    // --- rate (monotonic counter) ---
    let rate_udf = Rate::scalar_udf();
    for &(n, w) in params {
        let prepared = PreparedUdfCall::new(make_extrapolated_rate_input(
            n,
            w,
            1,
            build_monotonic_counter_values(n),
            500,
        ));
        group.bench_with_input(
            BenchmarkId::new("rate_counter", format!("n{n}_w{w}")),
            &(n, w),
            |b, _| b.iter(|| invoke_prepared(&rate_udf, &prepared)),
        );
    }

    // --- rate (periodic resets) ---
    for &(n, w) in params {
        let prepared = PreparedUdfCall::new(make_extrapolated_rate_input(
            n,
            w,
            1,
            build_resetting_counter_values(n),
            500,
        ));
        group.bench_with_input(
            BenchmarkId::new("rate_counter_reset", format!("n{n}_w{w}")),
            &(n, w),
            |b, _| b.iter(|| invoke_prepared(&rate_udf, &prepared)),
        );
    }

    // --- increase (monotonic counter) ---
    let increase_udf = Increase::scalar_udf();
    for &(n, w) in params {
        let prepared = PreparedUdfCall::new(make_extrapolated_rate_input(
            n,
            w,
            1,
            build_monotonic_counter_values(n),
            500,
        ));
        group.bench_with_input(
            BenchmarkId::new("increase_counter", format!("n{n}_w{w}")),
            &(n, w),
            |b, _| b.iter(|| invoke_prepared(&increase_udf, &prepared)),
        );
    }

    // --- increase (periodic resets) ---
    for &(n, w) in params {
        let prepared = PreparedUdfCall::new(make_extrapolated_rate_input(
            n,
            w,
            1,
            build_resetting_counter_values(n),
            500,
        ));
        group.bench_with_input(
            BenchmarkId::new("increase_counter_reset", format!("n{n}_w{w}")),
            &(n, w),
            |b, _| b.iter(|| invoke_prepared(&increase_udf, &prepared)),
        );
    }

    // --- delta (gauge) ---
    let delta_udf = Delta::scalar_udf();
    for &(n, w) in params {
        let prepared = PreparedUdfCall::new(make_extrapolated_rate_input(
            n,
            w,
            1,
            build_gauge_values(n),
            500,
        ));
        group.bench_with_input(
            BenchmarkId::new("delta_gauge", format!("n{n}_w{w}")),
            &(n, w),
            |b, _| b.iter(|| invoke_prepared(&delta_udf, &prepared)),
        );
    }

    // --- idelta ---
    let idelta_udf = IDelta::<false>::scalar_udf();
    for &(n, w) in params {
        let prepared = PreparedUdfCall::new(make_idelta_input(n, w));
        group.bench_with_input(
            BenchmarkId::new("idelta", format!("n{n}_w{w}")),
            &(n, w),
            |b, _| b.iter(|| invoke_prepared(&idelta_udf, &prepared)),
        );
    }

    // --- irate ---
    let irate_udf = IDelta::<true>::scalar_udf();
    for &(n, w) in params {
        let prepared = PreparedUdfCall::new(make_idelta_input(n, w));
        group.bench_with_input(
            BenchmarkId::new("irate", format!("n{n}_w{w}")),
            &(n, w),
            |b, _| b.iter(|| invoke_prepared(&irate_udf, &prepared)),
        );
    }

    // --- quantile_over_time ---
    let quantile_udf = QuantileOverTime::scalar_udf();
    for &(n, w) in params {
        let prepared = PreparedUdfCall::new(make_quantile_input(n, w));
        group.bench_with_input(
            BenchmarkId::new("quantile_over_time", format!("n{n}_w{w}")),
            &(n, w),
            |b, _| b.iter(|| invoke_prepared(&quantile_udf, &prepared)),
        );
    }

    // --- predict_linear ---
    let predict_udf = PredictLinear::scalar_udf();
    for &(n, w) in params {
        let prepared = PreparedUdfCall::new(make_predict_linear_input(n, w));
        group.bench_with_input(
            BenchmarkId::new("predict_linear", format!("n{n}_w{w}")),
            &(n, w),
            |b, _| b.iter(|| invoke_prepared(&predict_udf, &prepared)),
        );
    }

    // --- double_exponential_smoothing ---
    let smoothing_udf = DoubleExponentialSmoothing::scalar_udf();
    for (window_size, window_step, case) in [
        (4, 1, "N4096_w4_overlap"),
        (20, 1, "N4096_w20_overlap"),
        (240, 1, "N4096_w240_overlap"),
        (240, 240, "N4096_w240_nonoverlap"),
    ] {
        let prepared = PreparedUdfCall::new(make_double_exponential_smoothing_input(
            4_096,
            window_size,
            window_step,
        ));
        group.bench_with_input(
            BenchmarkId::new("double_exponential_smoothing", case),
            &(),
            |b, _| b.iter(|| invoke_prepared(&smoothing_udf, &prepared)),
        );
    }

    // --- RangeArray: get vs get_offset_length micro-benchmark ---
    // Isolates the overhead of array slicing vs offset/length lookup
    for &(n, w) in params {
        let step_ms = 1000i64;
        let timestamps: Vec<i64> = (0..n as i64).map(|i| (i + 1) * step_ms).collect();
        let ts_array = Arc::new(TimestampMillisecondArray::from(timestamps));
        let num_windows = n - w as usize + 1;
        let ranges: Vec<(u32, u32)> = (0..num_windows).map(|i| (i as u32, w)).collect();
        let range_array = RangeArray::from_ranges(ts_array, ranges).unwrap();

        group.bench_with_input(
            BenchmarkId::new("range_array_get", format!("n{n}_w{w}")),
            &(),
            |b, _| {
                b.iter(|| {
                    for i in 0..range_array.len() {
                        std::hint::black_box(range_array.get(i));
                    }
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("range_array_get_offset_length", format!("n{n}_w{w}")),
            &(),
            |b, _| {
                b.iter(|| {
                    for i in 0..range_array.len() {
                        std::hint::black_box(range_array.get_offset_length(i));
                    }
                })
            },
        );
    }

    group.finish();
}

fn bench_delta_rate_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("delta_rate_comparison");
    let series_count = 64;
    let hours = 4;
    let sample_step_seconds = 15;
    let window_seconds = 2 * 60 * 60;
    let delta_udf = SumOverTime::scalar_udf();
    let cumulative_udf = Rate::scalar_udf();

    // Release acceptance threshold: on both step sweeps, the sum reducer that
    // dominates delta-rate cost should stay below 100 ms and within 100x of
    // cumulative rate on this 64-series, four-hour data set. Optimize the
    // reducer before release if either bound is exceeded on a typical CI host.
    for query_step_seconds in [60, 300] {
        let (delta, cumulative) = make_delta_rate_comparison_input(
            series_count,
            hours,
            sample_step_seconds,
            query_step_seconds,
            window_seconds,
        );
        let delta = PreparedUdfCall::new(delta);
        let cumulative = PreparedUdfCall::new(cumulative);
        let parameters = format!(
            "series{series_count}_hours{hours}_window{}h_step{}s",
            window_seconds / 60 / 60,
            query_step_seconds
        );
        group.bench_with_input(
            BenchmarkId::new("delta_sum_over_time", &parameters),
            &(),
            |b, _| b.iter(|| invoke_prepared(&delta_udf, &delta)),
        );
        group.bench_with_input(
            BenchmarkId::new("cumulative_rate", &parameters),
            &(),
            |b, _| b.iter(|| invoke_prepared(&cumulative_udf, &cumulative)),
        );
    }

    group.finish();
}

/// Counter-reset correction is reduced per window, so its cost follows how much the windows
/// overlap and how many resets each one covers. `range_fn` fixes the query step at one sample
/// and `delta_rate_comparison` at four and twenty, so sweep both dimensions here.
fn bench_rate_window_steps(c: &mut Criterion) {
    let mut group = c.benchmark_group("rate_window_steps");
    let rate_udf = Rate::scalar_udf();
    let num_points = 20_280;
    let window_size = 120u32;

    // No resets, one reset per 24 window widths, and roughly three resets per window.
    for reset_period in [0usize, 2_880, 37] {
        let values: Vec<f64> = match reset_period {
            0 => (0..num_points).map(|i| i as f64).collect(),
            period => (0..num_points).map(|i| (i % period) as f64).collect(),
        };
        for window_step in [1usize, 10, 120] {
            let prepared = PreparedUdfCall::new(make_extrapolated_rate_input(
                num_points,
                window_size,
                window_step,
                values.clone(),
                500,
            ));
            group.bench_with_input(
                BenchmarkId::new(
                    "rate_counter",
                    format!("reset{reset_period}_step{window_step}"),
                ),
                &(),
                |b, _| b.iter(|| invoke_prepared(&rate_udf, &prepared)),
            );
        }
    }

    group.finish();
}

fn bench_extrema_functions(c: &mut Criterion) {
    let mut group = c.benchmark_group("extrema_fn");
    let num_points = 4_096;
    let values = build_gauge_values(num_points);
    let min_udf = MinOverTime::scalar_udf();
    let max_udf = MaxOverTime::scalar_udf();
    // Cases meant to reuse candidates use 40-sample windows: the UDF rescans batches
    // whose windows average fewer than 32 samples.
    let mut backwards_ranges = (0..=num_points - 40)
        .step_by(5)
        .map(|offset| (offset as u32, 40))
        .collect::<Vec<_>>();
    backwards_ranges.extend((0..=512).step_by(5).map(|offset| (offset as u32, 40)));

    // The last two controls use explicit ranges instead of a regular window/step sweep.
    let cases = vec![
        (
            "w4_step1",
            (0..=num_points - 4)
                .map(|offset| (offset as u32, 4))
                .collect::<Vec<_>>(),
        ),
        (
            "w40_step1",
            (0..=num_points - 40)
                .map(|offset| (offset as u32, 40))
                .collect::<Vec<_>>(),
        ),
        (
            "w40_step5",
            (0..=num_points - 40)
                .step_by(5)
                .map(|offset| (offset as u32, 40))
                .collect::<Vec<_>>(),
        ),
        (
            "w240_step1",
            (0..=num_points - 240)
                .map(|offset| (offset as u32, 240))
                .collect::<Vec<_>>(),
        ),
        // A quarter of the window is the widest step that still reuses candidates.
        (
            "w240_step60",
            (0..=num_points - 240)
                .step_by(60)
                .map(|offset| (offset as u32, 240))
                .collect::<Vec<_>>(),
        ),
        (
            "w240_step240",
            (0..=num_points - 240)
                .step_by(240)
                .map(|offset| (offset as u32, 240))
                .collect::<Vec<_>>(),
        ),
        // A query ending one window past the last sample closes on an empty window.
        (
            "w240_step240_trailing_empty",
            (0..=num_points - 240)
                .step_by(240)
                .map(|offset| (offset as u32, 240))
                .chain([(0, 0)])
                .collect::<Vec<_>>(),
        ),
        ("backwards_reset_rebuild_w40_step5", backwards_ranges),
        (
            "low_coverage_full_backing_w4",
            vec![
                (0, 4),
                (512, 4),
                (1_024, 4),
                (1_536, 4),
                (2_048, 4),
                (2_560, 4),
                (3_584, 4),
                (4_092, 4),
            ],
        ),
    ];
    let functions = [
        ("min_over_time", &min_udf, true),
        ("max_over_time", &max_udf, false),
    ];

    for (case_name, ranges) in cases {
        let prepared = PreparedUdfCall::new(make_extrema_input_with_ranges(
            values.clone(),
            ranges.clone(),
        ));
        for (function_name, udf, is_min) in functions {
            let expected = extrema_oracle(&values, &ranges, |value, extrema| {
                if is_min {
                    value < extrema
                } else {
                    value > extrema
                }
            });
            assert_extrema_output(udf, &prepared, &expected);
            group.bench_with_input(
                BenchmarkId::new(
                    format!("{function_name}_{case_name}"),
                    format!("N{num_points}"),
                ),
                &(),
                |b, _| b.iter(|| invoke_prepared(udf, &prepared)),
            );
        }
    }

    group.finish();
}

fn bench_edge_count_functions(c: &mut Criterion) {
    let mut group = c.benchmark_group("edge_count_fn");
    let num_points = 4_096;
    let window_sizes = [4u32, 20, 240];
    let changing_values = build_changing_values(num_points);
    let resetting_values = build_resetting_counter_values(num_points);
    let changes_udf = Changes::scalar_udf();
    let resets_udf = Resets::scalar_udf();

    for window_size in window_sizes {
        let ranges = (0..=num_points - window_size as usize)
            .map(|offset| (offset as u32, window_size))
            .collect::<Vec<_>>();

        let changes_prepared = PreparedUdfCall::new(make_edge_count_input(
            num_points,
            window_size,
            changing_values.clone(),
        ));
        let changes_expected = edge_count_oracle(&changing_values, &ranges, |a, b| {
            a != b && !(a.is_nan() && b.is_nan())
        });
        assert_edge_count_output(&changes_udf, &changes_prepared, &changes_expected);
        group.bench_with_input(
            BenchmarkId::new(
                "changes_prebuilt_range_array",
                format!("N{num_points}_w{window_size}"),
            ),
            &(),
            |b, _| b.iter(|| invoke_prepared(&changes_udf, &changes_prepared)),
        );

        let resets_prepared = PreparedUdfCall::new(make_edge_count_input(
            num_points,
            window_size,
            resetting_values.clone(),
        ));
        let resets_expected = edge_count_oracle(&resetting_values, &ranges, |a, b| b < a);
        assert_edge_count_output(&resets_udf, &resets_prepared, &resets_expected);
        group.bench_with_input(
            BenchmarkId::new(
                "resets_prebuilt_range_array",
                format!("N{num_points}_w{window_size}"),
            ),
            &(),
            |b, _| b.iter(|| invoke_prepared(&resets_udf, &resets_prepared)),
        );
    }

    // Keep the backing arrays at N=4096 while evaluating only eight four-sample windows.
    // This isolates implementations that scan a global backing prefix instead of each range.
    let low_coverage_ranges = vec![
        (0, 4),
        (512, 4),
        (1_024, 4),
        (1_536, 4),
        (2_048, 4),
        (2_560, 4),
        (3_584, 4),
        (4_092, 4),
    ];
    assert_eq!(low_coverage_ranges.len(), 8);

    let low_coverage_changes = PreparedUdfCall::new(make_edge_count_input_with_ranges(
        changing_values.clone(),
        low_coverage_ranges.clone(),
    ));
    let low_coverage_changes_expected =
        edge_count_oracle(&changing_values, &low_coverage_ranges, |a, b| {
            a != b && !(a.is_nan() && b.is_nan())
        });
    assert_edge_count_output(
        &changes_udf,
        &low_coverage_changes,
        &low_coverage_changes_expected,
    );
    group.bench_with_input(
        BenchmarkId::new("changes_low_coverage_full_backing", "N4096_windows8_w4"),
        &(),
        |b, _| b.iter(|| invoke_prepared(&changes_udf, &low_coverage_changes)),
    );

    let low_coverage_resets = PreparedUdfCall::new(make_edge_count_input_with_ranges(
        resetting_values.clone(),
        low_coverage_ranges.clone(),
    ));
    let low_coverage_resets_expected =
        edge_count_oracle(&resetting_values, &low_coverage_ranges, |a, b| b < a);
    assert_edge_count_output(
        &resets_udf,
        &low_coverage_resets,
        &low_coverage_resets_expected,
    );
    group.bench_with_input(
        BenchmarkId::new("resets_low_coverage_full_backing", "N4096_windows8_w4"),
        &(),
        |b, _| b.iter(|| invoke_prepared(&resets_udf, &low_coverage_resets)),
    );

    group.finish();
}

/// One cursor-scan workload: the timeline, the window and the aligned instants to visit.
#[derive(Clone, Debug)]
struct CursorScanWorkload {
    time_unit: TimeUnit,
    offset_ms: i64,
    timestamps: Vec<i64>,
    window_ms: i64,
    interval_ms: i64,
    start_ms: i64,
    end_ms: i64,
}

impl CursorScanWorkload {
    /// The workload through the narrow `i64` carrier, or `None` when production would fall
    /// back to `i128` for it.
    ///
    /// For the fallback cases the fast path is timed on a twin workload: the same shape
    /// translated onto a timeline the narrow carrier holds, which keeps the number of sample
    /// and window comparisons identical to the fallback run.
    fn bench_scan_narrow(&self) -> Option<Vec<(u32, u32)>> {
        RangeManipulateStream::bench_scan_narrow(
            self.time_unit,
            self.offset_ms,
            self.window_ms,
            self.interval_ms,
            &self.timestamps,
            self.start_ms,
            self.end_ms,
        )
    }

    /// The workload through the exact `i128` carrier, the fallback production takes
    /// whenever [`Self::bench_scan_narrow`] returns `None`.
    fn bench_scan_wide(&self) -> Vec<(u32, u32)> {
        RangeManipulateStream::bench_scan_wide(
            self.time_unit,
            self.offset_ms,
            self.window_ms,
            self.interval_ms,
            &self.timestamps,
            self.start_ms,
            self.end_ms,
        )
    }

    fn evaluations(&self) -> usize {
        ((self.end_ms - self.start_ms) / self.interval_ms) as usize + 1
    }

    fn label(&self) -> String {
        format!(
            "N{}_eval{}_window{}x{}ms",
            self.timestamps.len(),
            self.evaluations(),
            self.window_ms / self.interval_ms,
            self.interval_ms
        )
    }
}

/// Cadence of the cursor-scan benchmarks: PromQL's usual 15s step.
const CURSOR_SCAN_CADENCE_MS: i64 = 15_000;

/// Milliseconds of a one-hour window, the widest window benchmarked here.
const CURSOR_SCAN_HOUR_MS: i64 = 3_600_000;

/// Builds a workload from a first timestamp and a sample count.
///
/// `sample_spacing_native` is the distance between samples in native ticks: 15s worth, so
/// every native-precision workload has the same shape (one sample per cadence, 20 samples
/// per 300s window). The aligned instants start at the first shifted sample, so every
/// window overlaps the input the same way in both arms of the A/B comparison.
fn cursor_scan_workload(
    time_unit: TimeUnit,
    offset_ms: i64,
    first_timestamp: i64,
    samples: usize,
    window_ms: i64,
    sample_spacing_native: i64,
) -> CursorScanWorkload {
    let interval_ms = CURSOR_SCAN_CADENCE_MS;
    let timestamps = (0..samples as i64)
        .map(|index| first_timestamp + index * sample_spacing_native)
        .collect::<Vec<_>>();
    // `Millisecond` instants of the first shifted sample, flooring like `calculate_range`.
    let start_ms = shifted_floor_millis(time_unit, offset_ms, first_timestamp);
    let evaluations = samples - 1;
    CursorScanWorkload {
        time_unit,
        offset_ms,
        timestamps,
        window_ms,
        interval_ms,
        start_ms,
        end_ms: start_ms + evaluations as i64 * interval_ms,
    }
}

/// Native ticks of one benchmark cadence, i.e. 15s in the given precision.
fn cadence_in_native_ticks(time_unit: TimeUnit) -> i64 {
    match time_unit {
        TimeUnit::Second => CURSOR_SCAN_CADENCE_MS / 1_000,
        TimeUnit::Millisecond => CURSOR_SCAN_CADENCE_MS,
        TimeUnit::Microsecond => CURSOR_SCAN_CADENCE_MS * 1_000,
        TimeUnit::Nanosecond => CURSOR_SCAN_CADENCE_MS * 1_000_000,
    }
}

/// Floor of a shifted sample in milliseconds, in exact `i128` native precision.
fn shifted_floor_millis(time_unit: TimeUnit, offset_ms: i64, timestamp: i64) -> i64 {
    let scale = match time_unit {
        TimeUnit::Second => 1_000_000_000i128,
        TimeUnit::Millisecond => 1_000_000i128,
        TimeUnit::Microsecond => 1_000i128,
        TimeUnit::Nanosecond => 1i128,
    };
    i64::try_from(
        ((timestamp as i128) * scale + (offset_ms as i128) * 1_000_000).div_euclid(1_000_000),
    )
    .expect("the shifted millisecond must stay legal")
}

/// A/B benchmark of the cursor-scan carriers behind `RangeManipulate::scan_ranges`.
///
/// `RangeManipulate` scans the aligned instants in `i64` nanoseconds whenever
/// `narrow_scan` proves the batch and the bounds fit, and in exact `i128` nanoseconds
/// otherwise. Both arms below drive the production scan through the carrier the operator
/// would pick, so the numbers isolate the carrier and not the surrounding operator work:
///
/// - `ms_zero_offset`: millisecond batch with no offset, the `i64` fast path. The same
///   workload is also timed through `i128` to price the fallback on this input.
/// - `ms_zero_offset_long_window`: same, with a one-hour window, where the scan advances
///   both cursors much further per evaluation.
/// - `ms_large_offset`: an offset whose nanosecond product overflows `i64`, so production
///   falls back to `i128`. The `i64` arm times the identical workload shape translated onto
///   a narrow timeline, which keeps the selections and thus the work identical.
/// - `ns_native_floor`: nanosecond batch at the `i64::MIN` floor, where the shifted start
///   bound underflows `i64` nanoseconds, so production falls back to `i128`. The `i64` arm
///   again times the translated narrow shape.
///
/// Every arm asserts that both carriers selected the same ranges, so the comparison cannot
/// be won by doing less work. The fallback triggers themselves are pinned by the unit tests
/// in `range_manipulate.rs` (`scan_falls_back_to_i128_when_only_the_sample_tail_overflows`
/// and `scan_carriers_split_at_the_nanosecond_boundaries`).
fn bench_cursor_scan_carriers(c: &mut Criterion) {
    const SAMPLES: usize = 4_096;
    const SHORT_WINDOW_MS: i64 = 20 * CURSOR_SCAN_CADENCE_MS;
    // `i64::MAX` milliseconds is ~9.22e18 ms; one microsecond past it in the nanosecond
    // product makes `narrow_scan` reject the offset, while the shifted millisecond stays legal.
    const LARGE_OFFSET_MS: i64 = 9_223_372_036_855;
    const LARGE_OFFSET_BASE_MS: i64 = 1_000_000_000_000_000;
    const NANOSECOND_FLOOR_SHIFT_NS: i64 = 9_223_372_036_000_000_000;
    const NANOSECOND_FLOOR_SAMPLES: usize = 4_096;

    let ms_zero_offset = cursor_scan_workload(
        TimeUnit::Millisecond,
        0,
        0,
        SAMPLES,
        SHORT_WINDOW_MS,
        cadence_in_native_ticks(TimeUnit::Millisecond),
    );
    let ms_zero_offset_long_window = cursor_scan_workload(
        TimeUnit::Millisecond,
        0,
        0,
        SAMPLES,
        CURSOR_SCAN_HOUR_MS,
        cadence_in_native_ticks(TimeUnit::Millisecond),
    );
    // The large offset is shifted inside one millisecond of the nanosecond ceiling, so the
    // payload stays legal: the fallback is driven by the product, not by an illegal instant.
    let ms_large_offset = cursor_scan_workload(
        TimeUnit::Millisecond,
        LARGE_OFFSET_MS,
        LARGE_OFFSET_BASE_MS,
        SAMPLES,
        SHORT_WINDOW_MS,
        cadence_in_native_ticks(TimeUnit::Millisecond),
    );
    // The narrow twin of `ms_large_offset`: the same batch shape without the offset.
    let ms_large_offset_narrow = cursor_scan_workload(
        TimeUnit::Millisecond,
        0,
        0,
        SAMPLES,
        SHORT_WINDOW_MS,
        cadence_in_native_ticks(TimeUnit::Millisecond),
    );

    // Nanosecond batch on the native floor: samples at `i64::MIN + i * 15s`, whose shifted
    // start bound underflows `i64` nanoseconds.
    let nanosecond_floor_first = i64::MIN;
    let nanosecond_floor = cursor_scan_workload(
        TimeUnit::Nanosecond,
        0,
        nanosecond_floor_first,
        NANOSECOND_FLOOR_SAMPLES,
        SHORT_WINDOW_MS,
        cadence_in_native_ticks(TimeUnit::Nanosecond),
    );
    // The narrow twin: the same shape translated by a multiple of one millisecond, so both
    // the samples and the aligned instants keep their relative geometry.
    let nanosecond_floor_narrow = CursorScanWorkload {
        time_unit: TimeUnit::Nanosecond,
        offset_ms: 0,
        timestamps: nanosecond_floor
            .timestamps
            .iter()
            .map(|timestamp| timestamp + NANOSECOND_FLOOR_SHIFT_NS)
            .collect(),
        window_ms: nanosecond_floor.window_ms,
        interval_ms: nanosecond_floor.interval_ms,
        start_ms: nanosecond_floor.start_ms + NANOSECOND_FLOOR_SHIFT_NS / 1_000_000,
        end_ms: nanosecond_floor.end_ms + NANOSECOND_FLOOR_SHIFT_NS / 1_000_000,
    };

    let cases = [
        (
            "ms_zero_offset",
            ms_zero_offset.clone(),
            ms_zero_offset,
            false,
        ),
        (
            "ms_zero_offset_long_window",
            ms_zero_offset_long_window.clone(),
            ms_zero_offset_long_window,
            false,
        ),
        (
            "ms_large_offset",
            ms_large_offset_narrow,
            ms_large_offset,
            true,
        ),
        (
            "ns_native_floor",
            nanosecond_floor_narrow,
            nanosecond_floor,
            true,
        ),
    ];

    let mut group = c.benchmark_group("cursor_scan_carriers");
    group.warm_up_time(std::time::Duration::from_millis(500));
    group.measurement_time(std::time::Duration::from_secs(2));
    group.sample_size(50);

    for (case_name, narrow, wide, wide_falls_back) in cases {
        // Both arms must be timed on work the operator would really do: the narrow arm in
        // `i64`, and the wide arm in `i128` exactly when the narrow baselines overflow.
        let narrow_expected = narrow
            .bench_scan_narrow()
            .unwrap_or_else(|| panic!("{case_name}: the i64 arm must fit the narrow carrier"));
        assert_eq!(
            wide.bench_scan_narrow().is_none(),
            wide_falls_back,
            "{case_name}: the i128 arm's fallback trigger"
        );
        // Fairness check before timing: both carriers have to select the same ranges, which
        // makes the sample and window comparisons of the two arms identical.
        assert_eq!(
            wide.bench_scan_wide(),
            narrow_expected,
            "{case_name}: the two carriers must select the same ranges"
        );
        assert_eq!(
            narrow_expected.len(),
            narrow.evaluations(),
            "{case_name}: one range per aligned evaluation instant"
        );

        group.throughput(criterion::Throughput::Elements(
            narrow.timestamps.len() as u64
        ));
        group.bench_with_input(
            BenchmarkId::new(format!("{case_name}/i64"), narrow.label()),
            &narrow,
            |b, workload| {
                b.iter(|| std::hint::black_box(workload.bench_scan_narrow()));
            },
        );
        group.bench_with_input(
            BenchmarkId::new(format!("{case_name}/i128"), wide.label()),
            &wide,
            |b, workload| {
                b.iter(|| std::hint::black_box(workload.bench_scan_wide()));
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_range_functions,
    bench_presence_range_functions,
    bench_delta_rate_comparison,
    bench_rate_window_steps,
    bench_edge_count_functions,
    bench_extrema_functions,
    bench_cursor_scan_carriers
);
