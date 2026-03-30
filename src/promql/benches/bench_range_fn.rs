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
use datafusion::arrow::array::{Float64Array, TimestampMillisecondArray};
use datafusion::physical_plan::ColumnarValue;
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::ScalarFunctionArgs;
use datatypes::arrow::datatypes::{DataType, Field};
use promql::functions::{Delta, IDelta, Increase, PredictLinear, QuantileOverTime, Rate};
use promql::range_array::RangeArray;

fn build_sliding_ranges(
    num_points: usize,
    window_size: u32,
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

    let ranges: Vec<(u32, u32)> = (0..num_windows).map(|i| (i as u32, window_size)).collect();

    let eval_ts: Vec<i64> = (0..num_windows)
        .map(|i| timestamps[i + window_size as usize - 1] + eval_offset_ms)
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

fn make_extrapolated_rate_input(
    num_points: usize,
    window_size: u32,
    values: Vec<f64>,
    eval_offset_ms: i64,
) -> Vec<ColumnarValue> {
    let (ts_range, val_range, eval_ts) =
        build_sliding_ranges(num_points, window_size, values, eval_offset_ms);
    let range_length = window_size as i64 * 1000;
    vec![
        ColumnarValue::Array(Arc::new(ts_range.into_dict())),
        ColumnarValue::Array(Arc::new(val_range.into_dict())),
        ColumnarValue::Array(eval_ts),
        ColumnarValue::Scalar(ScalarValue::Int64(Some(range_length))),
    ]
}

fn make_idelta_input(num_points: usize, window_size: u32) -> Vec<ColumnarValue> {
    let (ts_range, val_range, _) =
        build_sliding_ranges(num_points, window_size, build_default_values(num_points), 0);
    vec![
        ColumnarValue::Array(Arc::new(ts_range.into_dict())),
        ColumnarValue::Array(Arc::new(val_range.into_dict())),
    ]
}

fn make_quantile_input(num_points: usize, window_size: u32) -> Vec<ColumnarValue> {
    let (ts_range, val_range, _) =
        build_sliding_ranges(num_points, window_size, build_default_values(num_points), 0);
    vec![
        ColumnarValue::Array(Arc::new(ts_range.into_dict())),
        ColumnarValue::Array(Arc::new(val_range.into_dict())),
        ColumnarValue::Scalar(ScalarValue::Float64(Some(0.9))),
    ]
}

fn make_predict_linear_input(num_points: usize, window_size: u32) -> Vec<ColumnarValue> {
    let (ts_range, val_range, _) =
        build_sliding_ranges(num_points, window_size, build_default_values(num_points), 0);
    vec![
        ColumnarValue::Array(Arc::new(ts_range.into_dict())),
        ColumnarValue::Array(Arc::new(val_range.into_dict())),
        // predict 60s into the future
        ColumnarValue::Scalar(ScalarValue::Int64(Some(60))),
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

fn invoke_prepared(udf: &datafusion::logical_expr::ScalarUDF, prepared: &PreparedUdfCall) {
    udf.invoke_with_args(ScalarFunctionArgs {
        args: prepared.args.clone(),
        arg_fields: prepared.arg_fields.clone(),
        number_rows: prepared.number_rows,
        return_field: prepared.return_field.clone(),
        config_options: prepared.config_options.clone(),
    })
    .unwrap();
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

criterion_group!(benches, bench_range_functions);
