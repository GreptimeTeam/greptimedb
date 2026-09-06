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
use datafusion::arrow::array::{
    Array, ArrayRef, DictionaryArray, Float64Array, TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Int64Type, Schema,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ToDFSchema;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{EmptyRelation, LogicalPlan};
use datafusion::physical_plan::{ColumnarValue, ExecutionPlan};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::ScalarFunctionArgs;
use datatypes::arrow::datatypes::{DataType, Field};
use futures::StreamExt;
use promql::extension_plan::RangeManipulate;
use promql::functions::{
    Changes, Delta, IDelta, Increase, PredictLinear, QuantileOverTime, Rate, Resets, SumOverTime,
};
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
    let (ts_range, val_range, _) =
        build_sliding_ranges(num_points, window_size, build_default_values(num_points), 0);
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
    let (ts_range, val_range, _) = build_sliding_ranges(num_points, window_size, values, 0);
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

const RANGE_MANIPULATE_CADENCE_MS: i64 = 15_000;

fn make_range_manipulate_batch(timestamps: Vec<i64>, field_count: usize) -> RecordBatch {
    let mut fields = Vec::with_capacity(field_count + 1);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(field_count + 1);
    fields.push(ArrowField::new(
        "timestamp",
        ArrowDataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, None),
        false,
    ));
    columns.push(Arc::new(TimestampMillisecondArray::from(timestamps)) as _);

    for field_index in 0..field_count {
        fields.push(ArrowField::new(
            format!("value_{field_index}"),
            ArrowDataType::Float64,
            false,
        ));
        let values = (0..columns[0].len())
            .map(|row| row as f64 + field_index as f64 * 0.01)
            .collect::<Vec<_>>();
        columns.push(Arc::new(Float64Array::from(values)) as _);
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

async fn execute_and_drain(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
) -> Vec<RecordBatch> {
    let mut stream = plan.execute(0, task_ctx).unwrap();
    let mut output = Vec::new();
    while let Some(batch) = stream.next().await {
        output.push(batch.unwrap());
    }
    output
}

fn range_keys(batch: &RecordBatch, index: usize) -> Vec<(u32, u32)> {
    let ranges = batch
        .column(index)
        .as_any()
        .downcast_ref::<DictionaryArray<Int64Type>>()
        .unwrap()
        .clone();
    RangeArray::try_new(ranges)
        .unwrap()
        .ranges()
        .map(Option::unwrap)
        .collect()
}

fn brute_force_range_keys(
    timestamps: &[i64],
    evaluations: usize,
    window_points: usize,
) -> Vec<(u32, u32)> {
    let range = window_points as i64 * RANGE_MANIPULATE_CADENCE_MS;
    (0..evaluations)
        .map(|evaluation| {
            let current = evaluation as i64 * RANGE_MANIPULATE_CADENCE_MS;
            let mut offset = None;
            let mut length = 0;
            for (index, timestamp) in timestamps.iter().enumerate() {
                if *timestamp > current - range && *timestamp <= current {
                    offset.get_or_insert(index as u32);
                    length += 1;
                }
            }
            (offset.unwrap_or(0), length)
        })
        .collect()
}

fn validate_range_manipulate_output(
    output: &[RecordBatch],
    field_count: usize,
    expected_range_keys: Option<&[(u32, u32)]>,
) {
    let Some(expected_range_keys) = expected_range_keys else {
        assert!(output.is_empty());
        return;
    };

    assert_eq!(output.len(), 1);
    let batch = &output[0];
    assert_eq!(batch.num_rows(), expected_range_keys.len());

    let timestamp_range_keys = range_keys(batch, field_count + 1);
    assert_eq!(timestamp_range_keys, expected_range_keys);

    for field_index in 0..field_count {
        assert_eq!(range_keys(batch, field_index + 1), expected_range_keys);
    }
}

fn bench_range_manipulate_wall_time(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_manipulate_wall_time");
    let primary_points = 4_096;
    let sparse_evaluations = primary_points - 1;
    let mut cases = vec![
        (
            "one_field",
            (0..primary_points)
                .map(|point| point as i64 * RANGE_MANIPULATE_CADENCE_MS)
                .collect::<Vec<_>>(),
            primary_points,
            4,
            1,
            false,
        ),
        (
            "one_field",
            (0..primary_points)
                .map(|point| point as i64 * RANGE_MANIPULATE_CADENCE_MS)
                .collect::<Vec<_>>(),
            primary_points,
            20,
            1,
            false,
        ),
        (
            "one_field",
            (0..primary_points)
                .map(|point| point as i64 * RANGE_MANIPULATE_CADENCE_MS)
                .collect::<Vec<_>>(),
            primary_points,
            240,
            1,
            false,
        ),
        (
            "regular_sparse",
            (0..sparse_evaluations)
                .step_by(2)
                .map(|point| point as i64 * RANGE_MANIPULATE_CADENCE_MS)
                .collect::<Vec<_>>(),
            sparse_evaluations,
            20,
            1,
            false,
        ),
        (
            "all_empty_pathological",
            (0..primary_points)
                .map(|point| {
                    (primary_points as i64 + 21 + point as i64) * RANGE_MANIPULATE_CADENCE_MS
                })
                .collect::<Vec<_>>(),
            primary_points,
            20,
            1,
            true,
        ),
        (
            "multi_field_control",
            (0..primary_points)
                .map(|point| point as i64 * RANGE_MANIPULATE_CADENCE_MS)
                .collect::<Vec<_>>(),
            primary_points,
            20,
            4,
            false,
        ),
    ];

    for (case_name, timestamps, evaluations, window_points, field_count, expect_empty) in
        cases.drain(..)
    {
        let expected_range_keys = (!expect_empty)
            .then(|| brute_force_range_keys(&timestamps, evaluations, window_points));
        let input_batch = make_range_manipulate_batch(timestamps, field_count);
        let input_rows = input_batch.num_rows();
        let input_schema = input_batch.schema();
        let logical_input = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: input_schema.clone().to_dfschema_ref().unwrap(),
        });
        let field_columns = (0..field_count)
            .map(|field_index| format!("value_{field_index}"))
            .collect::<Vec<_>>();
        let logical_plan = RangeManipulate::new(
            0,
            (evaluations as i64 - 1) * RANGE_MANIPULATE_CADENCE_MS,
            RANGE_MANIPULATE_CADENCE_MS,
            window_points as i64 * RANGE_MANIPULATE_CADENCE_MS,
            "timestamp".to_string(),
            field_columns,
            logical_input,
        )
        .unwrap();
        let physical_input: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![input_batch]], input_schema, None).unwrap(),
        )));
        let execution_plan = logical_plan.to_execution_plan(physical_input);
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let task_ctx = datafusion::prelude::SessionContext::new().task_ctx();

        // Validate the public operator output and RangeArray keys before timing.
        let output = runtime.block_on(execute_and_drain(execution_plan.clone(), task_ctx.clone()));
        validate_range_manipulate_output(&output, field_count, expected_range_keys.as_deref());

        // This measures public RangeManipulate execution wall time, including stream draining.
        group.bench_with_input(
            BenchmarkId::new(
                case_name,
                format!("N{input_rows}_eval{evaluations}_window{window_points}x15s"),
            ),
            &(),
            |b, _| {
                b.iter(|| {
                    let output = runtime
                        .block_on(execute_and_drain(execution_plan.clone(), task_ctx.clone()));
                    std::hint::black_box(output);
                })
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_range_functions,
    bench_delta_rate_comparison,
    bench_edge_count_functions,
    bench_range_manipulate_wall_time
);
