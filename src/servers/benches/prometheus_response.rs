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

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use axum::response::IntoResponse;
use common_query::Output;
use common_recordbatch::{RecordBatch, RecordBatches};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::vectors::{Float64Vector, StringVector, TimestampMillisecondVector, VectorRef};
use promql_parser::parser::value::ValueType;
use servers::http::prometheus::{
    PromQueryResult, PromSampleValue, PrometheusJsonResponse, PrometheusResponse,
};

const SERIES: usize = 64;
const POINTS: usize = 2048;
/// Label count of the one-sample-per-series shape an instant query returns.
const LABELS: usize = 4;

// Response building is dominated by allocation, so match the allocator the
// server binary uses instead of the platform default.
#[cfg(not(windows))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Builds a query result of `series_count` series with `points` samples each.
///
/// `run_length` is the number of consecutive rows that belong to the same
/// series, and `permuted` shuffles timestamps so the response has to sort them.
fn input(
    label_count: usize,
    run_length: usize,
    series_count: usize,
    points: usize,
    permuted: bool,
) -> (SchemaRef, Vec<RecordBatch>) {
    let mut columns = vec![
        ColumnSchema::new(
            "timestamp",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        ),
        ColumnSchema::new("value", ConcreteDataType::float64_datatype(), false),
    ];
    columns.extend((0..label_count).map(|index| {
        ColumnSchema::new(
            format!("label_{index}"),
            ConcreteDataType::string_datatype(),
            false,
        )
    }));
    let schema = Arc::new(Schema::new(columns));
    let point = |row: usize| {
        let point = row / (series_count * run_length) * run_length + row % run_length;
        if permuted {
            (point * 109 + 17) % points
        } else {
            point
        }
    };
    let mut vectors: Vec<VectorRef> = vec![
        Arc::new(TimestampMillisecondVector::from_vec(
            (0..series_count * points)
                .map(|row| point(row) as i64 * 300_000)
                .collect(),
        )),
        Arc::new(Float64Vector::from(
            (0..series_count * points)
                .map(|row| Some((point(row) % 1000) as f64 * 0.25))
                .collect::<Vec<_>>(),
        )),
    ];
    for label in 0..label_count {
        let values: Vec<_> = (0..series_count)
            .map(|series| format!("label-{label}-series-{series:04}"))
            .collect();
        vectors.push(Arc::new(StringVector::from(
            (0..series_count * points)
                .map(|row| Some(values[(row / run_length) % series_count].as_str()))
                .collect::<Vec<_>>(),
        )));
    }
    let batch = RecordBatch::new(schema.clone(), vectors).unwrap();
    let batches = (0..batch.num_rows())
        .step_by(1024)
        .map(|offset| batch.slice(offset, 1024).unwrap())
        .collect();
    (schema, batches)
}

fn bench_prometheus_response(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("prometheus_response_complete");
    group.sample_size(20);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(2));
    group.throughput(Throughput::Elements((SERIES * POINTS) as u64));
    for labels in [1, 4] {
        for run in [1, 32, POINTS] {
            for permuted in [false, true] {
                let (schema, batches) = input(labels, run, SERIES, POINTS, permuted);
                let convert = || {
                    runtime.block_on(PrometheusJsonResponse::from_query_result(
                        Ok(Output::new_with_record_batches(
                            RecordBatches::try_new(schema.clone(), batches.clone()).unwrap(),
                        )),
                        Some("metric".to_string()),
                        ValueType::Matrix,
                        None,
                    ))
                };
                let response = convert();
                assert_eq!(response.status, "success");
                let PrometheusResponse::PromData(data) = response.data else {
                    panic!("expected Prometheus data");
                };
                let PromQueryResult::Matrix(series) = data.result else {
                    panic!("expected matrix");
                };
                assert_eq!(series.len(), SERIES);
                for (index, series) in series.iter().enumerate() {
                    assert_eq!(series.metric.len(), labels + 1);
                    assert_eq!(series.metric["__name__"], "metric");
                    for label in 0..labels {
                        assert_eq!(
                            series.metric[&format!("label_{label}")],
                            format!("label-{label}-series-{index:04}")
                        );
                    }
                    assert!(series.histograms.is_empty());
                    assert_eq!(series.values.len(), POINTS);
                    for (point, (timestamp, value)) in series.values.iter().enumerate() {
                        assert_eq!(*timestamp, point as f64 * 300.0);
                        assert!(
                            matches!(value, PromSampleValue::Number(value) if *value == (point % 1000) as f64 * 0.25)
                        );
                    }
                }
                let order = if permuted { "permuted" } else { "ordered" };
                group.bench_function(format!("labels{labels}_run{run}_{order}"), |b| {
                    b.iter(|| black_box(convert().into_response()));
                });
            }
        }
    }
    group.finish();
}

fn bench_prometheus_single_point(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("prometheus_single_point_complete");
    group.sample_size(20);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(2));
    for series_count in [4096, 65536] {
        let (schema, batches) = input(LABELS, 1, series_count, 1, false);
        group.throughput(Throughput::Elements(series_count as u64));
        for result_type in [ValueType::Matrix, ValueType::Vector] {
            let convert = || {
                runtime.block_on(PrometheusJsonResponse::from_query_result(
                    Ok(Output::new_with_record_batches(
                        RecordBatches::try_new(schema.clone(), batches.clone()).unwrap(),
                    )),
                    Some("metric".to_string()),
                    result_type,
                    None,
                ))
            };
            let response = convert();
            assert_eq!(response.status, "success");
            let PrometheusResponse::PromData(data) = response.data else {
                panic!("expected Prometheus data");
            };
            match data.result {
                PromQueryResult::Matrix(series) => {
                    assert_eq!(series.len(), series_count);
                    assert!(series.iter().all(|series| {
                        series.metric.len() == LABELS + 1
                            && series.values.len() == 1
                            && series.values[0].0 == 0.0
                            && matches!(series.values[0].1, PromSampleValue::Number(0.0))
                            && series.histograms.is_empty()
                    }));
                }
                PromQueryResult::Vector(series) => {
                    assert_eq!(series.len(), series_count);
                    assert!(series.iter().all(|series| {
                        series.metric.len() == LABELS + 1
                            && series.value.as_ref().is_some_and(|(timestamp, value)| {
                                *timestamp == 0.0 && value == "0.0"
                            })
                            && series.histogram.is_none()
                    }));
                }
                _ => panic!("expected matrix or vector"),
            }
            group.bench_function(
                format!("{result_type}_labels{LABELS}_series{series_count}"),
                |b| b.iter(|| black_box(convert().into_response())),
            );
        }
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_prometheus_response,
    bench_prometheus_single_point
);
criterion_main!(benches);
