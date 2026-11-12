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

//! Measures OTLP request-to-row conversion, excluding request cloning and storage.
//! Save a baseline with `cargo bench -p servers --bench otlp_metrics -- --save-baseline before`,
//! then compare with `cargo bench -p servers --bench otlp_metrics -- --baseline before`.

#[path = "otlp_metrics/fixtures.rs"]
mod fixtures;

use std::hint::black_box;
use std::time::Duration;

use api::v1::value::ValueData;
use common_catalog::consts::SEMANTIC_GRAPH_WINDOW_NANOS;
use common_query::prelude::{greptime_timestamp, set_default_prefix};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use otel_arrow_rust::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
use otel_arrow_rust::proto::opentelemetry::common::v1::{AnyValue, KeyValue, any_value};
use otel_arrow_rust::proto::opentelemetry::metrics::v1::{
    AggregationTemporality, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram,
    HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
    exponential_histogram_data_point::Buckets, metric, number_data_point,
};
use otel_arrow_rust::proto::opentelemetry::resource::v1::Resource;
use servers::otlp::metrics::to_grpc_insert_requests;
use session::protocol_ctx::OtlpMetricCtx;

const POINTS: usize = 128;

fn attribute(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
    }
}

fn timestamp(index: usize) -> u64 {
    ((index % 4 + 1) as i64 * SEMANTIC_GRAPH_WINDOW_NANOS) as u64 + index as u64 * 1_000_000
}

fn point_attributes() -> Vec<KeyValue> {
    vec![
        attribute("http.request.method", "GET"),
        attribute("route", "/api"),
    ]
}

fn request(data: metric::Data) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![
                    attribute("service.name", "api"),
                    attribute("service.namespace", "shop"),
                    attribute("service.instance.id", "instance-1"),
                    attribute("host.id", "host-1"),
                    attribute("k8s.pod.name", "api-1"),
                ],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "request_duration".to_string(),
                    data: Some(data),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn exponential(bucket_counts: Vec<u64>, scale: i32, reject: bool) -> metric::Data {
    let count = 2 + 2 * bucket_counts.iter().sum::<u64>();
    metric::Data::ExponentialHistogram(ExponentialHistogram {
        aggregation_temporality: AggregationTemporality::Cumulative as i32,
        data_points: (0..POINTS)
            .map(|index| ExponentialHistogramDataPoint {
                attributes: point_attributes(),
                start_time_unix_nano: 1,
                time_unix_nano: timestamp(index),
                count: count + u64::from(reject && index % 4 == 3),
                sum: Some(100.0),
                scale,
                zero_count: 2,
                positive: Some(Buckets {
                    offset: -128,
                    bucket_counts: bucket_counts.clone(),
                }),
                negative: Some(Buckets {
                    offset: -128,
                    bucket_counts: bucket_counts.clone(),
                }),
                zero_threshold: 1e-6,
                ..Default::default()
            })
            .collect(),
    })
}

fn bench_otlp_metrics(c: &mut Criterion) {
    set_default_prefix(None).unwrap();
    let gauge = metric::Data::Gauge(Gauge {
        data_points: (0..POINTS)
            .map(|index| NumberDataPoint {
                attributes: point_attributes(),
                time_unix_nano: timestamp(index),
                value: Some(number_data_point::Value::AsDouble(1.0)),
                ..Default::default()
            })
            .collect(),
    });
    let histogram = metric::Data::Histogram(Histogram {
        aggregation_temporality: AggregationTemporality::Delta as i32,
        data_points: (0..POINTS)
            .map(|index| HistogramDataPoint {
                attributes: point_attributes(),
                start_time_unix_nano: 1,
                time_unix_nano: timestamp(index),
                count: 17,
                sum: Some(100.0),
                bucket_counts: vec![1; 17],
                explicit_bounds: (0..16).map(|index| index as f64).collect(),
                ..Default::default()
            })
            .collect(),
    });
    let workloads = [
        ("gauge", gauge, POINTS, POINTS),
        ("classic_16", histogram, POINTS, POINTS * 19),
        (
            "exponential_16",
            exponential(vec![1; 16], 0, false),
            POINTS,
            POINTS,
        ),
        (
            "exponential_256",
            exponential(vec![1; 256], 0, false),
            POINTS,
            POINTS,
        ),
        (
            "exponential_sparse_256",
            exponential((0..256).map(|i| u64::from(i % 16 == 0)).collect(), 0, false),
            POINTS,
            POINTS,
        ),
        (
            "exponential_downscale_256",
            exponential(vec![1; 256], 10, false),
            POINTS,
            POINTS,
        ),
        (
            "exponential_reject_quarter_256",
            exponential(vec![1; 256], 0, true),
            POINTS * 3 / 4,
            POINTS * 3 / 4,
        ),
    ];

    let mut group = c.benchmark_group("otlp_metrics");
    group.sample_size(30);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));
    group.throughput(Throughput::Elements(POINTS as u64));
    for (name, data, accepted, rows) in workloads {
        let request = request(data);
        for resource_info in [false, true] {
            let ctx = OtlpMetricCtx {
                resource_info,
                ..Default::default()
            };
            let conversion = to_grpc_insert_requests(request.clone(), &mut ctx.clone()).unwrap();
            assert_eq!(
                conversion.outcome.accepted_data_points, accepted as i64,
                "{name}"
            );
            assert_eq!(
                conversion.outcome.rejected_data_points,
                (POINTS - accepted) as i64,
                "{name}"
            );
            assert_eq!(conversion.rows, rows, "{name}");
            assert_eq!(conversion.resource_info.is_some(), resource_info, "{name}");
            if let Some(descriptor) = conversion.resource_info {
                let descriptor_rows = descriptor.inserts[0].rows.as_ref().unwrap();
                let ts_index = descriptor_rows
                    .schema
                    .iter()
                    .position(|column| column.column_name == greptime_timestamp())
                    .unwrap();
                let expected = (0..4)
                    .filter(|window| accepted == POINTS || *window != 3)
                    .map(|window| {
                        Some(ValueData::TimestampMillisecondValue(
                            (timestamp(POINTS - 4 + window) / 1_000_000) as i64,
                        ))
                    })
                    .collect::<Vec<_>>();
                let actual = descriptor_rows
                    .rows
                    .iter()
                    .map(|row| row.values[ts_index].value_data.clone())
                    .collect::<Vec<_>>();
                assert_eq!(actual, expected, "{name}");
            }
            group.bench_with_input(
                BenchmarkId::new(
                    name,
                    if resource_info {
                        "descriptor_on"
                    } else {
                        "descriptor_off"
                    },
                ),
                &request,
                |b, request| {
                    b.iter_batched(
                        || (request.clone(), ctx.clone()),
                        |(request, mut ctx)| {
                            black_box(
                                to_grpc_insert_requests(black_box(request), &mut ctx).unwrap(),
                            )
                        },
                        BatchSize::LargeInput,
                    );
                },
            );
        }
    }
    group.finish();
}

fn bench_conversion(c: &mut Criterion) {
    let fixtures = fixtures::fixtures();
    for (group_name, delta) in [
        ("otlp_conversion/common", false),
        ("otlp_conversion/delta", true),
    ] {
        let mut group = c.benchmark_group(group_name);
        for fixture in fixtures
            .iter()
            .filter(|fixture| fixture.name.starts_with("delta") == delta)
        {
            group.throughput(Throughput::Elements(fixture.points));
            for resource_info in [false, true] {
                let ctx = OtlpMetricCtx {
                    resource_info,
                    with_metric_engine: true,
                    ..Default::default()
                };
                group.bench_function(
                    BenchmarkId::new(
                        fixture.name,
                        if resource_info {
                            "descriptors_on"
                        } else {
                            "descriptors_off"
                        },
                    ),
                    |b| {
                        let output =
                            to_grpc_insert_requests(fixture.request.clone(), &mut ctx.clone())
                                .unwrap();
                        assert_eq!(
                            output.outcome.accepted_data_points,
                            fixture.points as i64 - fixture.rejected
                        );
                        assert_eq!(output.outcome.rejected_data_points, fixture.rejected);
                        assert_eq!(output.resource_info.is_some(), resource_info);
                        b.iter_batched(
                            || fixture.request.clone(),
                            |request| {
                                black_box(
                                    to_grpc_insert_requests(request, &mut ctx.clone()).unwrap(),
                                )
                            },
                            BatchSize::LargeInput,
                        );
                    },
                );
            }
        }
        group.finish();
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20).warm_up_time(Duration::from_secs(1)).measurement_time(Duration::from_secs(3));
    targets = bench_conversion, bench_otlp_metrics
}
criterion_main!(benches);
