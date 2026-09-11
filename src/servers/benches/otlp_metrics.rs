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

//! See `otlp_metrics.md` for baseline/comparison commands and HTTP setup.

use std::hint::black_box;
use std::time::Duration;

use bytes::Bytes;
use common_query::prelude::set_default_prefix;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::future::join_all;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceResponse;
use otel_arrow_rust::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
use otel_arrow_rust::proto::opentelemetry::common::v1::{
    AnyValue, InstrumentationScope, KeyValue, any_value,
};
use otel_arrow_rust::proto::opentelemetry::metrics::v1::summary_data_point::ValueAtQuantile;
use otel_arrow_rust::proto::opentelemetry::metrics::v1::{
    AggregationTemporality, Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint,
    ResourceMetrics, ScopeMetrics, Sum, Summary, SummaryDataPoint, metric, number_data_point,
};
use otel_arrow_rust::proto::opentelemetry::resource::v1::Resource;
use prost::Message;
use servers::otlp::metrics::to_grpc_insert_requests;
use session::protocol_ctx::OtlpMetricCtx;

// Match the production binary's allocator for allocation-heavy conversion.
#[cfg(not(windows))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const START_NANOS: u64 = 1_700_000_000_000_000_000;

#[derive(Clone, Copy)]
enum Kind {
    Gauge,
    Sum,
    Histogram(usize),
    Summary,
}

struct Workload {
    name: &'static str,
    kind: Kind,
    resources: usize,
    metrics: usize,
    points: usize,
    point_attrs: usize,
}

impl Workload {
    fn point_count(&self) -> usize {
        self.resources * self.metrics * self.points
    }

    fn row_count(&self) -> usize {
        self.point_count()
            * match self.kind {
                Kind::Gauge | Kind::Sum => 1,
                Kind::Histogram(bounds) => bounds + 3,
                Kind::Summary => 5,
            }
    }

    fn table_count(&self) -> usize {
        self.metrics
            * match self.kind {
                Kind::Gauge | Kind::Sum => 1,
                Kind::Histogram(_) | Kind::Summary => 3,
            }
    }

    fn request(&self, sequence: u64) -> ExportMetricsServiceRequest {
        // Each HTTP request gets new timestamps for the same series. Replaying
        // identical points would benchmark overwrites instead of append ingestion.
        let timestamp = START_NANOS
            .checked_add(
                sequence
                    .checked_mul(self.points as u64)
                    .unwrap()
                    .checked_mul(1_000_000)
                    .unwrap(),
            )
            .unwrap();
        ExportMetricsServiceRequest {
            resource_metrics: (0..self.resources)
                .map(|resource| ResourceMetrics {
                    resource: Some(Resource {
                        attributes: vec![
                            tag("service.name", "api"),
                            tag("service.namespace", "benchmark"),
                            tag("service.instance.id", &format!("instance-{resource}")),
                            tag("service.version", "1.0"),
                            tag("cloud.region", "region-a"),
                            tag("k8s.namespace.name", "default"),
                            tag("k8s.pod.name", &format!("pod-{resource}")),
                            tag("container.name", "api"),
                            // Exercise promotion filtering as well as retained attributes.
                            tag("unpromoted.attribute", "ignored"),
                        ],
                        ..Default::default()
                    }),
                    scope_metrics: vec![ScopeMetrics {
                        scope: Some(InstrumentationScope {
                            name: "benchmark.instrumentation".to_string(),
                            version: "1.0".to_string(),
                            attributes: vec![tag("scope.attribute", "shared")],
                            ..Default::default()
                        }),
                        metrics: (0..self.metrics)
                            .map(|metric| Metric {
                                name: format!("otlp_bench_{}_{metric}", self.name),
                                unit: "s".to_string(),
                                data: Some(self.metric_data(timestamp)),
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    }],
                    ..Default::default()
                })
                .collect(),
        }
    }

    fn attributes(&self, point: usize) -> Vec<KeyValue> {
        let mut attributes = vec![tag("series.id", &point.to_string())];
        attributes.extend((0..self.point_attrs).map(|attr| KeyValue {
            key: format!("point.attribute.{attr}"),
            value: Some(AnyValue {
                value: Some(if attr % 2 == 0 {
                    any_value::Value::StringValue(format!("value-{attr}"))
                } else {
                    any_value::Value::IntValue(attr as i64)
                }),
            }),
        }));
        // Later rows grow the schema, and intervening rows require null filling.
        if point % 8 == 7 {
            attributes.push(tag("optional.attribute", "present"));
        }
        attributes
    }

    fn metric_data(&self, timestamp: u64) -> metric::Data {
        match self.kind {
            Kind::Gauge | Kind::Sum => {
                let data_points = (0..self.points)
                    .map(|point| NumberDataPoint {
                        attributes: self.attributes(point),
                        start_time_unix_nano: START_NANOS,
                        time_unix_nano: timestamp + point as u64 * 1_000_000,
                        value: Some(number_data_point::Value::AsDouble(point as f64 + 1.0)),
                        ..Default::default()
                    })
                    .collect();
                if matches!(self.kind, Kind::Gauge) {
                    metric::Data::Gauge(Gauge { data_points })
                } else {
                    metric::Data::Sum(Sum {
                        data_points,
                        aggregation_temporality: AggregationTemporality::Delta as i32,
                        is_monotonic: true,
                    })
                }
            }
            Kind::Histogram(bounds) => metric::Data::Histogram(Histogram {
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                data_points: (0..self.points)
                    .map(|point| HistogramDataPoint {
                        attributes: self.attributes(point),
                        start_time_unix_nano: START_NANOS,
                        time_unix_nano: timestamp + point as u64 * 1_000_000,
                        count: (bounds + 1) as u64,
                        sum: Some(((bounds + 1) * (bounds + 2)) as f64 / 2.0),
                        explicit_bounds: (1..=bounds).map(|bound| bound as f64).collect(),
                        bucket_counts: vec![1; bounds + 1],
                        ..Default::default()
                    })
                    .collect(),
            }),
            Kind::Summary => metric::Data::Summary(Summary {
                data_points: (0..self.points)
                    .map(|point| SummaryDataPoint {
                        attributes: self.attributes(point),
                        start_time_unix_nano: START_NANOS,
                        time_unix_nano: timestamp + point as u64 * 1_000_000,
                        count: 100,
                        sum: 100.0,
                        quantile_values: [0.5, 0.9, 0.99]
                            .into_iter()
                            .map(|quantile| ValueAtQuantile {
                                quantile,
                                value: quantile,
                            })
                            .collect(),
                        ..Default::default()
                    })
                    .collect(),
            }),
        }
    }
}

fn tag(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
    }
}

fn context() -> OtlpMetricCtx {
    OtlpMetricCtx {
        promote_scope_attrs: true,
        with_metric_engine: true,
        ..Default::default()
    }
}

fn workloads() -> Vec<Workload> {
    [
        ("small", Kind::Gauge, 1, 4, 1, 2),
        ("shared", Kind::Gauge, 1, 16, 64, 8),
        ("resources", Kind::Gauge, 32, 4, 8, 8),
        ("wide", Kind::Gauge, 1, 16, 64, 32),
        ("delta_sum", Kind::Sum, 1, 16, 64, 8),
        ("histogram_10", Kind::Histogram(10), 1, 4, 64, 8),
        ("histogram_50", Kind::Histogram(50), 1, 4, 64, 8),
        ("summary", Kind::Summary, 1, 4, 64, 8),
    ]
    .into_iter()
    .map(
        |(name, kind, resources, metrics, points, point_attrs)| Workload {
            name,
            kind,
            resources,
            metrics,
            points,
            point_attrs,
        },
    )
    .collect()
}

#[allow(clippy::print_stderr)]
fn bench_metrics(c: &mut Criterion) {
    set_default_prefix(None).unwrap();
    for workload in workloads() {
        let bytes = Bytes::from(workload.request(0).encode_to_vec());
        let request = ExportMetricsServiceRequest::decode(bytes.clone()).unwrap();
        let conversion = to_grpc_insert_requests(request, &mut context()).unwrap();
        assert_eq!(
            conversion.outcome.accepted_data_points,
            workload.point_count() as i64
        );
        assert_eq!(conversion.outcome.rejected_data_points, 0);
        assert!(conversion.outcome.error_message.is_none());
        assert_eq!(conversion.rows, workload.row_count());
        assert_eq!(conversion.requests.inserts.len(), workload.table_count());
        assert_eq!(
            conversion
                .requests
                .inserts
                .iter()
                .map(|insert| {
                    let rows = insert.rows.as_ref().unwrap();
                    assert!(
                        rows.rows
                            .iter()
                            .all(|row| row.values.len() == rows.schema.len())
                    );
                    rows.rows.len()
                })
                .sum::<usize>(),
            workload.row_count(),
        );
        assert!(conversion.resource_info.is_none());
        drop(conversion);
        eprintln!(
            "{}: bytes={} points={} rows={} tables={}",
            workload.name,
            bytes.len(),
            workload.point_count(),
            workload.row_count(),
            workload.table_count(),
        );

        let mut group = c.benchmark_group(format!("otlp_metrics/{}", workload.name));
        group.throughput(Throughput::Elements(workload.point_count() as u64));
        group.bench_function("decode", |b| {
            b.iter(|| {
                drop(black_box(
                    ExportMetricsServiceRequest::decode(black_box(bytes.clone())).unwrap(),
                ));
            });
        });
        group.bench_function("convert", |b| {
            b.iter_batched(
                || {
                    (
                        ExportMetricsServiceRequest::decode(bytes.clone()).unwrap(),
                        context(),
                    )
                },
                |(request, mut ctx)| {
                    drop(black_box(
                        to_grpc_insert_requests(black_box(request), &mut ctx).unwrap(),
                    ));
                },
                BatchSize::PerIteration,
            );
        });
        group.bench_function("decode_to_rows", |b| {
            b.iter(|| {
                let request =
                    ExportMetricsServiceRequest::decode(black_box(bytes.clone())).unwrap();
                drop(black_box(
                    to_grpc_insert_requests(request, &mut context()).unwrap(),
                ));
            });
        });
        group.finish();
    }
}

async fn post_metrics(client: &reqwest::Client, url: &str, database: &str, body: Bytes) {
    let response = client
        .post(url)
        .header("content-type", "application/x-protobuf")
        .header("x-greptime-db-name", database)
        .header("x-greptime-otlp-metric-promote-scope-attrs", "true")
        .body(body)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.bytes().await.unwrap();
    assert_eq!(status, reqwest::StatusCode::OK, "{bytes:?}");
    let response = ExportMetricsServiceResponse::decode(bytes).unwrap();
    if let Some(partial) = response.partial_success {
        assert_eq!(partial.rejected_data_points, 0, "{partial:?}");
        assert!(partial.error_message.is_empty(), "{partial:?}");
    }
}

fn bench_http(c: &mut Criterion) {
    let Ok(url) = std::env::var("OTLP_BENCH_URL") else {
        return;
    };
    let database =
        std::env::var("OTLP_BENCH_DB").expect("set OTLP_BENCH_DB to a disposable database");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(32)
        .build()
        .unwrap();
    for workload in workloads()
        .into_iter()
        .filter(|w| matches!(w.kind, Kind::Gauge))
    {
        let mut sequence = 0u64;
        let mut group = c.benchmark_group(format!("otlp_http/{}", workload.name));
        for concurrency in [1, 32] {
            group.throughput(Throughput::Elements(
                (workload.point_count() * concurrency) as u64,
            ));
            group.bench_with_input(
                BenchmarkId::new("concurrency", concurrency),
                &concurrency,
                |b, &concurrency| {
                    // Create/warm tables outside the measured loop. Criterion warm-up
                    // still performs real writes, just like the measured requests.
                    runtime.block_on(post_metrics(
                        &client,
                        &url,
                        &database,
                        Bytes::from(workload.request(sequence).encode_to_vec()),
                    ));
                    sequence = sequence.checked_add(1).unwrap();
                    b.iter_batched(
                        || {
                            (0..concurrency)
                                .map(|_| {
                                    let bytes =
                                        Bytes::from(workload.request(sequence).encode_to_vec());
                                    sequence = sequence.checked_add(1).unwrap();
                                    bytes
                                })
                                .collect::<Vec<_>>()
                        },
                        |bodies| {
                            runtime.block_on(join_all(
                                bodies
                                    .into_iter()
                                    .map(|body| post_metrics(&client, &url, &database, body)),
                            ));
                        },
                        BatchSize::PerIteration,
                    );
                },
            );
        }
        group.finish();
        if sequence > 0 {
            // Verify acknowledged rows are queryable, outside the timed region.
            // The benchmark database must be fresh and batching must acknowledge
            // completed writes, not merely enqueue them.
            let tables = to_grpc_insert_requests(workload.request(0), &mut context()).unwrap();
            let counts = tables
                .requests
                .inserts
                .iter()
                .map(|insert| format!("SELECT COUNT(*) AS n FROM \"{}\"", insert.table_name))
                .collect::<Vec<_>>()
                .join(" UNION ALL ");
            let sql = format!("SELECT SUM(n) FROM ({counts})");
            let sql_url = reqwest::Url::parse(&url).unwrap().join("/v1/sql").unwrap();
            let response: serde_json::Value = runtime.block_on(async {
                client
                    .post(sql_url)
                    .header("x-greptime-db-name", &database)
                    .form(&[("sql", sql)])
                    .send()
                    .await
                    .unwrap()
                    .error_for_status()
                    .unwrap()
                    .json()
                    .await
                    .unwrap()
            });
            assert_eq!(
                response["output"][0]["records"]["rows"][0][0].as_u64(),
                Some(sequence.checked_mul(workload.row_count() as u64).unwrap()),
                "persisted row count mismatch: {response}",
            );
        }
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(20)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    targets = bench_metrics, bench_http
}
criterion_main!(benches);
