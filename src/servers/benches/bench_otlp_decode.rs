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
use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use otel_arrow_rust::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
use otel_arrow_rust::proto::opentelemetry::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric,
};
use prost::Message;

#[path = "otlp_decode/capacity_hint.rs"]
mod decode;

fn decode_metrics(c: &mut Criterion) {
    let directory = std::env::var_os("OTLP_DECODE_CORPUS")
        .expect("set OTLP_DECODE_CORPUS to uncompressed sample_loader OTLP .pb requests");
    let mut paths: Vec<PathBuf> = std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "pb"))
        .collect();
    paths.sort();
    let corpus: Vec<Bytes> = paths
        .into_iter()
        .map(|path| std::fs::read(path).unwrap().into())
        .collect();
    assert!(!corpus.is_empty());
    for payload in &corpus {
        assert_eq!(
            ExportMetricsServiceRequest::decode(payload.clone()).unwrap(),
            ExportMetricsServiceRequest::decode(payload.as_ref()).unwrap()
        );
        assert_eq!(
            ExportMetricsServiceRequest::decode(payload.clone()).unwrap(),
            decode::decode_metrics(payload.clone()).unwrap()
        );
    }
    if let Some(report) = std::env::var_os("OTLP_DECODE_CAPACITY_REPORT") {
        let standard: usize = corpus
            .iter()
            .map(|payload| {
                attribute_capacity(&ExportMetricsServiceRequest::decode(payload.clone()).unwrap())
            })
            .sum();
        let hinted: usize = corpus
            .iter()
            .map(|payload| attribute_capacity(&decode::decode_metrics(payload.clone()).unwrap()))
            .sum();
        std::fs::write(report, format!("standard_attribute_capacity_bytes={standard}\nhinted_attribute_capacity_bytes={hinted}\n")).unwrap();
    }
    let mut group = c.benchmark_group("otlp_metrics_decode");
    group.sample_size(50);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(5));
    group.throughput(Throughput::Bytes(
        corpus.iter().map(|payload| payload.len() as u64).sum(),
    ));
    group.bench_function("bytes", |b| {
        b.iter(|| {
            for payload in &corpus {
                black_box(ExportMetricsServiceRequest::decode(black_box(payload.clone())).unwrap());
            }
        })
    });
    group.bench_function("slice", |b| {
        b.iter(|| {
            for payload in &corpus {
                black_box(
                    ExportMetricsServiceRequest::decode(black_box(payload.as_ref())).unwrap(),
                );
            }
        })
    });
    group.bench_function("capacity_hint", |b| {
        b.iter(|| {
            for payload in &corpus {
                black_box(decode::decode_metrics(black_box(payload.clone())).unwrap());
            }
        })
    });
    group.finish();

    let small = Bytes::from(
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint::default()],
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
        .encode_to_vec(),
    );
    let mut group = c.benchmark_group("otlp_small_decode");
    group.measurement_time(Duration::from_secs(3));
    group.bench_function("bytes", |b| {
        b.iter(|| black_box(ExportMetricsServiceRequest::decode(black_box(small.clone())).unwrap()))
    });
    group.bench_function("capacity_hint", |b| {
        b.iter(|| black_box(decode::decode_metrics(black_box(small.clone())).unwrap()))
    });
    group.finish();
}

fn attribute_capacity(request: &ExportMetricsServiceRequest) -> usize {
    request
        .resource_metrics
        .iter()
        .flat_map(|resource| &resource.scope_metrics)
        .flat_map(|scope| &scope.metrics)
        .map(|metric| {
            let points = match &metric.data {
                Some(metric::Data::Gauge(gauge)) => &gauge.data_points,
                Some(metric::Data::Sum(sum)) => &sum.data_points,
                _ => return 0,
            };
            points
                .iter()
                .map(|point| {
                    point.attributes.capacity()
                        * std::mem::size_of::<
                            otel_arrow_rust::proto::opentelemetry::common::v1::KeyValue,
                        >()
                })
                .sum::<usize>()
        })
        .sum()
}

criterion_group!(benches, decode_metrics);
criterion_main!(benches);
