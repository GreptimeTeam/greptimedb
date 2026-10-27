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

use otel_arrow_rust::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
use otel_arrow_rust::proto::opentelemetry::common::v1::{AnyValue, KeyValue, any_value};
use otel_arrow_rust::proto::opentelemetry::metrics::v1::{
    AggregationTemporality, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram,
    HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
    exponential_histogram_data_point::Buckets, metric, number_data_point,
};
use otel_arrow_rust::proto::opentelemetry::resource::v1::Resource;

// Match the server allocator when measuring allocation-heavy conversion and validation.
#[cfg(not(windows))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

pub struct Fixture {
    pub name: &'static str,
    pub request: ExportMetricsServiceRequest,
    pub points: u64,
    pub rejected: i64,
}

pub fn fixtures() -> Vec<Fixture> {
    [
        ("gauge_1", 1, 8),
        ("gauge_1024", 1024, 8),
        ("wide_1024", 1024, 64),
        ("histogram_1024", 1024, 8),
        ("cumulative_1024", 1024, 8),
        ("mixed_1024", 1024, 8),
        ("required_tag_1024", 1024, 8),
        ("delta_1024", 1024, 8),
    ]
    .into_iter()
    .map(|(name, points, labels)| {
        let attributes = |index: u64| {
            (0..labels)
                .filter(|&label| {
                    !(name == "required_tag_1024" && label == 0 && index.is_multiple_of(10))
                })
                .map(|label| {
                    attribute(&format!("label_{label}"), &format!("value_{}", index % 100))
                })
                .collect()
        };
        let time = |index| 60_000_000_000 + index * 1_000_000;
        let data = if name.starts_with("histogram") {
            metric::Data::Histogram(Histogram {
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                data_points: (0..points)
                    .map(|index| HistogramDataPoint {
                        attributes: attributes(index),
                        time_unix_nano: time(index),
                        count: 11,
                        sum: Some(55.0),
                        bucket_counts: vec![1; 11],
                        explicit_bounds: (1..=10).map(f64::from).collect(),
                        ..Default::default()
                    })
                    .collect(),
            })
        } else if name.starts_with("cumulative")
            || name.starts_with("mixed")
            || name.starts_with("delta")
        {
            metric::Data::ExponentialHistogram(ExponentialHistogram {
                aggregation_temporality: if name.starts_with("delta") {
                    AggregationTemporality::Delta
                } else {
                    AggregationTemporality::Cumulative
                } as i32,
                data_points: (0..points)
                    .map(|index| ExponentialHistogramDataPoint {
                        attributes: attributes(index),
                        time_unix_nano: time(index),
                        count: 16,
                        sum: Some(100.0),
                        scale: if name.starts_with("mixed") && index.is_multiple_of(10) {
                            -5
                        } else {
                            0
                        },
                        zero_threshold: 0.001,
                        positive: Some(Buckets {
                            offset: 0,
                            bucket_counts: vec![1; 16],
                        }),
                        ..Default::default()
                    })
                    .collect(),
            })
        } else {
            metric::Data::Gauge(Gauge {
                data_points: (0..points)
                    .map(|index| NumberDataPoint {
                        attributes: attributes(index),
                        time_unix_nano: time(index),
                        value: Some(number_data_point::Value::AsDouble(index as f64)),
                        ..Default::default()
                    })
                    .collect(),
            })
        };
        Fixture {
            name,
            points,
            rejected: if name.starts_with("mixed") {
                points.div_ceil(10) as i64
            } else {
                0
            },
            request: ExportMetricsServiceRequest {
                resource_metrics: vec![ResourceMetrics {
                    resource: Some(Resource {
                        attributes: vec![
                            attribute("service.name", "bench"),
                            attribute("host.id", "host"),
                        ],
                        ..Default::default()
                    }),
                    scope_metrics: vec![ScopeMetrics {
                        metrics: vec![Metric {
                            name: name.into(),
                            data: Some(data),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            },
        }
    })
    .collect()
}

fn attribute(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.into())),
        }),
    }
}
