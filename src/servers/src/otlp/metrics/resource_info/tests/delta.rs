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

use otel_arrow_rust::proto::opentelemetry::metrics::v1::{
    DataPointFlags, Histogram, HistogramDataPoint,
};

use super::*;

#[test]
fn observe_ignores_rejected_classic_histogram_windows() {
    let window = SEMANTIC_GRAPH_WINDOW_NANOS;
    let resource = ResourceMetrics {
        scope_metrics: vec![ScopeMetrics {
            metrics: vec![Metric {
                data: Some(metric::Data::Histogram(Histogram {
                    data_points: vec![
                        HistogramDataPoint {
                            time_unix_nano: (window + 1) as u64,
                            count: 1,
                            bucket_counts: vec![1],
                            ..Default::default()
                        },
                        HistogramDataPoint {
                            time_unix_nano: (3 * window + 1) as u64,
                            count: 1,
                            bucket_counts: vec![1],
                            explicit_bounds: vec![1.0, 2.0],
                            ..Default::default()
                        },
                        HistogramDataPoint {
                            time_unix_nano: (4 * window + 1) as u64,
                            count: u64::MAX,
                            bucket_counts: vec![u64::MAX, 1],
                            flags: DataPointFlags::NoRecordedValueMask as u32,
                            ..Default::default()
                        },
                    ],
                    aggregation_temporality: AggregationTemporality::Delta as i32,
                })),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut data = ResourceInfoData::default();
    data.observe(
        &[kv("service.name", "api")],
        &resource,
        &OtlpMetricCtx::default(),
    );

    let windows = data.rows.values().next().unwrap();
    assert_eq!(
        vec![window + 1, 4 * window + 1],
        windows.values().copied().collect::<Vec<_>>()
    );
}
