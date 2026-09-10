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

use clap::Args as ClapArgs;
use prost::Message;
use serde_json::json;

use crate::query_perf_fixture::case::ValuePattern;
use crate::query_perf_fixture::prom_remote_write::deterministic_value;

#[derive(Debug, ClapArgs)]
pub(super) struct OtlpMetricsArgs {
    #[arg(long, default_value = "http://127.0.0.1:4000/v1/otlp/v1/metrics")]
    endpoint: String,
    #[arg(long, default_value = "public")]
    database: String,
    #[arg(long)]
    metric: String,
    #[arg(long, default_value_t = 8)]
    series_count: u64,
    #[arg(long, default_value_t = 30)]
    samples_per_series: u64,
    #[arg(long)]
    start_unix_nanos: u64,
    #[arg(long)]
    step_nanos: u64,
    #[arg(long, alias = "batch-size", default_value_t = 8)]
    chunk_series_count: u64,
    #[arg(long, default_value_t = 60)]
    timeout_seconds: u64,
    #[arg(long, default_value_t = ValuePattern::Linear)]
    value_pattern: ValuePattern,
    #[arg(long, default_value_t = 0.0)]
    value_base: f64,
    #[arg(long, default_value_t = 0.125)]
    value_step: f64,
    #[arg(long, default_value_t = 97)]
    value_cardinality: u64,
    #[arg(long, default_value_t = 0)]
    value_seed: u64,
    #[arg(long, default_value_t = 8)]
    value_run_length: u64,
    #[arg(long, default_value_t = 100)]
    value_stall_every: u64,
    #[arg(long, default_value_t = 16)]
    value_stall_length: u64,
    #[arg(long, default_value_t = 5)]
    value_mixed_every: u64,
}

pub(super) async fn run_otlp_metrics(
    args: OtlpMetricsArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let started = std::time::Instant::now();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(args.timeout_seconds))
        .build()?;
    let mut batches = 0_u64;
    let mut rows = 0_u64;
    let mut http_statuses = Vec::new();
    let mut rejected_data_points = 0_i64;
    for first in (0..args.series_count).step_by(args.chunk_series_count.max(1) as usize) {
        let last = (first + args.chunk_series_count.max(1)).min(args.series_count);
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: args.metric.clone(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: (first..last)
                                .flat_map(|series_idx| {
                                    (0..args.samples_per_series).map(move |sample_idx| {
                                        NumberDataPoint {
                                            attributes: vec![
                                                string_attribute(
                                                    "host",
                                                    format!("host{:04}", series_idx % 1024),
                                                ),
                                                string_attribute(
                                                    "instance",
                                                    format!("instance{:06}", series_idx),
                                                ),
                                            ],
                                            time_unix_nano: args.start_unix_nanos
                                                + sample_idx * args.step_nanos,
                                            value: Some(number_data_point::Value::AsDouble(
                                                deterministic_value(
                                                    args.value_pattern,
                                                    args.value_base,
                                                    args.value_step,
                                                    args.value_cardinality,
                                                    args.value_seed,
                                                    args.value_run_length,
                                                    args.value_stall_every,
                                                    args.value_stall_length,
                                                    args.value_mixed_every,
                                                    series_idx,
                                                    sample_idx,
                                                    args.samples_per_series,
                                                ),
                                            )),
                                        }
                                    })
                                })
                                .collect(),
                        })),
                    }],
                }],
            }],
        };
        let batch_rows = (last - first) * args.samples_per_series;
        let response = client
            .post(&args.endpoint)
            .header("content-type", "application/x-protobuf")
            .header("x-greptime-db-name", &args.database)
            .body(request.encode_to_vec())
            .send()
            .await?;
        let status = response.status();
        let bytes = response.bytes().await?;
        http_statuses.push(status.as_u16());
        if !status.is_success() {
            return Err(format!(
                "OTLP metrics export failed with status {status}: {}",
                String::from_utf8_lossy(&bytes)
            )
            .into());
        }
        let response = ExportMetricsServiceResponse::decode(bytes.as_ref())?;
        let rejected = response
            .partial_success
            .map(|partial| partial.rejected_data_points)
            .unwrap_or_default();
        if rejected != 0 {
            return Err(format!("OTLP metrics export rejected {rejected} data points").into());
        }
        rejected_data_points += rejected;
        rows += batch_rows;
        batches += 1;
    }
    println!(
        "{}",
        json!({"status":"ok","endpoint":args.endpoint,"database":args.database,"metric":args.metric,"input_protocol":"otlp_metrics","timestamp_precision":"ns","series_count":args.series_count,"samples_per_series":args.samples_per_series,"rows":rows,"samples_written":rows,"batches":batches,"elapsed_seconds":started.elapsed().as_secs_f64(),"http_statuses":http_statuses,"rejected_data_points":rejected_data_points})
    );
    Ok(())
}

fn string_attribute(key: &str, value: String) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value)),
        }),
    }
}

// The metrics endpoint uses the otel-arrow generated metric messages. Keep this
// small wire encoder local to the fixture binary so production protocol code is
// untouched.
#[derive(Clone, PartialEq, Message)]
struct ExportMetricsServiceRequest {
    #[prost(message, repeated, tag = "1")]
    resource_metrics: Vec<ResourceMetrics>,
}

#[derive(Clone, PartialEq, Message)]
struct ResourceMetrics {
    #[prost(message, repeated, tag = "2")]
    scope_metrics: Vec<ScopeMetrics>,
}

#[derive(Clone, PartialEq, Message)]
struct ScopeMetrics {
    #[prost(message, repeated, tag = "2")]
    metrics: Vec<Metric>,
}

#[derive(Clone, PartialEq, Message)]
struct Metric {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(oneof = "metric::Data", tags = "5")]
    data: Option<metric::Data>,
}

mod metric {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub(super) enum Data {
        #[prost(message, tag = "5")]
        Gauge(super::Gauge),
    }
}

#[derive(Clone, PartialEq, Message)]
struct Gauge {
    #[prost(message, repeated, tag = "1")]
    data_points: Vec<NumberDataPoint>,
}

#[derive(Clone, PartialEq, Message)]
struct NumberDataPoint {
    #[prost(message, repeated, tag = "7")]
    attributes: Vec<KeyValue>,
    #[prost(fixed64, tag = "3")]
    time_unix_nano: u64,
    #[prost(oneof = "number_data_point::Value", tags = "4")]
    value: Option<number_data_point::Value>,
}

mod number_data_point {
    #[derive(Clone, Copy, PartialEq, prost::Oneof)]
    pub(super) enum Value {
        #[prost(double, tag = "4")]
        AsDouble(f64),
    }
}

#[derive(Clone, PartialEq, Message)]
struct KeyValue {
    #[prost(string, tag = "1")]
    key: String,
    #[prost(message, optional, tag = "2")]
    value: Option<AnyValue>,
}

#[derive(Clone, PartialEq, Message)]
struct AnyValue {
    #[prost(oneof = "any_value::Value", tags = "1")]
    value: Option<any_value::Value>,
}

mod any_value {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub(super) enum Value {
        #[prost(string, tag = "1")]
        StringValue(String),
    }
}

#[derive(Clone, PartialEq, Message)]
struct ExportMetricsServiceResponse {
    #[prost(message, optional, tag = "1")]
    partial_success: Option<ExportMetricsPartialSuccess>,
}

#[derive(Clone, PartialEq, Message)]
struct ExportMetricsPartialSuccess {
    #[prost(int64, tag = "1")]
    rejected_data_points: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_gauge_timestamp_with_nanosecond_remainder() {
        let point = NumberDataPoint {
            attributes: vec![string_attribute("host", "host0000".to_string())],
            time_unix_nano: 1_704_067_200_000_000_123,
            value: Some(number_data_point::Value::AsDouble(1.0)),
        };
        let decoded = NumberDataPoint::decode(point.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded.time_unix_nano % 1_000_000_000, 123);
    }

    #[test]
    fn decodes_partial_success_rejections() {
        let response = ExportMetricsServiceResponse {
            partial_success: Some(ExportMetricsPartialSuccess {
                rejected_data_points: 2,
            }),
        };
        assert_eq!(
            ExportMetricsServiceResponse::decode(response.encode_to_vec().as_slice())
                .unwrap()
                .partial_success
                .unwrap()
                .rejected_data_points,
            2
        );
    }
}
