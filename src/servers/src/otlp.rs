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

use api::v1::{InsertRequest, InsertRequests};
use common_grpc::writer::{LinesWriter, Precision};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{metric, number_data_point, *};
use snafu::ResultExt;

use crate::error::{self, Result};

const GREPTIME_TIMESTAMP: &str = "greptime_timestamp";
const GREPTIME_VALUE: &str = "greptime_value";

/// Normalize otlp instrumentation, metric and attribute names
///
/// https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/api.md#instrument-name-syntax
/// - since the name are case-insensitive, we transform them to lowercase for
/// better sql usability
/// - replace `.` and `-` with `_`
fn normalize_otlp_name(name: &str) -> String {
    name.to_lowercase().replace(|c| c == '.' || c == '-', "_")
}

/// Convert OpenTelemetry metrics to GreptimeDB insert requests
///
/// See
/// https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto#L162
/// for data structure of OTLP metrics.
///
/// Returns `InsertRequests` and total number of rows to ingest
pub fn to_grpc_insert_requests(
    request: ExportMetricsServiceRequest,
) -> Result<(InsertRequests, usize)> {
    let mut insert_batch = Vec::new();
    let mut rows = 0;

    for resource in request.resource_metrics {
        let resource_attrs = resource.resource.map(|r| r.attributes);
        for scope in resource.scope_metrics {
            let scope_attrs = scope.scope.map(|s| s.attributes);
            for metric in scope.metrics {
                if let Some(insert) =
                    encode_metrics(&metric, resource_attrs.as_ref(), scope_attrs.as_ref())?
                {
                    rows += insert.row_count;
                    insert_batch.push(insert);
                }
            }
        }
    }

    let inserts = InsertRequests {
        inserts: insert_batch,
    };

    Ok((inserts, rows as usize))
}

fn encode_metrics(
    metric: &Metric,
    resource_attrs: Option<&Vec<KeyValue>>,
    scope_attrs: Option<&Vec<KeyValue>>,
) -> Result<Option<InsertRequest>> {
    let name = &metric.name;
    // note that we don't store description or unit, we might want to deal with
    // these fields in the future.
    if let Some(data) = &metric.data {
        match data {
            metric::Data::Gauge(gauge) => {
                encode_gauge(name, gauge, resource_attrs, scope_attrs).map(Some)
            }
            metric::Data::Sum(sum) => encode_sum(name, sum, resource_attrs, scope_attrs).map(Some),
            metric::Data::Summary(summary) => {
                encode_summary(name, summary, resource_attrs, scope_attrs).map(Some)
            }
            // TODO(sunng87) leave histogram for next release
            metric::Data::Histogram(_hist) => Ok(None),
            metric::Data::ExponentialHistogram(_hist) => Ok(None),
        }
    } else {
        Ok(None)
    }
}

fn write_attributes(lines: &mut LinesWriter, attrs: Option<&Vec<KeyValue>>) -> Result<()> {
    if let Some(attrs) = attrs {
        for attr in attrs {
            write_attribute(lines, attr)?;
        }
    }
    Ok(())
}

fn write_attribute(lines: &mut LinesWriter, attr: &KeyValue) -> Result<()> {
    if let Some(val) = attr.value.as_ref().and_then(|v| v.value.as_ref()) {
        match val {
            any_value::Value::StringValue(s) => lines
                .write_tag(&attr.key, s)
                .context(error::OtlpMetricsWriteSnafu)?,

            any_value::Value::IntValue(v) => lines
                .write_tag(&normalize_otlp_name(&attr.key), &v.to_string())
                .context(error::OtlpMetricsWriteSnafu)?,
            any_value::Value::DoubleValue(v) => lines
                .write_tag(&normalize_otlp_name(&attr.key), &v.to_string())
                .context(error::OtlpMetricsWriteSnafu)?,
            // TODO(sunng87): allow different type of values
            _ => {}
        }
    }

    Ok(())
}

fn write_timestamp(lines: &mut LinesWriter, time_nano: i64) -> Result<()> {
    lines
        .write_ts(GREPTIME_TIMESTAMP, (time_nano, Precision::Nanosecond))
        .context(error::OtlpMetricsWriteSnafu)?;
    Ok(())
}

fn write_data_point_value(
    lines: &mut LinesWriter,
    field: &str,
    value: &Option<number_data_point::Value>,
) -> Result<()> {
    match value {
        Some(number_data_point::Value::AsInt(val)) => {
            // we coerce all values to f64
            lines
                .write_f64(field, *val as f64)
                .context(error::OtlpMetricsWriteSnafu)?
        }
        Some(number_data_point::Value::AsDouble(val)) => lines
            .write_f64(field, *val)
            .context(error::OtlpMetricsWriteSnafu)?,
        _ => {}
    }
    Ok(())
}

/// encode this gauge metric
///
/// note that there can be multiple data points in the request, it's going to be
/// stored as multiple rows
fn encode_gauge(
    name: &str,
    gauge: &Gauge,
    resource_attrs: Option<&Vec<KeyValue>>,
    scope_attrs: Option<&Vec<KeyValue>>,
) -> Result<InsertRequest> {
    let mut lines = LinesWriter::with_lines(gauge.data_points.len());
    for data_point in &gauge.data_points {
        write_attributes(&mut lines, resource_attrs)?;
        write_attributes(&mut lines, scope_attrs)?;
        write_attributes(&mut lines, Some(data_point.attributes.as_ref()))?;
        write_timestamp(&mut lines, data_point.time_unix_nano as i64)?;
        write_data_point_value(&mut lines, GREPTIME_VALUE, &data_point.value)?;

        lines.commit();
    }

    let (columns, row_count) = lines.finish();
    Ok(InsertRequest {
        table_name: normalize_otlp_name(name),
        region_number: 0,
        columns,
        row_count,
    })
}

/// encode this sum metric
///
/// `aggregation_temporality` and `monotonic` are ignored for now
fn encode_sum(
    name: &str,
    sum: &Sum,
    resource_attrs: Option<&Vec<KeyValue>>,
    scope_attrs: Option<&Vec<KeyValue>>,
) -> Result<InsertRequest> {
    let mut lines = LinesWriter::with_lines(sum.data_points.len());

    for data_point in &sum.data_points {
        write_attributes(&mut lines, resource_attrs)?;
        write_attributes(&mut lines, scope_attrs)?;
        write_attributes(&mut lines, Some(data_point.attributes.as_ref()))?;

        write_timestamp(&mut lines, data_point.time_unix_nano as i64)?;

        write_data_point_value(&mut lines, GREPTIME_VALUE, &data_point.value)?;

        lines.commit();
    }

    let (columns, row_count) = lines.finish();
    Ok(InsertRequest {
        table_name: normalize_otlp_name(name),
        region_number: 0,
        columns,
        row_count,
    })
}

// TODO(sunng87): we may need better implementation for histogram
#[allow(dead_code)]
fn encode_histogram(name: &str, hist: &Histogram) -> Result<InsertRequest> {
    let mut lines = LinesWriter::with_lines(hist.data_points.len());

    for data_point in &hist.data_points {
        for attr in &data_point.attributes {
            write_attribute(&mut lines, attr)?;
        }

        write_timestamp(&mut lines, data_point.time_unix_nano as i64)?;

        for (idx, count) in data_point.bucket_counts.iter().enumerate() {
            // here we don't store bucket boundary
            lines
                .write_u64(&format!("bucket_{}", idx), *count)
                .context(error::OtlpMetricsWriteSnafu)?;
        }

        if let Some(min) = data_point.min {
            lines
                .write_f64("min", min)
                .context(error::OtlpMetricsWriteSnafu)?;
        }

        if let Some(max) = data_point.max {
            lines
                .write_f64("max", max)
                .context(error::OtlpMetricsWriteSnafu)?;
        }

        lines.commit();
    }

    let (columns, row_count) = lines.finish();
    Ok(InsertRequest {
        table_name: normalize_otlp_name(name),
        region_number: 0,
        columns,
        row_count,
    })
}

#[allow(dead_code)]
fn encode_exponential_histogram(name: &str, hist: &ExponentialHistogram) -> Result<InsertRequest> {
    let mut lines = LinesWriter::with_lines(hist.data_points.len());

    for data_point in &hist.data_points {
        for attr in &data_point.attributes {
            write_attribute(&mut lines, attr)?;
        }

        write_timestamp(&mut lines, data_point.time_unix_nano as i64)?;

        // TODO(sunng87): confirm if this working
        if let Some(positive_buckets) = &data_point.positive {
            for (idx, count) in positive_buckets.bucket_counts.iter().enumerate() {
                // here we don't store bucket boundary
                lines
                    .write_u64(
                        &format!("bucket_{}", idx + positive_buckets.offset as usize),
                        *count,
                    )
                    .context(error::OtlpMetricsWriteSnafu)?;
            }
        }

        if let Some(negative_buckets) = &data_point.negative {
            for (idx, count) in negative_buckets.bucket_counts.iter().enumerate() {
                lines
                    .write_u64(
                        &format!("bucket_{}", idx + negative_buckets.offset as usize),
                        *count,
                    )
                    .context(error::OtlpMetricsWriteSnafu)?;
            }
        }

        if let Some(min) = data_point.min {
            lines
                .write_f64("min", min)
                .context(error::OtlpMetricsWriteSnafu)?;
        }

        if let Some(max) = data_point.max {
            lines
                .write_f64("max", max)
                .context(error::OtlpMetricsWriteSnafu)?;
        }

        lines.commit();
    }

    let (columns, row_count) = lines.finish();
    Ok(InsertRequest {
        table_name: normalize_otlp_name(name),
        region_number: 0,
        columns,
        row_count,
    })
}

fn encode_summary(
    name: &str,
    summary: &Summary,
    resource_attrs: Option<&Vec<KeyValue>>,
    scope_attrs: Option<&Vec<KeyValue>>,
) -> Result<InsertRequest> {
    let mut lines = LinesWriter::with_lines(summary.data_points.len());

    for data_point in &summary.data_points {
        write_attributes(&mut lines, resource_attrs)?;
        write_attributes(&mut lines, scope_attrs)?;
        write_attributes(&mut lines, Some(data_point.attributes.as_ref()))?;

        write_timestamp(&mut lines, data_point.time_unix_nano as i64)?;

        for quantile in &data_point.quantile_values {
            // here we don't store bucket boundary
            lines
                .write_f64(
                    &format!("greptime_p{:02}", quantile.quantile * 100f64),
                    quantile.value,
                )
                .context(error::OtlpMetricsWriteSnafu)?;
        }

        lines
            .write_u64("greptime_count", data_point.count)
            .context(error::OtlpMetricsWriteSnafu)?;

        lines.commit();
    }

    let (columns, row_count) = lines.finish();
    Ok(InsertRequest {
        table_name: normalize_otlp_name(name),
        region_number: 0,
        columns,
        row_count,
    })
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::common::v1::any_value::Value as Val;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
    use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value;
    use opentelemetry_proto::tonic::metrics::v1::summary_data_point::ValueAtQuantile;
    use opentelemetry_proto::tonic::metrics::v1::NumberDataPoint;

    use super::*;

    #[test]
    fn test_normalize_otlp_name() {
        assert_eq!(normalize_otlp_name("jvm.memory.free"), "jvm_memory_free");
        assert_eq!(normalize_otlp_name("jvm-memory-free"), "jvm_memory_free");
        assert_eq!(normalize_otlp_name("jvm_memory_free"), "jvm_memory_free");
        assert_eq!(normalize_otlp_name("JVM_MEMORY_FREE"), "jvm_memory_free");
        assert_eq!(normalize_otlp_name("JVM_memory_FREE"), "jvm_memory_free");
    }

    fn keyvalue(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(Val::StringValue(value.into())),
            }),
        }
    }

    #[test]
    fn test_encode_gauge() {
        let data_points = vec![
            NumberDataPoint {
                attributes: vec![keyvalue("host", "testsevrer")],
                time_unix_nano: 100,
                value: Some(Value::AsInt(100)),
                ..Default::default()
            },
            NumberDataPoint {
                attributes: vec![keyvalue("host", "testserver")],
                time_unix_nano: 105,
                value: Some(Value::AsInt(105)),
                ..Default::default()
            },
        ];
        let gauge = Gauge { data_points };
        let inserts = encode_gauge(
            "datamon",
            &gauge,
            Some(&vec![keyvalue("resource", "app")]),
            Some(&vec![keyvalue("scope", "otel")]),
        )
        .unwrap();

        assert_eq!(inserts.table_name, "datamon");
        assert_eq!(inserts.row_count, 2);
        assert_eq!(inserts.columns.len(), 5);
        assert_eq!(
            inserts
                .columns
                .iter()
                .map(|c| &c.column_name)
                .collect::<Vec<&String>>(),
            vec![
                "resource",
                "scope",
                "host",
                "greptime_timestamp",
                "greptime_value"
            ]
        );
    }

    #[test]
    fn test_encode_sum() {
        let data_points = vec![
            NumberDataPoint {
                attributes: vec![keyvalue("host", "testserver")],
                time_unix_nano: 100,
                value: Some(Value::AsInt(100)),
                ..Default::default()
            },
            NumberDataPoint {
                attributes: vec![keyvalue("host", "testserver")],
                time_unix_nano: 105,
                value: Some(Value::AsInt(0)),
                ..Default::default()
            },
        ];
        let sum = Sum {
            data_points,
            ..Default::default()
        };
        let inserts = encode_sum(
            "datamon",
            &sum,
            Some(&vec![keyvalue("resource", "app")]),
            Some(&vec![keyvalue("scope", "otel")]),
        )
        .unwrap();

        assert_eq!(inserts.table_name, "datamon");
        assert_eq!(inserts.row_count, 2);
        assert_eq!(inserts.columns.len(), 5);
        assert_eq!(
            inserts
                .columns
                .iter()
                .map(|c| &c.column_name)
                .collect::<Vec<&String>>(),
            vec![
                "resource",
                "scope",
                "host",
                "greptime_timestamp",
                "greptime_value"
            ]
        );
    }

    #[test]
    fn test_encode_summary() {
        let data_points = vec![SummaryDataPoint {
            attributes: vec![keyvalue("host", "testserver")],
            time_unix_nano: 100,
            count: 25,
            sum: 5400.0,
            quantile_values: vec![
                ValueAtQuantile {
                    quantile: 0.90,
                    value: 1000.0,
                },
                ValueAtQuantile {
                    quantile: 0.95,
                    value: 3030.0,
                },
            ],
            ..Default::default()
        }];
        let summary = Summary { data_points };
        let inserts = encode_summary(
            "datamon",
            &summary,
            Some(&vec![keyvalue("resource", "app")]),
            Some(&vec![keyvalue("scope", "otel")]),
        )
        .unwrap();

        assert_eq!(inserts.table_name, "datamon");
        assert_eq!(inserts.row_count, 1);
        assert_eq!(inserts.columns.len(), 7);
        assert_eq!(
            inserts
                .columns
                .iter()
                .map(|c| &c.column_name)
                .collect::<Vec<&String>>(),
            vec![
                "resource",
                "scope",
                "host",
                "greptime_timestamp",
                "greptime_p90",
                "greptime_p95",
                "greptime_count"
            ]
        );
    }
}
