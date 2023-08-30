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
/// <https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/api.md#instrument-name-syntax>
/// - since the name are case-insensitive, we transform them to lowercase for
/// better sql usability
/// - replace `.` and `-` with `_`
fn normalize_otlp_name(name: &str) -> String {
    name.to_lowercase().replace(|c| c == '.' || c == '-', "_")
}

/// Convert OpenTelemetry metrics to GreptimeDB insert requests
///
/// See
/// <https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto#L162>
/// for data structure of OTLP metrics.
///
/// Returns `InsertRequests` and total number of rows to ingest
pub fn to_grpc_insert_requests(
    request: ExportMetricsServiceRequest,
) -> Result<(InsertRequests, usize)> {
    let mut insert_batch = Vec::new();

    for resource in request.resource_metrics {
        let resource_attrs = resource.resource.map(|r| r.attributes);
        for scope in resource.scope_metrics {
            let scope_attrs = scope.scope.map(|s| s.attributes);
            for metric in scope.metrics {
                let inserts =
                    encode_metrics(&metric, resource_attrs.as_ref(), scope_attrs.as_ref())?;

                insert_batch.extend(inserts);
            }
        }
    }

    let rows = insert_batch
        .iter()
        .map(|i| i.row_count as usize)
        .sum::<usize>();
    let inserts = InsertRequests {
        inserts: insert_batch,
    };

    Ok((inserts, rows))
}

fn encode_metrics(
    metric: &Metric,
    resource_attrs: Option<&Vec<KeyValue>>,
    scope_attrs: Option<&Vec<KeyValue>>,
) -> Result<Vec<InsertRequest>> {
    let name = &metric.name;
    // note that we don't store description or unit, we might want to deal with
    // these fields in the future.
    if let Some(data) = &metric.data {
        match data {
            metric::Data::Gauge(gauge) => {
                encode_gauge(name, gauge, resource_attrs, scope_attrs).map(|i| vec![i])
            }
            metric::Data::Sum(sum) => {
                encode_sum(name, sum, resource_attrs, scope_attrs).map(|i| vec![i])
            }
            metric::Data::Summary(summary) => {
                encode_summary(name, summary, resource_attrs, scope_attrs).map(|i| vec![i])
            }
            metric::Data::Histogram(hist) => {
                encode_histogram(name, hist, resource_attrs, scope_attrs)
            }
            // TODO(sunng87) leave ExponentialHistogram for next release
            metric::Data::ExponentialHistogram(_hist) => Ok(vec![]),
        }
    } else {
        Ok(vec![])
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
                .write_tag(&normalize_otlp_name(&attr.key), s)
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
        write_tags_and_timestamp(
            &mut lines,
            resource_attrs,
            scope_attrs,
            Some(data_point.attributes.as_ref()),
            data_point.time_unix_nano as i64,
        )?;

        write_data_point_value(&mut lines, GREPTIME_VALUE, &data_point.value)?;

        lines.commit();
    }

    Ok(insert_request_from_lines(lines, normalize_otlp_name(name)))
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
        write_tags_and_timestamp(
            &mut lines,
            resource_attrs,
            scope_attrs,
            Some(data_point.attributes.as_ref()),
            data_point.time_unix_nano as i64,
        )?;
        write_data_point_value(&mut lines, GREPTIME_VALUE, &data_point.value)?;

        lines.commit();
    }

    Ok(insert_request_from_lines(lines, normalize_otlp_name(name)))
}

const HISTOGRAM_LE_COLUMN: &str = "le";

fn write_tags_and_timestamp(
    lines: &mut LinesWriter,
    resource_attrs: Option<&Vec<KeyValue>>,
    scope_attrs: Option<&Vec<KeyValue>>,
    data_point_attrs: Option<&Vec<KeyValue>>,
    timestamp_nanos: i64,
) -> Result<()> {
    write_attributes(lines, resource_attrs)?;
    write_attributes(lines, scope_attrs)?;
    write_attributes(lines, data_point_attrs)?;

    write_timestamp(lines, timestamp_nanos)?;

    Ok(())
}

/// Encode histogram data. This function returns 3 insert requests for 3 tables.
///
/// The implementation has been following Prometheus histogram table format:
///
/// - A `%metric%_bucket` table including `le` tag that stores bucket upper
/// limit, and `greptime_value` for bucket count
/// - A `%metric%_sum` table storing sum of samples
/// -  A `%metric%_count` table storing count of samples.
///
/// By its Prometheus compatibility, we hope to be able to use prometheus
/// quantile functions on this table.
fn encode_histogram(
    name: &str,
    hist: &Histogram,
    resource_attrs: Option<&Vec<KeyValue>>,
    scope_attrs: Option<&Vec<KeyValue>>,
) -> Result<Vec<InsertRequest>> {
    let normalized_name = normalize_otlp_name(name);
    let bucket_table_name = format!("{}_bucket", normalized_name);
    let sum_table_name = format!("{}_sum", normalized_name);
    let count_table_name = format!("{}_count", normalized_name);

    let data_points_len = hist.data_points.len();
    let mut bucket_lines =
        LinesWriter::with_lines(hist.data_points.iter().map(|p| p.bucket_counts.len()).sum());
    let mut sum_lines = LinesWriter::with_lines(data_points_len);
    let mut count_lines = LinesWriter::with_lines(data_points_len);

    for data_point in &hist.data_points {
        for (idx, count) in data_point.bucket_counts.iter().enumerate() {
            write_tags_and_timestamp(
                &mut bucket_lines,
                resource_attrs,
                scope_attrs,
                Some(data_point.attributes.as_ref()),
                data_point.time_unix_nano as i64,
            )?;

            if let Some(upper_bounds) = data_point.explicit_bounds.get(idx) {
                bucket_lines
                    .write_tag(HISTOGRAM_LE_COLUMN, &upper_bounds.to_string())
                    .context(error::OtlpMetricsWriteSnafu)?;
            } else if idx == data_point.explicit_bounds.len() {
                // The last bucket
                bucket_lines
                    .write_tag(HISTOGRAM_LE_COLUMN, &f64::INFINITY.to_string())
                    .context(error::OtlpMetricsWriteSnafu)?;
            }

            bucket_lines
                .write_u64(GREPTIME_VALUE, *count)
                .context(error::OtlpMetricsWriteSnafu)?;

            bucket_lines.commit();
        }

        if let Some(sum) = data_point.sum {
            write_tags_and_timestamp(
                &mut sum_lines,
                resource_attrs,
                scope_attrs,
                Some(data_point.attributes.as_ref()),
                data_point.time_unix_nano as i64,
            )?;

            sum_lines
                .write_f64(GREPTIME_VALUE, sum)
                .context(error::OtlpMetricsWriteSnafu)?;
            sum_lines.commit();
        }

        write_tags_and_timestamp(
            &mut count_lines,
            resource_attrs,
            scope_attrs,
            Some(data_point.attributes.as_ref()),
            data_point.time_unix_nano as i64,
        )?;

        count_lines
            .write_u64(GREPTIME_VALUE, data_point.count)
            .context(error::OtlpMetricsWriteSnafu)?;

        count_lines.commit();
    }

    let bucket_insert = insert_request_from_lines(bucket_lines, bucket_table_name);
    let sum_insert = insert_request_from_lines(sum_lines, sum_table_name);
    let count_insert = insert_request_from_lines(count_lines, count_table_name);

    Ok(vec![bucket_insert, sum_insert, count_insert])
}

fn insert_request_from_lines(lines: LinesWriter, name: String) -> InsertRequest {
    let (columns, row_count) = lines.finish();
    InsertRequest {
        table_name: name,
        columns,
        row_count,
    }
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
        write_tags_and_timestamp(
            &mut lines,
            resource_attrs,
            scope_attrs,
            Some(data_point.attributes.as_ref()),
            data_point.time_unix_nano as i64,
        )?;

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

    Ok(insert_request_from_lines(lines, normalize_otlp_name(name)))
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::common::v1::any_value::Value as Val;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
    use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value;
    use opentelemetry_proto::tonic::metrics::v1::summary_data_point::ValueAtQuantile;
    use opentelemetry_proto::tonic::metrics::v1::{HistogramDataPoint, NumberDataPoint};

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

    #[test]
    fn test_encode_histogram() {
        let data_points = vec![HistogramDataPoint {
            attributes: vec![keyvalue("host", "testserver")],
            time_unix_nano: 100,
            start_time_unix_nano: 23,
            count: 25,
            sum: Some(100.),
            max: Some(200.),
            min: Some(0.03),
            bucket_counts: vec![2, 4, 6, 9, 4],
            explicit_bounds: vec![0.1, 1., 10., 100.],
            ..Default::default()
        }];

        let histogram = Histogram {
            data_points,
            aggregation_temporality: AggregationTemporality::Delta.into(),
        };
        let inserts = encode_histogram(
            "histo",
            &histogram,
            Some(&vec![keyvalue("resource", "app")]),
            Some(&vec![keyvalue("scope", "otel")]),
        )
        .unwrap();

        assert_eq!(3, inserts.len());

        // bucket table
        assert_eq!(inserts[0].table_name, "histo_bucket");
        assert_eq!(inserts[0].row_count, 5);
        assert_eq!(inserts[0].columns.len(), 6);
        assert_eq!(
            inserts[0]
                .columns
                .iter()
                .map(|c| &c.column_name)
                .collect::<Vec<&String>>(),
            vec![
                "resource",
                "scope",
                "host",
                "greptime_timestamp",
                "le",
                "greptime_value",
            ]
        );

        assert_eq!(inserts[1].table_name, "histo_sum");
        assert_eq!(inserts[1].row_count, 1);
        assert_eq!(inserts[1].columns.len(), 5);
        assert_eq!(
            inserts[1]
                .columns
                .iter()
                .map(|c| &c.column_name)
                .collect::<Vec<&String>>(),
            vec![
                "resource",
                "scope",
                "host",
                "greptime_timestamp",
                "greptime_value",
            ]
        );

        assert_eq!(inserts[2].table_name, "histo_count");
        assert_eq!(inserts[2].row_count, 1);
        assert_eq!(inserts[2].columns.len(), 5);
        assert_eq!(
            inserts[2]
                .columns
                .iter()
                .map(|c| &c.column_name)
                .collect::<Vec<&String>>(),
            vec![
                "resource",
                "scope",
                "host",
                "greptime_timestamp",
                "greptime_value",
            ]
        );
    }
}
