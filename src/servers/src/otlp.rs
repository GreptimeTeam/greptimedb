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

use api::v1::{RowInsertRequests, Value};
use common_grpc::writer::Precision;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{metric, number_data_point, *};

use crate::error::Result;
use crate::row_writer::{self, MultiTableData, TableData};

const GREPTIME_TIMESTAMP: &str = "greptime_timestamp";
const GREPTIME_VALUE: &str = "greptime_value";
const GREPTIME_COUNT: &str = "greptime_count";
/// the default column count for table writer
const APPROXIMATE_COLUMN_COUNT: usize = 8;

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
) -> Result<(RowInsertRequests, usize)> {
    let mut table_writer = MultiTableData::default();

    for resource in request.resource_metrics {
        let resource_attrs = resource.resource.map(|r| r.attributes);
        for scope in resource.scope_metrics {
            let scope_attrs = scope.scope.map(|s| s.attributes);
            for metric in scope.metrics {
                encode_metrics(
                    &mut table_writer,
                    &metric,
                    resource_attrs.as_ref(),
                    scope_attrs.as_ref(),
                )?;
            }
        }
    }

    Ok(table_writer.into_row_insert_requests())
}

fn encode_metrics<'a>(
    table_writer: &mut MultiTableData<'a>,
    metric: &'a Metric,
    resource_attrs: Option<&'a Vec<KeyValue>>,
    scope_attrs: Option<&'a Vec<KeyValue>>,
) -> Result<()> {
    let name = &metric.name;
    // note that we don't store description or unit, we might want to deal with
    // these fields in the future.
    if let Some(data) = &metric.data {
        match data {
            metric::Data::Gauge(gauge) => {
                encode_gauge(table_writer, name, gauge, resource_attrs, scope_attrs)?;
            }
            metric::Data::Sum(sum) => {
                encode_sum(table_writer, name, sum, resource_attrs, scope_attrs)?;
            }
            metric::Data::Summary(summary) => {
                encode_summary(table_writer, name, summary, resource_attrs, scope_attrs)?;
            }
            metric::Data::Histogram(hist) => {
                encode_histogram(table_writer, name, hist, resource_attrs, scope_attrs)?;
            }
            // TODO(sunng87) leave ExponentialHistogram for next release
            metric::Data::ExponentialHistogram(_hist) => {}
        }
    }

    Ok(())
}

fn write_attributes<'a>(
    writer: &mut TableData<'a>,
    row: &mut Vec<Value>,
    attrs: Option<&'a Vec<KeyValue>>,
) -> Result<()> {
    if let Some(attrs) = attrs {
        let table_tags = attrs.iter().filter_map(|attr| {
            if let Some(val) = attr.value.as_ref().and_then(|v| v.value.as_ref()) {
                match val {
                    any_value::Value::StringValue(s) => Some((attr.key.as_str(), s.as_str())),
                    any_value::Value::IntValue(v) => Some((attr.key.as_str(), v.to_string())),
                    any_value::Value::DoubleValue(v) => Some((attr.key.as_str(), v.to_string())),
                    _ => None, // TODO(sunng87): allow different type of values
                }
            } else {
                None
            }
        });

        row_writer::write_tags(writer, table_tags, row)?;
    }
    Ok(())
}

fn write_timestamp<'a>(
    lines: &mut TableData<'a>,
    row: &mut Vec<Value>,
    time_nano: i64,
) -> Result<()> {
    row_writer::write_ts_precision(
        lines,
        GREPTIME_TIMESTAMP,
        Some(time_nano),
        Precision::Nanosecond,
        row,
    )
}

fn write_data_point_value<'a>(
    lines: &mut TableData<'a>,
    row: &mut Vec<Value>,
    field: &'a str,
    value: &Option<number_data_point::Value>,
) -> Result<()> {
    match value {
        Some(number_data_point::Value::AsInt(val)) => {
            // we coerce all values to f64
            row_writer::write_f64(lines, field, *val as f64, row)?;
        }
        Some(number_data_point::Value::AsDouble(val)) => {
            row_writer::write_f64(lines, field, *val, row)?;
        }
        _ => {}
    }
    Ok(())
}

fn write_tags_and_timestamp<'a>(
    lines: &mut TableData<'a>,
    row: &mut Vec<Value>,
    resource_attrs: Option<&Vec<KeyValue>>,
    scope_attrs: Option<&Vec<KeyValue>>,
    data_point_attrs: Option<&Vec<KeyValue>>,
    timestamp_nanos: i64,
) -> Result<()> {
    write_attributes(lines, row, resource_attrs)?;
    write_attributes(lines, row, scope_attrs)?;
    write_attributes(lines, row, data_point_attrs)?;

    write_timestamp(lines, row, timestamp_nanos)?;

    Ok(())
}

/// encode this gauge metric
///
/// note that there can be multiple data points in the request, it's going to be
/// stored as multiple rows
fn encode_gauge<'a>(
    table_writer: &mut MultiTableData<'a>,
    name: &str,
    gauge: &Gauge,
    resource_attrs: Option<&Vec<KeyValue>>,
    scope_attrs: Option<&Vec<KeyValue>>,
) -> Result<()> {
    let mut lines = table_writer.get_or_default_table_data(
        &normalize_otlp_name(name),
        APPROXIMATE_COLUMN_COUNT,
        gauge.data_points.len(),
    );

    for data_point in &gauge.data_points {
        let mut row = lines.alloc_one_row();
        write_tags_and_timestamp(
            &mut lines,
            &mut row,
            resource_attrs,
            scope_attrs,
            Some(data_point.attributes.as_ref()),
            data_point.time_unix_nano as i64,
        )?;

        write_data_point_value(&mut lines, &mut row, GREPTIME_VALUE, &data_point.value)?;
    }

    Ok(())
}

/// encode this sum metric
///
/// `aggregation_temporality` and `monotonic` are ignored for now
fn encode_sum<'a>(
    table_writer: &mut MultiTableData<'a>,
    name: &str,
    sum: &Sum,
    resource_attrs: Option<&Vec<KeyValue>>,
    scope_attrs: Option<&Vec<KeyValue>>,
) -> Result<()> {
    let mut lines = table_writer.get_or_default_table_data(
        &normalize_otlp_name(name),
        APPROXIMATE_COLUMN_COUNT,
        sum.data_points.len(),
    );

    for data_point in &sum.data_points {
        let mut row = lines.alloc_one_row();
        write_tags_and_timestamp(
            &mut lines,
            &mut row,
            resource_attrs,
            scope_attrs,
            Some(data_point.attributes.as_ref()),
            data_point.time_unix_nano as i64,
        )?;
        write_data_point_value(&mut lines, &mut row, GREPTIME_VALUE, &data_point.value)?;
    }

    Ok(())
}

const HISTOGRAM_LE_COLUMN: &str = "le";

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
fn encode_histogram<'a>(
    table_writer: &mut MultiTableData<'a>,
    name: &str,
    hist: &Histogram,
    resource_attrs: Option<&Vec<KeyValue>>,
    scope_attrs: Option<&Vec<KeyValue>>,
) -> Result<()> {
    let normalized_name = normalize_otlp_name(name);

    let bucket_table_name = format!("{}_bucket", normalized_name);
    let sum_table_name = format!("{}_sum", normalized_name);
    let count_table_name = format!("{}_count", normalized_name);

    let data_points_len = hist.data_points.len();
    let mut bucket_lines = table_writer.get_or_default_table_data(
        &bucket_table_name,
        APPROXIMATE_COLUMN_COUNT,
        data_points_len,
    );
    let mut sum_lines = table_writer.get_or_default_table_data(
        &sum_table_name,
        APPROXIMATE_COLUMN_COUNT,
        data_points_len,
    );
    let mut count_lines = table_writer.get_or_default_table_data(
        &count_table_name,
        APPROXIMATE_COLUMN_COUNT,
        data_points_len,
    );

    for data_point in &hist.data_points {
        let mut bucket_row = bucket_lines.alloc_one_row();
        let mut accumulated_count = 0;
        for (idx, count) in data_point.bucket_counts.iter().enumerate() {
            write_tags_and_timestamp(
                &mut bucket_lines,
                &mut bucket_row,
                resource_attrs,
                scope_attrs,
                Some(data_point.attributes.as_ref()),
                data_point.time_unix_nano as i64,
            )?;

            if let Some(upper_bounds) = data_point.explicit_bounds.get(idx) {
                row_writer::write_tag(
                    bucket_lines,
                    HISTOGRAM_LE_COLUMN,
                    upper_bounds,
                    &mut bucket_row,
                )?;
            } else if idx == data_point.explicit_bounds.len() {
                // The last bucket
                row_writer::write_tag(
                    bucket_lines,
                    HISTOGRAM_LE_COLUMN,
                    f64::INFINITY,
                    &mut bucket_row,
                )?;
            }

            accumulated_count += count;
            row_writer::write_f64(
                &mut bucket_lines,
                GREPTIME_VALUE,
                accumulated_count as f64,
                &mut bucket_row,
            )?;
        }

        if let Some(sum) = data_point.sum {
            let mut sum_row = sum_lines.alloc_one_row();
            write_tags_and_timestamp(
                &mut sum_lines,
                &mut sum_row,
                resource_attrs,
                scope_attrs,
                Some(data_point.attributes.as_ref()),
                data_point.time_unix_nano as i64,
            )?;

            row_writer::write_f64(&mut sum_lines, GREPTIME_VALUE, sum, &mut sum_row)?;
        }

        let mut count_row = count_lines.alloc_one_row();
        write_tags_and_timestamp(
            &mut count_lines,
            &mut count_row,
            resource_attrs,
            scope_attrs,
            Some(data_point.attributes.as_ref()),
            data_point.time_unix_nano as i64,
        )?;

        row_writer::write_f64(
            &mut count_lines,
            GREPTIME_VALUE,
            data_point.count as f64,
            &mut count_row,
        )?;
    }

    Ok(())
}

#[allow(dead_code)]
fn encode_exponential_histogram(_name: &str, _hist: &ExponentialHistogram) -> Result<()> {
    // TODO(sunng87): implement this using a prometheus compatible way
    Ok(())
}

fn encode_summary<'a>(
    table_writer: &mut MultiTableData<'a>,
    name: &str,
    summary: &Summary,
    resource_attrs: Option<&Vec<KeyValue>>,
    scope_attrs: Option<&Vec<KeyValue>>,
) -> Result<()> {
    let mut lines = table_writer.get_or_default_table_data(
        &normalize_otlp_name(name),
        APPROXIMATE_COLUMN_COUNT,
        summary.data_points.len(),
    );

    for data_point in &summary.data_points {
        let mut row = lines.alloc_one_row();
        write_tags_and_timestamp(
            &mut lines,
            &mut row,
            resource_attrs,
            scope_attrs,
            Some(data_point.attributes.as_ref()),
            data_point.time_unix_nano as i64,
        )?;

        for quantile in &data_point.quantile_values {
            row_writer::write_f64(
                &mut lines,
                &format!("greptime_p{:02}", quantile.quantile * 100f64),
                quantile.value,
                &mut row,
            )?;
        }

        row_writer::write_f64(
            &mut lines,
            GREPTIME_COUNT,
            data_point.count as f64,
            &mut row,
        )?;
    }

    Ok(())
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
