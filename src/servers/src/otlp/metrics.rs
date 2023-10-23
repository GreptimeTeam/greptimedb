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

use super::{GREPTIME_COUNT, GREPTIME_TIMESTAMP, GREPTIME_VALUE};
use crate::error::Result;
use crate::row_writer::{self, MultiTableData, TableData};

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
/// <https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto>
/// for data structure of OTLP metrics.
///
/// Returns `InsertRequests` and total number of rows to ingest
pub fn to_grpc_insert_requests(
    request: ExportMetricsServiceRequest,
) -> Result<(RowInsertRequests, usize)> {
    let mut table_writer = MultiTableData::default();

    for resource in &request.resource_metrics {
        let resource_attrs = resource.resource.as_ref().map(|r| &r.attributes);
        for scope in &resource.scope_metrics {
            let scope_attrs = scope.scope.as_ref().map(|s| &s.attributes);
            for metric in &scope.metrics {
                encode_metrics(&mut table_writer, metric, resource_attrs, scope_attrs)?;
            }
        }
    }

    Ok(table_writer.into_row_insert_requests())
}

fn encode_metrics(
    table_writer: &mut MultiTableData,
    metric: &Metric,
    resource_attrs: Option<&Vec<KeyValue>>,
    scope_attrs: Option<&Vec<KeyValue>>,
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

fn write_attributes(
    writer: &mut TableData,
    row: &mut Vec<Value>,
    attrs: Option<&Vec<KeyValue>>,
) -> Result<()> {
    if let Some(attrs) = attrs {
        let table_tags = attrs.iter().filter_map(|attr| {
            if let Some(val) = attr.value.as_ref().and_then(|v| v.value.as_ref()) {
                let key = normalize_otlp_name(&attr.key);
                match val {
                    any_value::Value::StringValue(s) => Some((key, s.to_string())),
                    any_value::Value::IntValue(v) => Some((key, v.to_string())),
                    any_value::Value::DoubleValue(v) => Some((key, v.to_string())),
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

fn write_timestamp(table: &mut TableData, row: &mut Vec<Value>, time_nano: i64) -> Result<()> {
    row_writer::write_ts_precision(
        table,
        GREPTIME_TIMESTAMP,
        Some(time_nano),
        Precision::Nanosecond,
        row,
    )
}

fn write_data_point_value(
    table: &mut TableData,
    row: &mut Vec<Value>,
    field: &str,
    value: &Option<number_data_point::Value>,
) -> Result<()> {
    match value {
        Some(number_data_point::Value::AsInt(val)) => {
            // we coerce all values to f64
            row_writer::write_f64(table, field, *val as f64, row)?;
        }
        Some(number_data_point::Value::AsDouble(val)) => {
            row_writer::write_f64(table, field, *val, row)?;
        }
        _ => {}
    }
    Ok(())
}

fn write_tags_and_timestamp(
    table: &mut TableData,
    row: &mut Vec<Value>,
    resource_attrs: Option<&Vec<KeyValue>>,
    scope_attrs: Option<&Vec<KeyValue>>,
    data_point_attrs: Option<&Vec<KeyValue>>,
    timestamp_nanos: i64,
) -> Result<()> {
    write_attributes(table, row, resource_attrs)?;
    write_attributes(table, row, scope_attrs)?;
    write_attributes(table, row, data_point_attrs)?;

    write_timestamp(table, row, timestamp_nanos)?;

    Ok(())
}

/// encode this gauge metric
///
/// note that there can be multiple data points in the request, it's going to be
/// stored as multiple rows
fn encode_gauge(
    table_writer: &mut MultiTableData,
    name: &str,
    gauge: &Gauge,
    resource_attrs: Option<&Vec<KeyValue>>,
    scope_attrs: Option<&Vec<KeyValue>>,
) -> Result<()> {
    let table = table_writer.get_or_default_table_data(
        &normalize_otlp_name(name),
        APPROXIMATE_COLUMN_COUNT,
        gauge.data_points.len(),
    );

    for data_point in &gauge.data_points {
        let mut row = table.alloc_one_row();
        write_tags_and_timestamp(
            table,
            &mut row,
            resource_attrs,
            scope_attrs,
            Some(data_point.attributes.as_ref()),
            data_point.time_unix_nano as i64,
        )?;

        write_data_point_value(table, &mut row, GREPTIME_VALUE, &data_point.value)?;
        table.add_row(row);
    }

    Ok(())
}

/// encode this sum metric
///
/// `aggregation_temporality` and `monotonic` are ignored for now
fn encode_sum(
    table_writer: &mut MultiTableData,
    name: &str,
    sum: &Sum,
    resource_attrs: Option<&Vec<KeyValue>>,
    scope_attrs: Option<&Vec<KeyValue>>,
) -> Result<()> {
    let table = table_writer.get_or_default_table_data(
        &normalize_otlp_name(name),
        APPROXIMATE_COLUMN_COUNT,
        sum.data_points.len(),
    );

    for data_point in &sum.data_points {
        let mut row = table.alloc_one_row();
        write_tags_and_timestamp(
            table,
            &mut row,
            resource_attrs,
            scope_attrs,
            Some(data_point.attributes.as_ref()),
            data_point.time_unix_nano as i64,
        )?;
        write_data_point_value(table, &mut row, GREPTIME_VALUE, &data_point.value)?;
        table.add_row(row);
    }

    Ok(())
}

const HISTOGRAM_LE_COLUMN: &str = "le";

/// Encode histogram data. This function returns 3 insert requests for 3 tables.
///
/// The implementation has been following Prometheus histogram table format:
///
/// - A `%metric%_bucket` table including `greptime_le` tag that stores bucket upper
/// limit, and `greptime_value` for bucket count
/// - A `%metric%_sum` table storing sum of samples
/// -  A `%metric%_count` table storing count of samples.
///
/// By its Prometheus compatibility, we hope to be able to use prometheus
/// quantile functions on this table.
fn encode_histogram(
    table_writer: &mut MultiTableData,
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
    // Note that the row and columns number here is approximate
    let mut bucket_table = TableData::new(APPROXIMATE_COLUMN_COUNT, data_points_len * 3);
    let mut sum_table = TableData::new(APPROXIMATE_COLUMN_COUNT, data_points_len);
    let mut count_table = TableData::new(APPROXIMATE_COLUMN_COUNT, data_points_len);

    for data_point in &hist.data_points {
        let mut accumulated_count = 0;
        for (idx, count) in data_point.bucket_counts.iter().enumerate() {
            let mut bucket_row = bucket_table.alloc_one_row();
            write_tags_and_timestamp(
                &mut bucket_table,
                &mut bucket_row,
                resource_attrs,
                scope_attrs,
                Some(data_point.attributes.as_ref()),
                data_point.time_unix_nano as i64,
            )?;

            if let Some(upper_bounds) = data_point.explicit_bounds.get(idx) {
                row_writer::write_tag(
                    &mut bucket_table,
                    HISTOGRAM_LE_COLUMN,
                    upper_bounds,
                    &mut bucket_row,
                )?;
            } else if idx == data_point.explicit_bounds.len() {
                // The last bucket
                row_writer::write_tag(
                    &mut bucket_table,
                    HISTOGRAM_LE_COLUMN,
                    f64::INFINITY,
                    &mut bucket_row,
                )?;
            }

            accumulated_count += count;
            row_writer::write_f64(
                &mut bucket_table,
                GREPTIME_VALUE,
                accumulated_count as f64,
                &mut bucket_row,
            )?;

            bucket_table.add_row(bucket_row);
        }

        if let Some(sum) = data_point.sum {
            let mut sum_row = sum_table.alloc_one_row();
            write_tags_and_timestamp(
                &mut sum_table,
                &mut sum_row,
                resource_attrs,
                scope_attrs,
                Some(data_point.attributes.as_ref()),
                data_point.time_unix_nano as i64,
            )?;

            row_writer::write_f64(&mut sum_table, GREPTIME_VALUE, sum, &mut sum_row)?;
            sum_table.add_row(sum_row);
        }

        let mut count_row = count_table.alloc_one_row();
        write_tags_and_timestamp(
            &mut count_table,
            &mut count_row,
            resource_attrs,
            scope_attrs,
            Some(data_point.attributes.as_ref()),
            data_point.time_unix_nano as i64,
        )?;

        row_writer::write_f64(
            &mut count_table,
            GREPTIME_VALUE,
            data_point.count as f64,
            &mut count_row,
        )?;
        count_table.add_row(count_row);
    }

    table_writer.add_table_data(bucket_table_name, bucket_table);
    table_writer.add_table_data(sum_table_name, sum_table);
    table_writer.add_table_data(count_table_name, count_table);

    Ok(())
}

#[allow(dead_code)]
fn encode_exponential_histogram(_name: &str, _hist: &ExponentialHistogram) -> Result<()> {
    // TODO(sunng87): implement this using a prometheus compatible way
    Ok(())
}

fn encode_summary(
    table_writer: &mut MultiTableData,
    name: &str,
    summary: &Summary,
    resource_attrs: Option<&Vec<KeyValue>>,
    scope_attrs: Option<&Vec<KeyValue>>,
) -> Result<()> {
    let table = table_writer.get_or_default_table_data(
        &normalize_otlp_name(name),
        APPROXIMATE_COLUMN_COUNT,
        summary.data_points.len(),
    );

    for data_point in &summary.data_points {
        let mut row = table.alloc_one_row();
        write_tags_and_timestamp(
            table,
            &mut row,
            resource_attrs,
            scope_attrs,
            Some(data_point.attributes.as_ref()),
            data_point.time_unix_nano as i64,
        )?;

        for quantile in &data_point.quantile_values {
            row_writer::write_f64(
                table,
                &format!("greptime_p{:02}", quantile.quantile * 100f64),
                quantile.value,
                &mut row,
            )?;
        }

        row_writer::write_f64(table, GREPTIME_COUNT, data_point.count as f64, &mut row)?;
        table.add_row(row);
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
        let mut tables = MultiTableData::default();

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
        encode_gauge(
            &mut tables,
            "datamon",
            &gauge,
            Some(&vec![keyvalue("resource", "app")]),
            Some(&vec![keyvalue("scope", "otel")]),
        )
        .unwrap();

        let table = tables.get_or_default_table_data("datamon", 0, 0);
        assert_eq!(table.num_rows(), 2);
        assert_eq!(table.num_columns(), 5);
        assert_eq!(
            table
                .columns()
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
        let mut tables = MultiTableData::default();

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
        encode_sum(
            &mut tables,
            "datamon",
            &sum,
            Some(&vec![keyvalue("resource", "app")]),
            Some(&vec![keyvalue("scope", "otel")]),
        )
        .unwrap();

        let table = tables.get_or_default_table_data("datamon", 0, 0);
        assert_eq!(table.num_rows(), 2);
        assert_eq!(table.num_columns(), 5);
        assert_eq!(
            table
                .columns()
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
        let mut tables = MultiTableData::default();

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
        encode_summary(
            &mut tables,
            "datamon",
            &summary,
            Some(&vec![keyvalue("resource", "app")]),
            Some(&vec![keyvalue("scope", "otel")]),
        )
        .unwrap();

        let table = tables.get_or_default_table_data("datamon", 0, 0);
        assert_eq!(table.num_rows(), 1);
        assert_eq!(table.num_columns(), 7);
        assert_eq!(
            table
                .columns()
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
        let mut tables = MultiTableData::default();

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
        encode_histogram(
            &mut tables,
            "histo",
            &histogram,
            Some(&vec![keyvalue("resource", "app")]),
            Some(&vec![keyvalue("scope", "otel")]),
        )
        .unwrap();

        assert_eq!(3, tables.num_tables());

        // bucket table
        let bucket_table = tables.get_or_default_table_data("histo_bucket", 0, 0);
        assert_eq!(bucket_table.num_rows(), 5);
        assert_eq!(bucket_table.num_columns(), 6);
        assert_eq!(
            bucket_table
                .columns()
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

        let sum_table = tables.get_or_default_table_data("histo_sum", 0, 0);
        assert_eq!(sum_table.num_rows(), 1);
        assert_eq!(sum_table.num_columns(), 5);
        assert_eq!(
            sum_table
                .columns()
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

        let count_table = tables.get_or_default_table_data("histo_count", 0, 0);
        assert_eq!(count_table.num_rows(), 1);
        assert_eq!(count_table.num_columns(), 5);
        assert_eq!(
            count_table
                .columns()
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
