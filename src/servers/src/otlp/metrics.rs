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

use ahash::{HashMap, HashSet};
use api::v1::{RowInsertRequests, Value};
use common_grpc::precision::Precision;
use common_query::prelude::{GREPTIME_COUNT, greptime_timestamp, greptime_value};
use lazy_static::lazy_static;
use otel_arrow_rust::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
use otel_arrow_rust::proto::opentelemetry::common::v1::{AnyValue, KeyValue, any_value};
use otel_arrow_rust::proto::opentelemetry::metrics::v1::{metric, number_data_point, *};
use regex::Regex;
use session::protocol_ctx::{MetricType, OtlpMetricCtx};

use crate::error::Result;
use crate::otlp::trace::{KEY_SERVICE_INSTANCE_ID, KEY_SERVICE_NAME};
use crate::row_writer::{self, MultiTableData, TableData};

/// the default column count for table writer
const APPROXIMATE_COLUMN_COUNT: usize = 8;

const COUNT_TABLE_SUFFIX: &str = "_count";
const SUM_TABLE_SUFFIX: &str = "_sum";

const JOB_KEY: &str = "job";
const INSTANCE_KEY: &str = "instance";

const UNDERSCORE: &str = "_";
const DOUBLE_UNDERSCORE: &str = "__";
const TOTAL: &str = "total";
const RATIO: &str = "ratio";

// see: https://prometheus.io/docs/guides/opentelemetry/#promoting-resource-attributes
const DEFAULT_PROMOTE_ATTRS: [&str; 19] = [
    "service.instance.id",
    "service.name",
    "service.namespace",
    "service.version",
    "cloud.availability_zone",
    "cloud.region",
    "container.name",
    "deployment.environment",
    "deployment.environment.name",
    "k8s.cluster.name",
    "k8s.container.name",
    "k8s.cronjob.name",
    "k8s.daemonset.name",
    "k8s.deployment.name",
    "k8s.job.name",
    "k8s.namespace.name",
    "k8s.pod.name",
    "k8s.replicaset.name",
    "k8s.statefulset.name",
];

lazy_static! {
    static ref DEFAULT_PROMOTE_ATTRS_SET: HashSet<String> =
        HashSet::from_iter(DEFAULT_PROMOTE_ATTRS.iter().map(|s| s.to_string()));
    static ref NON_ALPHA_NUM_CHAR: Regex = Regex::new(r"[^a-zA-Z0-9]").unwrap();
    static ref UNIT_MAP: HashMap<String, String> = [
        // Time
        ("d", "days"),
        ("h", "hours"),
        ("min", "minutes"),
        ("s", "seconds"),
        ("ms", "milliseconds"),
        ("us", "microseconds"),
        ("ns", "nanoseconds"),
        // Bytes
        ("By", "bytes"),
        ("KiBy", "kibibytes"),
        ("MiBy", "mebibytes"),
        ("GiBy", "gibibytes"),
        ("TiBy", "tibibytes"),
        ("KBy", "kilobytes"),
        ("MBy", "megabytes"),
        ("GBy", "gigabytes"),
        ("TBy", "terabytes"),
        // SI
        ("m", "meters"),
        ("V", "volts"),
        ("A", "amperes"),
        ("J", "joules"),
        ("W", "watts"),
        ("g", "grams"),
        // Misc
        ("Cel", "celsius"),
        ("Hz", "hertz"),
        ("1", ""),
        ("%", "percent"),
    ].iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    static ref PER_UNIT_MAP: HashMap<String, String> = [
        ("s", "second"),
        ("m", "minute"),
        ("h", "hour"),
        ("d", "day"),
        ("w", "week"),
        ("mo", "month"),
        ("y", "year"),
    ].iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
}

const OTEL_SCOPE_NAME: &str = "name";
const OTEL_SCOPE_VERSION: &str = "version";
const OTEL_SCOPE_SCHEMA_URL: &str = "schema_url";

/// Convert OpenTelemetry metrics to GreptimeDB insert requests
///
/// See
/// <https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto>
/// for data structure of OTLP metrics.
///
/// Returns `InsertRequests` and total number of rows to ingest
pub fn to_grpc_insert_requests(
    request: ExportMetricsServiceRequest,
    metric_ctx: &mut OtlpMetricCtx,
) -> Result<(RowInsertRequests, usize)> {
    let mut table_writer = MultiTableData::default();

    for resource in &request.resource_metrics {
        let resource_attrs = resource.resource.as_ref().map(|r| {
            let mut attrs = r.attributes.clone();
            process_resource_attrs(&mut attrs, metric_ctx);
            attrs
        });

        for scope in &resource.scope_metrics {
            let scope_attrs = process_scope_attrs(scope, metric_ctx);

            for metric in &scope.metrics {
                if metric.data.is_none() {
                    continue;
                }
                if let Some(t) = metric.data.as_ref().map(from_metric_type) {
                    metric_ctx.set_metric_type(t);
                }

                encode_metrics(
                    &mut table_writer,
                    metric,
                    resource_attrs.as_ref(),
                    scope_attrs.as_ref(),
                    metric_ctx,
                )?;
            }
        }
    }

    Ok(table_writer.into_row_insert_requests())
}

fn from_metric_type(data: &metric::Data) -> MetricType {
    match data {
        metric::Data::Gauge(_) => MetricType::Gauge,
        metric::Data::Sum(s) => {
            if s.is_monotonic {
                MetricType::MonotonicSum
            } else {
                MetricType::NonMonotonicSum
            }
        }
        metric::Data::Histogram(_) => MetricType::Histogram,
        metric::Data::ExponentialHistogram(_) => MetricType::ExponentialHistogram,
        metric::Data::Summary(_) => MetricType::Summary,
    }
}

fn process_resource_attrs(attrs: &mut Vec<KeyValue>, metric_ctx: &OtlpMetricCtx) {
    if metric_ctx.is_legacy {
        return;
    }

    // remap service.name and service.instance.id to job and instance
    let mut tmp = Vec::with_capacity(2);
    for kv in attrs.iter() {
        match &kv.key as &str {
            KEY_SERVICE_NAME => {
                tmp.push(KeyValue {
                    key: JOB_KEY.to_string(),
                    value: kv.value.clone(),
                });
            }
            KEY_SERVICE_INSTANCE_ID => {
                tmp.push(KeyValue {
                    key: INSTANCE_KEY.to_string(),
                    value: kv.value.clone(),
                });
            }
            _ => {}
        }
    }

    // if promote all, then exclude the list, else, include the list
    if metric_ctx.promote_all_resource_attrs {
        attrs.retain(|kv| !metric_ctx.resource_attrs.contains(&kv.key));
    } else {
        attrs.retain(|kv| {
            metric_ctx.resource_attrs.contains(&kv.key)
                || DEFAULT_PROMOTE_ATTRS_SET.contains(&kv.key)
        });
    }

    attrs.extend(tmp);
}

fn process_scope_attrs(scope: &ScopeMetrics, metric_ctx: &OtlpMetricCtx) -> Option<Vec<KeyValue>> {
    if metric_ctx.is_legacy {
        return scope.scope.as_ref().map(|s| s.attributes.clone());
    };

    if !metric_ctx.promote_scope_attrs {
        return None;
    }

    // persist scope attrs with name, version and schema_url
    scope.scope.as_ref().map(|s| {
        let mut attrs = s.attributes.clone();
        attrs.push(KeyValue {
            key: OTEL_SCOPE_NAME.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(s.name.clone())),
            }),
        });
        attrs.push(KeyValue {
            key: OTEL_SCOPE_VERSION.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(s.version.clone())),
            }),
        });
        attrs.push(KeyValue {
            key: OTEL_SCOPE_SCHEMA_URL.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(scope.schema_url.clone())),
            }),
        });
        attrs
    })
}

// See https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/145942706622aba5c276ca47f48df438228bfea4/pkg/translator/prometheus/normalize_name.go#L55
pub fn normalize_metric_name(metric: &Metric, metric_type: &MetricType) -> String {
    // Split metric name in "tokens" (remove all non-alphanumeric), filtering out empty strings
    let mut name_tokens: Vec<String> = NON_ALPHA_NUM_CHAR
        .split(&metric.name)
        .filter_map(|s| {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        })
        .collect();

    // Append unit if it exists
    if !metric.unit.is_empty() {
        let (main, per) = build_unit_suffix(&metric.unit);
        if let Some(main) = main
            && !name_tokens.contains(&main)
        {
            name_tokens.push(main);
        }
        if let Some(per) = per
            && !name_tokens.contains(&per)
        {
            name_tokens.push("per".to_string());
            name_tokens.push(per);
        }
    }

    // Append _total for Counters (monotonic sums)
    if matches!(metric_type, MetricType::MonotonicSum) {
        // Remove existing "total" tokens first, then append
        name_tokens.retain(|t| t != TOTAL);
        name_tokens.push(TOTAL.to_string());
    }

    // Append _ratio for metrics with unit "1" (gauges only)
    if metric.unit == "1" && matches!(metric_type, MetricType::Gauge) {
        // Remove existing "ratio" tokens first, then append
        name_tokens.retain(|t| t != RATIO);
        name_tokens.push(RATIO.to_string());
    }

    // Build the string from the tokens, separated with underscores
    let name = name_tokens.join(UNDERSCORE);

    // Metric name cannot start with a digit, so prefix it with "_" in this case
    if let Some((_, first)) = name.char_indices().next()
        && first.is_ascii_digit()
    {
        format!("_{}", name)
    } else {
        name
    }
}

fn build_unit_suffix(unit: &str) -> (Option<String>, Option<String>) {
    let (main, per) = unit.split_once('/').unwrap_or((unit, ""));
    (check_unit(main, &UNIT_MAP), check_unit(per, &PER_UNIT_MAP))
}

fn check_unit(unit_str: &str, unit_map: &HashMap<String, String>) -> Option<String> {
    let u = unit_str.trim();
    // Skip units that are empty, contain "{" or "}" characters
    if !u.is_empty() && !u.contains('{') && !u.contains('}') {
        let u = unit_map.get(u).map(|s| s.as_ref()).unwrap_or(u);
        let u = clean_unit_name(u);
        if !u.is_empty() {
            return Some(u);
        }
    }
    None
}

fn clean_unit_name(name: &str) -> String {
    // Split on non-alphanumeric characters, filter out empty strings, then join with underscores
    // This matches the Go implementation: strings.FieldsFunc + strings.Join
    NON_ALPHA_NUM_CHAR
        .split(name)
        .filter(|s| !s.is_empty())
        .collect::<Vec<&str>>()
        .join(UNDERSCORE)
}

// See https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/145942706622aba5c276ca47f48df438228bfea4/pkg/translator/prometheus/normalize_label.go#L27
pub fn normalize_label_name(name: &str) -> String {
    if name.is_empty() {
        return name.to_string();
    }

    let n = NON_ALPHA_NUM_CHAR.replace_all(name, UNDERSCORE);
    if let Some((_, first)) = n.char_indices().next()
        && first.is_ascii_digit()
    {
        return format!("key_{}", n);
    }
    if n.starts_with(UNDERSCORE) && !n.starts_with(DOUBLE_UNDERSCORE) {
        return format!("key{}", n);
    }
    n.to_string()
}

/// Normalize otlp instrumentation, metric and attribute names
///
/// <https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/api.md#instrument-name-syntax>
/// - since the name are case-insensitive, we transform them to lowercase for
///   better sql usability
/// - replace `.` and `-` with `_`
pub fn legacy_normalize_otlp_name(name: &str) -> String {
    name.to_lowercase().replace(['.', '-'], "_")
}

fn encode_metrics(
    table_writer: &mut MultiTableData,
    metric: &Metric,
    resource_attrs: Option<&Vec<KeyValue>>,
    scope_attrs: Option<&Vec<KeyValue>>,
    metric_ctx: &OtlpMetricCtx,
) -> Result<()> {
    let name = if metric_ctx.is_legacy {
        legacy_normalize_otlp_name(&metric.name)
    } else {
        normalize_metric_name(metric, &metric_ctx.metric_type)
    };

    // note that we don't store description or unit, we might want to deal with
    // these fields in the future.
    if let Some(data) = &metric.data {
        match data {
            metric::Data::Gauge(gauge) => {
                encode_gauge(
                    table_writer,
                    &name,
                    gauge,
                    resource_attrs,
                    scope_attrs,
                    metric_ctx,
                )?;
            }
            metric::Data::Sum(sum) => {
                encode_sum(
                    table_writer,
                    &name,
                    sum,
                    resource_attrs,
                    scope_attrs,
                    metric_ctx,
                )?;
            }
            metric::Data::Summary(summary) => {
                encode_summary(
                    table_writer,
                    &name,
                    summary,
                    resource_attrs,
                    scope_attrs,
                    metric_ctx,
                )?;
            }
            metric::Data::Histogram(hist) => {
                encode_histogram(
                    table_writer,
                    &name,
                    hist,
                    resource_attrs,
                    scope_attrs,
                    metric_ctx,
                )?;
            }
            // TODO(sunng87) leave ExponentialHistogram for next release
            metric::Data::ExponentialHistogram(_hist) => {}
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AttributeType {
    Resource,
    Scope,
    DataPoint,
    Legacy,
}

fn write_attributes(
    writer: &mut TableData,
    row: &mut Vec<Value>,
    attrs: Option<&Vec<KeyValue>>,
    attribute_type: AttributeType,
) -> Result<()> {
    let Some(attrs) = attrs else {
        return Ok(());
    };

    let tags = attrs.iter().filter_map(|attr| {
        attr.value
            .as_ref()
            .and_then(|v| v.value.as_ref())
            .and_then(|val| {
                let key = match attribute_type {
                    AttributeType::Resource | AttributeType::DataPoint => {
                        normalize_label_name(&attr.key)
                    }
                    AttributeType::Scope => {
                        format!("otel_scope_{}", normalize_label_name(&attr.key))
                    }
                    AttributeType::Legacy => legacy_normalize_otlp_name(&attr.key),
                };
                match val {
                    any_value::Value::StringValue(s) => Some((key, s.clone())),
                    any_value::Value::IntValue(v) => Some((key, v.to_string())),
                    any_value::Value::DoubleValue(v) => Some((key, v.to_string())),
                    _ => None, // TODO(sunng87): allow different type of values
                }
            })
    });
    row_writer::write_tags(writer, tags, row)?;

    Ok(())
}

fn write_timestamp(
    table: &mut TableData,
    row: &mut Vec<Value>,
    time_nano: i64,
    legacy_mode: bool,
) -> Result<()> {
    if legacy_mode {
        row_writer::write_ts_to_nanos(
            table,
            greptime_timestamp(),
            Some(time_nano),
            Precision::Nanosecond,
            row,
        )
    } else {
        row_writer::write_ts_to_millis(
            table,
            greptime_timestamp(),
            Some(time_nano / 1000000),
            Precision::Millisecond,
            row,
        )
    }
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
    metric_ctx: &OtlpMetricCtx,
) -> Result<()> {
    if metric_ctx.is_legacy {
        write_attributes(table, row, resource_attrs, AttributeType::Legacy)?;
        write_attributes(table, row, scope_attrs, AttributeType::Legacy)?;
        write_attributes(table, row, data_point_attrs, AttributeType::Legacy)?;
    } else {
        // TODO(shuiyisong): check `__type__` and `__unit__` tags in prometheus
        write_attributes(table, row, resource_attrs, AttributeType::Resource)?;
        write_attributes(table, row, scope_attrs, AttributeType::Scope)?;
        write_attributes(table, row, data_point_attrs, AttributeType::DataPoint)?;
    }

    write_timestamp(table, row, timestamp_nanos, metric_ctx.is_legacy)?;

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
    metric_ctx: &OtlpMetricCtx,
) -> Result<()> {
    let table = table_writer.get_or_default_table_data(
        name,
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
            metric_ctx,
        )?;

        write_data_point_value(table, &mut row, greptime_value(), &data_point.value)?;
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
    metric_ctx: &OtlpMetricCtx,
) -> Result<()> {
    let table = table_writer.get_or_default_table_data(
        name,
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
            metric_ctx,
        )?;
        write_data_point_value(table, &mut row, greptime_value(), &data_point.value)?;
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
///   limit, and `greptime_value` for bucket count
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
    metric_ctx: &OtlpMetricCtx,
) -> Result<()> {
    let normalized_name = name;

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
                metric_ctx,
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
                greptime_value(),
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
                metric_ctx,
            )?;

            row_writer::write_f64(&mut sum_table, greptime_value(), sum, &mut sum_row)?;
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
            metric_ctx,
        )?;

        row_writer::write_f64(
            &mut count_table,
            greptime_value(),
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
    metric_ctx: &OtlpMetricCtx,
) -> Result<()> {
    if metric_ctx.is_legacy {
        let table = table_writer.get_or_default_table_data(
            name,
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
                metric_ctx,
            )?;

            for quantile in &data_point.quantile_values {
                row_writer::write_f64(
                    table,
                    format!("greptime_p{:02}", quantile.quantile * 100f64),
                    quantile.value,
                    &mut row,
                )?;
            }

            row_writer::write_f64(table, GREPTIME_COUNT, data_point.count as f64, &mut row)?;
            table.add_row(row);
        }
    } else {
        // 1. quantile table
        // 2. count table
        // 3. sum table

        let metric_name = name;
        let count_name = format!("{}{}", metric_name, COUNT_TABLE_SUFFIX);
        let sum_name = format!("{}{}", metric_name, SUM_TABLE_SUFFIX);

        for data_point in &summary.data_points {
            {
                let quantile_table = table_writer.get_or_default_table_data(
                    metric_name,
                    APPROXIMATE_COLUMN_COUNT,
                    summary.data_points.len(),
                );

                for quantile in &data_point.quantile_values {
                    let mut row = quantile_table.alloc_one_row();
                    write_tags_and_timestamp(
                        quantile_table,
                        &mut row,
                        resource_attrs,
                        scope_attrs,
                        Some(data_point.attributes.as_ref()),
                        data_point.time_unix_nano as i64,
                        metric_ctx,
                    )?;
                    row_writer::write_tag(quantile_table, "quantile", quantile.quantile, &mut row)?;
                    row_writer::write_f64(
                        quantile_table,
                        greptime_value(),
                        quantile.value,
                        &mut row,
                    )?;
                    quantile_table.add_row(row);
                }
            }
            {
                let count_table = table_writer.get_or_default_table_data(
                    &count_name,
                    APPROXIMATE_COLUMN_COUNT,
                    summary.data_points.len(),
                );
                let mut row = count_table.alloc_one_row();
                write_tags_and_timestamp(
                    count_table,
                    &mut row,
                    resource_attrs,
                    scope_attrs,
                    Some(data_point.attributes.as_ref()),
                    data_point.time_unix_nano as i64,
                    metric_ctx,
                )?;

                row_writer::write_f64(
                    count_table,
                    greptime_value(),
                    data_point.count as f64,
                    &mut row,
                )?;

                count_table.add_row(row);
            }
            {
                let sum_table = table_writer.get_or_default_table_data(
                    &sum_name,
                    APPROXIMATE_COLUMN_COUNT,
                    summary.data_points.len(),
                );

                let mut row = sum_table.alloc_one_row();
                write_tags_and_timestamp(
                    sum_table,
                    &mut row,
                    resource_attrs,
                    scope_attrs,
                    Some(data_point.attributes.as_ref()),
                    data_point.time_unix_nano as i64,
                    metric_ctx,
                )?;

                row_writer::write_f64(sum_table, greptime_value(), data_point.sum, &mut row)?;

                sum_table.add_row(row);
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use otel_arrow_rust::proto::opentelemetry::common::v1::AnyValue;
    use otel_arrow_rust::proto::opentelemetry::common::v1::any_value::Value as Val;
    use otel_arrow_rust::proto::opentelemetry::metrics::v1::number_data_point::Value;
    use otel_arrow_rust::proto::opentelemetry::metrics::v1::summary_data_point::ValueAtQuantile;
    use otel_arrow_rust::proto::opentelemetry::metrics::v1::{
        AggregationTemporality, HistogramDataPoint, NumberDataPoint, SummaryDataPoint,
    };

    use super::*;

    #[test]
    fn test_legacy_normalize_otlp_name() {
        assert_eq!(
            legacy_normalize_otlp_name("jvm.memory.free"),
            "jvm_memory_free"
        );
        assert_eq!(
            legacy_normalize_otlp_name("jvm-memory-free"),
            "jvm_memory_free"
        );
        assert_eq!(
            legacy_normalize_otlp_name("jvm_memory_free"),
            "jvm_memory_free"
        );
        assert_eq!(
            legacy_normalize_otlp_name("JVM_MEMORY_FREE"),
            "jvm_memory_free"
        );
        assert_eq!(
            legacy_normalize_otlp_name("JVM_memory_FREE"),
            "jvm_memory_free"
        );
    }

    #[test]
    fn test_normalize_metric_name() {
        let test_cases = vec![
            // Default case
            (Metric::default(), MetricType::Init, ""),
            // Basic metric with just name
            (
                Metric {
                    name: "foo".to_string(),
                    ..Default::default()
                },
                MetricType::Init,
                "foo",
            ),
            // Metric with unit "s" should append "seconds"
            (
                Metric {
                    name: "foo".to_string(),
                    unit: "s".to_string(),
                    ..Default::default()
                },
                MetricType::Init,
                "foo_seconds",
            ),
            // Metric already ending with unit suffix should not duplicate
            (
                Metric {
                    name: "foo_seconds".to_string(),
                    unit: "s".to_string(),
                    ..Default::default()
                },
                MetricType::Init,
                "foo_seconds",
            ),
            // Monotonic sum should append "total"
            (
                Metric {
                    name: "foo".to_string(),
                    ..Default::default()
                },
                MetricType::MonotonicSum,
                "foo_total",
            ),
            // Metric already ending with "total" should not duplicate
            (
                Metric {
                    name: "foo_total".to_string(),
                    ..Default::default()
                },
                MetricType::MonotonicSum,
                "foo_total",
            ),
            // Monotonic sum with unit should append both unit and "total"
            (
                Metric {
                    name: "foo".to_string(),
                    unit: "s".to_string(),
                    ..Default::default()
                },
                MetricType::MonotonicSum,
                "foo_seconds_total",
            ),
            // Metric with unit suffix and monotonic sum
            (
                Metric {
                    name: "foo_seconds".to_string(),
                    unit: "s".to_string(),
                    ..Default::default()
                },
                MetricType::MonotonicSum,
                "foo_seconds_total",
            ),
            // Metric already ending with "total" and has unit
            (
                Metric {
                    name: "foo_total".to_string(),
                    unit: "s".to_string(),
                    ..Default::default()
                },
                MetricType::MonotonicSum,
                "foo_seconds_total",
            ),
            // Metric already ending with both unit and "total"
            (
                Metric {
                    name: "foo_seconds_total".to_string(),
                    unit: "s".to_string(),
                    ..Default::default()
                },
                MetricType::MonotonicSum,
                "foo_seconds_total",
            ),
            // Metric with unusual order (total_seconds) should be normalized
            (
                Metric {
                    name: "foo_total_seconds".to_string(),
                    unit: "s".to_string(),
                    ..Default::default()
                },
                MetricType::MonotonicSum,
                "foo_seconds_total",
            ),
            // Gauge with unit "1" should append "ratio"
            (
                Metric {
                    name: "foo".to_string(),
                    unit: "1".to_string(),
                    ..Default::default()
                },
                MetricType::Gauge,
                "foo_ratio",
            ),
            // Complex unit like "m/s" should be converted to "meters_per_second"
            (
                Metric {
                    name: "foo".to_string(),
                    unit: "m/s".to_string(),
                    ..Default::default()
                },
                MetricType::Init,
                "foo_meters_per_second",
            ),
            // Metric with partial unit match
            (
                Metric {
                    name: "foo_second".to_string(),
                    unit: "m/s".to_string(),
                    ..Default::default()
                },
                MetricType::Init,
                "foo_second_meters",
            ),
            // Metric already containing the main unit
            (
                Metric {
                    name: "foo_meters".to_string(),
                    unit: "m/s".to_string(),
                    ..Default::default()
                },
                MetricType::Init,
                "foo_meters_per_second",
            ),
        ];

        for (metric, metric_type, expected) in test_cases {
            let result = normalize_metric_name(&metric, &metric_type);
            assert_eq!(
                result, expected,
                "Failed for metric name: '{}', unit: '{}', type: {:?}",
                metric.name, metric.unit, metric_type
            );
        }
    }

    #[test]
    fn test_normalize_metric_name_edge_cases() {
        let test_cases = vec![
            // Edge case: name with multiple non-alphanumeric chars in a row
            (
                Metric {
                    name: "foo--bar__baz".to_string(),
                    ..Default::default()
                },
                MetricType::Init,
                "foo_bar_baz",
            ),
            // Edge case: name starting and ending with non-alphanumeric
            (
                Metric {
                    name: "-foo_bar-".to_string(),
                    ..Default::default()
                },
                MetricType::Init,
                "foo_bar",
            ),
            // Edge case: name with only special chars (should be empty)
            (
                Metric {
                    name: "--___--".to_string(),
                    ..Default::default()
                },
                MetricType::Init,
                "",
            ),
            // Edge case: name starting with digit
            (
                Metric {
                    name: "2xx_requests".to_string(),
                    ..Default::default()
                },
                MetricType::Init,
                "_2xx_requests",
            ),
        ];

        for (metric, metric_type, expected) in test_cases {
            let result = normalize_metric_name(&metric, &metric_type);
            assert_eq!(
                result, expected,
                "Failed for metric name: '{}', unit: '{}', type: {:?}",
                metric.name, metric.unit, metric_type
            );
        }
    }

    #[test]
    fn test_normalize_label_name() {
        let test_cases = vec![
            ("", ""),
            ("foo", "foo"),
            ("foo_bar/baz:abc", "foo_bar_baz_abc"),
            ("1foo", "key_1foo"),
            ("_foo", "key_foo"),
            ("__bar", "__bar"),
        ];

        for (input, expected) in test_cases {
            let result = normalize_label_name(input);
            assert_eq!(
                result, expected,
                "unexpected result for input '{}'; got '{}'; want '{}'",
                input, result, expected
            );
        }
    }

    #[test]
    fn test_clean_unit_name() {
        // Test the improved clean_unit_name function
        assert_eq!(clean_unit_name("faults"), "faults");
        assert_eq!(clean_unit_name("{faults}"), "faults"); // clean_unit_name still processes braces internally
        assert_eq!(clean_unit_name("req/sec"), "req_sec");
        assert_eq!(clean_unit_name("m/s"), "m_s");
        assert_eq!(clean_unit_name("___test___"), "test");
        assert_eq!(
            clean_unit_name("multiple__underscores"),
            "multiple_underscores"
        );
        assert_eq!(clean_unit_name(""), "");
        assert_eq!(clean_unit_name("___"), "");
        assert_eq!(clean_unit_name("bytes.per.second"), "bytes_per_second");
    }

    #[test]
    fn test_normalize_metric_name_braced_units() {
        // Test that units with braces are rejected (not processed)
        let test_cases = vec![
            (
                Metric {
                    name: "test.metric".to_string(),
                    unit: "{faults}".to_string(),
                    ..Default::default()
                },
                MetricType::MonotonicSum,
                "test_metric_total", // braced units are rejected, no unit suffix added
            ),
            (
                Metric {
                    name: "test.metric".to_string(),
                    unit: "{operations}".to_string(),
                    ..Default::default()
                },
                MetricType::Gauge,
                "test_metric", // braced units are rejected, no unit suffix added
            ),
            (
                Metric {
                    name: "test.metric".to_string(),
                    unit: "{}".to_string(), // empty braces should be ignored due to contains('{') || contains('}')
                    ..Default::default()
                },
                MetricType::Gauge,
                "test_metric",
            ),
            (
                Metric {
                    name: "test.metric".to_string(),
                    unit: "faults".to_string(), // no braces, should work normally
                    ..Default::default()
                },
                MetricType::Gauge,
                "test_metric_faults",
            ),
        ];

        for (metric, metric_type, expected) in test_cases {
            let result = normalize_metric_name(&metric, &metric_type);
            assert_eq!(
                result, expected,
                "Failed for metric name: '{}', unit: '{}', type: {:?}. Got: '{}', Expected: '{}'",
                metric.name, metric.unit, metric_type, result, expected
            );
        }
    }

    #[test]
    fn test_normalize_metric_name_with_testdata() {
        // Test cases extracted from real OTLP metrics data from testdata.txt
        let test_cases = vec![
            // Basic system metrics with various units
            (
                Metric {
                    name: "system.paging.faults".to_string(),
                    unit: "{faults}".to_string(),
                    ..Default::default()
                },
                MetricType::MonotonicSum,
                "system_paging_faults_total", // braced units are rejected, no unit suffix added
            ),
            (
                Metric {
                    name: "system.paging.operations".to_string(),
                    unit: "{operations}".to_string(),
                    ..Default::default()
                },
                MetricType::MonotonicSum,
                "system_paging_operations_total", // braced units are rejected, no unit suffix added
            ),
            (
                Metric {
                    name: "system.paging.usage".to_string(),
                    unit: "By".to_string(),
                    ..Default::default()
                },
                MetricType::NonMonotonicSum,
                "system_paging_usage_bytes",
            ),
            // Load average metrics - gauge with custom unit
            (
                Metric {
                    name: "system.cpu.load_average.15m".to_string(),
                    unit: "{thread}".to_string(),
                    ..Default::default()
                },
                MetricType::Gauge,
                "system_cpu_load_average_15m", // braced units are rejected, no unit suffix added
            ),
            (
                Metric {
                    name: "system.cpu.load_average.1m".to_string(),
                    unit: "{thread}".to_string(),
                    ..Default::default()
                },
                MetricType::Gauge,
                "system_cpu_load_average_1m", // braced units are rejected, no unit suffix added
            ),
            // Disk I/O with bytes unit
            (
                Metric {
                    name: "system.disk.io".to_string(),
                    unit: "By".to_string(),
                    ..Default::default()
                },
                MetricType::MonotonicSum,
                "system_disk_io_bytes_total",
            ),
            // Time-based metrics with seconds unit
            (
                Metric {
                    name: "system.disk.io_time".to_string(),
                    unit: "s".to_string(),
                    ..Default::default()
                },
                MetricType::MonotonicSum,
                "system_disk_io_time_seconds_total",
            ),
            (
                Metric {
                    name: "system.disk.operation_time".to_string(),
                    unit: "s".to_string(),
                    ..Default::default()
                },
                MetricType::MonotonicSum,
                "system_disk_operation_time_seconds_total",
            ),
            // CPU time metric
            (
                Metric {
                    name: "system.cpu.time".to_string(),
                    unit: "s".to_string(),
                    ..Default::default()
                },
                MetricType::MonotonicSum,
                "system_cpu_time_seconds_total",
            ),
            // Process counts
            (
                Metric {
                    name: "system.processes.count".to_string(),
                    unit: "{processes}".to_string(),
                    ..Default::default()
                },
                MetricType::NonMonotonicSum,
                "system_processes_count", // braced units are rejected, no unit suffix added
            ),
            (
                Metric {
                    name: "system.processes.created".to_string(),
                    unit: "{processes}".to_string(),
                    ..Default::default()
                },
                MetricType::MonotonicSum,
                "system_processes_created_total", // braced units are rejected, no unit suffix added
            ),
            // Memory usage with bytes
            (
                Metric {
                    name: "system.memory.usage".to_string(),
                    unit: "By".to_string(),
                    ..Default::default()
                },
                MetricType::NonMonotonicSum,
                "system_memory_usage_bytes",
            ),
            // Uptime as gauge
            (
                Metric {
                    name: "system.uptime".to_string(),
                    unit: "s".to_string(),
                    ..Default::default()
                },
                MetricType::Gauge,
                "system_uptime_seconds",
            ),
            // Network metrics
            (
                Metric {
                    name: "system.network.connections".to_string(),
                    unit: "{connections}".to_string(),
                    ..Default::default()
                },
                MetricType::NonMonotonicSum,
                "system_network_connections", // braced units are rejected, no unit suffix added
            ),
            (
                Metric {
                    name: "system.network.dropped".to_string(),
                    unit: "{packets}".to_string(),
                    ..Default::default()
                },
                MetricType::MonotonicSum,
                "system_network_dropped_total", // braced units are rejected, no unit suffix added
            ),
            (
                Metric {
                    name: "system.network.errors".to_string(),
                    unit: "{errors}".to_string(),
                    ..Default::default()
                },
                MetricType::MonotonicSum,
                "system_network_errors_total", // braced units are rejected, no unit suffix added
            ),
            (
                Metric {
                    name: "system.network.io".to_string(),
                    unit: "By".to_string(),
                    ..Default::default()
                },
                MetricType::MonotonicSum,
                "system_network_io_bytes_total",
            ),
            (
                Metric {
                    name: "system.network.packets".to_string(),
                    unit: "{packets}".to_string(),
                    ..Default::default()
                },
                MetricType::MonotonicSum,
                "system_network_packets_total", // braced units are rejected, no unit suffix added
            ),
            // Filesystem metrics
            (
                Metric {
                    name: "system.filesystem.inodes.usage".to_string(),
                    unit: "{inodes}".to_string(),
                    ..Default::default()
                },
                MetricType::NonMonotonicSum,
                "system_filesystem_inodes_usage", // braced units are rejected, no unit suffix added
            ),
            (
                Metric {
                    name: "system.filesystem.usage".to_string(),
                    unit: "By".to_string(),
                    ..Default::default()
                },
                MetricType::NonMonotonicSum,
                "system_filesystem_usage_bytes",
            ),
            // Edge cases with special characters and numbers
            (
                Metric {
                    name: "system.load.1".to_string(),
                    unit: "1".to_string(),
                    ..Default::default()
                },
                MetricType::Gauge,
                "system_load_1_ratio",
            ),
            (
                Metric {
                    name: "http.request.2xx".to_string(),
                    unit: "{requests}".to_string(),
                    ..Default::default()
                },
                MetricType::MonotonicSum,
                "http_request_2xx_total", // braced units are rejected, no unit suffix added
            ),
            // Metric with dots and underscores mixed
            (
                Metric {
                    name: "jvm.memory.heap_usage".to_string(),
                    unit: "By".to_string(),
                    ..Default::default()
                },
                MetricType::Gauge,
                "jvm_memory_heap_usage_bytes",
            ),
            // Complex unit with per-second
            (
                Metric {
                    name: "http.request.rate".to_string(),
                    unit: "1/s".to_string(),
                    ..Default::default()
                },
                MetricType::Gauge,
                "http_request_rate_per_second",
            ),
        ];

        for (metric, metric_type, expected) in test_cases {
            let result = normalize_metric_name(&metric, &metric_type);
            assert_eq!(
                result, expected,
                "Failed for metric name: '{}', unit: '{}', type: {:?}. Got: '{}', Expected: '{}'",
                metric.name, metric.unit, metric_type, result, expected
            );
        }
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
            Some(&vec![]),
            Some(&vec![keyvalue("scope", "otel")]),
            &OtlpMetricCtx::default(),
        )
        .unwrap();

        let table = tables.get_or_default_table_data("datamon", 0, 0);
        assert_eq!(table.num_rows(), 2);
        assert_eq!(table.num_columns(), 4);
        assert_eq!(
            table
                .columns()
                .iter()
                .map(|c| &c.column_name)
                .collect::<Vec<&String>>(),
            vec![
                "otel_scope_scope",
                "host",
                greptime_timestamp(),
                greptime_value()
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
            Some(&vec![]),
            Some(&vec![keyvalue("scope", "otel")]),
            &OtlpMetricCtx::default(),
        )
        .unwrap();

        let table = tables.get_or_default_table_data("datamon", 0, 0);
        assert_eq!(table.num_rows(), 2);
        assert_eq!(table.num_columns(), 4);
        assert_eq!(
            table
                .columns()
                .iter()
                .map(|c| &c.column_name)
                .collect::<Vec<&String>>(),
            vec![
                "otel_scope_scope",
                "host",
                greptime_timestamp(),
                greptime_value()
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
            Some(&vec![]),
            Some(&vec![keyvalue("scope", "otel")]),
            &OtlpMetricCtx::default(),
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
                "otel_scope_scope",
                "host",
                greptime_timestamp(),
                "quantile",
                greptime_value()
            ]
        );

        let table = tables.get_or_default_table_data("datamon_count", 0, 0);
        assert_eq!(table.num_rows(), 1);
        assert_eq!(table.num_columns(), 4);
        assert_eq!(
            table
                .columns()
                .iter()
                .map(|c| &c.column_name)
                .collect::<Vec<&String>>(),
            vec![
                "otel_scope_scope",
                "host",
                greptime_timestamp(),
                greptime_value()
            ]
        );

        let table = tables.get_or_default_table_data("datamon_sum", 0, 0);
        assert_eq!(table.num_rows(), 1);
        assert_eq!(table.num_columns(), 4);
        assert_eq!(
            table
                .columns()
                .iter()
                .map(|c| &c.column_name)
                .collect::<Vec<&String>>(),
            vec![
                "otel_scope_scope",
                "host",
                greptime_timestamp(),
                greptime_value()
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
            Some(&vec![]),
            Some(&vec![keyvalue("scope", "otel")]),
            &OtlpMetricCtx::default(),
        )
        .unwrap();

        assert_eq!(3, tables.num_tables());

        // bucket table
        let bucket_table = tables.get_or_default_table_data("histo_bucket", 0, 0);
        assert_eq!(bucket_table.num_rows(), 5);
        assert_eq!(bucket_table.num_columns(), 5);
        assert_eq!(
            bucket_table
                .columns()
                .iter()
                .map(|c| &c.column_name)
                .collect::<Vec<&String>>(),
            vec![
                "otel_scope_scope",
                "host",
                greptime_timestamp(),
                "le",
                greptime_value(),
            ]
        );

        let sum_table = tables.get_or_default_table_data("histo_sum", 0, 0);
        assert_eq!(sum_table.num_rows(), 1);
        assert_eq!(sum_table.num_columns(), 4);
        assert_eq!(
            sum_table
                .columns()
                .iter()
                .map(|c| &c.column_name)
                .collect::<Vec<&String>>(),
            vec![
                "otel_scope_scope",
                "host",
                greptime_timestamp(),
                greptime_value()
            ]
        );

        let count_table = tables.get_or_default_table_data("histo_count", 0, 0);
        assert_eq!(count_table.num_rows(), 1);
        assert_eq!(count_table.num_columns(), 4);
        assert_eq!(
            count_table
                .columns()
                .iter()
                .map(|c| &c.column_name)
                .collect::<Vec<&String>>(),
            vec![
                "otel_scope_scope",
                "host",
                greptime_timestamp(),
                greptime_value()
            ]
        );
    }
}
