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

use ahash::HashSet;
use api::v1::{RowInsertRequests, Value};
use common_grpc::precision::Precision;
use common_query::prelude::{GREPTIME_COUNT, greptime_timestamp, greptime_value};
use lazy_static::lazy_static;
use otel_arrow_rust::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
use otel_arrow_rust::proto::opentelemetry::common::v1::{AnyValue, KeyValue, any_value};
use otel_arrow_rust::proto::opentelemetry::metrics::v1::{metric, number_data_point, *};
use session::protocol_ctx::{MetricType, OtlpMetricCtx};
use table::requests::{
    METADATA_QUALITY_DECLARED, SEMANTIC_METRIC_METADATA_QUALITY, SEMANTIC_METRIC_ORIGINAL_NAME,
    SEMANTIC_METRIC_TEMPORALITY, SEMANTIC_METRIC_TYPE, SEMANTIC_METRIC_UNIT,
};

use crate::error::Result;
use crate::otlp::trace::{KEY_SERVICE_INSTANCE_ID, KEY_SERVICE_NAME};
use crate::row_writer::{self, MultiTableData, TableData};

mod semantic;
mod translator;

pub use semantic::SemanticIndex;
pub use translator::legacy_normalize_otlp_name;
use translator::{translate_label_name, translate_metric_name};

/// the default column count for table writer
const APPROXIMATE_COLUMN_COUNT: usize = 8;

const COUNT_TABLE_SUFFIX: &str = "_count";
const SUM_TABLE_SUFFIX: &str = "_sum";
const BUCKET_TABLE_SUFFIX: &str = "_bucket";

// `greptime.semantic.metric.type` values stamped per emitted table. Must stay
// within the domain accepted by `validate_semantic_option`; the drift-guard test
// asserts this.
const METRIC_TYPE_COUNTER: &str = "counter";
const METRIC_TYPE_UPDOWN_COUNTER: &str = "updown_counter";
const METRIC_TYPE_GAUGE: &str = "gauge";
const METRIC_TYPE_HISTOGRAM: &str = "histogram";
const METRIC_TYPE_SUMMARY: &str = "summary";

const JOB_KEY: &str = "job";
const INSTANCE_KEY: &str = "instance";

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
/// Returns `InsertRequests`, total number of rows to ingest, and the per-table
/// semantic index for the auto-create path to stamp as table options.
pub fn to_grpc_insert_requests(
    request: ExportMetricsServiceRequest,
    metric_ctx: &mut OtlpMetricCtx,
) -> Result<(RowInsertRequests, usize, SemanticIndex)> {
    let mut table_writer = MultiTableData::default();
    let mut semantic_index = SemanticIndex::default();

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
                    &mut semantic_index,
                )?;
            }
        }
    }

    let (requests, rows) = table_writer.into_row_insert_requests();
    Ok((requests, rows, semantic_index))
}

/// The tables a metric emits and their per-table `metric.type`. Histogram fans
/// out into `_bucket` (the histogram) plus `_sum`/`_count` counters; summary
/// fans out into the quantile table plus `_count`/`_sum` counters (legacy
/// summary stays a single table).
fn emitted_semantic_tables(
    metric_type: &MetricType,
    is_legacy: bool,
    base: &str,
) -> Vec<(String, &'static str)> {
    match metric_type {
        MetricType::Gauge => vec![(base.to_string(), METRIC_TYPE_GAUGE)],
        MetricType::MonotonicSum => vec![(base.to_string(), METRIC_TYPE_COUNTER)],
        MetricType::NonMonotonicSum => vec![(base.to_string(), METRIC_TYPE_UPDOWN_COUNTER)],
        MetricType::Histogram => vec![
            (
                format!("{base}{BUCKET_TABLE_SUFFIX}"),
                METRIC_TYPE_HISTOGRAM,
            ),
            (format!("{base}{SUM_TABLE_SUFFIX}"), METRIC_TYPE_COUNTER),
            (format!("{base}{COUNT_TABLE_SUFFIX}"), METRIC_TYPE_COUNTER),
        ],
        MetricType::Summary if is_legacy => vec![(base.to_string(), METRIC_TYPE_SUMMARY)],
        MetricType::Summary => vec![
            (base.to_string(), METRIC_TYPE_SUMMARY),
            (format!("{base}{COUNT_TABLE_SUFFIX}"), METRIC_TYPE_COUNTER),
            (format!("{base}{SUM_TABLE_SUFFIX}"), METRIC_TYPE_COUNTER),
        ],
        // ExponentialHistogram is a no-op today; Init never reaches encoding.
        MetricType::ExponentialHistogram | MetricType::Init => vec![],
    }
}

/// Maps OTLP `aggregation_temporality` to the semantic value, or `None` when the
/// instrument has no temporality (gauge/summary) or it is unspecified.
fn temporality_value(data: &metric::Data) -> Option<&'static str> {
    let raw = match data {
        metric::Data::Sum(sum) => sum.aggregation_temporality,
        metric::Data::Histogram(hist) => hist.aggregation_temporality,
        _ => return None,
    };
    match AggregationTemporality::try_from(raw) {
        Ok(AggregationTemporality::Delta) => Some("delta"),
        Ok(AggregationTemporality::Cumulative) => Some("cumulative"),
        _ => None,
    }
}

/// Records the declared metric-level semantic keys for every table this metric
/// emits.
fn record_metric_semantics(
    index: &mut SemanticIndex,
    metric: &Metric,
    name: &str,
    metric_ctx: &OtlpMetricCtx,
) {
    let emitted = emitted_semantic_tables(&metric_ctx.metric_type, metric_ctx.is_legacy, name);
    if emitted.is_empty() {
        return;
    }

    let temporality = metric.data.as_ref().and_then(temporality_value);
    let unit = metric.unit.trim();
    // `original_name` is meaningful only when translation renamed the metric.
    let original_name = (name != metric.name.as_str()).then_some(metric.name.as_str());

    for (table, metric_type) in &emitted {
        index.record_scalar(table, SEMANTIC_METRIC_TYPE, metric_type);
        index.record_scalar(
            table,
            SEMANTIC_METRIC_METADATA_QUALITY,
            METADATA_QUALITY_DECLARED,
        );
        if let Some(temporality) = temporality {
            index.record_scalar(table, SEMANTIC_METRIC_TEMPORALITY, temporality);
        }
        if !unit.is_empty() {
            index.record_scalar(table, SEMANTIC_METRIC_UNIT, unit);
        }
        if let Some(original_name) = original_name {
            index.record_scalar(table, SEMANTIC_METRIC_ORIGINAL_NAME, original_name);
        }
    }
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

fn encode_metrics(
    table_writer: &mut MultiTableData,
    metric: &Metric,
    resource_attrs: Option<&Vec<KeyValue>>,
    scope_attrs: Option<&Vec<KeyValue>>,
    metric_ctx: &OtlpMetricCtx,
    semantic_index: &mut SemanticIndex,
) -> Result<()> {
    let name = if metric_ctx.is_legacy {
        legacy_normalize_otlp_name(&metric.name)
    } else {
        translate_metric_name(
            metric,
            &metric_ctx.metric_type,
            metric_ctx.metric_translation_strategy,
        )
    };

    // Stamp semantic metadata against the same table name(s) the data is written
    // to below. `unit` is captured here (it is otherwise discarded by the row
    // encoders) along with the declared type/temporality.
    record_metric_semantics(semantic_index, metric, &name, metric_ctx);

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
    metric_ctx: &OtlpMetricCtx,
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
                        translate_label_name(&attr.key, metric_ctx.metric_translation_strategy)
                    }
                    AttributeType::Scope => {
                        format!(
                            "otel_scope_{}",
                            translate_label_name(&attr.key, metric_ctx.metric_translation_strategy)
                        )
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
        write_attributes(
            table,
            row,
            resource_attrs,
            AttributeType::Legacy,
            metric_ctx,
        )?;
        write_attributes(table, row, scope_attrs, AttributeType::Legacy, metric_ctx)?;
        write_attributes(
            table,
            row,
            data_point_attrs,
            AttributeType::Legacy,
            metric_ctx,
        )?;
    } else {
        // TODO(shuiyisong): check `__type__` and `__unit__` tags in prometheus
        write_attributes(
            table,
            row,
            resource_attrs,
            AttributeType::Resource,
            metric_ctx,
        )?;
        write_attributes(table, row, scope_attrs, AttributeType::Scope, metric_ctx)?;
        write_attributes(
            table,
            row,
            data_point_attrs,
            AttributeType::DataPoint,
            metric_ctx,
        )?;
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

    let bucket_table_name = format!("{}{}", normalized_name, BUCKET_TABLE_SUFFIX);
    let sum_table_name = format!("{}{}", normalized_name, SUM_TABLE_SUFFIX);
    let count_table_name = format!("{}{}", normalized_name, COUNT_TABLE_SUFFIX);

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

    use std::collections::BTreeMap;

    use table::requests::validate_semantic_option;

    fn decode(index: &SemanticIndex) -> BTreeMap<String, BTreeMap<String, String>> {
        serde_json::from_str(&index.encode().expect("non-empty index")).unwrap()
    }

    fn record(metric: &Metric, metric_type: MetricType, name: &str) -> SemanticIndex {
        let ctx = OtlpMetricCtx {
            metric_type,
            ..Default::default()
        };
        let mut index = SemanticIndex::default();
        record_metric_semantics(&mut index, metric, name, &ctx);
        index
    }

    #[test]
    fn test_metric_type_constants_validate() {
        for value in [
            METRIC_TYPE_COUNTER,
            METRIC_TYPE_UPDOWN_COUNTER,
            METRIC_TYPE_GAUGE,
            METRIC_TYPE_HISTOGRAM,
            METRIC_TYPE_SUMMARY,
        ] {
            assert!(
                validate_semantic_option(SEMANTIC_METRIC_TYPE, value),
                "metric.type value `{value}` must be in the vocabulary domain"
            );
        }
        for value in ["delta", "cumulative"] {
            assert!(validate_semantic_option(SEMANTIC_METRIC_TEMPORALITY, value));
        }
    }

    #[test]
    fn test_record_monotonic_sum() {
        let metric = Metric {
            name: "claude_code.cost.usage".to_string(),
            unit: "USD".to_string(),
            data: Some(metric::Data::Sum(Sum {
                aggregation_temporality: AggregationTemporality::Delta as i32,
                is_monotonic: true,
                ..Default::default()
            })),
            ..Default::default()
        };
        let index = record(
            &metric,
            MetricType::MonotonicSum,
            "claude_code_cost_usage_USD_total",
        );
        let decoded = decode(&index);
        let t = &decoded["claude_code_cost_usage_USD_total"];

        assert_eq!(
            t.get(SEMANTIC_METRIC_TYPE).map(String::as_str),
            Some("counter")
        );
        assert_eq!(
            t.get(SEMANTIC_METRIC_TEMPORALITY).map(String::as_str),
            Some("delta")
        );
        assert_eq!(t.get(SEMANTIC_METRIC_UNIT).map(String::as_str), Some("USD"));
        assert_eq!(
            t.get(SEMANTIC_METRIC_ORIGINAL_NAME).map(String::as_str),
            Some("claude_code.cost.usage")
        );
        assert_eq!(
            t.get(SEMANTIC_METRIC_METADATA_QUALITY).map(String::as_str),
            Some("declared")
        );
    }

    #[test]
    fn test_record_non_monotonic_sum() {
        let metric = Metric {
            name: "queue_size".to_string(),
            data: Some(metric::Data::Sum(Sum {
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                is_monotonic: false,
                ..Default::default()
            })),
            ..Default::default()
        };
        let index = record(&metric, MetricType::NonMonotonicSum, "queue_size");
        let decoded = decode(&index);
        let t = &decoded["queue_size"];
        assert_eq!(
            t.get(SEMANTIC_METRIC_TYPE).map(String::as_str),
            Some("updown_counter")
        );
        assert_eq!(
            t.get(SEMANTIC_METRIC_TEMPORALITY).map(String::as_str),
            Some("cumulative")
        );
        // Name unchanged by translation -> no original_name.
        assert_eq!(t.get(SEMANTIC_METRIC_ORIGINAL_NAME), None);
    }

    #[test]
    fn test_record_gauge_has_no_temporality() {
        let metric = Metric {
            name: "temperature".to_string(),
            data: Some(metric::Data::Gauge(Gauge::default())),
            ..Default::default()
        };
        let index = record(&metric, MetricType::Gauge, "temperature");
        let decoded = decode(&index);
        let t = &decoded["temperature"];
        assert_eq!(
            t.get(SEMANTIC_METRIC_TYPE).map(String::as_str),
            Some("gauge")
        );
        assert_eq!(t.get(SEMANTIC_METRIC_TEMPORALITY), None);
    }

    #[test]
    fn test_record_histogram_fans_out_with_distinct_types() {
        let metric = Metric {
            name: "request.duration".to_string(),
            unit: "s".to_string(),
            data: Some(metric::Data::Histogram(Histogram {
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                ..Default::default()
            })),
            ..Default::default()
        };
        let index = record(&metric, MetricType::Histogram, "request_duration");
        let decoded = decode(&index);

        let bucket = &decoded["request_duration_bucket"];
        assert_eq!(
            bucket.get(SEMANTIC_METRIC_TYPE).map(String::as_str),
            Some("histogram")
        );
        assert_eq!(
            bucket.get(SEMANTIC_METRIC_UNIT).map(String::as_str),
            Some("s")
        );

        for companion in ["request_duration_sum", "request_duration_count"] {
            let t = &decoded[companion];
            assert_eq!(
                t.get(SEMANTIC_METRIC_TYPE).map(String::as_str),
                Some("counter")
            );
            assert_eq!(
                t.get(SEMANTIC_METRIC_TEMPORALITY).map(String::as_str),
                Some("cumulative")
            );
        }
    }

    #[test]
    fn test_record_summary_fans_out() {
        let metric = Metric {
            name: "rpc.latency".to_string(),
            data: Some(metric::Data::Summary(Summary::default())),
            ..Default::default()
        };
        let index = record(&metric, MetricType::Summary, "rpc_latency");
        let decoded = decode(&index);

        assert_eq!(
            decoded["rpc_latency"]
                .get(SEMANTIC_METRIC_TYPE)
                .map(String::as_str),
            Some("summary")
        );
        // Summary has no temporality.
        assert_eq!(
            decoded["rpc_latency"].get(SEMANTIC_METRIC_TEMPORALITY),
            None
        );
        for companion in ["rpc_latency_count", "rpc_latency_sum"] {
            assert_eq!(
                decoded[companion]
                    .get(SEMANTIC_METRIC_TYPE)
                    .map(String::as_str),
                Some("counter")
            );
        }
    }
}
