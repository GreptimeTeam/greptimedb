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
use api::greptime_proto::io::prometheus::write::v2::histogram::{
    Count as PromCount, ResetHint, ZeroCount as PromZeroCount,
};
use api::greptime_proto::io::prometheus::write::v2::{BucketSpan, Histogram as PromHistogram};
use api::v1::value::ValueData;
use api::v1::{RowInsertRequests, SemanticType, Value};
use common_grpc::precision::Precision;
use common_query::native_histogram::{
    encode_native_histogram, native_histogram_column_schema, native_histogram_value_type,
};
use common_query::prelude::{GREPTIME_COUNT, greptime_timestamp, greptime_value};
use common_query::prometheus::PROMETHEUS_STALE_NAN_BITS;
use common_telemetry::warn;
use lazy_static::lazy_static;
use otel_arrow_rust::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
use otel_arrow_rust::proto::opentelemetry::common::v1::{AnyValue, KeyValue, any_value};
use otel_arrow_rust::proto::opentelemetry::metrics::v1::{metric, number_data_point, *};
use session::protocol_ctx::{MetricType, OtlpMetricCtx};
use table::requests::{
    METADATA_QUALITY_DECLARED, SEMANTIC_METRIC_METADATA_QUALITY, SEMANTIC_METRIC_ORIGINAL_NAME,
    SEMANTIC_METRIC_TEMPORALITY, SEMANTIC_METRIC_TYPE, SEMANTIC_METRIC_UNIT,
};

use crate::error::{self, Result};
use crate::otlp::trace::{KEY_SERVICE_INSTANCE_ID, KEY_SERVICE_NAME, KEY_SERVICE_NAMESPACE};
use crate::query_handler::MetricsIngestOutcome;
use crate::row_writer::{self, MultiTableData, TableData};
pub use crate::semantic::SemanticIndex;
use crate::semantic::{
    METRIC_TYPE_COUNTER, METRIC_TYPE_GAUGE, METRIC_TYPE_HISTOGRAM, METRIC_TYPE_SUMMARY,
    METRIC_TYPE_UPDOWN_COUNTER,
};

mod resource_info;
mod translator;

pub use resource_info::OTEL_RESOURCE_INFO_TABLE_NAME;
use resource_info::ResourceInfoData;
pub use translator::legacy_normalize_otlp_name;
pub(crate) use translator::ucum_to_openmetrics_unit;
use translator::{translate_label_name, translate_metric_name};

/// the default column count for table writer
const APPROXIMATE_COLUMN_COUNT: usize = 8;

const COUNT_TABLE_SUFFIX: &str = "_count";
const SUM_TABLE_SUFFIX: &str = "_sum";
const BUCKET_TABLE_SUFFIX: &str = "_bucket";

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
const MIN_EXPONENTIAL_HISTOGRAM_SCALE: i32 = -4;
const MAX_EXPONENTIAL_HISTOGRAM_SCALE: i32 = 8;
const MAX_REJECTION_MESSAGE_BYTES: usize = 512;

/// Result of converting one OTLP metrics request.
#[derive(Debug)]
pub struct MetricsConversion {
    pub requests: RowInsertRequests,
    /// Row count of `requests`; the resource descriptor is not counted.
    pub rows: usize,
    /// Per-table semantic index for the auto-create path to stamp as table
    /// options; covers the descriptor table too.
    pub semantic_index: SemanticIndex,
    /// The synthesized resource descriptor, written separately from the
    /// metric data. See [`resource_info`].
    pub resource_info: Option<RowInsertRequests>,
    pub outcome: MetricsIngestOutcome,
}

/// Convert OpenTelemetry metrics to GreptimeDB insert requests
///
/// See
/// <https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto>
/// for data structure of OTLP metrics.
pub fn to_grpc_insert_requests(
    request: ExportMetricsServiceRequest,
    metric_ctx: &mut OtlpMetricCtx,
) -> Result<MetricsConversion> {
    let mut table_writer = MultiTableData::default();
    let mut semantic_index = SemanticIndex::default();
    let mut outcome = MetricsIngestOutcome::default();
    let mut resource_info = ResourceInfoData::default();

    for resource in &request.resource_metrics {
        if metric_ctx.resource_info
            && !metric_ctx.is_legacy
            && let Some(r) = resource.resource.as_ref()
        {
            resource_info.observe(&r.attributes, resource, metric_ctx);
        }

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
                    &mut outcome,
                )?;
            }
        }
    }

    let (requests, rows) = table_writer.into_row_insert_requests();

    validate_sample_kinds(&requests)?;

    // The metric and the descriptor would fight over the same table across
    // two engines. The user's metric wins.
    let resource_info = if !metric_ctx.resource_info {
        None
    } else if requests
        .inserts
        .iter()
        .any(|r| r.table_name == OTEL_RESOURCE_INFO_TABLE_NAME)
    {
        warn!(
            "Skipping OTLP resource descriptor synthesis: the request writes \
             a metric named `{OTEL_RESOURCE_INFO_TABLE_NAME}`"
        );
        None
    } else {
        resource_info.into_row_insert_requests()?
    };
    if resource_info.is_some() {
        semantic_index.record_scalar(
            OTEL_RESOURCE_INFO_TABLE_NAME,
            SEMANTIC_METRIC_TYPE,
            crate::semantic::METRIC_TYPE_INFO,
        );
        semantic_index.record_scalar(
            OTEL_RESOURCE_INFO_TABLE_NAME,
            SEMANTIC_METRIC_METADATA_QUALITY,
            METADATA_QUALITY_DECLARED,
        );
    }

    Ok(MetricsConversion {
        requests,
        rows,
        semantic_index,
        outcome,
        resource_info,
    })
}

fn validate_sample_kinds(requests: &RowInsertRequests) -> Result<()> {
    for request in &requests.inserts {
        let Some(rows) = &request.rows else {
            continue;
        };
        let field_count = rows
            .schema
            .iter()
            .filter(|column| column.semantic_type == SemanticType::Field as i32)
            .count();
        let has_native_histogram = rows.schema.iter().any(|column| {
            column.semantic_type == SemanticType::Field as i32
                && api::helper::is_column_type_value_eq(
                    column.datatype,
                    column.datatype_extension.clone(),
                    native_histogram_value_type(),
                )
        });
        if has_native_histogram && field_count != 1 {
            return Err(error::InvalidParameterSnafu {
                reason: format!(
                    "OTLP metric `{}` cannot mix native histogram and float sample fields",
                    request.table_name
                ),
            }
            .build());
        }
    }
    Ok(())
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
        MetricType::ExponentialHistogram => {
            vec![(base.to_string(), METRIC_TYPE_HISTOGRAM)]
        }
        MetricType::Init => vec![],
    }
}

/// Maps OTLP `aggregation_temporality` to the semantic value, or `None` when the
/// instrument has no temporality (gauge/summary) or it is unspecified.
fn temporality_value(data: &metric::Data) -> Option<&'static str> {
    let raw = match data {
        metric::Data::Sum(sum) => sum.aggregation_temporality,
        metric::Data::Histogram(hist) => hist.aggregation_temporality,
        metric::Data::ExponentialHistogram(hist) => hist.aggregation_temporality,
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

/// Non-scalar values (bool, arrays, maps, bytes) are not representable as tags.
fn scalar_value_string(value: Option<&AnyValue>) -> Option<String> {
    match value.and_then(|v| v.value.as_ref())? {
        any_value::Value::StringValue(s) => Some(s.clone()),
        any_value::Value::IntValue(v) => Some(v.to_string()),
        any_value::Value::DoubleValue(v) => Some(v.to_string()),
        _ => None,
    }
}

/// Prometheus-style `(job, instance)` identity. Per the OTel Prometheus
/// compatibility spec `job` folds in `service.namespace` when present; a
/// resource without `service.name` gets no job rather than a fabricated one.
/// Both fields are optional strings, so a named type keeps a swapped
/// destructuring from type-checking at the call sites.
pub(crate) struct ServiceIdentity {
    pub job: Option<String>,
    pub instance: Option<String>,
}

pub(crate) fn service_identity(attrs: &[KeyValue]) -> ServiceIdentity {
    let mut name = None;
    let mut namespace = None;
    let mut instance = None;
    for kv in attrs {
        match kv.key.as_str() {
            KEY_SERVICE_NAME => name = scalar_value_string(kv.value.as_ref()),
            KEY_SERVICE_NAMESPACE => namespace = scalar_value_string(kv.value.as_ref()),
            KEY_SERVICE_INSTANCE_ID => instance = scalar_value_string(kv.value.as_ref()),
            _ => {}
        }
    }
    let job = name.map(|name| match namespace {
        Some(ns) if !ns.is_empty() => format!("{ns}/{name}"),
        _ => name,
    });
    ServiceIdentity { job, instance }
}

fn string_key_value(key: &str, value: String) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value)),
        }),
    }
}

fn process_resource_attrs(attrs: &mut Vec<KeyValue>, metric_ctx: &OtlpMetricCtx) {
    if metric_ctx.is_legacy {
        return;
    }

    // remap the service identity attributes to job and instance
    let ServiceIdentity { job, instance } = service_identity(attrs);

    // if promote all, then exclude the list, else, include the list
    if metric_ctx.promote_all_resource_attrs {
        attrs.retain(|kv| !metric_ctx.resource_attrs.contains(&kv.key));
    } else {
        attrs.retain(|kv| {
            metric_ctx.resource_attrs.contains(&kv.key)
                || DEFAULT_PROMOTE_ATTRS_SET.contains(&kv.key)
        });
    }

    if let Some(job) = job {
        attrs.push(string_key_value(JOB_KEY, job));
    }
    if let Some(instance) = instance {
        attrs.push(string_key_value(INSTANCE_KEY, instance));
    }
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
    outcome: &mut MetricsIngestOutcome,
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

    let emitted = if let Some(data) = &metric.data {
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
                add_accepted_data_points(outcome, gauge.data_points.len())?;
                !gauge.data_points.is_empty()
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
                add_accepted_data_points(outcome, sum.data_points.len())?;
                !sum.data_points.is_empty()
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
                add_accepted_data_points(outcome, summary.data_points.len())?;
                !summary.data_points.is_empty()
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
                add_accepted_data_points(outcome, hist.data_points.len())?;
                !hist.data_points.is_empty()
            }
            metric::Data::ExponentialHistogram(hist) => encode_exponential_histogram(
                table_writer,
                &name,
                hist,
                resource_attrs,
                scope_attrs,
                metric_ctx,
                outcome,
            )?,
        }
    } else {
        false
    };

    if emitted {
        // Stamp semantic metadata only after at least one row was accepted.
        record_metric_semantics(semantic_index, metric, &name, metric_ctx);
    }

    Ok(())
}

fn add_accepted_data_points(outcome: &mut MetricsIngestOutcome, count: usize) -> Result<()> {
    let count = i64::try_from(count).map_err(|_| {
        error::InvalidParameterSnafu {
            reason: "OTLP metrics data-point count exceeds i64",
        }
        .build()
    })?;
    outcome.accepted_data_points =
        outcome
            .accepted_data_points
            .checked_add(count)
            .ok_or_else(|| {
                error::InvalidParameterSnafu {
                    reason: "OTLP accepted data-point count overflows i64",
                }
                .build()
            })?;
    Ok(())
}

fn reject_data_points(
    outcome: &mut MetricsIngestOutcome,
    count: usize,
    reason: impl FnOnce() -> String,
) -> Result<()> {
    if count == 0 {
        return Ok(());
    }
    let count = i64::try_from(count).map_err(|_| {
        error::InvalidParameterSnafu {
            reason: "OTLP rejected data-point count exceeds i64",
        }
        .build()
    })?;
    outcome.rejected_data_points =
        outcome
            .rejected_data_points
            .checked_add(count)
            .ok_or_else(|| {
                error::InvalidParameterSnafu {
                    reason: "OTLP rejected data-point count overflows i64",
                }
                .build()
            })?;
    append_rejection_message(&mut outcome.error_message, reason);
    Ok(())
}

fn append_rejection_message(message: &mut Option<String>, reason: impl FnOnce() -> String) {
    let message = message.get_or_insert_with(String::new);
    let separator = if message.is_empty() { "" } else { "; " };
    let Some(available) = MAX_REJECTION_MESSAGE_BYTES.checked_sub(message.len()) else {
        return;
    };
    if available <= separator.len() {
        return;
    }
    let reason = reason();
    message.push_str(separator);

    let available = MAX_REJECTION_MESSAGE_BYTES - message.len();
    if reason.len() <= available {
        message.push_str(&reason);
        return;
    }

    const ELLIPSIS: &str = "...";
    let mut end = available.saturating_sub(ELLIPSIS.len());
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    message.push_str(&reason[..end]);
    if available >= ELLIPSIS.len() {
        message.push_str(ELLIPSIS);
    }
}

fn encode_exponential_histogram(
    table_writer: &mut MultiTableData,
    name: &str,
    histogram: &ExponentialHistogram,
    resource_attrs: Option<&Vec<KeyValue>>,
    scope_attrs: Option<&Vec<KeyValue>>,
    metric_ctx: &OtlpMetricCtx,
    outcome: &mut MetricsIngestOutcome,
) -> Result<bool> {
    if let Err(rejection) = exponential_histogram_gate(histogram, metric_ctx) {
        reject_data_points(outcome, histogram.data_points.len(), || {
            rejection.message(name)
        })?;
        return Ok(false);
    }

    let column_schema = native_histogram_column_schema().map_err(|error| {
        error::InternalSnafu {
            err_msg: error.to_string(),
        }
        .build()
    })?;
    let mut emitted = false;
    for (index, data_point) in histogram.data_points.iter().enumerate() {
        let (value, timestamp_nanos) = match exponential_histogram_value(data_point) {
            Ok(value) => value,
            Err(reason) => {
                reject_data_points(outcome, 1, || {
                    format!("metric `{name}` data point {index}: {reason}")
                })?;
                continue;
            }
        };

        let table = table_writer.get_or_default_table_data(
            name,
            APPROXIMATE_COLUMN_COUNT,
            histogram.data_points.len(),
        );
        let mut row = table.alloc_one_row();
        write_tags_and_timestamp(
            table,
            &mut row,
            resource_attrs,
            scope_attrs,
            Some(data_point.attributes.as_ref()),
            timestamp_nanos,
            metric_ctx,
        )?;
        row_writer::write_by_schema(
            table,
            std::iter::once((column_schema.clone(), Some(value))),
            &mut row,
        )?;
        table.add_row(row);
        add_accepted_data_points(outcome, 1)?;
        emitted = true;
    }

    Ok(emitted)
}

pub(crate) enum ExponentialHistogramRejection {
    Disabled,
    DeltaTemporality,
    UnspecifiedTemporality,
}

impl ExponentialHistogramRejection {
    fn message(&self, name: &str) -> String {
        match self {
            Self::Disabled => format!(
                "metric `{name}` uses OTLP exponential histograms; set otlp.experimental_enable_exponential_histogram = true to enable ingestion"
            ),
            Self::DeltaTemporality => format!(
                "metric `{name}` uses delta OTLP exponential histograms; only cumulative temporality is supported"
            ),
            Self::UnspecifiedTemporality => format!(
                "metric `{name}` has unspecified OTLP exponential histogram temporality; cumulative temporality is required"
            ),
        }
    }
}

/// Whole-metric acceptance, decided once for the encoder and for the resource
/// descriptor, which must not describe a resource whose data was rejected.
/// Individual points can still fail [`exponential_histogram_value`].
pub(crate) fn exponential_histogram_gate(
    histogram: &ExponentialHistogram,
    metric_ctx: &OtlpMetricCtx,
) -> std::result::Result<(), ExponentialHistogramRejection> {
    if !metric_ctx.experimental_enable_exponential_histogram {
        return Err(ExponentialHistogramRejection::Disabled);
    }
    match AggregationTemporality::try_from(histogram.aggregation_temporality) {
        Ok(AggregationTemporality::Cumulative) => Ok(()),
        Ok(AggregationTemporality::Delta) => Err(ExponentialHistogramRejection::DeltaTemporality),
        _ => Err(ExponentialHistogramRejection::UnspecifiedTemporality),
    }
}

pub(crate) fn exponential_histogram_value(
    data_point: &ExponentialHistogramDataPoint,
) -> std::result::Result<(ValueData, i64), String> {
    if data_point.start_time_unix_nano > data_point.time_unix_nano {
        return Err(format!(
            "start_time_unix_nano {} exceeds time_unix_nano {}",
            data_point.start_time_unix_nano, data_point.time_unix_nano
        ));
    }

    let timestamp_nanos = i64::try_from(data_point.time_unix_nano)
        .map_err(|_| format!("time_unix_nano {} overflows i64", data_point.time_unix_nano))?;
    let timestamp = timestamp_millis(data_point.time_unix_nano, "time_unix_nano")?;
    let start_timestamp =
        timestamp_millis(data_point.start_time_unix_nano, "start_time_unix_nano")?;

    let no_recorded_value = data_point.flags & DataPointFlags::NoRecordedValueMask as u32 != 0;
    let histogram = if no_recorded_value {
        PromHistogram {
            sum: f64::from_bits(PROMETHEUS_STALE_NAN_BITS),
            schema: 0,
            zero_threshold: 0.0,
            reset_hint: ResetHint::Unspecified as i32,
            timestamp,
            start_timestamp,
            count: Some(PromCount::CountInt(0)),
            zero_count: Some(PromZeroCount::ZeroCountInt(0)),
            ..Default::default()
        }
    } else {
        if data_point.scale < MIN_EXPONENTIAL_HISTOGRAM_SCALE {
            return Err(format!(
                "scale {} is unsupported; minimum supported scale is {}",
                data_point.scale, MIN_EXPONENTIAL_HISTOGRAM_SCALE
            ));
        }
        if !data_point.zero_threshold.is_finite() || data_point.zero_threshold < 0.0 {
            return Err(format!(
                "zero_threshold {} must be finite and non-negative",
                data_point.zero_threshold
            ));
        }

        let downscale_shift = if data_point.scale > MAX_EXPONENTIAL_HISTOGRAM_SCALE {
            let shift = data_point
                .scale
                .checked_sub(MAX_EXPONENTIAL_HISTOGRAM_SCALE)
                .ok_or_else(|| "downscale shift overflows i32".to_string())?;
            u32::try_from(shift).map_err(|_| "downscale shift exceeds u32".to_string())?
        } else {
            0
        };
        let (positive_spans, positive_deltas, positive_count) =
            convert_bucket_range("positive", data_point.positive.as_ref(), downscale_shift)?;
        let (negative_spans, negative_deltas, negative_count) =
            convert_bucket_range("negative", data_point.negative.as_ref(), downscale_shift)?;
        let bucket_count = data_point
            .zero_count
            .checked_add(positive_count)
            .and_then(|count| count.checked_add(negative_count))
            .ok_or_else(|| "bucket observation total overflows u64".to_string())?;
        if bucket_count != data_point.count {
            return Err(format!(
                "buckets contain {bucket_count} observations, declared count is {}",
                data_point.count
            ));
        }

        i64::try_from(data_point.count)
            .map_err(|_| format!("count {} overflows i64", data_point.count))?;
        i64::try_from(data_point.zero_count)
            .map_err(|_| format!("zero_count {} overflows i64", data_point.zero_count))?;

        let sum = match data_point.sum {
            Some(sum) if sum.is_nan() => f64::NAN,
            Some(sum) => sum,
            None => f64::NAN,
        };
        PromHistogram {
            sum,
            schema: data_point.scale.min(MAX_EXPONENTIAL_HISTOGRAM_SCALE),
            zero_threshold: data_point.zero_threshold,
            negative_spans,
            negative_deltas,
            positive_spans,
            positive_deltas,
            reset_hint: ResetHint::Unspecified as i32,
            timestamp,
            start_timestamp,
            count: Some(PromCount::CountInt(data_point.count)),
            zero_count: Some(PromZeroCount::ZeroCountInt(data_point.zero_count)),
            ..Default::default()
        }
    };

    encode_native_histogram(&histogram)
        .map(|value| (value, timestamp_nanos))
        .map_err(|error| format!("OTLP exponential histogram cannot be encoded: {error}"))
}

fn timestamp_millis(timestamp_nanos: u64, name: &str) -> std::result::Result<i64, String> {
    i64::try_from(timestamp_nanos / 1_000_000)
        .map_err(|_| format!("{name} {timestamp_nanos} milliseconds overflow i64"))
}

fn convert_bucket_range(
    name: &str,
    buckets: Option<&exponential_histogram_data_point::Buckets>,
    downscale_shift: u32,
) -> std::result::Result<(Vec<BucketSpan>, Vec<i64>, u64), String> {
    let Some(buckets) = buckets else {
        return Ok((Vec::new(), Vec::new(), 0));
    };
    if buckets.bucket_counts.is_empty() {
        return Ok((Vec::new(), Vec::new(), 0));
    }

    let mut merged = Vec::<(i32, u64)>::with_capacity(buckets.bucket_counts.len());
    let mut total = 0u64;
    for (position, count) in buckets.bucket_counts.iter().copied().enumerate() {
        let position =
            i32::try_from(position).map_err(|_| format!("{name} bucket length exceeds i32"))?;
        let source_index = buckets.offset.checked_add(position).ok_or_else(|| {
            format!(
                "{name} bucket index overflows i32 at offset {} and position {position}",
                buckets.offset
            )
        })?;
        let target_index = downscale_bucket_index(source_index, downscale_shift)?
            .checked_add(1)
            .ok_or_else(|| format!("{name} shifted bucket index overflows i32"))?;
        if let Some((last_index, last_count)) = merged.last_mut() {
            if *last_index == target_index {
                *last_count = last_count
                    .checked_add(count)
                    .ok_or_else(|| format!("{name} merged bucket count overflows u64"))?;
                total = total
                    .checked_add(count)
                    .ok_or_else(|| format!("{name} bucket count total overflows u64"))?;
                continue;
            }
            let next_index = last_index
                .checked_add(1)
                .ok_or_else(|| format!("{name} target bucket index overflows i32"))?;
            if target_index != next_index {
                return Err(format!(
                    "{name} bucket indexes are not contiguous after downscaling"
                ));
            }
        }
        total = total
            .checked_add(count)
            .ok_or_else(|| format!("{name} bucket count total overflows u64"))?;
        merged.push((target_index, count));
    }

    let length = u32::try_from(merged.len())
        .map_err(|_| format!("{name} bucket span length exceeds u32"))?;
    let span = BucketSpan {
        offset: merged[0].0,
        length,
    };
    let mut deltas = Vec::with_capacity(merged.len());
    let mut previous = 0i64;
    for (_, count) in merged {
        let count = i64::try_from(count)
            .map_err(|_| format!("{name} bucket count {count} overflows i64"))?;
        let delta = count
            .checked_sub(previous)
            .ok_or_else(|| format!("{name} bucket delta overflows i64"))?;
        deltas.push(delta);
        previous = count;
    }

    Ok((vec![span], deltas, total))
}

fn downscale_bucket_index(index: i32, downscale_shift: u32) -> std::result::Result<i32, String> {
    if downscale_shift == 0 {
        return Ok(index);
    }
    if downscale_shift >= i32::BITS {
        return Ok(if index < 0 { -1 } else { 0 });
    }

    let divisor = 1i64
        .checked_shl(downscale_shift)
        .ok_or_else(|| format!("downscale shift {downscale_shift} is invalid"))?;
    i32::try_from(i64::from(index).div_euclid(divisor))
        .map_err(|_| format!("downscaled bucket index {index} overflows i32"))
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
        // TODO(sunng87): allow different type of values
        let value = scalar_value_string(attr.value.as_ref())?;
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
        Some((key, value))
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
    for data_point in &hist.data_points {
        let bucket_table = table_writer.get_or_default_table_data(
            &bucket_table_name,
            APPROXIMATE_COLUMN_COUNT,
            data_points_len * 3,
        );
        let mut accumulated_count = 0;
        for (idx, count) in data_point.bucket_counts.iter().enumerate() {
            let mut bucket_row = bucket_table.alloc_one_row();
            write_tags_and_timestamp(
                bucket_table,
                &mut bucket_row,
                resource_attrs,
                scope_attrs,
                Some(data_point.attributes.as_ref()),
                data_point.time_unix_nano as i64,
                metric_ctx,
            )?;

            if let Some(upper_bounds) = data_point.explicit_bounds.get(idx) {
                row_writer::write_tag(
                    bucket_table,
                    HISTOGRAM_LE_COLUMN,
                    upper_bounds,
                    &mut bucket_row,
                )?;
            } else if idx == data_point.explicit_bounds.len() {
                // The last bucket
                row_writer::write_tag(
                    bucket_table,
                    HISTOGRAM_LE_COLUMN,
                    f64::INFINITY,
                    &mut bucket_row,
                )?;
            }

            accumulated_count += count;
            row_writer::write_f64(
                bucket_table,
                greptime_value(),
                accumulated_count as f64,
                &mut bucket_row,
            )?;

            bucket_table.add_row(bucket_row);
        }

        if let Some(sum) = data_point.sum {
            let sum_table = table_writer.get_or_default_table_data(
                &sum_table_name,
                APPROXIMATE_COLUMN_COUNT,
                data_points_len,
            );
            let mut sum_row = sum_table.alloc_one_row();
            write_tags_and_timestamp(
                sum_table,
                &mut sum_row,
                resource_attrs,
                scope_attrs,
                Some(data_point.attributes.as_ref()),
                data_point.time_unix_nano as i64,
                metric_ctx,
            )?;

            row_writer::write_f64(sum_table, greptime_value(), sum, &mut sum_row)?;
            sum_table.add_row(sum_row);
        }

        let count_table = table_writer.get_or_default_table_data(
            &count_table_name,
            APPROXIMATE_COLUMN_COUNT,
            data_points_len,
        );
        let mut count_row = count_table.alloc_one_row();
        write_tags_and_timestamp(
            count_table,
            &mut count_row,
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
            &mut count_row,
        )?;
        count_table.add_row(count_row);
    }

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
    use common_query::prelude::set_default_prefix;
    use otel_arrow_rust::proto::opentelemetry::common::v1::AnyValue;
    use otel_arrow_rust::proto::opentelemetry::common::v1::any_value::Value as Val;
    use otel_arrow_rust::proto::opentelemetry::metrics::v1::number_data_point::Value;
    use otel_arrow_rust::proto::opentelemetry::metrics::v1::summary_data_point::ValueAtQuantile;
    use otel_arrow_rust::proto::opentelemetry::metrics::v1::{
        AggregationTemporality, HistogramDataPoint, NumberDataPoint, SummaryDataPoint,
    };
    use otel_arrow_rust::proto::opentelemetry::resource::v1::Resource;

    use super::*;

    fn keyvalue(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(Val::StringValue(value.into())),
            }),
        }
    }

    fn descriptor_ctx() -> OtlpMetricCtx {
        OtlpMetricCtx {
            resource_info: true,
            ..Default::default()
        }
    }

    fn attr_value(attrs: &[KeyValue], key: &str) -> Option<String> {
        attrs
            .iter()
            .find(|kv| kv.key == key)
            .and_then(|kv| scalar_value_string(kv.value.as_ref()))
    }

    fn gauge_request(
        resource_attrs: Vec<KeyValue>,
        metric_name: &str,
    ) -> ExportMetricsServiceRequest {
        use otel_arrow_rust::proto::opentelemetry::resource::v1::Resource;
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: resource_attrs,
                    ..Default::default()
                }),
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: metric_name.to_string(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: 1_000_000,
                                value: Some(Value::AsInt(1)),
                                ..Default::default()
                            }],
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn column_names(request: &RowInsertRequests, table: &str) -> Vec<String> {
        request
            .inserts
            .iter()
            .find(|r| r.table_name == table)
            .unwrap_or_else(|| panic!("missing table {table}"))
            .rows
            .as_ref()
            .unwrap()
            .schema
            .iter()
            .map(|c| c.column_name.clone())
            .collect()
    }

    #[test]
    fn test_conversion_synthesizes_resource_descriptor() {
        set_default_prefix(None).unwrap();
        let request = gauge_request(
            vec![keyvalue("service.name", "api"), keyvalue("host.id", "h-1")],
            "my_gauge",
        );
        let conversion = to_grpc_insert_requests(request, &mut descriptor_ctx()).unwrap();

        // descriptor keeps raw OTel keys while the metric table's labels went
        // through the (default underscore-escaping) translation strategy
        let resource_info = conversion.resource_info.expect("descriptor synthesized");
        let descriptor_cols = column_names(&resource_info, OTEL_RESOURCE_INFO_TABLE_NAME);
        assert!(descriptor_cols.contains(&"host.id".to_string()));
        assert!(descriptor_cols.contains(&"service.name".to_string()));
        assert!(descriptor_cols.contains(&"job".to_string()));
        let metric_cols = column_names(&conversion.requests, "my_gauge");
        assert!(metric_cols.contains(&"service_name".to_string()));
        assert!(!metric_cols.contains(&"service.name".to_string()));
        // host.id is not in the promote list: only the descriptor keeps it
        assert!(!metric_cols.contains(&"host_id".to_string()));

        let decoded = decode(&conversion.semantic_index);
        let t = &decoded[OTEL_RESOURCE_INFO_TABLE_NAME];
        assert_eq!(
            t.get(SEMANTIC_METRIC_TYPE).map(String::as_str),
            Some("info")
        );
        assert_eq!(
            t.get(SEMANTIC_METRIC_METADATA_QUALITY).map(String::as_str),
            Some("declared")
        );
    }

    #[test]
    fn test_conversion_skips_descriptor_for_legacy_mode() {
        set_default_prefix(None).unwrap();
        let request = gauge_request(
            vec![keyvalue("service.name", "api"), keyvalue("host.id", "h-1")],
            "my_gauge",
        );
        let mut ctx = OtlpMetricCtx {
            is_legacy: true,
            ..descriptor_ctx()
        };
        let conversion = to_grpc_insert_requests(request, &mut ctx).unwrap();
        assert!(conversion.resource_info.is_none());

        // legacy tables predate job/instance and the promote filter; adding
        // either would alter the schema of tables already in use
        let cols = column_names(&conversion.requests, "my_gauge");
        assert!(!cols.contains(&"job".to_string()));
        assert!(cols.contains(&"host_id".to_string()));
    }

    #[test]
    fn test_conversion_skips_descriptor_on_metric_name_collision() {
        set_default_prefix(None).unwrap();
        let request = gauge_request(
            vec![keyvalue("service.name", "api")],
            OTEL_RESOURCE_INFO_TABLE_NAME,
        );
        let conversion = to_grpc_insert_requests(request, &mut descriptor_ctx()).unwrap();
        assert!(conversion.resource_info.is_none());
        // the metric itself still goes through the main path
        assert!(
            conversion
                .requests
                .inserts
                .iter()
                .any(|r| r.table_name == OTEL_RESOURCE_INFO_TABLE_NAME)
        );
    }

    #[test]
    fn test_job_composition_follows_service_namespace() {
        let mut attrs = vec![
            keyvalue("service.name", "api"),
            keyvalue("service.namespace", "shop"),
        ];
        process_resource_attrs(&mut attrs, &OtlpMetricCtx::default());
        assert_eq!(attr_value(&attrs, "job").as_deref(), Some("shop/api"));

        let mut attrs = vec![keyvalue("service.name", "api")];
        process_resource_attrs(&mut attrs, &OtlpMetricCtx::default());
        assert_eq!(attr_value(&attrs, "job").as_deref(), Some("api"));

        let mut attrs = vec![
            keyvalue("service.name", "api"),
            keyvalue("service.namespace", ""),
        ];
        process_resource_attrs(&mut attrs, &OtlpMetricCtx::default());
        assert_eq!(attr_value(&attrs, "job").as_deref(), Some("api"));

        // no service.name: no job is fabricated
        let mut attrs = vec![
            keyvalue("service.namespace", "shop"),
            keyvalue("service.instance.id", "inst-1"),
        ];
        process_resource_attrs(&mut attrs, &OtlpMetricCtx::default());
        assert_eq!(attr_value(&attrs, "job"), None);
        assert_eq!(attr_value(&attrs, "instance").as_deref(), Some("inst-1"));
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
    fn test_encode_legacy_summary_keeps_legacy_column_names() {
        set_default_prefix(Some("custom")).unwrap();
        let mut tables = MultiTableData::default();
        let summary = Summary {
            data_points: vec![SummaryDataPoint {
                attributes: vec![keyvalue("host", "testserver")],
                time_unix_nano: 100,
                count: 25,
                quantile_values: vec![ValueAtQuantile {
                    quantile: 0.90,
                    value: 1000.0,
                }],
                ..Default::default()
            }],
        };

        encode_summary(
            &mut tables,
            "datamon",
            &summary,
            None,
            None,
            &OtlpMetricCtx {
                is_legacy: true,
                ..Default::default()
            },
        )
        .unwrap();

        let table = tables.get_or_default_table_data("datamon", 0, 0);
        assert_eq!(
            table
                .columns()
                .iter()
                .map(|column| column.column_name.as_str())
                .collect::<Vec<_>>(),
            vec!["host", "custom_timestamp", "greptime_p90", GREPTIME_COUNT,]
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
        let nested: BTreeMap<String, BTreeMap<String, BTreeMap<String, String>>> =
            serde_json::from_str(&index.encode("public").expect("non-empty index")).unwrap();
        nested.into_values().next().unwrap()
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

    fn exponential_buckets(
        offset: i32,
        bucket_counts: Vec<u64>,
    ) -> exponential_histogram_data_point::Buckets {
        exponential_histogram_data_point::Buckets {
            offset,
            bucket_counts,
        }
    }

    fn exponential_point() -> ExponentialHistogramDataPoint {
        ExponentialHistogramDataPoint {
            start_time_unix_nano: 1_000_000,
            time_unix_nano: 2_000_000,
            count: 28,
            sum: None,
            scale: 9,
            zero_count: 7,
            positive: Some(exponential_buckets(-3, vec![1, 2, 3, 4, 5, 6])),
            zero_threshold: 0.0,
            ..Default::default()
        }
    }

    fn native_field(value: &ValueData, name: &str) -> Option<ValueData> {
        let ValueData::StructValue(value) = value else {
            panic!("expected native histogram Struct value");
        };
        let index = common_query::native_histogram::NATIVE_HISTOGRAM_FIELD_NAMES
            .iter()
            .position(|field| *field == name)
            .unwrap();
        value.items[index].value_data.clone()
    }

    fn i32_list(value: Option<ValueData>) -> Vec<i32> {
        let Some(ValueData::ListValue(value)) = value else {
            panic!("expected i32 list");
        };
        value
            .items
            .into_iter()
            .map(|item| match item.value_data {
                Some(ValueData::I32Value(value)) => value,
                _ => panic!("expected i32 value"),
            })
            .collect()
    }

    fn i64_list(value: Option<ValueData>) -> Vec<i64> {
        let Some(ValueData::ListValue(value)) = value else {
            panic!("expected i64 list");
        };
        value
            .items
            .into_iter()
            .map(|item| match item.value_data {
                Some(ValueData::I64Value(value)) => value,
                _ => panic!("expected i64 value"),
            })
            .collect()
    }

    #[test]
    fn test_downscale_bucket_index_uses_signed_floor_division() {
        assert_eq!(downscale_bucket_index(-3, 1).unwrap(), -2);
        assert_eq!(downscale_bucket_index(-2, 1).unwrap(), -1);
        assert_eq!(downscale_bucket_index(-1, 1).unwrap(), -1);
        assert_eq!(downscale_bucket_index(0, 1).unwrap(), 0);
        assert_eq!(downscale_bucket_index(1, 1).unwrap(), 0);
        assert_eq!(downscale_bucket_index(2, 1).unwrap(), 1);
        assert_eq!(downscale_bucket_index(i32::MIN, 32).unwrap(), -1);
        assert_eq!(downscale_bucket_index(i32::MAX, 32).unwrap(), 0);
    }

    #[test]
    fn test_convert_bucket_range_downscales_before_prometheus_shift() {
        let buckets = exponential_buckets(-3, vec![1, 2, 3, 4, 5, 6]);
        let (spans, deltas, total) = convert_bucket_range("positive", Some(&buckets), 1).unwrap();

        assert_eq!(
            spans,
            vec![BucketSpan {
                offset: -1,
                length: 4
            }]
        );
        assert_eq!(deltas, vec![1, 4, 4, -3]);
        assert_eq!(total, 21);
    }

    #[test]
    fn test_exponential_histogram_value_uses_integer_family() {
        use common_query::native_histogram::{
            COUNT_F64_FIELD, COUNT_I64_FIELD, POSITIVE_BUCKETS_I64_FIELD,
            POSITIVE_SPAN_LENGTHS_FIELD, POSITIVE_SPAN_OFFSETS_FIELD, SCHEMA_FIELD, SUM_FIELD,
            ZERO_COUNT_I64_FIELD,
        };

        let (value, timestamp_nanos) = exponential_histogram_value(&exponential_point()).unwrap();

        assert_eq!(timestamp_nanos, 2_000_000);
        assert_eq!(
            native_field(&value, SCHEMA_FIELD),
            Some(ValueData::I32Value(8))
        );
        assert_eq!(
            native_field(&value, COUNT_I64_FIELD),
            Some(ValueData::I64Value(28))
        );
        assert_eq!(
            native_field(&value, ZERO_COUNT_I64_FIELD),
            Some(ValueData::I64Value(7))
        );
        assert_eq!(native_field(&value, COUNT_F64_FIELD), None);
        assert_eq!(
            i32_list(native_field(&value, POSITIVE_SPAN_OFFSETS_FIELD)),
            vec![-1]
        );
        assert_eq!(
            i32_list(native_field(&value, POSITIVE_SPAN_LENGTHS_FIELD)),
            vec![4]
        );
        assert_eq!(
            i64_list(native_field(&value, POSITIVE_BUCKETS_I64_FIELD)),
            vec![1, 5, 9, 6]
        );
        let Some(ValueData::F64Value(sum)) = native_field(&value, SUM_FIELD) else {
            panic!("expected histogram sum");
        };
        assert_eq!(sum.to_bits(), f64::NAN.to_bits());
        assert_ne!(sum.to_bits(), PROMETHEUS_STALE_NAN_BITS);
    }

    #[test]
    fn test_no_recorded_value_ignores_other_value_fields() {
        use common_query::native_histogram::{
            COUNT_I64_FIELD, POSITIVE_BUCKETS_I64_FIELD, SCHEMA_FIELD, SUM_FIELD,
            ZERO_COUNT_I64_FIELD, ZERO_THRESHOLD_FIELD,
        };

        let point = ExponentialHistogramDataPoint {
            start_time_unix_nano: 1_000_000,
            time_unix_nano: 2_000_000,
            count: u64::MAX,
            sum: Some(1.0),
            scale: i32::MIN,
            zero_count: u64::MAX,
            positive: Some(exponential_buckets(i32::MAX, vec![u64::MAX])),
            flags: DataPointFlags::NoRecordedValueMask as u32,
            zero_threshold: f64::NAN,
            ..Default::default()
        };
        let (value, _) = exponential_histogram_value(&point).unwrap();

        assert_eq!(
            native_field(&value, SCHEMA_FIELD),
            Some(ValueData::I32Value(0))
        );
        assert_eq!(
            native_field(&value, ZERO_THRESHOLD_FIELD),
            Some(ValueData::F64Value(0.0))
        );
        assert_eq!(
            native_field(&value, COUNT_I64_FIELD),
            Some(ValueData::I64Value(0))
        );
        assert_eq!(
            native_field(&value, ZERO_COUNT_I64_FIELD),
            Some(ValueData::I64Value(0))
        );
        assert!(i64_list(native_field(&value, POSITIVE_BUCKETS_I64_FIELD)).is_empty());
        let Some(ValueData::F64Value(sum)) = native_field(&value, SUM_FIELD) else {
            panic!("expected histogram sum");
        };
        assert_eq!(sum.to_bits(), PROMETHEUS_STALE_NAN_BITS);
    }

    #[test]
    fn test_non_flag_nan_sum_is_normalized() {
        use common_query::native_histogram::SUM_FIELD;

        let point = ExponentialHistogramDataPoint {
            sum: Some(f64::from_bits(PROMETHEUS_STALE_NAN_BITS)),
            ..Default::default()
        };
        let (value, _) = exponential_histogram_value(&point).unwrap();
        let Some(ValueData::F64Value(sum)) = native_field(&value, SUM_FIELD) else {
            panic!("expected histogram sum");
        };
        assert_eq!(sum.to_bits(), f64::NAN.to_bits());
        assert_ne!(sum.to_bits(), PROMETHEUS_STALE_NAN_BITS);
    }

    #[test]
    fn test_exponential_histogram_rejects_invalid_values() {
        let mut cases = Vec::new();

        let mut point = exponential_point();
        point.scale = -5;
        cases.push((point, "scale -5 is unsupported"));

        let mut point = exponential_point();
        point.zero_threshold = f64::INFINITY;
        cases.push((point, "must be finite and non-negative"));

        let mut point = exponential_point();
        point.start_time_unix_nano = point.time_unix_nano + 1;
        cases.push((point, "start_time_unix_nano"));

        let mut point = exponential_point();
        point.count = 27;
        cases.push((point, "declared count is 27"));

        let point = ExponentialHistogramDataPoint {
            count: u64::MAX,
            zero_count: u64::MAX,
            ..Default::default()
        };
        cases.push((point, "count 18446744073709551615 overflows i64"));

        let point = ExponentialHistogramDataPoint {
            count: 1,
            scale: 8,
            positive: Some(exponential_buckets(i32::MAX, vec![1])),
            ..Default::default()
        };
        cases.push((point, "shifted bucket index overflows i32"));

        for (point, expected) in cases {
            let error = exponential_histogram_value(&point).unwrap_err();
            assert!(
                error.contains(expected),
                "expected {expected:?}, got {error}"
            );
        }

        let buckets = exponential_buckets(0, vec![u64::MAX, 1]);
        let error = convert_bucket_range("positive", Some(&buckets), 1).unwrap_err();
        assert!(
            error.contains("merged bucket count overflows u64"),
            "{error}"
        );

        let buckets = exponential_buckets(i32::MAX, vec![1, 1]);
        let error = convert_bucket_range("positive", Some(&buckets), 1).unwrap_err();
        assert!(error.contains("bucket index overflows i32"), "{error}");
    }

    fn metrics_request(metrics: Vec<Metric>) -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn exponential_metric(
        name: impl Into<String>,
        data_points: Vec<ExponentialHistogramDataPoint>,
        temporality: AggregationTemporality,
    ) -> Metric {
        Metric {
            name: name.into(),
            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points,
                aggregation_temporality: temporality as i32,
            })),
            ..Default::default()
        }
    }

    fn histogram_metric(name: impl Into<String>) -> Metric {
        Metric {
            name: name.into(),
            data: Some(metric::Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    start_time_unix_nano: 1_000_000,
                    time_unix_nano: 2_000_000,
                    count: 1,
                    sum: Some(1.0),
                    bucket_counts: vec![1],
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
            })),
            ..Default::default()
        }
    }

    #[test]
    fn test_exponential_histogram_gate_and_partial_outcome() {
        let request = metrics_request(vec![
            Metric {
                name: "temperature".to_string(),
                data: Some(metric::Data::Gauge(Gauge {
                    data_points: vec![NumberDataPoint::default()],
                })),
                ..Default::default()
            },
            exponential_metric(
                "latency",
                vec![exponential_point()],
                AggregationTemporality::Cumulative,
            ),
        ]);
        let MetricsConversion {
            requests,
            semantic_index,
            outcome,
            ..
        } = to_grpc_insert_requests(request, &mut OtlpMetricCtx::default()).unwrap();

        assert_eq!(outcome.accepted_data_points, 1);
        assert_eq!(outcome.rejected_data_points, 1);
        assert!(
            outcome
                .error_message
                .as_deref()
                .unwrap()
                .contains("otlp.experimental_enable_exponential_histogram")
        );
        assert_eq!(requests.inserts.len(), 1);
        assert_eq!(requests.inserts[0].table_name, "temperature");
        let semantics = decode(&semantic_index);
        assert!(semantics.contains_key("temperature"));
        assert!(!semantics.contains_key("latency"));

        let empty = metrics_request(vec![exponential_metric(
            "empty",
            vec![],
            AggregationTemporality::Cumulative,
        )]);
        let outcome = to_grpc_insert_requests(empty, &mut OtlpMetricCtx::default())
            .unwrap()
            .outcome;
        assert_eq!(outcome.rejected_data_points, 0);
        assert_eq!(outcome.error_message, None);
    }

    #[test]
    fn test_exponential_histogram_cannot_share_table_with_scalar_metric() {
        let request = metrics_request(vec![
            Metric {
                name: "latency".to_string(),
                data: Some(metric::Data::Gauge(Gauge {
                    data_points: vec![NumberDataPoint {
                        value: Some(number_data_point::Value::AsDouble(1.0)),
                        ..Default::default()
                    }],
                })),
                ..Default::default()
            },
            exponential_metric(
                "latency",
                vec![exponential_point()],
                AggregationTemporality::Cumulative,
            ),
        ]);
        let mut ctx = OtlpMetricCtx {
            experimental_enable_exponential_histogram: true,
            ..Default::default()
        };

        let error = to_grpc_insert_requests(request, &mut ctx).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("cannot mix native histogram and float sample fields")
        );
    }

    #[test]
    fn test_histogram_cannot_replace_exponential_histogram_table() {
        let request = metrics_request(vec![
            exponential_metric(
                "latency_bucket",
                vec![exponential_point()],
                AggregationTemporality::Cumulative,
            ),
            histogram_metric("latency"),
        ]);
        let mut ctx = OtlpMetricCtx {
            experimental_enable_exponential_histogram: true,
            ..Default::default()
        };

        let error = to_grpc_insert_requests(request, &mut ctx).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("cannot mix native histogram and float sample fields")
        );
    }

    #[test]
    fn test_histograms_with_same_name_across_resources_are_merged() {
        let metric = histogram_metric("latency");
        let request = ExportMetricsServiceRequest {
            resource_metrics: ["service-a", "service-b"]
                .into_iter()
                .map(|service| ResourceMetrics {
                    resource: Some(Resource {
                        attributes: vec![keyvalue("service.name", service)],
                        ..Default::default()
                    }),
                    scope_metrics: vec![ScopeMetrics {
                        metrics: vec![metric.clone()],
                        ..Default::default()
                    }],
                    ..Default::default()
                })
                .collect(),
        };

        let MetricsConversion {
            requests,
            rows,
            outcome,
            ..
        } = to_grpc_insert_requests(request, &mut OtlpMetricCtx::default()).unwrap();

        assert_eq!(outcome.accepted_data_points, 2);
        assert_eq!(rows, 6);
        assert_eq!(requests.inserts.len(), 3);
        for request in requests.inserts {
            assert_eq!(
                request.rows.unwrap().rows.len(),
                2,
                "{}",
                request.table_name
            );
        }
    }

    #[test]
    fn test_exponential_histogram_rejects_temporality_before_stale_point() {
        let stale = ExponentialHistogramDataPoint {
            flags: DataPointFlags::NoRecordedValueMask as u32,
            ..Default::default()
        };
        for temporality in [
            AggregationTemporality::Delta,
            AggregationTemporality::Unspecified,
        ] {
            let request = metrics_request(vec![exponential_metric(
                "latency",
                vec![stale.clone()],
                temporality,
            )]);
            let mut ctx = OtlpMetricCtx {
                experimental_enable_exponential_histogram: true,
                ..Default::default()
            };
            let MetricsConversion {
                requests,
                rows,
                semantic_index,
                outcome,
                ..
            } = to_grpc_insert_requests(request, &mut ctx).unwrap();

            assert_eq!(outcome.accepted_data_points, 0);
            assert_eq!(outcome.rejected_data_points, 1);
            assert_eq!(rows, 0);
            assert!(requests.inserts.is_empty());
            assert!(semantic_index.is_empty());
        }
    }

    #[test]
    fn test_exponential_histogram_legacy_and_new_modes_share_struct() {
        use common_query::prelude::greptime_native_histogram;

        let mut point = exponential_point();
        point.sum = Some(42.0);
        let request = metrics_request(vec![exponential_metric(
            "request.duration",
            vec![point],
            AggregationTemporality::Cumulative,
        )]);
        let mut new_ctx = OtlpMetricCtx {
            experimental_enable_exponential_histogram: true,
            ..Default::default()
        };
        let new_requests = to_grpc_insert_requests(request.clone(), &mut new_ctx)
            .unwrap()
            .requests;
        let mut legacy_ctx = OtlpMetricCtx {
            experimental_enable_exponential_histogram: true,
            is_legacy: true,
            ..Default::default()
        };
        let legacy_requests = to_grpc_insert_requests(request, &mut legacy_ctx)
            .unwrap()
            .requests;

        let new_insert = &new_requests.inserts[0];
        let legacy_insert = &legacy_requests.inserts[0];
        assert_eq!(new_insert.table_name, "request_duration");
        assert_eq!(legacy_insert.table_name, "request_duration");
        let new_rows = new_insert.rows.as_ref().unwrap();
        let legacy_rows = legacy_insert.rows.as_ref().unwrap();
        let field = greptime_native_histogram();
        let new_histogram = new_rows.rows[0].values[new_rows
            .schema
            .iter()
            .position(|column| column.column_name == field)
            .unwrap()]
        .clone();
        let legacy_histogram = legacy_rows.rows[0].values[legacy_rows
            .schema
            .iter()
            .position(|column| column.column_name == field)
            .unwrap()]
        .clone();
        assert_eq!(new_histogram, legacy_histogram);
        assert!(matches!(
            new_rows.rows[0].values[new_rows
                .schema
                .iter()
                .position(|column| column.column_name == greptime_timestamp())
                .unwrap()]
            .value_data,
            Some(ValueData::TimestampMillisecondValue(2))
        ));
        assert!(matches!(
            legacy_rows.rows[0].values[legacy_rows
                .schema
                .iter()
                .position(|column| column.column_name == greptime_timestamp())
                .unwrap()]
            .value_data,
            Some(ValueData::TimestampNanosecondValue(2_000_000))
        ));
    }

    #[test]
    fn test_rejection_message_is_bounded() {
        let request = metrics_request(vec![exponential_metric(
            "x".repeat(1_000),
            vec![ExponentialHistogramDataPoint::default()],
            AggregationTemporality::Cumulative,
        )]);
        let outcome = to_grpc_insert_requests(request, &mut OtlpMetricCtx::default())
            .unwrap()
            .outcome;

        assert_eq!(outcome.rejected_data_points, 1);
        assert!(outcome.error_message.unwrap().len() <= MAX_REJECTION_MESSAGE_BYTES);
    }

    #[test]
    fn test_rejection_message_reason_is_lazy_after_cap() {
        let mut outcome = MetricsIngestOutcome {
            error_message: Some("x".repeat(MAX_REJECTION_MESSAGE_BYTES)),
            ..Default::default()
        };
        let mut reason_built = false;

        reject_data_points(&mut outcome, 1, || {
            reason_built = true;
            "unused".to_string()
        })
        .unwrap();

        assert_eq!(outcome.rejected_data_points, 1);
        assert!(!reason_built);
    }
}
