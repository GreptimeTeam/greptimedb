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
    let metrics = request
        .resource_metrics
        .iter()
        .flat_map(|resource_metrics| &resource_metrics.scope_metrics)
        .flat_map(|scope_metrics| &scope_metrics.metrics);

    let mut insert_batch = Vec::with_capacity(metrics.size_hint().0);
    for metric in metrics {
        if let Some(insert) = encode_metrics(metric)? {
            insert_batch.push(insert);
        }
    }

    let rows = insert_batch.iter().map(|i| i.row_count as usize).sum();
    let inserts = InsertRequests {
        inserts: insert_batch,
    };

    Ok((inserts, rows))
}

fn encode_metrics(metric: &Metric) -> Result<Option<InsertRequest>> {
    let name = &metric.name;
    // note that we don't store description or unit, we might want to deal with
    // these fields in the future.
    if let Some(data) = &metric.data {
        match data {
            metric::Data::Gauge(gauge) => encode_gauge(name, gauge).map(Some),
            metric::Data::Sum(sum) => encode_sum(name, sum).map(Some),
            metric::Data::Summary(summary) => encode_summary(name, summary).map(Some),
            // TODO(sunng87) leave histogram for next release
            metric::Data::Histogram(_hist) => Ok(None),
            metric::Data::ExponentialHistogram(_hist) => Ok(None),
        }
    } else {
        Ok(None)
    }
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

/// encode this gauge metric
///
/// note that there can be multiple data points in the request, it's going to be
/// stored as multiple rows
fn encode_gauge(name: &str, gauge: &Gauge) -> Result<InsertRequest> {
    let mut lines = LinesWriter::with_lines(gauge.data_points.len());

    for data_point in &gauge.data_points {
        for attr in &data_point.attributes {
            write_attribute(&mut lines, attr)?;
        }

        write_timestamp(&mut lines, data_point.time_unix_nano as i64)?;

        match data_point.value {
            Some(number_data_point::Value::AsInt(val)) => {
                // we coerce all values to f64
                lines
                    .write_f64(GREPTIME_VALUE, val as f64)
                    .context(error::OtlpMetricsWriteSnafu)?
            }
            Some(number_data_point::Value::AsDouble(val)) => {
                lines
                    .write_f64(GREPTIME_VALUE, val)
                    .context(error::OtlpMetricsWriteSnafu)?
            }
            _ => {}
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

/// encode this sum metric
///
/// `aggregation_temporality` and `monotonic` are ignored for now
fn encode_sum(name: &str, sum: &Sum) -> Result<InsertRequest> {
    let mut lines = LinesWriter::with_lines(sum.data_points.len());

    for data_point in &sum.data_points {
        for attr in &data_point.attributes {
            write_attribute(&mut lines, attr)?;
        }

        write_timestamp(&mut lines, data_point.time_unix_nano as i64)?;

        match data_point.value {
            Some(number_data_point::Value::AsInt(val)) => {
                // we coerce all values to f64
                lines
                    .write_f64(GREPTIME_VALUE, val as f64)
                    .context(error::OtlpMetricsWriteSnafu)?
            }
            Some(number_data_point::Value::AsDouble(val)) => {
                lines
                    .write_f64(GREPTIME_VALUE, val)
                    .context(error::OtlpMetricsWriteSnafu)?
            }
            _ => {}
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

fn encode_summary(name: &str, summary: &Summary) -> Result<InsertRequest> {
    let mut lines = LinesWriter::with_lines(summary.data_points.len());

    for data_point in &summary.data_points {
        for attr in &data_point.attributes {
            write_attribute(&mut lines, attr)?;
        }

        write_timestamp(&mut lines, data_point.time_unix_nano as i64)?;

        for quantile in &data_point.quantile_values {
            // here we don't store bucket boundary
            lines
                .write_f64(&format!("p{:02}", quantile.quantile), quantile.value)
                .context(error::OtlpMetricsWriteSnafu)?;
        }

        lines
            .write_u64("count", data_point.count)
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

    use super::*;

    #[test]
    fn test_normalize_otlp_name() {
        assert_eq!(normalize_otlp_name("jvm.memory.free"), "jvm_memory_free");
        assert_eq!(normalize_otlp_name("jvm-memory-free"), "jvm_memory_free");
        assert_eq!(normalize_otlp_name("jvm_memory_free"), "jvm_memory_free");
        assert_eq!(normalize_otlp_name("JVM_MEMORY_FREE"), "jvm_memory_free");
        assert_eq!(normalize_otlp_name("JVM_memory_FREE"), "jvm_memory_free");
    }
}
