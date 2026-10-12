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

mod otlp;
mod summary;

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::rejection::QueryRejection;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode as GreptimeStatusCode;
use common_query::Output;
use common_telemetry::error;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::Deserialize;
use serde_json::{Value, json};
use session::context::QueryContext;

use crate::error::{Error, status_code_to_http_status};
use crate::http::extractor::TraceTableName;
use crate::http::jaeger::{
    QueryTraceParams, convert_string_to_boolean, convert_string_to_number, covert_to_records,
    empty_string_as_none, operations_from_records, services_from_records, update_query_context,
};
use crate::metrics::METRIC_JAEGER_QUERY_ELAPSED;
use crate::query_handler::JaegerQueryHandlerRef;

/// Jaeger API v3 uses an error envelope rather than the legacy `errors` array.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn not_found() -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: "No traces found".into(),
        }
    }
}

impl From<Error> for ApiError {
    fn from(err: Error) -> Self {
        error!(err; "Jaeger v3 query failed");
        Self {
            status: status_code_to_http_status(&err.status_code()),
            message: err.output_msg(),
        }
    }
}

impl From<QueryRejection> for ApiError {
    fn from(err: QueryRejection) -> Self {
        Self::invalid(err.body_text())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({"error": {"httpCode": self.status.as_u16(), "message": self.message}})),
        )
            .into_response()
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct OperationsParams {
    service: String,
    #[serde(rename = "spanKind", alias = "span_kind")]
    span_kind: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct GetTraceParams {
    #[serde(rename = "startTime", alias = "start_time")]
    #[serde(default, deserialize_with = "empty_string_as_none")]
    start_time: Option<String>,
    #[serde(rename = "endTime", alias = "end_time")]
    #[serde(default, deserialize_with = "empty_string_as_none")]
    end_time: Option<String>,
    // GreptimeDB returns stored spans without Jaeger's clock-skew enrichment.
    #[serde(rename = "rawTraces", alias = "raw_traces")]
    #[serde(default, deserialize_with = "empty_string_as_none")]
    _raw_traces: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
pub struct FindTracesParams {
    #[serde(rename = "query.serviceName", alias = "query.service_name")]
    service_name: Option<String>,
    #[serde(rename = "query.operationName", alias = "query.operation_name")]
    operation_name: Option<String>,
    #[serde(rename = "query.startTimeMin", alias = "query.start_time_min")]
    start_time_min: String,
    #[serde(rename = "query.startTimeMax", alias = "query.start_time_max")]
    start_time_max: String,
    #[serde(rename = "query.durationMin", alias = "query.duration_min")]
    #[serde(default, deserialize_with = "empty_string_as_none")]
    duration_min: Option<String>,
    #[serde(rename = "query.durationMax", alias = "query.duration_max")]
    #[serde(default, deserialize_with = "empty_string_as_none")]
    duration_max: Option<String>,
    #[serde(
        rename = "query.searchDepth",
        alias = "query.search_depth",
        alias = "query.num_traces"
    )]
    #[serde(default, deserialize_with = "empty_string_as_none")]
    search_depth: Option<i32>,
    #[serde(rename = "query.attributes")]
    #[serde(default, deserialize_with = "empty_string_as_none")]
    attributes: Option<String>,
    #[serde(rename = "query.rawTraces", alias = "query.raw_traces")]
    #[serde(default, deserialize_with = "empty_string_as_none")]
    _raw_traces: Option<bool>,
    #[serde(rename = "query.filter")]
    filter: Option<String>,
}

impl FindTracesParams {
    fn into_query(self) -> Result<QueryTraceParams, ApiError> {
        if self.filter.is_some() {
            return Err(ApiError::invalid("query.filter is not supported"));
        }
        let start = parse_timestamp(&self.start_time_min)?;
        let end = parse_timestamp(&self.start_time_max)?;
        if start >= end {
            return Err(ApiError::invalid(
                "query.startTimeMin must be before query.startTimeMax",
            ));
        }
        let limit = match self.search_depth.unwrap_or(0) {
            0 => 100,
            limit @ 1..=10_000 => limit,
            _ => {
                return Err(ApiError::invalid(
                    "query.searchDepth must be between 0 and 10000",
                ));
            }
        };
        let tags = self
            .attributes
            .map(|attributes| {
                let values = serde_json::from_str::<HashMap<String, String>>(&attributes)
                    .map_err(|err| ApiError::invalid(format!("Invalid query.attributes: {err}")))?;
                Ok::<_, ApiError>(
                    values
                        .into_iter()
                        .map(|(key, value)| {
                            let value = Value::String(value);
                            let value = convert_string_to_number(&value)
                                .or_else(|| convert_string_to_boolean(&value))
                                .unwrap_or(value);
                            (key, value)
                        })
                        .collect(),
                )
            })
            .transpose()?;
        let query = QueryTraceParams {
            service_name: self.service_name.filter(|name| !name.is_empty()),
            operation_name: self.operation_name.filter(|name| !name.is_empty()),
            start_time: Some(start),
            // The backend uses an inclusive bound; API v3 specifies an exclusive bound.
            end_time: Some(end - 1),
            min_duration: self
                .duration_min
                .as_deref()
                .map(parse_duration)
                .transpose()?,
            max_duration: self
                .duration_max
                .as_deref()
                .map(parse_duration)
                .transpose()?,
            limit: Some(limit as usize),
            tags,
            fetch_full_trace: true,
            ..Default::default()
        };
        if let (Some(min), Some(max)) = (query.min_duration, query.max_duration)
            && min > max
        {
            return Err(ApiError::invalid(
                "query.durationMin must not exceed query.durationMax",
            ));
        }
        Ok(query)
    }
}

fn parse_timestamp(value: &str) -> Result<i64, ApiError> {
    chrono::DateTime::parse_from_rfc3339(value)
        .map_err(|err| ApiError::invalid(format!("Invalid RFC3339 timestamp '{value}': {err}")))?
        .timestamp_nanos_opt()
        .ok_or_else(|| ApiError::invalid("Timestamp is outside the supported nanosecond range"))
}

fn parse_duration(value: &str) -> Result<u64, ApiError> {
    let invalid = || ApiError::invalid(format!("Invalid Go duration '{value}'"));
    let mut rest = value.strip_prefix('+').unwrap_or(value);
    if rest == "0" {
        return Ok(0);
    }
    if rest.is_empty() {
        return Err(invalid());
    }
    let mut total = 0_u64;
    while !rest.is_empty() {
        let number_end = rest
            .find(|ch: char| !ch.is_ascii_digit() && ch != '.')
            .ok_or_else(invalid)?;
        let amount: Decimal = rest[..number_end].parse().map_err(|_| invalid())?;
        rest = &rest[number_end..];
        let unit_end = rest
            .find(|ch: char| ch.is_ascii_digit() || ch == '.')
            .unwrap_or(rest.len());
        let unit = match &rest[..unit_end] {
            "ns" => 1_u64,
            "us" | "µs" | "μs" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60_000_000_000,
            "h" => 3_600_000_000_000,
            _ => return Err(invalid()),
        };
        let nanos = amount
            .checked_mul(Decimal::from(unit))
            .and_then(|amount| amount.trunc().to_u64())
            .ok_or_else(invalid)?;
        total = total
            .checked_add(nanos)
            .filter(|total| *total <= i64::MAX as u64)
            .ok_or_else(invalid)?;
        rest = &rest[unit_end..];
    }
    Ok(total)
}

pub async fn handle_get_services(
    State(handler): State<JaegerQueryHandlerRef>,
    Extension(mut ctx): Extension<QueryContext>,
    TraceTableName(table): TraceTableName,
) -> Result<Json<Value>, ApiError> {
    update_query_context(&mut ctx, table);
    let _timer = METRIC_JAEGER_QUERY_ELAPSED
        .with_label_values(&[&ctx.get_db_string(), "/api/v3/services"])
        .start_timer();
    let services = match query_records(handler.get_services(Arc::new(ctx)).await).await? {
        Some(records) => services_from_records(records)?,
        None => vec![],
    };
    Ok(Json(json!({"services": services})))
}

pub async fn handle_get_operations(
    State(handler): State<JaegerQueryHandlerRef>,
    params: Result<Query<OperationsParams>, QueryRejection>,
    Extension(mut ctx): Extension<QueryContext>,
    TraceTableName(table): TraceTableName,
) -> Result<Json<Value>, ApiError> {
    let Query(params) = params?;
    if params.service.is_empty() {
        return Err(ApiError::invalid("service is required"));
    }
    update_query_context(&mut ctx, table);
    let _timer = METRIC_JAEGER_QUERY_ELAPSED
        .with_label_values(&[&ctx.get_db_string(), "/api/v3/operations"])
        .start_timer();
    let operations = match query_records(
        handler
            .get_operations(
                Arc::new(ctx),
                &params.service,
                params.span_kind.as_deref().filter(|kind| !kind.is_empty()),
            )
            .await,
    )
    .await?
    {
        Some(records) => operations_from_records(records, true)?,
        None => vec![],
    };
    let operations: Vec<_> = operations.into_iter().map(|operation| json!({
        "name": operation.name,
        "spanKind": operation.span_kind.filter(|kind| !kind.is_empty()).unwrap_or_else(|| "internal".into()),
    })).collect();
    Ok(Json(json!({"operations": operations})))
}

pub async fn handle_get_trace(
    State(handler): State<JaegerQueryHandlerRef>,
    Path(trace_id): Path<String>,
    params: Result<Query<GetTraceParams>, QueryRejection>,
    Extension(mut ctx): Extension<QueryContext>,
    TraceTableName(table): TraceTableName,
) -> Result<Json<Value>, ApiError> {
    let Query(params) = params?;
    if !matches!(trace_id.len(), 16 | 32) || !trace_id.bytes().all(|ch| ch.is_ascii_hexdigit()) {
        return Err(ApiError::invalid(
            "trace_id must be a 64-bit or 128-bit hexadecimal ID",
        ));
    }
    let trace_id = format!("{:0>32}", trace_id.to_ascii_lowercase());
    let start = params
        .start_time
        .as_deref()
        .map(parse_timestamp)
        .transpose()?;
    let end = params
        .end_time
        .as_deref()
        .map(parse_timestamp)
        .transpose()?;
    if let (Some(start), Some(end)) = (start, end)
        && start > end
    {
        return Err(ApiError::invalid("startTime must not exceed endTime"));
    }
    update_query_context(&mut ctx, table);
    let _timer = METRIC_JAEGER_QUERY_ELAPSED
        .with_label_values(&[&ctx.get_db_string(), "/api/v3/traces/{trace_id}"])
        .start_timer();
    // API v3 time bounds are lookup hints. Applying them as span filters would truncate the trace,
    // so they are only validated.
    trace_response(
        handler
            .get_trace(Arc::new(ctx), &trace_id, None, None, None)
            .await,
    )
    .await
}

pub async fn handle_find_traces(
    State(handler): State<JaegerQueryHandlerRef>,
    params: Result<Query<FindTracesParams>, QueryRejection>,
    Extension(mut ctx): Extension<QueryContext>,
    TraceTableName(table): TraceTableName,
) -> Result<Json<Value>, ApiError> {
    let Query(params) = params?;
    let query = params.into_query()?;
    update_query_context(&mut ctx, table);
    let _timer = METRIC_JAEGER_QUERY_ELAPSED
        .with_label_values(&[&ctx.get_db_string(), "/api/v3/traces"])
        .start_timer();
    trace_response(handler.find_traces(Arc::new(ctx), query).await).await
}

pub async fn handle_find_trace_summaries(
    State(handler): State<JaegerQueryHandlerRef>,
    params: Result<Query<FindTracesParams>, QueryRejection>,
    Extension(mut ctx): Extension<QueryContext>,
    TraceTableName(table): TraceTableName,
) -> Result<Json<Value>, ApiError> {
    let Query(params) = params?;
    let query = params.into_query()?;
    update_query_context(&mut ctx, table);
    let _timer = METRIC_JAEGER_QUERY_ELAPSED
        .with_label_values(&[&ctx.get_db_string(), "/api/v3/trace-summaries"])
        .start_timer();
    let summaries = match query_records(handler.find_traces(Arc::new(ctx), query).await).await? {
        Some(records) => summary::summarize(otlp::traces_from_records(records)?),
        None => vec![],
    };
    Ok(Json(json!({"summaries": summaries})))
}

async fn query_records(
    output: crate::error::Result<Output>,
) -> Result<Option<crate::http::HttpRecordsOutput>, ApiError> {
    match output {
        Ok(output) => covert_to_records(output)
            .await?
            .map(Some)
            .ok_or_else(|| ApiError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                message: "Unexpected Jaeger query output".into(),
            }),
        Err(err) if err.status_code() == GreptimeStatusCode::TableNotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

async fn trace_response(output: crate::error::Result<Output>) -> Result<Json<Value>, ApiError> {
    let records = query_records(output)
        .await?
        .ok_or_else(ApiError::not_found)?;
    if records.rows.is_empty() {
        return Err(ApiError::not_found());
    }
    Ok(Json(json!({"result": otlp::traces_from_records(records)?})))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_traces_parameters() {
        for (start, end, service, operation, depth, duration) in [
            (
                "start_time_min",
                "start_time_max",
                "service_name",
                "operation_name",
                "num_traces",
                "duration_min",
            ),
            (
                "startTimeMin",
                "startTimeMax",
                "serviceName",
                "operationName",
                "searchDepth",
                "durationMin",
            ),
        ] {
            let uri = format!("/api/v3/traces?query.{start}=2026-01-01T00:00:00.000000001Z&query.{end}=2026-01-01T00:00:01Z&query.{service}=checkout&query.{operation}=GET&query.{depth}=7&query.{duration}=1.5ms&query.attributes=%7B%22error%22%3A%22true%22%7D").parse().unwrap();
            let Query(params) = Query::<FindTracesParams>::try_from_uri(&uri).unwrap();
            let query = params.into_query().unwrap();
            assert_eq!(query.service_name.as_deref(), Some("checkout"));
            assert_eq!(query.operation_name.as_deref(), Some("GET"));
            assert_eq!(query.limit, Some(7));
            assert_eq!(query.min_duration, Some(1_500_000));
            assert_eq!(query.start_time, Some(1767225600000000001));
            assert_eq!(query.end_time, Some(1767225600999999999));
            assert_eq!(query.tags.unwrap()["error"], json!(true));
            assert!(query.fetch_full_trace);
        }
        let uri = "/api/v3/traces?query.startTimeMin=2026-01-01T00:00:00Z&query.startTimeMax=2026-01-01T01:00:00Z&query.durationMin=&query.durationMax=&query.searchDepth=&query.rawTraces=&query.attributes=".parse().unwrap();
        let Query(params) = Query::<FindTracesParams>::try_from_uri(&uri).unwrap();
        let query = params.into_query().unwrap();
        assert_eq!(query.service_name, None);
        assert_eq!(query.limit, Some(100));
        for name in [
            "query.searchDepth",
            "query.search_depth",
            "query.num_traces",
        ] {
            for (depth, expected) in [(0, 100), (1, 1), (10_000, 10_000)] {
                let uri = format!("/api/v3/traces?query.startTimeMin=2026-01-01T00:00:00Z&query.startTimeMax=2026-01-01T01:00:00Z&{name}={depth}").parse().unwrap();
                let Query(params) = Query::<FindTracesParams>::try_from_uri(&uri).unwrap();
                assert_eq!(params.into_query().unwrap().limit, Some(expected));
            }
        }
    }

    #[test]
    fn test_go_durations() {
        for (value, expected) in [
            ("0", 0),
            ("1.5ms", 1_500_000),
            ("1h2m3.5s", 3_723_500_000_000),
            ("+.5us", 500),
            ("1.µs", 1_000),
            ("1μs", 1_000),
            ("0.1ns", 0),
            ("9223372036854775807ns", i64::MAX as u64),
        ] {
            assert_eq!(parse_duration(value).unwrap(), expected, "{value}");
        }
        for value in [
            "",
            "1",
            "-1s",
            "1.2.3s",
            "s",
            "1 s",
            "1d",
            "1s-1s",
            "9223372036854775808ns",
        ] {
            assert!(parse_duration(value).is_err(), "{value}");
        }
    }

    #[test]
    fn test_invalid_find_traces_parameters() {
        for extra in [
            "query.searchDepth=-1",
            "query.searchDepth=10001",
            "query.searchDepth=2147483647",
            "query.durationMin=-1s",
            "query.durationMin=2s&query.durationMax=1s",
            "query.durationMax=999999999999h",
            "query.attributes=%7B%22error%22%3Atrue%7D",
            "query.attributes=invalid",
            "query.filter=%7B%7D",
        ] {
            let uri = format!("/api/v3/traces?query.startTimeMin=2026-01-01T00:00:00Z&query.startTimeMax=2026-01-01T01:00:00Z&{extra}").parse().unwrap();
            let Query(params) = Query::<FindTracesParams>::try_from_uri(&uri).unwrap();
            assert_eq!(
                params.into_query().unwrap_err().status,
                StatusCode::BAD_REQUEST,
                "{extra}"
            );
        }
        for (start, end) in [
            ("invalid", "2026-01-01T00:00:00Z"),
            ("2500-01-01T00:00:00Z", "2500-01-02T00:00:00Z"),
            ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        ] {
            assert!(
                FindTracesParams {
                    start_time_min: start.into(),
                    start_time_max: end.into(),
                    ..Default::default()
                }
                .into_query()
                .is_err()
            );
        }
    }
}
