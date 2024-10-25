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

use core::str;
use std::result::Result as StdResult;
use std::sync::Arc;

use axum::extract::{FromRequestParts, State};
use axum::http::header::HeaderValue;
use axum::http::request::Parts;
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::{async_trait, Extension};
use bytes::Bytes;
use common_telemetry::tracing;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use pipeline::util::to_pipeline_version;
use pipeline::{PipelineWay, SelectInfo};
use prost::Message;
use session::context::{Channel, QueryContext};
use snafu::prelude::*;

use super::header::constants::GREPTIME_LOG_EXTRACT_KEYS_HEADER_NAME;
use super::header::{write_cost_header_map, CONTENT_TYPE_PROTOBUF};
use crate::error::{self, PipelineSnafu, Result};
use crate::http::header::constants::{
    GREPTIME_LOG_PIPELINE_NAME_HEADER_NAME, GREPTIME_LOG_PIPELINE_VERSION_HEADER_NAME,
    GREPTIME_LOG_TABLE_NAME_HEADER_NAME,
};
use crate::query_handler::OpenTelemetryProtocolHandlerRef;

#[axum_macros::debug_handler]
#[tracing::instrument(skip_all, fields(protocol = "otlp", request_type = "metrics"))]
pub async fn metrics(
    State(handler): State<OpenTelemetryProtocolHandlerRef>,
    Extension(mut query_ctx): Extension<QueryContext>,

    bytes: Bytes,
) -> Result<OtlpResponse<ExportMetricsServiceResponse>> {
    let db = query_ctx.get_db_string();
    query_ctx.set_channel(Channel::Otlp);
    let query_ctx = Arc::new(query_ctx);
    let _timer = crate::metrics::METRIC_HTTP_OPENTELEMETRY_METRICS_ELAPSED
        .with_label_values(&[db.as_str()])
        .start_timer();
    let request =
        ExportMetricsServiceRequest::decode(bytes).context(error::DecodeOtlpRequestSnafu)?;

    handler
        .metrics(request, query_ctx)
        .await
        .map(|o| OtlpResponse {
            resp_body: ExportMetricsServiceResponse {
                partial_success: None,
            },
            write_cost: o.meta.cost,
        })
}

#[axum_macros::debug_handler]
#[tracing::instrument(skip_all, fields(protocol = "otlp", request_type = "traces"))]
pub async fn traces(
    State(handler): State<OpenTelemetryProtocolHandlerRef>,
    Extension(mut query_ctx): Extension<QueryContext>,
    bytes: Bytes,
) -> Result<OtlpResponse<ExportTraceServiceResponse>> {
    let db = query_ctx.get_db_string();
    query_ctx.set_channel(Channel::Otlp);
    let query_ctx = Arc::new(query_ctx);
    let _timer = crate::metrics::METRIC_HTTP_OPENTELEMETRY_TRACES_ELAPSED
        .with_label_values(&[db.as_str()])
        .start_timer();
    let request =
        ExportTraceServiceRequest::decode(bytes).context(error::DecodeOtlpRequestSnafu)?;
    handler
        .traces(request, query_ctx)
        .await
        .map(|o| OtlpResponse {
            resp_body: ExportTraceServiceResponse {
                partial_success: None,
            },
            write_cost: o.meta.cost,
        })
}

pub struct PipelineInfo {
    pub pipeline_name: Option<String>,
    pub pipeline_version: Option<String>,
}

fn pipeline_header_error(
    header: &HeaderValue,
    key: &str,
) -> StdResult<String, (http::StatusCode, String)> {
    let header_utf8 = str::from_utf8(header.as_bytes());
    match header_utf8 {
        Ok(s) => Ok(s.to_string()),
        Err(_) => Err((
            StatusCode::BAD_REQUEST,
            format!("`{}` header is not valid UTF-8 string type.", key),
        )),
    }
}

#[async_trait]
impl<S> FromRequestParts<S> for PipelineInfo
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, String);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> StdResult<Self, Self::Rejection> {
        let pipeline_name = parts.headers.get(GREPTIME_LOG_PIPELINE_NAME_HEADER_NAME);
        let pipeline_version = parts.headers.get(GREPTIME_LOG_PIPELINE_VERSION_HEADER_NAME);
        match (pipeline_name, pipeline_version) {
            (Some(name), Some(version)) => Ok(PipelineInfo {
                pipeline_name: Some(pipeline_header_error(
                    name,
                    GREPTIME_LOG_PIPELINE_NAME_HEADER_NAME,
                )?),
                pipeline_version: Some(pipeline_header_error(
                    version,
                    GREPTIME_LOG_PIPELINE_VERSION_HEADER_NAME,
                )?),
            }),
            (None, _) => Ok(PipelineInfo {
                pipeline_name: None,
                pipeline_version: None,
            }),
            (Some(name), None) => Ok(PipelineInfo {
                pipeline_name: Some(pipeline_header_error(
                    name,
                    GREPTIME_LOG_PIPELINE_NAME_HEADER_NAME,
                )?),
                pipeline_version: None,
            }),
        }
    }
}

pub struct TableInfo {
    table_name: String,
}

#[async_trait]
impl<S> FromRequestParts<S> for TableInfo
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, String);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> StdResult<Self, Self::Rejection> {
        let table_name = parts.headers.get(GREPTIME_LOG_TABLE_NAME_HEADER_NAME);

        match table_name {
            Some(name) => Ok(TableInfo {
                table_name: pipeline_header_error(name, GREPTIME_LOG_TABLE_NAME_HEADER_NAME)?,
            }),
            None => Ok(TableInfo {
                table_name: "opentelemetry_logs".to_string(),
            }),
        }
    }
}

pub struct SelectInfoWrapper(SelectInfo);

#[async_trait]
impl<S> FromRequestParts<S> for SelectInfoWrapper
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, String);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> StdResult<Self, Self::Rejection> {
        let select = parts.headers.get(GREPTIME_LOG_EXTRACT_KEYS_HEADER_NAME);

        match select {
            Some(name) => {
                let select_header =
                    pipeline_header_error(name, GREPTIME_LOG_EXTRACT_KEYS_HEADER_NAME)?;
                if select_header.is_empty() {
                    Ok(SelectInfoWrapper(Default::default()))
                } else {
                    Ok(SelectInfoWrapper(SelectInfo::from(select_header)))
                }
            }
            None => Ok(SelectInfoWrapper(Default::default())),
        }
    }
}

#[axum_macros::debug_handler]
#[tracing::instrument(skip_all, fields(protocol = "otlp", request_type = "logs"))]
pub async fn logs(
    State(handler): State<OpenTelemetryProtocolHandlerRef>,
    Extension(mut query_ctx): Extension<QueryContext>,
    pipeline_info: PipelineInfo,
    table_info: TableInfo,
    SelectInfoWrapper(select_info): SelectInfoWrapper,
    bytes: Bytes,
) -> Result<OtlpResponse<ExportLogsServiceResponse>> {
    let db = query_ctx.get_db_string();
    query_ctx.set_channel(Channel::Otlp);
    let query_ctx = Arc::new(query_ctx);
    let _timer = crate::metrics::METRIC_HTTP_OPENTELEMETRY_LOGS_ELAPSED
        .with_label_values(&[db.as_str()])
        .start_timer();
    let request = ExportLogsServiceRequest::decode(bytes).context(error::DecodeOtlpRequestSnafu)?;

    let pipeline_way = if let Some(pipeline_name) = &pipeline_info.pipeline_name {
        let pipeline_version =
            to_pipeline_version(pipeline_info.pipeline_version).context(PipelineSnafu)?;
        let pipeline = match handler
            .get_pipeline(pipeline_name, pipeline_version, query_ctx.clone())
            .await
        {
            Ok(p) => p,
            Err(e) => {
                return Err(e);
            }
        };
        PipelineWay::Custom(pipeline)
    } else {
        PipelineWay::OtlpLog(Box::new(select_info))
    };

    handler
        .logs(request, pipeline_way, table_info.table_name, query_ctx)
        .await
        .map(|o| OtlpResponse {
            resp_body: ExportLogsServiceResponse {
                partial_success: None,
            },
            write_cost: o.meta.cost,
        })
}

pub struct OtlpResponse<T: Message> {
    resp_body: T,
    write_cost: usize,
}

impl<T: Message> IntoResponse for OtlpResponse<T> {
    fn into_response(self) -> axum::response::Response {
        let mut header_map = write_cost_header_map(self.write_cost);
        header_map.insert(header::CONTENT_TYPE, CONTENT_TYPE_PROTOBUF.clone());

        (header_map, self.resp_body.encode_to_vec()).into_response()
    }
}
