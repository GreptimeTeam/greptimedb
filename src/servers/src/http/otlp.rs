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

use axum::extract::{RawBody, State};
use axum::http::header;
use axum::response::IntoResponse;
use axum::Extension;
use hyper::Body;
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use prost::Message;
use session::context::QueryContextRef;
use snafu::prelude::*;

use crate::error::{self, Result};
use crate::query_handler::OpenTelemetryProtocolHandlerRef;

#[axum_macros::debug_handler]
pub async fn metrics(
    State(handler): State<OpenTelemetryProtocolHandlerRef>,
    Extension(query_ctx): Extension<QueryContextRef>,
    RawBody(body): RawBody,
) -> Result<OtlpMetricsResponse> {
    let db = query_ctx.get_db_string();
    let _timer = crate::metrics::METRIC_HTTP_OPENTELEMETRY_METRICS_ELAPSED
        .with_label_values(&[db.as_str()])
        .start_timer();
    let request = parse_metrics_body(body).await?;
    handler
        .metrics(request, query_ctx)
        .await
        .map(OtlpMetricsResponse)
}

async fn parse_metrics_body(body: Body) -> Result<ExportMetricsServiceRequest> {
    hyper::body::to_bytes(body)
        .await
        .context(error::HyperSnafu)
        .and_then(|buf| {
            ExportMetricsServiceRequest::decode(&buf[..]).context(error::DecodeOtlpRequestSnafu)
        })
}

pub struct OtlpMetricsResponse(ExportMetricsServiceResponse);

impl IntoResponse for OtlpMetricsResponse {
    fn into_response(self) -> axum::response::Response {
        (
            [(header::CONTENT_TYPE, "application/x-protobuf")],
            self.0.encode_to_vec(),
        )
            .into_response()
    }
}

#[axum_macros::debug_handler]
pub async fn traces(
    State(handler): State<OpenTelemetryProtocolHandlerRef>,
    Extension(query_ctx): Extension<QueryContextRef>,
    RawBody(body): RawBody,
) -> Result<OtlpTracesResponse> {
    let db = query_ctx.get_db_string();
    let _timer = crate::metrics::METRIC_HTTP_OPENTELEMETRY_TRACES_ELAPSED
        .with_label_values(&[db.as_str()])
        .start_timer();
    let request = parse_traces_body(body).await?;
    handler
        .traces(request, query_ctx)
        .await
        .map(OtlpTracesResponse)
}

async fn parse_traces_body(body: Body) -> Result<ExportTraceServiceRequest> {
    hyper::body::to_bytes(body)
        .await
        .context(error::HyperSnafu)
        .and_then(|buf| {
            ExportTraceServiceRequest::decode(&buf[..]).context(error::DecodeOtlpRequestSnafu)
        })
}

pub struct OtlpTracesResponse(ExportTraceServiceResponse);

impl IntoResponse for OtlpTracesResponse {
    fn into_response(self) -> axum::response::Response {
        (
            [(header::CONTENT_TYPE, "application/x-protobuf")],
            self.0.encode_to_vec(),
        )
            .into_response()
    }
}
