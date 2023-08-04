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
use axum::TypedHeader;
use common_telemetry::timer;
use hyper::Body;
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use prost::Message;
use session::context::QueryContext;
use snafu::prelude::*;

use crate::error::{self, Result};
use crate::http::header::GreptimeDbName;
use crate::query_handler::OpenTelemetryProtocolHandlerRef;

#[axum_macros::debug_handler]
pub async fn metrics(
    State(handler): State<OpenTelemetryProtocolHandlerRef>,
    TypedHeader(db): TypedHeader<GreptimeDbName>,
    RawBody(body): RawBody,
) -> Result<OtlpResponse> {
    let ctx = QueryContext::with_db_name(db.value());
    let _timer = timer!(
        crate::metrics::METRIC_HTTP_OPENTELEMETRY_ELAPSED,
        &[(crate::metrics::METRIC_DB_LABEL, ctx.get_db_string())]
    );
    let request = parse_body(body).await?;
    handler.metrics(request, ctx).await.map(OtlpResponse)
}

async fn parse_body(body: Body) -> Result<ExportMetricsServiceRequest> {
    hyper::body::to_bytes(body)
        .await
        .context(error::HyperSnafu)
        .and_then(|buf| {
            ExportMetricsServiceRequest::decode(&buf[..]).context(error::DecodeOtlpRequestSnafu)
        })
}

pub struct OtlpResponse(ExportMetricsServiceResponse);

impl IntoResponse for OtlpResponse {
    fn into_response(self) -> axum::response::Response {
        (
            [(header::CONTENT_TYPE, "application/x-protobuf")],
            self.0.encode_to_vec(),
        )
            .into_response()
    }
}
