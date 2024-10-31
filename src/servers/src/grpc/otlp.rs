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

use std::result::Result as StdResult;
use std::sync::Arc;

use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsService;
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceService;
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use session::context::{Channel, QueryContext};
use snafu::{OptionExt, ResultExt};
use tonic::{Request, Response, Status};

use crate::error;
use crate::http::header::constants::GREPTIME_TRACE_TABLE_NAME_HEADER_NAME;
use crate::otlp::trace::TRACE_TABLE_NAME;
use crate::query_handler::OpenTelemetryProtocolHandlerRef;

pub struct OtlpService {
    handler: OpenTelemetryProtocolHandlerRef,
}

impl OtlpService {
    pub fn new(handler: OpenTelemetryProtocolHandlerRef) -> Self {
        Self { handler }
    }
}

#[async_trait::async_trait]
impl TraceService for OtlpService {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> StdResult<Response<ExportTraceServiceResponse>, Status> {
        let (headers, extensions, req) = request.into_parts();

        let table_name = match headers.get(GREPTIME_TRACE_TABLE_NAME_HEADER_NAME) {
            Some(table_name) => table_name
                .to_str()
                .context(error::InvalidTableNameSnafu)?
                .to_string(),
            None => TRACE_TABLE_NAME.to_string(),
        };

        let mut ctx = extensions
            .get::<QueryContext>()
            .cloned()
            .context(error::MissingQueryContextSnafu)?;
        ctx.set_channel(Channel::Otlp);
        let ctx = Arc::new(ctx);

        let _ = self.handler.traces(req, table_name, ctx).await?;

        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}

#[async_trait::async_trait]
impl MetricsService for OtlpService {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> StdResult<Response<ExportMetricsServiceResponse>, Status> {
        let (_headers, extensions, req) = request.into_parts();

        let mut ctx = extensions
            .get::<QueryContext>()
            .cloned()
            .context(error::MissingQueryContextSnafu)?;
        ctx.set_channel(Channel::Otlp);
        let ctx = Arc::new(ctx);

        let _ = self.handler.metrics(req, ctx).await?;

        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
}
