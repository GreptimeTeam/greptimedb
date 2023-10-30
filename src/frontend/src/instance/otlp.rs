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

use async_trait::async_trait;
use auth::{PermissionChecker, PermissionCheckerRef, PermissionReq};
use common_error::ext::BoxedError;
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use servers::error::{self, AuthSnafu, Result as ServerResult};
use servers::otlp;
use servers::otlp::plugin::TraceParserRef;
use servers::query_handler::OpenTelemetryProtocolHandler;
use session::context::QueryContextRef;
use snafu::ResultExt;

use crate::instance::Instance;
use crate::metrics::{OTLP_METRICS_ROWS, OTLP_TRACES_ROWS};

#[async_trait]
impl OpenTelemetryProtocolHandler for Instance {
    async fn metrics(
        &self,
        request: ExportMetricsServiceRequest,
        ctx: QueryContextRef,
    ) -> ServerResult<ExportMetricsServiceResponse> {
        self.plugins
            .get::<PermissionCheckerRef>()
            .as_ref()
            .check_permission(ctx.current_user(), PermissionReq::Otlp)
            .context(AuthSnafu)?;
        let (requests, rows) = otlp::metrics::to_grpc_insert_requests(request)?;
        let _ = self
            .handle_row_inserts(requests, ctx)
            .await
            .map_err(BoxedError::new)
            .context(error::ExecuteGrpcQuerySnafu)?;

        OTLP_METRICS_ROWS.inc_by(rows as u64);

        let resp = ExportMetricsServiceResponse {
            // TODO(sunng87): add support for partial_success in future patch
            partial_success: None,
        };
        Ok(resp)
    }

    async fn traces(
        &self,
        request: ExportTraceServiceRequest,
        ctx: QueryContextRef,
    ) -> ServerResult<ExportTraceServiceResponse> {
        self.plugins
            .get::<PermissionCheckerRef>()
            .as_ref()
            .check_permission(ctx.current_user(), PermissionReq::Otlp)
            .context(AuthSnafu)?;

        let (table_name, spans) = match self.plugins.get::<TraceParserRef>() {
            Some(parser) => (parser.table_name(), parser.parse(request)),
            None => (
                otlp::trace::TRACE_TABLE_NAME.to_string(),
                otlp::trace::parse(request),
            ),
        };

        let (requests, rows) = otlp::trace::to_grpc_insert_requests(table_name, spans)?;

        let _ = self
            .handle_row_inserts(requests, ctx)
            .await
            .map_err(BoxedError::new)
            .context(error::ExecuteGrpcQuerySnafu)?;

        OTLP_TRACES_ROWS.inc_by(rows as u64);

        let resp = ExportTraceServiceResponse {
            // TODO(fys): add support for partial_success in future patch
            partial_success: None,
        };
        Ok(resp)
    }
}
