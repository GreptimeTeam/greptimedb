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
use client::Output;
use common_error::ext::BoxedError;
use common_telemetry::tracing;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use pipeline::PipelineWay;
use servers::error::{self, AuthSnafu, Result as ServerResult};
use servers::interceptor::{OpenTelemetryProtocolInterceptor, OpenTelemetryProtocolInterceptorRef};
use servers::otlp;
use servers::otlp::plugin::TraceParserRef;
use servers::query_handler::OpenTelemetryProtocolHandler;
use session::context::QueryContextRef;
use snafu::ResultExt;

use crate::instance::Instance;
use crate::metrics::{OTLP_LOGS_ROWS, OTLP_METRICS_ROWS, OTLP_TRACES_ROWS};

#[async_trait]
impl OpenTelemetryProtocolHandler for Instance {
    #[tracing::instrument(skip_all)]
    async fn metrics(
        &self,
        request: ExportMetricsServiceRequest,
        ctx: QueryContextRef,
    ) -> ServerResult<Output> {
        self.plugins
            .get::<PermissionCheckerRef>()
            .as_ref()
            .check_permission(ctx.current_user(), PermissionReq::Otlp)
            .context(AuthSnafu)?;

        let interceptor_ref = self
            .plugins
            .get::<OpenTelemetryProtocolInterceptorRef<servers::error::Error>>();
        interceptor_ref.pre_execute(ctx.clone())?;

        let (requests, rows) = otlp::metrics::to_grpc_insert_requests(request)?;
        OTLP_METRICS_ROWS.inc_by(rows as u64);

        self.handle_row_inserts(requests, ctx)
            .await
            .map_err(BoxedError::new)
            .context(error::ExecuteGrpcQuerySnafu)
    }

    #[tracing::instrument(skip_all)]
    async fn traces(
        &self,
        request: ExportTraceServiceRequest,
        ctx: QueryContextRef,
    ) -> ServerResult<Output> {
        self.plugins
            .get::<PermissionCheckerRef>()
            .as_ref()
            .check_permission(ctx.current_user(), PermissionReq::Otlp)
            .context(AuthSnafu)?;

        let interceptor_ref = self
            .plugins
            .get::<OpenTelemetryProtocolInterceptorRef<servers::error::Error>>();
        interceptor_ref.pre_execute(ctx.clone())?;

        let (table_name, spans) = match self.plugins.get::<TraceParserRef>() {
            Some(parser) => (parser.table_name(), parser.parse(request)),
            None => (
                otlp::trace::TRACE_TABLE_NAME.to_string(),
                otlp::trace::parse(request),
            ),
        };

        let (requests, rows) = otlp::trace::to_grpc_insert_requests(table_name, spans)?;

        OTLP_TRACES_ROWS.inc_by(rows as u64);

        self.handle_log_inserts(requests, ctx)
            .await
            .map_err(BoxedError::new)
            .context(error::ExecuteGrpcQuerySnafu)
    }

    #[tracing::instrument(skip_all)]
    async fn logs(
        &self,
        request: ExportLogsServiceRequest,
        pipeline: PipelineWay,
        table_name: String,
        ctx: QueryContextRef,
    ) -> ServerResult<Output> {
        self.plugins
            .get::<PermissionCheckerRef>()
            .as_ref()
            .check_permission(ctx.current_user(), PermissionReq::Otlp)
            .context(AuthSnafu)?;

        let interceptor_ref = self
            .plugins
            .get::<OpenTelemetryProtocolInterceptorRef<servers::error::Error>>();
        interceptor_ref.pre_execute(ctx.clone())?;
        let (requests, rows) = otlp::logs::to_grpc_insert_requests(request, pipeline, table_name)?;

        self.handle_row_inserts(requests, ctx)
            .await
            .inspect(|_| OTLP_LOGS_ROWS.inc_by(rows as u64))
            .map_err(BoxedError::new)
            .context(error::ExecuteGrpcQuerySnafu)
    }
}
