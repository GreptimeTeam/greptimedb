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

mod trace_ingest;
pub mod trace_semconv;
pub mod trace_types;

use std::sync::Arc;

use async_trait::async_trait;
use auth::{
    PermissionChecker, PermissionCheckerRef, PermissionReq, PermissionTableTarget,
    PermissionTableTargets,
};
use client::Output;
use common_catalog::consts::{trace_operations_table_name, trace_services_table_name};
use common_error::ext::BoxedError;
use common_query::prelude::GREPTIME_PHYSICAL_TABLE;
use common_telemetry::tracing;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use otel_arrow_rust::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
use pipeline::{GreptimePipelineParams, PipelineWay};
use servers::error::{self, AuthSnafu, Result as ServerResult};
use servers::http::prom_store::PHYSICAL_TABLE_PARAM;
use servers::interceptor::{OpenTelemetryProtocolInterceptor, OpenTelemetryProtocolInterceptorRef};
use servers::otlp;
use servers::otlp::trace::span::TraceSpanGroup;
use servers::query_handler::{
    OpenTelemetryProtocolHandler, PipelineHandlerRef, TraceIngestOutcome,
};
use session::context::QueryContextRef;
use snafu::ResultExt;
use table::requests::{
    OTLP_METRIC_COMPAT_KEY, OTLP_METRIC_COMPAT_PROM, SEMANTIC_PER_TABLE_INDEX_KEY,
    SEMANTIC_SIGNAL_TYPE, SEMANTIC_SOURCE, SIGNAL_TYPE_LOG, SIGNAL_TYPE_METRIC,
    SOURCE_OPENTELEMETRY,
};

use self::trace_ingest::trace_conventions;
use crate::instance::Instance;
use crate::metrics::{OTLP_LOGS_ROWS, OTLP_METRICS_ROWS};

fn trace_permission_targets(
    table_name: &str,
    groups: &[TraceSpanGroup],
    ctx: &QueryContextRef,
) -> PermissionTableTargets {
    let catalog = ctx.current_catalog();
    let schema = ctx.current_schema();
    if catalog.is_empty() || schema.is_empty() || table_name.is_empty() {
        return PermissionTableTargets::Unresolved;
    }

    let has_spans = groups.iter().any(|group| !group.spans.is_empty());
    if !has_spans {
        return PermissionTableTargets::resolved(Vec::new());
    }

    let mut targets = vec![PermissionTableTarget::new(catalog, &schema, table_name)];
    if groups
        .iter()
        .flat_map(|group| &group.spans)
        .any(|span| span.service_name.is_some())
    {
        targets.extend([
            PermissionTableTarget::new(catalog, &schema, trace_services_table_name(table_name)),
            PermissionTableTarget::new(catalog, &schema, trace_operations_table_name(table_name)),
        ]);
    }

    PermissionTableTargets::resolved(targets)
}

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
        let ctx = Arc::new(ctx.fork());

        let input_names = request
            .resource_metrics
            .iter()
            .flat_map(|r| r.scope_metrics.iter())
            .flat_map(|s| s.metrics.iter().map(|m| m.name.clone()))
            .collect::<Vec<_>>();

        // See [`OtlpMetricCtx`] for details
        let is_legacy = self.check_otlp_legacy(&input_names, &ctx).await?;

        let mut metric_ctx = ctx
            .protocol_ctx()
            .get_otlp_metric_ctx()
            .cloned()
            .unwrap_or_default();
        metric_ctx.is_legacy = is_legacy;

        let (requests, rows, semantic_index) =
            otlp::metrics::to_grpc_insert_requests(request, &mut metric_ctx)?;
        self.check_row_insert_permission(&requests, &ctx, PermissionReq::Otlp)
            .context(AuthSnafu)?;
        self.cache_otlp_legacy(&input_names, &ctx, is_legacy)?;
        OTLP_METRICS_ROWS.inc_by(rows as u64);

        let ctx = {
            let mut c = (*ctx).clone();
            c.set_extension(SEMANTIC_SIGNAL_TYPE, SIGNAL_TYPE_METRIC);
            c.set_extension(SEMANTIC_SOURCE, SOURCE_OPENTELEMETRY);
            // Per-table metric specifics + resource/scope lineage ride this
            // internal channel; the auto-create path folds them per table name.
            if let Some(index) = semantic_index.encode() {
                c.set_extension(SEMANTIC_PER_TABLE_INDEX_KEY, index);
            }
            if !is_legacy {
                c.set_extension(OTLP_METRIC_COMPAT_KEY, OTLP_METRIC_COMPAT_PROM.to_string());
            }
            Arc::new(c)
        };

        // If the user uses the legacy path, it is by default without metric engine.
        if metric_ctx.is_legacy || !metric_ctx.with_metric_engine {
            self.handle_row_inserts(requests, ctx, false, false)
                .await
                .map_err(BoxedError::new)
                .context(error::ExecuteGrpcQuerySnafu)
        } else {
            let physical_table = ctx
                .extension(PHYSICAL_TABLE_PARAM)
                .unwrap_or(GREPTIME_PHYSICAL_TABLE)
                .to_string();
            self.handle_metric_row_inserts(requests, ctx, physical_table.clone())
                .await
                .map_err(BoxedError::new)
                .context(error::ExecuteGrpcQuerySnafu)
        }
    }

    #[tracing::instrument(skip_all)]
    async fn traces(
        &self,
        pipeline_handler: PipelineHandlerRef,
        request: ExportTraceServiceRequest,
        pipeline: PipelineWay,
        pipeline_params: GreptimePipelineParams,
        table_name: String,
        ctx: QueryContextRef,
    ) -> ServerResult<TraceIngestOutcome> {
        self.plugins
            .get::<PermissionCheckerRef>()
            .as_ref()
            .check_permission(ctx.current_user(), PermissionReq::Otlp)
            .context(AuthSnafu)?;

        let interceptor_ref = self
            .plugins
            .get::<OpenTelemetryProtocolInterceptorRef<servers::error::Error>>();
        interceptor_ref.pre_execute(ctx.clone())?;
        let ctx = Arc::new(ctx.fork());

        // `schema_url` is consumed by `parse`, so derive conventions first.
        let conventions = trace_conventions(&request);
        let spans = otlp::trace::span::parse(request);
        let targets = trace_permission_targets(&table_name, &spans, &ctx);
        self.check_table_permission(&ctx, PermissionReq::Otlp, targets)
            .context(AuthSnafu)?;
        self.ingest_trace_spans(
            pipeline_handler,
            &pipeline,
            &pipeline_params,
            table_name,
            spans,
            &conventions,
            ctx,
        )
        .await
    }

    #[tracing::instrument(skip_all)]
    async fn logs(
        &self,
        pipeline_handler: PipelineHandlerRef,
        request: ExportLogsServiceRequest,
        pipeline: PipelineWay,
        pipeline_params: GreptimePipelineParams,
        table_name: String,
        ctx: QueryContextRef,
    ) -> ServerResult<Vec<Output>> {
        self.plugins
            .get::<PermissionCheckerRef>()
            .as_ref()
            .check_permission(ctx.current_user(), PermissionReq::Otlp)
            .context(AuthSnafu)?;

        let interceptor_ref = self
            .plugins
            .get::<OpenTelemetryProtocolInterceptorRef<servers::error::Error>>();
        interceptor_ref.pre_execute(ctx.clone())?;
        let ctx = Arc::new(ctx.fork());

        // `as_req_iter` clones this ctx into each `temp_ctx`, so identity set here
        // reaches the context that drives table auto-create.
        let ctx = {
            let mut c = (*ctx).clone();
            c.set_extension(SEMANTIC_SIGNAL_TYPE, SIGNAL_TYPE_LOG);
            c.set_extension(SEMANTIC_SOURCE, SOURCE_OPENTELEMETRY);
            Arc::new(c)
        };

        let opt_req = otlp::logs::to_grpc_insert_requests(
            request,
            pipeline,
            pipeline_params,
            table_name,
            &ctx,
            pipeline_handler,
        )
        .await?;

        let batches = opt_req.as_req_iter(ctx).collect::<Vec<_>>();
        for (temp_ctx, requests) in &batches {
            self.check_row_insert_permission(requests, temp_ctx, PermissionReq::Otlp)
                .context(AuthSnafu)?;
        }

        let mut outputs = Vec::with_capacity(batches.len());
        for (temp_ctx, requests) in batches {
            let cnt = requests
                .inserts
                .iter()
                .filter_map(|r| r.rows.as_ref().map(|r| r.rows.len()))
                .sum::<usize>();

            let o = self
                .handle_log_inserts(requests, temp_ctx)
                .await
                .inspect(|_| OTLP_LOGS_ROWS.inc_by(cnt as u64))
                .map_err(BoxedError::new)
                .context(error::ExecuteGrpcQuerySnafu)?;
            outputs.push(o);
        }

        Ok(outputs)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use auth::{PermissionTableTarget, PermissionTableTargets};
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
    use session::context::QueryContext;

    use super::trace_permission_targets;

    #[test]
    fn test_trace_permission_targets() {
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("frontend".to_string())),
                        }),
                    }],
                    ..Default::default()
                }),
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span::default()],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let groups = servers::otlp::trace::span::parse(request);
        let ctx = Arc::new(QueryContext::with("greptime", "public"));

        assert_eq!(
            PermissionTableTargets::Resolved(vec![
                PermissionTableTarget::new("greptime", "public", "traces"),
                PermissionTableTarget::new("greptime", "public", "traces_services"),
                PermissionTableTarget::new("greptime", "public", "traces_operations"),
            ]),
            trace_permission_targets("traces", &groups, &ctx)
        );
        assert_eq!(
            PermissionTableTargets::Unresolved,
            trace_permission_targets("", &groups, &ctx)
        );
    }
}
