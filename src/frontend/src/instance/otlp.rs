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
    OTLP_WRITE, PermissionChecker, PermissionCheckerRef, PermissionReq, PermissionTableTarget,
    PermissionTableTargets,
};
use client::Output;
use common_catalog::consts::{trace_operations_table_name, trace_services_table_name};
use common_error::ext::BoxedError;
use common_query::prelude::GREPTIME_PHYSICAL_TABLE;
use common_telemetry::{tracing, warn};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use otel_arrow_rust::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
use pipeline::{GreptimePipelineParams, PipelineWay};
use servers::error::{self, AuthSnafu, Result as ServerResult};
use servers::http::prom_store::PHYSICAL_TABLE_PARAM;
use servers::interceptor::{OpenTelemetryProtocolInterceptor, OpenTelemetryProtocolInterceptorRef};
use servers::metrics::METRIC_ROW_BATCH_ELIGIBILITY_FALLBACKS;
use servers::otlp;
use servers::otlp::trace::span::TraceSpanGroup;
use servers::pending_rows_batcher::{
    MetricRecordBatchSubmission, MetricRowBatchProtocol, MetricRowBatchSubmission,
    MetricRowBatcherRef, is_scalar_metric_batchable,
};
use servers::query_handler::{
    MetricsIngestOutcome, OpenTelemetryProtocolHandler, PipelineHandlerRef, TraceIngestOutcome,
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
use crate::metrics::{
    OTLP_LOGS_ROWS, OTLP_METRICS_INGEST_STAGE_ELAPSED, OTLP_METRICS_ROWS,
    OTLP_RESOURCE_INFO_WRITE_ERRORS,
};

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

fn metric_write_context(
    ctx: &QueryContextRef,
    semantic_index: &otlp::metrics::SemanticIndex,
    is_legacy: bool,
) -> QueryContextRef {
    let mut ctx = ctx.as_ref().clone();
    ctx.set_extension(SEMANTIC_SIGNAL_TYPE, SIGNAL_TYPE_METRIC);
    ctx.set_extension(SEMANTIC_SOURCE, SOURCE_OPENTELEMETRY);
    if let Some(index) = semantic_index.encode(&ctx.current_schema()) {
        ctx.set_extension(SEMANTIC_PER_TABLE_INDEX_KEY, index);
    }
    if !is_legacy {
        ctx.set_extension(OTLP_METRIC_COMPAT_KEY, OTLP_METRIC_COMPAT_PROM.to_string());
    }
    Arc::new(ctx)
}

#[async_trait]
impl OpenTelemetryProtocolHandler for Instance {
    #[tracing::instrument(skip_all)]
    async fn metrics(
        &self,
        request: ExportMetricsServiceRequest,
        metric_row_batcher: Option<MetricRowBatcherRef>,
        ctx: QueryContextRef,
    ) -> ServerResult<MetricsIngestOutcome> {
        {
            let _timer = OTLP_METRICS_INGEST_STAGE_ELAPSED
                .with_label_values(&["request_permission"])
                .start_timer();
            self.plugins
                .get::<PermissionCheckerRef>()
                .as_ref()
                .check_permission(ctx.current_user(), PermissionReq::Action(OTLP_WRITE))
                .context(AuthSnafu)?;
        }

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
        let is_legacy = {
            let _timer = OTLP_METRICS_INGEST_STAGE_ELAPSED
                .with_label_values(&["legacy_check"])
                .start_timer();
            self.check_otlp_legacy(&input_names, &ctx).await?
        };

        let mut metric_ctx = ctx
            .protocol_ctx()
            .get_otlp_metric_ctx()
            .cloned()
            .unwrap_or_default();
        metric_ctx.is_legacy = is_legacy;
        metric_ctx.resource_info = self.otlp_resource_info;

        let mut batching_candidate =
            !metric_ctx.is_legacy && metric_ctx.with_metric_engine && metric_row_batcher.is_some();
        if batching_candidate {
            let direct_conversion = {
                let _timer = OTLP_METRICS_INGEST_STAGE_ELAPSED
                    .with_label_values(&["direct_conversion"])
                    .start_timer();
                otlp::metrics::try_to_scalar_record_batches(&request, &metric_ctx)?
            };
            if let Some(otlp::metrics::ScalarMetricsConversion {
                batches,
                permission_tables,
                rows,
                semantic_index,
                mut outcome,
            }) = direct_conversion
            {
                if outcome.accepted_data_points == 0 {
                    return Ok(outcome);
                }
                let targets = PermissionTableTargets::resolved(
                    permission_tables
                        .into_iter()
                        .map(|table_name| {
                            PermissionTableTarget::new(
                                ctx.current_catalog(),
                                ctx.current_schema(),
                                table_name,
                            )
                        })
                        .collect(),
                );
                self.check_table_permission(&ctx, PermissionReq::Action(OTLP_WRITE), targets)
                    .context(AuthSnafu)?;
                let write_ctx = metric_write_context(&ctx, &semantic_index, is_legacy);
                let Some(metric_row_batcher) = metric_row_batcher.as_ref() else {
                    return Err(error::InternalSnafu {
                        err_msg: "OTLP batching candidate has no batcher".to_string(),
                    }
                    .build());
                };
                let submission = {
                    let _timer = OTLP_METRICS_INGEST_STAGE_ELAPSED
                        .with_label_values(&["batch_submit"])
                        .start_timer();
                    metric_row_batcher
                        .submit_record_batches_with_fallback(
                            batches,
                            write_ctx,
                            MetricRowBatchProtocol::Otlp,
                        )
                        .await?
                };
                if submission == MetricRecordBatchSubmission::Submitted {
                    self.cache_otlp_legacy(&input_names, &ctx, is_legacy)?;
                    OTLP_METRICS_ROWS.inc_by(rows as u64);
                    outcome.write_cost = 0;
                    return Ok(outcome);
                }
                METRIC_ROW_BATCH_ELIGIBILITY_FALLBACKS
                    .with_label_values(&[MetricRowBatchProtocol::Otlp.as_str()])
                    .inc();
                batching_candidate = false;
            }
        }

        let otlp::metrics::MetricsConversion {
            requests,
            rows,
            semantic_index,
            resource_info,
            mut outcome,
        } = {
            let _timer = OTLP_METRICS_INGEST_STAGE_ELAPSED
                .with_label_values(&["conversion"])
                .start_timer();
            otlp::metrics::to_grpc_insert_requests(request, &mut metric_ctx)?
        };
        if outcome.rejected_data_points > 0 {
            warn!(
                "Rejected {} OTLP metrics data points: {}",
                outcome.rejected_data_points,
                outcome.error_message.as_deref().unwrap_or_default()
            );
        }
        if outcome.accepted_data_points == 0 {
            return Ok(outcome);
        }

        {
            let _timer = OTLP_METRICS_INGEST_STAGE_ELAPSED
                .with_label_values(&["row_permission"])
                .start_timer();
            self.check_row_insert_permission(&requests, &ctx, PermissionReq::Action(OTLP_WRITE))
                .context(AuthSnafu)?;
        }
        self.cache_otlp_legacy(&input_names, &ctx, is_legacy)?;
        OTLP_METRICS_ROWS.inc_by(rows as u64);

        let ctx = metric_write_context(&ctx, &semantic_index, is_legacy);

        let scalar_batchable = if batching_candidate {
            let _timer = OTLP_METRICS_INGEST_STAGE_ELAPSED
                .with_label_values(&["batchability"])
                .start_timer();
            is_scalar_metric_batchable(&requests)?
        } else {
            false
        };
        let direct_requests = if let Some(metric_row_batcher) =
            metric_row_batcher.as_ref().filter(|_| scalar_batchable)
        {
            let submission = {
                let _timer = OTLP_METRICS_INGEST_STAGE_ELAPSED
                    .with_label_values(&["batch_submit"])
                    .start_timer();
                metric_row_batcher
                    .submit_with_fallback(requests, ctx.clone(), MetricRowBatchProtocol::Otlp)
                    .await?
            };
            match submission {
                MetricRowBatchSubmission::Submitted => None,
                MetricRowBatchSubmission::UnmatchedPhysicalTable(requests) => Some(requests),
            }
        } else {
            Some(requests)
        };
        let write_cost = if let Some(requests) = direct_requests {
            if batching_candidate {
                METRIC_ROW_BATCH_ELIGIBILITY_FALLBACKS
                    .with_label_values(&[MetricRowBatchProtocol::Otlp.as_str()])
                    .inc();
            }
            let _timer = OTLP_METRICS_INGEST_STAGE_ELAPSED
                .with_label_values(&["direct_write"])
                .start_timer();
            // OTLP tables have one sample field in both the legacy and physical paths.
            let output = if metric_ctx.is_legacy || !metric_ctx.with_metric_engine {
                self.handle_row_inserts(requests, ctx.clone(), false, true)
                    .await
                    .map_err(BoxedError::new)
                    .context(error::ExecuteGrpcQuerySnafu)
            } else {
                let physical_table = ctx
                    .extension(PHYSICAL_TABLE_PARAM)
                    .unwrap_or(GREPTIME_PHYSICAL_TABLE)
                    .to_string();
                self.handle_metric_row_inserts(requests, ctx.clone(), physical_table)
                    .await
                    .map_err(BoxedError::new)
                    .context(error::ExecuteGrpcQuerySnafu)
            }?;
            output.meta.cost
        } else {
            0
        };
        outcome.write_cost = write_cost;

        // Derived enrichment, written after the metric data is committed:
        // failing here would make the client retry data the server already
        // accepted, so every failure degrades to a warning instead.
        if let Some(resource_info) = resource_info {
            let written = match self.check_row_insert_permission(
                &resource_info,
                &ctx,
                PermissionReq::Action(OTLP_WRITE),
            ) {
                Ok(_) => self
                    .handle_row_inserts(resource_info, ctx, false, false)
                    .await
                    .map_err(BoxedError::new)
                    .map_err(|e| e.to_string()),
                Err(e) => Err(e.to_string()),
            };
            match written {
                Ok(descriptor_output) => outcome.write_cost += descriptor_output.meta.cost,
                Err(e) => {
                    OTLP_RESOURCE_INFO_WRITE_ERRORS.inc();
                    warn!("Failed to write the OTLP resource descriptor table: {e}");
                    outcome.error_message.get_or_insert(format!(
                        "metric data was accepted, but writing the resource \
                         descriptor table `{}` failed: {e}",
                        otlp::metrics::OTEL_RESOURCE_INFO_TABLE_NAME
                    ));
                }
            }
        }

        Ok(outcome)
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
            .check_permission(ctx.current_user(), PermissionReq::Action(OTLP_WRITE))
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
        self.check_table_permission(&ctx, PermissionReq::Action(OTLP_WRITE), targets)
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
            .check_permission(ctx.current_user(), PermissionReq::Action(OTLP_WRITE))
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
            self.check_row_insert_permission(requests, temp_ctx, PermissionReq::Action(OTLP_WRITE))
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
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use api::v1::meta::Role;
    use api::v1::{RowInsertRequest, RowInsertRequests, Rows};
    use async_trait::async_trait;
    use auth::{
        PermissionChecker, PermissionCheckerRef, PermissionReq, PermissionResp,
        PermissionTableTarget, PermissionTableTargets, UserInfoRef,
    };
    use common_base::Plugins;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_error::ext::ErrorExt;
    use meta_client::client::MetaClientBuilder;
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
    use otel_arrow_rust::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
    use otel_arrow_rust::proto::opentelemetry::common::v1::{
        AnyValue as MetricAnyValue, KeyValue as MetricKeyValue, any_value as metric_any_value,
    };
    use otel_arrow_rust::proto::opentelemetry::metrics::v1::number_data_point::Value;
    use otel_arrow_rust::proto::opentelemetry::metrics::v1::{
        AggregationTemporality, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Metric,
        NumberDataPoint, ResourceMetrics, ScopeMetrics, metric,
    };
    use otel_arrow_rust::proto::opentelemetry::resource::v1::Resource as MetricResource;
    use servers::metrics::METRIC_ROW_BATCH_ELIGIBILITY_FALLBACKS;
    use servers::pending_rows_batcher::{
        MetricRecordBatch, MetricRecordBatchSubmission, MetricRowBatchProtocol, MetricRowBatcher,
    };
    use session::context::QueryContext;
    use session::protocol_ctx::{OtlpMetricCtx, ProtocolCtx};

    use super::*;
    use crate::frontend::FrontendOptions;
    use crate::instance::builder::FrontendBuilder;

    static FALLBACK_METRIC_ASSERTION_LOCK: tokio::sync::Mutex<()> =
        tokio::sync::Mutex::const_new(());

    #[derive(Default)]
    struct FakeMetricRowBatcher {
        requests: Mutex<Vec<RowInsertRequests>>,
        record_batch_tables: Mutex<Vec<Vec<String>>>,
        protocols: Mutex<Vec<MetricRowBatchProtocol>>,
        flush_error: Option<&'static str>,
        accepted_rows: u64,
        reject_placement: bool,
    }

    impl FakeMetricRowBatcher {
        fn successful(accepted_rows: u64) -> Self {
            Self {
                accepted_rows,
                ..Default::default()
            }
        }

        fn failing(message: &'static str) -> Self {
            Self {
                flush_error: Some(message),
                ..Default::default()
            }
        }

        fn rejecting_placement() -> Self {
            Self {
                reject_placement: true,
                ..Default::default()
            }
        }
    }

    #[async_trait]
    impl MetricRowBatcher for FakeMetricRowBatcher {
        async fn submit(
            &self,
            requests: RowInsertRequests,
            _ctx: QueryContextRef,
            protocol: MetricRowBatchProtocol,
        ) -> ServerResult<u64> {
            self.requests.lock().unwrap().push(requests);
            self.protocols.lock().unwrap().push(protocol);
            if let Some(message) = self.flush_error {
                return Err(servers::error::InternalSnafu {
                    err_msg: message.to_string(),
                }
                .build());
            }
            Ok(self.accepted_rows)
        }

        async fn submit_with_fallback(
            &self,
            requests: RowInsertRequests,
            ctx: QueryContextRef,
            protocol: MetricRowBatchProtocol,
        ) -> ServerResult<MetricRowBatchSubmission> {
            if self.reject_placement {
                self.requests.lock().unwrap().push(requests.clone());
                self.protocols.lock().unwrap().push(protocol);
                return Ok(MetricRowBatchSubmission::UnmatchedPhysicalTable(requests));
            }
            self.submit(requests, ctx, protocol)
                .await
                .map(|_| MetricRowBatchSubmission::Submitted)
        }

        async fn submit_record_batches_with_fallback(
            &self,
            batches: Vec<MetricRecordBatch>,
            _ctx: QueryContextRef,
            protocol: MetricRowBatchProtocol,
        ) -> ServerResult<MetricRecordBatchSubmission> {
            self.record_batch_tables.lock().unwrap().push(
                batches
                    .iter()
                    .map(|batch| batch.table_name().to_string())
                    .collect(),
            );
            self.protocols.lock().unwrap().push(protocol);
            if self.reject_placement {
                return Ok(MetricRecordBatchSubmission::UnmatchedPhysicalTable);
            }
            if let Some(message) = self.flush_error {
                return Err(servers::error::InternalSnafu {
                    err_msg: message.to_string(),
                }
                .build());
            }
            Ok(MetricRecordBatchSubmission::Submitted)
        }
    }

    async fn test_instance() -> Instance {
        test_instance_with(FrontendOptions::default(), Plugins::new()).await
    }

    async fn test_instance_with(options: FrontendOptions, plugins: Plugins) -> Instance {
        let meta_client = Arc::new(
            MetaClientBuilder::new(0, Role::Frontend)
                .enable_procedure()
                .build(),
        );
        FrontendBuilder::new_test(&options, meta_client)
            .with_plugin(plugins)
            .try_build()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_all_empty_metric_insert_is_noop_without_physical_table() {
        let instance = test_instance().await;
        let ctx = metric_query_ctx(true);
        let physical_table = "empty_metric_physical";
        let requests = RowInsertRequests {
            inserts: vec![
                RowInsertRequest {
                    table_name: "missing_rows".to_string(),
                    rows: None,
                },
                RowInsertRequest {
                    table_name: "empty_rows".to_string(),
                    rows: Some(Rows::default()),
                },
            ],
        };

        let output = instance
            .handle_metric_row_inserts(requests, ctx.clone(), physical_table.to_string())
            .await
            .unwrap();

        assert!(matches!(output.data, client::OutputData::AffectedRows(0)));
        assert!(output.meta.plan.is_none());
        assert!(
            instance
                .catalog_manager()
                .table(
                    DEFAULT_CATALOG_NAME,
                    DEFAULT_SCHEMA_NAME,
                    physical_table,
                    Some(ctx.as_ref()),
                )
                .await
                .unwrap()
                .is_none()
        );
    }

    #[derive(Default)]
    struct CountingPermissionChecker {
        checks: AtomicUsize,
        targets: Mutex<Vec<PermissionTableTargets>>,
    }

    impl PermissionChecker for CountingPermissionChecker {
        fn check_permission(
            &self,
            _user_info: UserInfoRef,
            _req: PermissionReq,
        ) -> auth::error::Result<PermissionResp> {
            self.checks.fetch_add(1, Ordering::Relaxed);
            Ok(PermissionResp::Allow)
        }

        fn check_permission_with_table_targets(
            &self,
            _user_info: UserInfoRef,
            _req: PermissionReq,
            targets: PermissionTableTargets,
        ) -> auth::error::Result<PermissionResp> {
            self.checks.fetch_add(1, Ordering::Relaxed);
            self.targets.lock().unwrap().push(targets);
            Ok(PermissionResp::Allow)
        }
    }

    fn metric_query_ctx(with_metric_engine: bool) -> QueryContextRef {
        let mut ctx = QueryContext::with(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME);
        ctx.set_protocol_ctx(ProtocolCtx::OtlpMetric(OtlpMetricCtx {
            with_metric_engine,
            experimental_enable_exponential_histogram: true,
            ..Default::default()
        }));
        Arc::new(ctx)
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

    fn gauge_metric(metric_name: &str) -> Metric {
        Metric {
            name: metric_name.to_string(),
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    time_unix_nano: 1_000_000,
                    value: Some(Value::AsDouble(1.0)),
                    ..Default::default()
                }],
            })),
            ..Default::default()
        }
    }

    fn gauge_request(metric_name: &str) -> ExportMetricsServiceRequest {
        metrics_request(vec![gauge_metric(metric_name)])
    }

    fn native_histogram_metric(metric_name: &str) -> Metric {
        Metric {
            name: metric_name.to_string(),
            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    time_unix_nano: 1_000_000,
                    count: 1,
                    sum: Some(1.0),
                    zero_count: 1,
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
            })),
            ..Default::default()
        }
    }

    async fn assert_metrics_falls_back(
        instance: &Instance,
        request: ExportMetricsServiceRequest,
        ctx: QueryContextRef,
    ) {
        let batcher = Arc::new(FakeMetricRowBatcher::default());

        let error = instance
            .metrics(request, Some(batcher.clone()), ctx)
            .await
            .unwrap_err();

        let servers::error::Error::ExecuteGrpcQuery { source, .. } = error else {
            panic!("expected ExecuteGrpcQuery from Inserter, got {error:?}");
        };
        let Some(crate::error::Error::TableOperation { source, .. }) =
            source.as_any().downcast_ref::<crate::error::Error>()
        else {
            panic!("expected frontend TableOperation from Inserter, got {source:?}");
        };
        assert!(
            matches!(
                source,
                operator::error::Error::SchemaNotFound { schema_info, .. }
                    if schema_info == DEFAULT_SCHEMA_NAME
            ),
            "expected test-environment schema lookup failure, got {source:?}"
        );
        assert!(batcher.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_metrics_routes_eligible_request_to_batcher() {
        let instance = test_instance().await;
        let batcher = Arc::new(FakeMetricRowBatcher::successful(7));

        let outcome = instance
            .metrics(
                gauge_request("temperature"),
                Some(batcher.clone()),
                metric_query_ctx(true),
            )
            .await
            .unwrap();

        assert_eq!(1, outcome.accepted_data_points);
        assert_eq!(0, outcome.rejected_data_points);
        assert_eq!(0, outcome.write_cost);
        assert!(batcher.requests.lock().unwrap().is_empty());
        assert_eq!(
            vec![vec!["temperature".to_string()]],
            *batcher.record_batch_tables.lock().unwrap()
        );
        assert_eq!(
            vec![MetricRowBatchProtocol::Otlp],
            *batcher.protocols.lock().unwrap()
        );
    }

    #[tokio::test]
    async fn test_direct_metrics_permission_includes_empty_metric_table() {
        let checker = Arc::new(CountingPermissionChecker::default());
        let plugins = Plugins::new();
        plugins.insert::<PermissionCheckerRef>(checker.clone());
        let instance = test_instance_with(FrontendOptions::default(), plugins).await;
        let batcher = Arc::new(FakeMetricRowBatcher::successful(1));
        let mut empty = gauge_metric("empty_temperature");
        let Some(metric::Data::Gauge(gauge)) = empty.data.as_mut() else {
            panic!("expected gauge")
        };
        gauge.data_points.clear();

        instance
            .metrics(
                metrics_request(vec![gauge_metric("temperature"), empty]),
                Some(batcher),
                metric_query_ctx(true),
            )
            .await
            .unwrap();

        let targets = checker.targets.lock().unwrap();
        let PermissionTableTargets::Resolved(targets) = &targets[0] else {
            panic!("expected resolved table targets")
        };
        let table_names = targets
            .iter()
            .map(|target| target.table.as_str())
            .collect::<HashSet<_>>();
        assert_eq!(
            HashSet::from(["temperature", "empty_temperature"]),
            table_names
        );
    }

    #[tokio::test]
    async fn test_metrics_falls_back_when_batcher_rejects_placement() {
        let instance = test_instance().await;
        let batcher = Arc::new(FakeMetricRowBatcher::rejecting_placement());

        let error = instance
            .metrics(
                gauge_request("temperature"),
                Some(batcher.clone()),
                metric_query_ctx(true),
            )
            .await
            .unwrap_err();

        let servers::error::Error::ExecuteGrpcQuery { source, .. } = error else {
            panic!("expected ExecuteGrpcQuery from Inserter, got {error:?}");
        };
        assert!(
            source
                .as_any()
                .downcast_ref::<crate::error::Error>()
                .is_some_and(|source| matches!(source, crate::error::Error::TableOperation { .. })),
            "expected frontend TableOperation from Inserter, got {source:?}"
        );
        assert!(batcher.requests.lock().unwrap().is_empty());
        assert_eq!(1, batcher.record_batch_tables.lock().unwrap().len());
        assert_eq!(
            vec![MetricRowBatchProtocol::Otlp],
            *batcher.protocols.lock().unwrap()
        );
    }

    #[tokio::test]
    async fn test_metrics_legacy_mode_falls_back_to_inserter() {
        let instance = test_instance().await;
        let ctx = metric_query_ctx(true);
        instance
            .otlp_metrics_table_legacy_cache
            .entry(ctx.get_db_string())
            .or_default()
            .insert("temperature".to_string(), true);

        assert_metrics_falls_back(&instance, gauge_request("temperature"), ctx).await;
    }

    #[tokio::test]
    async fn test_metrics_without_metric_engine_falls_back_to_inserter() {
        let instance = test_instance().await;

        assert_metrics_falls_back(
            &instance,
            gauge_request("temperature"),
            metric_query_ctx(false),
        )
        .await;
    }

    #[tokio::test]
    async fn test_metrics_native_histogram_falls_back_to_inserter() {
        let _guard = FALLBACK_METRIC_ASSERTION_LOCK.lock().await;
        let instance = test_instance().await;
        let fallback = METRIC_ROW_BATCH_ELIGIBILITY_FALLBACKS
            .with_label_values(&[MetricRowBatchProtocol::Otlp.as_str()]);
        let initial = fallback.get();

        assert_metrics_falls_back(
            &instance,
            metrics_request(vec![native_histogram_metric("latency")]),
            metric_query_ctx(true),
        )
        .await;
        assert_eq!(initial + 1, fallback.get());
    }

    #[tokio::test]
    async fn test_metrics_mixed_request_falls_back_to_inserter() {
        let _guard = FALLBACK_METRIC_ASSERTION_LOCK.lock().await;
        let instance = test_instance().await;

        assert_metrics_falls_back(
            &instance,
            metrics_request(vec![
                gauge_metric("temperature"),
                native_histogram_metric("latency"),
            ]),
            metric_query_ctx(true),
        )
        .await;
    }

    #[tokio::test]
    async fn test_metrics_batcher_error_skips_resource_info_insert() {
        let checker = Arc::new(CountingPermissionChecker::default());
        let plugins = Plugins::new();
        plugins.insert::<PermissionCheckerRef>(checker.clone());
        let mut options = FrontendOptions::default();
        options.otlp.experimental_enable_resource_info = true;
        let instance = test_instance_with(options, plugins).await;
        let batcher = Arc::new(FakeMetricRowBatcher::failing("flush failed"));
        let mut request = gauge_request("temperature");
        request.resource_metrics[0].resource = Some(MetricResource {
            attributes: vec![MetricKeyValue {
                key: "service.name".to_string(),
                value: Some(MetricAnyValue {
                    value: Some(metric_any_value::Value::StringValue("frontend".to_string())),
                }),
            }],
            ..Default::default()
        });

        let error = instance
            .metrics(request, Some(batcher.clone()), metric_query_ctx(true))
            .await
            .unwrap_err();

        assert!(error.to_string().contains("flush failed"), "{error:?}");
        assert_eq!(1, batcher.requests.lock().unwrap().len());
        assert_eq!(2, checker.checks.load(Ordering::Relaxed));
    }

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
                        ..Default::default()
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
