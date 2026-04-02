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

use std::sync::Arc;

use api::helper::ColumnDataTypeWrapper;
use api::v1::alter_table_expr::Kind;
use api::v1::{
    AlterTableExpr, ColumnDataType, ModifyColumnType, ModifyColumnTypes, RowInsertRequests,
};
use async_trait::async_trait;
use auth::{PermissionChecker, PermissionCheckerRef, PermissionReq};
use client::Output;
use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_query::prelude::GREPTIME_PHYSICAL_TABLE;
use common_telemetry::tracing;
use itertools::Itertools;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use otel_arrow_rust::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
use pipeline::{GreptimePipelineParams, PipelineWay};
use servers::error::{self, AuthSnafu, Result as ServerResult};
use servers::http::prom_store::PHYSICAL_TABLE_PARAM;
use servers::interceptor::{OpenTelemetryProtocolInterceptor, OpenTelemetryProtocolInterceptorRef};
use servers::otlp;
use servers::otlp::trace::TraceAuxData;
use servers::otlp::trace::coerce::{
    coerce_value_data, is_supported_trace_coercion, resolve_new_trace_column_type,
    trace_value_datatype,
};
use servers::otlp::trace::span::{TraceSpan, TraceSpanGroup};
use servers::query_handler::{
    OpenTelemetryProtocolHandler, PipelineHandlerRef, TraceIngestOutcome,
};
use session::context::QueryContextRef;
use snafu::ResultExt;
use table::requests::{OTLP_METRIC_COMPAT_KEY, OTLP_METRIC_COMPAT_PROM};

use crate::instance::Instance;
use crate::metrics::{
    OTLP_LOGS_ROWS, OTLP_METRICS_ROWS, OTLP_TRACES_FAILURE_COUNT, OTLP_TRACES_ROWS,
};

const TRACE_INGEST_CHUNK_SIZE: usize = 64;
const TRACE_FAILURE_MESSAGE_LIMIT: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChunkFailureReaction {
    RetryPerSpan,
    DiscardChunk,
    Propagate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TraceReconcileDecision {
    UseExisting(ColumnDataType),
    UseRequestLocal(ColumnDataType),
    AlterExistingTo(ColumnDataType),
}

impl TraceReconcileDecision {
    fn target_type(self) -> ColumnDataType {
        match self {
            Self::UseExisting(target_type)
            | Self::UseRequestLocal(target_type)
            | Self::AlterExistingTo(target_type) => target_type,
        }
    }

    fn requires_alter(self) -> bool {
        matches!(self, Self::AlterExistingTo(_))
    }
}

struct PendingTraceColumnRewrite {
    col_idx: usize,
    target_type: ColumnDataType,
    column_name: String,
}

impl ChunkFailureReaction {
    fn as_metric_label(self) -> &'static str {
        match self {
            Self::RetryPerSpan => "retry_per_span",
            Self::DiscardChunk => "discard_chunk",
            Self::Propagate => "propagate_failure",
        }
    }
}

struct TraceChunkIngestContext<'a> {
    pipeline_handler: PipelineHandlerRef,
    pipeline: &'a PipelineWay,
    pipeline_params: &'a GreptimePipelineParams,
    table_name: &'a str,
    is_trace_v1_model: bool,
}

struct TraceIngestState {
    aux_data: TraceAuxData,
    outcome: TraceIngestOutcome,
    failure_messages: Vec<String>,
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

        let input_names = request
            .resource_metrics
            .iter()
            .flat_map(|r| r.scope_metrics.iter())
            .flat_map(|s| s.metrics.iter().map(|m| &m.name))
            .collect::<Vec<_>>();

        // See [`OtlpMetricCtx`] for details
        let is_legacy = self.check_otlp_legacy(&input_names, ctx.clone()).await?;

        let mut metric_ctx = ctx
            .protocol_ctx()
            .get_otlp_metric_ctx()
            .cloned()
            .unwrap_or_default();
        metric_ctx.is_legacy = is_legacy;

        let (requests, rows) = otlp::metrics::to_grpc_insert_requests(request, &mut metric_ctx)?;
        OTLP_METRICS_ROWS.inc_by(rows as u64);

        let ctx = if !is_legacy {
            let mut c = (*ctx).clone();
            c.set_extension(OTLP_METRIC_COMPAT_KEY, OTLP_METRIC_COMPAT_PROM.to_string());
            Arc::new(c)
        } else {
            ctx
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

        let spans = otlp::trace::span::parse(request);
        self.ingest_trace_spans(
            pipeline_handler,
            &pipeline,
            &pipeline_params,
            table_name,
            spans,
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

        let opt_req = otlp::logs::to_grpc_insert_requests(
            request,
            pipeline,
            pipeline_params,
            table_name,
            &ctx,
            pipeline_handler,
        )
        .await?;

        let mut outputs = vec![];

        for (temp_ctx, requests) in opt_req.as_req_iter(ctx) {
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

impl Instance {
    /// Ingest OTLP trace spans with chunk-level writes and span-level fallback on
    /// deterministic chunk failures.
    async fn ingest_trace_spans(
        &self,
        pipeline_handler: PipelineHandlerRef,
        pipeline: &PipelineWay,
        pipeline_params: &GreptimePipelineParams,
        table_name: String,
        groups: Vec<TraceSpanGroup>,
        ctx: QueryContextRef,
    ) -> ServerResult<TraceIngestOutcome> {
        let is_trace_v1_model = matches!(pipeline, PipelineWay::OtlpTraceDirectV1);
        let ingest_ctx = TraceChunkIngestContext {
            pipeline_handler,
            pipeline,
            pipeline_params,
            table_name: &table_name,
            is_trace_v1_model,
        };
        let mut ingest_state = TraceIngestState {
            aux_data: TraceAuxData::default(),
            outcome: TraceIngestOutcome::default(),
            failure_messages: Vec::new(),
        };

        for group in groups {
            let chunks = group
                .spans
                .into_iter()
                .chunks(TRACE_INGEST_CHUNK_SIZE)
                .into_iter()
                .map(|chunk| chunk.collect::<Vec<_>>())
                .collect::<Vec<_>>();
            for chunk in chunks {
                self.ingest_trace_chunk(&ingest_ctx, chunk, ctx.clone(), &mut ingest_state)
                    .await?;
            }
        }

        OTLP_TRACES_ROWS.inc_by(ingest_state.outcome.accepted_spans as u64);

        if !ingest_state.aux_data.is_empty() {
            // Auxiliary trace tables are derived from spans whose main-table
            // writes are already confirmed, so they never create new accepted
            // spans and they do not affect rejected span counts.
            let (aux_requests, _) = otlp::trace::to_grpc_insert_requests_for_aux_tables(
                std::mem::take(&mut ingest_state.aux_data),
                ingest_ctx.pipeline,
                ingest_ctx.table_name,
            )?;

            if !aux_requests.inserts.is_empty() {
                match self
                    .insert_trace_requests(aux_requests, ingest_ctx.is_trace_v1_model, ctx)
                    .await
                {
                    Ok(output) => {
                        Self::add_trace_write_cost(&mut ingest_state.outcome, output.meta.cost);
                    }
                    Err(err) => {
                        Self::push_trace_failure_message(
                            &mut ingest_state.failure_messages,
                            "aux_table_update_failed",
                            format!(
                                "Auxiliary trace tables were not fully updated ({})",
                                err.status_code().as_ref()
                            ),
                        );
                    }
                }
            }
        }

        ingest_state.outcome.error_message = Self::finish_trace_failure_message(
            ingest_state.outcome.accepted_spans,
            ingest_state.outcome.rejected_spans,
            ingest_state.failure_messages,
        );

        Ok(ingest_state.outcome)
    }

    /// Ingest one owned trace chunk so successful spans can be moved into the
    /// accepted set without extra cloning.
    async fn ingest_trace_chunk(
        &self,
        ingest_ctx: &TraceChunkIngestContext<'_>,
        chunk: Vec<TraceSpan>,
        ctx: QueryContextRef,
        ingest_state: &mut TraceIngestState,
    ) -> ServerResult<()> {
        // Try the fast path first so healthy batches keep their original
        // throughput and write amplification stays low.
        let (requests, chunk_rows) = otlp::trace::to_grpc_insert_requests_from_spans(
            &chunk,
            ingest_ctx.pipeline,
            ingest_ctx.pipeline_params,
            ingest_ctx.table_name,
            &ctx,
            ingest_ctx.pipeline_handler.clone(),
        )?;

        match self
            .insert_trace_requests(requests, ingest_ctx.is_trace_v1_model, ctx.clone())
            .await
        {
            Ok(output) => {
                Self::add_trace_write_cost(&mut ingest_state.outcome, output.meta.cost);
                ingest_state.outcome.accepted_spans += chunk_rows;
                for span in &chunk {
                    ingest_state.aux_data.observe_span(span);
                }
            }
            Err(err) => match Self::classify_trace_chunk_failure(err.status_code()) {
                ChunkFailureReaction::RetryPerSpan => {
                    Self::push_trace_failure_message(
                        &mut ingest_state.failure_messages,
                        ChunkFailureReaction::RetryPerSpan.as_metric_label(),
                        format!("Chunk fallback triggered by {}", err.status_code().as_ref()),
                    );
                    // Only deterministic failures are retried span by span.
                    // This includes schemaless table or column creation paths for
                    // trace ingestion. Ambiguous failures are handled below
                    // without retrying because the chunk may already have been
                    // ingested.
                    self.ingest_trace_chunk_span_by_span(
                        ingest_ctx,
                        chunk,
                        ctx.clone(),
                        ingest_state,
                    )
                    .await?;
                }
                ChunkFailureReaction::DiscardChunk => {
                    ingest_state.outcome.rejected_spans += chunk.len();
                    Self::push_trace_failure_message(
                        &mut ingest_state.failure_messages,
                        ChunkFailureReaction::DiscardChunk.as_metric_label(),
                        format!(
                            "Discarded {} spans after ambiguous chunk failure ({})",
                            chunk.len(),
                            err.status_code().as_ref()
                        ),
                    );
                    // TODO(shuiyisong): Add an idempotent retry-safe recovery path for
                    // ambiguous chunk failures such as timeout-like errors.
                }
                // Retryable or ambiguous failures must fail the request instead of
                // becoming partial success. This path is not retry-safe because the
                // chunk may already have been committed before the error surfaced.
                ChunkFailureReaction::Propagate => {
                    Self::push_trace_failure_message(
                        &mut ingest_state.failure_messages,
                        ChunkFailureReaction::Propagate.as_metric_label(),
                        format!(
                            "Propagating retryable chunk failure ({})",
                            err.status_code().as_ref()
                        ),
                    );
                    return Err(err);
                }
            },
        }

        Ok(())
    }

    /// Retry spans one by one only after a deterministic chunk failure.
    async fn ingest_trace_chunk_span_by_span(
        &self,
        ingest_ctx: &TraceChunkIngestContext<'_>,
        chunk: Vec<TraceSpan>,
        ctx: QueryContextRef,
        ingest_state: &mut TraceIngestState,
    ) -> ServerResult<()> {
        for span in chunk {
            let (requests, rows) = otlp::trace::to_grpc_insert_requests_from_spans(
                std::slice::from_ref(&span),
                ingest_ctx.pipeline,
                ingest_ctx.pipeline_params,
                ingest_ctx.table_name,
                &ctx,
                ingest_ctx.pipeline_handler.clone(),
            )?;

            match self
                .insert_trace_requests(requests, ingest_ctx.is_trace_v1_model, ctx.clone())
                .await
            {
                Ok(output) => {
                    Self::add_trace_write_cost(&mut ingest_state.outcome, output.meta.cost);
                    ingest_state.outcome.accepted_spans += rows;
                    ingest_state.aux_data.observe_span(&span);
                }
                Err(err) => {
                    if Self::should_propagate_trace_span_failure(err.status_code()) {
                        Self::push_trace_failure_message(
                            &mut ingest_state.failure_messages,
                            ChunkFailureReaction::Propagate.as_metric_label(),
                            format!(
                                "Propagating retryable span failure for {}:{} ({})",
                                span.trace_id,
                                span.span_id,
                                err.status_code().as_ref()
                            ),
                        );
                        return Err(err);
                    }

                    ingest_state.outcome.rejected_spans += 1;
                    Self::push_trace_failure_message(
                        &mut ingest_state.failure_messages,
                        "span_rejected",
                        format!(
                            "Rejected span {}:{} ({})",
                            span.trace_id,
                            span.span_id,
                            err.status_code().as_ref()
                        ),
                    );
                }
            }
        }

        Ok(())
    }

    /// Reconcile and insert one trace request batch.
    async fn insert_trace_requests(
        &self,
        mut requests: RowInsertRequests,
        is_trace_v1_model: bool,
        ctx: QueryContextRef,
    ) -> ServerResult<Output> {
        if is_trace_v1_model {
            self.reconcile_trace_column_types(&mut requests, &ctx)
                .await?;
            self.handle_trace_inserts(requests, ctx)
                .await
                .map_err(BoxedError::new)
                .context(error::ExecuteGrpcQuerySnafu)
        } else {
            self.handle_log_inserts(requests, ctx)
                .await
                .map_err(BoxedError::new)
                .context(error::ExecuteGrpcQuerySnafu)
        }
    }

    fn classify_trace_chunk_failure(status: StatusCode) -> ChunkFailureReaction {
        match status {
            StatusCode::InvalidArguments
            | StatusCode::InvalidSyntax
            | StatusCode::Unsupported
            | StatusCode::TableNotFound
            | StatusCode::TableColumnNotFound => ChunkFailureReaction::RetryPerSpan,
            StatusCode::DatabaseNotFound => ChunkFailureReaction::DiscardChunk,
            StatusCode::Cancelled | StatusCode::DeadlineExceeded => ChunkFailureReaction::Propagate,
            _ if status.is_retryable() => ChunkFailureReaction::Propagate,
            _ => ChunkFailureReaction::DiscardChunk,
        }
    }

    fn should_propagate_trace_span_failure(status: StatusCode) -> bool {
        matches!(
            Self::classify_trace_chunk_failure(status),
            ChunkFailureReaction::Propagate
        )
    }

    fn add_trace_write_cost(outcome: &mut TraceIngestOutcome, cost: usize) {
        outcome.write_cost += cost;
    }

    fn push_trace_failure_message(messages: &mut Vec<String>, label: &str, message: String) {
        OTLP_TRACES_FAILURE_COUNT.with_label_values(&[label]).inc();

        if messages.len() < TRACE_FAILURE_MESSAGE_LIMIT {
            messages.push(message);
        } else if messages.len() == TRACE_FAILURE_MESSAGE_LIMIT {
            tracing::debug!(
                label,
                limit = TRACE_FAILURE_MESSAGE_LIMIT,
                "Trace ingest failure message limit reached; suppressing additional failure details"
            );
        }
    }

    fn finish_trace_failure_message(
        accepted_spans: usize,
        rejected_spans: usize,
        messages: Vec<String>,
    ) -> Option<String> {
        if rejected_spans == 0 && messages.is_empty() {
            return None;
        }

        let mut summary = format!(
            "Accepted {} spans, rejected {} spans",
            accepted_spans, rejected_spans
        );

        if !messages.is_empty() {
            summary.push_str(": ");
            summary.push_str(&messages.join("; "));
        }

        Some(summary)
    }

    /// Picks the reconciliation action for one trace column.
    ///
    /// Existing table schema is authoritative unless the only incompatible case is
    /// widening an existing Int64 column to Float64 for incoming Int64/Float64 data.
    fn choose_trace_reconcile_decision(
        observed_types: &[ColumnDataType],
        existing_type: Option<ColumnDataType>,
    ) -> ServerResult<Option<TraceReconcileDecision>> {
        let Some(existing_type) = existing_type else {
            return resolve_new_trace_column_type(observed_types.iter().copied())
                .map(|target_type| target_type.map(TraceReconcileDecision::UseRequestLocal))
                .map_err(|_| {
                    error::InvalidParameterSnafu {
                        reason: "unsupported trace type mix".to_string(),
                    }
                    .build()
                });
        };

        if observed_types.iter().copied().all(|request_type| {
            request_type == existing_type
                || is_supported_trace_coercion(request_type, existing_type)
        }) {
            return Ok(Some(TraceReconcileDecision::UseExisting(existing_type)));
        }

        if existing_type == ColumnDataType::Int64
            && observed_types.contains(&ColumnDataType::Float64)
            && observed_types.iter().all(|observed_type| {
                matches!(
                    observed_type,
                    ColumnDataType::Int64 | ColumnDataType::Float64
                )
            })
        {
            return Ok(Some(TraceReconcileDecision::AlterExistingTo(
                ColumnDataType::Float64,
            )));
        }

        error::InvalidParameterSnafu {
            reason: "unsupported trace type mix".to_string(),
        }
        .fail()
    }

    /// Widen existing trace table columns to Float64 before request rewrite.
    async fn alter_trace_table_columns_to_float64(
        &self,
        ctx: &QueryContextRef,
        table_name: &str,
        column_names: &[String],
    ) -> ServerResult<()> {
        let catalog_name = ctx.current_catalog().to_string();
        let schema_name = ctx.current_schema();
        let alter_expr = AlterTableExpr {
            catalog_name: catalog_name.clone(),
            schema_name: schema_name.clone(),
            table_name: table_name.to_string(),
            kind: Some(Kind::ModifyColumnTypes(ModifyColumnTypes {
                modify_column_types: column_names
                    .iter()
                    .map(|column_name| ModifyColumnType {
                        column_name: column_name.clone(),
                        target_type: ColumnDataType::Float64 as i32,
                        target_type_extension: None,
                    })
                    .collect(),
            })),
        };

        if let Err(err) = self
            .statement_executor
            .alter_table_inner(alter_expr, ctx.clone())
            .await
        {
            let table = self
                .catalog_manager
                .table(&catalog_name, &schema_name, table_name, None)
                .await
                .map_err(servers::error::Error::from)?;
            let alter_already_applied = table
                .map(|table| {
                    let table_schema = table.schema();
                    column_names.iter().all(|column_name| {
                        table_schema
                            .column_schema_by_name(column_name)
                            .and_then(|table_col| {
                                ColumnDataTypeWrapper::try_from(table_col.data_type.clone())
                                    .ok()
                                    .map(|wrapper| wrapper.datatype())
                            })
                            == Some(ColumnDataType::Float64)
                    })
                })
                .unwrap_or(false);

            if alter_already_applied {
                return Ok(());
            }

            tracing::warn!(
                table_name,
                columns = ?column_names,
                error = %err,
                "failed to widen trace columns before insert"
            );

            return error::InternalSnafu {
                err_msg: format!(
                    "failed to widen trace columns {:?} in table '{}' to Float64 after alter failure ({})",
                    column_names,
                    table_name,
                    err.status_code().as_ref()
                ),
            }
            .fail();
        }

        Ok(())
    }

    /// Coerce request column types and values to match the existing table schema
    /// for compatible type pairs. Existing table schema wins when present;
    /// otherwise the full request batch decides a stable target type.
    async fn reconcile_trace_column_types(
        &self,
        requests: &mut RowInsertRequests,
        ctx: &QueryContextRef,
    ) -> ServerResult<()> {
        let catalog = ctx.current_catalog();
        let schema = ctx.current_schema();

        for req in &mut requests.inserts {
            let table = self
                .catalog_manager
                .table(catalog, &schema, &req.table_name, None)
                .await?;

            let Some(rows) = req.rows.as_mut() else {
                continue;
            };

            let table_schema = table.map(|table| table.schema());
            let mut pending_rewrites = Vec::new();
            let mut pending_alter_columns = Vec::new();

            for (col_idx, col_schema) in rows.schema.iter().enumerate() {
                let Some(current_type) = ColumnDataType::try_from(col_schema.datatype).ok() else {
                    continue;
                };

                let mut observed_types = Vec::new();
                push_observed_trace_type(&mut observed_types, current_type);

                // Scan the full request first so the final type decision is not affected
                // by row order inside the batch.
                for row in &rows.rows {
                    let Some(value) = row
                        .values
                        .get(col_idx)
                        .and_then(|value| value.value_data.as_ref())
                    else {
                        continue;
                    };

                    let Some(value_type) = trace_value_datatype(value) else {
                        continue;
                    };
                    push_observed_trace_type(&mut observed_types, value_type);
                }

                let existing_type = table_schema
                    .as_ref()
                    .and_then(|schema| schema.column_schema_by_name(&col_schema.column_name))
                    .and_then(|table_col| {
                        ColumnDataTypeWrapper::try_from(table_col.data_type.clone())
                            .ok()
                            .map(|wrapper| wrapper.datatype())
                    });

                if !observed_types
                    .iter()
                    .copied()
                    .any(is_trace_reconcile_candidate_type)
                    && existing_type
                        .map(|datatype| !is_trace_reconcile_candidate_type(datatype))
                        .unwrap_or(true)
                {
                    continue;
                }

                // Decide the final type once per column, then rewrite all affected cells
                // together in one row pass below.
                let Some(decision) =
                    Self::choose_trace_reconcile_decision(&observed_types, existing_type).map_err(
                        |_| {
                            enrich_trace_reconcile_error(
                                &req.table_name,
                                &col_schema.column_name,
                                &observed_types,
                                existing_type,
                            )
                        },
                    )?
                else {
                    continue;
                };
                let target_type = decision.target_type();

                if !decision.requires_alter()
                    && observed_types
                        .iter()
                        .all(|observed| *observed == target_type)
                    && col_schema.datatype == target_type as i32
                {
                    continue;
                }

                if decision.requires_alter()
                    && !pending_alter_columns.contains(&col_schema.column_name)
                {
                    pending_alter_columns.push(col_schema.column_name.clone());
                }

                pending_rewrites.push(PendingTraceColumnRewrite {
                    col_idx,
                    target_type,
                    column_name: col_schema.column_name.clone(),
                });
            }

            if pending_rewrites.is_empty() {
                continue;
            }

            if !pending_alter_columns.is_empty() {
                self.alter_trace_table_columns_to_float64(
                    ctx,
                    &req.table_name,
                    &pending_alter_columns,
                )
                .await?;
            }

            // Update schema metadata before mutating row values so both stay in sync.
            for pending_rewrite in &pending_rewrites {
                rows.schema[pending_rewrite.col_idx].datatype = pending_rewrite.target_type as i32;
            }

            // Apply all pending column rewrites in one row pass.
            for row in &mut rows.rows {
                for pending_rewrite in &pending_rewrites {
                    let Some(value) = row.values.get_mut(pending_rewrite.col_idx) else {
                        continue;
                    };
                    let Some(request_type) =
                        value.value_data.as_ref().and_then(trace_value_datatype)
                    else {
                        continue;
                    };
                    if request_type == pending_rewrite.target_type {
                        continue;
                    }

                    value.value_data = coerce_value_data(
                        &value.value_data,
                        pending_rewrite.target_type,
                        request_type,
                    )
                    .map_err(|_| {
                        error::InvalidParameterSnafu {
                            reason: format!(
                                "failed to coerce trace column '{}' in table '{}' from {:?} to {:?}",
                                pending_rewrite.column_name,
                                req.table_name,
                                request_type,
                                pending_rewrite.target_type
                            ),
                        }
                        .build()
                    })?;
                }
            }
        }

        Ok(())
    }
}

fn enrich_trace_reconcile_error(
    table_name: &str,
    column_name: &str,
    observed_types: &[ColumnDataType],
    existing_type: Option<ColumnDataType>,
) -> servers::error::Error {
    let observed_types = observed_types
        .iter()
        .map(|datatype| format!("{datatype:?}"))
        .collect::<Vec<_>>()
        .join(", ");

    error::InvalidParameterSnafu {
        reason: match existing_type {
            Some(existing_type) => format!(
                "failed to reconcile trace column '{}' in table '{}' with observed types [{}] against existing {:?}",
                column_name, table_name, observed_types, existing_type
            ),
            None => format!(
                "failed to reconcile trace column '{}' in table '{}' with observed types [{}]",
                column_name, table_name, observed_types
            ),
        },
    }
    .build()
}

/// Only these trace scalar types participate in reconciliation. Other column kinds
/// such as JSON and binary keep their original write path and schema checks.
fn is_trace_reconcile_candidate_type(datatype: ColumnDataType) -> bool {
    matches!(
        datatype,
        ColumnDataType::String
            | ColumnDataType::Boolean
            | ColumnDataType::Int64
            | ColumnDataType::Float64
    )
}

/// Keeps the observed type list small without depending on enum ordering.
fn push_observed_trace_type(observed_types: &mut Vec<ColumnDataType>, datatype: ColumnDataType) {
    if !observed_types.contains(&datatype) {
        observed_types.push(datatype);
    }
}

#[cfg(test)]
mod tests {
    use api::v1::ColumnDataType;
    use common_error::ext::ErrorExt;
    use common_error::status_code::StatusCode;
    use servers::query_handler::TraceIngestOutcome;

    use super::{ChunkFailureReaction, Instance, TraceReconcileDecision};
    use crate::metrics::OTLP_TRACES_FAILURE_COUNT;

    #[test]
    fn test_classify_trace_chunk_failure() {
        assert_eq!(
            Instance::classify_trace_chunk_failure(StatusCode::InvalidArguments),
            ChunkFailureReaction::RetryPerSpan
        );
        assert_eq!(
            Instance::classify_trace_chunk_failure(StatusCode::InvalidSyntax),
            ChunkFailureReaction::RetryPerSpan
        );
        assert_eq!(
            Instance::classify_trace_chunk_failure(StatusCode::Unsupported),
            ChunkFailureReaction::RetryPerSpan
        );
        assert_eq!(
            Instance::classify_trace_chunk_failure(StatusCode::TableColumnNotFound),
            ChunkFailureReaction::RetryPerSpan
        );
        assert_eq!(
            Instance::classify_trace_chunk_failure(StatusCode::TableNotFound),
            ChunkFailureReaction::RetryPerSpan
        );
        assert_eq!(
            Instance::classify_trace_chunk_failure(StatusCode::DatabaseNotFound),
            ChunkFailureReaction::DiscardChunk
        );
        assert_eq!(
            Instance::classify_trace_chunk_failure(StatusCode::DeadlineExceeded),
            ChunkFailureReaction::Propagate
        );
        assert_eq!(
            Instance::classify_trace_chunk_failure(StatusCode::Cancelled),
            ChunkFailureReaction::Propagate
        );
        assert_eq!(
            Instance::classify_trace_chunk_failure(StatusCode::StorageUnavailable),
            ChunkFailureReaction::Propagate
        );
        assert_eq!(
            Instance::classify_trace_chunk_failure(StatusCode::Internal),
            ChunkFailureReaction::Propagate
        );
        assert_eq!(
            Instance::classify_trace_chunk_failure(StatusCode::RegionNotReady),
            ChunkFailureReaction::Propagate
        );
        assert_eq!(
            Instance::classify_trace_chunk_failure(StatusCode::TableUnavailable),
            ChunkFailureReaction::Propagate
        );
        assert_eq!(
            Instance::classify_trace_chunk_failure(StatusCode::RegionBusy),
            ChunkFailureReaction::Propagate
        );
        assert_eq!(
            Instance::classify_trace_chunk_failure(StatusCode::RuntimeResourcesExhausted),
            ChunkFailureReaction::Propagate
        );
    }

    #[test]
    fn test_classify_trace_span_failure() {
        assert!(Instance::should_propagate_trace_span_failure(
            StatusCode::DeadlineExceeded
        ));
        assert!(Instance::should_propagate_trace_span_failure(
            StatusCode::StorageUnavailable
        ));
        assert!(!Instance::should_propagate_trace_span_failure(
            StatusCode::InvalidArguments
        ));
    }

    #[test]
    fn test_add_trace_write_cost() {
        let mut outcome = TraceIngestOutcome::default();
        Instance::add_trace_write_cost(&mut outcome, 3);
        Instance::add_trace_write_cost(&mut outcome, 5);
        assert_eq!(outcome.write_cost, 8);
    }

    #[test]
    fn test_finish_trace_failure_message() {
        let message = Instance::finish_trace_failure_message(
            3,
            2,
            vec!["Rejected span trace:span (InvalidArguments)".to_string()],
        )
        .unwrap();
        assert!(message.contains("Accepted 3 spans, rejected 2 spans"));
        assert!(message.contains("Rejected span trace:span"));

        assert_eq!(Instance::finish_trace_failure_message(2, 0, vec![]), None);
    }

    #[test]
    fn test_finish_trace_failure_message_without_detail_messages() {
        assert_eq!(
            Instance::finish_trace_failure_message(0, 2, vec![]),
            Some("Accepted 0 spans, rejected 2 spans".to_string())
        );
    }

    #[test]
    fn test_push_trace_failure_message_increments_labeled_counter() {
        let label = "retry_per_span_counter_test";
        let initial = OTLP_TRACES_FAILURE_COUNT.with_label_values(&[label]).get();
        let mut messages = Vec::new();

        Instance::push_trace_failure_message(
            &mut messages,
            label,
            "Chunk fallback triggered by InvalidArguments".to_string(),
        );

        assert_eq!(messages.len(), 1);
        assert_eq!(
            OTLP_TRACES_FAILURE_COUNT.with_label_values(&[label]).get(),
            initial + 1
        );
    }

    #[test]
    fn test_push_trace_failure_message_caps_recorded_messages() {
        let label = "retry_per_span_limit_test";
        let mut messages = Vec::new();

        for idx in 0..=4 {
            Instance::push_trace_failure_message(&mut messages, label, format!("failure-{idx}"));
        }

        assert_eq!(messages.len(), 4);
        assert_eq!(
            messages,
            vec![
                "failure-0".to_string(),
                "failure-1".to_string(),
                "failure-2".to_string(),
                "failure-3".to_string()
            ]
        );
    }

    #[test]
    fn test_classify_trace_chunk_failure_defaults_to_discard() {
        assert_eq!(
            Instance::classify_trace_chunk_failure(StatusCode::Unknown),
            ChunkFailureReaction::DiscardChunk
        );
    }

    #[test]
    fn test_choose_trace_reconcile_decision_existing_int64_keeps_int64() {
        assert_eq!(
            Instance::choose_trace_reconcile_decision(
                &[ColumnDataType::Int64],
                Some(ColumnDataType::Int64)
            )
            .unwrap(),
            Some(TraceReconcileDecision::UseExisting(ColumnDataType::Int64))
        );
    }

    #[test]
    fn test_choose_trace_reconcile_decision_existing_int64_widens_to_float64() {
        assert_eq!(
            Instance::choose_trace_reconcile_decision(
                &[ColumnDataType::Int64, ColumnDataType::Float64],
                Some(ColumnDataType::Int64)
            )
            .unwrap(),
            Some(TraceReconcileDecision::AlterExistingTo(
                ColumnDataType::Float64
            ))
        );
    }

    #[test]
    fn test_choose_trace_reconcile_decision_existing_float64_stays_authoritative() {
        assert_eq!(
            Instance::choose_trace_reconcile_decision(
                &[ColumnDataType::Int64, ColumnDataType::Float64],
                Some(ColumnDataType::Float64)
            )
            .unwrap(),
            Some(TraceReconcileDecision::UseExisting(ColumnDataType::Float64))
        );
    }

    #[test]
    fn test_choose_trace_reconcile_decision_existing_int64_with_boolean_is_error() {
        let err = Instance::choose_trace_reconcile_decision(
            &[ColumnDataType::Boolean, ColumnDataType::Int64],
            Some(ColumnDataType::Int64),
        )
        .unwrap_err();
        assert_eq!(err.status_code(), StatusCode::InvalidArguments);
    }

    #[test]
    fn test_choose_trace_reconcile_decision_request_local_prefers_float64() {
        assert_eq!(
            Instance::choose_trace_reconcile_decision(
                &[ColumnDataType::Int64, ColumnDataType::Float64],
                None
            )
            .unwrap(),
            Some(TraceReconcileDecision::UseRequestLocal(
                ColumnDataType::Float64
            ))
        );
    }
}
