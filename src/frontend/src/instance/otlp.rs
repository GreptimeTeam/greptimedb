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
use api::v1::{ColumnDataType, RowInsertRequests};
use async_trait::async_trait;
use auth::{PermissionChecker, PermissionCheckerRef, PermissionReq};
use client::Output;
use common_error::ext::BoxedError;
use common_query::prelude::GREPTIME_PHYSICAL_TABLE;
use common_telemetry::tracing;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use otel_arrow_rust::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
use pipeline::{GreptimePipelineParams, PipelineWay};
use servers::error::{self, AuthSnafu, CatalogSnafu, Result as ServerResult};
use servers::http::prom_store::PHYSICAL_TABLE_PARAM;
use servers::interceptor::{OpenTelemetryProtocolInterceptor, OpenTelemetryProtocolInterceptorRef};
use servers::otlp;
use servers::otlp::trace::coerce::{
    coerce_value_data, is_supported_trace_coercion, resolve_new_trace_column_type,
    trace_value_datatype,
};
use servers::query_handler::{OpenTelemetryProtocolHandler, PipelineHandlerRef};
use session::context::QueryContextRef;
use snafu::ResultExt;
use table::requests::{OTLP_METRIC_COMPAT_KEY, OTLP_METRIC_COMPAT_PROM};

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

        let is_trace_v1_model = matches!(pipeline, PipelineWay::OtlpTraceDirectV1);

        let (mut requests, rows) = otlp::trace::to_grpc_insert_requests(
            request,
            pipeline,
            pipeline_params,
            table_name,
            &ctx,
            pipeline_handler,
        )?;

        OTLP_TRACES_ROWS.inc_by(rows as u64);

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
    /// Picks the final datatype for one trace column.
    ///
    /// Existing table schema is authoritative when present. Otherwise we resolve the
    /// request-local observed types using the shared trace coercion rules.
    fn choose_trace_target_type(
        observed_types: &[ColumnDataType],
        existing_type: Option<ColumnDataType>,
    ) -> ServerResult<Option<ColumnDataType>> {
        let Some(existing_type) = existing_type else {
            return resolve_new_trace_column_type(observed_types.iter().copied()).map_err(|_| {
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
            Ok(Some(existing_type))
        } else {
            error::InvalidParameterSnafu {
                reason: "unsupported trace type mix".to_string(),
            }
            .fail()
        }
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
                .await
                .context(CatalogSnafu)?;

            let Some(rows) = req.rows.as_mut() else {
                continue;
            };

            let table_schema = table.map(|table| table.schema());
            let mut pending_coercions = Vec::new();

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
                let Some(target_type) =
                    Self::choose_trace_target_type(&observed_types, existing_type).map_err(
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

                if observed_types
                    .iter()
                    .all(|observed| *observed == target_type)
                    && col_schema.datatype == target_type as i32
                {
                    continue;
                }

                pending_coercions.push((col_idx, target_type, col_schema.column_name.clone()));
            }

            if pending_coercions.is_empty() {
                continue;
            }

            // Update schema metadata before mutating row values so both stay in sync.
            for (col_idx, target_type, ..) in &pending_coercions {
                rows.schema[*col_idx].datatype = *target_type as i32;
            }

            // Apply all pending column rewrites in one row pass.
            for row in &mut rows.rows {
                for (col_idx, target_type, column_name) in &pending_coercions {
                    let Some(value) = row.values.get_mut(*col_idx) else {
                        continue;
                    };
                    let Some(request_type) =
                        value.value_data.as_ref().and_then(trace_value_datatype)
                    else {
                        continue;
                    };
                    if request_type == *target_type {
                        continue;
                    }

                    value.value_data = coerce_value_data(
                        &value.value_data,
                        *target_type,
                        request_type,
                    )
                    .map_err(|_| {
                        error::InvalidParameterSnafu {
                            reason: format!(
                                "failed to coerce trace column '{}' in table '{}' from {:?} to {:?}",
                                column_name, req.table_name, request_type, target_type
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
