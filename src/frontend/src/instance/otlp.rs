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
use api::v1::value::ValueData;
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
            self.coerce_trace_numeric_columns(&mut requests, &ctx)
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

/// Try to coerce a single `ValueData` to a target `ColumnDataType`.
/// Returns the coerced value on success, or `None` if coercion is not possible.
fn coerce_value_data(
    value: &Option<ValueData>,
    target: ColumnDataType,
    request_type: ColumnDataType,
) -> Option<Option<ValueData>> {
    let Some(v) = value else {
        return Some(None);
    };
    match (request_type, target, v) {
        // Int64 -> Float64
        (ColumnDataType::Int64, ColumnDataType::Float64, ValueData::I64Value(n)) => {
            Some(Some(ValueData::F64Value(*n as f64)))
        }
        // String -> Int64
        (ColumnDataType::String, ColumnDataType::Int64, ValueData::StringValue(s)) => {
            s.parse::<i64>().ok().map(|n| Some(ValueData::I64Value(n)))
        }
        // String -> Float64
        (ColumnDataType::String, ColumnDataType::Float64, ValueData::StringValue(s)) => {
            s.parse::<f64>().ok().map(|n| Some(ValueData::F64Value(n)))
        }
        // String -> Boolean
        (ColumnDataType::String, ColumnDataType::Boolean, ValueData::StringValue(s)) => s
            .parse::<bool>()
            .ok()
            .map(|b| Some(ValueData::BoolValue(b))),
        _ => None,
    }
}

impl Instance {
    /// Coerce request column types and values to match the existing table schema
    /// for compatible type pairs. This handles the cross-batch case where a prior
    /// batch created the table with one type (e.g. Float64) but the current batch
    /// has a different type (e.g. Int64) for the same column.
    async fn coerce_trace_numeric_columns(
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

            let Some(table) = table else {
                continue;
            };
            let table_schema = table.schema();

            let Some(rows) = req.rows.as_mut() else {
                continue;
            };

            for (col_idx, col_schema) in rows.schema.iter_mut().enumerate() {
                let Some(table_col) = table_schema.column_schema_by_name(&col_schema.column_name)
                else {
                    continue;
                };

                let Ok(wrapper) = ColumnDataTypeWrapper::try_from(table_col.data_type.clone())
                else {
                    continue;
                };
                let table_datatype = wrapper.datatype() as i32;

                if col_schema.datatype == table_datatype {
                    continue;
                }

                let Some(request_type) = ColumnDataType::try_from(col_schema.datatype).ok() else {
                    continue;
                };
                let target_type = wrapper.datatype();

                let mut all_coerced = true;
                for row in &mut rows.rows {
                    if col_idx >= row.values.len() {
                        continue;
                    }
                    let value = &row.values[col_idx].value_data;
                    if let Some(coerced) = coerce_value_data(value, target_type, request_type) {
                        row.values[col_idx].value_data = coerced;
                    } else {
                        all_coerced = false;
                        break;
                    }
                }

                if all_coerced {
                    col_schema.datatype = table_datatype;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use api::v1::ColumnDataType;
    use api::v1::value::ValueData;

    use super::coerce_value_data;

    #[test]
    fn test_coerce_int64_to_float64() {
        let result = coerce_value_data(
            &Some(ValueData::I64Value(42)),
            ColumnDataType::Float64,
            ColumnDataType::Int64,
        );
        assert_eq!(result, Some(Some(ValueData::F64Value(42.0))));
    }

    #[test]
    fn test_coerce_string_to_int64() {
        let result = coerce_value_data(
            &Some(ValueData::StringValue("123".to_string())),
            ColumnDataType::Int64,
            ColumnDataType::String,
        );
        assert_eq!(result, Some(Some(ValueData::I64Value(123))));
    }

    #[test]
    fn test_coerce_string_to_float64() {
        let result = coerce_value_data(
            &Some(ValueData::StringValue("1.5".to_string())),
            ColumnDataType::Float64,
            ColumnDataType::String,
        );
        assert_eq!(result, Some(Some(ValueData::F64Value(1.5))));
    }

    #[test]
    fn test_coerce_string_to_boolean() {
        let result = coerce_value_data(
            &Some(ValueData::StringValue("true".to_string())),
            ColumnDataType::Boolean,
            ColumnDataType::String,
        );
        assert_eq!(result, Some(Some(ValueData::BoolValue(true))));

        let result = coerce_value_data(
            &Some(ValueData::StringValue("false".to_string())),
            ColumnDataType::Boolean,
            ColumnDataType::String,
        );
        assert_eq!(result, Some(Some(ValueData::BoolValue(false))));
    }

    #[test]
    fn test_coerce_unparseable_string() {
        let result = coerce_value_data(
            &Some(ValueData::StringValue("not_a_number".to_string())),
            ColumnDataType::Int64,
            ColumnDataType::String,
        );
        assert_eq!(result, None);
    }

    #[test]
    fn test_coerce_float64_to_int64_not_supported() {
        let result = coerce_value_data(
            &Some(ValueData::F64Value(1.5)),
            ColumnDataType::Int64,
            ColumnDataType::Float64,
        );
        assert_eq!(result, None);
    }

    #[test]
    fn test_coerce_none_value() {
        let result = coerce_value_data(&None, ColumnDataType::Float64, ColumnDataType::Int64);
        assert_eq!(result, Some(None));
    }
}
