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

use axum::extract::State;
use axum::http::header;
use axum::response::IntoResponse;
use axum::Extension;
use bytes::Bytes;
use common_catalog::consts::{TRACE_TABLE_NAME, TRACE_TABLE_NAME_SESSION_KEY};
use common_telemetry::tracing;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceResponse;
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use otel_arrow_rust::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
use pipeline::PipelineWay;
use prost::Message;
use session::context::{Channel, QueryContext};
use session::protocol_ctx::{MetricType, OtlpMetricCtx, ProtocolCtx};
use snafu::prelude::*;

use crate::error::{self, PipelineSnafu, Result};
use crate::http::extractor::{
    LogTableName, OtlpMetricOptions, PipelineInfo, SelectInfoWrapper, TraceTableName,
};
// use crate::http::header::constants::GREPTIME_METRICS_LEGACY_MODE_HEADER_NAME;
use crate::http::header::{write_cost_header_map, CONTENT_TYPE_PROTOBUF};
use crate::metrics::METRIC_HTTP_OPENTELEMETRY_LOGS_ELAPSED;
use crate::query_handler::{OpenTelemetryProtocolHandlerRef, PipelineHandler};

#[derive(Clone)]
pub struct OtlpState {
    pub with_metric_engine: bool,
    pub handler: OpenTelemetryProtocolHandlerRef,
}

#[axum_macros::debug_handler]
#[tracing::instrument(skip_all, fields(protocol = "otlp", request_type = "metrics"))]
pub async fn metrics(
    State(state): State<OtlpState>,
    Extension(mut query_ctx): Extension<QueryContext>,
    http_opts: OtlpMetricOptions,
    bytes: Bytes,
) -> Result<OtlpResponse<ExportMetricsServiceResponse>> {
    let db = query_ctx.get_db_string();
    query_ctx.set_channel(Channel::Otlp);

    let _timer = crate::metrics::METRIC_HTTP_OPENTELEMETRY_METRICS_ELAPSED
        .with_label_values(&[db.as_str()])
        .start_timer();
    let request =
        ExportMetricsServiceRequest::decode(bytes).context(error::DecodeOtlpRequestSnafu)?;

    let OtlpState {
        with_metric_engine,
        handler,
    } = state;

    query_ctx.set_protocol_ctx(ProtocolCtx::OtlpMetric(OtlpMetricCtx {
        promote_all_resource_attrs: http_opts.promote_all_resource_attrs,
        resource_attrs: http_opts.resource_attrs,
        promote_scope_attrs: http_opts.promote_scope_attrs,
        with_metric_engine,
        // set is_legacy later
        is_legacy: false,
        metric_type: MetricType::Init,
    }));
    let query_ctx = Arc::new(query_ctx);

    handler
        .metrics(request, query_ctx)
        .await
        .map(|o| OtlpResponse {
            resp_body: ExportMetricsServiceResponse {
                partial_success: None,
            },
            write_cost: o.meta.cost,
        })
}

#[axum_macros::debug_handler]
#[tracing::instrument(skip_all, fields(protocol = "otlp", request_type = "traces"))]
pub async fn traces(
    State(state): State<OtlpState>,
    TraceTableName(table_name): TraceTableName,
    pipeline_info: PipelineInfo,
    Extension(mut query_ctx): Extension<QueryContext>,
    bytes: Bytes,
) -> Result<OtlpResponse<ExportTraceServiceResponse>> {
    let db = query_ctx.get_db_string();
    let table_name = table_name.unwrap_or_else(|| TRACE_TABLE_NAME.to_string());

    query_ctx.set_channel(Channel::Otlp);
    query_ctx.set_extension(TRACE_TABLE_NAME_SESSION_KEY, &table_name);

    let query_ctx = Arc::new(query_ctx);
    let _timer = crate::metrics::METRIC_HTTP_OPENTELEMETRY_TRACES_ELAPSED
        .with_label_values(&[db.as_str()])
        .start_timer();
    let request =
        ExportTraceServiceRequest::decode(bytes).context(error::DecodeOtlpRequestSnafu)?;

    let pipeline = PipelineWay::from_name_and_default(
        pipeline_info.pipeline_name.as_deref(),
        pipeline_info.pipeline_version.as_deref(),
        None,
    )
    .context(PipelineSnafu)?;

    let pipeline_params = pipeline_info.pipeline_params;

    let OtlpState { handler, .. } = state;

    // here we use nightly feature `trait_upcasting` to convert handler to
    // pipeline_handler
    let pipeline_handler: Arc<dyn PipelineHandler + Send + Sync> = handler.clone();

    handler
        .traces(
            pipeline_handler,
            request,
            pipeline,
            pipeline_params,
            table_name,
            query_ctx,
        )
        .await
        .map(|o| OtlpResponse {
            resp_body: ExportTraceServiceResponse {
                partial_success: None,
            },
            write_cost: o.meta.cost,
        })
}

#[axum_macros::debug_handler]
#[tracing::instrument(skip_all, fields(protocol = "otlp", request_type = "logs"))]
pub async fn logs(
    State(state): State<OtlpState>,
    Extension(mut query_ctx): Extension<QueryContext>,
    pipeline_info: PipelineInfo,
    LogTableName(tablename): LogTableName,
    SelectInfoWrapper(select_info): SelectInfoWrapper,
    bytes: Bytes,
) -> Result<OtlpResponse<ExportLogsServiceResponse>> {
    let tablename = tablename.unwrap_or_else(|| "opentelemetry_logs".to_string());
    let db = query_ctx.get_db_string();
    query_ctx.set_channel(Channel::Otlp);
    let query_ctx = Arc::new(query_ctx);
    let _timer = METRIC_HTTP_OPENTELEMETRY_LOGS_ELAPSED
        .with_label_values(&[db.as_str()])
        .start_timer();
    let request = ExportLogsServiceRequest::decode(bytes).context(error::DecodeOtlpRequestSnafu)?;

    let pipeline = PipelineWay::from_name_and_default(
        pipeline_info.pipeline_name.as_deref(),
        pipeline_info.pipeline_version.as_deref(),
        Some(PipelineWay::OtlpLogDirect(Box::new(select_info))),
    )
    .context(PipelineSnafu)?;
    let pipeline_params = pipeline_info.pipeline_params;

    let OtlpState { handler, .. } = state;

    // here we use nightly feature `trait_upcasting` to convert handler to
    // pipeline_handler
    let pipeline_handler: Arc<dyn PipelineHandler + Send + Sync> = handler.clone();
    handler
        .logs(
            pipeline_handler,
            request,
            pipeline,
            pipeline_params,
            tablename,
            query_ctx,
        )
        .await
        .map(|o| OtlpResponse {
            resp_body: ExportLogsServiceResponse {
                partial_success: None,
            },
            write_cost: o.iter().map(|o| o.meta.cost).sum(),
        })
}

pub struct OtlpResponse<T: Message> {
    resp_body: T,
    write_cost: usize,
}

impl<T: Message> IntoResponse for OtlpResponse<T> {
    fn into_response(self) -> axum::response::Response {
        let mut header_map = write_cost_header_map(self.write_cost);
        header_map.insert(header::CONTENT_TYPE, CONTENT_TYPE_PROTOBUF.clone());

        (header_map, self.resp_body.encode_to_vec()).into_response()
    }
}
