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

pub mod attributes;
pub mod coerce;
pub mod span;
pub mod v0;
pub mod v1;

use std::collections::HashSet;

use api::v1::RowInsertRequests;
pub use common_catalog::consts::{
    PARENT_SPAN_ID_COLUMN, SPAN_ID_COLUMN, SPAN_NAME_COLUMN, TRACE_ID_COLUMN,
};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use pipeline::{GreptimePipelineParams, PipelineWay};
use session::context::QueryContextRef;

use crate::error::{NotSupportedSnafu, Result};
use crate::otlp::trace::span::{TraceSpan, TraceSpans};
use crate::query_handler::PipelineHandlerRef;

// column names
pub const SERVICE_NAME_COLUMN: &str = "service_name";
pub const TIMESTAMP_COLUMN: &str = "timestamp";
pub const DURATION_NANO_COLUMN: &str = "duration_nano";
pub const SPAN_KIND_COLUMN: &str = "span_kind";
pub const SPAN_STATUS_CODE: &str = "span_status_code";
pub const SPAN_STATUS_MESSAGE_COLUMN: &str = "span_status_message";
pub const SPAN_ATTRIBUTES_COLUMN: &str = "span_attributes";
pub const SPAN_EVENTS_COLUMN: &str = "span_events";
pub const SCOPE_NAME_COLUMN: &str = "scope_name";
pub const SCOPE_VERSION_COLUMN: &str = "scope_version";
pub const RESOURCE_ATTRIBUTES_COLUMN: &str = "resource_attributes";
pub const TRACE_STATE_COLUMN: &str = "trace_state";

// const keys
pub const KEY_SERVICE_NAME: &str = "service.name";
pub const KEY_SERVICE_INSTANCE_ID: &str = "service.instance.id";
pub const KEY_SPAN_KIND: &str = "span.kind";

// jaeger const keys, not sure if they are general
pub const KEY_OTEL_SCOPE_NAME: &str = "otel.scope.name";
pub const KEY_OTEL_SCOPE_VERSION: &str = "otel.scope.version";
pub const KEY_OTEL_STATUS_CODE: &str = "otel.status_code";
pub const KEY_OTEL_STATUS_MESSAGE: &str = "otel.status_description";
pub const KEY_OTEL_STATUS_ERROR_KEY: &str = "error";
pub const KEY_OTEL_TRACE_STATE: &str = "w3c.tracestate";

/// The span kind prefix in the database.
/// If the span kind is `server`, it will be stored as `SPAN_KIND_SERVER` in the database.
pub const SPAN_KIND_PREFIX: &str = "SPAN_KIND_";

// The span status code prefix in the database.
pub const SPAN_STATUS_PREFIX: &str = "STATUS_CODE_";
pub const SPAN_STATUS_UNSET: &str = "STATUS_CODE_UNSET";
pub const SPAN_STATUS_ERROR: &str = "STATUS_CODE_ERROR";

#[derive(Debug, Default)]
pub struct TraceAuxData {
    pub services: HashSet<String>,
    pub operations: HashSet<(String, String, String)>,
}

impl TraceAuxData {
    pub fn observe_span(&mut self, span: &TraceSpan) {
        if let Some(service_name) = &span.service_name {
            self.services.insert(service_name.clone());
            self.operations.insert((
                service_name.clone(),
                span.span_name.clone(),
                span.span_kind.clone(),
            ));
        }
    }

    pub fn is_empty(&self) -> bool {
        self.services.is_empty() && self.operations.is_empty()
    }
}

pub fn parse(request: ExportTraceServiceRequest) -> TraceSpans {
    span::parse(request)
}

/// Convert a subset of trace spans to GreptimeDB row insert requests.
pub fn to_grpc_insert_requests_from_spans(
    spans: &[TraceSpan],
    pipeline: &PipelineWay,
    pipeline_params: &GreptimePipelineParams,
    table_name: &str,
    query_ctx: &QueryContextRef,
    pipeline_handler: PipelineHandlerRef,
) -> Result<(RowInsertRequests, usize)> {
    match pipeline {
        PipelineWay::OtlpTraceDirectV0 => v0::v0_to_grpc_main_insert_requests(
            spans,
            pipeline,
            pipeline_params,
            table_name,
            query_ctx,
            pipeline_handler,
        ),
        PipelineWay::OtlpTraceDirectV1 => v1::v1_to_grpc_main_insert_requests(
            spans,
            pipeline,
            pipeline_params,
            table_name,
            query_ctx,
            pipeline_handler,
        ),
        _ => NotSupportedSnafu {
            feat: "Unsupported pipeline for trace",
        }
        .fail(),
    }
}

/// Build insert requests for the auxiliary trace tables derived from accepted
/// spans.
///
/// "Aux" here refers to the trace service and trace operation tables, not the
/// main trace span table itself.
pub fn to_grpc_insert_requests_for_aux_tables(
    aux_data: TraceAuxData,
    pipeline: &PipelineWay,
    table_name: &str,
) -> Result<(RowInsertRequests, usize)> {
    match pipeline {
        PipelineWay::OtlpTraceDirectV0 => v0::build_aux_table_requests(aux_data, table_name),
        PipelineWay::OtlpTraceDirectV1 => v1::build_aux_table_requests(aux_data, table_name),
        _ => NotSupportedSnafu {
            feat: "Unsupported pipeline for trace",
        }
        .fail(),
    }
}
