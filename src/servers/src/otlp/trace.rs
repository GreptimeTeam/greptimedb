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
pub mod span;
pub mod v0;
pub mod v1;

use api::v1::RowInsertRequests;
pub use common_catalog::consts::{
    PARENT_SPAN_ID_COLUMN, SPAN_ID_COLUMN, SPAN_NAME_COLUMN, TRACE_ID_COLUMN,
};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use pipeline::{GreptimePipelineParams, PipelineWay};
use session::context::QueryContextRef;

use crate::error::{NotSupportedSnafu, Result};
use crate::query_handler::PipelineHandlerRef;

pub const TRACE_TABLE_NAME: &str = "opentelemetry_traces";

pub const SERVICE_NAME_COLUMN: &str = "service_name";
pub const TIMESTAMP_COLUMN: &str = "timestamp";
pub const DURATION_NANO_COLUMN: &str = "duration_nano";
pub const SPAN_KIND_COLUMN: &str = "span_kind";
pub const SPAN_ATTRIBUTES_COLUMN: &str = "span_attributes";
/// The span kind prefix in the database.
/// If the span kind is `server`, it will be stored as `SPAN_KIND_SERVER` in the database.
pub const SPAN_KIND_PREFIX: &str = "SPAN_KIND_";

/// Convert SpanTraces to GreptimeDB row insert requests.
/// Returns `InsertRequests` and total number of rows to ingest
pub fn to_grpc_insert_requests(
    request: ExportTraceServiceRequest,
    pipeline: PipelineWay,
    pipeline_params: GreptimePipelineParams,
    table_name: String,
    query_ctx: &QueryContextRef,
    pipeline_handler: PipelineHandlerRef,
) -> Result<(RowInsertRequests, usize)> {
    match pipeline {
        PipelineWay::OtlpTraceDirectV0 => v0::v0_to_grpc_insert_requests(
            request,
            pipeline,
            pipeline_params,
            table_name,
            query_ctx,
            pipeline_handler,
        ),
        PipelineWay::OtlpTraceDirectV1 => v1::v1_to_grpc_insert_requests(
            request,
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
