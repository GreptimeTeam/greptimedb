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

use api::v1::RowInsertRequests;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use pipeline::{GreptimePipelineParams, PipelineWay};
use session::context::QueryContextRef;

use crate::error::{NotSupportedSnafu, Result};
use crate::query_handler::PipelineHandlerRef;

pub const TRACE_TABLE_NAME: &str = "opentelemetry_traces";

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
        _ => NotSupportedSnafu {
            feat: "Unsupported pipeline for logs",
        }
        .fail(),
    }
}
