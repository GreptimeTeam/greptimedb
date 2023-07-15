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
use common_error::ext::BoxedError;
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use servers::error::{self, Result as ServerResult};
use servers::otlp;
use servers::query_handler::OpenTelemetryProtocolHandler;
use session::context::QueryContextRef;
use snafu::ResultExt;

use crate::instance::Instance;

#[async_trait]
impl OpenTelemetryProtocolHandler for Instance {
    async fn metrics(
        &self,
        request: ExportMetricsServiceRequest,
        ctx: QueryContextRef,
    ) -> ServerResult<ExportMetricsServiceResponse> {
        let requests = otlp::to_grpc_insert_requests(request)?;
        let _ = self
            .handle_inserts(requests, ctx)
            .await
            .map_err(BoxedError::new)
            .context(error::ExecuteGrpcQuerySnafu)?;

        // TODO(sunng97): metrics
        // counter!(OTLP_REMOTE_WRITE_SAMPLES, samples as u64);
        let resp = ExportMetricsServiceResponse {
            partial_success: None,
        };
        Ok(resp)
    }
}
