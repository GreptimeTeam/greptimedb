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

use api::region::RegionResponse;
use api::v1::flow::{DirtyWindowRequests, FlowRequest, FlowResponse};
use api::v1::region::{InsertRequests, RegionRequest, RegionResponse as RegionResponseV1};
use client::flow::FlowRequester;
use client::region::check_response_header;
use common_error::ext::BoxedError;
use common_meta::error::{self as meta_error, Result as MetaResult};
use common_meta::flow_rpc::FlowRpc;
use common_meta::peer::Peer;
use common_meta::region_rpc::RegionRpc;
use common_query::request::QueryRequest;
use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::tracing;
use common_telemetry::tracing_context::{FutureExt, TracingContext};
use datanode::region_server::RegionServer;
use servers::grpc::region_server::RegionServerHandler;
use snafu::{OptionExt, ResultExt};

use crate::error::{InvalidRegionRequestSnafu, InvokeRegionServerSnafu, Result};

pub struct StandaloneRegionRpc {
    pub region_server: RegionServer,
}

#[async_trait::async_trait]
impl RegionRpc for StandaloneRegionRpc {
    async fn handle_region(
        &self,
        _peer: &Peer,
        request: RegionRequest,
    ) -> MetaResult<RegionResponse> {
        let span = request
            .header
            .as_ref()
            .map(|h| TracingContext::from_w3c(&h.tracing_context))
            .unwrap_or_default()
            .attach(tracing::info_span!("StandaloneRegionRpc::handle_region"));
        let response = self
            .handle_region_inner(request)
            .trace(span)
            .await
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)?;
        check_response_header(&response.header)
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)?;
        Ok(RegionResponse::from_region_response(response))
    }

    async fn handle_query(
        &self,
        _peer: &Peer,
        request: QueryRequest,
    ) -> MetaResult<SendableRecordBatchStream> {
        let region_id = request.region_id.to_string();
        let span = request
            .header
            .as_ref()
            .map(|h| TracingContext::from_w3c(&h.tracing_context))
            .unwrap_or_default()
            .attach(tracing::info_span!(
                "StandaloneRegionRpc::handle_query",
                region_id = region_id
            ));
        self.region_server
            .handle_read(request)
            .trace(span)
            .await
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)
    }
}

pub struct StandaloneFlowRpc {
    pub flow_server: FlowRequester,
}

#[async_trait::async_trait]
impl FlowRpc for StandaloneFlowRpc {
    async fn handle_flow(&self, _peer: &Peer, request: FlowRequest) -> MetaResult<FlowResponse> {
        self.flow_server.handle_flow(request).await
    }

    async fn handle_flow_inserts(
        &self,
        _peer: &Peer,
        request: InsertRequests,
    ) -> MetaResult<FlowResponse> {
        self.flow_server.handle_flow_inserts(request).await
    }

    async fn handle_mark_window_dirty(
        &self,
        _peer: &Peer,
        req: DirtyWindowRequests,
    ) -> MetaResult<FlowResponse> {
        self.flow_server.handle_mark_window_dirty(req).await
    }
}

impl StandaloneRegionRpc {
    async fn handle_region_inner(&self, request: RegionRequest) -> Result<RegionResponseV1> {
        let body = request.body.with_context(|| InvalidRegionRequestSnafu {
            reason: "body not found",
        })?;

        self.region_server
            .handle(body)
            .await
            .context(InvokeRegionServerSnafu)
    }
}
