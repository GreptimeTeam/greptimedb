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

use api::region::RegionResponse;
use api::v1::region::{RegionRequest, RegionResponse as RegionResponseV1};
use async_trait::async_trait;
use client::region::check_response_header;
use common_error::ext::BoxedError;
use common_meta::error::{self as meta_error, Result as MetaResult};
use common_meta::node_manager::{Datanode, DatanodeRef, FlownodeRef, NodeManager};
use common_meta::peer::Peer;
use common_query::request::QueryRequest;
use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::tracing;
use common_telemetry::tracing_context::{FutureExt, TracingContext};
use datanode::region_server::RegionServer;
use servers::grpc::region_server::RegionServerHandler;
use snafu::{OptionExt, ResultExt};

use crate::error::{InvalidRegionRequestSnafu, InvokeRegionServerSnafu, Result};

pub struct StandaloneDatanodeManager {
    pub region_server: RegionServer,
    pub flow_server: FlownodeRef,
}

#[async_trait]
impl NodeManager for StandaloneDatanodeManager {
    async fn datanode(&self, _datanode: &Peer) -> DatanodeRef {
        RegionInvoker::arc(self.region_server.clone())
    }

    async fn flownode(&self, _node: &Peer) -> FlownodeRef {
        self.flow_server.clone()
    }
}

/// Relative to [client::region::RegionRequester]
pub struct RegionInvoker {
    region_server: RegionServer,
}

impl RegionInvoker {
    pub fn arc(region_server: RegionServer) -> Arc<Self> {
        Arc::new(Self { region_server })
    }

    async fn handle_inner(&self, request: RegionRequest) -> Result<RegionResponseV1> {
        let body = request.body.with_context(|| InvalidRegionRequestSnafu {
            reason: "body not found",
        })?;

        self.region_server
            .handle(body)
            .await
            .context(InvokeRegionServerSnafu)
    }
}

#[async_trait]
impl Datanode for RegionInvoker {
    async fn handle(&self, request: RegionRequest) -> MetaResult<RegionResponse> {
        let span = request
            .header
            .as_ref()
            .map(|h| TracingContext::from_w3c(&h.tracing_context))
            .unwrap_or_default()
            .attach(tracing::info_span!("RegionInvoker::handle_region_request"));
        let response = self
            .handle_inner(request)
            .trace(span)
            .await
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)?;
        check_response_header(&response.header)
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)?;
        Ok(RegionResponse::from_region_response(response))
    }

    async fn handle_query(&self, request: QueryRequest) -> MetaResult<SendableRecordBatchStream> {
        let span = request
            .header
            .as_ref()
            .map(|h| TracingContext::from_w3c(&h.tracing_context))
            .unwrap_or_default()
            .attach(tracing::info_span!("RegionInvoker::handle_query"));
        self.region_server
            .handle_read(request)
            .trace(span)
            .await
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)
    }
}
