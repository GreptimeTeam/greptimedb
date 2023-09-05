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

use api::v1::greptime_request::Request;
use api::v1::region::{region_request, QueryRequest, RegionResponse};
use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_query::Output;
use common_recordbatch::SendableRecordBatchStream;
use datanode::error::Error as DatanodeError;
use datanode::region_server::RegionServer;
use servers::error::ExecutePlanSnafu;
use servers::grpc::region_server::RegionServerHandler;
use servers::query_handler::grpc::{GrpcQueryHandler, GrpcQueryHandlerRef};
use session::context::QueryContextRef;
use snafu::ResultExt;

use super::region_handler::RegionRequestHandler;
use crate::error::{Error, InvokeDatanodeSnafu, InvokeRegionServerSnafu, Result};

pub(crate) struct StandaloneGrpcQueryHandler(GrpcQueryHandlerRef<DatanodeError>);

impl StandaloneGrpcQueryHandler {
    pub(crate) fn arc(handler: GrpcQueryHandlerRef<DatanodeError>) -> Arc<Self> {
        Arc::new(Self(handler))
    }
}

#[async_trait]
impl GrpcQueryHandler for StandaloneGrpcQueryHandler {
    type Error = Error;

    async fn do_query(&self, query: Request, ctx: QueryContextRef) -> Result<Output> {
        self.0
            .do_query(query, ctx)
            .await
            .context(InvokeDatanodeSnafu)
    }
}

pub(crate) struct StandaloneRegionRequestHandler {
    region_server: RegionServer,
}

impl StandaloneRegionRequestHandler {
    pub fn arc(region_server: RegionServer) -> Arc<Self> {
        Arc::new(Self { region_server })
    }
}

#[async_trait]
impl RegionRequestHandler for StandaloneRegionRequestHandler {
    async fn handle(
        &self,
        request: region_request::Body,
        _ctx: QueryContextRef,
    ) -> Result<RegionResponse> {
        self.region_server
            .handle(request)
            .await
            .context(InvokeRegionServerSnafu)
    }

    async fn do_get(&self, request: QueryRequest) -> Result<SendableRecordBatchStream> {
        self.region_server
            .handle_read(request)
            .await
            .map_err(BoxedError::new)
            .context(ExecutePlanSnafu)
            .context(InvokeRegionServerSnafu)
    }
}
