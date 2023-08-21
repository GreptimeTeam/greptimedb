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

use std::pin::Pin;
use std::sync::Arc;

use api::v1::region::region_server_server::RegionServer as RegionServerService;
use api::v1::region::{RegionRequest, RegionResponse};
use api::v1::GreptimeRequest;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use async_trait::async_trait;
use auth::UserProviderRef;
use common_grpc::flight::{FlightEncoder, FlightMessage};
use common_query::Output;
use common_runtime::Runtime;
use futures::Stream;
use prost::Message;
use snafu::ResultExt;
use tonic::{Request, Response, Status, Streaming};

use crate::error::Result;
use crate::grpc::greptime_handler::GreptimeRequestHandler;
use crate::grpc::TonicResult;

#[async_trait]
pub trait RegionServerHandler: Send + Sync {
    async fn handle(&self, request: RegionRequest) -> Result<RegionResponse>;
}

#[derive(Clone)]
pub struct RegionServerRequestHandler {
    handler: Arc<dyn RegionServerHandler>,
    user_provider: Option<UserProviderRef>,
    runtime: Arc<Runtime>,
}

#[async_trait]
impl RegionServerService for RegionServerRequestHandler {
    async fn handle(
        &self,
        request: Request<RegionRequest>,
    ) -> TonicResult<Response<RegionResponse>> {
        todo!()
    }
}
