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

//! PrometheusGateway provides a gRPC interface to query Prometheus metrics
//! by PromQL. The behavior is similar to the Prometheus HTTP API.

use api::v1::prometheus_gateway_server::PrometheusGateway;
use api::v1::{PromqlRequest, PromqlResponse};
use async_trait::async_trait;
use tonic::{Request, Response};

use crate::grpc::TonicResult;

pub struct PrometheusGatewayServer;

#[async_trait]
impl PrometheusGateway for PrometheusGatewayServer {
    async fn handle(&self, req: Request<PromqlRequest>) -> TonicResult<Response<PromqlResponse>> {
        todo!()
    }
}
