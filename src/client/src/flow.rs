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

use api::v1::flow::{FlowRequest, FlowResponse};
use api::v1::region::InsertRequests;
use common_error::ext::BoxedError;
use common_meta::node_manager::Flownode;
use snafu::{location, ResultExt};

use crate::error::Result;
use crate::Client;

#[derive(Debug)]
pub struct FlowRequester {
    client: Client,
}

#[async_trait::async_trait]
impl Flownode for FlowRequester {
    async fn handle(&self, request: FlowRequest) -> common_meta::error::Result<FlowResponse> {
        self.handle_inner(request)
            .await
            .map_err(BoxedError::new)
            .context(common_meta::error::ExternalSnafu)
    }

    async fn handle_inserts(
        &self,
        request: InsertRequests,
    ) -> common_meta::error::Result<FlowResponse> {
        self.handle_inserts_inner(request)
            .await
            .map_err(BoxedError::new)
            .context(common_meta::error::ExternalSnafu)
    }
}

impl FlowRequester {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    async fn handle_inner(&self, request: FlowRequest) -> Result<FlowResponse> {
        let (addr, mut client) = self.client.raw_flow_client()?;

        let response = client
            .handle_create_remove(request)
            .await
            .map_err(|e| {
                let code = e.code();
                let err: crate::error::Error = e.into();
                crate::error::Error::FlowServer {
                    addr,
                    code,
                    source: BoxedError::new(err),
                    location: location!(),
                }
            })?
            .into_inner();
        Ok(response)
    }

    async fn handle_inserts_inner(&self, request: InsertRequests) -> Result<FlowResponse> {
        let (addr, mut client) = self.client.raw_flow_client()?;

        let requests = api::v1::flow::InsertRequests {
            requests: request
                .requests
                .into_iter()
                .map(|insert| api::v1::flow::InsertRequest {
                    region_id: insert.region_id,
                    rows: insert.rows,
                })
                .collect(),
        };

        let response = client
            .handle_mirror_request(requests)
            .await
            .map_err(|e| {
                let code = e.code();
                let err: crate::error::Error = e.into();
                crate::error::Error::FlowServer {
                    addr,
                    code,
                    source: BoxedError::new(err),
                    location: location!(),
                }
            })?
            .into_inner();
        Ok(response)
    }
}
