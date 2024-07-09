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

use api::v1::GreptimeRequest;

use crate::error::Result;
use crate::{from_grpc_response, Client};

#[derive(Debug, Clone)]
pub struct FrontendRequester {
    client: Client,
}

impl FrontendRequester {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    pub async fn handle(&self, request: GreptimeRequest) -> Result<u32> {
        let (_, mut client) = self.client.raw_frontend_client()?;
        let response = client.handle(request).await?.into_inner();
        from_grpc_response(response)
    }
}
