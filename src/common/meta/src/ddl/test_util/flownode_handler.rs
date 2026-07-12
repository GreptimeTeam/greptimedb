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
use common_telemetry::debug;

use crate::error::Result;
use crate::peer::Peer;
use crate::test_util::MockFlownodeHandler;

#[derive(Clone)]
pub struct NaiveFlownodeHandler;

#[async_trait::async_trait]
impl MockFlownodeHandler for NaiveFlownodeHandler {
    async fn handle(&self, peer: &Peer, request: FlowRequest) -> Result<FlowResponse> {
        debug!("Returning Ok(0) for request: {request:?}, peer: {peer:?}");
        Ok(FlowResponse {
            affected_rows: 0,
            ..Default::default()
        })
    }

    async fn handle_inserts(
        &self,
        _peer: &Peer,
        _requests: InsertRequests,
    ) -> Result<FlowResponse> {
        unreachable!()
    }
}
