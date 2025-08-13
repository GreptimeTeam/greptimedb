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

use api::v1::meta::{HeartbeatRequest, ResponseHeader, Role, PROTOCOL_VERSION};

use crate::error::Result;
use crate::handler::{HandleControl, HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::Context;

pub struct ResponseHeaderHandler;

#[async_trait::async_trait]
impl HeartbeatHandler for ResponseHeaderHandler {
    fn is_acceptable(&self, _: Role) -> bool {
        true
    }

    async fn handle(
        &self,
        _req: &HeartbeatRequest,
        _ctx: &mut Context,
        acc: &mut HeartbeatAccumulator,
    ) -> Result<HandleControl> {
        let res_header = ResponseHeader {
            protocol_version: PROTOCOL_VERSION,
            ..Default::default()
        };
        acc.header = Some(res_header);
        Ok(HandleControl::Continue)
    }
}

#[cfg(test)]
mod tests {
    use api::v1::meta::RequestHeader;
    use common_telemetry::tracing_context::W3cTrace;

    use super::*;
    use crate::handler::test_utils::TestEnv;

    #[tokio::test]
    async fn test_handle_heartbeat_resp_header() {
        let env = TestEnv::new();
        let mut ctx = env.ctx();

        let req = HeartbeatRequest {
            header: Some(RequestHeader::new(2, Role::Datanode, W3cTrace::new())),
            ..Default::default()
        };
        let mut acc = HeartbeatAccumulator::default();

        let response_handler = ResponseHeaderHandler {};
        response_handler
            .handle(&req, &mut ctx, &mut acc)
            .await
            .unwrap();
    }
}
