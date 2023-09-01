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

use api::v1::meta::{Error, HeartbeatRequest, Role};
use common_telemetry::warn;

use crate::error::Result;
use crate::handler::{HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::Context;

pub struct CheckLeaderHandler;

#[async_trait::async_trait]
impl HeartbeatHandler for CheckLeaderHandler {
    fn is_acceptable(&self, role: Role) -> bool {
        role == Role::Datanode
    }

    async fn handle(
        &self,
        req: &HeartbeatRequest,
        ctx: &mut Context,
        acc: &mut HeartbeatAccumulator,
    ) -> Result<()> {
        if let Some(election) = &ctx.election {
            if election.is_leader() {
                return Ok(());
            }
            if let Some(header) = &mut acc.header {
                header.error = Some(Error::is_not_leader());
                ctx.set_skip_all();
                warn!("Received a heartbeat {:?}, but the current node is not the leader, so the heartbeat will be ignored.", req.header);
            }
        }
        Ok(())
    }
}
