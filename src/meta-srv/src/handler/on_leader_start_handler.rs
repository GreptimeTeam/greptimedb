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

use api::v1::meta::{HeartbeatRequest, Role};

use crate::error::Result;
use crate::handler::{HandleControl, HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::Context;

pub struct OnLeaderStartHandler;

#[async_trait::async_trait]
impl HeartbeatHandler for OnLeaderStartHandler {
    fn is_acceptable(&self, role: Role) -> bool {
        role == Role::Datanode
    }

    async fn handle(
        &self,
        _req: &HeartbeatRequest,
        ctx: &mut Context,
        _acc: &mut HeartbeatAccumulator,
    ) -> Result<HandleControl> {
        let Some(election) = &ctx.election else {
            return Ok(HandleControl::Continue);
        };

        if election.in_infancy() {
            ctx.is_infancy = true;
            // TODO(weny): Unifies the multiple leader state between Context and MetaSrv.
            // we can't ensure the in-memory kv has already been reset in the outside loop.
            // We still use heartbeat requests to trigger resetting in-memory kv.
            ctx.reset_in_memory();
        }

        Ok(HandleControl::Continue)
    }
}
