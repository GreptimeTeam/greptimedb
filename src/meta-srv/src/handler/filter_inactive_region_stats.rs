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
use async_trait::async_trait;
use common_telemetry::warn;

use crate::error::Result;
use crate::handler::{HandleControl, HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::Context;

pub struct FilterInactiveRegionStatsHandler;

#[async_trait]
impl HeartbeatHandler for FilterInactiveRegionStatsHandler {
    fn is_acceptable(&self, role: Role) -> bool {
        role == Role::Datanode
    }

    async fn handle(
        &self,
        req: &HeartbeatRequest,
        _ctx: &mut Context,
        acc: &mut HeartbeatAccumulator,
    ) -> Result<HandleControl> {
        if acc.inactive_region_ids.is_empty() {
            return Ok(HandleControl::Continue);
        }

        warn!(
            "The heartbeat of this node {:?} contains some inactive regions: {:?}",
            req.peer, acc.inactive_region_ids
        );

        let Some(stat) = acc.stat.as_mut() else {
            return Ok(HandleControl::Continue);
        };

        stat.retain_active_region_stats(&acc.inactive_region_ids);

        Ok(HandleControl::Continue)
    }
}
