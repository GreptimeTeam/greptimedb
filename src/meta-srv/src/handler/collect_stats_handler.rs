// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use api::v1::meta::HeartbeatRequest;
use common_telemetry::debug;

use super::node_stat::Stat;
use crate::error::Result;
use crate::handler::{HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::Context;

pub struct CollectStatsHandler;

#[async_trait::async_trait]
impl HeartbeatHandler for CollectStatsHandler {
    async fn handle(
        &self,
        req: &HeartbeatRequest,
        ctx: &mut Context,
        acc: &mut HeartbeatAccumulator,
    ) -> Result<()> {
        if ctx.is_skip_all() {
            return Ok(());
        }

        match Stat::try_from(req.clone()) {
            Ok(stat) => {
                let _ = acc.stat.insert(stat);
            }
            Err(_) => {
                debug!("Incomplete heartbeat data: {:?}", req);
            }
        };

        Ok(())
    }
}
