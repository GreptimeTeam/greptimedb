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

use crate::error::Result;
use crate::handler::{HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::Context;

#[derive(Default)]
pub struct PersistStatsHandler;

#[async_trait::async_trait]
impl HeartbeatHandler for PersistStatsHandler {
    async fn handle(
        &self,
        _req: &HeartbeatRequest,
        ctx: &Context,
        acc: &mut HeartbeatAccumulator,
    ) -> Result<()> {
        if ctx.is_skip_all() || acc.stats.is_empty() {
            return Ok(());
        }

        // TODO(jiachun): remove stats from `acc` and persist to store

        Ok(())
    }
}
