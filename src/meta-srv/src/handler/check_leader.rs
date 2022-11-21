// Copyright 2022 Greptime Team
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

use api::v1::meta::{Error, HeartbeatRequest};

use crate::error::Result;
use crate::handler::{HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::Context;

pub struct CheckLeaderHandler;

#[async_trait::async_trait]
impl HeartbeatHandler for CheckLeaderHandler {
    async fn handle(
        &self,
        _req: &HeartbeatRequest,
        ctx: &Context,
        acc: &mut HeartbeatAccumulator,
    ) -> Result<()> {
        if let Some(election) = &ctx.election {
            if election.is_leader() {
                return Ok(());
            }
            if let Some(header) = &mut acc.header {
                header.error = Some(Error::is_not_leader());
                ctx.set_skip_all();
            }
        }
        Ok(())
    }
}
