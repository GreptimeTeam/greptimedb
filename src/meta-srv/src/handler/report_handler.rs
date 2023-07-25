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

use crate::error::Result;
use crate::handler::{HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::Context;
use crate::pubsub::Message;

pub struct ReportHandler;

#[async_trait]
impl HeartbeatHandler for ReportHandler {
    fn is_acceptable(&self, role: Role) -> bool {
        role == Role::Datanode
    }

    async fn handle(
        &self,
        req: &HeartbeatRequest,
        ctx: &mut Context,
        _: &mut HeartbeatAccumulator,
    ) -> Result<()> {
        let req = Box::new(req.clone());

        if let Some(publish) = ctx.publish.as_ref() {
            publish.send_msg(Message::Heartbeat(req)).await;
        }

        Ok(())
    }
}
