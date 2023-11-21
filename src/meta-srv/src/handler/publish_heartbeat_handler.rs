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
use crate::handler::{HandleControl, HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::Context;
use crate::pubsub::{Message, PublishRef};

pub struct PublishHeartbeatHandler {
    publish: PublishRef,
}

impl PublishHeartbeatHandler {
    pub fn new(publish: PublishRef) -> PublishHeartbeatHandler {
        PublishHeartbeatHandler { publish }
    }
}

#[async_trait]
impl HeartbeatHandler for PublishHeartbeatHandler {
    fn is_acceptable(&self, role: Role) -> bool {
        role == Role::Datanode
    }

    async fn handle(
        &self,
        req: &HeartbeatRequest,
        _: &mut Context,
        _: &mut HeartbeatAccumulator,
    ) -> Result<HandleControl> {
        let msg = Message::Heartbeat(Box::new(req.clone()));
        self.publish.send_msg(msg).await;

        Ok(HandleControl::Continue)
    }
}
