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

use async_trait::async_trait;

use crate::error::Result;
use crate::heartbeat::handler::{
    HandleControl, HeartbeatResponseHandler, HeartbeatResponseHandlerContext,
};
use crate::heartbeat::utils::mailbox_message_to_incoming_message;

#[derive(Default)]
pub struct ParseMailboxMessageHandler;

#[async_trait]
impl HeartbeatResponseHandler for ParseMailboxMessageHandler {
    fn is_acceptable(&self, _ctx: &HeartbeatResponseHandlerContext) -> bool {
        true
    }

    async fn handle(&self, ctx: &mut HeartbeatResponseHandlerContext) -> Result<HandleControl> {
        if let Some(message) = &ctx.response.mailbox_message {
            if message.payload.is_some() {
                // mailbox_message_to_incoming_message will raise an error if payload is none
                ctx.incoming_message = Some(mailbox_message_to_incoming_message(message.clone())?)
            }
        }

        Ok(HandleControl::Continue)
    }
}
