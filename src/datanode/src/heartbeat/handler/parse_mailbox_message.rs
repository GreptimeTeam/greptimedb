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

use crate::error::Result;
use crate::heartbeat::handler::{
    HeartbeatResponseHandler, HeartbeatResponseHandlerContext, IncomingMessage,
};
use crate::heartbeat::utils::mailbox_message_to_incoming_message;

#[derive(Default)]
pub struct ParseMailboxMessageHandler {}

impl HeartbeatResponseHandler for ParseMailboxMessageHandler {
    fn handle(&self, ctx: &mut HeartbeatResponseHandlerContext) -> Result<()> {
        let executable_messages = ctx
            .response
            .mailbox_messages
            .iter()
            .filter_map(|m| {
                m.payload
                    .as_ref()
                    .map(|_| mailbox_message_to_incoming_message(m.clone()))
            })
            .collect::<Result<Vec<IncomingMessage>>>()?;

        ctx.incoming_messages.extend(executable_messages);

        Ok(())
    }
}
