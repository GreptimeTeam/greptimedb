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

use std::sync::Arc;

use api::v1::meta::HeartbeatResponse;
use common_meta::instruction::{Instruction, InstructionReply};
use common_telemetry::error;
use tokio::sync::mpsc::Sender;

use crate::error::{self, Result};

pub mod close_table;
pub mod open_table;
pub mod parse_mailbox_message;

pub type IncomingMessage = (MessageMeta, Instruction);
pub type OutgoingMessage = (MessageMeta, InstructionReply);
pub struct MessageMeta {
    pub id: u64,
    pub subject: String,
    pub to: String,
    pub from: String,
}

pub struct HeartbeatMailbox {
    sender: Sender<OutgoingMessage>,
}

impl HeartbeatMailbox {
    pub fn new(sender: Sender<OutgoingMessage>) -> Self {
        Self { sender }
    }

    pub async fn send(&self, message: OutgoingMessage) -> Result<()> {
        self.sender.send(message).await.map_err(|e| {
            error::SendMessageSnafu {
                err_msg: e.to_string(),
            }
            .build()
        })
    }
}

pub type MailboxRef = Arc<HeartbeatMailbox>;

pub type HeartbeatResponseHandlerExecutorRef = Arc<dyn HeartbeatResponseHandlerExecutor>;

pub struct HeartbeatResponseHandlerContext {
    pub mailbox: MailboxRef,
    pub response: HeartbeatResponse,
    pub incoming_messages: Vec<IncomingMessage>,
}

impl HeartbeatResponseHandlerContext {
    pub fn new(mailbox: MailboxRef, response: HeartbeatResponse) -> Self {
        Self {
            mailbox,
            response,
            incoming_messages: Vec::new(),
        }
    }
}

pub trait HeartbeatResponseHandler: Send + Sync {
    fn handle(&self, ctx: &mut HeartbeatResponseHandlerContext) -> Result<()>;
}

pub trait HeartbeatResponseHandlerExecutor: Send + Sync {
    fn handle(&self, ctx: HeartbeatResponseHandlerContext) -> Result<()>;
}

pub struct HandlerGroupExecutor {
    handlers: Vec<Arc<dyn HeartbeatResponseHandler>>,
}

impl HandlerGroupExecutor {
    pub fn new(handlers: Vec<Arc<dyn HeartbeatResponseHandler>>) -> Self {
        Self { handlers }
    }
}

impl HeartbeatResponseHandlerExecutor for HandlerGroupExecutor {
    fn handle(&self, mut ctx: HeartbeatResponseHandlerContext) -> Result<()> {
        for handler in &self.handlers {
            if let Err(e) = handler.handle(&mut ctx) {
                error!("Error while handling: {:?}, source: {}", ctx.response, e);
            }
        }
        Ok(())
    }
}
