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
use async_trait::async_trait;
use common_telemetry::error;

use crate::error::Result;
use crate::heartbeat::mailbox::{IncomingMessage, MailboxRef};

pub mod parse_mailbox_message;
#[cfg(test)]
mod tests;

pub type HeartbeatResponseHandlerExecutorRef = Arc<dyn HeartbeatResponseHandlerExecutor>;
pub type HeartbeatResponseHandlerRef = Arc<dyn HeartbeatResponseHandler>;

pub struct HeartbeatResponseHandlerContext {
    pub mailbox: MailboxRef,
    pub response: HeartbeatResponse,
    pub incoming_message: Option<IncomingMessage>,
}

/// HandleControl
///
/// Controls process of handling heartbeat response.
#[derive(Debug, PartialEq)]
pub enum HandleControl {
    Continue,
    Done,
}

impl HeartbeatResponseHandlerContext {
    pub fn new(mailbox: MailboxRef, response: HeartbeatResponse) -> Self {
        Self {
            mailbox,
            response,
            incoming_message: None,
        }
    }
}

/// HeartbeatResponseHandler
///
/// [`HeartbeatResponseHandler::is_acceptable`] returns true if handler can handle incoming [`HeartbeatResponseHandlerContext`].
///
/// [`HeartbeatResponseHandler::handle`] handles all or part of incoming [`HeartbeatResponseHandlerContext`].
#[async_trait]
pub trait HeartbeatResponseHandler: Send + Sync {
    fn is_acceptable(&self, ctx: &HeartbeatResponseHandlerContext) -> bool;

    async fn handle(&self, ctx: &mut HeartbeatResponseHandlerContext) -> Result<HandleControl>;
}

#[async_trait]
pub trait HeartbeatResponseHandlerExecutor: Send + Sync {
    async fn handle(&self, ctx: HeartbeatResponseHandlerContext) -> Result<()>;
}

pub struct HandlerGroupExecutor {
    handlers: Vec<HeartbeatResponseHandlerRef>,
}

impl HandlerGroupExecutor {
    pub fn new(handlers: Vec<HeartbeatResponseHandlerRef>) -> Self {
        Self { handlers }
    }
}

#[async_trait]
impl HeartbeatResponseHandlerExecutor for HandlerGroupExecutor {
    async fn handle(&self, mut ctx: HeartbeatResponseHandlerContext) -> Result<()> {
        for handler in &self.handlers {
            if !handler.is_acceptable(&ctx) {
                continue;
            }

            match handler.handle(&mut ctx).await {
                Ok(HandleControl::Done) => break,
                Ok(HandleControl::Continue) => {}
                Err(e) => {
                    error!(e;"Error while handling: {:?}", ctx.response);
                    break;
                }
            }
        }
        Ok(())
    }
}
