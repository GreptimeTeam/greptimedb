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

use std::fmt::{Display, Formatter};
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use api::v1::meta::{MailboxMessage, Role};
use futures::Future;
use tokio::sync::oneshot;

use crate::error::{self, Result};

pub type MailboxRef = Arc<dyn Mailbox>;

pub type MessageId = u64;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Channel {
    Datanode(u64),
    Frontend(u64),
}

impl Display for Channel {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Channel::Datanode(id) => {
                write!(f, "Datanode-{}", id)
            }
            Channel::Frontend(id) => {
                write!(f, "Frontend-{}", id)
            }
        }
    }
}

impl Channel {
    pub(crate) fn pusher_id(&self) -> String {
        match self {
            Channel::Datanode(id) => format!("{}-{}", Role::Datanode as i32, id),
            Channel::Frontend(id) => format!("{}-{}", Role::Frontend as i32, id),
        }
    }
}
pub enum BroadcastChannel {
    Datanode,
    Frontend,
}

impl BroadcastChannel {
    pub(crate) fn pusher_range(&self) -> Range<String> {
        match self {
            BroadcastChannel::Datanode => Range {
                start: format!("{}-", Role::Datanode as i32),
                end: format!("{}-", Role::Frontend as i32),
            },
            BroadcastChannel::Frontend => Range {
                start: format!("{}-", Role::Frontend as i32),
                end: format!("{}-", Role::Frontend as i32 + 1),
            },
        }
    }
}

pub struct MailboxReceiver {
    message_id: MessageId,
    rx: oneshot::Receiver<Result<MailboxMessage>>,
    ch: Channel,
}

impl MailboxReceiver {
    pub fn new(
        message_id: MessageId,
        rx: oneshot::Receiver<Result<MailboxMessage>>,
        ch: Channel,
    ) -> Self {
        Self { message_id, rx, ch }
    }

    pub fn message_id(&self) -> MessageId {
        self.message_id
    }

    pub fn channel(&self) -> Channel {
        self.ch
    }
}

impl Future for MailboxReceiver {
    type Output = Result<Result<MailboxMessage>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.rx).poll(cx).map(|r| {
            r.map_err(|e| {
                error::MailboxReceiverSnafu {
                    id: self.message_id,
                    err_msg: e.to_string(),
                }
                .build()
            })
        })
    }
}

#[async_trait::async_trait]
pub trait Mailbox: Send + Sync {
    async fn send(
        &self,
        ch: &Channel,
        msg: MailboxMessage,
        timeout: Duration,
    ) -> Result<MailboxReceiver>;

    async fn broadcast(&self, ch: &BroadcastChannel, msg: &MailboxMessage) -> Result<()>;

    async fn on_recv(&self, id: MessageId, maybe_msg: Result<MailboxMessage>) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel_pusher_range() {
        assert_eq!(
            BroadcastChannel::Datanode.pusher_range(),
            ("0-".to_string().."1-".to_string())
        );
        assert_eq!(
            BroadcastChannel::Frontend.pusher_range(),
            ("1-".to_string().."2-".to_string())
        );
    }
}
