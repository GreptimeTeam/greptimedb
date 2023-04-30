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

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use api::v1::meta::MailboxMessage;
use futures::Future;
use tokio::sync::oneshot;

use crate::error::{self, Result};

pub type MailboxRef = Arc<dyn Mailbox>;

pub type MessageId = u64;

pub enum Channel {
    Datanode(u64),
    Frontend(u64),
}

pub struct MailboxReceiver {
    message_id: MessageId,
    rx: oneshot::Receiver<Result<MailboxMessage>>,
}

impl MailboxReceiver {
    pub fn new(message_id: MessageId, rx: oneshot::Receiver<Result<MailboxMessage>>) -> Self {
        Self { message_id, rx }
    }

    pub fn message_id(&self) -> MessageId {
        self.message_id
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

    async fn on_recv(&self, id: MessageId, maybe_msg: Result<MailboxMessage>) -> Result<()>;
}
