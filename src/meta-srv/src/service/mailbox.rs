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
use std::time::Duration;

use api::v1::meta::MailboxMessage;

use crate::error::Result;

pub type MailboxRef = Arc<dyn Mailbox>;

pub type MessageId = u64;

pub enum Channel {
    Datanode(u64),
    Frontend(u64),
}

#[async_trait::async_trait]
pub trait Mailbox: Send + Sync {
    /// Send a message to the mailbox, it will return a `id` immediately,
    /// then we can use the `id` to call `recv` to get the response.
    async fn send(&self, ch: &Channel, msg: MailboxMessage) -> Result<MessageId>;

    /// Receive a message from the mailbox with the given `id`.
    async fn recv(&self, id: MessageId) -> Result<MailboxMessage>;

    async fn recv_timeout(&self, id: MessageId, timeout: Duration) -> Result<MailboxMessage>;

    async fn send_and_recv(&self, ch: &Channel, msg: MailboxMessage) -> Result<MailboxMessage> {
        let id = self.send(ch, msg).await?;
        self.recv(id).await
    }

    async fn send_and_recv_timeout(
        &self,
        ch: &Channel,
        msg: MailboxMessage,
        timeout: Duration,
    ) -> Result<MailboxMessage> {
        let id = self.send(ch, msg).await?;
        self.recv_timeout(id, timeout).await
    }

    async fn on_recv(&self, id: MessageId, maybe_msg: Result<MailboxMessage>) -> Result<()>;
}
