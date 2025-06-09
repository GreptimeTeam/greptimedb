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
use futures::{Future, FutureExt};
use tokio::sync::oneshot;

use crate::error::{self, Result};
use crate::handler::{DeregisterSignalReceiver, PusherId};

pub type MailboxRef = Arc<dyn Mailbox>;

pub type MessageId = u64;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Channel {
    Datanode(u64),
    Frontend(u64),
    Flownode(u64),
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
            Channel::Flownode(id) => {
                write!(f, "Flownode-{}", id)
            }
        }
    }
}

impl Channel {
    pub(crate) fn pusher_id(&self) -> PusherId {
        match self {
            Channel::Datanode(id) => PusherId::new(Role::Datanode, *id),
            Channel::Frontend(id) => PusherId::new(Role::Frontend, *id),
            Channel::Flownode(id) => PusherId::new(Role::Flownode, *id),
        }
    }
}
pub enum BroadcastChannel {
    Datanode,
    Frontend,
    Flownode,
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
                end: format!("{}-", Role::Flownode as i32),
            },
            BroadcastChannel::Flownode => Range {
                start: format!("{}-", Role::Flownode as i32),
                end: format!("{}-", Role::Flownode as i32 + 1),
            },
        }
    }
}

/// The mailbox receiver
pub enum MailboxReceiver {
    Init {
        message_id: MessageId,
        ch: Channel,
        /// The [`MailboxMessage`] receiver
        rx: Option<oneshot::Receiver<Result<MailboxMessage>>>,
        /// The pusher deregister signal receiver
        pusher_deregister_signal_receiver: Option<DeregisterSignalReceiver>,
    },
    Polling {
        message_id: MessageId,
        ch: Channel,
        inner_future: Pin<Box<dyn Future<Output = Result<MailboxMessage>> + Send + 'static>>,
    },
}

impl MailboxReceiver {
    pub fn new(
        message_id: MessageId,
        rx: oneshot::Receiver<Result<MailboxMessage>>,
        pusher_deregister_signal_receiver: DeregisterSignalReceiver,
        ch: Channel,
    ) -> Self {
        Self::Init {
            message_id,
            rx: Some(rx),
            pusher_deregister_signal_receiver: Some(pusher_deregister_signal_receiver),
            ch,
        }
    }

    /// Get the message id of the mailbox receiver
    pub fn message_id(&self) -> MessageId {
        match self {
            MailboxReceiver::Init { message_id, .. } => *message_id,
            MailboxReceiver::Polling { message_id, .. } => *message_id,
        }
    }

    /// Get the channel of the mailbox receiver
    pub fn channel(&self) -> Channel {
        match self {
            MailboxReceiver::Init { ch, .. } => *ch,
            MailboxReceiver::Polling { ch, .. } => *ch,
        }
    }

    async fn wait_for_message(
        rx: oneshot::Receiver<Result<MailboxMessage>>,
        mut pusher_deregister_signal_receiver: DeregisterSignalReceiver,
        channel: Channel,
        message_id: MessageId,
    ) -> Result<MailboxMessage> {
        tokio::select! {
            res = rx => {
                res.map_err(|e| error::MailboxReceiverSnafu {
                    id: message_id,
                    err_msg: e.to_string(),
                }.build())?
            }
            _ = pusher_deregister_signal_receiver.changed() => {
                 Err(error::MailboxChannelClosedSnafu {
                    channel,
                }.build())
            }
        }
    }
}

impl Future for MailboxReceiver {
    type Output = Result<MailboxMessage>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut *self {
            MailboxReceiver::Init {
                message_id,
                ch,
                rx,
                pusher_deregister_signal_receiver,
            } => {
                let polling = MailboxReceiver::Polling {
                    message_id: *message_id,
                    ch: *ch,
                    inner_future: Self::wait_for_message(
                        rx.take().expect("rx already taken"),
                        pusher_deregister_signal_receiver
                            .take()
                            .expect("pusher_deregister_signal_receiver already taken"),
                        *ch,
                        *message_id,
                    )
                    .boxed(),
                };

                *self = polling;
                self.poll(cx)
            }
            MailboxReceiver::Polling { inner_future, .. } => {
                let result = futures::ready!(inner_future.as_mut().poll(cx));
                Poll::Ready(result)
            }
        }
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

    async fn send_oneway(&self, ch: &Channel, msg: MailboxMessage) -> Result<()>;

    async fn broadcast(&self, ch: &BroadcastChannel, msg: &MailboxMessage) -> Result<()>;

    async fn on_recv(&self, id: MessageId, maybe_msg: Result<MailboxMessage>) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use common_time::util::current_time_millis;
    use tokio::sync::watch;

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
        assert_eq!(
            BroadcastChannel::Flownode.pusher_range(),
            ("2-".to_string().."3-".to_string())
        );
    }

    #[tokio::test]
    async fn test_mailbox_receiver() {
        let (tx, rx) = oneshot::channel();
        let (_deregister_signal_tx, deregister_signal_rx) = watch::channel(false);
        let receiver = MailboxReceiver::new(1, rx, deregister_signal_rx, Channel::Datanode(1));

        let timestamp_millis = current_time_millis();
        tokio::spawn(async move {
            tx.send(Ok(MailboxMessage {
                id: 1,
                subject: "test-subject".to_string(),
                from: "test-from".to_string(),
                to: "test-to".to_string(),
                timestamp_millis,
                payload: None,
            }))
        });

        let result = receiver.await.unwrap();
        assert_eq!(result.id, 1);
        assert_eq!(result.subject, "test-subject");
        assert_eq!(result.from, "test-from");
        assert_eq!(result.to, "test-to");
    }

    #[tokio::test]
    async fn test_mailbox_receiver_deregister_signal() {
        let (_tx, rx) = oneshot::channel();
        let (deregister_signal_tx, deregister_signal_rx) = watch::channel(false);
        let receiver = MailboxReceiver::new(1, rx, deregister_signal_rx, Channel::Datanode(1));

        // Sends the deregister signal
        tokio::spawn(async move {
            let _ = deregister_signal_tx.send(true);
        });

        let err = receiver.await.unwrap_err();
        assert_matches!(err, error::Error::MailboxChannelClosed { .. });
    }
}
