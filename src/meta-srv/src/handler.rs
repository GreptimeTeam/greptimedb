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

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use api::v1::meta::{HeartbeatRequest, HeartbeatResponse, MailboxMessage, ResponseHeader, Role};
pub use check_leader_handler::CheckLeaderHandler;
pub use collect_stats_handler::CollectStatsHandler;
use common_telemetry::{info, warn};
use dashmap::DashMap;
pub use failure_handler::RegionFailureHandler;
pub use keep_lease_handler::KeepLeaseHandler;
pub use on_leader_start::OnLeaderStartHandler;
pub use persist_stats_handler::PersistStatsHandler;
pub use response_header_handler::ResponseHeaderHandler;
use snafu::OptionExt;
use tokio::sync::mpsc::Sender;
use tokio::sync::{oneshot, Notify, RwLock};

use self::instruction::Instruction;
use self::node_stat::Stat;
use crate::error::{self, Result};
use crate::metasrv::Context;
use crate::sequence::Sequence;
use crate::service::mailbox::{Channel, Mailbox, MailboxReceiver, MailboxRef, MessageId};

mod check_leader_handler;
mod collect_stats_handler;
mod failure_handler;
mod instruction;
mod keep_lease_handler;
pub mod mailbox_handler;
pub mod node_stat;
mod on_leader_start;
mod persist_stats_handler;
mod response_header_handler;

#[async_trait::async_trait]
pub trait HeartbeatHandler: Send + Sync {
    fn is_acceptable(&self, role: Role) -> bool;

    async fn handle(
        &self,
        req: &HeartbeatRequest,
        ctx: &mut Context,
        acc: &mut HeartbeatAccumulator,
    ) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct HeartbeatAccumulator {
    pub header: Option<ResponseHeader>,
    pub instructions: Vec<Instruction>,
    pub stat: Option<Stat>,
}

impl HeartbeatAccumulator {
    pub fn into_mailbox_messages(self) -> Vec<MailboxMessage> {
        // TODO(jiachun): to HeartbeatResponse payload
        vec![]
    }
}

pub type Pusher = Sender<std::result::Result<HeartbeatResponse, tonic::Status>>;

#[derive(Clone, Default)]
pub struct HeartbeatHandlerGroup {
    handlers: Arc<RwLock<Vec<Box<dyn HeartbeatHandler>>>>,
    pushers: Arc<RwLock<BTreeMap<String, Pusher>>>,
}

impl HeartbeatHandlerGroup {
    pub async fn add_handler(&self, handler: impl HeartbeatHandler + 'static) {
        let mut handlers = self.handlers.write().await;
        handlers.push(Box::new(handler));
    }

    pub async fn register(&self, key: impl AsRef<str>, pusher: Pusher) {
        let mut pushers = self.pushers.write().await;
        let key = key.as_ref();
        info!("Pusher register: {}", key);
        pushers.insert(key.into(), pusher);
    }

    pub async fn unregister(&self, key: impl AsRef<str>) -> Option<Pusher> {
        let mut pushers = self.pushers.write().await;
        let key = key.as_ref();
        info!("Pusher unregister: {}", key);
        pushers.remove(key)
    }

    pub fn pushers(&self) -> Arc<RwLock<BTreeMap<String, Pusher>>> {
        self.pushers.clone()
    }

    pub async fn handle(
        &self,
        req: HeartbeatRequest,
        mut ctx: Context,
    ) -> Result<HeartbeatResponse> {
        let mut acc = HeartbeatAccumulator::default();
        let handlers = self.handlers.read().await;
        for h in handlers.iter() {
            if ctx.is_skip_all() {
                break;
            }

            let role = req
                .header
                .as_ref()
                .and_then(|h| Role::from_i32(h.role))
                .context(error::InvalidArgumentsSnafu {
                    err_msg: format!("invalid role: {:?}", req.header),
                })?;

            if h.is_acceptable(role) {
                h.handle(&req, &mut ctx, &mut acc).await?;
            }
        }
        let header = std::mem::take(&mut acc.header);
        let res = HeartbeatResponse {
            header,
            mailbox_messages: acc.into_mailbox_messages(),
        };
        Ok(res)
    }
}

pub struct HeartbeatMailbox {
    pushers: Arc<RwLock<BTreeMap<String, Pusher>>>,
    sequence: Sequence,
    senders: DashMap<MessageId, oneshot::Sender<Result<MailboxMessage>>>,
    timeouts: DashMap<MessageId, Duration>,
    timeout_notify: Notify,
}

impl HeartbeatMailbox {
    pub fn create(
        pushers: Arc<RwLock<BTreeMap<String, Pusher>>>,
        sequence: Sequence,
    ) -> MailboxRef {
        let mailbox = Arc::new(Self::new(pushers, sequence));

        let timeout_checker = mailbox.clone();
        common_runtime::spawn_bg(async move {
            timeout_checker.check_timeout_bg(1).await;
        });

        mailbox
    }

    fn new(pushers: Arc<RwLock<BTreeMap<String, Pusher>>>, sequence: Sequence) -> Self {
        Self {
            pushers,
            sequence,
            senders: DashMap::default(),
            timeouts: DashMap::default(),
            timeout_notify: Notify::new(),
        }
    }

    async fn check_timeout_bg(&self, interval_millis: u64) {
        let mut interval = tokio::time::interval(Duration::from_millis(interval_millis));

        loop {
            interval.tick().await;

            if self.timeouts.is_empty() {
                self.timeout_notify.notified().await;
            }

            let now = Duration::from_millis(common_time::util::current_time_millis() as u64);
            let timeout_ids = self
                .timeouts
                .iter()
                .filter_map(|entry| {
                    let (id, deadline) = entry.pair();
                    if deadline < &now {
                        Some(*id)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();

            for id in timeout_ids {
                let _ = self
                    .on_recv(id, Err(error::MailboxTimeoutSnafu { id }.build()))
                    .await;
            }
        }
    }
}

#[async_trait::async_trait]
impl Mailbox for HeartbeatMailbox {
    async fn send(
        &self,
        ch: &Channel,
        mut msg: MailboxMessage,
        timeout: Duration,
    ) -> Result<MailboxReceiver> {
        let message_id = self.sequence.next().await?;

        msg.id = message_id;
        let res = HeartbeatResponse {
            header: None,
            mailbox_messages: vec![msg],
        };

        let pusher_id = match ch {
            Channel::Datanode(id) => format!("{}-{}", Role::Datanode as i32, id),
            Channel::Frontend(id) => format!("{}-{}", Role::Frontend as i32, id),
        };
        let pushers = self.pushers.read().await;
        let pusher = pushers
            .get(&pusher_id)
            .context(error::PusherNotFoundSnafu { pusher_id })?;

        let (tx, rx) = oneshot::channel();
        self.senders.insert(message_id, tx);
        let deadline =
            Duration::from_millis(common_time::util::current_time_millis() as u64) + timeout;
        self.timeouts.insert(message_id, deadline);
        self.timeout_notify.notify_one();

        pusher.send(Ok(res)).await.map_err(|e| {
            error::PushMessageSnafu {
                err_msg: e.to_string(),
            }
            .build()
        })?;

        Ok(MailboxReceiver::new(message_id, rx))
    }

    async fn on_recv(&self, id: MessageId, maybe_msg: Result<MailboxMessage>) -> Result<()> {
        self.timeouts.remove(&id);

        if let Some((_, tx)) = self.senders.remove(&id) {
            tx.send(maybe_msg)
                .map_err(|_| error::MailboxClosedSnafu { id }.build())?;
        } else if let Ok(finally_msg) = maybe_msg {
            let MailboxMessage {
                id,
                subject,
                from,
                to,
                timestamp_millis,
                ..
            } = finally_msg;
            warn!("The response arrived too late, id={id}, subject={subject}, from={from}, to={to}, timestamp={timestamp_millis}");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use api::v1::meta::{MailboxMessage, Role};
    use tokio::sync::mpsc;

    use crate::handler::{HeartbeatHandlerGroup, HeartbeatMailbox, Pusher};
    use crate::sequence::Sequence;
    use crate::service::mailbox::{Channel, MailboxReceiver, MailboxRef};
    use crate::service::store::memory::MemStore;

    #[tokio::test]
    async fn test_mailbox() {
        let (mailbox, receiver) = push_msg_via_mailbox().await;
        let id = receiver.message_id();

        let resp_msg = MailboxMessage {
            id,
            subject: "resp-test".to_string(),
            timestamp_millis: 456,
            ..Default::default()
        };

        mailbox.on_recv(id, Ok(resp_msg)).await.unwrap();

        let recv_msg = receiver.await.unwrap().unwrap();
        assert_eq!(recv_msg.id, id);
        assert_eq!(recv_msg.timestamp_millis, 456);
        assert_eq!(recv_msg.subject, "resp-test".to_string());
    }

    #[tokio::test]
    async fn test_mailbox_timeout() {
        let (_, receiver) = push_msg_via_mailbox().await;
        let res = receiver.await.unwrap();
        assert!(res.is_err());
    }

    async fn push_msg_via_mailbox() -> (MailboxRef, MailboxReceiver) {
        let datanode_id = 12;
        let (pusher_tx, mut pusher_rx) = mpsc::channel(16);
        let pusher: Pusher = pusher_tx;
        let handler_group = HeartbeatHandlerGroup::default();
        handler_group
            .register(format!("{}-{}", Role::Datanode as i32, datanode_id), pusher)
            .await;

        let kv_store = Arc::new(MemStore::new());
        let seq = Sequence::new("test_seq", 0, 10, kv_store);
        let mailbox = HeartbeatMailbox::create(handler_group.pushers(), seq);

        let msg = MailboxMessage {
            id: 0,
            subject: "req-test".to_string(),
            timestamp_millis: 123,
            ..Default::default()
        };
        let ch = Channel::Datanode(datanode_id);

        let receiver = mailbox
            .send(&ch, msg, Duration::from_secs(1))
            .await
            .unwrap();

        let recv_obj = pusher_rx.recv().await.unwrap().unwrap();
        assert_eq!(recv_obj.mailbox_messages[0].timestamp_millis, 123);
        assert_eq!(recv_obj.mailbox_messages[0].subject, "req-test".to_string());

        (mailbox, receiver)
    }
}
