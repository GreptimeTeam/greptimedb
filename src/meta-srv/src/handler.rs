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

use std::collections::{BTreeMap, HashSet};
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

use api::v1::meta::mailbox_message::Payload;
use api::v1::meta::{
    HeartbeatRequest, HeartbeatResponse, MailboxMessage, RegionLease, RequestHeader,
    ResponseHeader, Role, PROTOCOL_VERSION,
};
use common_meta::instruction::{Instruction, InstructionReply};
use common_meta::sequence::Sequence;
use common_telemetry::{debug, info, warn};
use dashmap::DashMap;
use futures::future::join_all;
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;
use tokio::sync::mpsc::Sender;
use tokio::sync::{oneshot, Notify, RwLock};

use self::node_stat::Stat;
use crate::error::{self, DeserializeFromJsonSnafu, Result, UnexpectedInstructionReplySnafu};
use crate::metasrv::Context;
use crate::metrics::{METRIC_META_HANDLER_EXECUTE, METRIC_META_HEARTBEAT_CONNECTION_NUM};
use crate::service::mailbox::{
    BroadcastChannel, Channel, Mailbox, MailboxReceiver, MailboxRef, MessageId,
};

pub mod check_leader_handler;
pub mod collect_stats_handler;
pub mod failure_handler;
pub mod filter_inactive_region_stats;
pub mod keep_lease_handler;
pub mod mailbox_handler;
pub mod node_stat;
pub mod on_leader_start_handler;
pub mod persist_stats_handler;
pub mod publish_heartbeat_handler;
pub mod region_lease_handler;
pub mod response_header_handler;

#[async_trait::async_trait]
pub trait HeartbeatHandler: Send + Sync {
    fn is_acceptable(&self, role: Role) -> bool;

    fn name(&self) -> &'static str {
        let type_name = std::any::type_name::<Self>();
        // short name
        type_name.split("::").last().unwrap_or(type_name)
    }

    async fn handle(
        &self,
        req: &HeartbeatRequest,
        ctx: &mut Context,
        acc: &mut HeartbeatAccumulator,
    ) -> Result<HandleControl>;
}

/// HandleControl
///
/// Controls process of handling heartbeat request.
#[derive(PartialEq)]
pub enum HandleControl {
    Continue,
    Done,
}

#[derive(Debug, Default)]
pub struct HeartbeatAccumulator {
    pub header: Option<ResponseHeader>,
    pub instructions: Vec<Instruction>,
    pub stat: Option<Stat>,
    pub inactive_region_ids: HashSet<RegionId>,
    pub region_lease: Option<RegionLease>,
}

impl HeartbeatAccumulator {
    pub fn into_mailbox_message(self) -> Option<MailboxMessage> {
        // TODO(jiachun): to HeartbeatResponse payload
        None
    }
}

pub struct Pusher {
    sender: Sender<std::result::Result<HeartbeatResponse, tonic::Status>>,
    res_header: ResponseHeader,
}

impl Pusher {
    pub fn new(
        sender: Sender<std::result::Result<HeartbeatResponse, tonic::Status>>,
        req_header: &RequestHeader,
    ) -> Self {
        let res_header = ResponseHeader {
            protocol_version: PROTOCOL_VERSION,
            cluster_id: req_header.cluster_id,
            ..Default::default()
        };

        Self { sender, res_header }
    }

    #[inline]
    pub async fn push(&self, res: HeartbeatResponse) -> Result<()> {
        self.sender.send(Ok(res)).await.map_err(|e| {
            error::PushMessageSnafu {
                err_msg: e.to_string(),
            }
            .build()
        })
    }

    #[inline]
    pub fn header(&self) -> ResponseHeader {
        self.res_header.clone()
    }
}

#[derive(Clone, Default)]
pub struct Pushers(Arc<RwLock<BTreeMap<String, Pusher>>>);

impl Pushers {
    async fn push(&self, pusher_id: &str, mailbox_message: MailboxMessage) -> Result<()> {
        let pushers = self.0.read().await;
        let pusher = pushers
            .get(pusher_id)
            .context(error::PusherNotFoundSnafu { pusher_id })?;
        pusher
            .push(HeartbeatResponse {
                header: Some(pusher.header()),
                mailbox_message: Some(mailbox_message),
                ..Default::default()
            })
            .await
    }

    async fn broadcast(
        &self,
        range: Range<String>,
        mailbox_message: &MailboxMessage,
    ) -> Result<()> {
        let pushers = self.0.read().await;
        let pushers = pushers
            .range(range)
            .map(|(_, value)| value)
            .collect::<Vec<_>>();
        let mut results = Vec::with_capacity(pushers.len());

        for pusher in pushers {
            let mut mailbox_message = mailbox_message.clone();
            mailbox_message.id = 0; // one-way message

            results.push(pusher.push(HeartbeatResponse {
                header: Some(pusher.header()),
                mailbox_message: Some(mailbox_message),
                ..Default::default()
            }))
        }

        // Checks the error out of the loop.
        let _ = join_all(results)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        Ok(())
    }

    pub(crate) async fn insert(&self, pusher_id: String, pusher: Pusher) -> Option<Pusher> {
        self.0.write().await.insert(pusher_id, pusher)
    }

    async fn remove(&self, pusher_id: &str) -> Option<Pusher> {
        self.0.write().await.remove(pusher_id)
    }
}

struct NameCachedHandler {
    name: &'static str,
    handler: Box<dyn HeartbeatHandler>,
}

impl NameCachedHandler {
    fn new(handler: impl HeartbeatHandler + 'static) -> Self {
        let name = handler.name();
        let handler = Box::new(handler);
        Self { name, handler }
    }
}

#[derive(Clone, Default)]
pub struct HeartbeatHandlerGroup {
    handlers: Arc<RwLock<Vec<NameCachedHandler>>>,
    pushers: Pushers,
}

impl HeartbeatHandlerGroup {
    pub(crate) fn new(pushers: Pushers) -> Self {
        Self {
            handlers: Arc::new(RwLock::new(vec![])),
            pushers,
        }
    }

    pub async fn add_handler(&self, handler: impl HeartbeatHandler + 'static) {
        let mut handlers = self.handlers.write().await;
        handlers.push(NameCachedHandler::new(handler));
    }

    pub async fn register(&self, key: impl AsRef<str>, pusher: Pusher) {
        let key = key.as_ref();
        METRIC_META_HEARTBEAT_CONNECTION_NUM.inc();
        info!("Pusher register: {}", key);
        let _ = self.pushers.insert(key.to_string(), pusher).await;
    }

    pub async fn deregister(&self, key: impl AsRef<str>) -> Option<Pusher> {
        let key = key.as_ref();
        METRIC_META_HEARTBEAT_CONNECTION_NUM.dec();
        info!("Pusher unregister: {}", key);
        self.pushers.remove(key).await
    }

    pub fn pushers(&self) -> Pushers {
        self.pushers.clone()
    }

    pub async fn handle(
        &self,
        req: HeartbeatRequest,
        mut ctx: Context,
    ) -> Result<HeartbeatResponse> {
        let mut acc = HeartbeatAccumulator::default();
        let handlers = self.handlers.read().await;
        let role = req
            .header
            .as_ref()
            .and_then(|h| Role::try_from(h.role).ok())
            .context(error::InvalidArgumentsSnafu {
                err_msg: format!("invalid role: {:?}", req.header),
            })?;

        for NameCachedHandler { name, handler } in handlers.iter() {
            if !handler.is_acceptable(role) {
                continue;
            }

            let _timer = METRIC_META_HANDLER_EXECUTE
                .with_label_values(&[*name])
                .start_timer();

            if handler.handle(&req, &mut ctx, &mut acc).await? == HandleControl::Done {
                break;
            }
        }
        let header = std::mem::take(&mut acc.header);
        let res = HeartbeatResponse {
            header,
            region_lease: acc.region_lease,
            ..Default::default()
        };
        Ok(res)
    }
}

pub struct HeartbeatMailbox {
    pushers: Pushers,
    sequence: Sequence,
    senders: DashMap<MessageId, oneshot::Sender<Result<MailboxMessage>>>,
    timeouts: DashMap<MessageId, Instant>,
    timeout_notify: Notify,
}

impl HeartbeatMailbox {
    pub(crate) fn json_reply(msg: &MailboxMessage) -> Result<InstructionReply> {
        let Payload::Json(payload) =
            msg.payload
                .as_ref()
                .with_context(|| UnexpectedInstructionReplySnafu {
                    mailbox_message: msg.to_string(),
                    reason: format!("empty payload, msg: {msg:?}"),
                })?;
        serde_json::from_str(payload).context(DeserializeFromJsonSnafu { input: payload })
    }

    /// Parses the [Instruction] from [MailboxMessage].
    #[cfg(test)]
    pub(crate) fn json_instruction(msg: &MailboxMessage) -> Result<Instruction> {
        let Payload::Json(payload) =
            msg.payload
                .as_ref()
                .with_context(|| UnexpectedInstructionReplySnafu {
                    mailbox_message: msg.to_string(),
                    reason: format!("empty payload, msg: {msg:?}"),
                })?;
        serde_json::from_str(payload).context(DeserializeFromJsonSnafu { input: payload })
    }

    pub fn create(pushers: Pushers, sequence: Sequence) -> MailboxRef {
        let mailbox = Arc::new(Self::new(pushers, sequence));

        let timeout_checker = mailbox.clone();
        let _handle = common_runtime::spawn_bg(async move {
            timeout_checker.check_timeout_bg(10).await;
        });

        mailbox
    }

    fn new(pushers: Pushers, sequence: Sequence) -> Self {
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
            let _ = interval.tick().await;

            if self.timeouts.is_empty() {
                self.timeout_notify.notified().await;
            }

            let now = Instant::now();
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

    #[inline]
    async fn next_message_id(&self) -> Result<u64> {
        // In this implementation, we pre-occupy the message_id of 0,
        // and we use `message_id = 0` to mark a Message as a one-way call.
        loop {
            let next = self
                .sequence
                .next()
                .await
                .context(error::NextSequenceSnafu)?;
            if next > 0 {
                return Ok(next);
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
        let message_id = self.next_message_id().await?;
        msg.id = message_id;

        let pusher_id = ch.pusher_id();
        debug!("Sending mailbox message {msg:?} to {pusher_id}");

        let (tx, rx) = oneshot::channel();
        let _ = self.senders.insert(message_id, tx);
        let deadline = Instant::now() + timeout;
        let _ = self.timeouts.insert(message_id, deadline);
        self.timeout_notify.notify_one();

        self.pushers.push(&pusher_id, msg).await?;

        Ok(MailboxReceiver::new(message_id, rx, *ch))
    }

    async fn broadcast(&self, ch: &BroadcastChannel, msg: &MailboxMessage) -> Result<()> {
        self.pushers.broadcast(ch.pusher_range(), msg).await
    }

    async fn on_recv(&self, id: MessageId, maybe_msg: Result<MailboxMessage>) -> Result<()> {
        debug!("Received mailbox message {maybe_msg:?}");

        let _ = self.timeouts.remove(&id);

        if let Some((_, tx)) = self.senders.remove(&id) {
            tx.send(maybe_msg)
                .map_err(|_| error::MailboxClosedSnafu { id }.build())?;
        } else if let Ok(finally_msg) = maybe_msg {
            warn!("The response arrived too late: {finally_msg:?}");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;
    use std::time::Duration;

    use api::v1::meta::{MailboxMessage, RequestHeader, Role, PROTOCOL_VERSION};
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::sequence::SequenceBuilder;
    use tokio::sync::mpsc;

    use crate::handler::check_leader_handler::CheckLeaderHandler;
    use crate::handler::collect_stats_handler::CollectStatsHandler;
    use crate::handler::mailbox_handler::MailboxHandler;
    use crate::handler::on_leader_start_handler::OnLeaderStartHandler;
    use crate::handler::persist_stats_handler::PersistStatsHandler;
    use crate::handler::response_header_handler::ResponseHeaderHandler;
    use crate::handler::{HeartbeatHandlerGroup, HeartbeatMailbox, Pusher};
    use crate::service::mailbox::{Channel, MailboxReceiver, MailboxRef};

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
        let res_header = RequestHeader {
            protocol_version: PROTOCOL_VERSION,
            ..Default::default()
        };
        let pusher: Pusher = Pusher::new(pusher_tx, &res_header);
        let handler_group = HeartbeatHandlerGroup::default();
        handler_group
            .register(format!("{}-{}", Role::Datanode as i32, datanode_id), pusher)
            .await;

        let kv_backend = Arc::new(MemoryKvBackend::new());
        let seq = SequenceBuilder::new("test_seq", kv_backend).build();
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
        let message = recv_obj.mailbox_message.unwrap();
        assert_eq!(message.timestamp_millis, 123);
        assert_eq!(message.subject, "req-test".to_string());

        (mailbox, receiver)
    }

    #[tokio::test]
    async fn test_handler_name() {
        let group = HeartbeatHandlerGroup::default();
        group.add_handler(ResponseHeaderHandler).await;
        group.add_handler(CheckLeaderHandler).await;
        group.add_handler(OnLeaderStartHandler).await;
        group.add_handler(CollectStatsHandler).await;
        group.add_handler(MailboxHandler).await;
        group.add_handler(PersistStatsHandler::default()).await;

        let handlers = group.handlers.read().await;

        assert_eq!(6, handlers.len());
        assert_eq!("ResponseHeaderHandler", handlers[0].handler.name());
        assert_eq!("CheckLeaderHandler", handlers[1].handler.name());
        assert_eq!("OnLeaderStartHandler", handlers[2].handler.name());
        assert_eq!("CollectStatsHandler", handlers[3].handler.name());
        assert_eq!("MailboxHandler", handlers[4].handler.name());
        assert_eq!("PersistStatsHandler", handlers[5].handler.name());
    }
}
