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
use std::fmt::{Debug, Display};
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use api::v1::meta::mailbox_message::Payload;
use api::v1::meta::{
    HeartbeatRequest, HeartbeatResponse, MailboxMessage, RegionLease, ResponseHeader, Role,
    PROTOCOL_VERSION,
};
use check_leader_handler::CheckLeaderHandler;
use collect_cluster_info_handler::{
    CollectDatanodeClusterInfoHandler, CollectFlownodeClusterInfoHandler,
    CollectFrontendClusterInfoHandler,
};
use collect_leader_region_handler::CollectLeaderRegionHandler;
use collect_stats_handler::CollectStatsHandler;
use common_base::Plugins;
use common_meta::datanode::Stat;
use common_meta::instruction::{Instruction, InstructionReply};
use common_meta::sequence::Sequence;
use common_telemetry::{debug, info, warn};
use dashmap::DashMap;
use extract_stat_handler::ExtractStatHandler;
use failure_handler::RegionFailureHandler;
use filter_inactive_region_stats::FilterInactiveRegionStatsHandler;
use futures::future::join_all;
use keep_lease_handler::{DatanodeKeepLeaseHandler, FlownodeKeepLeaseHandler};
use mailbox_handler::MailboxHandler;
use on_leader_start_handler::OnLeaderStartHandler;
use publish_heartbeat_handler::PublishHeartbeatHandler;
use region_lease_handler::RegionLeaseHandler;
use remap_flow_peer_handler::RemapFlowPeerHandler;
use response_header_handler::ResponseHeaderHandler;
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;
use tokio::sync::mpsc::Sender;
use tokio::sync::{oneshot, watch, Notify, RwLock};

use crate::error::{self, DeserializeFromJsonSnafu, Result, UnexpectedInstructionReplySnafu};
use crate::handler::flow_state_handler::FlowStateHandler;
use crate::metasrv::Context;
use crate::metrics::{METRIC_META_HANDLER_EXECUTE, METRIC_META_HEARTBEAT_CONNECTION_NUM};
use crate::pubsub::PublisherRef;
use crate::service::mailbox::{
    BroadcastChannel, Channel, Mailbox, MailboxReceiver, MailboxRef, MessageId,
};

pub mod check_leader_handler;
pub mod collect_cluster_info_handler;
pub mod collect_leader_region_handler;
pub mod collect_stats_handler;
pub mod extract_stat_handler;
pub mod failure_handler;
pub mod filter_inactive_region_stats;
pub mod flow_state_handler;
pub mod keep_lease_handler;
pub mod mailbox_handler;
pub mod on_leader_start_handler;
pub mod publish_heartbeat_handler;
pub mod region_lease_handler;
pub mod remap_flow_peer_handler;
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
#[derive(PartialEq, Debug)]
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

#[derive(Copy, Clone)]
pub struct PusherId {
    pub role: Role,
    pub id: u64,
}

impl Debug for PusherId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}-{}", self.role, self.id)
    }
}

impl Display for PusherId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}-{}", self.role, self.id)
    }
}

impl PusherId {
    pub fn new(role: Role, id: u64) -> Self {
        Self { role, id }
    }

    pub fn string_key(&self) -> String {
        format!("{}-{}", self.role as i32, self.id)
    }
}

/// The receiver of the deregister signal.
pub type DeregisterSignalReceiver = watch::Receiver<bool>;

/// The pusher of the heartbeat response.
pub struct Pusher {
    sender: Sender<std::result::Result<HeartbeatResponse, tonic::Status>>,
    // The sender of the deregister signal.
    // default is false, means the pusher is not deregistered.
    // when the pusher is deregistered, the sender will be notified.
    deregister_signal_sender: watch::Sender<bool>,
    deregister_signal_receiver: DeregisterSignalReceiver,

    res_header: ResponseHeader,
}

impl Drop for Pusher {
    fn drop(&mut self) {
        // Ignore the error here.
        // if all the receivers have been dropped, means no body cares the deregister signal.
        let _ = self.deregister_signal_sender.send(true);
    }
}

impl Pusher {
    pub fn new(sender: Sender<std::result::Result<HeartbeatResponse, tonic::Status>>) -> Self {
        let res_header = ResponseHeader {
            protocol_version: PROTOCOL_VERSION,
            ..Default::default()
        };
        let (deregister_signal_sender, deregister_signal_receiver) = watch::channel(false);
        Self {
            sender,
            deregister_signal_sender,
            deregister_signal_receiver,
            res_header,
        }
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

/// The group of heartbeat pushers.
#[derive(Clone, Default)]
pub struct Pushers(Arc<RwLock<BTreeMap<String, Pusher>>>);

impl Pushers {
    async fn push(
        &self,
        pusher_id: PusherId,
        mailbox_message: MailboxMessage,
    ) -> Result<DeregisterSignalReceiver> {
        let pusher_id = pusher_id.string_key();
        let pushers = self.0.read().await;
        let pusher = pushers
            .get(&pusher_id)
            .context(error::PusherNotFoundSnafu { pusher_id })?;

        pusher
            .push(HeartbeatResponse {
                header: Some(pusher.header()),
                mailbox_message: Some(mailbox_message),
                ..Default::default()
            })
            .await?;

        Ok(pusher.deregister_signal_receiver.clone())
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

#[derive(Clone)]
pub struct NameCachedHandler {
    name: &'static str,
    handler: Arc<dyn HeartbeatHandler>,
}

impl NameCachedHandler {
    fn new(handler: impl HeartbeatHandler + 'static) -> Self {
        let name = handler.name();
        let handler = Arc::new(handler);
        Self { name, handler }
    }
}

pub type HeartbeatHandlerGroupRef = Arc<HeartbeatHandlerGroup>;

/// The group of heartbeat handlers.
#[derive(Default, Clone)]
pub struct HeartbeatHandlerGroup {
    handlers: Vec<NameCachedHandler>,
    pushers: Pushers,
}

impl HeartbeatHandlerGroup {
    /// Registers the heartbeat response [`Pusher`] with the given key to the group.
    pub async fn register_pusher(&self, pusher_id: PusherId, pusher: Pusher) {
        METRIC_META_HEARTBEAT_CONNECTION_NUM.inc();
        info!("Pusher register: {}", pusher_id);
        let _ = self.pushers.insert(pusher_id.string_key(), pusher).await;
    }

    /// Deregisters the heartbeat response [`Pusher`] with the given key from the group.
    ///
    /// Returns the [`Pusher`] if it exists.
    pub async fn deregister_push(&self, pusher_id: PusherId) -> Option<Pusher> {
        METRIC_META_HEARTBEAT_CONNECTION_NUM.dec();
        info!("Pusher unregister: {}", pusher_id);
        self.pushers.remove(&pusher_id.string_key()).await
    }

    /// Returns the [`Pushers`] of the group.
    pub fn pushers(&self) -> Pushers {
        self.pushers.clone()
    }

    /// Handles the heartbeat request.
    pub async fn handle(
        &self,
        req: HeartbeatRequest,
        mut ctx: Context,
    ) -> Result<HeartbeatResponse> {
        let mut acc = HeartbeatAccumulator::default();
        let role = req
            .header
            .as_ref()
            .and_then(|h| Role::try_from(h.role).ok())
            .context(error::InvalidArgumentsSnafu {
                err_msg: format!("invalid role: {:?}", req.header),
            })?;

        for NameCachedHandler { name, handler } in self.handlers.iter() {
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
    pub fn json_reply(msg: &MailboxMessage) -> Result<InstructionReply> {
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
        let _handle = common_runtime::spawn_global(async move {
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
        self.timeouts.insert(message_id, deadline);
        self.timeout_notify.notify_one();

        let deregister_signal_receiver = self.pushers.push(pusher_id, msg).await?;

        Ok(MailboxReceiver::new(
            message_id,
            rx,
            deregister_signal_receiver,
            *ch,
        ))
    }

    async fn send_oneway(&self, ch: &Channel, mut msg: MailboxMessage) -> Result<()> {
        let message_id = 0; // one-way message, same as `broadcast`
        msg.id = message_id;

        let pusher_id = ch.pusher_id();
        debug!("Sending mailbox message {msg:?} to {pusher_id}");

        self.pushers.push(pusher_id, msg).await?;

        Ok(())
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

/// The builder to build the group of heartbeat handlers.
pub struct HeartbeatHandlerGroupBuilder {
    /// The handler to handle region failure.
    region_failure_handler: Option<RegionFailureHandler>,

    /// The handler to handle region lease.
    region_lease_handler: Option<RegionLeaseHandler>,

    /// The factor that determines how often statistics should be flushed,
    /// based on the number of received heartbeats. When the number of heartbeats
    /// reaches this factor, a flush operation is triggered.
    flush_stats_factor: Option<usize>,
    /// A simple handler for flow internal state report
    flow_state_handler: Option<FlowStateHandler>,

    /// The plugins.
    plugins: Option<Plugins>,

    /// The heartbeat response pushers.
    pushers: Pushers,

    /// The group of heartbeat handlers.
    handlers: Vec<NameCachedHandler>,
}

impl HeartbeatHandlerGroupBuilder {
    pub fn new(pushers: Pushers) -> Self {
        Self {
            region_failure_handler: None,
            region_lease_handler: None,
            flush_stats_factor: None,
            flow_state_handler: None,
            plugins: None,
            pushers,
            handlers: vec![],
        }
    }

    pub fn with_flow_state_handler(mut self, handler: Option<FlowStateHandler>) -> Self {
        self.flow_state_handler = handler;
        self
    }

    pub fn with_region_lease_handler(mut self, handler: Option<RegionLeaseHandler>) -> Self {
        self.region_lease_handler = handler;
        self
    }

    /// Sets the [`RegionFailureHandler`].
    pub fn with_region_failure_handler(mut self, handler: Option<RegionFailureHandler>) -> Self {
        self.region_failure_handler = handler;
        self
    }

    /// Sets the flush stats factor.
    pub fn with_flush_stats_factor(mut self, flush_stats_factor: Option<usize>) -> Self {
        self.flush_stats_factor = flush_stats_factor;
        self
    }

    /// Sets the [`Plugins`].
    pub fn with_plugins(mut self, plugins: Option<Plugins>) -> Self {
        self.plugins = plugins;
        self
    }

    /// Adds the default handlers.
    pub fn add_default_handlers(mut self) -> Self {
        // Extract the `PublishHeartbeatHandler` from the plugins.
        let publish_heartbeat_handler = if let Some(plugins) = self.plugins.as_ref() {
            plugins
                .get::<PublisherRef>()
                .map(|publish| PublishHeartbeatHandler::new(publish.clone()))
        } else {
            None
        };

        self.add_handler_last(ResponseHeaderHandler);
        // `KeepLeaseHandler` should preferably be in front of `CheckLeaderHandler`,
        // because even if the current meta-server node is no longer the leader it can
        // still help the datanode to keep lease.
        self.add_handler_last(DatanodeKeepLeaseHandler);
        self.add_handler_last(FlownodeKeepLeaseHandler);
        self.add_handler_last(CheckLeaderHandler);
        self.add_handler_last(OnLeaderStartHandler);
        self.add_handler_last(ExtractStatHandler);
        self.add_handler_last(CollectDatanodeClusterInfoHandler);
        self.add_handler_last(CollectFrontendClusterInfoHandler);
        self.add_handler_last(CollectFlownodeClusterInfoHandler);
        self.add_handler_last(MailboxHandler);
        if let Some(region_lease_handler) = self.region_lease_handler.take() {
            self.add_handler_last(region_lease_handler);
        }
        self.add_handler_last(FilterInactiveRegionStatsHandler);
        if let Some(region_failure_handler) = self.region_failure_handler.take() {
            self.add_handler_last(region_failure_handler);
        }
        if let Some(publish_heartbeat_handler) = publish_heartbeat_handler {
            self.add_handler_last(publish_heartbeat_handler);
        }
        self.add_handler_last(CollectLeaderRegionHandler);
        self.add_handler_last(CollectStatsHandler::new(self.flush_stats_factor));
        self.add_handler_last(RemapFlowPeerHandler::default());

        if let Some(flow_state_handler) = self.flow_state_handler.take() {
            self.add_handler_last(flow_state_handler);
        }

        self
    }

    /// Builds the group of heartbeat handlers.
    ///
    /// Applies the customizer if it exists.
    pub fn build(mut self) -> Result<HeartbeatHandlerGroup> {
        if let Some(customizer) = self
            .plugins
            .as_ref()
            .and_then(|plugins| plugins.get::<HeartbeatHandlerGroupBuilderCustomizerRef>())
        {
            debug!("Customizing the heartbeat handler group builder");
            customizer.customize(&mut self)?;
        }

        Ok(HeartbeatHandlerGroup {
            handlers: self.handlers,
            pushers: self.pushers,
        })
    }

    fn add_handler_after_inner(&mut self, target: &str, handler: NameCachedHandler) -> Result<()> {
        if let Some(pos) = self.handlers.iter().position(|x| x.name == target) {
            self.handlers.insert(pos + 1, handler);
            return Ok(());
        }

        error::HandlerNotFoundSnafu { name: target }.fail()
    }

    /// Adds the handler after the specified handler.
    pub fn add_handler_after(
        &mut self,
        target: &'static str,
        handler: impl HeartbeatHandler + 'static,
    ) -> Result<()> {
        self.add_handler_after_inner(target, NameCachedHandler::new(handler))
    }

    fn add_handler_before_inner(&mut self, target: &str, handler: NameCachedHandler) -> Result<()> {
        if let Some(pos) = self.handlers.iter().position(|x| x.name == target) {
            self.handlers.insert(pos, handler);
            return Ok(());
        }

        error::HandlerNotFoundSnafu { name: target }.fail()
    }

    /// Adds the handler before the specified handler.
    pub fn add_handler_before(
        &mut self,
        target: &'static str,
        handler: impl HeartbeatHandler + 'static,
    ) -> Result<()> {
        self.add_handler_before_inner(target, NameCachedHandler::new(handler))
    }

    fn replace_handler_inner(&mut self, target: &str, handler: NameCachedHandler) -> Result<()> {
        if let Some(pos) = self.handlers.iter().position(|x| x.name == target) {
            self.handlers[pos] = handler;
            return Ok(());
        }

        error::HandlerNotFoundSnafu { name: target }.fail()
    }

    /// Replaces the handler with the specified name.
    pub fn replace_handler(
        &mut self,
        target: &'static str,
        handler: impl HeartbeatHandler + 'static,
    ) -> Result<()> {
        self.replace_handler_inner(target, NameCachedHandler::new(handler))
    }

    fn add_handler_last_inner(&mut self, handler: NameCachedHandler) {
        self.handlers.push(handler);
    }

    fn add_handler_last(&mut self, handler: impl HeartbeatHandler + 'static) {
        self.add_handler_last_inner(NameCachedHandler::new(handler));
    }
}

pub type HeartbeatHandlerGroupBuilderCustomizerRef =
    Arc<dyn HeartbeatHandlerGroupBuilderCustomizer>;

pub enum CustomizeHeartbeatGroupAction {
    AddHandlerAfter {
        target: String,
        handler: NameCachedHandler,
    },
    AddHandlerBefore {
        target: String,
        handler: NameCachedHandler,
    },
    ReplaceHandler {
        target: String,
        handler: NameCachedHandler,
    },
    AddHandlerLast {
        handler: NameCachedHandler,
    },
}

impl CustomizeHeartbeatGroupAction {
    pub fn new_add_handler_after(
        target: &'static str,
        handler: impl HeartbeatHandler + 'static,
    ) -> Self {
        Self::AddHandlerAfter {
            target: target.to_string(),
            handler: NameCachedHandler::new(handler),
        }
    }

    pub fn new_add_handler_before(
        target: &'static str,
        handler: impl HeartbeatHandler + 'static,
    ) -> Self {
        Self::AddHandlerBefore {
            target: target.to_string(),
            handler: NameCachedHandler::new(handler),
        }
    }

    pub fn new_replace_handler(
        target: &'static str,
        handler: impl HeartbeatHandler + 'static,
    ) -> Self {
        Self::ReplaceHandler {
            target: target.to_string(),
            handler: NameCachedHandler::new(handler),
        }
    }

    pub fn new_add_handler_last(handler: impl HeartbeatHandler + 'static) -> Self {
        Self::AddHandlerLast {
            handler: NameCachedHandler::new(handler),
        }
    }
}

/// The customizer of the [`HeartbeatHandlerGroupBuilder`].
pub trait HeartbeatHandlerGroupBuilderCustomizer: Send + Sync {
    fn customize(&self, builder: &mut HeartbeatHandlerGroupBuilder) -> Result<()>;

    fn add_action(&self, action: CustomizeHeartbeatGroupAction);
}

#[derive(Default)]
pub struct DefaultHeartbeatHandlerGroupBuilderCustomizer {
    actions: Mutex<Vec<CustomizeHeartbeatGroupAction>>,
}

impl HeartbeatHandlerGroupBuilderCustomizer for DefaultHeartbeatHandlerGroupBuilderCustomizer {
    fn customize(&self, builder: &mut HeartbeatHandlerGroupBuilder) -> Result<()> {
        info!("Customizing the heartbeat handler group builder");
        let mut actions = self.actions.lock().unwrap();
        for action in actions.drain(..) {
            match action {
                CustomizeHeartbeatGroupAction::AddHandlerAfter { target, handler } => {
                    builder.add_handler_after_inner(&target, handler)?;
                }
                CustomizeHeartbeatGroupAction::AddHandlerBefore { target, handler } => {
                    builder.add_handler_before_inner(&target, handler)?;
                }
                CustomizeHeartbeatGroupAction::ReplaceHandler { target, handler } => {
                    builder.replace_handler_inner(&target, handler)?;
                }
                CustomizeHeartbeatGroupAction::AddHandlerLast { handler } => {
                    builder.add_handler_last_inner(handler);
                }
            }
        }
        Ok(())
    }

    fn add_action(&self, action: CustomizeHeartbeatGroupAction) {
        self.actions.lock().unwrap().push(action);
    }
}

#[cfg(test)]
mod tests {

    use std::assert_matches::assert_matches;
    use std::sync::Arc;
    use std::time::Duration;

    use api::v1::meta::{MailboxMessage, Role};
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::sequence::SequenceBuilder;
    use tokio::sync::mpsc;

    use super::{HeartbeatHandlerGroupBuilder, PusherId, Pushers};
    use crate::error;
    use crate::handler::collect_stats_handler::CollectStatsHandler;
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

        let recv_msg = receiver.await.unwrap();
        assert_eq!(recv_msg.id, id);
        assert_eq!(recv_msg.timestamp_millis, 456);
        assert_eq!(recv_msg.subject, "resp-test".to_string());
    }

    #[tokio::test]
    async fn test_mailbox_timeout() {
        let (_, receiver) = push_msg_via_mailbox().await;
        let res = receiver.await;
        assert!(res.is_err());
    }

    async fn push_msg_via_mailbox() -> (MailboxRef, MailboxReceiver) {
        let datanode_id = 12;
        let (pusher_tx, mut pusher_rx) = mpsc::channel(16);
        let pusher_id = PusherId::new(Role::Datanode, datanode_id);
        let pusher: Pusher = Pusher::new(pusher_tx);
        let handler_group = HeartbeatHandlerGroup::default();
        handler_group.register_pusher(pusher_id, pusher).await;

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

    #[test]
    fn test_handler_group_builder() {
        let group = HeartbeatHandlerGroupBuilder::new(Pushers::default())
            .add_default_handlers()
            .build()
            .unwrap();

        let handlers = group.handlers;
        assert_eq!(14, handlers.len());

        let names = [
            "ResponseHeaderHandler",
            "DatanodeKeepLeaseHandler",
            "FlownodeKeepLeaseHandler",
            "CheckLeaderHandler",
            "OnLeaderStartHandler",
            "ExtractStatHandler",
            "CollectDatanodeClusterInfoHandler",
            "CollectFrontendClusterInfoHandler",
            "CollectFlownodeClusterInfoHandler",
            "MailboxHandler",
            "FilterInactiveRegionStatsHandler",
            "CollectLeaderRegionHandler",
            "CollectStatsHandler",
            "RemapFlowPeerHandler",
        ];

        for (handler, name) in handlers.iter().zip(names.into_iter()) {
            assert_eq!(handler.name, name);
        }
    }

    #[test]
    fn test_handler_group_builder_add_before() {
        let mut builder =
            HeartbeatHandlerGroupBuilder::new(Pushers::default()).add_default_handlers();
        builder
            .add_handler_before(
                "FilterInactiveRegionStatsHandler",
                CollectStatsHandler::default(),
            )
            .unwrap();

        let group = builder.build().unwrap();
        let handlers = group.handlers;
        assert_eq!(15, handlers.len());

        let names = [
            "ResponseHeaderHandler",
            "DatanodeKeepLeaseHandler",
            "FlownodeKeepLeaseHandler",
            "CheckLeaderHandler",
            "OnLeaderStartHandler",
            "ExtractStatHandler",
            "CollectDatanodeClusterInfoHandler",
            "CollectFrontendClusterInfoHandler",
            "CollectFlownodeClusterInfoHandler",
            "MailboxHandler",
            "CollectStatsHandler",
            "FilterInactiveRegionStatsHandler",
            "CollectLeaderRegionHandler",
            "CollectStatsHandler",
            "RemapFlowPeerHandler",
        ];

        for (handler, name) in handlers.iter().zip(names.into_iter()) {
            assert_eq!(handler.name, name);
        }
    }

    #[test]
    fn test_handler_group_builder_add_before_first() {
        let mut builder =
            HeartbeatHandlerGroupBuilder::new(Pushers::default()).add_default_handlers();
        builder
            .add_handler_before("ResponseHeaderHandler", CollectStatsHandler::default())
            .unwrap();

        let group = builder.build().unwrap();
        let handlers = group.handlers;
        assert_eq!(15, handlers.len());

        let names = [
            "CollectStatsHandler",
            "ResponseHeaderHandler",
            "DatanodeKeepLeaseHandler",
            "FlownodeKeepLeaseHandler",
            "CheckLeaderHandler",
            "OnLeaderStartHandler",
            "ExtractStatHandler",
            "CollectDatanodeClusterInfoHandler",
            "CollectFrontendClusterInfoHandler",
            "CollectFlownodeClusterInfoHandler",
            "MailboxHandler",
            "FilterInactiveRegionStatsHandler",
            "CollectLeaderRegionHandler",
            "CollectStatsHandler",
            "RemapFlowPeerHandler",
        ];

        for (handler, name) in handlers.iter().zip(names.into_iter()) {
            assert_eq!(handler.name, name);
        }
    }

    #[test]
    fn test_handler_group_builder_add_after() {
        let mut builder =
            HeartbeatHandlerGroupBuilder::new(Pushers::default()).add_default_handlers();
        builder
            .add_handler_after("MailboxHandler", CollectStatsHandler::default())
            .unwrap();

        let group = builder.build().unwrap();
        let handlers = group.handlers;
        assert_eq!(15, handlers.len());

        let names = [
            "ResponseHeaderHandler",
            "DatanodeKeepLeaseHandler",
            "FlownodeKeepLeaseHandler",
            "CheckLeaderHandler",
            "OnLeaderStartHandler",
            "ExtractStatHandler",
            "CollectDatanodeClusterInfoHandler",
            "CollectFrontendClusterInfoHandler",
            "CollectFlownodeClusterInfoHandler",
            "MailboxHandler",
            "CollectStatsHandler",
            "FilterInactiveRegionStatsHandler",
            "CollectLeaderRegionHandler",
            "CollectStatsHandler",
            "RemapFlowPeerHandler",
        ];

        for (handler, name) in handlers.iter().zip(names.into_iter()) {
            assert_eq!(handler.name, name);
        }
    }

    #[test]
    fn test_handler_group_builder_add_after_last() {
        let mut builder =
            HeartbeatHandlerGroupBuilder::new(Pushers::default()).add_default_handlers();
        builder
            .add_handler_after("CollectStatsHandler", ResponseHeaderHandler)
            .unwrap();

        let group = builder.build().unwrap();
        let handlers = group.handlers;
        assert_eq!(15, handlers.len());

        let names = [
            "ResponseHeaderHandler",
            "DatanodeKeepLeaseHandler",
            "FlownodeKeepLeaseHandler",
            "CheckLeaderHandler",
            "OnLeaderStartHandler",
            "ExtractStatHandler",
            "CollectDatanodeClusterInfoHandler",
            "CollectFrontendClusterInfoHandler",
            "CollectFlownodeClusterInfoHandler",
            "MailboxHandler",
            "FilterInactiveRegionStatsHandler",
            "CollectLeaderRegionHandler",
            "CollectStatsHandler",
            "ResponseHeaderHandler",
            "RemapFlowPeerHandler",
        ];

        for (handler, name) in handlers.iter().zip(names.into_iter()) {
            assert_eq!(handler.name, name);
        }
    }

    #[test]
    fn test_handler_group_builder_replace() {
        let mut builder =
            HeartbeatHandlerGroupBuilder::new(Pushers::default()).add_default_handlers();
        builder
            .replace_handler("MailboxHandler", CollectStatsHandler::default())
            .unwrap();

        let group = builder.build().unwrap();
        let handlers = group.handlers;
        assert_eq!(14, handlers.len());

        let names = [
            "ResponseHeaderHandler",
            "DatanodeKeepLeaseHandler",
            "FlownodeKeepLeaseHandler",
            "CheckLeaderHandler",
            "OnLeaderStartHandler",
            "ExtractStatHandler",
            "CollectDatanodeClusterInfoHandler",
            "CollectFrontendClusterInfoHandler",
            "CollectFlownodeClusterInfoHandler",
            "CollectStatsHandler",
            "FilterInactiveRegionStatsHandler",
            "CollectLeaderRegionHandler",
            "CollectStatsHandler",
            "RemapFlowPeerHandler",
        ];

        for (handler, name) in handlers.iter().zip(names.into_iter()) {
            assert_eq!(handler.name, name);
        }
    }

    #[test]
    fn test_handler_group_builder_replace_last() {
        let mut builder =
            HeartbeatHandlerGroupBuilder::new(Pushers::default()).add_default_handlers();
        builder
            .replace_handler("CollectStatsHandler", ResponseHeaderHandler)
            .unwrap();

        let group = builder.build().unwrap();
        let handlers = group.handlers;
        assert_eq!(14, handlers.len());

        let names = [
            "ResponseHeaderHandler",
            "DatanodeKeepLeaseHandler",
            "FlownodeKeepLeaseHandler",
            "CheckLeaderHandler",
            "OnLeaderStartHandler",
            "ExtractStatHandler",
            "CollectDatanodeClusterInfoHandler",
            "CollectFrontendClusterInfoHandler",
            "CollectFlownodeClusterInfoHandler",
            "MailboxHandler",
            "FilterInactiveRegionStatsHandler",
            "CollectLeaderRegionHandler",
            "ResponseHeaderHandler",
            "RemapFlowPeerHandler",
        ];

        for (handler, name) in handlers.iter().zip(names.into_iter()) {
            assert_eq!(handler.name, name);
        }
    }

    #[test]
    fn test_handler_group_builder_replace_first() {
        let mut builder =
            HeartbeatHandlerGroupBuilder::new(Pushers::default()).add_default_handlers();
        builder
            .replace_handler("ResponseHeaderHandler", CollectStatsHandler::default())
            .unwrap();

        let group = builder.build().unwrap();
        let handlers = group.handlers;
        assert_eq!(14, handlers.len());

        let names = [
            "CollectStatsHandler",
            "DatanodeKeepLeaseHandler",
            "FlownodeKeepLeaseHandler",
            "CheckLeaderHandler",
            "OnLeaderStartHandler",
            "ExtractStatHandler",
            "CollectDatanodeClusterInfoHandler",
            "CollectFrontendClusterInfoHandler",
            "CollectFlownodeClusterInfoHandler",
            "MailboxHandler",
            "FilterInactiveRegionStatsHandler",
            "CollectLeaderRegionHandler",
            "CollectStatsHandler",
            "RemapFlowPeerHandler",
        ];

        for (handler, name) in handlers.iter().zip(names.into_iter()) {
            assert_eq!(handler.name, name);
        }
    }

    #[test]
    fn test_handler_group_builder_handler_not_found() {
        let mut builder =
            HeartbeatHandlerGroupBuilder::new(Pushers::default()).add_default_handlers();
        let err = builder
            .add_handler_before("NotExists", CollectStatsHandler::default())
            .unwrap_err();
        assert_matches!(err, error::Error::HandlerNotFound { .. });

        let err = builder
            .add_handler_after("NotExists", CollectStatsHandler::default())
            .unwrap_err();
        assert_matches!(err, error::Error::HandlerNotFound { .. });

        let err = builder
            .replace_handler("NotExists", CollectStatsHandler::default())
            .unwrap_err();
        assert_matches!(err, error::Error::HandlerNotFound { .. });
    }

    #[tokio::test]
    async fn test_pusher_drop() {
        let (tx, _rx) = mpsc::channel(1);
        let pusher = Pusher::new(tx);
        let mut deregister_signal_tx = pusher.deregister_signal_receiver.clone();

        drop(pusher);
        deregister_signal_tx.changed().await.unwrap();
    }
}
