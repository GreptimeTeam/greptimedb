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

//! Fences stale datanode processes by their `node_epoch`.
//!
//! A datanode process reports the wall-clock time it started as `node_epoch`. The leader keeps
//! the highest epoch of each datanode in the kv backend, and in memory the highest one it has
//! read or written there in its current term. It answers a datanode request that carries an
//! epoch from memory, unless the epoch is higher or the node has no epoch in memory yet: then
//! it reads the kv backend, uncached, and raises the record when the epoch is higher. A lower
//! epoch belongs to a process that a newer process with the same node id has replaced, so its
//! requests get no grant, whichever metasrv they reach.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use api::v1::meta::{HeartbeatRequest, Role};
use common_meta::DatanodeId;
use common_meta::datanode::{NodeEpochFence, STARTUP_HANDSHAKE_KEY};
use common_meta::election::{ElectionRef, LeaderChangeMessage};
use common_meta::key::MetadataValue;
use common_meta::key::datanode_epoch::{DatanodeEpochKey, DatanodeEpochValue};
use common_meta::kv_backend::KvBackendRef;
use common_meta::rpc::store::CompareAndPutRequest;
use common_telemetry::{info, warn};
use common_time::util::current_time_millis;
use snafu::ResultExt;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::TryRecvError;

use crate::error::{self, Result};
use crate::metasrv::Context;
use crate::metrics::{METRIC_META_NODE_EPOCH_CHECK, METRIC_META_NODE_EPOCH_CHECK_ELAPSED};

/// The timeout of reading, and raising, a datanode's epoch in the kv backend. It is shorter
/// than the datanode's handshake timeout.
const NODE_EPOCH_CHECK_TIMEOUT: Duration = Duration::from_secs(2);

/// How far ahead of the leader's wall clock an epoch can be recorded. Recording an epoch from
/// a clock that jumped further ahead would fence every later process with the node id.
const MAX_NODE_EPOCH_AHEAD: Duration = Duration::from_secs(5 * 60);

pub(crate) type NodeEpochCacheRef = Arc<NodeEpochCache>;

/// The highest epoch of each datanode that the leader has read or written in the kv backend
/// in its current term.
///
/// It follows the leader changes of the election itself rather than the metasrv's leader
/// change loop, so a slow leader change callback cannot delay it. It is cleared on every
/// change, and used only while the last change it has received elected this metasrv. The
/// election reports this metasrv as the leader before it sends the change, so a check that
/// sees the leadership first may find the cache not yet in use, but never filled before the
/// change.
pub(crate) struct NodeEpochCache {
    inner: Mutex<NodeEpochCacheInner>,
}

impl Default for NodeEpochCache {
    fn default() -> Self {
        Self::new(None)
    }
}

#[derive(Default)]
struct NodeEpochCacheInner {
    /// The leader changes of the election; `None` without an election.
    leader_changes: Option<broadcast::Receiver<LeaderChangeMessage>>,
    /// Whether the last leader change received elected this metasrv. Always true without an
    /// election.
    elected: bool,
    /// Grows on every leader change, so a kv read started before a change cannot fill the cache
    /// after it.
    term: u64,
    epochs: HashMap<DatanodeId, u64>,
}

impl NodeEpochCacheInner {
    /// Clears the cache if the election has sent a leader change since the last call.
    fn observe_leader_changes(&mut self) {
        let Some(leader_changes) = &mut self.leader_changes else {
            return;
        };
        let mut changed = false;
        loop {
            match leader_changes.try_recv() {
                Ok(msg) => self.elected = matches!(msg, LeaderChangeMessage::Elected(_)),
                // The changes that follow tell the current leadership.
                Err(TryRecvError::Lagged(_)) => self.elected = false,
                Err(TryRecvError::Empty) | Err(TryRecvError::Closed) => break,
            }
            changed = true;
        }
        if changed {
            self.term += 1;
            self.epochs.clear();
        }
    }
}

impl NodeEpochCache {
    /// Creates a cache that follows the leader changes of `election`.
    pub(crate) fn new(election: Option<&ElectionRef>) -> Self {
        Self {
            inner: Mutex::new(NodeEpochCacheInner {
                leader_changes: election.map(|election| election.subscribe_leader_change()),
                elected: election.is_none(),
                ..Default::default()
            }),
        }
    }

    /// Returns the current term and the highest epoch of `node_id` in it.
    fn get(&self, node_id: DatanodeId) -> (u64, Option<u64>) {
        let mut inner = self.inner.lock().unwrap();
        inner.observe_leader_changes();
        let highest = if inner.elected {
            inner.epochs.get(&node_id).copied()
        } else {
            None
        };
        (inner.term, highest)
    }

    /// Records `node_epoch`, read or written in the kv backend in `term`, as the highest epoch
    /// of `node_id` if it is higher, no leader change has happened since, and the last one
    /// elected this metasrv.
    fn fill(&self, term: u64, node_id: DatanodeId, node_epoch: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.observe_leader_changes();
        if inner.elected && inner.term == term {
            let highest = inner.epochs.entry(node_id).or_default();
            *highest = (*highest).max(node_epoch);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NodeEpochCheck {
    /// Not a datanode request, or a datanode request without an epoch: the handshake of a
    /// datanode that does not report its epoch on handshakes, or the placeholder a datanode
    /// sends before it reconnects.
    Unchecked,
    /// This metasrv is not the leader, so the epoch was not checked.
    NotLeader,
    /// The epoch was checked against the highest one in memory or in the kv backend.
    Checked(NodeEpochFence),
}

/// Returns whether `req` is a datanode's startup handshake.
pub(crate) fn is_startup_handshake(req: &HeartbeatRequest, is_handshake: bool) -> bool {
    is_handshake && req.extensions.contains_key(STARTUP_HANDSHAKE_KEY)
}

/// Checks the `node_epoch` of a datanode request against the highest epoch recorded for its
/// node id, raising the record first when the request's epoch is higher.
pub(crate) async fn check_node_epoch(
    req: &HeartbeatRequest,
    role: Role,
    node_id: u64,
    ctx: &Context,
) -> NodeEpochCheck {
    if role != Role::Datanode {
        return NodeEpochCheck::Unchecked;
    }
    let request = request_kind(req, ctx.is_handshake);
    if req.node_epoch == 0 {
        METRIC_META_NODE_EPOCH_CHECK
            .with_label_values(&[request, "no_epoch"])
            .inc();
        return NodeEpochCheck::Unchecked;
    }
    // Checks leadership before reading the epoch, so a metasrv that becomes the leader while
    // it handles the request cannot grant without checking the epoch.
    if let Some(election) = &ctx.election
        && !election.is_leader()
    {
        return NodeEpochCheck::NotLeader;
    }

    let node_epoch = req.node_epoch;
    let (term, cached) = ctx.node_epoch_cache.get(node_id);
    let fence = match cached.and_then(|highest| compare_epoch(node_id, node_epoch, highest)) {
        Some(fence) => fence,
        None => {
            let result = {
                let _timer = METRIC_META_NODE_EPOCH_CHECK_ELAPSED.start_timer();
                tokio::time::timeout(
                    NODE_EPOCH_CHECK_TIMEOUT,
                    fence_node_epoch(&ctx.kv_backend, node_id, node_epoch),
                )
                .await
            };
            match result {
                Ok(Ok((fence, highest))) => {
                    ctx.node_epoch_cache.fill(term, node_id, highest);
                    fence
                }
                Ok(Err(err)) => {
                    warn!(err; "Failed to check epoch {} of datanode {}", node_epoch, node_id);
                    NodeEpochFence::Unverified
                }
                Err(_) => {
                    warn!(
                        "Timed out checking epoch {} of datanode {} after {:?}",
                        node_epoch, node_id, NODE_EPOCH_CHECK_TIMEOUT
                    );
                    NodeEpochFence::Unverified
                }
            }
        }
    };
    METRIC_META_NODE_EPOCH_CHECK
        .with_label_values(&[request, fence.as_str()])
        .inc();
    NodeEpochCheck::Checked(fence)
}

fn request_kind(req: &HeartbeatRequest, is_handshake: bool) -> &'static str {
    if is_startup_handshake(req, is_handshake) {
        "startup_handshake"
    } else if is_handshake {
        "handshake"
    } else if req.mailbox_message.is_some() {
        "mailbox"
    } else {
        "heartbeat"
    }
}

/// Compares `node_epoch` with the highest epoch of `node_id`. Returns `None` if it is higher.
fn compare_epoch(node_id: u64, node_epoch: u64, highest: u64) -> Option<NodeEpochFence> {
    match node_epoch.cmp(&highest) {
        Ordering::Less => {
            warn!(
                "Fenced datanode {} with epoch {}: a newer process with epoch {} has been accepted",
                node_id, node_epoch, highest
            );
            Some(NodeEpochFence::Stale)
        }
        Ordering::Equal => Some(NodeEpochFence::Accepted),
        Ordering::Greater => None,
    }
}

/// Checks `node_epoch` against the highest epoch of `node_id` in the kv backend, raising it
/// first when `node_epoch` is higher. Returns the result and the highest epoch afterwards.
async fn fence_node_epoch(
    kv_backend: &KvBackendRef,
    node_id: u64,
    node_epoch: u64,
) -> Result<(NodeEpochFence, u64)> {
    let key = DatanodeEpochKey::new(node_id).to_bytes();
    let mut current = kv_backend.get(&key).await.context(error::KvBackendSnafu)?;
    loop {
        let (expect, highest) = match current {
            Some(kv) => {
                let value = DatanodeEpochValue::try_from_raw_value(&kv.value)
                    .context(error::KvBackendSnafu)?;
                (kv.value, value.node_epoch)
            }
            None => (vec![], 0),
        };
        if let Some(fence) = compare_epoch(node_id, node_epoch, highest) {
            return Ok((fence, highest));
        }

        let limit = (current_time_millis().max(0) as u64)
            .saturating_add(MAX_NODE_EPOCH_AHEAD.as_millis() as u64);
        if node_epoch > limit {
            warn!(
                "Refused to record epoch {} of datanode {}: it is more than {:?} ahead of this metasrv's clock",
                node_epoch, node_id, MAX_NODE_EPOCH_AHEAD
            );
            return Ok((NodeEpochFence::ClockAhead, highest));
        }

        let value = DatanodeEpochValue { node_epoch }
            .try_as_raw_value()
            .context(error::KvBackendSnafu)?;
        let resp = kv_backend
            .compare_and_put(CompareAndPutRequest {
                key: key.clone(),
                expect,
                value,
            })
            .await
            .context(error::KvBackendSnafu)?;
        if resp.success {
            info!(
                "Raised the epoch of datanode {} from {} to {}",
                node_id, highest, node_epoch
            );
            return Ok((NodeEpochFence::Accepted, node_epoch));
        }
        current = resp.prev_kv;
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::any::Any;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    use api::v1::meta::{
        HeartbeatRequest, HeartbeatResponse, MailboxMessage, Peer, RequestHeader, Role,
    };
    use common_meta::datanode::{NodeEpochFence, STARTUP_HANDSHAKE_KEY};
    use common_meta::election::{
        Election, ElectionRef, LeaderChangeMessage, LeaderKey, LeaderValue,
    };
    use common_meta::key::datanode_epoch::{DatanodeEpochKey, DatanodeEpochValue};
    use common_meta::key::{MetadataValue, TableMetadataManager};
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::kv_backend::txn::{Txn, TxnResponse, TxnService};
    use common_meta::kv_backend::{KvBackend, KvBackendRef};
    use common_meta::region_keeper::MemoryRegionKeeper;
    use common_meta::rpc::KeyValue;
    use common_meta::rpc::store::{
        BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse,
        BatchPutRequest, BatchPutResponse, CompareAndPutRequest, CompareAndPutResponse,
        DeleteRangeRequest, DeleteRangeResponse, PutRequest, PutResponse, RangeRequest,
        RangeResponse,
    };
    use common_meta::sequence::SequenceBuilder;
    use common_time::util::current_time_millis;
    use tokio::sync::{Notify, broadcast, mpsc};

    use super::NodeEpochCache;
    use crate::discovery::utils::find_datanode_lease_value;
    use crate::handler::failure_handler::RegionFailureHandler;
    use crate::handler::region_lease_handler::RegionLeaseHandler;
    use crate::handler::test_utils::TestEnv;
    use crate::handler::{
        HandleControl, HeartbeatAccumulator, HeartbeatHandler, HeartbeatHandlerGroup,
        HeartbeatHandlerGroupBuilder, HeartbeatMailbox, Pusher, PusherId, Pushers,
    };
    use crate::metasrv::Context;
    use crate::region::supervisor::tests::new_test_supervisor;
    use crate::region::supervisor::{Event, HeartbeatAcceptor, RegionSupervisor};
    use crate::service::mailbox::{Channel, MailboxReceiver};

    pub(crate) const NODE_ID: u64 = 1;

    /// A memory kv backend whose reads can be slowed down, failed or held after they read,
    /// and whose compare-and-put can be held. It counts reads and compare-and-puts.
    #[derive(Default)]
    pub(crate) struct HookedKvBackend {
        inner: MemoryKvBackend<common_meta::error::Error>,
        reads: AtomicUsize,
        writes: AtomicUsize,
        read_delay: std::sync::Mutex<Option<Duration>>,
        fail_reads: AtomicBool,
        /// Holds the next read after it has read: notifies the first, waits for the second.
        pub(crate) hold_next_read: std::sync::Mutex<Option<(Arc<Notify>, Arc<Notify>)>>,
        cas_entered: Notify,
        cas_gate: std::sync::Mutex<Option<Arc<Notify>>>,
    }

    #[async_trait::async_trait]
    impl TxnService for HookedKvBackend {
        type Error = common_meta::error::Error;

        async fn txn(&self, txn: Txn) -> common_meta::error::Result<TxnResponse> {
            self.inner.txn(txn).await
        }

        fn max_txn_ops(&self) -> usize {
            self.inner.max_txn_ops()
        }
    }

    #[async_trait::async_trait]
    impl KvBackend for HookedKvBackend {
        fn name(&self) -> &str {
            "hooked"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        async fn range(&self, req: RangeRequest) -> common_meta::error::Result<RangeResponse> {
            self.inner.range(req).await
        }

        async fn put(&self, req: PutRequest) -> common_meta::error::Result<PutResponse> {
            self.inner.put(req).await
        }

        async fn batch_put(
            &self,
            req: BatchPutRequest,
        ) -> common_meta::error::Result<BatchPutResponse> {
            self.inner.batch_put(req).await
        }

        async fn batch_get(
            &self,
            req: BatchGetRequest,
        ) -> common_meta::error::Result<BatchGetResponse> {
            self.inner.batch_get(req).await
        }

        async fn delete_range(
            &self,
            req: DeleteRangeRequest,
        ) -> common_meta::error::Result<DeleteRangeResponse> {
            self.inner.delete_range(req).await
        }

        async fn batch_delete(
            &self,
            req: BatchDeleteRequest,
        ) -> common_meta::error::Result<BatchDeleteResponse> {
            self.inner.batch_delete(req).await
        }

        async fn get(&self, key: &[u8]) -> common_meta::error::Result<Option<KeyValue>> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            let delay = *self.read_delay.lock().unwrap();
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            if self.fail_reads.load(Ordering::Relaxed) {
                return common_meta::error::UnexpectedSnafu {
                    err_msg: "injected read failure",
                }
                .fail();
            }
            let value = self.inner.get(key).await;
            let hold = self.hold_next_read.lock().unwrap().take();
            if let Some((entered, release)) = hold {
                entered.notify_one();
                release.notified().await;
            }
            value
        }

        async fn compare_and_put(
            &self,
            req: CompareAndPutRequest,
        ) -> common_meta::error::Result<CompareAndPutResponse> {
            self.writes.fetch_add(1, Ordering::Relaxed);
            self.cas_entered.notify_one();
            let gate = self.cas_gate.lock().unwrap().clone();
            if let Some(gate) = gate {
                gate.notified().await;
            }
            self.inner.compare_and_put(req).await
        }
    }

    #[derive(Debug)]
    struct TestLeaderKey;

    impl LeaderKey for TestLeaderKey {
        fn name(&self) -> &[u8] {
            b"test"
        }

        fn key(&self) -> &[u8] {
            b"test"
        }

        fn revision(&self) -> i64 {
            0
        }

        fn lease_id(&self) -> i64 {
            0
        }
    }

    /// An election whose leadership the test flips.
    struct TestElection {
        leader: AtomicBool,
        /// Becomes the leader right after the next leadership check, without a leader change.
        promote_after_check: AtomicBool,
        /// Steps down right after the next leadership check.
        step_down_after_check: AtomicBool,
        leader_changes: broadcast::Sender<LeaderChangeMessage>,
    }

    impl Default for TestElection {
        fn default() -> Self {
            Self {
                leader: AtomicBool::default(),
                promote_after_check: AtomicBool::default(),
                step_down_after_check: AtomicBool::default(),
                leader_changes: broadcast::channel(16).0,
            }
        }
    }

    impl TestElection {
        /// Sets the leadership, then sends the leader change, as the elections do.
        fn set_leader(&self, leader: bool) {
            self.leader.store(leader, Ordering::Relaxed);
            let key = Arc::new(TestLeaderKey);
            let _ = self.leader_changes.send(if leader {
                LeaderChangeMessage::Elected(key)
            } else {
                LeaderChangeMessage::StepDown(key)
            });
        }
    }

    #[async_trait::async_trait]
    impl Election for TestElection {
        type Leader = LeaderValue;

        fn is_leader(&self) -> bool {
            let leader = self.leader.load(Ordering::Relaxed);
            if self.promote_after_check.swap(false, Ordering::Relaxed) {
                self.leader.store(true, Ordering::Relaxed);
            }
            if self.step_down_after_check.swap(false, Ordering::Relaxed) {
                self.set_leader(false);
            }
            leader
        }

        fn in_leader_infancy(&self) -> bool {
            false
        }

        async fn register_candidate(
            &self,
            _: &common_meta::election::MetasrvNodeInfo,
        ) -> common_meta::error::Result<()> {
            Ok(())
        }

        async fn all_candidates(
            &self,
        ) -> common_meta::error::Result<Vec<common_meta::election::MetasrvNodeInfo>> {
            Ok(vec![])
        }

        async fn campaign(&self) -> common_meta::error::Result<()> {
            Ok(())
        }

        async fn leader(&self) -> common_meta::error::Result<Self::Leader> {
            Ok(LeaderValue("127.0.0.1:3002".to_string()))
        }

        async fn resign(&self) -> common_meta::error::Result<()> {
            Ok(())
        }

        fn subscribe_leader_change(&self) -> broadcast::Receiver<LeaderChangeMessage> {
            self.leader_changes.subscribe()
        }
    }

    fn new_group_builder() -> HeartbeatHandlerGroupBuilder {
        let lease_handler = RegionLeaseHandler::new(
            10,
            Arc::new(TableMetadataManager::new(Arc::new(MemoryKvBackend::new()))),
            Arc::new(MemoryRegionKeeper::default()),
            None,
        );
        HeartbeatHandlerGroupBuilder::new(Pushers::default())
            .with_region_lease_handler(Some(lease_handler))
    }

    pub(crate) fn new_group() -> HeartbeatHandlerGroup {
        new_group_builder().add_default_handlers().build().unwrap()
    }

    /// A metasrv context: each one has its own in-memory state.
    fn new_ctx(kv_backend: KvBackendRef, election: Option<ElectionRef>) -> Context {
        let mut ctx = TestEnv::new().ctx();
        ctx.kv_backend = kv_backend;
        ctx.node_epoch_cache = Arc::new(NodeEpochCache::new(election.as_ref()));
        ctx.election = election;
        ctx
    }

    fn header(role: Role) -> Option<RequestHeader> {
        Some(RequestHeader {
            role: role.into(),
            member_id: NODE_ID,
            ..Default::default()
        })
    }

    pub(crate) fn handshake(node_epoch: u64) -> HeartbeatRequest {
        HeartbeatRequest {
            header: header(Role::Datanode),
            node_epoch,
            ..Default::default()
        }
    }

    pub(crate) fn startup_handshake(node_epoch: u64) -> HeartbeatRequest {
        let mut req = handshake(node_epoch);
        req.extensions
            .insert(STARTUP_HANDSHAKE_KEY.to_string(), vec![]);
        req
    }

    pub(crate) fn heartbeat(node_epoch: u64, addr: &str) -> HeartbeatRequest {
        HeartbeatRequest {
            header: header(Role::Datanode),
            peer: Some(Peer {
                id: NODE_ID,
                addr: addr.to_string(),
            }),
            node_epoch,
            ..Default::default()
        }
    }

    fn mailbox_reply(node_epoch: u64, id: u64) -> HeartbeatRequest {
        HeartbeatRequest {
            mailbox_message: Some(MailboxMessage {
                id,
                ..Default::default()
            }),
            ..heartbeat(node_epoch, "a")
        }
    }

    pub(crate) fn fence(res: &HeartbeatResponse) -> Option<&str> {
        res.extensions
            .get(NodeEpochFence::NODE_EPOCH_FENCE_KEY)
            .map(|value| std::str::from_utf8(value).unwrap())
    }

    impl HookedKvBackend {
        /// Returns the reads and compare-and-puts so far.
        fn ops(&self) -> (usize, usize) {
            (
                self.reads.load(Ordering::Relaxed),
                self.writes.load(Ordering::Relaxed),
            )
        }

        async fn record(&self, node_epoch: u64) {
            self.inner
                .put(PutRequest {
                    key: DatanodeEpochKey::new(NODE_ID).to_bytes(),
                    value: DatanodeEpochValue { node_epoch }
                        .try_as_raw_value()
                        .unwrap(),
                    ..Default::default()
                })
                .await
                .unwrap();
        }
    }

    async fn recorded_epoch(kv_backend: &KvBackendRef) -> Option<u64> {
        kv_backend
            .get(&DatanodeEpochKey::new(NODE_ID).to_bytes())
            .await
            .unwrap()
            .map(|kv| {
                DatanodeEpochValue::try_from_raw_value(&kv.value)
                    .unwrap()
                    .node_epoch
            })
    }

    async fn lease_addr(ctx: &Context) -> Option<String> {
        let in_memory = ctx.in_memory.clone() as KvBackendRef;
        find_datanode_lease_value(&in_memory, NODE_ID)
            .await
            .unwrap()
            .map(|lease| lease.node_addr)
    }

    /// Replaces the context's mailbox with one that waits for a reply from the datanode.
    async fn wait_for_reply(ctx: &mut Context) -> MailboxReceiver {
        let pushers = Pushers::default();
        let (tx, _rx) = mpsc::channel(8);
        pushers
            .insert(PusherId::new(Role::Datanode, NODE_ID), Pusher::new(tx))
            .await;
        let seq = SequenceBuilder::new("test_seq", Arc::new(MemoryKvBackend::new())).build();
        ctx.mailbox = HeartbeatMailbox::create(pushers, seq);
        ctx.mailbox
            .send(
                &Channel::Datanode(NODE_ID),
                MailboxMessage::default(),
                Duration::from_millis(500),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_stale_requests_get_no_grant() {
        let kv_backend: KvBackendRef = Arc::new(MemoryKvBackend::new());
        let group = new_group();
        let mut ctx = new_ctx(kv_backend.clone(), None);
        let now = current_time_millis() as u64;
        let (e1, e2) = (now - 1000, now);

        // A2 records the higher epoch before it is answered.
        let res = group
            .handle(heartbeat(e2, "a2"), ctx.clone())
            .await
            .unwrap();
        assert_eq!(fence(&res), Some("accepted"));
        assert!(res.region_lease.is_some());
        assert_eq!(recorded_epoch(&kv_backend).await, Some(e2));

        // A1's handshake, heartbeat and mailbox reply are all rejected.
        let res = group
            .handle(handshake(e1), ctx.clone().with_handshake(true))
            .await
            .unwrap();
        assert_eq!(fence(&res), Some("stale"));
        assert!(res.heartbeat_config.is_some());
        assert!(res.header.unwrap().error.is_none());

        let res = group
            .handle(heartbeat(e1, "a1"), ctx.clone())
            .await
            .unwrap();
        assert_eq!(fence(&res), Some("stale"));
        assert!(res.region_lease.is_none());
        // A1 does not keep the node alive at its address.
        assert_eq!(lease_addr(&ctx).await.as_deref(), Some("a2"));

        let receiver = wait_for_reply(&mut ctx).await;
        let res = group
            .handle(mailbox_reply(e1, receiver.message_id()), ctx.clone())
            .await
            .unwrap();
        assert_eq!(fence(&res), Some("stale"));
        assert!(receiver.await.is_err());

        // A2's mailbox reply is delivered.
        let receiver = wait_for_reply(&mut ctx).await;
        group
            .handle(mailbox_reply(e2, receiver.message_id()), ctx.clone())
            .await
            .unwrap();
        assert!(receiver.await.is_ok());
        assert_eq!(recorded_epoch(&kv_backend).await, Some(e2));
    }

    #[tokio::test]
    async fn test_epoch_far_ahead_is_not_recorded() {
        let kv_backend: KvBackendRef = Arc::new(MemoryKvBackend::new());
        let group = new_group();
        let ctx = new_ctx(kv_backend.clone(), None);
        let now = current_time_millis() as u64;

        let ahead = now + Duration::from_secs(10 * 60).as_millis() as u64;
        let res = group
            .handle(heartbeat(ahead, "a"), ctx.clone())
            .await
            .unwrap();
        assert_eq!(fence(&res), Some("clock_ahead"));
        assert!(res.region_lease.is_none());
        assert_eq!(recorded_epoch(&kv_backend).await, None);
        // It does not keep the node alive either, so failure detection can act on the node.
        assert_eq!(lease_addr(&ctx).await, None);

        let slightly_ahead = now + Duration::from_secs(60).as_millis() as u64;
        let res = group
            .handle(heartbeat(slightly_ahead, "a"), ctx)
            .await
            .unwrap();
        assert_eq!(fence(&res), Some("accepted"));
        assert_eq!(recorded_epoch(&kv_backend).await, Some(slightly_ahead));
    }

    #[tokio::test]
    async fn test_only_datanodes_are_checked() {
        let kv_backend = Arc::new(HookedKvBackend::default());
        let group = new_group();
        let ctx = new_ctx(kv_backend.clone(), None);

        for role in [Role::Flownode, Role::Frontend] {
            let req = HeartbeatRequest {
                header: header(role),
                node_epoch: 1,
                ..Default::default()
            };
            let res = group.handle(req, ctx.clone()).await.unwrap();
            assert_eq!(fence(&res), None);
        }
        assert_eq!(kv_backend.reads.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_epoch_is_read_from_kv_once_per_leader_term() {
        let now = current_time_millis() as u64;
        let (e0, e1, e2) = (now - 2000, now - 1000, now);
        let group = new_group();
        let kv_backend = Arc::new(HookedKvBackend::default());
        let election = Arc::new(TestElection::default());
        let ctx = new_ctx(kv_backend.clone(), Some(election.clone()));
        election.set_leader(true);

        // The first request of the node in the term reads the kv backend, and records the
        // epoch since there is none.
        let res = group
            .handle(heartbeat(e1, "a1"), ctx.clone())
            .await
            .unwrap();
        assert_eq!(fence(&res), Some("accepted"));
        assert_eq!(kv_backend.ops(), (1, 1));

        // Equal and lower epochs are answered from memory.
        for _ in 0..3 {
            let res = group
                .handle(heartbeat(e1, "a1"), ctx.clone())
                .await
                .unwrap();
            assert_eq!(fence(&res), Some("accepted"));
            assert!(res.region_lease.is_some());
        }
        let res = group
            .handle(mailbox_reply(e1, 1), ctx.clone())
            .await
            .unwrap();
        assert_eq!(fence(&res), Some("accepted"));
        let res = group
            .handle(heartbeat(e0, "a0"), ctx.clone())
            .await
            .unwrap();
        assert_eq!(fence(&res), Some("stale"));
        assert_eq!(kv_backend.ops(), (1, 1));

        // A higher epoch reads the record and raises it.
        let res = group
            .handle(heartbeat(e2, "a2"), ctx.clone())
            .await
            .unwrap();
        assert_eq!(fence(&res), Some("accepted"));
        assert_eq!(kv_backend.ops(), (2, 2));
        let res = group
            .handle(heartbeat(e1, "a1"), ctx.clone())
            .await
            .unwrap();
        assert_eq!(fence(&res), Some("stale"));
        let res = group
            .handle(heartbeat(e2, "a2"), ctx.clone())
            .await
            .unwrap();
        assert_eq!(fence(&res), Some("accepted"));
        assert_eq!(kv_backend.ops(), (2, 2));

        // Stepping down and being elected again clears the memory, so the next request reads
        // the kv backend again.
        election.set_leader(false);
        election.set_leader(true);
        let res = group
            .handle(heartbeat(e1, "a1"), ctx.clone())
            .await
            .unwrap();
        assert_eq!(fence(&res), Some("stale"));
        let res = group.handle(heartbeat(e2, "a2"), ctx).await.unwrap();
        assert_eq!(fence(&res), Some("accepted"));
        assert_eq!(kv_backend.ops(), (3, 2));
    }

    #[tokio::test]
    async fn test_epoch_is_read_again_after_leadership_moves() {
        let now = current_time_millis() as u64;
        let (e1, e2) = (now - 1000, now);
        let (m1_group, m2_group) = (new_group(), new_group());

        // M1 accepts A1, steps down while M2 accepts A2, and is elected again. No leader change
        // callback of the metasrv runs here: M1's memory is cleared by the leader changes it
        // receives from the election itself.
        let kv_backend = Arc::new(HookedKvBackend::default());
        let election = Arc::new(TestElection::default());
        let m1 = new_ctx(kv_backend.clone(), Some(election.clone()));
        let m2 = new_ctx(kv_backend.clone(), None);
        election.set_leader(true);
        let res = m1_group
            .handle(heartbeat(e1, "a1"), m1.clone())
            .await
            .unwrap();
        assert_eq!(fence(&res), Some("accepted"));
        election.set_leader(false);
        let res = m1_group
            .handle(heartbeat(e1, "a1"), m1.clone())
            .await
            .unwrap();
        assert!(res.is_not_leader());
        let res = m2_group.handle(heartbeat(e2, "a2"), m2).await.unwrap();
        assert_eq!(fence(&res), Some("accepted"));
        election.set_leader(true);
        let res = m1_group.handle(heartbeat(e1, "a1"), m1).await.unwrap();
        assert_eq!(fence(&res), Some("stale"));
        assert!(res.region_lease.is_none());
        // One read per leader term, none on the follower.
        assert_eq!(kv_backend.ops(), (3, 2));
    }

    #[tokio::test]
    async fn test_read_from_an_earlier_term_does_not_fill_memory() {
        let now = current_time_millis() as u64;
        let (e1, e2) = (now - 1000, now);
        let group = Arc::new(new_group());
        let kv_backend = Arc::new(HookedKvBackend::default());
        let election = Arc::new(TestElection::default());
        let ctx = new_ctx(kv_backend.clone(), Some(election.clone()));
        election.set_leader(true);
        kv_backend.record(e1).await;

        // A1's check reads e1, then waits while this metasrv steps down, another leader
        // accepts A2, and this metasrv is elected again.
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        *kv_backend.hold_next_read.lock().unwrap() = Some((entered.clone(), release.clone()));
        let a1 = tokio::spawn({
            let group = group.clone();
            let ctx = ctx.clone();
            async move { group.handle(heartbeat(e1, "a1"), ctx).await.unwrap() }
        });
        entered.notified().await;
        election.set_leader(false);
        kv_backend.record(e2).await;
        election.set_leader(true);
        release.notify_one();
        a1.await.unwrap();

        // The read from the earlier term is not kept, so A1's next request reads A2's epoch.
        let res = group.handle(heartbeat(e1, "a1"), ctx).await.unwrap();
        assert_eq!(fence(&res), Some("stale"));
        assert!(res.region_lease.is_none());
        assert_eq!(kv_backend.ops(), (2, 0));
    }

    #[tokio::test]
    async fn test_follower_does_not_fill_memory_for_a_later_term() {
        let now = current_time_millis() as u64;
        let (e1, e2) = (now - 1000, now);
        let (m1_group, m2_group) = (new_group(), new_group());
        let kv_backend = Arc::new(HookedKvBackend::default());
        let election = Arc::new(TestElection::default());
        let m1 = new_ctx(kv_backend.clone(), Some(election.clone()));
        let m2 = new_ctx(kv_backend.clone(), None);
        election.set_leader(true);
        kv_backend.record(e1).await;

        // A1's check sees M1 as the leader, then M1 steps down before the check reads memory,
        // so the check reads e1 from the kv backend as a follower.
        election
            .step_down_after_check
            .store(true, Ordering::Relaxed);
        m1_group
            .handle(heartbeat(e1, "a1"), m1.clone())
            .await
            .unwrap();
        let res = m2_group.handle(heartbeat(e2, "a2"), m2).await.unwrap();
        assert_eq!(fence(&res), Some("accepted"));

        // M1 is elected again, and its election reports the leadership before it sends the
        // change. A1 is checked against the kv backend, not against what M1 read as a follower.
        election.leader.store(true, Ordering::Relaxed);
        let res = m1_group
            .handle(heartbeat(e1, "a1"), m1.clone())
            .await
            .unwrap();
        assert_eq!(fence(&res), Some("stale"));
        assert_eq!(kv_backend.ops(), (3, 1));

        // Memory is used once the change arrives.
        election.set_leader(true);
        for _ in 0..2 {
            let res = m1_group
                .handle(heartbeat(e1, "a1"), m1.clone())
                .await
                .unwrap();
            assert_eq!(fence(&res), Some("stale"));
        }
        assert_eq!(kv_backend.ops(), (4, 1));
    }

    #[tokio::test]
    async fn test_reply_waits_for_the_epoch_write() {
        let kv_backend = Arc::new(HookedKvBackend::default());
        let gate = Arc::new(Notify::new());
        *kv_backend.cas_gate.lock().unwrap() = Some(gate.clone());
        let group = Arc::new(new_group());
        let ctx = new_ctx(kv_backend.clone(), None);
        let now = current_time_millis() as u64;
        let (e1, e2) = (now - 1000, now);
        kv_backend.record(e1).await;

        let a2 = tokio::spawn({
            let group = group.clone();
            let ctx = ctx.clone().with_handshake(true);
            async move { group.handle(startup_handshake(e2), ctx).await.unwrap() }
        });
        kv_backend.cas_entered.notified().await;
        assert!(!a2.is_finished());

        // Until the write commits, A1 is granted as before. Such a grant is anchored before
        // A2 is answered, so it expires before A2 stops waiting.
        let res = group
            .handle(heartbeat(e1, "a1"), ctx.clone())
            .await
            .unwrap();
        assert_eq!(fence(&res), Some("accepted"));
        assert!(res.region_lease.is_some());

        gate.notify_one();
        let res = a2.await.unwrap();
        assert_eq!(fence(&res), Some("accepted"));

        // A2's epoch is in memory once A2 is answered.
        let res = group.handle(heartbeat(e1, "a1"), ctx).await.unwrap();
        assert_eq!(fence(&res), Some("stale"));
        assert_eq!(kv_backend.ops(), (2, 1));
        assert_eq!(recorded_epoch(&(kv_backend as _)).await, Some(e2));
    }

    #[tokio::test]
    async fn test_follower_does_not_read_kv() {
        let kv_backend = Arc::new(HookedKvBackend::default());
        let group = new_group();
        let election = Arc::new(TestElection::default());
        let ctx = new_ctx(kv_backend.clone(), Some(election.clone()));
        let epoch = current_time_millis() as u64;

        let res = group
            .handle(heartbeat(epoch, "a"), ctx.clone())
            .await
            .unwrap();
        assert!(res.is_not_leader());
        assert!(res.region_lease.is_none());

        // Becoming the leader while the request is handled does not grant it.
        election.promote_after_check.store(true, Ordering::Relaxed);
        let res = group
            .handle(heartbeat(epoch, "a"), ctx.clone())
            .await
            .unwrap();
        assert!(res.is_not_leader());
        assert!(res.region_lease.is_none());
        assert_eq!(lease_addr(&ctx).await, None);
        assert_eq!(kv_backend.reads.load(Ordering::Relaxed), 0);

        let res = group.handle(heartbeat(epoch, "a"), ctx).await.unwrap();
        assert_eq!(fence(&res), Some("accepted"));
        assert_eq!(kv_backend.reads.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_unverified_request_keeps_stats_but_gets_no_grant() {
        let kv_backend = Arc::new(HookedKvBackend::default());
        kv_backend.fail_reads.store(true, Ordering::Relaxed);
        let (supervisor, _) = new_test_supervisor();
        let (tx, mut rx) = RegionSupervisor::channel();
        let group = new_group_builder()
            .with_region_failure_handler(Some(RegionFailureHandler::new(
                supervisor,
                HeartbeatAcceptor::new(tx),
            )))
            .add_default_handlers()
            .build()
            .unwrap();
        let mut ctx = new_ctx(kv_backend, None);
        let epoch = current_time_millis() as u64;

        let res = group
            .handle(heartbeat(epoch, "a"), ctx.clone())
            .await
            .unwrap();
        assert_eq!(fence(&res), Some("unverified"));
        assert!(res.region_lease.is_none());
        // Liveness and stats still reach the failure detection.
        assert_eq!(lease_addr(&ctx).await.as_deref(), Some("a"));
        assert!(matches!(rx.try_recv(), Ok(Event::HeartbeatArrived(_))));

        let receiver = wait_for_reply(&mut ctx).await;
        let res = group
            .handle(mailbox_reply(epoch, receiver.message_id()), ctx)
            .await
            .unwrap();
        assert_eq!(fence(&res), Some("unverified"));
        assert!(res.region_lease.is_none());
        assert!(receiver.await.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn test_slow_kv_reads() {
        let kv_backend = Arc::new(HookedKvBackend::default());
        let group = new_group();
        let ctx = new_ctx(kv_backend.clone(), None);
        let epoch = current_time_millis() as u64;

        *kv_backend.read_delay.lock().unwrap() = Some(Duration::from_millis(1500));
        let res = group
            .handle(heartbeat(epoch, "a"), ctx.clone())
            .await
            .unwrap();
        assert_eq!(fence(&res), Some("accepted"));
        assert!(res.region_lease.is_some());

        *kv_backend.read_delay.lock().unwrap() = Some(Duration::from_secs(3600));
        let res = group
            .handle(heartbeat(epoch + 1, "b"), ctx.clone())
            .await
            .unwrap();
        assert_eq!(fence(&res), Some("unverified"));
        assert!(res.region_lease.is_none());
        assert_eq!(lease_addr(&ctx).await.as_deref(), Some("b"));

        // A request answered from memory does not wait for the kv backend.
        let res = group
            .handle(heartbeat(epoch, "a"), ctx.clone())
            .await
            .unwrap();
        assert_eq!(fence(&res), Some("accepted"));
        assert!(res.region_lease.is_some());
        assert_eq!(kv_backend.ops(), (2, 1));
    }

    #[tokio::test]
    async fn test_replaced_handlers_are_still_fenced() {
        struct GrantAll;

        #[async_trait::async_trait]
        impl HeartbeatHandler for GrantAll {
            fn is_acceptable(&self, _: Role) -> bool {
                true
            }

            async fn handle(
                &self,
                _: &HeartbeatRequest,
                _: &mut Context,
                acc: &mut HeartbeatAccumulator,
            ) -> crate::error::Result<HandleControl> {
                acc.region_lease = Some(Default::default());
                Ok(HandleControl::Done)
            }
        }

        let mut builder = new_group_builder().add_default_handlers();
        builder
            .replace_handler("RegionLeaseHandler", GrantAll)
            .unwrap();
        let group = builder.build().unwrap();
        let kv_backend = Arc::new(HookedKvBackend::default());
        let ctx = new_ctx(kv_backend.clone(), None);
        let now = current_time_millis() as u64;

        let res = group
            .handle(heartbeat(now, "a2"), ctx.clone())
            .await
            .unwrap();
        assert!(res.region_lease.is_some());
        let res = group
            .handle(heartbeat(now - 1, "a1"), ctx.clone())
            .await
            .unwrap();
        assert_eq!(fence(&res), Some("stale"));
        assert!(res.region_lease.is_none());

        kv_backend.fail_reads.store(true, Ordering::Relaxed);
        let res = group.handle(heartbeat(now + 1, "a3"), ctx).await.unwrap();
        assert_eq!(fence(&res), Some("unverified"));
        assert!(res.region_lease.is_none());
    }

    /// Runs the fence across two metasrvs, each with its own client of `kv_backend`.
    async fn fence_across_metasrvs(m1_kv: KvBackendRef, m2_kv: KvBackendRef) {
        let key = DatanodeEpochKey::new(NODE_ID).to_bytes();
        m1_kv.delete(&key, false).await.unwrap();
        let (m1_group, m2_group) = (new_group(), new_group());
        let m1 = new_ctx(m1_kv.clone(), None);
        let m2_election = Arc::new(TestElection::default());
        let m2 = new_ctx(m2_kv, Some(m2_election.clone()));
        m2_election.set_leader(true);
        let now = current_time_millis() as u64;
        let (e1, e2) = (now - 1000, now);

        // M2 accepts A1, then steps down before A2's startup handshake is accepted on M1.
        let res = m2_group
            .handle(heartbeat(e1, "a1"), m2.clone())
            .await
            .unwrap();
        assert_eq!(fence(&res), Some("accepted"));
        m2_election.set_leader(false);
        let res = m1_group
            .handle(startup_handshake(e2), m1.with_handshake(true))
            .await
            .unwrap();
        assert_eq!(fence(&res), Some("accepted"));
        assert_eq!(recorded_epoch(&m1_kv).await, Some(e2));

        // M2 is elected again.
        m2_election.set_leader(true);
        let res = m2_group
            .handle(heartbeat(e1, "a1"), m2.clone())
            .await
            .unwrap();
        assert_eq!(fence(&res), Some("stale"));
        assert!(res.region_lease.is_none());
        let res = m2_group.handle(heartbeat(e2, "a2"), m2).await.unwrap();
        assert_eq!(fence(&res), Some("accepted"));
        assert!(res.region_lease.is_some());

        m1_kv.delete(&key, false).await.unwrap();
    }

    #[tokio::test]
    async fn test_fence_on_etcd() {
        let endpoints = std::env::var("GT_ETCD_ENDPOINTS").unwrap_or_default();
        if endpoints.is_empty() {
            return;
        }
        let endpoints = endpoints.split(',').collect::<Vec<_>>();
        let connect = || common_meta::kv_backend::etcd::EtcdStore::with_endpoints(&endpoints, 128);
        fence_across_metasrvs(connect().await.unwrap(), connect().await.unwrap()).await;
    }

    #[cfg(feature = "pg_kvbackend")]
    #[tokio::test]
    async fn test_fence_on_postgres() {
        common_meta::maybe_skip_postgres_integration_test!();
        let url = std::env::var("GT_POSTGRES_ENDPOINTS").unwrap();
        let connect =
            || common_meta::kv_backend::rds::PgStore::with_url(&url, "node_epoch_fence_test", 128);
        fence_across_metasrvs(connect().await.unwrap(), connect().await.unwrap()).await;
    }

    #[cfg(feature = "mysql_kvbackend")]
    #[tokio::test]
    async fn test_fence_on_mysql() {
        common_meta::maybe_skip_mysql_integration_test!();
        let url = std::env::var("GT_MYSQL_ENDPOINTS").unwrap();
        let connect = || {
            common_meta::kv_backend::rds::MySqlStore::with_url(&url, "node_epoch_fence_test", 128)
        };
        fence_across_metasrvs(connect().await.unwrap(), connect().await.unwrap()).await;
    }
}
