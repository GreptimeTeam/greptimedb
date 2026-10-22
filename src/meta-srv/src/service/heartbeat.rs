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

use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use api::v1::meta::{
    AskLeaderRequest, AskLeaderResponse, HeartbeatRequest, HeartbeatResponse, Peer, RequestHeader,
    ResponseHeader, Role, heartbeat_server,
};
use common_meta::datanode::NodeEpochFence;
use common_meta::election::LeaderChangeMessage;
use common_telemetry::{debug, error, info, warn};
use common_time::util::current_time_millis;
use futures::StreamExt;
use once_cell::sync::OnceCell;
use snafu::{OptionExt, ResultExt};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::error::{self, Result};
use crate::handler::node_epoch::{NodeEpochCheck, is_startup_handshake};
use crate::handler::{HeartbeatHandlerGroup, Pusher, PusherId};
use crate::metasrv::{Context, ElectionRef, Metasrv};
use crate::metrics::METRIC_META_HEARTBEAT_RECV;
use crate::service::{GrpcResult, GrpcStream};

type HeartbeatResponseResult = std::result::Result<HeartbeatResponse, Status>;

#[async_trait::async_trait]
trait HeartbeatRequestStream {
    async fn next(&mut self) -> Option<std::result::Result<HeartbeatRequest, Status>>;
}

struct TonicHeartbeatRequestStream {
    inner: Streaming<HeartbeatRequest>,
}

impl TonicHeartbeatRequestStream {
    fn new(inner: Streaming<HeartbeatRequest>) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl HeartbeatRequestStream for TonicHeartbeatRequestStream {
    async fn next(&mut self) -> Option<std::result::Result<HeartbeatRequest, Status>> {
        self.inner.next().await
    }
}

enum LeaderStepDownEvent {
    StepDown,
    Closed,
}

#[async_trait::async_trait]
trait LeaderStepDown {
    async fn wait(&mut self) -> LeaderStepDownEvent;
}

struct ElectionLeaderStepDown {
    rx: tokio::sync::broadcast::Receiver<LeaderChangeMessage>,
}

impl ElectionLeaderStepDown {
    fn new(election: ElectionRef) -> Self {
        Self {
            rx: election.subscribe_leader_change(),
        }
    }
}

#[async_trait::async_trait]
impl LeaderStepDown for ElectionLeaderStepDown {
    async fn wait(&mut self) -> LeaderStepDownEvent {
        loop {
            match self.rx.recv().await {
                Ok(LeaderChangeMessage::StepDown(_)) => return LeaderStepDownEvent::StepDown,
                Ok(LeaderChangeMessage::Elected(_)) => {}
                Err(RecvError::Lagged(skipped)) => {
                    warn!(
                        "Leader step-down watcher lagged, skipped {} leader change events",
                        skipped
                    );
                }
                Err(RecvError::Closed) => return LeaderStepDownEvent::Closed,
            }
        }
    }
}

struct HeartbeatSession<R, L> {
    requests: R,
    tx: Sender<HeartbeatResponseResult>,
    leader_step_down: Option<L>,
    handler_group: Arc<HeartbeatHandlerGroup>,
    ctx: Context,
    sender_id: PusherId,
    /// Whether the stream has yet to register its pusher. A datanode stream registers it on
    /// its first request that passes the node epoch check, other streams on the handshake.
    /// Later requests of a datanode stream that pass the check register it again if the slot
    /// is empty or holds an older process's pusher.
    pusher_pending: bool,
}

impl<R, L> HeartbeatSession<R, L>
where
    R: HeartbeatRequestStream,
    L: LeaderStepDown,
{
    /// Initializes the heartbeat session by receiving the first request,
    /// and returns `None` if the stream is closed or an error occurs.
    async fn init(
        mut requests: R,
        tx: Sender<HeartbeatResponseResult>,
        leader_step_down: Option<L>,
        handler_group: Arc<HeartbeatHandlerGroup>,
        ctx: Context,
    ) -> Option<Self> {
        let msg = requests.next().await?;

        let req = match msg {
            Ok(req) => req,
            Err(err) => {
                error!("Failed to receive the first heartbeat request, error: {err}");
                let _ = handle_request_stream_error(None, &tx, err).await;
                return None;
            }
        };

        let Some(header) = req.header.as_ref() else {
            error!("Exit on malformed request: MissingRequestHeader");
            let _ = tx
                .send(Err(error::MissingRequestHeaderSnafu {}.build().into()))
                .await;
            return None;
        };

        let sender_id = PusherId::new(header.role(), get_node_id(header));
        let pusher_pending = sender_id.role == Role::Datanode;
        if !pusher_pending {
            handler_group
                .register_pusher(sender_id, Pusher::new(tx.clone()))
                .await;
        }
        let mut session = Self {
            requests,
            tx,
            leader_step_down,
            handler_group,
            ctx,
            sender_id,
            pusher_pending,
        };

        if session.handle_request(req, true).await {
            Some(session)
        } else {
            session.cleanup().await;
            None
        }
    }

    /// Runs the heartbeat session until the stream is closed or an error occurs.
    async fn run(mut self) {
        let mut leader_step_down = self.leader_step_down.take();

        loop {
            tokio::select! {
                msg = self.requests.next() => {
                    let Some(msg) = msg else {
                        break;
                    };

                    if !self.handle_message(msg).await {
                        break;
                    }
                }
                event = wait_leader_step_down(leader_step_down.as_mut()), if leader_step_down.is_some() => {
                    match event {
                        LeaderStepDownEvent::StepDown => {
                            self.send_not_leader_error().await;
                            break;
                        }
                        LeaderStepDownEvent::Closed => {
                            warn!("Leader step-down watcher closed");
                            self.send_election_unavailable_error().await;
                            break;
                        }
                    }
                }
            }
        }

        self.cleanup().await;
    }

    /// Handles the incoming message, and returns whether to continue the session.
    async fn handle_message(&mut self, msg: std::result::Result<HeartbeatRequest, Status>) -> bool {
        match msg {
            Ok(req) => self.handle_request(req, false).await,
            Err(err) => handle_request_stream_error(Some(self.sender_id), &self.tx, err).await,
        }
    }

    /// Handles the incoming heartbeat request, and returns whether to continue the session.
    async fn handle_request(&mut self, req: HeartbeatRequest, is_handshake: bool) -> bool {
        let mailbox_message_id = req.mailbox_message.as_ref().map(|msg| msg.id);
        debug!(
            sender_id = %self.sender_id,
            ?mailbox_message_id,
            is_handshake,
            "Receiving heartbeat request"
        );

        let sender_id = self.sender_id.to_string();
        METRIC_META_HEARTBEAT_RECV
            .with_label_values(&[sender_id.as_str()])
            .inc();

        let ctx = self.ctx.clone().with_handshake(is_handshake);
        let res = match self.handler_group.check_request(&req, &ctx).await {
            Ok((role, epoch_check)) => {
                // Updates the pushers before the handlers run, so the stream receives the
                // messages they broadcast.
                let is_startup = is_startup_handshake(&req, is_handshake);
                self.on_node_epoch_check(epoch_check, req.node_epoch, is_startup)
                    .await;
                self.handler_group
                    .handle_checked(req, ctx, role, epoch_check)
                    .await
            }
            Err(e) => Err(e),
        };
        let res = res
            .inspect_err(
                |e| warn!(e; "Failed to handle heartbeat request, sender: {}", self.sender_id),
            )
            .map_err(|e| e.into());

        let is_not_leader = res.as_ref().is_ok_and(|r| r.is_not_leader());

        debug!(
            sender_id = %self.sender_id,
            success = res.is_ok(),
            is_not_leader,
            "Sending heartbeat response"
        );

        if self.tx.send(res).await.is_err() {
            info!(
                "ReceiverStream was dropped; shutting down, sender: {}",
                self.sender_id
            );
            return false;
        }

        if is_not_leader {
            warn!(
                "Quit because it is no longer the leader, sender: {}",
                self.sender_id
            );
            self.send_not_leader_error().await;
            return false;
        }

        true
    }

    /// Updates the pushers by the node epoch check of a datanode request.
    async fn on_node_epoch_check(
        &mut self,
        epoch_check: NodeEpochCheck,
        node_epoch: u64,
        is_startup: bool,
    ) {
        match epoch_check {
            // A startup handshake comes before the process opens regions. It registers no
            // pusher, and removes the pushers of the processes it replaces.
            NodeEpochCheck::Checked(NodeEpochFence::Accepted) if is_startup => {
                self.ctx
                    .datanode_restart_grace
                    .record_startup(self.sender_id.id, current_time_millis());
                self.handler_group
                    .deregister_older_push(self.sender_id, node_epoch)
                    .await;
            }
            NodeEpochCheck::Checked(NodeEpochFence::Accepted) if self.pusher_pending => {
                self.pusher_pending = false;
                self.handler_group
                    .register_pusher(
                        self.sender_id,
                        Pusher::new(self.tx.clone()).with_node_epoch(node_epoch),
                    )
                    .await;
            }
            NodeEpochCheck::Checked(NodeEpochFence::Accepted) => {
                self.handler_group
                    .reregister_pusher(self.sender_id, &self.tx, node_epoch)
                    .await;
            }
            // Keeps the stream open: a datanode that does not understand the fence would
            // only reconnect.
            NodeEpochCheck::Checked(NodeEpochFence::Stale) => {
                self.handler_group
                    .deregister_push(self.sender_id, &self.tx)
                    .await;
            }
            _ => {}
        }
    }

    async fn send_not_leader_error(&mut self) {
        let _ = self
            .tx
            .send(Err(Status::aborted(format!(
                "The requested metasrv node is not leader, node addr: {}",
                self.ctx.server_addr
            ))))
            .await;
    }

    async fn send_election_unavailable_error(&mut self) {
        let _ = self
            .tx
            .send(Err(Status::unavailable(format!(
                "The requested metasrv node is shutting down, node addr: {}",
                self.ctx.server_addr
            ))))
            .await;
    }

    async fn cleanup(&self) {
        info!("Heartbeat stream closed, sender: {}", self.sender_id);
        self.handler_group
            .deregister_push(self.sender_id, &self.tx)
            .await;
    }
}

async fn wait_leader_step_down<L>(leader_step_down: Option<&mut L>) -> LeaderStepDownEvent
where
    L: LeaderStepDown,
{
    match leader_step_down {
        Some(leader_step_down) => leader_step_down.wait().await,
        None => std::future::pending().await,
    }
}

/// Handles request stream error by logging and forwarding the error to the client if possible.
///
/// Returns `false` if the stream should be terminated.
async fn handle_request_stream_error(
    sender_id: Option<PusherId>,
    tx: &Sender<HeartbeatResponseResult>,
    err: Status,
) -> bool {
    if let Some(io_err) = error::match_for_io_error(&err)
        && io_err.kind() == ErrorKind::BrokenPipe
    {
        error!("Client disconnected: broken pipe, sender: {:?}", sender_id);
        return false;
    }
    error!(err; "Error while receiving heartbeat request, sender: {:?}", sender_id);

    if tx.send(Err(err)).await.is_err() {
        info!(
            "Failed to forward heartbeat request stream error; response stream was dropped, sender: {:?}",
            sender_id
        );
        return false;
    }

    true
}

#[async_trait::async_trait]
impl heartbeat_server::Heartbeat for Metasrv {
    type HeartbeatStream = GrpcStream<HeartbeatResponse>;

    async fn heartbeat(
        &self,
        req: Request<Streaming<HeartbeatRequest>>,
    ) -> GrpcResult<Self::HeartbeatStream> {
        let (tx, rx) = mpsc::channel(128);
        let handler_group = self.handler_group().context(error::UnexpectedSnafu {
            violated: "expected heartbeat handlers",
        })?;

        let ctx = self.new_ctx();
        let requests = TonicHeartbeatRequestStream::new(req.into_inner());
        let _handle = common_runtime::spawn_global(async move {
            if let Some(session) = HeartbeatSession::init(
                requests,
                tx,
                ctx.election
                    .as_ref()
                    .map(|r| ElectionLeaderStepDown::new(r.clone())),
                handler_group,
                ctx,
            )
            .await
            {
                session.run().await;
            }
        });

        let out_stream = ReceiverStream::new(rx);

        Ok(Response::new(Box::pin(out_stream)))
    }

    async fn ask_leader(&self, req: Request<AskLeaderRequest>) -> GrpcResult<AskLeaderResponse> {
        let req = req.into_inner();
        let ctx = self.new_ctx();
        let res = handle_ask_leader(req, ctx).await?;

        Ok(Response::new(res))
    }
}

async fn handle_ask_leader(_req: AskLeaderRequest, ctx: Context) -> Result<AskLeaderResponse> {
    let addr = match ctx.election {
        Some(election) => {
            if election.is_leader() {
                ctx.server_addr
            } else {
                election.leader().await.context(error::KvBackendSnafu)?.0
            }
        }
        None => ctx.server_addr,
    };

    let leader = Some(Peer {
        id: 0, // TODO(jiachun): meta node should have a Id
        addr,
    });

    let header = Some(ResponseHeader::success());
    Ok(AskLeaderResponse { header, leader })
}

fn get_node_id(header: &RequestHeader) -> u64 {
    static ID: OnceCell<Arc<AtomicU64>> = OnceCell::new();

    fn next_id() -> u64 {
        let id = ID.get_or_init(|| Arc::new(AtomicU64::new(0))).clone();
        id.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    match header.role() {
        Role::Frontend => next_id(),
        Role::Datanode | Role::Flownode => header.member_id,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Arc;

    use api::v1::meta::heartbeat_server::Heartbeat;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_telemetry::tracing_context::W3cTrace;
    use servers::grpc::GrpcOptions;
    use tokio::sync::mpsc;
    use tonic::{Code, IntoRequest};

    use super::*;
    use crate::handler::test_utils::TestEnv;
    use crate::metasrv::MetasrvOptions;
    use crate::metasrv::builder::MetasrvBuilder;

    struct MockHeartbeatRequestStream {
        messages: VecDeque<std::result::Result<HeartbeatRequest, Status>>,
        pending_when_empty: bool,
    }

    impl MockHeartbeatRequestStream {
        fn new(messages: Vec<std::result::Result<HeartbeatRequest, Status>>) -> Self {
            Self {
                messages: messages.into(),
                pending_when_empty: false,
            }
        }

        fn pending_after(messages: Vec<std::result::Result<HeartbeatRequest, Status>>) -> Self {
            Self {
                messages: messages.into(),
                pending_when_empty: true,
            }
        }
    }

    #[async_trait::async_trait]
    impl HeartbeatRequestStream for MockHeartbeatRequestStream {
        async fn next(&mut self) -> Option<std::result::Result<HeartbeatRequest, Status>> {
            if let Some(message) = self.messages.pop_front() {
                return Some(message);
            }

            if self.pending_when_empty {
                std::future::pending().await
            } else {
                None
            }
        }
    }

    struct MockLeaderStepDown {
        event: Option<LeaderStepDownEvent>,
    }

    impl MockLeaderStepDown {
        fn new(event: LeaderStepDownEvent) -> Self {
            Self { event: Some(event) }
        }
    }

    #[async_trait::async_trait]
    impl LeaderStepDown for MockLeaderStepDown {
        async fn wait(&mut self) -> LeaderStepDownEvent {
            self.event.take().unwrap()
        }
    }

    fn heartbeat_request(role: Role, member_id: u64) -> HeartbeatRequest {
        HeartbeatRequest {
            header: Some(RequestHeader {
                role: role.into(),
                member_id,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn sender_id(role: Role, member_id: u64) -> PusherId {
        PusherId::new(role, member_id)
    }

    fn test_context() -> Context {
        TestEnv::new().ctx()
    }

    fn test_handler_group() -> Arc<HeartbeatHandlerGroup> {
        Arc::new(HeartbeatHandlerGroup::default())
    }

    async fn init_session<L>(
        requests: MockHeartbeatRequestStream,
        tx: Sender<HeartbeatResponseResult>,
        leader_step_down: Option<L>,
        handler_group: Arc<HeartbeatHandlerGroup>,
    ) -> Option<HeartbeatSession<MockHeartbeatRequestStream, L>>
    where
        L: LeaderStepDown,
    {
        HeartbeatSession::init(
            requests,
            tx,
            leader_step_down,
            handler_group,
            test_context(),
        )
        .await
    }

    async fn recv_response(
        rx: &mut mpsc::Receiver<HeartbeatResponseResult>,
    ) -> HeartbeatResponseResult {
        rx.recv().await.unwrap()
    }

    #[tokio::test]
    async fn test_heartbeat_session_init_returns_none_on_empty_stream() {
        let (tx, _rx) = mpsc::channel(8);
        let handler_group = test_handler_group();
        let requests = MockHeartbeatRequestStream::new(vec![]);

        let session = init_session(
            requests,
            tx,
            None::<MockLeaderStepDown>,
            handler_group.clone(),
        )
        .await;

        assert!(session.is_none());
        assert!(
            !handler_group
                .contains_pusher(&sender_id(Role::Datanode, 42))
                .await
        );
    }

    #[tokio::test]
    async fn test_heartbeat_session_init_forwards_first_stream_error() {
        let (tx, mut rx) = mpsc::channel(8);
        let handler_group = test_handler_group();
        let requests = MockHeartbeatRequestStream::new(vec![Err(Status::internal("boom"))]);

        let session = init_session(requests, tx, None::<MockLeaderStepDown>, handler_group).await;

        assert!(session.is_none());
        let status = recv_response(&mut rx).await.unwrap_err();
        assert_eq!(Code::Internal, status.code());
        assert_eq!("boom", status.message());
    }

    #[tokio::test]
    async fn test_heartbeat_session_init_sends_error_on_missing_header() {
        let (tx, mut rx) = mpsc::channel(8);
        let handler_group = test_handler_group();
        let requests = MockHeartbeatRequestStream::new(vec![Ok(HeartbeatRequest::default())]);

        let session = init_session(
            requests,
            tx,
            None::<MockLeaderStepDown>,
            handler_group.clone(),
        )
        .await;

        assert!(session.is_none());
        assert!(
            !handler_group
                .contains_pusher(&sender_id(Role::Datanode, 42))
                .await
        );

        let status = recv_response(&mut rx).await.unwrap_err();
        assert_eq!(Code::InvalidArgument, status.code());
    }

    #[tokio::test]
    async fn test_heartbeat_session_init_registers_sender() {
        let (tx, mut rx) = mpsc::channel(8);
        let handler_group = test_handler_group();
        let sender_id = sender_id(Role::Flownode, 42);
        let requests =
            MockHeartbeatRequestStream::new(vec![Ok(heartbeat_request(Role::Flownode, 42))]);

        let session = init_session(
            requests,
            tx,
            None::<MockLeaderStepDown>,
            handler_group.clone(),
        )
        .await;

        assert!(session.is_some());
        assert!(handler_group.contains_pusher(&sender_id).await);

        let response = recv_response(&mut rx).await.unwrap();
        assert!(response.heartbeat_config.is_some());
    }

    #[tokio::test]
    async fn test_heartbeat_session_run_deregisters_sender_on_stream_close() {
        let (tx, mut rx) = mpsc::channel(8);
        let handler_group = test_handler_group();
        let sender_id = sender_id(Role::Flownode, 42);
        let requests =
            MockHeartbeatRequestStream::new(vec![Ok(heartbeat_request(Role::Flownode, 42))]);
        let session = init_session(
            requests,
            tx,
            None::<MockLeaderStepDown>,
            handler_group.clone(),
        )
        .await
        .unwrap();
        let _ = recv_response(&mut rx).await.unwrap();

        session.run().await;

        assert!(!handler_group.contains_pusher(&sender_id).await);
    }

    #[tokio::test]
    async fn test_heartbeat_session_run_forwards_stream_error_after_init() {
        let (tx, mut rx) = mpsc::channel(8);
        let handler_group = test_handler_group();
        let sender_id = sender_id(Role::Flownode, 42);
        let requests = MockHeartbeatRequestStream::new(vec![
            Ok(heartbeat_request(Role::Flownode, 42)),
            Err(Status::unavailable("temporary")),
        ]);
        let session = init_session(
            requests,
            tx,
            None::<MockLeaderStepDown>,
            handler_group.clone(),
        )
        .await
        .unwrap();
        let _ = recv_response(&mut rx).await.unwrap();

        session.run().await;

        let status = recv_response(&mut rx).await.unwrap_err();
        assert_eq!(Code::Unavailable, status.code());
        assert_eq!("temporary", status.message());
        assert!(!handler_group.contains_pusher(&sender_id).await);
    }

    #[tokio::test]
    async fn test_heartbeat_session_leader_step_down_sends_aborted_and_deregisters() {
        let (tx, mut rx) = mpsc::channel(8);
        let handler_group = test_handler_group();
        let sender_id = sender_id(Role::Flownode, 42);
        let requests = MockHeartbeatRequestStream::pending_after(vec![Ok(heartbeat_request(
            Role::Flownode,
            42,
        ))]);
        let session = init_session(
            requests,
            tx,
            Some(MockLeaderStepDown::new(LeaderStepDownEvent::StepDown)),
            handler_group.clone(),
        )
        .await
        .unwrap();
        let _ = recv_response(&mut rx).await.unwrap();

        session.run().await;

        let status = recv_response(&mut rx).await.unwrap_err();
        assert_eq!(Code::Aborted, status.code());
        assert!(!handler_group.contains_pusher(&sender_id).await);
    }

    #[tokio::test]
    async fn test_heartbeat_session_leader_watcher_closed_sends_unavailable_and_deregisters() {
        let (tx, mut rx) = mpsc::channel(8);
        let handler_group = test_handler_group();
        let sender_id = sender_id(Role::Flownode, 42);
        let requests = MockHeartbeatRequestStream::pending_after(vec![Ok(heartbeat_request(
            Role::Flownode,
            42,
        ))]);
        let session = init_session(
            requests,
            tx,
            Some(MockLeaderStepDown::new(LeaderStepDownEvent::Closed)),
            handler_group.clone(),
        )
        .await
        .unwrap();
        let _ = recv_response(&mut rx).await.unwrap();

        session.run().await;

        let status = recv_response(&mut rx).await.unwrap_err();
        assert_eq!(Code::Unavailable, status.code());
        assert!(!handler_group.contains_pusher(&sender_id).await);
    }

    #[tokio::test]
    async fn test_ask_leader() {
        let kv_backend = Arc::new(MemoryKvBackend::new());

        let metasrv = MetasrvBuilder::new()
            .kv_backend(kv_backend)
            .options(MetasrvOptions {
                grpc: GrpcOptions {
                    server_addr: "127.0.0.1:3002".to_string(),
                    ..Default::default()
                },
                ..Default::default()
            })
            .build()
            .await
            .unwrap();

        let req = AskLeaderRequest {
            header: Some(RequestHeader::new(1, Role::Datanode, W3cTrace::new())),
        };

        let res = metasrv.ask_leader(req.into_request()).await.unwrap();
        let res = res.into_inner();
        assert_eq!(metasrv.options().grpc.server_addr, res.leader.unwrap().addr);
    }

    #[test]
    fn test_get_node_id() {
        let header = RequestHeader {
            role: Role::Datanode.into(),
            member_id: 11,
            ..Default::default()
        };
        assert_eq!(11, get_node_id(&header));

        let header = RequestHeader {
            role: Role::Frontend.into(),
            ..Default::default()
        };
        for i in 0..10 {
            assert_eq!(i, get_node_id(&header));
        }

        let header = RequestHeader {
            role: Role::Frontend.into(),
            member_id: 11,
            ..Default::default()
        };
        for i in 10..20 {
            assert_eq!(i, get_node_id(&header));
        }
    }

    mod node_epoch_fence {
        use std::sync::Arc;
        use std::time::Duration;

        use api::v1::meta::{HeartbeatRequest, MailboxMessage, Role};
        use common_meta::key::MetadataValue;
        use common_meta::key::datanode_epoch::{DatanodeEpochKey, DatanodeEpochValue};
        use common_meta::kv_backend::memory::MemoryKvBackend;
        use common_meta::rpc::store::PutRequest;
        use common_meta::sequence::SequenceBuilder;
        use common_time::util::current_time_millis;
        use tokio::sync::Notify;
        use tokio::sync::mpsc;

        use super::{MockHeartbeatRequestStream, MockLeaderStepDown};
        use crate::handler::node_epoch::tests::{
            HookedKvBackend, NODE_ID, fence, handshake, heartbeat, new_group, startup_handshake,
        };
        use crate::handler::test_utils::TestEnv;
        use crate::handler::{
            HandleControl, HeartbeatAccumulator, HeartbeatHandler, HeartbeatHandlerGroup,
            HeartbeatHandlerGroupBuilder, HeartbeatMailbox, Pusher, PusherId, Pushers,
        };
        use crate::metasrv::Context;
        use crate::service::heartbeat::{HeartbeatResponseResult, HeartbeatSession};
        use crate::service::mailbox::{BroadcastChannel, Channel};

        type Session = HeartbeatSession<MockHeartbeatRequestStream, MockLeaderStepDown>;

        /// A datanode stream that has sent its handshake.
        struct Stream {
            session: Session,
            tx: mpsc::Sender<HeartbeatResponseResult>,
            rx: mpsc::Receiver<HeartbeatResponseResult>,
        }

        impl Stream {
            async fn open(
                group: &Arc<HeartbeatHandlerGroup>,
                ctx: &Context,
                handshake: HeartbeatRequest,
            ) -> Self {
                let (tx, mut rx) = mpsc::channel(8);
                let session = HeartbeatSession::init(
                    MockHeartbeatRequestStream::new(vec![Ok(handshake)]),
                    tx.clone(),
                    None,
                    group.clone(),
                    ctx.clone(),
                )
                .await
                .unwrap();
                let res = rx.recv().await.unwrap().unwrap();
                assert!(res.heartbeat_config.is_some());
                Self { session, tx, rx }
            }

            /// Sends a request, and returns the response and whether the stream stays open.
            async fn send(
                &mut self,
                req: HeartbeatRequest,
            ) -> (api::v1::meta::HeartbeatResponse, bool) {
                let open = self.session.handle_request(req, false).await;
                (self.rx.recv().await.unwrap().unwrap(), open)
            }

            /// Returns the node epoch of the datanode pusher if this stream registered it.
            async fn pusher_epoch(&self, group: &HeartbeatHandlerGroup) -> Option<u64> {
                group
                    .pusher_epoch_of(&PusherId::new(Role::Datanode, NODE_ID), &self.tx)
                    .await
            }
        }

        fn epochs() -> (u64, u64) {
            let now = current_time_millis() as u64;
            (now - 1000, now)
        }

        #[tokio::test]
        async fn test_datanode_pusher_is_registered_after_epoch_check() {
            let group = Arc::new(new_group());
            let ctx = TestEnv::new().ctx();
            let (e1, _) = epochs();

            // A datanode that sends no epoch on its handshake is not fenced, and gets its
            // pusher on its first heartbeat that passes the check.
            let mut a1 = Stream::open(&group, &ctx, handshake(0)).await;
            assert_eq!(a1.pusher_epoch(&group).await, None);
            let (res, open) = a1.send(heartbeat(e1, "a1")).await;
            assert!(open);
            assert_eq!(fence(&res), Some("accepted"));
            assert!(res.region_lease.is_some());
            assert_eq!(a1.pusher_epoch(&group).await, Some(e1));
        }

        struct BroadcastToDatanodes;

        #[async_trait::async_trait]
        impl HeartbeatHandler for BroadcastToDatanodes {
            fn is_acceptable(&self, role: Role) -> bool {
                role == Role::Datanode
            }

            async fn handle(
                &self,
                _: &HeartbeatRequest,
                ctx: &mut Context,
                _: &mut HeartbeatAccumulator,
            ) -> crate::error::Result<HandleControl> {
                let msg = MailboxMessage {
                    subject: "broadcast".to_string(),
                    ..Default::default()
                };
                ctx.mailbox
                    .broadcast(&BroadcastChannel::Datanode, &msg)
                    .await?;
                Ok(HandleControl::Continue)
            }
        }

        #[tokio::test]
        async fn test_first_accepted_request_receives_handler_broadcasts() {
            let mut builder =
                HeartbeatHandlerGroupBuilder::new(Pushers::default()).add_default_handlers();
            builder
                .add_handler_after("ResponseHeaderHandler", BroadcastToDatanodes)
                .unwrap();
            let group = Arc::new(builder.build().unwrap());
            let mut ctx = TestEnv::new().ctx();
            let seq = SequenceBuilder::new("test_seq", Arc::new(MemoryKvBackend::new())).build();
            ctx.mailbox = HeartbeatMailbox::create(group.pushers(), seq);
            let (e1, _) = epochs();

            // The pusher is registered before the handlers of the first accepted request run,
            // so their broadcast reaches the stream ahead of the response.
            let mut a1 = Stream::open(&group, &ctx, handshake(0)).await;
            assert!(a1.session.handle_request(heartbeat(e1, "a1"), false).await);
            let broadcast = a1.rx.recv().await.unwrap().unwrap();
            assert_eq!(broadcast.mailbox_message.unwrap().subject, "broadcast");
            let res = a1.rx.recv().await.unwrap().unwrap();
            assert_eq!(fence(&res), Some("accepted"));
        }

        #[tokio::test]
        async fn test_stale_stream_stays_open_without_pusher() {
            let group = Arc::new(new_group());
            let ctx = TestEnv::new().ctx();
            let (e1, e2) = epochs();
            let mut a1 = Stream::open(&group, &ctx, handshake(0)).await;
            a1.send(heartbeat(e1, "a1")).await;

            // A higher epoch takes over the pusher on its handshake, a lower one does not.
            let a2 = Stream::open(&group, &ctx, handshake(e2)).await;
            assert_eq!(a2.pusher_epoch(&group).await, Some(e2));
            let a1_reconnected = Stream::open(&group, &ctx, handshake(e1)).await;
            assert_eq!(a1_reconnected.pusher_epoch(&group).await, None);
            assert_eq!(a2.pusher_epoch(&group).await, Some(e2));

            // A1 is told it is stale without an error, so it does not reconnect, and its
            // stream stays open.
            let (res, open) = a1.send(heartbeat(e1, "a1")).await;
            assert!(open);
            assert_eq!(fence(&res), Some("stale"));
            assert!(res.header.unwrap().error.is_none());
            assert!(res.region_lease.is_none());
            assert_eq!(a2.pusher_epoch(&group).await, Some(e2));

            // A lower epoch cannot overwrite the pusher, and A1's stream ending leaves it.
            group
                .register_pusher(
                    PusherId::new(Role::Datanode, NODE_ID),
                    Pusher::new(a1.tx.clone()).with_node_epoch(e1),
                )
                .await;
            assert_eq!(a2.pusher_epoch(&group).await, Some(e2));
            a1.session.run().await;
            assert_eq!(a2.pusher_epoch(&group).await, Some(e2));

            // A2 reconnects: the new stream takes over the pusher with the same epoch, and the
            // old one ending removes only its own.
            let a2_reconnected = Stream::open(&group, &ctx, handshake(e2)).await;
            assert_eq!(a2_reconnected.pusher_epoch(&group).await, Some(e2));
            a2.session.run().await;
            assert_eq!(a2_reconnected.pusher_epoch(&group).await, Some(e2));
        }

        #[tokio::test]
        async fn test_stream_registers_again_after_the_other_stream_of_its_process_ends() {
            let group = Arc::new(new_group());
            let ctx = TestEnv::new().ctx();
            let pusher_id = PusherId::new(Role::Datanode, NODE_ID);
            let (e1, _) = epochs();

            // 1. The process has two streams: S0 is an old one, S1 replaced it.
            let mut s0 = Stream::open(&group, &ctx, handshake(0)).await;
            let mut s1 = Stream::open(&group, &ctx, handshake(0)).await;
            // 2. S1 registers its pusher.
            s1.send(heartbeat(e1, "a1")).await;
            assert_eq!(s1.pusher_epoch(&group).await, Some(e1));
            // 3. S0's first accepted request comes later, and takes over the pusher. S1's
            // next heartbeat leaves it, since it has the same epoch.
            s0.send(heartbeat(e1, "a1")).await;
            assert_eq!(s0.pusher_epoch(&group).await, Some(e1));
            s1.send(heartbeat(e1, "a1")).await;
            assert_eq!(s0.pusher_epoch(&group).await, Some(e1));
            // 4. S0 ends, and removes its pusher.
            s0.session.run().await;
            assert!(!group.contains_pusher(&pusher_id).await);
            // 5. S1's next heartbeat registers its pusher again.
            let (res, open) = s1.send(heartbeat(e1, "a1")).await;
            assert!(open);
            assert_eq!(fence(&res), Some("accepted"));
            assert_eq!(s1.pusher_epoch(&group).await, Some(e1));
        }

        #[tokio::test]
        async fn test_startup_handshake_replaces_older_pushers() {
            let group = Arc::new(new_group());
            let ctx = TestEnv::new().ctx();
            let (e1, e2) = epochs();
            let mut a1 = Stream::open(&group, &ctx, handshake(0)).await;
            a1.send(heartbeat(e1, "a1")).await;
            let seq = SequenceBuilder::new("test_seq", Arc::new(MemoryKvBackend::new())).build();
            let mailbox = HeartbeatMailbox::create(group.pushers(), seq);
            let waiting = mailbox
                .send(
                    &Channel::Datanode(NODE_ID),
                    MailboxMessage::default(),
                    Duration::from_secs(10),
                )
                .await
                .unwrap();

            let before = current_time_millis();
            let a2 = Stream::open(&group, &ctx, startup_handshake(e2)).await;
            // The startup handshake registers no pusher, and removes A1's: instructions
            // waiting on A1 fail at once.
            assert_eq!(a2.pusher_epoch(&group).await, None);
            assert!(
                !group
                    .contains_pusher(&PusherId::new(Role::Datanode, NODE_ID))
                    .await
            );
            assert!(
                tokio::time::timeout(Duration::from_secs(1), waiting)
                    .await
                    .unwrap()
                    .is_err()
            );
            assert!(ctx.datanode_restart_grace.is_restarting(
                NODE_ID,
                before - 1,
                current_time_millis()
            ));
        }

        #[tokio::test]
        async fn test_request_accepted_before_startup_fence_cannot_register_pusher() {
            let (e1, e2) = epochs();
            let group = Arc::new(new_group());
            let kv_backend = Arc::new(HookedKvBackend::default());
            let mut ctx = TestEnv::new().ctx();
            ctx.kv_backend = kv_backend.clone() as _;
            // A1 has been accepted before.
            ctx.kv_backend
                .put(PutRequest {
                    key: DatanodeEpochKey::new(NODE_ID).to_bytes(),
                    value: DatanodeEpochValue { node_epoch: e1 }
                        .try_as_raw_value()
                        .unwrap(),
                    ..Default::default()
                })
                .await
                .unwrap();
            let Stream {
                mut session,
                tx: a1_tx,
                rx: mut a1_rx,
            } = Stream::open(&group, &ctx, handshake(0)).await;

            // A1's heartbeat on a new stream reads the epoch, then waits before it registers
            // its pusher while A2's startup handshake fences A1 off.
            let entered = Arc::new(Notify::new());
            let release = Arc::new(Notify::new());
            *kv_backend.hold_next_read.lock().unwrap() = Some((entered.clone(), release.clone()));
            let a1 = tokio::spawn(async move {
                session.handle_request(heartbeat(e1, "a1"), false).await;
                session
            });
            entered.notified().await;
            let _a2 = Stream::open(&group, &ctx, startup_handshake(e2)).await;
            release.notify_one();
            let _a1_session = a1.await.unwrap();
            let res = a1_rx.recv().await.unwrap().unwrap();
            assert_eq!(fence(&res), Some("accepted"));

            assert_eq!(
                group
                    .pusher_epoch_of(&PusherId::new(Role::Datanode, NODE_ID), &a1_tx)
                    .await,
                None
            );
            let seq = SequenceBuilder::new("test_seq", Arc::new(MemoryKvBackend::new())).build();
            let mailbox = HeartbeatMailbox::create(group.pushers(), seq);
            assert!(
                mailbox
                    .send_oneway(&Channel::Datanode(NODE_ID), MailboxMessage::default())
                    .await
                    .is_err()
            );
        }

        #[tokio::test]
        async fn test_old_datanode_after_epoch_is_recorded() {
            let group = Arc::new(new_group());
            let ctx = TestEnv::new().ctx();
            let (e1, e2) = epochs();
            ctx.kv_backend
                .put(PutRequest {
                    key: DatanodeEpochKey::new(NODE_ID).to_bytes(),
                    value: DatanodeEpochValue { node_epoch: e1 }
                        .try_as_raw_value()
                        .unwrap(),
                    ..Default::default()
                })
                .await
                .unwrap();

            // The old datanode reconnects with the recorded epoch.
            let mut a1 = Stream::open(&group, &ctx, handshake(0)).await;
            let (res, _) = a1.send(heartbeat(e1, "a1")).await;
            assert_eq!(fence(&res), Some("accepted"));
            assert!(res.region_lease.is_some());
            assert_eq!(a1.pusher_epoch(&group).await, Some(e1));

            // It restarts while the previous process is still connected.
            let mut a2 = Stream::open(&group, &ctx, handshake(0)).await;
            let (res, _) = a2.send(heartbeat(e2, "a2")).await;
            assert_eq!(fence(&res), Some("accepted"));
            assert!(res.region_lease.is_some());
            assert_eq!(a2.pusher_epoch(&group).await, Some(e2));

            // The previous process keeps its stream, without lease or pusher.
            let (res, open) = a1.send(heartbeat(e1, "a1")).await;
            assert!(open);
            assert!(res.header.unwrap().error.is_none());
            assert!(res.region_lease.is_none());
            assert_eq!(a2.pusher_epoch(&group).await, Some(e2));
        }
    }
}
