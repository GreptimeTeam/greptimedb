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
    AskLeaderRequest, AskLeaderResponse, HeartbeatRequest, HeartbeatResponse, PROTOCOL_VERSION,
    Peer, RequestHeader, ResponseHeader, Role, heartbeat_server,
};
use common_meta::election::LeaderChangeMessage;
use common_telemetry::{debug, error, info, warn};
use futures::StreamExt;
use once_cell::sync::OnceCell;
use snafu::{OptionExt, ResultExt};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::error::{self, Result};
use crate::handler::node_epoch::check_node_epoch;
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
    /// The node epoch the stream's pusher was registered with.
    pusher_epoch: u64,
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

        let sender_id = register_pusher(&handler_group, header, tx.clone()).await;
        let mut session = Self {
            requests,
            tx,
            leader_step_down,
            handler_group,
            ctx,
            sender_id,
            pusher_epoch: 0,
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

        let res = match self.check_node_epoch(&req).await {
            Some(res) => res,
            None => {
                self.handler_group
                    .handle(req, self.ctx.clone().with_handshake(is_handshake))
                    .await
            }
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

    /// Checks the node epoch of a datanode request, and returns the response to send instead
    /// of running the handlers if the request must not reach them.
    async fn check_node_epoch(
        &mut self,
        req: &HeartbeatRequest,
    ) -> Option<Result<HeartbeatResponse>> {
        let node_epoch = req.node_epoch;
        // Datanodes that do not report their epoch are not checked.
        if self.sender_id.role != Role::Datanode || node_epoch == 0 {
            return None;
        }
        // Only the leader reads and raises the record.
        let error = if let Some(election) = &self.ctx.election
            && !election.is_leader()
        {
            Some(api::v1::meta::Error::is_not_leader())
        } else {
            let kv_backend = &*self.ctx.leader_cached_kv_backend;
            match check_node_epoch(kv_backend, self.sender_id.id, node_epoch).await {
                Ok(true) => {
                    // Registers the pusher before the handlers run, so the stream receives
                    // the messages they broadcast.
                    if node_epoch > self.pusher_epoch {
                        self.pusher_epoch = node_epoch;
                        let pusher = Pusher::new(self.tx.clone()).with_node_epoch(node_epoch);
                        self.handler_group
                            .register_pusher(self.sender_id, pusher)
                            .await;
                    }
                    return None;
                }
                // The response carries no error and the stream stays open: a datanode that is
                // not told why would only reconnect.
                Ok(false) => {
                    self.handler_group
                        .deregister_push(self.sender_id, &self.tx)
                        .await;
                    None
                }
                Err(e) => return Some(Err(e)),
            }
        };
        Some(Ok(HeartbeatResponse {
            header: Some(ResponseHeader {
                protocol_version: PROTOCOL_VERSION,
                error,
            }),
            ..Default::default()
        }))
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

async fn register_pusher(
    handler_group: &HeartbeatHandlerGroup,
    header: &RequestHeader,
    sender: Sender<std::result::Result<HeartbeatResponse, tonic::Status>>,
) -> PusherId {
    let role = header.role();
    let id = get_node_id(header);
    let pusher_id = PusherId::new(role, id);
    let pusher = Pusher::new(sender);
    handler_group.register_pusher(pusher_id, pusher).await;
    pusher_id
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
        let sender_id = sender_id(Role::Datanode, 42);
        let requests =
            MockHeartbeatRequestStream::new(vec![Ok(heartbeat_request(Role::Datanode, 42))]);

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
        let sender_id = sender_id(Role::Datanode, 42);
        let requests =
            MockHeartbeatRequestStream::new(vec![Ok(heartbeat_request(Role::Datanode, 42))]);
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
        let sender_id = sender_id(Role::Datanode, 42);
        let requests = MockHeartbeatRequestStream::new(vec![
            Ok(heartbeat_request(Role::Datanode, 42)),
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
        let sender_id = sender_id(Role::Datanode, 42);
        let requests = MockHeartbeatRequestStream::pending_after(vec![Ok(heartbeat_request(
            Role::Datanode,
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
        let sender_id = sender_id(Role::Datanode, 42);
        let requests = MockHeartbeatRequestStream::pending_after(vec![Ok(heartbeat_request(
            Role::Datanode,
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

    mod node_epoch {
        use std::sync::Arc;
        use std::time::Duration;

        use api::v1::meta::{HeartbeatRequest, HeartbeatResponse, MailboxMessage, Peer, Role};
        use common_meta::key::TableMetadataManager;
        use common_meta::key::datanode_epoch::DatanodeEpochKey;
        use common_meta::kv_backend::KvBackendRef;
        use common_meta::kv_backend::memory::MemoryKvBackend;
        use common_meta::region_keeper::MemoryRegionKeeper;
        use common_meta::sequence::SequenceBuilder;
        use common_time::util::current_time_millis;
        use tokio::sync::mpsc;

        use super::{MockHeartbeatRequestStream, MockLeaderStepDown, heartbeat_request};
        use crate::discovery::utils::find_datanode_lease_value;
        use crate::handler::region_lease_handler::RegionLeaseHandler;
        use crate::handler::test_utils::TestEnv;
        use crate::handler::{
            HeartbeatHandlerGroup, HeartbeatHandlerGroupBuilder, HeartbeatMailbox, Pusher,
            PusherId, Pushers,
        };
        use crate::metasrv::Context;
        use crate::service::heartbeat::{HeartbeatResponseResult, HeartbeatSession};
        use crate::service::mailbox::{Channel, MailboxRef};

        const NODE_ID: u64 = 1;

        /// A datanode stream that has sent its handshake, which carries no epoch.
        struct Stream {
            session: HeartbeatSession<MockHeartbeatRequestStream, MockLeaderStepDown>,
            rx: mpsc::Receiver<HeartbeatResponseResult>,
        }

        impl Stream {
            async fn open(group: &Arc<HeartbeatHandlerGroup>, ctx: &Context) -> Self {
                let (tx, mut rx) = mpsc::channel(8);
                let handshake = heartbeat_request(Role::Datanode, NODE_ID);
                let session = HeartbeatSession::init(
                    MockHeartbeatRequestStream::new(vec![Ok(handshake)]),
                    tx,
                    None,
                    group.clone(),
                    ctx.clone(),
                )
                .await
                .unwrap();
                rx.recv().await.unwrap().unwrap();
                Self { session, rx }
            }

            async fn send(&mut self, req: HeartbeatRequest) -> HeartbeatResponse {
                assert!(self.session.handle_request(req, false).await);
                self.rx.recv().await.unwrap().unwrap()
            }

            /// Returns whether the stream has received a pushed message.
            fn received_push(&mut self) -> bool {
                self.rx.try_recv().is_ok()
            }
        }

        fn new_group() -> Arc<HeartbeatHandlerGroup> {
            let lease_handler = RegionLeaseHandler::new(
                10,
                Arc::new(TableMetadataManager::new(Arc::new(MemoryKvBackend::new()))),
                Arc::new(MemoryRegionKeeper::default()),
                None,
            );
            let builder = HeartbeatHandlerGroupBuilder::new(Pushers::default())
                .with_region_lease_handler(Some(lease_handler));
            Arc::new(builder.add_default_handlers().build().unwrap())
        }

        fn heartbeat(node_epoch: u64, addr: &str) -> HeartbeatRequest {
            HeartbeatRequest {
                peer: Some(Peer::new(NODE_ID, addr)),
                node_epoch,
                ..heartbeat_request(Role::Datanode, NODE_ID)
            }
        }

        fn mailbox(pushers: Pushers) -> MailboxRef {
            let seq = SequenceBuilder::new("test_seq", Arc::new(MemoryKvBackend::new())).build();
            HeartbeatMailbox::create(pushers, seq)
        }

        async fn push(group: &HeartbeatHandlerGroup) {
            mailbox(group.pushers())
                .send_oneway(&Channel::Datanode(NODE_ID), MailboxMessage::default())
                .await
                .unwrap();
        }

        async fn lease_addr(ctx: &Context) -> Option<String> {
            let in_memory = ctx.in_memory.clone() as KvBackendRef;
            find_datanode_lease_value(&in_memory, NODE_ID)
                .await
                .unwrap()
                .map(|lease| lease.node_addr)
        }

        fn epochs() -> (u64, u64) {
            let now = current_time_millis() as u64;
            (now - 1000, now)
        }

        #[tokio::test]
        async fn test_newer_process_fences_the_older_one() {
            let group = new_group();
            let mut ctx = TestEnv::new().ctx();
            // A mailbox waiting for replies from the datanode.
            let pushers = Pushers::default();
            let (tx, _rx) = mpsc::channel(8);
            pushers
                .insert(PusherId::new(Role::Datanode, NODE_ID), Pusher::new(tx))
                .await;
            ctx.mailbox = mailbox(pushers);
            let wait_for_reply = || {
                ctx.mailbox.send(
                    &Channel::Datanode(NODE_ID),
                    MailboxMessage::default(),
                    Duration::from_millis(500),
                )
            };
            let (e1, e2) = epochs();
            let mut a1 = Stream::open(&group, &ctx).await;
            let mut a2 = Stream::open(&group, &ctx).await;

            let res = a1.send(heartbeat(e1, "a1")).await;
            assert!(res.region_lease.is_some());
            // A2 replaces A1 and records its epoch.
            let res = a2.send(heartbeat(e2, "a2")).await;
            assert!(res.region_lease.is_some());

            // A1 is fenced: no lease, no keep-alive, no error that would make it reconnect.
            let res = a1.send(heartbeat(e1, "a1")).await;
            assert!(res.region_lease.is_none());
            assert!(res.header.unwrap().error.is_none());
            assert_eq!(lease_addr(&ctx).await.as_deref(), Some("a2"));

            // A1's mailbox reply is dropped, A2's is delivered.
            let a1_reply = wait_for_reply().await.unwrap();
            let a2_reply = wait_for_reply().await.unwrap();
            for (stream, reply, epoch) in [(&mut a1, &a1_reply, e1), (&mut a2, &a2_reply, e2)] {
                let mut req = heartbeat(epoch, "a");
                req.mailbox_message = Some(MailboxMessage {
                    id: reply.message_id(),
                    ..Default::default()
                });
                stream.send(req).await;
            }
            assert!(a1_reply.await.is_err());
            assert!(a2_reply.await.is_ok());
        }

        #[tokio::test]
        async fn test_pusher_follows_the_newer_process() {
            let group = new_group();
            let ctx = TestEnv::new().ctx();
            let (e1, e2) = epochs();
            let mut a1 = Stream::open(&group, &ctx).await;
            a1.send(heartbeat(e1, "a1")).await;
            let mut a2 = Stream::open(&group, &ctx).await;
            a2.send(heartbeat(e2, "a2")).await;
            push(&group).await;
            assert!(a2.received_push());

            // A1's next request and its new stream do not take the pusher back.
            a1.send(heartbeat(e1, "a1")).await;
            let mut a1_again = Stream::open(&group, &ctx).await;
            a1_again.send(heartbeat(e1, "a1")).await;
            push(&group).await;
            assert!(a2.received_push());

            // A stream ending removes only its own pusher.
            a1.session.run().await;
            a1_again.session.run().await;
            push(&group).await;
            assert!(a2.received_push());
            // A2's new stream takes over with the same epoch, and A2's old stream ending
            // keeps it.
            let mut a2_again = Stream::open(&group, &ctx).await;
            a2_again.send(heartbeat(e2, "a2")).await;
            a2.session.run().await;
            push(&group).await;
            assert!(a2_again.received_push());
        }

        #[tokio::test]
        async fn test_datanode_without_epoch_is_not_checked() {
            let group = new_group();
            let ctx = TestEnv::new().ctx();
            let mut a1 = Stream::open(&group, &ctx).await;

            let res = a1.send(heartbeat(0, "a1")).await;
            assert!(res.region_lease.is_some());
            let key = DatanodeEpochKey::new(NODE_ID).to_bytes();
            assert!(ctx.kv_backend.get(&key).await.unwrap().is_none());
            push(&group).await;
            assert!(a1.received_push());
        }
    }
}
