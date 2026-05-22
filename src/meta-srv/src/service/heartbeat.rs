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
        debug!("Receiving heartbeat request: {:?}", req);

        if !self.handler_group.contains_pusher(&self.sender_id).await
            && register_pusher_if_missing(&self.handler_group, self.sender_id, &self.tx).await
        {
            info!(
                "Re-register sender for existing heartbeat stream, sender: {}",
                self.sender_id
            );
        }

        METRIC_META_HEARTBEAT_RECV.with_label_values(&[&self.sender_id.to_string()]);

        let res = self
            .handler_group
            .handle(req, self.ctx.clone().with_handshake(is_handshake))
            .await
            .inspect_err(
                |e| warn!(e; "Failed to handle heartbeat request, sender: {}", self.sender_id),
            )
            .map_err(|e| e.into());

        let is_not_leader = res.as_ref().is_ok_and(|r| r.is_not_leader());

        debug!("Sending heartbeat response: {:?}", res);

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
        let _ = self.handler_group.deregister_push(self.sender_id).await;
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

/// Registers the heartbeat response [`Pusher`] with the given key to the group if absent,
/// and returns whether the pusher is inserted.
async fn register_pusher_if_missing(
    handler_group: &HeartbeatHandlerGroup,
    pusher_id: PusherId,
    sender: &Sender<std::result::Result<HeartbeatResponse, tonic::Status>>,
) -> bool {
    let pusher = Pusher::new(sender.clone());
    handler_group
        .register_pusher_if_absent(pusher_id, pusher)
        .await
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Arc;

    use api::v1::meta::heartbeat_server::Heartbeat;
    use api::v1::meta::*;
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
    async fn test_heartbeat_session_reregisters_missing_sender() {
        let (tx, mut rx) = mpsc::channel(8);
        let handler_group = test_handler_group();
        let sender_id = sender_id(Role::Datanode, 42);
        let requests =
            MockHeartbeatRequestStream::new(vec![Ok(heartbeat_request(Role::Datanode, 42))]);
        let mut session = init_session(
            requests,
            tx,
            None::<MockLeaderStepDown>,
            handler_group.clone(),
        )
        .await
        .unwrap();
        let _ = recv_response(&mut rx).await.unwrap();
        handler_group.deregister_push(sender_id).await;
        assert!(!handler_group.contains_pusher(&sender_id).await);

        assert!(
            session
                .handle_request(heartbeat_request(Role::Datanode, 42), false)
                .await
        );

        assert!(handler_group.contains_pusher(&sender_id).await);
        let response = recv_response(&mut rx).await.unwrap();
        assert!(response.heartbeat_config.is_none());
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
}
