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

use std::cmp::Ordering;
use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use api::v1::meta::{
    AskLeaderRequest, AskLeaderResponse, Error, HeartbeatRequest, HeartbeatResponse,
    PROTOCOL_VERSION, Peer, RequestHeader, ResponseHeader, Role, heartbeat_server,
};
use common_meta::election::LeaderChangeMessage;
use common_meta::key::MetadataValue;
use common_meta::key::datanode_epoch::{DatanodeEpochKey, DatanodeEpochValue};
use common_meta::kv_backend::KvBackend;
use common_meta::rpc::store::CompareAndPutRequest;
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
use crate::handler::{HeartbeatHandlerGroup, Pusher, PusherId};
use crate::metasrv::{Context, ElectionRef, Metasrv};
use crate::metrics::METRIC_META_HEARTBEAT_RECV;
use crate::service::{GrpcResult, GrpcStream};

type HeartbeatResponseResult = std::result::Result<HeartbeatResponse, Status>;

/// How far ahead of the leader's clock a datanode epoch can be recorded.
const MAX_NODE_EPOCH_AHEAD: Duration = Duration::from_secs(5 * 60);

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
    /// The node epoch the stream's pusher was registered with, if any.
    pusher_epoch: Option<u64>,
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
        // A datanode stream registers its pusher on its first request that passes the check.
        if sender_id.role != Role::Datanode {
            let pusher = Pusher::new(tx.clone());
            handler_group.register_pusher(sender_id, pusher).await;
        }
        let mut session = Self {
            requests,
            tx,
            leader_step_down,
            handler_group,
            ctx,
            sender_id,
            pusher_epoch: None,
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

        let res = match self.check_node_epoch(&req, is_handshake).await {
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

    /// Checks the node epoch of a datanode request, and registers the stream's pusher once
    /// the request passes. Returns the response to send instead of running the handlers, if any.
    async fn check_node_epoch(
        &mut self,
        req: &HeartbeatRequest,
        is_handshake: bool,
    ) -> Option<Result<HeartbeatResponse>> {
        let node_epoch = req.node_epoch;
        if self.sender_id.role != Role::Datanode || (node_epoch == 0 && is_handshake) {
            return None;
        }
        // Datanodes that do not report their epoch are not checked.
        if node_epoch != 0 {
            if let Some(election) = &self.ctx.election
                && !election.is_leader()
            {
                return Some(Ok(header_only(Some(Error::is_not_leader()))));
            }
            let kv_backend = &*self.ctx.leader_cached_kv_backend;
            match accept_node_epoch(kv_backend, self.sender_id.id, node_epoch).await {
                Ok(true) => {}
                // No error, so the datanode does not reconnect.
                Ok(false) => {
                    self.handler_group
                        .deregister_push(self.sender_id, &self.tx)
                        .await;
                    return Some(Ok(header_only(None)));
                }
                Err(e) => return Some(Err(e)),
            }
        }
        // Before the handlers run, so the stream receives what they broadcast.
        if self.pusher_epoch.is_none_or(|epoch| node_epoch > epoch) {
            self.pusher_epoch = Some(node_epoch);
            let pusher = Pusher::new(self.tx.clone()).with_node_epoch(node_epoch);
            self.handler_group
                .register_pusher(self.sender_id, pusher)
                .await;
        }
        None
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

/// Returns whether `node_epoch` is not lower than the highest epoch recorded for the datanode,
/// raising the record when it is higher. An epoch too far ahead of this metasrv's clock is
/// refused, since recording it would fence every later process.
async fn accept_node_epoch(
    kv_backend: &dyn KvBackend<Error = common_meta::error::Error>,
    node_id: u64,
    node_epoch: u64,
) -> Result<bool> {
    let key = DatanodeEpochKey::new(node_id).to_bytes();
    let mut current = kv_backend.get(&key).await.context(error::KvBackendSnafu)?;
    let limit = current_time_millis().max(0) as u64 + MAX_NODE_EPOCH_AHEAD.as_millis() as u64;
    loop {
        let (expect, highest) = match current {
            Some(kv) => {
                let value = DatanodeEpochValue::try_from_raw_value(&kv.value)
                    .context(error::KvBackendSnafu)?;
                (kv.value, value.node_epoch)
            }
            None => (vec![], 0),
        };
        match node_epoch.cmp(&highest) {
            Ordering::Equal => return Ok(true),
            Ordering::Greater if node_epoch <= limit => {}
            _ => {
                warn!("Fenced datanode {node_id} with epoch {node_epoch}, recorded {highest}");
                return Ok(false);
            }
        }
        let value = DatanodeEpochValue { node_epoch }
            .try_as_raw_value()
            .context(error::KvBackendSnafu)?;
        let req = CompareAndPutRequest {
            key: key.clone(),
            expect,
            value,
        };
        let resp = kv_backend
            .compare_and_put(req)
            .await
            .context(error::KvBackendSnafu)?;
        if resp.success {
            info!("Recorded epoch {node_epoch} of datanode {node_id}");
            return Ok(true);
        }
        current = resp.prev_kv;
    }
}

fn header_only(error: Option<Error>) -> HeartbeatResponse {
    HeartbeatResponse {
        header: Some(ResponseHeader {
            protocol_version: PROTOCOL_VERSION,
            error,
        }),
        ..Default::default()
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

    mod node_epoch {
        use std::sync::Arc;
        use std::time::Duration;

        use api::v1::meta::{HeartbeatRequest, HeartbeatResponse, MailboxMessage, Peer, Role};
        use common_meta::error::UnexpectedSnafu;
        use common_meta::key::datanode_epoch::{DatanodeEpochKey, DatanodeEpochValue};
        use common_meta::key::{MetadataValue, TableMetadataManager};
        use common_meta::kv_backend::memory::MemoryKvBackend;
        use common_meta::kv_backend::test_util::MockKvBackendBuilder;
        use common_meta::kv_backend::{KvBackend, KvBackendRef, ResettableKvBackend};
        use common_meta::region_keeper::MemoryRegionKeeper;
        use common_meta::rpc::store::{PutRequest, RangeResponse};
        use common_meta::sequence::SequenceBuilder;
        use common_time::util::current_time_millis;
        use tokio::sync::mpsc;

        use super::{MockHeartbeatRequestStream, MockLeaderStepDown, heartbeat_request};
        use crate::discovery::utils::find_datanode_lease_value;
        use crate::handler::region_lease_handler::RegionLeaseHandler;
        use crate::handler::test_utils::TestEnv;
        use crate::handler::{
            HandleControl, HeartbeatAccumulator, HeartbeatHandler, HeartbeatHandlerGroup,
            HeartbeatHandlerGroupBuilder, HeartbeatMailbox, Pusher, PusherId, Pushers,
        };
        use crate::metasrv::Context;
        use crate::service::heartbeat::{
            HeartbeatResponseResult, HeartbeatSession, accept_node_epoch,
        };
        use crate::service::mailbox::{BroadcastChannel, Channel, MailboxRef};
        use crate::service::store::cached_kv::LeaderCachedKvBackend;

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
            let mut builder = HeartbeatHandlerGroupBuilder::new(Pushers::default())
                .with_region_lease_handler(Some(lease_handler))
                .add_default_handlers();
            builder
                .add_handler_after("ResponseHeaderHandler", BroadcastHandler)
                .unwrap();
            Arc::new(builder.build().unwrap())
        }

        /// Broadcasts to datanodes while handling a request from the address "broadcast".
        struct BroadcastHandler;

        #[async_trait::async_trait]
        impl HeartbeatHandler for BroadcastHandler {
            fn is_acceptable(&self, role: Role) -> bool {
                role == Role::Datanode
            }

            async fn handle(
                &self,
                req: &HeartbeatRequest,
                ctx: &mut Context,
                _: &mut HeartbeatAccumulator,
            ) -> crate::error::Result<HandleControl> {
                if req
                    .peer
                    .as_ref()
                    .is_some_and(|peer| peer.addr == "broadcast")
                {
                    let msg = MailboxMessage::default();
                    ctx.mailbox
                        .broadcast(&BroadcastChannel::Datanode, &msg)
                        .await?;
                }
                Ok(HandleControl::Continue)
            }
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

        /// Records `node_epoch` as the highest epoch of the node, through the leader cache.
        async fn record(ctx: &Context, node_epoch: u64) {
            let key = DatanodeEpochKey::new(NODE_ID).to_bytes();
            let value = DatanodeEpochValue { node_epoch }
                .try_as_raw_value()
                .unwrap();
            let req = PutRequest::new().with_key(key).with_value(value);
            ctx.leader_cached_kv_backend.put(req).await.unwrap();
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
            let mut ctx = TestEnv::new().ctx();
            ctx.mailbox = mailbox(group.pushers());
            let (e1, e2) = epochs();
            let mut a1 = Stream::open(&group, &ctx).await;
            a1.send(heartbeat(e1, "a1")).await;
            // A2 takes over before the handlers of its first heartbeat run, so it receives
            // what they broadcast ahead of the response.
            let mut a2 = Stream::open(&group, &ctx).await;
            assert!(
                a2.session
                    .handle_request(heartbeat(e2, "broadcast"), false)
                    .await
            );
            let broadcast = a2.rx.recv().await.unwrap().unwrap();
            assert!(broadcast.mailbox_message.is_some());
            let res = a2.rx.recv().await.unwrap().unwrap();
            assert!(res.region_lease.is_some());
            assert!(!a1.received_push());
            push(&group).await;
            assert!(a2.received_push());

            // A1's new stream does not take the pusher back.
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

            // A fenced stream removes its own pusher, and a handshake does not take the slot.
            record(&ctx, e2 + 1).await;
            a2_again.send(heartbeat(e2, "a2")).await;
            let pusher_id = PusherId::new(Role::Datanode, NODE_ID);
            assert!(!group.contains_pusher(&pusher_id).await);
            let _a1_reconnected = Stream::open(&group, &ctx).await;
            assert!(!group.contains_pusher(&pusher_id).await);
        }

        #[tokio::test]
        async fn test_kv_error_fails_the_request() {
            let group = new_group();
            let mut ctx = TestEnv::new().ctx();
            let kv_backend = MockKvBackendBuilder::default()
                .range_fn(Arc::new(|_| {
                    Ok(RangeResponse {
                        kvs: vec![],
                        more: false,
                    })
                }))
                .txn(Arc::new(|_| {
                    UnexpectedSnafu {
                        err_msg: "injected",
                    }
                    .fail()
                }))
                .build()
                .unwrap();
            let cached = LeaderCachedKvBackend::with_always_leader(Arc::new(kv_backend));
            ctx.leader_cached_kv_backend = Arc::new(cached);
            let (e1, _) = epochs();
            let mut a1 = Stream::open(&group, &ctx).await;

            let req = HeartbeatRequest {
                node_epoch: e1,
                ..heartbeat_request(Role::Datanode, NODE_ID)
            };
            assert!(a1.session.handle_request(req, false).await);
            assert!(a1.rx.recv().await.unwrap().is_err());
        }

        #[tokio::test]
        async fn test_first_epoch_keeps_waiting_messages() {
            let group = new_group();
            let mut ctx = TestEnv::new().ctx();
            ctx.mailbox = mailbox(group.pushers());
            let (e1, _) = epochs();
            let mut a1 = Stream::open(&group, &ctx).await;
            // A request without an epoch registers the pusher first.
            a1.send(heartbeat(0, "a1")).await;
            let reply = ctx
                .mailbox
                .send(
                    &Channel::Datanode(NODE_ID),
                    MailboxMessage::default(),
                    Duration::from_secs(10),
                )
                .await
                .unwrap();
            let id = a1
                .rx
                .recv()
                .await
                .unwrap()
                .unwrap()
                .mailbox_message
                .unwrap()
                .id;
            tokio::pin!(reply);

            // The stream's pusher takes the epoch of its first heartbeat without cancelling
            // the message, which a later heartbeat answers.
            a1.send(heartbeat(e1, "a1")).await;
            assert!(futures::poll!(reply.as_mut()).is_pending());
            let mut req = heartbeat(e1, "a1");
            req.mailbox_message = Some(MailboxMessage {
                id,
                ..Default::default()
            });
            a1.send(req).await;
            assert!(reply.await.is_ok());
        }

        #[tokio::test]
        async fn test_record_is_reloaded_on_leader_change() {
            let store: KvBackendRef = Arc::new(MemoryKvBackend::new());
            let cached = LeaderCachedKvBackend::with_always_leader(store.clone());
            let now = current_time_millis() as u64;
            let (e1, e2) = (now - 1000, now);
            assert!(accept_node_epoch(&cached, 1, e1).await.unwrap());
            assert!(accept_node_epoch(&cached, 1, e2).await.unwrap());

            // The cache is reset and loaded again when this metasrv becomes the leader. The record
            // is then served from the loaded cache, even without the store.
            cached.reset();
            cached.load().await.unwrap();
            store
                .delete(&DatanodeEpochKey::new(1).to_bytes(), false)
                .await
                .unwrap();
            assert!(!accept_node_epoch(&cached, 1, e1).await.unwrap());
            assert!(accept_node_epoch(&cached, 1, e2).await.unwrap());
        }

        #[tokio::test]
        async fn test_epoch_far_ahead_is_not_recorded() {
            let kv_backend = MemoryKvBackend::new();
            let now = current_time_millis() as u64;
            let ahead = now + Duration::from_secs(10 * 60).as_millis() as u64;
            assert!(!accept_node_epoch(&kv_backend, 1, ahead).await.unwrap());
            let key = DatanodeEpochKey::new(1).to_bytes();
            assert!(kv_backend.get(&key).await.unwrap().is_none());

            let slightly_ahead = now + Duration::from_secs(60).as_millis() as u64;
            assert!(
                accept_node_epoch(&kv_backend, 1, slightly_ahead)
                    .await
                    .unwrap()
            );
        }

        #[tokio::test]
        async fn test_datanode_without_epoch_is_not_checked() {
            let group = new_group();
            let ctx = TestEnv::new().ctx();
            // Even after another process of the node has recorded its epoch.
            let (_, e2) = epochs();
            record(&ctx, e2).await;
            let mut a1 = Stream::open(&group, &ctx).await;

            let res = a1.send(heartbeat(0, "a1")).await;
            assert!(res.region_lease.is_some());
            push(&group).await;
            assert!(a1.received_push());
        }
    }
}
