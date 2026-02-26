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
    AskLeaderRequest, AskLeaderResponse, HeartbeatRequest, HeartbeatResponse, Peer,
    PullMetaConfigRequest, PullMetaConfigResponse, RequestHeader, ResponseHeader, Role,
    heartbeat_server,
};
use common_options::meta_config::PluginOptionsSerializerRef;
use common_telemetry::{debug, error, info, warn};
use futures::StreamExt;
use once_cell::sync::OnceCell;
use snafu::OptionExt;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::error::{self, Result};
use crate::handler::{HeartbeatHandlerGroup, Pusher, PusherId};
use crate::metasrv::{Context, Metasrv};
use crate::metrics::METRIC_META_HEARTBEAT_RECV;
use crate::service::{GrpcResult, GrpcStream};

#[async_trait::async_trait]
impl heartbeat_server::Heartbeat for Metasrv {
    type HeartbeatStream = GrpcStream<HeartbeatResponse>;

    async fn heartbeat(
        &self,
        req: Request<Streaming<HeartbeatRequest>>,
    ) -> GrpcResult<Self::HeartbeatStream> {
        let mut in_stream = req.into_inner();
        let (tx, rx) = mpsc::channel(128);
        let handler_group = self.handler_group().context(error::UnexpectedSnafu {
            violated: "expected heartbeat handlers",
        })?;

        let ctx = self.new_ctx();
        let _handle = common_runtime::spawn_global(async move {
            let mut pusher_id = None;
            while let Some(msg) = in_stream.next().await {
                let mut is_not_leader = false;
                match msg {
                    Ok(req) => {
                        debug!("Receiving heartbeat request: {:?}", req);

                        let Some(header) = req.header.as_ref() else {
                            error!("Exit on malformed request: MissingRequestHeader");
                            let _ = tx
                                .send(Err(error::MissingRequestHeaderSnafu {}.build().into()))
                                .await;
                            break;
                        };

                        let is_handshake = pusher_id.is_none();
                        if is_handshake {
                            pusher_id =
                                Some(register_pusher(&handler_group, header, tx.clone()).await);
                        }
                        if let Some(k) = &pusher_id {
                            METRIC_META_HEARTBEAT_RECV.with_label_values(&[&k.to_string()]);
                        } else {
                            METRIC_META_HEARTBEAT_RECV.with_label_values(&["none"]);
                        }

                        let res = handler_group
                            .handle(req, ctx.clone().with_handshake(is_handshake))
                            .await
                            .inspect_err(|e| warn!(e; "Failed to handle heartbeat request, pusher: {pusher_id:?}", ))
                            .map_err(|e| e.into());

                        is_not_leader = res.as_ref().is_ok_and(|r| r.is_not_leader());

                        debug!("Sending heartbeat response: {:?}", res);

                        if tx.send(res).await.is_err() {
                            info!("ReceiverStream was dropped; shutting down");
                            break;
                        }
                    }
                    Err(err) => {
                        if let Some(io_err) = error::match_for_io_error(&err)
                            && io_err.kind() == ErrorKind::BrokenPipe
                        {
                            // client disconnected in unexpected way
                            error!("Client disconnected: broken pipe");
                            break;
                        }
                        error!(err; "Sending heartbeat response error");

                        if tx.send(Err(err)).await.is_err() {
                            info!("ReceiverStream was dropped; shutting down");
                            break;
                        }
                    }
                }

                if is_not_leader {
                    warn!("Quit because it is no longer the leader");
                    let _ = tx
                        .send(Err(Status::aborted(format!(
                            "The requested metasrv node is not leader, node addr: {}",
                            ctx.server_addr
                        ))))
                        .await;
                    break;
                }
            }

            info!("Heartbeat stream closed: {pusher_id:?}");

            if let Some(pusher_id) = pusher_id {
                let _ = handler_group.deregister_push(pusher_id).await;
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

    async fn pull_meta_config(
        &self,
        req: Request<PullMetaConfigRequest>,
    ) -> GrpcResult<PullMetaConfigResponse> {
        let payload = self
            .plugins()
            .get::<PluginOptionsSerializerRef>()
            .and_then(|p| p.serialize().ok())
            .unwrap_or_default();

        let res = PullMetaConfigResponse {
            header: Some(ResponseHeader::success()),
            payload,
        };

        let member_id = req.into_inner().header.as_ref().map(|h| h.member_id);
        info!("Sending meta config to member: {member_id:?}");

        Ok(Response::new(res))
    }
}

async fn handle_ask_leader(_req: AskLeaderRequest, ctx: Context) -> Result<AskLeaderResponse> {
    let addr = match ctx.election {
        Some(election) => {
            if election.is_leader() {
                ctx.server_addr
            } else {
                election.leader().await?.0
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
    use std::sync::Arc;

    use api::v1::meta::heartbeat_server::Heartbeat;
    use api::v1::meta::*;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_telemetry::tracing_context::W3cTrace;
    use servers::grpc::GrpcOptions;
    use tonic::IntoRequest;

    use super::get_node_id;
    use crate::metasrv::MetasrvOptions;
    use crate::metasrv::builder::MetasrvBuilder;

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
