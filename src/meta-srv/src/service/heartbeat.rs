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
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use api::v1::meta::{
    heartbeat_server, AskLeaderRequest, AskLeaderResponse, HeartbeatRequest, HeartbeatResponse,
    Peer, RequestHeader, ResponseHeader, Role,
};
use common_telemetry::{debug, error, info, warn};
use futures::StreamExt;
use once_cell::sync::OnceCell;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Streaming};

use crate::error;
use crate::error::Result;
use crate::handler::Pusher;
use crate::metasrv::{Context, MetaSrv};
use crate::service::{GrpcResult, GrpcStream};

#[async_trait::async_trait]
impl heartbeat_server::Heartbeat for MetaSrv {
    type HeartbeatStream = GrpcStream<HeartbeatResponse>;

    async fn heartbeat(
        &self,
        req: Request<Streaming<HeartbeatRequest>>,
    ) -> GrpcResult<Self::HeartbeatStream> {
        let mut in_stream = req.into_inner();
        let (tx, rx) = mpsc::channel(128);
        let handler_group = self.handler_group().clone();
        let ctx = self.new_ctx();
        let _handle = common_runtime::spawn_bg(async move {
            let mut pusher_key = None;
            while let Some(msg) = in_stream.next().await {
                let mut is_not_leader = false;
                match msg {
                    Ok(req) => {
                        let header = match req.header.as_ref() {
                            Some(header) => header,
                            None => {
                                let err = error::MissingRequestHeaderSnafu {}.build();
                                error!("Exit on malformed request: MissingRequestHeader");
                                let _ = tx.send(Err(err.into())).await;
                                break;
                            }
                        };

                        debug!("Receiving heartbeat request: {:?}", req);

                        if pusher_key.is_none() {
                            let node_id = get_node_id(header);
                            let role = header.role() as i32;
                            let key = format!("{}-{}", role, node_id);
                            let pusher = Pusher::new(tx.clone(), header);
                            handler_group.register(&key, pusher).await;
                            pusher_key = Some(key);
                        }

                        let res = handler_group
                            .handle(req, ctx.clone())
                            .await
                            .map_err(|e| e.into());

                        is_not_leader = res.as_ref().map_or(false, |r| r.is_not_leader());

                        debug!("Sending heartbeat response: {:?}", res);
                        if tx.send(res).await.is_err() {
                            info!("ReceiverStream was dropped; shutting down");
                            break;
                        }
                    }
                    Err(err) => {
                        if let Some(io_err) = error::match_for_io_error(&err) {
                            if io_err.kind() == ErrorKind::BrokenPipe {
                                // client disconnected in unexpected way
                                error!("Client disconnected: broken pipe");
                                break;
                            }
                        }

                        if tx.send(Err(err)).await.is_err() {
                            info!("ReceiverStream was dropped; shutting down");
                            break;
                        }
                    }
                }

                if is_not_leader {
                    warn!("Quit because it is no longer the leader");
                    break;
                }
            }

            info!(
                "Heartbeat stream closed: {:?}",
                pusher_key.as_ref().unwrap_or(&"unknown".to_string())
            );

            if let Some(key) = pusher_key {
                let _ = handler_group.deregister(&key).await;
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

async fn handle_ask_leader(req: AskLeaderRequest, ctx: Context) -> Result<AskLeaderResponse> {
    let cluster_id = req.header.as_ref().map_or(0, |h| h.cluster_id);

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

    let header = Some(ResponseHeader::success(cluster_id));
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
        Role::Datanode => header.member_id,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::meta::heartbeat_server::Heartbeat;
    use api::v1::meta::*;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_telemetry::tracing_context::W3cTrace;
    use tonic::IntoRequest;

    use super::get_node_id;
    use crate::metasrv::builder::MetaSrvBuilder;

    #[tokio::test]
    async fn test_ask_leader() {
        let kv_backend = Arc::new(MemoryKvBackend::new());

        let meta_srv = MetaSrvBuilder::new()
            .kv_backend(kv_backend)
            .build()
            .await
            .unwrap();

        let req = AskLeaderRequest {
            header: Some(RequestHeader::new((1, 1), Role::Datanode, W3cTrace::new())),
        };

        let res = meta_srv.ask_leader(req.into_request()).await.unwrap();
        let res = res.into_inner();
        assert_eq!(1, res.header.unwrap().cluster_id);
        assert_eq!(meta_srv.options().bind_addr, res.leader.unwrap().addr);
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
