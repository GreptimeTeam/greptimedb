use std::io::ErrorKind;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use api::v1::meta::heartbeat_server;
use api::v1::meta::AskLeaderRequest;
use api::v1::meta::AskLeaderResponse;
use api::v1::meta::HeartbeatRequest;
use api::v1::meta::HeartbeatResponse;
use api::v1::meta::Peer;
use api::v1::meta::ResponseHeader;
use common_telemetry::error;
use common_telemetry::info;
use common_telemetry::warn;
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::Response;
use tonic::Streaming;

use crate::error;
use crate::error::Result;
use crate::metasrv::Context;
use crate::metasrv::MetaSrv;
use crate::service::GrpcResult;
use crate::service::GrpcStream;

static PUSHER_ID: AtomicU64 = AtomicU64::new(0);

#[async_trait::async_trait]
impl heartbeat_server::Heartbeat for MetaSrv {
    type HeartbeatStream = GrpcStream<HeartbeatResponse>;

    async fn heartbeat(
        &self,
        req: Request<Streaming<HeartbeatRequest>>,
    ) -> GrpcResult<Self::HeartbeatStream> {
        let mut in_stream = req.into_inner();
        let (tx, rx) = mpsc::channel(128);
        let handler_group = self.handler_group();
        let ctx = self.new_ctx();
        common_runtime::spawn_bg(async move {
            let mut pusher_key = None;
            while let Some(msg) = in_stream.next().await {
                let mut quit = false;
                match msg {
                    Ok(req) => {
                        if pusher_key.is_none() {
                            if let Some(peer) = &req.peer {
                                let key = format!(
                                    "{}-{}-{}",
                                    peer.addr,
                                    peer.id,
                                    PUSHER_ID.fetch_add(1, Ordering::Relaxed)
                                );
                                handler_group.register(&key, tx.clone()).await;
                                pusher_key = Some(key);
                            }
                        }

                        let res = handler_group
                            .handle(req, ctx.clone())
                            .await
                            .map_err(|e| e.into());

                        if let Ok(res) = &res {
                            quit = res.is_not_leader();
                        }

                        tx.send(res).await.expect("working rx");
                    }
                    Err(err) => {
                        if let Some(io_err) = error::match_for_io_error(&err) {
                            if io_err.kind() == ErrorKind::BrokenPipe {
                                // client disconnected in unexpected way
                                error!("Client disconnected: broken pipe");
                                break;
                            }
                        }

                        match tx.send(Err(err)).await {
                            Ok(_) => (),
                            Err(_err) => break, // response was droped
                        }
                    }
                }

                if quit {
                    warn!("Quit because it is no longer the leader");
                    break;
                }
            }
            info!(
                "Heartbeat stream broken: {:?}",
                pusher_key.as_ref().unwrap_or(&"unknow".to_string())
            );
            if let Some(key) = pusher_key {
                let _ = handler_group.unregister(&key);
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::meta::heartbeat_server::Heartbeat;
    use api::v1::meta::*;
    use tonic::IntoRequest;

    use super::*;
    use crate::metasrv::MetaSrvOptions;
    use crate::service::store::memory::MemStore;

    #[tokio::test]
    async fn test_ask_leader() {
        let kv_store = Arc::new(MemStore::new());
        let meta_srv = MetaSrv::new(MetaSrvOptions::default(), kv_store, None, None).await;

        let req = AskLeaderRequest {
            header: Some(RequestHeader::new((1, 1))),
        };

        let res = meta_srv.ask_leader(req.into_request()).await.unwrap();
        let res = res.into_inner();
        assert_eq!(1, res.header.unwrap().cluster_id);
        assert_eq!(meta_srv.options().bind_addr, res.leader.unwrap().addr);
    }
}
