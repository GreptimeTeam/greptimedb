use std::io::ErrorKind;

use api::v1::meta::heartbeat_server;
use api::v1::meta::AskLeaderRequest;
use api::v1::meta::AskLeaderResponse;
use api::v1::meta::Endpoint;
use api::v1::meta::HeartbeatRequest;
use api::v1::meta::HeartbeatResponse;
use api::v1::meta::ResponseHeader;
use api::v1::meta::PROTOCOL_VERSION;
use common_telemetry::error;
use common_telemetry::info;
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::Response;
use tonic::Streaming;

use super::GrpcResult;
use super::GrpcStream;
use crate::error;
use crate::error::Result;
use crate::metasrv::MetaSrv;

#[async_trait::async_trait]
impl heartbeat_server::Heartbeat for MetaSrv {
    type HeartbeatStream = GrpcStream<HeartbeatResponse>;

    async fn heartbeat(
        &self,
        req: Request<Streaming<HeartbeatRequest>>,
    ) -> GrpcResult<Self::HeartbeatStream> {
        let mut in_stream = req.into_inner();
        let (tx, rx) = mpsc::channel(128);
        let handlers = self.heartbeat_handlers();
        common_runtime::spawn_bg(async move {
            let mut pusher_key = None;

            while let Some(msg) = in_stream.next().await {
                match msg {
                    Ok(req) => {
                        if pusher_key.is_none() {
                            if let Some(ref peer) = req.peer {
                                let key = format!(
                                    "{}/{}",
                                    peer.id,
                                    peer.endpoint.as_ref().map_or("unknow", |ep| &ep.addr)
                                );
                                handlers.register(&key, tx.clone()).await;
                                pusher_key = Some(key);
                            }
                        }

                        tx.send(handlers.handle(req).await.map_err(|e| e.into()))
                            .await
                            .expect("working rx");
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
            }

            info!(
                "Heartbeat stream broken: {:?}",
                pusher_key.as_ref().unwrap_or(&"unknow".to_string())
            );

            if let Some(key) = pusher_key {
                let _ = handlers.unregister(&key);
            }
        });

        let out_stream = ReceiverStream::new(rx);

        Ok(Response::new(Box::pin(out_stream) as Self::HeartbeatStream))
    }

    async fn ask_leader(&self, req: Request<AskLeaderRequest>) -> GrpcResult<AskLeaderResponse> {
        let req = req.into_inner();
        let res = self.handle_ask_leader(req).await?;

        Ok(Response::new(res))
    }
}

impl MetaSrv {
    // TODO(jiachun): move out when we can get the leader peer from kv store
    async fn handle_ask_leader(&self, req: AskLeaderRequest) -> Result<AskLeaderResponse> {
        let AskLeaderRequest { header, .. } = req;

        let res_header = ResponseHeader {
            protocol_version: PROTOCOL_VERSION,
            cluster_id: header.map_or(0u64, |h| h.cluster_id),
            ..Default::default()
        };

        // TODO(jiachun): return leader
        let res = AskLeaderResponse {
            header: Some(res_header),
            leader: Some(Endpoint {
                addr: self.options().server_addr.clone(),
            }),
        };

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::meta::heartbeat_server::Heartbeat;
    use api::v1::meta::*;
    use tonic::IntoRequest;

    use super::*;
    use crate::metasrv::MetaSrvOptions;
    use crate::service::store::noop::NoopKvStore;

    #[tokio::test]
    async fn test_ask_leader() {
        let kv_store = Arc::new(NoopKvStore {});
        let meta_srv = MetaSrv::new(MetaSrvOptions::default(), kv_store).await;

        let header = RequestHeader::new(1, 1);
        let req = AskLeaderRequest::new(header);

        let res = meta_srv.ask_leader(req.into_request()).await.unwrap();
        let res = res.into_inner();
        assert_eq!(1, res.header.unwrap().cluster_id);
        assert_eq!(meta_srv.options().bind_addr, res.leader.unwrap().addr);
    }
}
