use api::v1::meta::heartbeat_server;
use api::v1::meta::AskLeaderRequest;
use api::v1::meta::AskLeaderResponse;
use api::v1::meta::Endpoint;
use api::v1::meta::HeartbeatRequest;
use api::v1::meta::HeartbeatResponse;
use api::v1::meta::ResponseHeader;
use futures::StreamExt;
use futures::TryFutureExt;
use snafu::OptionExt;
use tonic::Request;
use tonic::Response;
use tonic::Streaming;

use super::GrpcResult;
use super::GrpcStream;
use super::PROTOCOL_VERSION;
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
        let msg = req
            .into_inner()
            .next()
            .await
            .context(error::StreamNoneSnafu {})??;

        let res = handle_heartbeat(msg).map_err(|e| e.into());

        let output = futures::stream::once(res);

        Ok(Response::new(Box::pin(output)))
    }

    async fn ask_leader(&self, req: Request<AskLeaderRequest>) -> GrpcResult<AskLeaderResponse> {
        let AskLeaderRequest { header, .. } = req.into_inner();

        let res_header = ResponseHeader {
            protocol_version: PROTOCOL_VERSION,
            cluster_id: header.map_or(0u64, |h| h.cluster_id),
            ..Default::default()
        };

        // TODO(jiachun): return leader
        let res = AskLeaderResponse {
            header: Some(res_header),
            leader: Some(Endpoint {
                addr: "127.0.0.1:3002".to_string(),
            }),
        };

        Ok(Response::new(res))
    }
}

async fn handle_heartbeat(msg: HeartbeatRequest) -> Result<HeartbeatResponse> {
    let HeartbeatRequest { header, .. } = msg;

    let res_header = ResponseHeader {
        protocol_version: PROTOCOL_VERSION,
        cluster_id: header.map_or(0u64, |h| h.cluster_id),
        ..Default::default()
    };

    // TODO(jiachun) Do something high-end

    let res = HeartbeatResponse {
        header: Some(res_header),
        ..Default::default()
    };

    Ok(res)
}
