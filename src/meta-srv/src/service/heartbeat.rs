use api::v1::meta::{
    heartbeat_server, AskLeaderRequest, AskLeaderResponse, HeartbeatRequest, HeartbeatResponse,
    ResponseHeader,
};
use futures::{StreamExt, TryFutureExt};
use snafu::OptionExt;
use tonic::{Request, Response, Streaming};

use super::{GrpcResult, GrpcStream, MetaServer, PROTOCOL_VERSION};
use crate::error::{self, Result};

#[async_trait::async_trait]
impl heartbeat_server::Heartbeat for MetaServer {
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

    async fn ask_leader(&self, _req: Request<AskLeaderRequest>) -> GrpcResult<AskLeaderResponse> {
        todo!()
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
