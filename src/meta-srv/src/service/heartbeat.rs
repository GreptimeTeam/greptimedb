use api::v1::meta::heartbeat_server;
use api::v1::meta::AskLeaderRequest;
use api::v1::meta::AskLeaderResponse;
use api::v1::meta::Endpoint;
use api::v1::meta::HeartbeatRequest;
use api::v1::meta::HeartbeatResponse;
use api::v1::meta::ResponseHeader;
use api::v1::meta::PROTOCOL_VERSION;
use futures::StreamExt;
use futures::TryFutureExt;
use snafu::OptionExt;
use tonic::Request;
use tonic::Response;
use tonic::Streaming;

use super::store::kv::KvStoreRef;
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
        let req = req.into_inner();
        let kv_store = self.kv_store();
        let res = handle_ask_leader(req, kv_store).await?;

        Ok(Response::new(res))
    }
}

async fn handle_heartbeat(msg: HeartbeatRequest) -> Result<HeartbeatResponse> {
    let HeartbeatRequest { header, .. } = msg;

    let res_header = ResponseHeader {
        protocol_version: PROTOCOL_VERSION,
        cluster_id: header.map_or(0, |h| h.cluster_id),
        ..Default::default()
    };

    // TODO(jiachun) Do something high-end

    let res = HeartbeatResponse {
        header: Some(res_header),
        ..Default::default()
    };

    Ok(res)
}

async fn handle_ask_leader(
    req: AskLeaderRequest,
    _kv_store: KvStoreRef,
) -> Result<AskLeaderResponse> {
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
            addr: "127.0.0.1:3002".to_string(),
        }),
    };

    Ok(res)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::meta::heartbeat_server::Heartbeat;
    use api::v1::meta::*;
    use tonic::IntoRequest;

    use super::*;
    use crate::metasrv::MetaSrvOptions;
    use crate::service::store::kv::KvStore;

    #[derive(Clone)]
    pub struct NoopKvStore {
        _opts: MetaSrvOptions,
    }

    impl NoopKvStore {
        pub fn new(opts: MetaSrvOptions) -> Self {
            Self { _opts: opts }
        }
    }

    #[async_trait::async_trait]
    impl KvStore for NoopKvStore {
        async fn range(&self, _req: RangeRequest) -> crate::Result<RangeResponse> {
            unreachable!()
        }

        async fn put(&self, _req: PutRequest) -> crate::Result<PutResponse> {
            unreachable!()
        }

        async fn delete_range(
            &self,
            _req: DeleteRangeRequest,
        ) -> crate::Result<DeleteRangeResponse> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn test_handle_heartbeat_resp_header() {
        let header = RequestHeader::new(1, 2);
        let req = HeartbeatRequest::new(header);

        let res = handle_heartbeat(req).await.unwrap();

        assert_eq!(1, res.header.unwrap().cluster_id);
    }

    #[tokio::test]
    async fn test_ask_leader() {
        let kv_store = Arc::new(NoopKvStore::new(MetaSrvOptions::default()));
        let meta_srv = MetaSrv::new(kv_store);

        let header = RequestHeader::new(1, 1);
        let req = AskLeaderRequest::new(header);

        let res = meta_srv.ask_leader(req.into_request()).await.unwrap();
        let res = res.into_inner();
        assert_eq!(1, res.header.unwrap().cluster_id);
        assert_eq!("127.0.0.1:3002".to_string(), res.leader.unwrap().addr);
    }
}
