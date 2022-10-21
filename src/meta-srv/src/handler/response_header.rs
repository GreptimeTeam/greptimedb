use api::v1::meta::HeartbeatRequest;
use api::v1::meta::HeartbeatResponse;
use api::v1::meta::ResponseHeader;
use api::v1::meta::PROTOCOL_VERSION;

use super::HeartbeatHandler;
use crate::error::Result;
use crate::service::store::kv::KvStoreRef;

pub struct ResponseHeaderHandler;

#[async_trait::async_trait]
impl HeartbeatHandler for ResponseHeaderHandler {
    async fn handle(
        &self,
        req: &HeartbeatRequest,
        res: HeartbeatResponse,
        _store: KvStoreRef,
    ) -> Result<HeartbeatResponse> {
        let HeartbeatRequest { header, .. } = req;
        let res_header = ResponseHeader {
            protocol_version: PROTOCOL_VERSION,
            cluster_id: header.as_ref().map_or(0, |h| h.cluster_id),
            ..Default::default()
        };

        let res = HeartbeatResponse {
            header: Some(res_header),
            ..res
        };

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::meta::RequestHeader;

    use super::*;
    use crate::service::store::noop::NoopKvStore;

    #[tokio::test]
    async fn test_handle_heartbeat_resp_header() {
        let kv_store = Arc::new(NoopKvStore {});

        let header = RequestHeader::new(1, 2);
        let req = HeartbeatRequest::new(header);
        let res = HeartbeatResponse::default();

        let response_handler = ResponseHeaderHandler {};
        let res = response_handler.handle(&req, res, kv_store).await.unwrap();

        assert_eq!(1, res.header.unwrap().cluster_id);
    }
}
