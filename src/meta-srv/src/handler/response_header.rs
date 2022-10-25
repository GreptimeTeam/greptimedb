use api::v1::meta::HeartbeatRequest;
use api::v1::meta::ResponseHeader;
use api::v1::meta::PROTOCOL_VERSION;

use super::Context;
use super::HeartbeatHandler;
use crate::error::Result;
use crate::service::store::kv::KvStoreRef;

pub struct ResponseHeaderHandler;

#[async_trait::async_trait]
impl HeartbeatHandler for ResponseHeaderHandler {
    async fn handle(
        &self,
        req: &HeartbeatRequest,
        ctx: &mut Context,
        _store: KvStoreRef,
    ) -> Result<()> {
        let HeartbeatRequest { header, .. } = req;
        let res_header = ResponseHeader {
            protocol_version: PROTOCOL_VERSION,
            cluster_id: header.as_ref().map_or(0, |h| h.cluster_id),
            ..Default::default()
        };
        ctx.header = Some(res_header);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::meta::{request_header, HeartbeatResponse};

    use super::*;
    use crate::service::store::noop::NoopKvStore;

    #[tokio::test]
    async fn test_handle_heartbeat_resp_header() {
        let kv_store = Arc::new(NoopKvStore {});
        let req = HeartbeatRequest {
            header: request_header((1, 2)),
            ..Default::default()
        };
        let mut ctx = Context::default();

        let response_handler = ResponseHeaderHandler {};
        response_handler
            .handle(&req, &mut ctx, kv_store)
            .await
            .unwrap();
        let res: HeartbeatResponse = ctx.into();
        assert_eq!(1, res.header.unwrap().cluster_id);
    }
}
