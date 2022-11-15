use api::v1::meta::HeartbeatRequest;
use api::v1::meta::PutRequest;
use common_telemetry::info;
use common_time::util as time_util;

use crate::error::Result;
use crate::handler::HeartbeatAccumulator;
use crate::handler::HeartbeatHandler;
use crate::keys::LeaseKey;
use crate::keys::LeaseValue;
use crate::metasrv::Context;

pub struct DatanodeLeaseHandler;

#[async_trait::async_trait]
impl HeartbeatHandler for DatanodeLeaseHandler {
    async fn handle(
        &self,
        req: &HeartbeatRequest,
        ctx: &Context,
        _acc: &mut HeartbeatAccumulator,
    ) -> Result<()> {
        if ctx.is_skip_all() {
            return Ok(());
        }

        let HeartbeatRequest { header, peer, .. } = req;
        if let Some(peer) = &peer {
            let key = LeaseKey {
                cluster_id: header.as_ref().map_or(0, |h| h.cluster_id),
                node_id: peer.id,
            };
            let value = LeaseValue {
                timestamp_millis: time_util::current_time_millis(),
                node_addr: peer.addr.clone(),
            };

            info!("Receive a heartbeat: {:?}, {:?}", key, value);

            let key = key.try_into()?;
            let value = value.try_into()?;
            let put = PutRequest {
                key,
                value,
                ..Default::default()
            };

            ctx.kv_store.put(put).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    use api::v1::meta::Peer;
    use api::v1::meta::RangeRequest;
    use api::v1::meta::RequestHeader;

    use super::*;
    use crate::service::store::memory::MemStore;

    #[tokio::test]
    async fn test_handle_datanode_lease() {
        let kv_store = Arc::new(MemStore::new());
        let ctx = Context {
            datanode_lease_secs: 30,
            server_addr: "0.0.0.0:0000".to_string(),
            kv_store,
            election: None,
            skip_all: Arc::new(AtomicBool::new(false)),
        };

        let req = HeartbeatRequest {
            header: Some(RequestHeader::new((1, 2))),
            peer: Some(Peer {
                id: 3,
                addr: "127.0.0.1:1111".to_string(),
            }),
            ..Default::default()
        };
        let mut acc = HeartbeatAccumulator::default();

        let lease_handler = DatanodeLeaseHandler {};
        lease_handler.handle(&req, &ctx, &mut acc).await.unwrap();

        let key = LeaseKey {
            cluster_id: 1,
            node_id: 3,
        };

        let req = RangeRequest {
            key: key.try_into().unwrap(),
            ..Default::default()
        };

        let res = ctx.kv_store.range(req).await.unwrap();

        assert_eq!(1, res.kvs.len());
    }
}
