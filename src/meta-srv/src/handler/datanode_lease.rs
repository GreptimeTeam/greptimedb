use api::v1::meta::HeartbeatRequest;
use api::v1::meta::PutRequest;
use common_telemetry::info;
use common_time::util as time_util;

use super::Context;
use super::HeartbeatAccumulator;
use super::HeartbeatHandler;
use crate::error::Result;
use crate::keys::DatanodeKey;
use crate::keys::DatanodeValue;
use crate::keys::KvBytes;

pub struct DatanodeLeaseHandler;

#[async_trait::async_trait]
impl HeartbeatHandler for DatanodeLeaseHandler {
    async fn handle(
        &self,
        req: &HeartbeatRequest,
        ctx: &Context,
        _acc: &mut HeartbeatAccumulator,
    ) -> Result<()> {
        let HeartbeatRequest { header, peer, .. } = req;
        if let Some(ref peer) = peer {
            let key = DatanodeKey {
                cluster_id: header.as_ref().map_or(0, |h| h.cluster_id),
                node_id: peer.id,
            };
            let value = DatanodeValue {
                timestamp_millis: time_util::current_time_millis(),
                node_addr: peer.addr.clone(),
            };

            info!("Receive a heartbeat from datanode: {:?}, {:?}", key, value);

            let key = key.into_bytes()?;
            let value = value.into_bytes()?;
            let put = PutRequest {
                key,
                value,
                ..Default::default()
            };

            let kv_store = ctx.kv_store();
            let _ = kv_store.put(put).await?;
        }

        Ok(())
    }
}
