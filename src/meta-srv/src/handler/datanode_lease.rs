use api::v1::meta::HeartbeatRequest;
use api::v1::meta::PutRequest;
use common_telemetry::info;
use common_time::util as time_util;

use super::Context;
use super::HeartbeatAccumulator;
use super::HeartbeatHandler;
use crate::error::Result;
use crate::keys::LeaseKey;
use crate::keys::LeaseValue;

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

            let _ = ctx.kv_store().put(put).await?;
        }

        Ok(())
    }
}
