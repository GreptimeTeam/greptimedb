use api::v1::meta::HeartbeatRequest;

use super::HeartbeatAccumulator;
use super::HeartbeatHandler;
use crate::error::Result;
use crate::service::store::kv::KvStoreRef;

pub struct DatanodeLeaseHandler;

#[async_trait::async_trait]
impl HeartbeatHandler for DatanodeLeaseHandler {
    async fn handle(
        &self,
        _req: &HeartbeatRequest,
        _acc: &mut HeartbeatAccumulator,
        _store: KvStoreRef,
    ) -> Result<()> {
        // TODO(jiachun): datanode lease
        Ok(())
    }
}
