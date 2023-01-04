use api::v1::meta::HeartbeatRequest;

use crate::error::Result;
use crate::handler::{HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::Context;

pub struct PersistStatsHandler;

#[async_trait::async_trait]
impl HeartbeatHandler for PersistStatsHandler {
    async fn handle(
        &self,
        _req: &HeartbeatRequest,
        ctx: &Context,
        _acc: &mut HeartbeatAccumulator,
    ) -> Result<()> {
        if ctx.is_skip_all() {
            return Ok(());
        }

        todo!()
    }
}
