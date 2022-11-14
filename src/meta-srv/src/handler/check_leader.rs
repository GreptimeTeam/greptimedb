use api::v1::meta::Error;
use api::v1::meta::HeartbeatRequest;

use crate::error::Result;
use crate::handler::HeartbeatAccumulator;
use crate::handler::HeartbeatHandler;
use crate::metasrv::Context;

pub struct CheckLeaderHandler;

#[async_trait::async_trait]
impl HeartbeatHandler for CheckLeaderHandler {
    async fn handle(
        &self,
        _req: &HeartbeatRequest,
        ctx: &Context,
        acc: &mut HeartbeatAccumulator,
    ) -> Result<()> {
        if let Some(election) = &ctx.election {
            if election.is_leader() {
                return Ok(());
            }
        }

        if let Some(header) = &mut acc.header {
            header.error = Some(Error::is_not_leader());
            ctx.set_skip_all();
        }

        Ok(())
    }
}
