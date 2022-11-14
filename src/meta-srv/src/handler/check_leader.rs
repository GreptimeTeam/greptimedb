use std::sync::atomic::Ordering;

use api::v1::meta::Error;
use api::v1::meta::HeartbeatRequest;

use super::Context;
use super::HeartbeatAccumulator;
use super::HeartbeatHandler;
use crate::error::Result;

pub struct CheckLeaderHandler;

#[async_trait::async_trait]
impl HeartbeatHandler for CheckLeaderHandler {
    async fn handle(
        &self,
        _req: &HeartbeatRequest,
        ctx: &Context,
        acc: &mut HeartbeatAccumulator,
    ) -> Result<()> {
        if let Some(ref election) = ctx.election {
            if election.is_leader() {
                return Ok(());
            }
        }

        if let Some(ref mut header) = acc.header {
            header.error = Some(Error::not_leader());
            ctx.skip_all.store(true, Ordering::Relaxed);
        }

        Ok(())
    }
}
