// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::time::Duration;

use api::v1::meta::MailboxMessage;
use common_meta::distributed_time_constants::REGION_LEASE_SECS;
use common_meta::instruction::{Instruction, InstructionReply, SimpleReply};
use common_meta::key::datanode_table::RegionInfo;
use common_meta::RegionIdent;
use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::{info, warn};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::error::{self, Result};
use crate::handler::HeartbeatMailbox;
use crate::procedure::region_migration::migration_end::RegionMigrationEnd;
use crate::procedure::region_migration::{Context, State};
use crate::service::mailbox::Channel;

/// Uses lease time of a region as the timeout of closing a downgraded region.
const CLOSE_DOWNGRADED_REGION_TIMEOUT: Duration = Duration::from_secs(REGION_LEASE_SECS);

#[derive(Debug, Serialize, Deserialize)]
pub struct CloseDowngradedRegion;

#[async_trait::async_trait]
#[typetag::serde]
impl State for CloseDowngradedRegion {
    async fn next(
        &mut self,
        ctx: &mut Context,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        if let Err(err) = self.close_downgraded_leader_region(ctx).await {
            let downgrade_leader_datanode = &ctx.persistent_ctx.from_peer;
            let region_id = ctx.region_id();
            warn!(err; "Failed to close downgraded leader region: {region_id} on datanode {:?}", downgrade_leader_datanode);
        }
        info!(
            "Region migration is finished: region_id: {}, from_peer: {}, to_peer: {}, {}",
            ctx.region_id(),
            ctx.persistent_ctx.from_peer,
            ctx.persistent_ctx.to_peer,
            ctx.volatile_ctx.metrics,
        );
        Ok((Box::new(RegionMigrationEnd), Status::done()))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl CloseDowngradedRegion {
    /// Builds close region instruction.
    ///
    /// Abort(non-retry):
    /// - Datanode Table is not found.
    async fn build_close_region_instruction(&self, ctx: &mut Context) -> Result<Instruction> {
        let pc = &ctx.persistent_ctx;
        let downgrade_leader_datanode_id = pc.from_peer.id;
        let table_id = pc.region_id.table_id();
        let region_number = pc.region_id.region_number();
        let datanode_table_value = ctx.get_from_peer_datanode_table_value().await?;

        let RegionInfo { engine, .. } = datanode_table_value.region_info.clone();

        Ok(Instruction::CloseRegion(RegionIdent {
            datanode_id: downgrade_leader_datanode_id,
            table_id,
            region_number,
            engine,
        }))
    }

    /// Closes the downgraded leader region.
    async fn close_downgraded_leader_region(&self, ctx: &mut Context) -> Result<()> {
        let close_instruction = self.build_close_region_instruction(ctx).await?;
        let region_id = ctx.region_id();
        let pc = &ctx.persistent_ctx;
        let downgrade_leader_datanode = &pc.from_peer;
        let msg = MailboxMessage::json_message(
            &format!("Close downgraded region: {}", region_id),
            &format!("Metasrv@{}", ctx.server_addr()),
            &format!(
                "Datanode-{}@{}",
                downgrade_leader_datanode.id, downgrade_leader_datanode.addr
            ),
            common_time::util::current_time_millis(),
            &close_instruction,
        )
        .with_context(|_| error::SerializeToJsonSnafu {
            input: close_instruction.to_string(),
        })?;

        let ch = Channel::Datanode(downgrade_leader_datanode.id);
        let receiver = ctx
            .mailbox
            .send(&ch, msg, CLOSE_DOWNGRADED_REGION_TIMEOUT)
            .await?;

        match receiver.await {
            Ok(msg) => {
                let reply = HeartbeatMailbox::json_reply(&msg)?;
                info!(
                    "Received close downgraded leade region reply: {:?}, region: {}",
                    reply, region_id
                );
                let InstructionReply::CloseRegion(SimpleReply { result, error }) = reply else {
                    return error::UnexpectedInstructionReplySnafu {
                        mailbox_message: msg.to_string(),
                        reason: "expect close region reply",
                    }
                    .fail();
                };

                if result {
                    Ok(())
                } else {
                    error::UnexpectedSnafu {
                        violated: format!(
                            "Failed to close downgraded leader region: {region_id} on datanode {:?}, error: {error:?}",
                            downgrade_leader_datanode,
                        ),
                    }
                    .fail()
                }
            }

            Err(e) => Err(e),
        }
    }
}
