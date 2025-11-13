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
use std::sync::Arc;
use std::time::Duration;

use api::v1::meta::MailboxMessage;
use common_meta::instruction::{self, GcRegions, InstructionReply};
use common_meta::lock_key::RegionLock;
use common_meta::peer::Peer;
use common_procedure::error::ToJsonSnafu;
use common_procedure::{
    Context as ProcedureContext, Error as ProcedureError, LockKey, Procedure,
    Result as ProcedureResult, Status,
};
use common_telemetry::error;
use itertools::Itertools as _;
use serde::{Deserialize, Serialize};
use snafu::ResultExt as _;
use store_api::storage::GcReport;

use crate::error::{self, Result, SerializeToJsonSnafu};
use crate::handler::HeartbeatMailbox;
use crate::service::mailbox::{Channel, MailboxRef};

/// TODO(discord9): another procedure which do both get file refs and gc regions.
pub struct GcRegionProcedure {
    mailbox: MailboxRef,
    data: GcRegionData,
}

#[derive(Serialize, Deserialize)]
pub struct GcRegionData {
    server_addr: String,
    peer: Peer,
    gc_regions: GcRegions,
    description: String,
    timeout: Duration,
}

impl GcRegionProcedure {
    const TYPE_NAME: &'static str = "metasrv-procedure::GcRegionProcedure";

    pub fn new(
        mailbox: MailboxRef,
        server_addr: String,
        peer: Peer,
        gc_regions: GcRegions,
        description: String,
        timeout: Duration,
    ) -> Self {
        Self {
            mailbox,
            data: GcRegionData {
                peer,
                server_addr,
                gc_regions,
                description,
                timeout,
            },
        }
    }

    async fn send_gc_instr(&self) -> Result<GcReport> {
        let peer = &self.data.peer;
        let instruction = instruction::Instruction::GcRegions(self.data.gc_regions.clone());
        let msg = MailboxMessage::json_message(
            &format!("{}: {}", self.data.description, instruction),
            &format!("Metasrv@{}", self.data.server_addr),
            &format!("Datanode-{}@{}", peer.id, peer.addr),
            common_time::util::current_time_millis(),
            &instruction,
        )
        .with_context(|_| SerializeToJsonSnafu {
            input: instruction.to_string(),
        })?;

        let mailbox_rx = self
            .mailbox
            .send(&Channel::Datanode(peer.id), msg, self.data.timeout)
            .await?;

        let reply = match mailbox_rx.await {
            Ok(reply_msg) => HeartbeatMailbox::json_reply(&reply_msg)?,
            Err(e) => {
                error!(
                    "Failed to receive reply from datanode {} for {}: {}",
                    peer, self.data.description, e
                );
                return Err(e);
            }
        };

        let InstructionReply::GcRegions(reply) = reply else {
            return error::UnexpectedInstructionReplySnafu {
                mailbox_message: format!("{:?}", reply),
                reason: "Unexpected reply of the GcRegions instruction",
            }
            .fail();
        };

        let res = reply.result;
        match res {
            Ok(report) => Ok(report),
            Err(e) => {
                error!(
                    "Datanode {} reported error during GC for regions {:?}: {}",
                    peer, self.data.gc_regions, e
                );
                Err(error::UnexpectedSnafu {
                    violated: format!(
                        "Datanode {} reported error during GC for regions {:?}: {}",
                        peer, self.data.gc_regions, e
                    ),
                }
                .fail()?)
            }
        }
    }

    pub fn cast_result(res: Arc<dyn Any>) -> Result<GcReport> {
        res.downcast_ref::<GcReport>().cloned().ok_or_else(|| {
            error::UnexpectedSnafu {
                violated: format!(
                    "Failed to downcast procedure result to GcReport, got {:?}",
                    std::any::type_name_of_val(&res.as_ref())
                ),
            }
            .build()
        })
    }
}

#[async_trait::async_trait]
impl Procedure for GcRegionProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        // Send GC instruction to the datanode. This procedure only handle lock&send, results or other kind of
        // errors will be reported back via the oneshot channel.
        let reply = self
            .send_gc_instr()
            .await
            .map_err(ProcedureError::external)?;

        Ok(Status::done_with_output(reply))
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    /// Write lock all regions involved in this GC procedure.
    /// So i.e. region migration won't happen during GC and cause race conditions.
    fn lock_key(&self) -> LockKey {
        let lock_key: Vec<_> = self
            .data
            .gc_regions
            .regions
            .iter()
            .sorted() // sort to have a deterministic lock order
            .map(|id| RegionLock::Write(*id).into())
            .collect();

        LockKey::new(lock_key)
    }
}
