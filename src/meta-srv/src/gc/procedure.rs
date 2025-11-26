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
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use api::v1::meta::MailboxMessage;
use common_meta::instruction::{self, GcRegions, GetFileRefs, GetFileRefsReply, InstructionReply};
use common_meta::lock_key::RegionLock;
use common_meta::peer::Peer;
use common_procedure::error::ToJsonSnafu;
use common_procedure::{
    Context as ProcedureContext, Error as ProcedureError, LockKey, Procedure,
    Result as ProcedureResult, Status,
};
use common_telemetry::{debug, error, info};
use itertools::Itertools as _;
use serde::{Deserialize, Serialize};
use snafu::ResultExt as _;
use store_api::storage::{FileRefsManifest, GcReport, RegionId};

use crate::error::{self, Result, SerializeToJsonSnafu};
use crate::gc::Region2Peers;
use crate::handler::HeartbeatMailbox;
use crate::service::mailbox::{Channel, MailboxRef};

/// Helper function to send GetFileRefs instruction and wait for reply.
async fn send_get_file_refs(
    mailbox: &MailboxRef,
    server_addr: &str,
    peer: &Peer,
    instruction: GetFileRefs,
    timeout: Duration,
) -> Result<GetFileRefsReply> {
    let instruction = instruction::Instruction::GetFileRefs(instruction);
    let msg = MailboxMessage::json_message(
        &format!("Get file references: {}", instruction),
        &format!("Metasrv@{}", server_addr),
        &format!("Datanode-{}@{}", peer.id, peer.addr),
        common_time::util::current_time_millis(),
        &instruction,
    )
    .with_context(|_| SerializeToJsonSnafu {
        input: instruction.to_string(),
    })?;

    let mailbox_rx = mailbox
        .send(&Channel::Datanode(peer.id), msg, timeout)
        .await?;

    let reply = match mailbox_rx.await {
        Ok(reply_msg) => HeartbeatMailbox::json_reply(&reply_msg)?,
        Err(e) => {
            error!(
                "Failed to receive reply from datanode {} for GetFileRefs: {}",
                peer, e
            );
            return Err(e);
        }
    };

    let InstructionReply::GetFileRefs(reply) = reply else {
        return error::UnexpectedInstructionReplySnafu {
            mailbox_message: format!("{:?}", reply),
            reason: "Unexpected reply of the GetFileRefs instruction",
        }
        .fail();
    };

    Ok(reply)
}

/// Helper function to send GcRegions instruction and wait for reply.
async fn send_gc_regions(
    mailbox: &MailboxRef,
    peer: &Peer,
    gc_regions: GcRegions,
    server_addr: &str,
    timeout: Duration,
    description: &str,
) -> Result<GcReport> {
    let instruction = instruction::Instruction::GcRegions(gc_regions.clone());
    let msg = MailboxMessage::json_message(
        &format!("{}: {}", description, instruction),
        &format!("Metasrv@{}", server_addr),
        &format!("Datanode-{}@{}", peer.id, peer.addr),
        common_time::util::current_time_millis(),
        &instruction,
    )
    .with_context(|_| SerializeToJsonSnafu {
        input: instruction.to_string(),
    })?;

    let mailbox_rx = mailbox
        .send(&Channel::Datanode(peer.id), msg, timeout)
        .await?;

    let reply = match mailbox_rx.await {
        Ok(reply_msg) => HeartbeatMailbox::json_reply(&reply_msg)?,
        Err(e) => {
            error!(
                "Failed to receive reply from datanode {} for {}: {}",
                peer, description, e
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
                peer, gc_regions, e
            );
            Err(error::UnexpectedSnafu {
                violated: format!(
                    "Datanode {} reported error during GC for regions {:?}: {}",
                    peer, gc_regions, e
                ),
            }
            .fail()?)
        }
    }
}

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
    pub const TYPE_NAME: &'static str = "metasrv-procedure::GcRegionProcedure";

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
        send_gc_regions(
            &self.mailbox,
            &self.data.peer,
            self.data.gc_regions.clone(),
            &self.data.server_addr,
            self.data.timeout,
            &self.data.description,
        )
        .await
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

    /// Read lock all regions involved in this GC procedure.
    /// So i.e. region migration won't happen during GC and cause race conditions.
    ///
    /// only read lock the regions not catatlog/schema because it can run concurrently with other procedures(i.e. drop database/table)
    /// TODO:(discord9): integration test to verify this
    fn lock_key(&self) -> LockKey {
        let lock_key: Vec<_> = self
            .data
            .gc_regions
            .regions
            .iter()
            .sorted() // sort to have a deterministic lock order
            .map(|id| RegionLock::Read(*id).into())
            .collect();

        LockKey::new(lock_key)
    }
}

/// Procedure to perform get file refs then batch GC for multiple regions, should only be used by admin function
/// for triggering manual gc, as it holds locks for too long and for all regions during the procedure.
pub struct BatchGcProcedure {
    mailbox: MailboxRef,
    data: BatchGcData,
}

#[derive(Serialize, Deserialize)]
pub struct BatchGcData {
    state: State,
    server_addr: String,
    /// The regions to be GC-ed
    regions: Vec<RegionId>,
    full_file_listing: bool,
    region_routes: Region2Peers,
    /// Related regions (e.g., for shared files). Map: RegionId -> List of related RegionIds.
    related_regions: HashMap<RegionId, Vec<RegionId>>,
    /// Acquired file references (Populated in Acquiring state)
    file_refs: FileRefsManifest,
    /// mailbox timeout duration
    timeout: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum State {
    /// Initial state
    Start,
    /// Fetching file references from datanodes
    Acquiring,
    /// Sending GC instruction to the target datanode
    Gcing,
}

impl BatchGcProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::BatchGcProcedure";

    pub fn new(
        mailbox: MailboxRef,
        server_addr: String,
        regions: Vec<RegionId>,
        full_file_listing: bool,
        region_routes: Region2Peers,
        related_regions: HashMap<RegionId, Vec<RegionId>>,
        timeout: Duration,
    ) -> Self {
        Self {
            mailbox,
            data: BatchGcData {
                state: State::Start,
                server_addr,
                regions,
                full_file_listing,
                region_routes,
                related_regions,
                file_refs: FileRefsManifest::default(),
                timeout,
            },
        }
    }

    /// Get file references from all datanodes that host the regions
    async fn get_file_references(&self) -> Result<FileRefsManifest> {
        use std::collections::{HashMap, HashSet};

        let query_regions = &self.data.regions;
        let related_regions = &self.data.related_regions;
        let region_routes = &self.data.region_routes;
        let timeout = self.data.timeout;

        // Group regions by datanode to minimize RPC calls
        let mut datanode2query_regions: HashMap<Peer, Vec<RegionId>> = HashMap::new();

        for region_id in query_regions {
            if let Some((leader, followers)) = region_routes.get(region_id) {
                datanode2query_regions
                    .entry(leader.clone())
                    .or_default()
                    .push(*region_id);
                // also need to send for follower regions for file refs in case query is running on follower
                for follower in followers {
                    datanode2query_regions
                        .entry(follower.clone())
                        .or_default()
                        .push(*region_id);
                }
            } else {
                return error::UnexpectedSnafu {
                    violated: format!(
                        "region_routes: {region_routes:?} does not contain region_id: {region_id}",
                    ),
                }
                .fail();
            }
        }

        let mut datanode2related_regions: HashMap<Peer, HashMap<RegionId, Vec<RegionId>>> =
            HashMap::new();
        for (related_region, queries) in related_regions {
            if let Some((leader, _followers)) = region_routes.get(related_region) {
                datanode2related_regions
                    .entry(leader.clone())
                    .or_default()
                    .insert(*related_region, queries.clone());
            } // since read from manifest, no need to send to followers
        }

        // Send GetFileRefs instructions to each datanode
        let mut all_file_refs: HashMap<RegionId, HashSet<store_api::storage::FileId>> =
            HashMap::new();
        let mut all_manifest_versions = HashMap::new();

        for (peer, regions) in datanode2query_regions {
            let related_regions_for_peer =
                datanode2related_regions.remove(&peer).unwrap_or_default();

            let instruction = GetFileRefs {
                query_regions: regions.clone(),
                related_regions: related_regions_for_peer,
            };

            let reply = send_get_file_refs(
                &self.mailbox,
                &self.data.server_addr,
                &peer,
                instruction,
                timeout,
            )
            .await?;

            if !reply.success {
                return error::UnexpectedSnafu {
                    violated: format!(
                        "Failed to get file references from datanode {}: {:?}",
                        peer, reply.error
                    ),
                }
                .fail();
            }

            // Merge the file references from this datanode
            for (region_id, file_refs) in reply.file_refs_manifest.file_refs {
                all_file_refs
                    .entry(region_id)
                    .or_default()
                    .extend(file_refs);
            }

            // region manifest version should be the smallest one among all peers, so outdated region can be detected
            for (region_id, version) in reply.file_refs_manifest.manifest_version {
                let entry = all_manifest_versions.entry(region_id).or_insert(version);
                *entry = (*entry).min(version);
            }
        }

        Ok(FileRefsManifest {
            file_refs: all_file_refs,
            manifest_version: all_manifest_versions,
        })
    }

    /// Send GC instruction to all datanodes that host the regions
    async fn send_gc_instructions(&self) -> Result<()> {
        let regions = &self.data.regions;
        let region_routes = &self.data.region_routes;
        let file_refs = &self.data.file_refs;
        let timeout = self.data.timeout;

        // Group regions by datanode
        let mut datanode2regions: HashMap<Peer, Vec<RegionId>> = HashMap::new();

        for region_id in regions {
            if let Some((leader, _followers)) = region_routes.get(region_id) {
                datanode2regions
                    .entry(leader.clone())
                    .or_default()
                    .push(*region_id);
            } else {
                return error::UnexpectedSnafu {
                    violated: format!(
                        "region_routes: {region_routes:?} does not contain region_id: {region_id}",
                    ),
                }
                .fail();
            }
        }

        // Send GC instructions to each datanode
        for (peer, regions_for_peer) in datanode2regions {
            let gc_regions = GcRegions {
                regions: regions_for_peer.clone(),
                // file_refs_manifest can be large; cloning for each datanode is acceptable here since this is an admin-only operation.
                file_refs_manifest: file_refs.clone(),
                full_file_listing: self.data.full_file_listing,
            };

            let _report = send_gc_regions(
                &self.mailbox,
                &peer,
                gc_regions,
                self.data.server_addr.as_str(),
                timeout,
                "Batch GC",
            )
            .await?;

            // GC successful for this datanode
            debug!(
                "GC successful for datanode {} regions {:?}",
                peer, regions_for_peer
            );
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl Procedure for BatchGcProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        match self.data.state {
            State::Start => {
                // Transition to Acquiring state
                self.data.state = State::Acquiring;
                Ok(Status::executing(true))
            }
            State::Acquiring => {
                // Get file references from all datanodes
                match self.get_file_references().await {
                    Ok(file_refs) => {
                        self.data.file_refs = file_refs;
                        self.data.state = State::Gcing;
                        Ok(Status::executing(true))
                    }
                    Err(e) => {
                        error!("Failed to get file references: {}", e);
                        Err(ProcedureError::external(e))
                    }
                }
            }
            State::Gcing => {
                // Send GC instructions to all datanodes
                match self.send_gc_instructions().await {
                    Ok(()) => {
                        info!(
                            "Batch GC completed successfully for regions {:?}",
                            self.data.regions
                        );
                        Ok(Status::done())
                    }
                    Err(e) => {
                        error!("Failed to send GC instructions: {}", e);
                        Err(ProcedureError::external(e))
                    }
                }
            }
        }
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    /// Read lock all regions involved in this GC procedure.
    /// So i.e. region migration won't happen during GC and cause race conditions.
    fn lock_key(&self) -> LockKey {
        let lock_key: Vec<_> = self
            .data
            .regions
            .iter()
            .sorted() // sort to have a deterministic lock order
            .map(|id| RegionLock::Read(*id).into())
            .collect();

        LockKey::new(lock_key)
    }
}
