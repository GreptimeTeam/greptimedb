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

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use api::v1::meta::MailboxMessage;
use common_meta::datanode::RegionStat;
use common_meta::instruction::{
    GcRegions, GetFileRefs, GetFileRefsReply, Instruction, InstructionReply,
};
use common_meta::key::TableMetadataManagerRef;
use common_meta::key::table_route::PhysicalTableRouteValue;
use common_meta::peer::Peer;
use common_procedure::{ProcedureManagerRef, ProcedureWithId, watcher};
use common_telemetry::{debug, error, warn};
use snafu::{OptionExt as _, ResultExt as _};
use store_api::storage::{FileId, FileRefsManifest, GcReport, RegionId};
use table::metadata::TableId;

use crate::cluster::MetaPeerClientRef;
use crate::error::{self, Result, TableMetadataManagerSnafu, UnexpectedSnafu};
use crate::gc::Region2Peers;
use crate::gc::procedure::GcRegionProcedure;
use crate::handler::HeartbeatMailbox;
use crate::service::mailbox::{Channel, MailboxRef};

#[async_trait::async_trait]
pub(crate) trait SchedulerCtx: Send + Sync {
    async fn get_table_to_region_stats(&self) -> Result<HashMap<TableId, Vec<RegionStat>>>;

    async fn get_table_route(
        &self,
        table_id: TableId,
    ) -> Result<(TableId, PhysicalTableRouteValue)>;

    async fn get_file_references(
        &self,
        query_regions: &[RegionId],
        related_regions: HashMap<RegionId, Vec<RegionId>>,
        region_routes: &Region2Peers,
        timeout: Duration,
    ) -> Result<FileRefsManifest>;

    async fn gc_regions(
        &self,
        peer: Peer,
        region_ids: &[RegionId],
        file_refs_manifest: &FileRefsManifest,
        full_file_listing: bool,
        timeout: Duration,
    ) -> Result<GcReport>;
}

pub(crate) struct DefaultGcSchedulerCtx {
    /// The metadata manager.
    pub(crate) table_metadata_manager: TableMetadataManagerRef,
    /// Procedure manager.
    pub(crate) procedure_manager: ProcedureManagerRef,
    /// For getting `RegionStats`.
    pub(crate) meta_peer_client: MetaPeerClientRef,
    /// The mailbox to send messages.
    pub(crate) mailbox: MailboxRef,
    /// The server address.
    pub(crate) server_addr: String,
}

impl DefaultGcSchedulerCtx {
    pub fn try_new(
        table_metadata_manager: TableMetadataManagerRef,
        procedure_manager: ProcedureManagerRef,
        meta_peer_client: MetaPeerClientRef,
        mailbox: MailboxRef,
        server_addr: String,
    ) -> Result<Self> {
        // register a noop loader for `GcRegionProcedure` to avoid error when deserializing procedure when rebooting

        procedure_manager
            .register_loader(
                GcRegionProcedure::TYPE_NAME,
                Box::new(move |json| {
                    common_procedure::error::ProcedureLoaderNotImplementedSnafu {
                        type_name: GcRegionProcedure::TYPE_NAME.to_string(),
                        reason:
                            "GC procedure should be retried by scheduler, not reloaded from storage"
                                .to_string(),
                    }
                    .fail()
                }),
            )
            .context(error::RegisterProcedureLoaderSnafu {
                type_name: GcRegionProcedure::TYPE_NAME,
            });

        Ok(Self {
            table_metadata_manager,
            procedure_manager,
            meta_peer_client,
            mailbox,
            server_addr,
        })
    }
}

#[async_trait::async_trait]
impl SchedulerCtx for DefaultGcSchedulerCtx {
    async fn get_table_to_region_stats(&self) -> Result<HashMap<TableId, Vec<RegionStat>>> {
        let dn_stats = self.meta_peer_client.get_all_dn_stat_kvs().await?;
        let mut table_to_region_stats: HashMap<TableId, Vec<RegionStat>> = HashMap::new();
        for (_dn_id, stats) in dn_stats {
            let mut stats = stats.stats;

            let Some(latest_stat) = stats.iter().max_by_key(|s| s.timestamp_millis).cloned() else {
                continue;
            };

            for region_stat in latest_stat.region_stats {
                table_to_region_stats
                    .entry(region_stat.id.table_id())
                    .or_default()
                    .push(region_stat);
            }
        }
        Ok(table_to_region_stats)
    }

    async fn get_table_route(
        &self,
        table_id: TableId,
    ) -> Result<(TableId, PhysicalTableRouteValue)> {
        self.table_metadata_manager
            .table_route_manager()
            .get_physical_table_route(table_id)
            .await
            .context(TableMetadataManagerSnafu)
    }

    async fn gc_regions(
        &self,
        peer: Peer,
        region_ids: &[RegionId],
        file_refs_manifest: &FileRefsManifest,
        full_file_listing: bool,
        timeout: Duration,
    ) -> Result<GcReport> {
        self.gc_regions_inner(
            peer,
            region_ids,
            file_refs_manifest,
            full_file_listing,
            timeout,
        )
        .await
    }

    async fn get_file_references(
        &self,
        query_regions: &[RegionId],
        related_regions: HashMap<RegionId, Vec<RegionId>>,
        region_routes: &Region2Peers,
        timeout: Duration,
    ) -> Result<FileRefsManifest> {
        debug!(
            "Getting file references for {} regions",
            query_regions.len()
        );

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
            if let Some((leader, followers)) = region_routes.get(&related_region) {
                datanode2related_regions
                    .entry(leader.clone())
                    .or_default()
                    .insert(related_region, queries.clone());
            } // since read from manifest, no need to send to followers
        }

        // Send GetFileRefs instructions to each datanode
        let mut all_file_refs: HashMap<RegionId, HashSet<FileId>> = HashMap::new();
        let mut all_manifest_versions = HashMap::new();

        for (peer, regions) in datanode2query_regions {
            let related_regions = datanode2related_regions.remove(&peer).unwrap_or_default();
            match self
                .send_get_file_refs_instruction(&peer, &regions, related_regions, timeout)
                .await
            {
                Ok(manifest) => {
                    // TODO(discord9): if other regions provide file refs for one region on other datanode, and no version,
                    // is it correct to merge manifest_version directly?
                    // FIXME: follower region how to merge version???

                    for (region_id, file_refs) in manifest.file_refs {
                        all_file_refs
                            .entry(region_id)
                            .or_default()
                            .extend(file_refs);
                    }
                    // region manifest version should be the smallest one among all peers, so outdated region can be detected
                    for (region_id, version) in manifest.manifest_version {
                        let entry = all_manifest_versions.entry(region_id).or_insert(version);
                        *entry = (*entry).min(version);
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to get file refs from datanode {}: {}. Skipping regions on this datanode.",
                        peer, e
                    );
                    // Continue processing other datanodes instead of failing the entire operation
                    continue;
                }
            }
        }

        Ok(FileRefsManifest {
            file_refs: all_file_refs,
            manifest_version: all_manifest_versions,
        })
    }
}

impl DefaultGcSchedulerCtx {
    async fn gc_regions_inner(
        &self,
        peer: Peer,
        region_ids: &[RegionId],
        file_refs_manifest: &FileRefsManifest,
        full_file_listing: bool,
        timeout: Duration,
    ) -> Result<GcReport> {
        debug!(
            "Sending GC instruction to datanode {} for {} regions (full_file_listing: {})",
            peer,
            region_ids.len(),
            full_file_listing
        );

        let gc_regions = GcRegions {
            regions: region_ids.to_vec(),
            file_refs_manifest: file_refs_manifest.clone(),
            full_file_listing,
        };
        let procedure = GcRegionProcedure::new(
            self.mailbox.clone(),
            self.server_addr.clone(),
            peer,
            gc_regions,
            format!("GC for {} regions", region_ids.len()),
            timeout,
        );
        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        let id = procedure_with_id.id;

        let mut watcher = self
            .procedure_manager
            .submit(procedure_with_id)
            .await
            .context(error::SubmitProcedureSnafu)?;
        let res = watcher::wait(&mut watcher)
            .await
            .context(error::WaitProcedureSnafu)?
            .with_context(|| error::UnexpectedSnafu {
                violated: format!(
                    "GC procedure {id} successfully completed but no result returned"
                ),
            })?;

        let gc_report = GcRegionProcedure::cast_result(res)?;

        Ok(gc_report)
    }

    /// TODO(discord9): add support to read manifest of related regions for file refs too
    /// (now it's only reading  active FileHandles)
    async fn send_get_file_refs_instruction(
        &self,
        peer: &Peer,
        query_regions: &[RegionId],
        related_regions: HashMap<RegionId, Vec<RegionId>>,
        timeout: Duration,
    ) -> Result<FileRefsManifest> {
        debug!(
            "Sending GetFileRefs instruction to datanode {} for {} regions",
            peer,
            query_regions.len()
        );

        let instruction = Instruction::GetFileRefs(GetFileRefs {
            query_regions: query_regions.to_vec(),
            related_regions,
        });

        let reply = self
            .send_instruction(peer, instruction, "Get file references", timeout)
            .await?;

        let InstructionReply::GetFileRefs(GetFileRefsReply {
            file_refs_manifest,
            success,
            error,
        }) = reply
        else {
            return error::UnexpectedInstructionReplySnafu {
                mailbox_message: format!("{:?}", reply),
                reason: "Unexpected reply of the GetFileRefs instruction",
            }
            .fail();
        };

        if !success {
            return error::UnexpectedSnafu {
                violated: format!(
                    "Failed to get file references from datanode {}: {:?}",
                    peer, error
                ),
            }
            .fail();
        }

        Ok(file_refs_manifest)
    }

    async fn send_instruction(
        &self,
        peer: &Peer,
        instruction: Instruction,
        description: &str,
        timeout: Duration,
    ) -> Result<InstructionReply> {
        let msg = MailboxMessage::json_message(
            &format!("{}: {}", description, instruction),
            &format!("Metasrv@{}", self.server_addr),
            &format!("Datanode-{}@{}", peer.id, peer.addr),
            common_time::util::current_time_millis(),
            &instruction,
        )
        .with_context(|_| error::SerializeToJsonSnafu {
            input: instruction.to_string(),
        })?;

        let mailbox_rx = self
            .mailbox
            .send(&Channel::Datanode(peer.id), msg, timeout)
            .await?;

        match mailbox_rx.await {
            Ok(reply_msg) => {
                let reply = HeartbeatMailbox::json_reply(&reply_msg)?;
                Ok(reply)
            }
            Err(e) => {
                error!(
                    "Failed to receive reply from datanode {} for {}: {}",
                    peer, description, e
                );
                Err(e)
            }
        }
    }
}
