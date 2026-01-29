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
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use api::v1::meta::MailboxMessage;
use common_meta::instruction::{self, GcRegions, GetFileRefs, GetFileRefsReply, InstructionReply};
use common_meta::key::TableMetadataManagerRef;
use common_meta::key::table_repart::TableRepartValue;
use common_meta::key::table_route::PhysicalTableRouteValue;
use common_meta::lock_key::{RegionLock, TableLock};
use common_meta::peer::Peer;
use common_procedure::error::ToJsonSnafu;
use common_procedure::{
    Context as ProcedureContext, Error as ProcedureError, LockKey, Procedure,
    Result as ProcedureResult, Status,
};
use common_telemetry::{error, info, warn};
use itertools::Itertools as _;
use serde::{Deserialize, Serialize};
use snafu::ResultExt as _;
use store_api::storage::{FileRefsManifest, GcReport, RegionId};
use table::metadata::TableId;

use crate::error::{self, KvBackendSnafu, Result, SerializeToJsonSnafu, TableMetadataManagerSnafu};
use crate::gc::util::table_route_to_region;
use crate::gc::{Peer2Regions, Region2Peers};
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
                e; "Failed to receive reply from datanode {} for GetFileRefs instruction",
                peer,
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
                e; "Failed to receive reply from datanode {} for {}",
                peer, description
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
                e; "Datanode {} reported error during GC for regions {:?}",
                peer, gc_regions
            );
            error::UnexpectedSnafu {
                violated: format!(
                    "Datanode {} reported error during GC for regions {:?}: {}",
                    peer, gc_regions, e
                ),
            }
            .fail()
        }
    }
}

/// Procedure to perform get file refs then batch GC for multiple regions,
/// it holds locks for all regions during the whole procedure.
pub struct BatchGcProcedure {
    mailbox: MailboxRef,
    table_metadata_manager: TableMetadataManagerRef,
    data: BatchGcData,
}

#[derive(Serialize, Deserialize)]
pub struct BatchGcData {
    state: State,
    /// Meta server address
    server_addr: String,
    /// The regions to be GC-ed
    regions: Vec<RegionId>,
    full_file_listing: bool,
    region_routes: Region2Peers,
    /// Routes assigned by the scheduler for regions missing from table routes.
    #[serde(default)]
    region_routes_override: Region2Peers,
    /// Related regions (e.g., for shared files after repartition).
    /// The source regions (where those files originally came from) are used as the key, and the destination regions (where files are currently stored) are used as the value.
    related_regions: HashMap<RegionId, HashSet<RegionId>>,
    /// Acquired file references (Populated in Acquiring state)
    file_refs: FileRefsManifest,
    /// mailbox timeout duration
    timeout: Duration,
    gc_report: Option<GcReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum State {
    /// Initial state
    Start,
    /// Fetching file references from datanodes
    Acquiring,
    /// Sending GC instruction to the target datanode
    Gcing,
    /// Updating region repartition info in kvbackend after GC based on the GC result
    UpdateRepartition,
}

impl BatchGcProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::BatchGcProcedure";

    pub fn new(
        mailbox: MailboxRef,
        table_metadata_manager: TableMetadataManagerRef,
        server_addr: String,
        regions: Vec<RegionId>,
        full_file_listing: bool,
        timeout: Duration,
        region_routes_override: Region2Peers,
    ) -> Self {
        Self {
            mailbox,
            table_metadata_manager,
            data: BatchGcData {
                state: State::Start,
                server_addr,
                regions,
                full_file_listing,
                timeout,
                region_routes: HashMap::new(),
                region_routes_override,
                related_regions: HashMap::new(),
                file_refs: FileRefsManifest::default(),
                gc_report: None,
            },
        }
    }

    /// Test-only constructor to jump directly into the repartition update state.
    /// Intended for integration tests that validate `cleanup_region_repartition` without
    /// running the full batch GC state machine.
    #[cfg(feature = "mock")]
    pub fn new_update_repartition_for_test(
        mailbox: MailboxRef,
        table_metadata_manager: TableMetadataManagerRef,
        server_addr: String,
        regions: Vec<RegionId>,
        file_refs: FileRefsManifest,
        timeout: Duration,
    ) -> Self {
        Self {
            mailbox,
            table_metadata_manager,
            data: BatchGcData {
                state: State::UpdateRepartition,
                server_addr,
                regions,
                full_file_listing: false,
                timeout,
                region_routes: HashMap::new(),
                region_routes_override: HashMap::new(),
                related_regions: HashMap::new(),
                file_refs,
                gc_report: Some(GcReport::default()),
            },
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

    /// Return related regions for the given regions.
    /// The returned map uses the input region as key, and all other regions
    /// from the same table as values (excluding the input region itself).
    async fn find_related_regions(
        &self,
        regions: &[RegionId],
    ) -> Result<HashMap<RegionId, HashSet<RegionId>>> {
        let mut table_to_regions: HashMap<TableId, HashSet<RegionId>> = HashMap::new();
        for region_id in regions {
            table_to_regions
                .entry(region_id.table_id())
                .or_default()
                .insert(*region_id);
        }

        let mut table_all_regions: HashMap<TableId, HashSet<RegionId>> = HashMap::new();
        for table_id in table_to_regions.keys() {
            match self.get_table_route(*table_id).await {
                Ok((_phy_table_id, table_route)) => {
                    let all_regions: HashSet<RegionId> = table_route
                        .region_routes
                        .iter()
                        .map(|r| r.region.id)
                        .collect();
                    table_all_regions.insert(*table_id, all_regions);
                }
                Err(e) => {
                    warn!(
                        "Failed to get table route for table {}: {}, skipping",
                        table_id, e
                    );
                }
            }
        }

        let mut related_regions: HashMap<RegionId, HashSet<RegionId>> = HashMap::new();
        for region_id in regions {
            let table_id = region_id.table_id();
            if let Some(all_regions) = table_all_regions.get(&table_id) {
                let mut related: HashSet<RegionId> = all_regions.clone();
                related.remove(region_id);
                related_regions.insert(*region_id, related);
            } else {
                related_regions.insert(*region_id, Default::default());
            }
        }

        Ok(related_regions)
    }

    /// Clean up region repartition info in kvbackend after GC
    /// according to cross reference in `FileRefsManifest`.
    async fn cleanup_region_repartition(&self, procedure_ctx: &ProcedureContext) -> Result<()> {
        let mut cross_refs_grouped: HashMap<TableId, HashMap<RegionId, HashSet<RegionId>>> =
            HashMap::new();
        for (src_region, dst_regions) in &self.data.file_refs.cross_region_refs {
            cross_refs_grouped
                .entry(src_region.table_id())
                .or_default()
                .entry(*src_region)
                .or_default()
                .extend(dst_regions.iter().copied());
        }

        let mut tmp_refs_grouped: HashMap<TableId, HashSet<RegionId>> = HashMap::new();
        for (src_region, refs) in &self.data.file_refs.file_refs {
            if refs.is_empty() {
                continue;
            }

            tmp_refs_grouped
                .entry(src_region.table_id())
                .or_default()
                .insert(*src_region);
        }

        let repart_mgr = self.table_metadata_manager.table_repart_manager();

        let mut table_ids: HashSet<TableId> = cross_refs_grouped
            .keys()
            .copied()
            .chain(tmp_refs_grouped.keys().copied())
            .collect();
        table_ids.extend(self.data.regions.iter().map(|r| r.table_id()));

        for table_id in table_ids {
            let table_lock = TableLock::Write(table_id).into();
            let _guard = procedure_ctx.provider.acquire_lock(&table_lock).await;

            let cross_refs = cross_refs_grouped
                .get(&table_id)
                .cloned()
                .unwrap_or_default();
            let tmp_refs = tmp_refs_grouped.get(&table_id).cloned().unwrap_or_default();

            let current = repart_mgr
                .get_with_raw_bytes(table_id)
                .await
                .context(KvBackendSnafu)?;

            let mut new_value = current
                .as_ref()
                .map(|v| (**v).clone())
                .unwrap_or_else(TableRepartValue::new);

            // We only touch regions involved in this GC batch for the current table to avoid
            // clobbering unrelated repart entries. Start from the batch regions of this table.
            let batch_src_regions: HashSet<RegionId> = self
                .data
                .regions
                .iter()
                .copied()
                .filter(|r| r.table_id() == table_id)
                .collect();

            // Merge targets: only the batch regions of this table. This avoids touching unrelated
            // repart entries; we just reconcile mappings for regions involved in the current GC
            // cycle for this table.
            let all_src_regions: HashSet<RegionId> = batch_src_regions;

            for src_region in all_src_regions {
                let cross_dst = cross_refs.get(&src_region);
                let has_tmp_ref = tmp_refs.contains(&src_region);

                if let Some(dst_regions) = cross_dst {
                    let mut set = BTreeSet::new();
                    set.extend(dst_regions.iter().copied());
                    new_value.src_to_dst.insert(src_region, set);
                } else if has_tmp_ref {
                    // Keep a tombstone entry with an empty set so dropped regions that still
                    // have tmp refs are preserved; removing it would lose the repartition trace.
                    new_value.src_to_dst.insert(src_region, BTreeSet::new());
                } else {
                    new_value.src_to_dst.remove(&src_region);
                }
            }

            // If there is no repartition info to persist, skip creating/updating the key
            if new_value.src_to_dst.is_empty() && current.is_none() {
                continue;
            }

            repart_mgr
                .upsert_value(table_id, current, &new_value)
                .await
                .context(KvBackendSnafu)?;
        }

        Ok(())
    }

    /// Discover region routes for the given regions.
    async fn discover_route_for_regions(
        &self,
        regions: &[RegionId],
    ) -> Result<(Region2Peers, Peer2Regions)> {
        let mut region_to_peer = HashMap::new();
        let mut peer_to_regions = HashMap::new();

        // Group regions by table ID for batch processing
        let mut table_to_regions: HashMap<TableId, Vec<RegionId>> = HashMap::new();
        for region_id in regions {
            let table_id = region_id.table_id();
            table_to_regions
                .entry(table_id)
                .or_default()
                .push(*region_id);
        }

        // Process each table's regions together for efficiency
        for (table_id, table_regions) in table_to_regions {
            match self.get_table_route(table_id).await {
                Ok((_phy_table_id, table_route)) => {
                    table_route_to_region(
                        &table_route,
                        &table_regions,
                        &mut region_to_peer,
                        &mut peer_to_regions,
                    );
                }
                Err(e) => {
                    // Continue with other tables instead of failing completely
                    // TODO(discord9): consider failing here instead
                    warn!(
                        "Failed to get table route for table {}: {}, skipping its regions",
                        table_id, e
                    );
                    continue;
                }
            }
        }

        Ok((region_to_peer, peer_to_regions))
    }

    /// Set region routes and related regions for GC procedure
    async fn set_routes_and_related_regions(&mut self) -> Result<()> {
        let related_regions = self.find_related_regions(&self.data.regions).await?;

        self.data.related_regions = related_regions.clone();

        // Discover routes for all regions involved in GC, including both the
        // primary GC regions and their related regions.
        let mut regions_set: HashSet<RegionId> = self.data.regions.iter().cloned().collect();

        regions_set.extend(related_regions.keys().cloned());
        regions_set.extend(related_regions.values().flat_map(|v| v.iter()).cloned());

        let regions_to_discover = regions_set.into_iter().collect_vec();

        let (mut region_to_peer, _) = self
            .discover_route_for_regions(&regions_to_discover)
            .await?;

        for (region_id, route) in &self.data.region_routes_override {
            region_to_peer
                .entry(*region_id)
                .or_insert_with(|| route.clone());
        }

        self.data.region_routes = region_to_peer;

        Ok(())
    }

    /// Get file references from all datanodes that host the regions
    async fn get_file_references(&mut self) -> Result<FileRefsManifest> {
        self.set_routes_and_related_regions().await?;

        let query_regions = &self.data.regions;
        let related_regions = &self.data.related_regions;
        let region_routes = &self.data.region_routes;
        let timeout = self.data.timeout;
        let dropped_regions = self
            .data
            .region_routes_override
            .keys()
            .collect::<HashSet<_>>();

        // Group regions by datanode to minimize RPC calls
        let mut datanode2query_regions: HashMap<Peer, Vec<RegionId>> = HashMap::new();

        for region_id in query_regions {
            if dropped_regions.contains(region_id) {
                continue;
            }
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

        let mut datanode2related_regions: HashMap<Peer, HashMap<RegionId, HashSet<RegionId>>> =
            HashMap::new();
        for (src_region, dst_regions) in related_regions {
            for dst_region in dst_regions {
                if let Some((leader, _followers)) = region_routes.get(dst_region) {
                    datanode2related_regions
                        .entry(leader.clone())
                        .or_default()
                        .entry(*src_region)
                        .or_default()
                        .insert(*dst_region);
                } // since read from manifest, no need to send to followers
            }
        }

        // Send GetFileRefs instructions to each datanode
        let mut all_file_refs: HashMap<RegionId, HashSet<_>> = HashMap::new();
        let mut all_manifest_versions = HashMap::new();
        let mut all_cross_region_refs = HashMap::new();

        let mut peers = HashSet::new();
        peers.extend(datanode2query_regions.keys().cloned());
        peers.extend(datanode2related_regions.keys().cloned());

        for peer in peers {
            let regions = datanode2query_regions.remove(&peer).unwrap_or_default();
            let related_regions_for_peer =
                datanode2related_regions.remove(&peer).unwrap_or_default();

            if regions.is_empty() && related_regions_for_peer.is_empty() {
                continue;
            }

            let instruction = GetFileRefs {
                query_regions: regions,
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

            for (region_id, related_region_ids) in reply.file_refs_manifest.cross_region_refs {
                let entry = all_cross_region_refs
                    .entry(region_id)
                    .or_insert_with(HashSet::new);
                entry.extend(related_region_ids);
            }
        }

        Ok(FileRefsManifest {
            file_refs: all_file_refs,
            manifest_version: all_manifest_versions,
            cross_region_refs: all_cross_region_refs,
        })
    }

    /// Send GC instruction to all datanodes that host the regions,
    /// returns regions that need retry.
    async fn send_gc_instructions(&self) -> Result<GcReport> {
        let regions = &self.data.regions;
        let region_routes = &self.data.region_routes;
        let file_refs = &self.data.file_refs;
        let timeout = self.data.timeout;

        // Group regions by datanode
        let mut datanode2regions: HashMap<Peer, Vec<RegionId>> = HashMap::new();
        let mut all_report = GcReport::default();

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

        let mut all_need_retry = HashSet::new();
        // Send GC instructions to each datanode
        for (peer, regions_for_peer) in datanode2regions {
            let gc_regions = GcRegions {
                regions: regions_for_peer.clone(),
                // file_refs_manifest can be large; cloning for each datanode is acceptable here since this is an admin-only operation.
                file_refs_manifest: file_refs.clone(),
                full_file_listing: self.data.full_file_listing,
            };

            let report = send_gc_regions(
                &self.mailbox,
                &peer,
                gc_regions,
                self.data.server_addr.as_str(),
                timeout,
                "Batch GC",
            )
            .await?;

            let success = report.deleted_files.keys().collect_vec();
            let need_retry = report.need_retry_regions.iter().cloned().collect_vec();

            if need_retry.is_empty() {
                info!(
                    "GC report from datanode {}: successfully deleted files for regions {:?}",
                    peer, success
                );
            } else {
                warn!(
                    "GC report from datanode {}: successfully deleted files for regions {:?}, need retry for regions {:?}",
                    peer, success, need_retry
                );
            }
            all_need_retry.extend(report.need_retry_regions.clone());
            all_report.merge(report);
        }

        if !all_need_retry.is_empty() {
            warn!("Regions need retry after batch GC: {:?}", all_need_retry);
        }

        Ok(all_report)
    }
}

#[async_trait::async_trait]
impl Procedure for BatchGcProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, ctx: &ProcedureContext) -> ProcedureResult<Status> {
        match self.data.state {
            State::Start => {
                // Transition to Acquiring state
                self.data.state = State::Acquiring;
                Ok(Status::executing(false))
            }
            State::Acquiring => {
                // Get file references from all datanodes
                match self.get_file_references().await {
                    Ok(file_refs) => {
                        self.data.file_refs = file_refs;
                        self.data.state = State::Gcing;
                        Ok(Status::executing(false))
                    }
                    Err(e) => {
                        error!(e; "Failed to get file references");
                        Err(ProcedureError::external(e))
                    }
                }
            }
            State::Gcing => {
                // Send GC instructions to all datanodes
                // TODO(discord9): handle need-retry regions
                match self.send_gc_instructions().await {
                    Ok(report) => {
                        self.data.state = State::UpdateRepartition;
                        self.data.gc_report = Some(report);
                        Ok(Status::executing(false))
                    }
                    Err(e) => {
                        error!(e; "Failed to send GC instructions");
                        Err(ProcedureError::external(e))
                    }
                }
            }
            State::UpdateRepartition => match self.cleanup_region_repartition(ctx).await {
                Ok(()) => {
                    info!(
                        "Cleanup region repartition info completed successfully for regions {:?}",
                        self.data.regions
                    );
                    info!(
                        "Batch GC completed successfully for regions {:?}",
                        self.data.regions
                    );
                    let Some(report) = self.data.gc_report.take() else {
                        return common_procedure::error::UnexpectedSnafu {
                            err_msg: "GC report should be present after GC completion".to_string(),
                        }
                        .fail();
                    };
                    Ok(Status::done_with_output(report))
                }
                Err(e) => {
                    error!(e; "Failed to cleanup region repartition info");
                    Err(ProcedureError::external(e))
                }
            },
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
