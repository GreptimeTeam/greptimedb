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
use common_meta::instruction::{
    self, GetPackedFileRefs, GetPackedFileRefsReply, InstructionReply, PackedGcRegions,
};
use common_meta::key::TableMetadataManagerRef;
use common_meta::key::runtime_switch::RuntimeSwitchManagerRef;
use common_meta::key::table_repart::TableRepartValue;
use common_meta::key::table_route::PhysicalTableRouteValue;
use common_meta::lock_key::{RegionLock, TableLock};
use common_meta::peer::Peer;
use common_meta::rpc::ddl::TriggerReason;
use common_procedure::error::ToJsonSnafu;
use common_procedure::{
    Context as ProcedureContext, Error as ProcedureError, EventContext, EventTrigger, LockKey,
    Procedure, ProcedureState, Result as ProcedureResult, Status,
};
use common_telemetry::tracing::Instrument as _;
use common_telemetry::tracing_context::TracingContext;
use common_telemetry::{debug, error, info, warn};
use futures::future::join_all;
use futures::stream::{FuturesUnordered, StreamExt};
use itertools::Itertools as _;
use serde::{Deserialize, Serialize};
use snafu::ResultExt as _;
use store_api::storage::{FileRefsManifest, GcReport, RegionId};
use table::metadata::TableId;

use crate::error::{self, KvBackendSnafu, Result, SerializeToJsonSnafu, TableMetadataManagerSnafu};
use crate::event::gc::{BATCH_GC_EVENT_TYPE, BatchGcEvent};
use crate::gc::util::table_route_to_region;
use crate::gc::{Peer2Regions, Region2Peers};
use crate::handler::HeartbeatMailbox;
use crate::metrics::{METRIC_META_GC_DATANODE_CALLS_TOTAL, METRIC_META_GC_FAILED_REGIONS_TOTAL};
use crate::procedure::utils::{instruction_error_result, instruction_to_error};
use crate::service::mailbox::{Channel, MailboxReceiver, MailboxRef};

async fn send_get_packed_file_refs_inner(
    mailbox: &MailboxRef,
    server_addr: &str,
    peer: &Peer,
    instruction: GetPackedFileRefs,
    timeout: Duration,
) -> Result<MailboxReceiver> {
    let instruction = instruction::Instruction::GetPackedFileRefs(instruction);
    let tracing_ctx = TracingContext::from_current_span();
    let msg = MailboxMessage::json_message(
        &format!("Get file references: {}", instruction),
        &format!("Metasrv@{}", server_addr),
        &format!("Datanode-{}@{}", peer.id, peer.addr),
        common_time::util::current_time_millis(),
        &instruction,
        Some(tracing_ctx.to_w3c()),
    )
    .with_context(|_| SerializeToJsonSnafu {
        input: instruction.to_string(),
    })?;

    mailbox
        .send(&Channel::Datanode(peer.id), msg, timeout)
        .await
}

async fn recv_get_packed_file_refs_reply(
    peer: &Peer,
    mailbox_rx: MailboxReceiver,
) -> Result<GetPackedFileRefsReply> {
    let reply = match mailbox_rx.await {
        Ok(reply_msg) => HeartbeatMailbox::json_reply(&reply_msg)?,
        Err(e) => {
            error!(
                e; "Failed to receive reply from datanode {} for GetPackedFileRefs instruction",
                peer,
            );
            return Err(e);
        }
    };

    let InstructionReply::GetPackedFileRefs(reply) = reply else {
        return error::UnexpectedInstructionReplySnafu {
            mailbox_message: "unexpected instruction reply for GetPackedFileRefs".to_string(),
            reason: "Unexpected reply of the GetPackedFileRefs instruction",
        }
        .fail();
    };

    Ok(reply)
}

async fn send_gc_regions_inner(
    mailbox: &MailboxRef,
    peer: &Peer,
    instruction: instruction::Instruction,
    server_addr: &str,
    timeout: Duration,
    description: &str,
) -> Result<MailboxReceiver> {
    let tracing_ctx = TracingContext::from_current_span();
    let msg = MailboxMessage::json_message(
        &format!("{}: {}", description, instruction),
        &format!("Metasrv@{}", server_addr),
        &format!("Datanode-{}@{}", peer.id, peer.addr),
        common_time::util::current_time_millis(),
        &instruction,
        Some(tracing_ctx.to_w3c()),
    )
    .with_context(|_| SerializeToJsonSnafu {
        input: instruction.to_string(),
    })?;

    mailbox
        .send(&Channel::Datanode(peer.id), msg, timeout)
        .await
}

fn scoped_gc_instruction(
    regions: Vec<RegionId>,
    full_manifest: &FileRefsManifest,
    full_file_listing: bool,
) -> instruction::Instruction {
    let scoped = FileRefsManifest {
        file_refs: regions
            .iter()
            .filter_map(|region| {
                full_manifest
                    .file_refs
                    .get(region)
                    .map(|refs| (*region, refs.clone()))
            })
            .collect(),
        manifest_version: regions
            .iter()
            .filter_map(|region| {
                full_manifest
                    .manifest_version
                    .get(region)
                    .map(|version| (*region, *version))
            })
            .collect(),
        cross_region_refs: HashMap::new(),
    };

    instruction::Instruction::PackedGcRegions(PackedGcRegions {
        regions,
        packed_file_refs_manifest: common_meta::instruction::PackedFileRefsManifest::from_manifest(
            &scoped,
        ),
        full_file_listing,
    })
}

async fn recv_gc_regions_reply(
    peer: &Peer,
    regions: &[RegionId],
    description: &str,
    mailbox_rx: MailboxReceiver,
) -> Result<GcReport> {
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
            mailbox_message: "unexpected instruction reply for GcRegions".to_string(),
            reason: "Unexpected reply of the GcRegions instruction",
        }
        .fail();
    };

    let res = reply.result;
    match res {
        Ok(report) => Ok(report),
        Err(e) => {
            error!(
                e; "Datanode {} reported error during GC for {} regions",
                peer, regions.len()
            );
            instruction_error_result(
                &e,
                format!(
                    "Datanode {} reported error during GC for {} regions: {}",
                    peer,
                    regions.len(),
                    e
                ),
            )
        }
    }
}

/// Procedure to perform get file refs then batch GC for multiple regions,
/// it holds locks for all regions during the whole procedure.
pub struct BatchGcProcedure {
    mailbox: MailboxRef,
    table_metadata_manager: TableMetadataManagerRef,
    runtime_switch_manager: RuntimeSwitchManagerRef,
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

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        mailbox: MailboxRef,
        table_metadata_manager: TableMetadataManagerRef,
        runtime_switch_manager: RuntimeSwitchManagerRef,
        server_addr: String,
        regions: Vec<RegionId>,
        full_file_listing: bool,
        timeout: Duration,
        region_routes_override: Region2Peers,
    ) -> Self {
        Self {
            mailbox,
            table_metadata_manager,
            runtime_switch_manager,
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
        runtime_switch_manager: RuntimeSwitchManagerRef,
        server_addr: String,
        regions: Vec<RegionId>,
        file_refs: FileRefsManifest,
        timeout: Duration,
    ) -> Self {
        Self {
            mailbox,
            table_metadata_manager,
            runtime_switch_manager,
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

    async fn check_maintenance_mode(&self) -> ProcedureResult<()> {
        let enabled = self
            .runtime_switch_manager
            .maintenance_mode()
            .await
            .context(error::RuntimeSwitchManagerSnafu)
            .map_err(ProcedureError::retry_later)?;
        if enabled {
            return Err(ProcedureError::retry_later(
                error::RetryLaterSnafu {
                    reason: "maintenance mode is enabled".to_string(),
                }
                .build(),
            ));
        }
        Ok(())
    }

    fn merge_gc_report(&mut self, report: GcReport) {
        let accumulated = self.data.gc_report.get_or_insert_default();
        // Deleted objects are cumulative, while these sets describe the latest outcome
        // for each region covered by this report.
        let affected_regions: HashSet<_> = report
            .processed_regions
            .iter()
            .chain(&report.need_retry_regions)
            .copied()
            .collect();

        let mut processed_regions = std::mem::take(&mut accumulated.processed_regions);
        processed_regions.retain(|region| !affected_regions.contains(region));
        processed_regions.extend(report.processed_regions.iter().copied());

        let mut need_retry_regions = std::mem::take(&mut accumulated.need_retry_regions);
        need_retry_regions.retain(|region| !affected_regions.contains(region));
        need_retry_regions.extend(report.need_retry_regions.iter().copied());

        accumulated.merge(report);
        accumulated.processed_regions = processed_regions;
        accumulated.need_retry_regions = need_retry_regions;
    }

    fn done_with_gc_report(&self) -> ProcedureResult<Status> {
        let Some(report) = self.data.gc_report.clone() else {
            return common_procedure::error::UnexpectedSnafu {
                err_msg: "GC report should be present after GC completion".to_string(),
            }
            .fail();
        };

        Ok(Status::done_with_output(report))
    }

    #[cfg(test)]
    pub(crate) fn set_gc_report_for_test(&mut self, report: GcReport) {
        self.data.gc_report = Some(report);
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
        let table_ids: HashSet<TableId> = regions.iter().map(|r| r.table_id()).collect();
        let table_ids = table_ids.into_iter().collect::<Vec<_>>();

        let table_routes = self
            .table_metadata_manager
            .table_route_manager()
            .batch_get_physical_table_routes(&table_ids)
            .await
            .context(TableMetadataManagerSnafu)?;

        if table_routes.len() != table_ids.len() {
            // batch_get_physical_table_routes returns a subset on misses; treat that as error
            for table_id in &table_ids {
                if !table_routes.contains_key(table_id) {
                    // indicate is a logical table id
                    return error::InvalidArgumentsSnafu {
                    err_msg: format!(
                        "Unexpected logical table route: table {} resolved to physical table regions",
                        table_id
                    ),
                }
                    .fail();
                }
            }
        }

        let mut table_all_regions: HashMap<TableId, HashSet<RegionId>> = HashMap::new();
        for (table_id, table_route) in table_routes {
            let all_regions: HashSet<RegionId> = table_route
                .region_routes
                .iter()
                .map(|r| r.region.id)
                .collect();

            table_all_regions.insert(table_id, all_regions);
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

        // Regions whose extension sidecar cleanup failed and need retry. Keep
        // their tombstone so the next GC cycle can replay the cleanup.
        let need_retry: HashSet<RegionId> = self
            .data
            .gc_report
            .as_ref()
            .map(|r| r.need_retry_regions.clone())
            .unwrap_or_default();

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
                } else if has_tmp_ref || need_retry.contains(&src_region) {
                    // Keep the tombstone: tmp refs or pending extension cleanup
                    // still need a future GC pass.
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
        let region_count = self.data.regions.len();
        self.set_routes_and_related_regions()
            .instrument(common_telemetry::tracing::info_span!(
                "meta_gc_procedure_prepare_routes",
                region_count = region_count
            ))
            .await?;

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

        // Send packed GetFileRefs instructions to each datanode
        let mut all_file_refs: HashMap<RegionId, HashSet<_>> = HashMap::new();
        let mut all_manifest_versions = HashMap::new();
        let mut all_cross_region_refs: HashMap<RegionId, HashSet<RegionId>> = HashMap::new();

        let mut peers = HashSet::new();
        peers.extend(datanode2query_regions.keys().cloned());
        peers.extend(datanode2related_regions.keys().cloned());

        let mailbox = &self.mailbox;
        let server_addr = &self.data.server_addr;
        // Each future owns both send and receive, so a completed reply is merged and
        // dropped immediately rather than retained in a second join_all buffer.
        let mut tasks = FuturesUnordered::new();
        for peer in peers {
            let regions = datanode2query_regions.remove(&peer).unwrap_or_default();
            let related_regions_for_peer =
                datanode2related_regions.remove(&peer).unwrap_or_default();
            if regions.is_empty() && related_regions_for_peer.is_empty() {
                continue;
            }
            tasks.push(async move {
                let instruction = GetPackedFileRefs {
                    query_regions: regions,
                    related_regions: related_regions_for_peer,
                };
                let rx = send_get_packed_file_refs_inner(
                    mailbox,
                    server_addr,
                    &peer,
                    instruction,
                    timeout,
                )
                .await?;
                let reply = recv_get_packed_file_refs_reply(&peer, rx).await?;
                Ok::<_, crate::error::Error>((peer, reply))
            });
        }

        let mut first_error = None;
        let mut record_get_file_refs_error = |e| {
            METRIC_META_GC_DATANODE_CALLS_TOTAL
                .with_label_values(&["get_file_refs", "error"])
                .inc();
            if first_error.is_none() {
                first_error = Some(e);
            }
        };
        while let Some(result) = tasks.next().await {
            let (peer, reply) = match result {
                Ok(reply) => reply,
                Err(e) => {
                    record_get_file_refs_error(e);
                    continue;
                }
            };
            if !reply.success {
                let err = if let Some(error) = &reply.error {
                    instruction_to_error(
                        error,
                        format!("Failed to get file references from datanode {peer}"),
                    )
                } else {
                    error::UnexpectedSnafu {
                        violated:
                            "Datanode returned an unsuccessful GetPackedFileRefs reply without an error"
                                .to_string(),
                    }
                    .build()
                };
                record_get_file_refs_error(err);
                continue;
            }
            let manifest = match reply.packed_file_refs_manifest.into_manifest() {
                Ok(manifest) => manifest,
                Err(err) => {
                    record_get_file_refs_error(error::UnexpectedSnafu { violated: err }.build());
                    continue;
                }
            };
            METRIC_META_GC_DATANODE_CALLS_TOTAL
                .with_label_values(&["get_file_refs", "success"])
                .inc();
            for (region_id, refs) in manifest.file_refs {
                all_file_refs.entry(region_id).or_default().extend(refs);
            }
            for (region_id, version) in manifest.manifest_version {
                let entry = all_manifest_versions.entry(region_id).or_insert(version);
                *entry = (*entry).min(version);
            }
            for (region_id, related) in manifest.cross_region_refs {
                all_cross_region_refs
                    .entry(region_id)
                    .or_default()
                    .extend(related);
            }
        }

        if let Some(e) = first_error {
            return Err(e);
        }

        Ok(FileRefsManifest {
            file_refs: all_file_refs,
            manifest_version: all_manifest_versions,
            cross_region_refs: all_cross_region_refs,
        })
    }

    /// Sends GC instructions to all datanodes that host the regions.
    async fn send_gc_instructions(&mut self) -> Result<()> {
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
        let mailbox = &self.mailbox;
        let server_addr = self.data.server_addr.as_str();
        let full_file_listing = self.data.full_file_listing;
        let tasks = datanode2regions
            .into_iter()
            .map(|(peer, regions_for_peer)| {
                let region_count = regions_for_peer.len() as u64;
                let regions = regions_for_peer.clone();
                let instruction =
                    scoped_gc_instruction(regions_for_peer, file_refs, full_file_listing);

                async move {
                    let report = send_gc_regions_inner(
                        mailbox,
                        &peer,
                        instruction,
                        server_addr,
                        timeout,
                        "Batch GC",
                    )
                    .await;

                    (peer, regions, region_count, report)
                }
            });

        let mut recv_tasks = Vec::new();
        let mut first_error = None;
        let mut record_gc_error = |e, region_count| {
            METRIC_META_GC_DATANODE_CALLS_TOTAL
                .with_label_values(&["gc_regions", "error"])
                .inc();
            if region_count > 0 {
                METRIC_META_GC_FAILED_REGIONS_TOTAL.inc_by(region_count);
            }
            if first_error.is_none() {
                first_error = Some(e);
            }
        };
        for (peer, regions, region_count, report) in join_all(tasks).await {
            match report {
                Ok(mailbox_rx) => {
                    recv_tasks.push(async move {
                        let report =
                            recv_gc_regions_reply(&peer, &regions, "Batch GC", mailbox_rx).await;
                        (peer, region_count, report)
                    });
                }
                Err(e) => record_gc_error(e, region_count),
            }
        }

        for (peer, region_count, report) in join_all(recv_tasks).await {
            let report = match report {
                Ok(report) => {
                    METRIC_META_GC_DATANODE_CALLS_TOTAL
                        .with_label_values(&["gc_regions", "success"])
                        .inc();
                    let need_retry_count = report.need_retry_regions.len() as u64;
                    if need_retry_count > 0 {
                        METRIC_META_GC_FAILED_REGIONS_TOTAL.inc_by(need_retry_count);
                    }
                    report
                }
                Err(e) => {
                    record_gc_error(e, region_count);
                    continue;
                }
            };

            let success = report.deleted_files.keys().collect_vec();
            let need_retry = report.need_retry_regions.iter().cloned().collect_vec();

            if need_retry.is_empty() {
                info!(
                    "GC report from datanode {}: successfully deleted files for region IDs {:?}",
                    peer, success
                );
            } else {
                warn!(
                    "GC report from datanode {}: successfully deleted files for region IDs {:?}, need retry for region IDs {:?}",
                    peer, success, need_retry
                );
            }
            all_need_retry.extend(report.need_retry_regions.clone());
            all_report.merge(report);
        }

        self.merge_gc_report(all_report);

        if let Some(e) = first_error {
            return Err(e);
        }

        if !all_need_retry.is_empty() {
            warn!("Regions need retry after batch GC: {:?}", all_need_retry);
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl Procedure for BatchGcProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, ctx: &ProcedureContext) -> ProcedureResult<Status> {
        self.check_maintenance_mode().await?;

        match self.data.state {
            State::Start => {
                let _regions_span = common_telemetry::tracing::debug_span!(
                    "meta_gc_procedure_regions",
                    state = "start",
                    regions = ?self.data.regions
                )
                .entered();
                info!(
                    "Batch GC procedure transitioning from Start to Acquiring for {} regions",
                    self.data.regions.len()
                );
                // Transition to Acquiring state
                self.data.state = State::Acquiring;
                Ok(Status::executing(false))
            }
            State::Acquiring => {
                let region_count = self.data.regions.len();
                let full_file_listing = self.data.full_file_listing;
                let regions = self.data.regions.clone();
                info!(
                    "Batch GC procedure acquiring file references for {} regions",
                    region_count
                );
                // Get file references from all datanodes
                match self
                    .get_file_references()
                    .instrument(common_telemetry::tracing::debug_span!(
                        "meta_gc_procedure_regions",
                        state = "acquiring",
                        regions = ?regions
                    ))
                    .instrument(common_telemetry::tracing::info_span!(
                        "meta_gc_procedure_get_file_references",
                        region_count = region_count,
                        full_file_listing = full_file_listing
                    ))
                    .await
                {
                    Ok(file_refs) => {
                        info!(
                            "Batch GC procedure acquired file references for {} regions",
                            file_refs.file_refs.len()
                        );
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
                info!(
                    "Batch GC procedure sending GC instructions for {} regions",
                    self.data.regions.len()
                );
                // Send GC instructions to all datanodes
                // TODO(discord9): handle need-retry regions
                let debug_span = common_telemetry::tracing::debug_span!(
                    "meta_gc_procedure_regions",
                    state = "gcing",
                    regions = ?self.data.regions
                );
                let info_span = common_telemetry::tracing::info_span!(
                    "meta_gc_procedure_send_gc_instructions",
                    region_count = self.data.regions.len(),
                    full_file_listing = self.data.full_file_listing
                );
                match self
                    .send_gc_instructions()
                    .instrument(debug_span)
                    .instrument(info_span)
                    .await
                {
                    Ok(()) => {
                        info!(
                            "Batch GC procedure received GC report, retry region count: {}",
                            self.data
                                .gc_report
                                .as_ref()
                                .map_or(0, |report| report.need_retry_regions.len())
                        );
                        self.data.state = State::UpdateRepartition;
                        Ok(Status::executing(false))
                    }
                    Err(e) => {
                        error!(e; "Failed to send GC instructions");
                        Err(ProcedureError::external(e))
                    }
                }
            }
            State::UpdateRepartition => match self
                .cleanup_region_repartition(ctx)
                .instrument(common_telemetry::tracing::debug_span!(
                    "meta_gc_procedure_regions",
                    state = "update_repartition",
                    regions = ?self.data.regions
                ))
                .instrument(common_telemetry::tracing::info_span!(
                    "meta_gc_procedure_update_repartition",
                    region_count = self.data.regions.len()
                ))
                .await
            {
                Ok(()) => {
                    debug!(
                        "Cleanup region repartition info completed successfully for regions {:?}",
                        self.data.regions
                    );
                    info!(
                        "Batch GC completed successfully for regions {:?}",
                        self.data.regions
                    );
                    info!("GC report: {:?}", self.data.gc_report);
                    self.done_with_gc_report()
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

    fn event(&self, ctx: &EventContext<'_>) -> Option<Box<dyn common_event_recorder::Event>> {
        if !ctx.event_type_filter.allows(BATCH_GC_EVENT_TYPE) {
            return None;
        }

        let event = match &ctx.trigger {
            // Keep scheduled GC low-noise; record submitted manual requests for auditability.
            EventTrigger::Submitted => ctx
                .event_context
                .is_some_and(|context| context.reason == TriggerReason::Manual)
                .then(|| {
                    BatchGcEvent::with_config(
                        &self.data.regions,
                        self.data.full_file_listing,
                        self.data.timeout,
                    )
                })?,
            EventTrigger::Recovered | EventTrigger::ChildSubmitted { .. } => return None,
            EventTrigger::Succeeded => {
                let ProcedureState::Done {
                    output: Some(output),
                } = ctx.lifecycle_state
                else {
                    return None;
                };
                let report = output.downcast_ref::<GcReport>()?;
                BatchGcEvent::with_report(report)?
            }
            EventTrigger::Retrying { .. } | EventTrigger::RollingBack => BatchGcEvent::with_config(
                &self.data.regions,
                self.data.full_file_listing,
                self.data.timeout,
            ),
            EventTrigger::Failed | EventTrigger::Poisoned => self
                .data
                .gc_report
                .as_ref()
                .and_then(BatchGcEvent::with_report)
                .unwrap_or_else(|| {
                    BatchGcEvent::with_config(
                        &self.data.regions,
                        self.data.full_file_listing,
                        self.data.timeout,
                    )
                }),
        };
        Some(Box::new(event))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use api::v1::meta::MailboxMessage;
    use api::v1::meta::mailbox_message::Payload;
    use common_meta::instruction::{GcRegionsReply, Instruction, InstructionReply};
    use common_meta::key::TableMetadataManager;
    use common_meta::key::runtime_switch::RuntimeSwitchManager;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::kv_backend::test_util::MockKvBackendBuilder;
    use common_meta::peer::Peer;
    use common_meta::sequence::SequenceBuilder;
    use common_procedure::Context as ProcedureContext;
    use common_procedure_test::MockContextProvider;
    use common_time::util::current_time_millis;
    use store_api::storage::FileId;
    use tokio::sync::mpsc;

    use super::*;
    use crate::procedure::test_util::{MailboxContext, send_mock_reply};
    use crate::service::mailbox::Channel;

    #[test]
    fn test_scoped_gc_instruction_selects_and_scopes_manifest() {
        let region = RegionId::new(7, 3);
        let other = RegionId::new(7, 4);
        let mut manifest = FileRefsManifest::default();
        manifest.file_refs.insert(region, HashSet::new());
        manifest.file_refs.insert(other, HashSet::new());
        manifest.manifest_version.insert(region, 42);

        let packed = scoped_gc_instruction(vec![region], &manifest, true);
        let Instruction::PackedGcRegions(packed) = packed else {
            panic!("expected packed instruction");
        };
        assert_eq!(packed.regions, vec![region]);
        assert_eq!(
            packed.packed_file_refs_manifest.manifest_version[&region],
            42
        );
        assert!(
            !packed
                .packed_file_refs_manifest
                .file_refs
                .contains_key(&other)
        );
        assert!(
            packed
                .packed_file_refs_manifest
                .cross_region_refs
                .is_empty()
        );
    }

    #[test]
    fn test_done_with_gc_report_keeps_report() {
        let region_id = RegionId::new(1024, 1);
        let file_id = FileId::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let mut procedure = batch_gc_procedure();
        procedure.data.gc_report = Some(GcReport {
            deleted_files: HashMap::from([(region_id, vec![file_id])]),
            ..Default::default()
        });

        for _ in 0..2 {
            let status = procedure.done_with_gc_report().unwrap();
            assert_eq!(
                status.downcast_output_ref::<GcReport>(),
                procedure.data.gc_report.as_ref()
            );
        }
    }

    #[test]
    fn test_merge_gc_report_preserves_partial_outcomes() {
        let first_region = RegionId::new(1024, 1);
        let second_region = RegionId::new(1024, 2);
        let first_file = FileId::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let second_file = FileId::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
        let mut procedure = batch_gc_procedure();

        procedure.merge_gc_report(GcReport {
            deleted_files: HashMap::from([(first_region, vec![first_file])]),
            ..Default::default()
        });
        procedure.merge_gc_report(GcReport {
            deleted_files: HashMap::from([
                (first_region, vec![first_file]),
                (second_region, vec![second_file]),
            ]),
            ..Default::default()
        });

        let report = procedure.data.gc_report.unwrap();
        assert_eq!(report.deleted_files.len(), 2);
        assert_eq!(report.deleted_files[&first_region], vec![first_file]);
        assert_eq!(report.deleted_files[&second_region], vec![second_file]);
    }

    #[test]
    fn test_merge_gc_report_uses_latest_region_outcome() {
        let region_id = RegionId::new(1024, 1);
        let mut procedure = batch_gc_procedure();

        procedure.merge_gc_report(GcReport {
            deleted_files: HashMap::from([(region_id, vec![])]),
            processed_regions: HashSet::from([region_id]),
            ..Default::default()
        });
        procedure.merge_gc_report(GcReport {
            need_retry_regions: HashSet::from([region_id]),
            ..Default::default()
        });

        let report = procedure.data.gc_report.unwrap();
        assert_eq!(report.deleted_files[&region_id], Vec::<FileId>::new());
        assert!(!report.processed_regions.contains(&region_id));
        assert!(report.need_retry_regions.contains(&region_id));
    }

    #[tokio::test]
    async fn test_send_gc_instructions_preserves_partial_report() {
        let first_region = RegionId::new(1024, 1);
        let second_region = RegionId::new(1024, 2);
        let first_peer = Peer::new(1, "first");
        let second_peer = Peer::new(2, "second");
        let file_id = FileId::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let report = GcReport {
            deleted_files: HashMap::from([(first_region, vec![file_id])]),
            ..Default::default()
        };

        let kv_backend = Arc::new(MemoryKvBackend::new());
        let table_metadata_manager = Arc::new(TableMetadataManager::new(kv_backend.clone()));
        let runtime_switch_manager = Arc::new(RuntimeSwitchManager::new(kv_backend.clone()));
        let mailbox_sequence =
            SequenceBuilder::new("test_batch_gc_partial_report", kv_backend).build();
        let mut mailbox = MailboxContext::new(mailbox_sequence);
        let (tx, rx) = mpsc::channel(1);
        mailbox
            .insert_heartbeat_response_receiver(Channel::Datanode(first_peer.id), tx)
            .await;
        send_mock_reply(mailbox.mailbox().clone(), rx, {
            let report = report.clone();
            move |id| gc_reply(id, report.clone())
        });

        let mut procedure = BatchGcProcedure::new(
            mailbox.mailbox().clone(),
            table_metadata_manager,
            runtime_switch_manager,
            "localhost".to_string(),
            vec![first_region, second_region],
            true,
            Duration::from_secs(10),
            HashMap::new(),
        );
        procedure.data.region_routes = HashMap::from([
            (first_region, (first_peer, vec![])),
            (second_region, (second_peer, vec![])),
        ]);

        assert!(procedure.send_gc_instructions().await.is_err());
        assert_eq!(procedure.data.gc_report.as_ref(), Some(&report));
    }

    fn gc_reply(id: u64, report: GcReport) -> Result<MailboxMessage> {
        Ok(MailboxMessage {
            id,
            subject: "mock".to_string(),
            from: "datanode".to_string(),
            to: "meta".to_string(),
            timestamp_millis: current_time_millis(),
            payload: Some(Payload::Json(
                serde_json::to_string(&InstructionReply::GcRegions(GcRegionsReply {
                    result: Ok(report),
                }))
                .unwrap(),
            )),
            header: None,
        })
    }

    fn batch_gc_procedure() -> BatchGcProcedure {
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let table_metadata_manager = Arc::new(TableMetadataManager::new(kv_backend.clone()));
        let runtime_switch_manager = Arc::new(RuntimeSwitchManager::new(kv_backend));
        let mailbox_sequence =
            SequenceBuilder::new("test_batch_gc_procedure", Arc::new(MemoryKvBackend::new()))
                .build();
        let mailbox = MailboxContext::new(mailbox_sequence);
        BatchGcProcedure::new(
            mailbox.mailbox().clone(),
            table_metadata_manager,
            runtime_switch_manager,
            "localhost".to_string(),
            vec![RegionId::new(1024, 1)],
            true,
            Duration::from_secs(10),
            HashMap::new(),
        )
    }

    #[tokio::test]
    async fn test_maintenance_mode_gates_every_gc_state() {
        let states = [
            State::Start,
            State::Acquiring,
            State::Gcing,
            State::UpdateRepartition,
        ];

        for state in states {
            let kv_backend = Arc::new(MemoryKvBackend::new());
            let table_metadata_manager = Arc::new(TableMetadataManager::new(kv_backend.clone()));
            let runtime_switch_manager = Arc::new(RuntimeSwitchManager::new(kv_backend.clone()));
            runtime_switch_manager.set_maintenance_mode().await.unwrap();
            let mailbox_sequence =
                SequenceBuilder::new("test_batch_gc_maintenance_gate", kv_backend).build();
            let mut mailbox = MailboxContext::new(mailbox_sequence);
            let (tx, mut rx) = mpsc::channel(1);
            mailbox
                .insert_heartbeat_response_receiver(Channel::Datanode(1), tx)
                .await;
            let mut procedure = BatchGcProcedure::new(
                mailbox.mailbox().clone(),
                table_metadata_manager,
                runtime_switch_manager,
                "localhost".to_string(),
                vec![RegionId::new(1024, 1)],
                true,
                Duration::from_secs(10),
                HashMap::new(),
            );
            procedure.data.state = state.clone();
            let dump_before = procedure.dump().unwrap();

            let ctx = ProcedureContext {
                procedure_id: common_procedure::ProcedureId::random(),
                provider: Arc::new(MockContextProvider::default()),
                event_context: None,
            };
            let err = procedure.execute(&ctx).await.unwrap_err();

            assert!(err.is_retry_later());
            assert_eq!(procedure.data.state, state);
            assert_eq!(procedure.dump().unwrap(), dump_before);
            if matches!(state, State::Acquiring | State::Gcing) {
                assert!(rx.try_recv().is_err());
            }
        }
    }

    #[tokio::test]
    async fn test_maintenance_mode_read_error_retries_without_advancing_state() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let failing_once_calls = calls.clone();
        let kv_backend = Arc::new(
            MockKvBackendBuilder::default()
                .range_fn(Arc::new(move |_| {
                    if failing_once_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                        common_meta::error::UnexpectedSnafu {
                            err_msg: "maintenance read failed",
                        }
                        .fail()
                    } else {
                        Ok(common_meta::rpc::store::RangeResponse {
                            kvs: vec![],
                            more: false,
                        })
                    }
                }))
                .build()
                .unwrap(),
        );
        let table_metadata_manager = Arc::new(TableMetadataManager::new(kv_backend.clone()));
        let runtime_switch_manager = Arc::new(RuntimeSwitchManager::new(kv_backend.clone()));
        let mailbox_sequence =
            SequenceBuilder::new("test_batch_gc_maintenance_read_error", kv_backend).build();
        let mailbox = MailboxContext::new(mailbox_sequence);
        let mut procedure = BatchGcProcedure::new(
            mailbox.mailbox().clone(),
            table_metadata_manager,
            runtime_switch_manager,
            "localhost".to_string(),
            vec![RegionId::new(1024, 1)],
            true,
            Duration::from_secs(10),
            HashMap::new(),
        );
        let ctx = ProcedureContext {
            procedure_id: common_procedure::ProcedureId::random(),
            provider: Arc::new(MockContextProvider::default()),
            event_context: None,
        };
        let dump_before = procedure.dump().unwrap();

        assert!(procedure.execute(&ctx).await.unwrap_err().is_retry_later());
        assert_eq!(procedure.data.state, State::Start);
        assert_eq!(procedure.dump().unwrap(), dump_before);

        assert!(matches!(
            procedure.execute(&ctx).await.unwrap(),
            Status::Executing { .. }
        ));
        assert_eq!(procedure.data.state, State::Acquiring);
    }
}
