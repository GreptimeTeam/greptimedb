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

use std::collections::{BTreeSet, HashMap};

use api::region::RegionResponse;
use api::v1::region::{
    ApplyStagedManifestRequest, RemapManifestRequest, RemapManifestSource, RemapManifestTarget,
};
use common_meta::key::DeserializedValueWithBytes;
use common_meta::key::datanode_table::{DatanodeTableKey, RegionInfo};
use common_meta::key::table_route::{PhysicalTableRouteValue, TableRouteValue};
use common_meta::lock_key::{CatalogLock, SchemaLock, TableLock};
use common_meta::peer::Peer;
use common_meta::rpc::router::{Region, RegionRoute};
use common_procedure::error::{Result as ProcedureResult, ToJsonSnafu};
use common_procedure::{Context as ProcedureContext, LockKey, Procedure, Status};
use common_telemetry::{debug, info};
use serde::{Deserialize, Serialize};
use serde_json::{self, Value};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::storage::{RegionId, TableId};
use strum::AsRefStr;

use crate::error::{self, Result};
use crate::procedure::repartition::context::{
    GroupManifestSummary, ManifestStatus, REMAP_MANIFEST_STATS_EXTENSION, RepartitionContext,
};
use crate::procedure::repartition::plan::{PlanEntry, PlanGroupId, RegionDescriptor};

/// Logical states executed by the group procedure state machine.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, AsRefStr)]
pub enum GroupState {
    Prepare,
    Freeze,
    UpdateMetadata,
    UpdateRegionRule,
    UpdateManifests,
    Confirm,
    Cleanup,
    Finished,
}

/// Persisted snapshot of group execution, replayed by the procedure framework
/// when the workflow is resumed.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct GroupProcedureData {
    table_id: TableId,
    entry: PlanEntry,
    state: GroupState,
    route_snapshot: PhysicalTableRouteValue,
    #[serde(skip)]
    route_raw: Option<DeserializedValueWithBytes<TableRouteValue>>,
    #[serde(skip)]
    region_info: Option<RegionInfo>,
    #[serde(default)]
    prepare_result: Option<GroupPrepareResult>,
    #[serde(default)]
    freeze_completed: bool,
    #[serde(default)]
    metadata_updated: bool,
    #[serde(default)]
    staged_regions: Vec<RegionId>,
    #[serde(default)]
    region_rule_updated: bool,
    #[serde(default)]
    region_rule_version: Option<String>,
    #[serde(default)]
    manifests_generated: bool,
    #[serde(default)]
    manifest_stats: Option<Value>,
    #[serde(default)]
    catalog_name: String,
    #[serde(default)]
    schema_name: String,
    #[serde(default)]
    manifest_summary: GroupManifestSummary,
    #[serde(default)]
    route_committed: bool,
    #[serde(default)]
    rollback_registered: bool,
}

impl GroupProcedureData {
    /// Record the latest manifest status and return a cloned summary for
    /// immediate use by the caller.
    fn record_manifest_summary(
        &mut self,
        status: ManifestStatus,
        staged_region_count: u64,
        error: Option<String>,
    ) -> GroupManifestSummary {
        self.manifest_summary.status = status;
        self.manifest_summary.staged_region_count = staged_region_count;
        self.manifest_summary.stats = self.manifest_stats.clone();
        self.manifest_summary.error = error;
        self.manifests_generated =
            matches!(status, ManifestStatus::Staged) && staged_region_count > 0;
        self.manifest_summary.clone()
    }

    /// Drop manifest tracking state – typically called around rollback paths.
    fn reset_manifest_state(&mut self) {
        self.manifest_stats = None;
        self.manifests_generated = false;
    }

    /// Decode manifest stats from the RPC response and update bookkeeping.
    fn note_remap_stats(&mut self, response: &RegionResponse) {
        if let Some(payload) = response.extensions.get(REMAP_MANIFEST_STATS_EXTENSION) {
            match serde_json::from_slice::<Value>(payload) {
                Ok(value) => {
                    self.manifest_stats = Some(value.clone());
                    let total_refs = value
                        .get("total_file_refs")
                        .and_then(Value::as_u64)
                        .unwrap_or_default();
                    info!(
                        "repartition group {:?}: staged manifests for {} regions (total file refs {})",
                        self.entry.group_id, response.affected_rows, total_refs
                    );
                    debug!(
                        "repartition group {:?}: manifest remap detail {:?}",
                        self.entry.group_id, value
                    );
                }
                Err(err) => {
                    debug!(
                        error = ?err,
                        "repartition group {:?}: failed to decode manifest remap stats",
                        self.entry.group_id
                    );
                    self.reset_manifest_state();
                }
            }
        } else {
            debug!(
                "repartition group {:?}: manifest remap response missing stats extension",
                self.entry.group_id
            );
            self.reset_manifest_state();
        }
    }

    /// Update manifest bookkeeping after publishing or discarding staged files.
    fn note_manifest_application(
        &mut self,
        publish: bool,
        response: Option<&RegionResponse>,
    ) -> GroupManifestSummary {
        match response {
            Some(resp) => {
                self.note_remap_stats(resp);
                let status = if publish {
                    ManifestStatus::Published
                } else {
                    ManifestStatus::Discarded
                };
                let summary = self.record_manifest_summary(status, resp.affected_rows as u64, None);
                if !publish {
                    self.reset_manifest_state();
                }
                summary
            }
            None => {
                self.reset_manifest_state();
                self.record_manifest_summary(ManifestStatus::Skipped, 0, None)
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct GroupRollbackRecord {
    pub(crate) group_id: PlanGroupId,
    data: GroupProcedureData,
}

impl GroupRollbackRecord {
    fn new(data: &GroupProcedureData) -> Option<Self> {
        if data.route_raw.is_none() {
            return None;
        }

        let mut snapshot = data.clone();
        snapshot.route_committed = true;
        Some(Self {
            group_id: data.entry.group_id,
            data: snapshot,
        })
    }

    fn into_inner(self) -> (PlanGroupId, GroupProcedureData) {
        (self.group_id, self.data)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GroupPrepareResult {
    source_routes: Vec<RegionRoute>,
    target_routes: Vec<Option<RegionRoute>>,
    central_region: RegionId,
}

/// Stateful executor that drives a single plan group through the repartition
/// lifecycle. It is scheduled by the parent procedure and persisted/resumed by
/// the procedure framework.
pub struct RepartitionGroupProcedure {
    context: RepartitionContext,
    data: GroupProcedureData,
}

/// Lazy payload describing the table route delta that the confirm stage writes.
#[allow(dead_code)]
pub struct RouteMetadataPayload<'a> {
    pub table_id: TableId,
    pub original: &'a DeserializedValueWithBytes<TableRouteValue>,
    pub new_routes: Vec<RegionRoute>,
}

impl RepartitionGroupProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::RepartitionGroup";

    pub fn new(
        entry: PlanEntry,
        table_id: TableId,
        route_snapshot: PhysicalTableRouteValue,
        catalog_name: String,
        schema_name: String,
        context: RepartitionContext,
    ) -> Self {
        let group_id = entry.group_id;
        Self {
            context,
            data: GroupProcedureData {
                table_id,
                entry,
                state: GroupState::Prepare,
                route_snapshot,
                route_raw: None,
                region_info: None,
                prepare_result: None,
                freeze_completed: false,
                metadata_updated: false,
                staged_regions: Vec::new(),
                region_rule_updated: false,
                region_rule_version: None,
                manifests_generated: false,
                manifest_stats: None,
                catalog_name,
                schema_name,
                manifest_summary: GroupManifestSummary {
                    group_id,
                    ..Default::default()
                },
                route_committed: false,
                rollback_registered: false,
            },
        }
    }

    #[allow(dead_code)]
    pub fn route_metadata_payload(&self) -> Option<RouteMetadataPayload<'_>> {
        if !self.data.metadata_updated {
            return None;
        }

        let original = self.data.route_raw.as_ref()?;
        Some(RouteMetadataPayload {
            table_id: self.data.table_id,
            original,
            new_routes: self.data.route_snapshot.region_routes.clone(),
        })
    }

    /// Update the shared context with the latest manifest summary.
    fn update_manifest_summary(
        &mut self,
        status: ManifestStatus,
        staged_region_count: u64,
        error: Option<String>,
    ) {
        let summary = self
            .data
            .record_manifest_summary(status, staged_region_count, error);
        self.context.record_manifest_summary(summary);
    }

    /// Standardised bookkeeping when a manifest-related RPC fails.
    fn handle_manifest_failure(&mut self, status: ManifestStatus, error: String) {
        self.data.reset_manifest_state();
        self.update_manifest_summary(status, 0, Some(error));
    }

    fn register_success_record(&mut self) {
        if self.data.rollback_registered {
            return;
        }

        if let Some(record) = GroupRollbackRecord::new(&self.data) {
            self.context.register_group_success(record);
            self.data.rollback_registered = true;
        }
    }

    pub(crate) async fn execute_rollback(
        context: RepartitionContext,
        record: GroupRollbackRecord,
    ) -> Result<()> {
        let (_, data) = record.into_inner();
        let mut procedure = Self { context, data };
        procedure.rollback_group(GroupState::Prepare).await
    }

    /// Roll the group back to an earlier state after a failure. The method
    /// attempts to reverse any local state, resume regions, and reapply the
    /// original table route if necessary.
    async fn rollback_group(&mut self, reset_state: GroupState) -> Result<()> {
        debug!(
            "repartition group {:?}: rolling back to {:?}",
            self.data.entry.group_id, reset_state
        );

        let needs_cleanup = self.data.prepare_result.is_some()
            && (self.data.freeze_completed
                || self.data.metadata_updated
                || self.data.region_rule_updated
                || self.data.manifests_generated
                || !self.data.staged_regions.is_empty());

        if needs_cleanup {
            self.cleanup_resources(false).await?;
        }

        if self.data.route_committed {
            self.revert_table_route().await?;
        } else {
            self.refresh_route_snapshot().await?;
        }

        self.reset_in_memory_state();
        self.data.state = reset_state;
        Ok(())
    }

    /// Restore the original table route snapshot that was captured before this
    /// group wrote a new route during the confirm phase.
    async fn revert_table_route(&mut self) -> Result<()> {
        let original = self
            .data
            .route_raw
            .as_ref()
            .context(error::UnexpectedSnafu {
                violated: format!(
                    "group {} missing original route snapshot during rollback",
                    self.data.entry.group_id
                ),
            })?;

        let route_manager = self.context.table_metadata_manager.table_route_manager();
        let current = route_manager
            .table_route_storage()
            .get_with_raw_bytes(self.data.table_id)
            .await
            .context(error::TableMetadataManagerSnafu)?
            .context(error::UnexpectedSnafu {
                violated: format!(
                    "group {} missing current route snapshot during rollback",
                    self.data.entry.group_id
                ),
            })?;

        let original_routes = original
            .region_routes()
            .context(error::TableMetadataManagerSnafu)?
            .clone();
        let region_info = self
            .data
            .region_info
            .clone()
            .context(error::UnexpectedSnafu {
                violated: format!(
                    "group {} missing region info for rollback",
                    self.data.entry.group_id
                ),
            })?;

        self.context
            .table_metadata_manager
            .update_table_route(
                self.data.table_id,
                region_info.clone(),
                &current,
                original_routes,
                &region_info.region_options,
                &region_info.region_wal_options,
            )
            .await
            .context(error::TableMetadataManagerSnafu)?;

        self.refresh_route_snapshot().await?;
        self.data.route_committed = false;
        Ok(())
    }

    /// Refresh the in-memory table route snapshot from storage. This should be
    /// used after either commit or rollback paths run.
    async fn refresh_route_snapshot(&mut self) -> Result<()> {
        let table_route_manager = self.context.table_metadata_manager.table_route_manager();
        let (_, latest_route) = table_route_manager
            .get_physical_table_route(self.data.table_id)
            .await
            .context(error::TableMetadataManagerSnafu)?;
        let raw_route = table_route_manager
            .table_route_storage()
            .get_with_raw_bytes(self.data.table_id)
            .await
            .context(error::TableMetadataManagerSnafu)?
            .context(error::UnexpectedSnafu {
                violated: format!(
                    "group {} failed to refresh route snapshot",
                    self.data.entry.group_id
                ),
            })?;

        self.data.route_snapshot = latest_route;
        self.data.route_raw = Some(raw_route);
        Ok(())
    }

    /// Reset volatile fields so the procedure can safely retry the workflow.
    fn reset_in_memory_state(&mut self) {
        self.data.prepare_result = None;
        self.data.region_info = None;
        self.data.freeze_completed = false;
        self.data.metadata_updated = false;
        self.data.region_rule_updated = false;
        self.data.region_rule_version = None;
        self.data.manifests_generated = false;
        self.data.manifest_stats = None;
        self.data.staged_regions.clear();
        self.data.route_committed = false;
    }

    /// Drive the procedure state machine by executing the handler that
    /// corresponds to the current state and returning the framework status.
    pub async fn step(&mut self) -> Result<Status> {
        match self.data.state {
            GroupState::Prepare => {
                self.on_prepare().await?;
                self.data.state = GroupState::Freeze;
                Ok(Status::executing(true))
            }
            GroupState::Freeze => {
                self.on_freeze().await?;
                self.data.state = GroupState::UpdateMetadata;
                Ok(Status::executing(true))
            }
            GroupState::UpdateMetadata => {
                self.on_update_metadata().await?;
                self.data.state = GroupState::UpdateRegionRule;
                Ok(Status::executing(true))
            }
            GroupState::UpdateRegionRule => {
                self.on_update_region_rule().await?;
                self.data.state = GroupState::UpdateManifests;
                Ok(Status::executing(true))
            }
            GroupState::UpdateManifests => {
                self.on_update_manifests().await?;
                self.data.state = GroupState::Confirm;
                Ok(Status::executing(true))
            }
            GroupState::Confirm => {
                self.on_confirm().await?;
                self.data.state = GroupState::Cleanup;
                Ok(Status::executing(true))
            }
            GroupState::Cleanup => {
                self.on_cleanup().await?;
                self.register_success_record();
                self.data.state = GroupState::Finished;
                Ok(Status::done())
            }
            GroupState::Finished => Ok(Status::done()),
        }
    }

    /// Capture the latest metadata snapshot prior to any modifications.
    async fn on_prepare(&mut self) -> Result<()> {
        if self.data.prepare_result.is_some() {
            return Ok(());
        }

        info!(
            "repartition group {:?}: preparing metadata snapshot for table {}",
            self.data.entry.group_id, self.data.table_id
        );

        let table_route_manager = self.context.table_metadata_manager.table_route_manager();
        let (_, latest_route) = table_route_manager
            .get_physical_table_route(self.data.table_id)
            .await
            .context(error::TableMetadataManagerSnafu)?;

        let raw_route = table_route_manager
            .table_route_storage()
            .get_with_raw_bytes(self.data.table_id)
            .await
            .context(error::TableMetadataManagerSnafu)?
            .context(error::UnexpectedSnafu {
                violated: format!(
                    "table {} route missing raw snapshot during repartition",
                    self.data.table_id
                ),
            })?;

        self.data.route_snapshot = latest_route.clone();
        self.data.route_raw = Some(raw_route.clone());
        self.ensure_target_routes_present()?;

        let mut source_routes = Vec::with_capacity(self.data.entry.sources.len());
        for descriptor in &self.data.entry.sources {
            let Some(region_id) = descriptor.region_id else {
                return error::RepartitionMissingSourceRegionIdSnafu {
                    group_id: self.data.entry.group_id,
                }
                .fail();
            };

            let Some(route) = latest_route
                .region_routes
                .iter()
                .find(|route| route.region.id == region_id)
            else {
                return error::RepartitionSourceRegionRouteMissingSnafu {
                    group_id: self.data.entry.group_id,
                    region_id,
                }
                .fail();
            };
            source_routes.push(route.clone());
        }

        if source_routes.is_empty() {
            return error::RepartitionNoSourceRegionsSnafu {
                group_id: self.data.entry.group_id,
            }
            .fail();
        }

        let mut target_routes = Vec::with_capacity(self.data.entry.targets.len());
        for descriptor in &self.data.entry.targets {
            if let Some(region_id) = descriptor.region_id {
                if let Some(route) = latest_route
                    .region_routes
                    .iter()
                    .find(|route| route.region.id == region_id)
                {
                    target_routes.push(Some(route.clone()));
                } else {
                    target_routes.push(None);
                }
            } else {
                target_routes.push(None);
            }
        }

        let central_region = source_routes[0].region.id;
        self.data.prepare_result = Some(GroupPrepareResult {
            source_routes,
            target_routes,
            central_region,
        });

        debug!(
            "repartition group {:?}: captured {} sources, {} targets",
            self.data.entry.group_id,
            self.data
                .prepare_result
                .as_ref()
                .map(|r| r.source_routes.len())
                .unwrap_or(0),
            self.data
                .prepare_result
                .as_ref()
                .map(|r| r.target_routes.len())
                .unwrap_or(0)
        );

        Ok(())
    }

    /// Pause IO on the source/target regions in preparation for metadata work.
    async fn on_freeze(&mut self) -> Result<()> {
        if self.data.freeze_completed {
            return Ok(());
        }

        info!(
            "repartition group {:?}: entering freeze stage",
            self.data.entry.group_id
        );

        let prepare_result = self.prepare_result()?;

        for route in &prepare_result.source_routes {
            self.pause_region(route).await?;
        }

        for route in prepare_result.target_routes.iter().flatten() {
            self.pause_region(route).await?;
        }

        self.data.freeze_completed = true;
        debug!(
            "repartition group {:?}: freeze stage completed",
            self.data.entry.group_id
        );
        Ok(())
    }

    /// Apply partition rule updates and mark the involved regions as staging in
    /// the metadata layer.
    async fn on_update_metadata(&mut self) -> Result<()> {
        if self.data.metadata_updated {
            return Ok(());
        }

        self.ensure_targets_allocated()?;

        info!(
            "repartition group {:?}: applying metadata updates",
            self.data.entry.group_id
        );

        self.apply_target_partition_rules()?;

        let prepare_result = self.prepare_result()?;
        let region_ids = self.collect_existing_region_ids(prepare_result);
        if region_ids.is_empty() {
            self.data.metadata_updated = true;
            return Ok(());
        }

        let mut staged_set: BTreeSet<RegionId> = self.data.staged_regions.iter().copied().collect();
        let route_manager = self.context.table_metadata_manager.table_route_manager();

        for region_id in &region_ids {
            if staged_set.insert(*region_id) {
                route_manager
                    .set_region_staging_state(*region_id, true)
                    .await
                    .context(error::TableMetadataManagerSnafu)?;
            }
        }

        Self::mark_regions_staging(&mut self.data.route_snapshot.region_routes, &region_ids);
        self.data.staged_regions = staged_set.into_iter().collect();

        self.data.metadata_updated = true;
        debug!(
            "repartition group {:?}: staged regions {:?}",
            self.data.entry.group_id, self.data.staged_regions
        );
        Ok(())
    }

    /// Stage the next rule version on every region that participates in the
    /// repartition group.
    async fn on_update_region_rule(&mut self) -> Result<()> {
        if self.data.region_rule_updated {
            return Ok(());
        }

        ensure!(
            self.data.metadata_updated,
            error::UnexpectedSnafu {
                violated: format!(
                    "group {} region rule update before metadata stage",
                    self.data.entry.group_id
                ),
            }
        );

        let prepare_result = self.prepare_result()?;
        let region_ids = self.collect_existing_region_ids(prepare_result);

        info!(
            "repartition group {:?}: scheduling region rule update for {:?}",
            self.data.entry.group_id, region_ids
        );

        let rule_version = self
            .data
            .region_rule_version
            .get_or_insert_with(|| format!("{}", self.data.entry.group_id))
            .clone();

        let stage_targets: Vec<_> = {
            let prepare_result = self.prepare_result()?;
            prepare_result
                .source_routes
                .iter()
                .chain(prepare_result.target_routes.iter().flatten())
                .map(|route| (route.leader_peer.clone(), route.region.id))
                .collect()
        };

        for (peer, region_id) in stage_targets {
            if let Some(peer) = peer {
                self.context
                    .stage_region_rule_on_datanode(&peer, region_id, &rule_version)
                    .await?;
            } else {
                debug!(
                    "repartition group {:?}: skip region rule staging, region {} has no leader",
                    self.data.entry.group_id, region_id
                );
            }
        }

        self.data.region_rule_updated = true;
        Ok(())
    }

    /// Trigger manifest remapping on the datanode and record the resulting stats.
    async fn on_update_manifests(&mut self) -> Result<()> {
        if self.data.manifests_generated {
            return Ok(());
        }

        ensure!(
            self.data.region_rule_updated,
            error::UnexpectedSnafu {
                violated: format!(
                    "group {} manifest stage invoked before region rule update",
                    self.data.entry.group_id
                ),
            }
        );

        self.ensure_targets_allocated()?;

        let prepare_result = self.prepare_result()?;
        let leader_peer = self.leader_peer_for_central(prepare_result)?;

        let mut target_index_lookup = HashMap::new();
        for (position, &global_idx) in self.data.entry.subtask.to_expr_indices.iter().enumerate() {
            target_index_lookup.insert(global_idx, position);
        }

        let mut sources = Vec::with_capacity(self.data.entry.sources.len());
        for (source_position, descriptor) in self.data.entry.sources.iter().enumerate() {
            let Some(region_id) = descriptor.region_id else {
                continue;
            };

            let transitions = self
                .data
                .entry
                .subtask
                .transition_map
                .get(source_position)
                .context(error::UnexpectedSnafu {
                    violated: format!(
                        "group {} transition map missing entry {}",
                        self.data.entry.group_id, source_position
                    ),
                })?;

            let mut target_region_ids = Vec::with_capacity(transitions.len());
            for global_target_idx in transitions {
                let target_position =
                    target_index_lookup
                        .get(global_target_idx)
                        .context(error::UnexpectedSnafu {
                            violated: format!(
                                "group {} transition references unknown target expr {}",
                                self.data.entry.group_id, global_target_idx
                            ),
                        })?;

                let target_descriptor = self.data.entry.targets.get(*target_position).context(
                    error::UnexpectedSnafu {
                        violated: format!(
                            "group {} missing target descriptor at {}",
                            self.data.entry.group_id, target_position
                        ),
                    },
                )?;

                let target_region_id =
                    target_descriptor
                        .region_id
                        .context(error::UnexpectedSnafu {
                            violated: format!(
                                "group {} target {} missing allocated region id",
                                self.data.entry.group_id, target_position
                            ),
                        })?;

                target_region_ids.push(target_region_id.as_u64());
            }

            sources.push(RemapManifestSource {
                region_id: region_id.as_u64(),
                target_region_ids,
            });
        }

        let mut targets = Vec::with_capacity(self.data.entry.targets.len());
        for descriptor in &self.data.entry.targets {
            let region_id = descriptor.region_id.context(error::UnexpectedSnafu {
                violated: format!(
                    "group {} missing target region allocation",
                    self.data.entry.group_id
                ),
            })?;

            targets.push(RemapManifestTarget {
                region_id: region_id.as_u64(),
                partition_expr: descriptor
                    .partition_expr
                    .as_json_str()
                    .context(error::RepartitionSerializePartitionExprSnafu)?,
            });
        }

        let request = RemapManifestRequest {
            table_id: self.data.table_id as u64,
            group_id: self.data.entry.group_id.to_string(),
            sources,
            targets,
        };

        info!(
            "repartition group {:?}: scheduling manifest remap via {:?}",
            self.data.entry.group_id, leader_peer.id
        );

        let response = match self
            .context
            .remap_manifests_on_datanode(&leader_peer, request)
            .await
        {
            Ok(response) => response,
            Err(err) => {
                let err_msg = err.to_string();
                self.handle_manifest_failure(ManifestStatus::Failed, err_msg);
                self.rollback_group(GroupState::Prepare).await?;
                return Err(err);
            }
        };

        self.data.note_remap_stats(&response);

        if response.affected_rows > 0 {
            self.update_manifest_summary(
                ManifestStatus::Staged,
                response.affected_rows as u64,
                None,
            );
        } else {
            self.data.reset_manifest_state();
            self.update_manifest_summary(ManifestStatus::Skipped, 0, None);
        }

        Ok(())
    }

    /// Commit the table route change and publish staged manifests along with the
    /// staged region rule version.
    async fn on_confirm(&mut self) -> Result<()> {
        info!(
            "repartition group {:?}: confirming metadata update",
            self.data.entry.group_id
        );

        self.ensure_targets_allocated()?;

        let region_info = self.ensure_region_info().await?.clone();
        let region_options = region_info.region_options.clone();
        let region_wal_options = region_info.region_wal_options.clone();

        let payload = self
            .route_metadata_payload()
            .context(error::UnexpectedSnafu {
                violated: format!(
                    "group {} metadata not prepared for confirmation",
                    self.data.entry.group_id
                ),
            })?;
        let RouteMetadataPayload {
            table_id,
            original,
            new_routes,
        } = payload;

        match self
            .context
            .table_metadata_manager
            .update_table_route(
                table_id,
                region_info,
                original,
                new_routes,
                &region_options,
                &region_wal_options,
            )
            .await
            .context(error::TableMetadataManagerSnafu)
        {
            Ok(()) => {}
            Err(err) => {
                self.rollback_group(GroupState::Prepare).await?;
                return Err(err);
            }
        }
        self.data.route_committed = true;

        info!(
            "repartition group {:?}: table route updated",
            self.data.entry.group_id
        );

        match self.apply_staged_manifests(true).await {
            Ok(Some(response)) => {
                let summary = self.data.note_manifest_application(true, Some(&response));
                self.context.record_manifest_summary(summary);
            }
            Ok(None) => {
                debug!(
                    "repartition group {:?}: no staged manifests to publish",
                    self.data.entry.group_id
                );
                let summary = self.data.note_manifest_application(true, None);
                self.context.record_manifest_summary(summary);
            }
            Err(err) => {
                let err_msg = err.to_string();
                self.handle_manifest_failure(ManifestStatus::Failed, err_msg);
                self.rollback_group(GroupState::Prepare).await?;
                return Err(err);
            }
        }

        self.refresh_route_snapshot().await?;

        if self.data.region_rule_updated {
            if let Some(rule_version) = self.data.region_rule_version.clone() {
                let publish_targets: Vec<_> = {
                    let prepare_result = self.prepare_result()?;
                    prepare_result
                        .source_routes
                        .iter()
                        .chain(prepare_result.target_routes.iter().flatten())
                        .filter_map(|route| {
                            route
                                .leader_peer
                                .clone()
                                .map(|peer| (peer, route.region.id))
                        })
                        .collect()
                };

                for (peer, region_id) in publish_targets {
                    if let Err(err) = self
                        .context
                        .publish_region_rule_on_datanode(&peer, region_id, &rule_version)
                        .await
                    {
                        let err_msg = err.to_string();
                        self.handle_manifest_failure(ManifestStatus::Failed, err_msg);
                        self.rollback_group(GroupState::Prepare).await?;
                        return Err(err);
                    }
                }
            }

            self.data.region_rule_updated = false;
        }
        self.data.route_committed = false;
        Ok(())
    }

    /// Resume region IO, clear staging marks, and optionally publish region rules.
    async fn on_cleanup(&mut self) -> Result<()> {
        self.cleanup_resources(true).await
    }

    /// Shared cleanup implementation used for the success path as well as the
    /// rollback handler.
    async fn cleanup_resources(&mut self, publish_rules: bool) -> Result<()> {
        let leader_targets: Vec<_> = {
            let prepare_result = self.prepare_result()?;

            for route in &prepare_result.source_routes {
                self.resume_region(route).await?;
            }
            for route in prepare_result.target_routes.iter().flatten() {
                self.resume_region(route).await?;
            }

            prepare_result
                .source_routes
                .iter()
                .chain(prepare_result.target_routes.iter().flatten())
                .filter_map(|route| {
                    route
                        .leader_peer
                        .clone()
                        .map(|peer| (peer, route.region.id))
                })
                .collect()
        };

        if self.data.manifests_generated {
            match self.apply_staged_manifests(false).await {
                Ok(Some(response)) => {
                    let summary = self.data.note_manifest_application(false, Some(&response));
                    self.context.record_manifest_summary(summary);
                }
                Ok(None) => {
                    debug!(
                        "repartition group {:?}: no staged manifests to discard",
                        self.data.entry.group_id
                    );
                    let summary = self.data.note_manifest_application(false, None);
                    self.context.record_manifest_summary(summary);
                }
                Err(err) => {
                    let err_msg = err.to_string();
                    self.handle_manifest_failure(ManifestStatus::Failed, err_msg);
                    return Err(err);
                }
            }
        }

        if !self.data.staged_regions.is_empty() {
            let route_manager = self.context.table_metadata_manager.table_route_manager();

            for region_id in &self.data.staged_regions {
                route_manager
                    .set_region_staging_state(*region_id, false)
                    .await
                    .context(error::TableMetadataManagerSnafu)?;
            }

            Self::clear_regions_staging(
                &mut self.data.route_snapshot.region_routes,
                &self.data.staged_regions,
            );
            self.data.staged_regions.clear();
            self.data.metadata_updated = false;
        }

        if self.data.region_rule_updated {
            if let Some(rule_version) = self.data.region_rule_version.clone() {
                if publish_rules {
                    for (peer, region_id) in &leader_targets {
                        self.context
                            .publish_region_rule_on_datanode(peer, *region_id, &rule_version)
                            .await?;
                    }
                } else {
                    for (peer, region_id) in &leader_targets {
                        self.context
                            .clear_region_rule_stage_on_datanode(peer, *region_id)
                            .await?;
                    }
                }
            }
            self.data.region_rule_updated = false;
            self.data.region_rule_version = None;
        }

        self.data.freeze_completed = false;
        self.data.route_committed = false;

        if publish_rules {
            info!(
                "repartition group {:?}: cleanup finished",
                self.data.entry.group_id
            );
        } else {
            info!(
                "repartition group {:?}: rollback cleanup finished",
                self.data.entry.group_id
            );
        }
        Ok(())
    }

    /// Borrow the cached prepare result or surface a friendly error if it has
    /// not been initialised yet.
    fn prepare_result(&self) -> Result<&GroupPrepareResult> {
        self.data.prepare_result.as_ref().ok_or_else(|| {
            error::RepartitionMissingPrepareContextSnafu {
                group_id: self.data.entry.group_id,
            }
            .build()
        })
    }

    /// Resolve the leader peer for the central region – a prerequisite for the
    /// datanode-side RPCs we send throughout the workflow.
    fn leader_peer_for_central(&self, prepare_result: &GroupPrepareResult) -> Result<Peer> {
        let central_region = prepare_result.central_region;
        let leader_peer = prepare_result
            .source_routes
            .iter()
            .find(|route| route.region.id == central_region)
            .and_then(|route| route.leader_peer.clone())
            .or_else(|| {
                self.data
                    .route_snapshot
                    .region_routes
                    .iter()
                    .find(|route| route.region.id == central_region)
                    .and_then(|route| route.leader_peer.clone())
            });

        let Some(peer) = leader_peer else {
            return error::RetryLaterSnafu {
                reason: format!(
                    "group {} missing leader for central region {}",
                    self.data.entry.group_id, central_region
                ),
            }
            .fail();
        };

        Ok(peer)
    }

    /// Ask the datanode to either publish or discard staged manifests for all
    /// regions tracked as staging.
    async fn apply_staged_manifests(&self, publish: bool) -> Result<Option<RegionResponse>> {
        if self.data.staged_regions.is_empty() {
            return Ok(None);
        }

        let prepare_result = self.prepare_result()?;
        let leader_peer = self.leader_peer_for_central(prepare_result)?;

        let request = ApplyStagedManifestRequest {
            table_id: self.data.table_id as u64,
            group_id: self.data.entry.group_id.to_string(),
            region_ids: self
                .data
                .staged_regions
                .iter()
                .map(|region_id| region_id.as_u64())
                .collect(),
            publish,
        };

        let response = self
            .context
            .apply_staged_manifests_on_datanode(&leader_peer, request)
            .await?;

        Ok(Some(response))
    }

    /// Pause the target region if it currently has a leader peer.
    async fn pause_region(&self, _route: &RegionRoute) -> Result<()> {
        if let Some(peer) = &_route.leader_peer {
            self.context
                .pause_region_on_datanode(peer, _route.region.id)
                .await?;
        } else {
            debug!(
                "repartition group {:?}: skip pause, region {} has no leader peer",
                self.data.entry.group_id, _route.region.id
            );
        }
        Ok(())
    }

    /// Resume the target region if it currently has a leader peer.
    async fn resume_region(&self, _route: &RegionRoute) -> Result<()> {
        if let Some(peer) = &_route.leader_peer {
            self.context
                .resume_region_on_datanode(peer, _route.region.id)
                .await?;
        } else {
            debug!(
                "repartition group {:?}: skip resume, region {} has no leader peer",
                self.data.entry.group_id, _route.region.id
            );
        }
        Ok(())
    }

    fn mark_regions_staging(routes: &mut [RegionRoute], region_ids: &[RegionId]) {
        for region_id in region_ids.iter().copied() {
            if let Some(route) = routes.iter_mut().find(|route| route.region.id == region_id) {
                route.set_leader_staging();
            }
        }
    }

    fn clear_regions_staging(routes: &mut [RegionRoute], region_ids: &[RegionId]) {
        for region_id in region_ids.iter().copied() {
            if let Some(route) = routes.iter_mut().find(|route| route.region.id == region_id) {
                route.clear_leader_staging();
            }
        }
    }

    fn collect_existing_region_ids(&self, prepare_result: &GroupPrepareResult) -> Vec<RegionId> {
        let mut set = BTreeSet::new();
        for route in &prepare_result.source_routes {
            set.insert(route.region.id);
        }
        for route in prepare_result.target_routes.iter().flatten() {
            set.insert(route.region.id);
        }
        set.into_iter().collect()
    }

    fn apply_target_partition_rules(&mut self) -> Result<()> {
        for target in &self.data.entry.targets {
            if let Some(region_id) = target.region_id {
                Self::apply_partition_rule(
                    &mut self.data.route_snapshot.region_routes,
                    region_id,
                    target,
                )?;
            }
        }
        Ok(())
    }

    async fn ensure_region_info(&mut self) -> Result<&RegionInfo> {
        if self.data.region_info.is_none() {
            let central_region = self.prepare_result()?.central_region;
            let leader_peer_id = self
                .data
                .route_snapshot
                .region_routes
                .iter()
                .find(|route| route.region.id == central_region)
                .and_then(|route| route.leader_peer.as_ref())
                .map(|peer| peer.id)
                .context(error::UnexpectedSnafu {
                    violated: format!(
                        "group {} has no leader peer for region {}",
                        self.data.entry.group_id, central_region
                    ),
                })?;

            let datanode_table_value = self
                .context
                .table_metadata_manager
                .datanode_table_manager()
                .get(&DatanodeTableKey {
                    datanode_id: leader_peer_id,
                    table_id: self.data.table_id,
                })
                .await
                .context(error::TableMetadataManagerSnafu)?;

            let Some(datanode_table_value) = datanode_table_value else {
                return error::RetryLaterSnafu {
                    reason: format!(
                        "group {} waiting for datanode {} metadata to propagate",
                        self.data.entry.group_id, leader_peer_id
                    ),
                }
                .fail();
            };

            self.data.region_info = Some(datanode_table_value.region_info.clone());
        }

        Ok(self.data.region_info.as_ref().unwrap())
    }

    fn ensure_target_routes_present(&mut self) -> Result<()> {
        let leader_candidate = self
            .data
            .entry
            .sources
            .iter()
            .filter_map(|descriptor| descriptor.region_id)
            .find_map(|region_id| {
                self.data
                    .route_snapshot
                    .region_routes
                    .iter()
                    .find(|route| route.region.id == region_id)
                    .and_then(|route| route.leader_peer.clone())
            });

        for descriptor in &self.data.entry.targets {
            let Some(region_id) = descriptor.region_id else {
                continue;
            };

            if self
                .data
                .route_snapshot
                .region_routes
                .iter()
                .any(|route| route.region.id == region_id)
            {
                continue;
            }

            let partition_expr = descriptor
                .partition_expr
                .as_json_str()
                .context(error::RepartitionSerializePartitionExprSnafu)?;

            let region = Region {
                id: region_id,
                partition_expr,
                ..Default::default()
            };

            self.data.route_snapshot.region_routes.push(RegionRoute {
                region,
                leader_peer: leader_candidate.clone(),
                ..Default::default()
            });
        }
        Ok(())
    }

    fn ensure_targets_allocated(&self) -> Result<()> {
        if let Some((idx, _)) = self
            .data
            .entry
            .targets
            .iter()
            .enumerate()
            .find(|(_, descriptor)| descriptor.region_id.is_none())
        {
            return error::RepartitionMissingTargetRegionIdSnafu {
                group_id: self.data.entry.group_id,
                target_index: idx,
            }
            .fail();
        }

        Ok(())
    }

    fn apply_partition_rule(
        routes: &mut [RegionRoute],
        region_id: RegionId,
        descriptor: &RegionDescriptor,
    ) -> Result<()> {
        let Some(route) = routes.iter_mut().find(|route| route.region.id == region_id) else {
            return error::RepartitionTargetRegionRouteMissingSnafu { region_id }.fail();
        };

        route.region.partition_expr = descriptor
            .partition_expr
            .as_json_str()
            .context(error::RepartitionSerializePartitionExprSnafu)?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Procedure for RepartitionGroupProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let status = self.step().await.map_err(super::map_repartition_error)?;
        Ok(status)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        LockKey::new(vec![
            CatalogLock::Read(self.data.catalog_name.as_str()).into(),
            SchemaLock::read(
                self.data.catalog_name.as_str(),
                self.data.schema_name.as_str(),
            )
            .into(),
            TableLock::Write(self.data.table_id).into(),
        ])
    }
}
