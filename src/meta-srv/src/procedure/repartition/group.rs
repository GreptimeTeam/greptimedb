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

use std::collections::BTreeSet;

use common_meta::ddl::utils::map_to_procedure_error;
use common_meta::error::{self, Result as MetaResult};
use common_meta::rpc::router::RegionRoute;
use common_procedure::error::{Result as ProcedureResult, ToJsonSnafu};
use common_procedure::{Context as ProcedureContext, LockKey, Procedure, Status};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::storage::{RegionId, TableId};
use strum::AsRefStr;

use crate::procedure::repartition::context::RepartitionContext;
use crate::procedure::repartition::plan::PlanEntry;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GroupProcedureData {
    table_id: TableId,
    entry: PlanEntry,
    state: GroupState,
    route_snapshot: Vec<RegionRoute>,
    #[serde(default)]
    prepare_result: Option<GroupPrepareResult>,
    #[serde(default)]
    freeze_completed: bool,
    #[serde(default)]
    metadata_updated: bool,
    #[serde(default)]
    staged_regions: Vec<RegionId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GroupPrepareResult {
    source_routes: Vec<RegionRoute>,
    target_routes: Vec<Option<RegionRoute>>,
    central_region: u64,
}

pub struct RepartitionGroupProcedure {
    context: RepartitionContext,
    data: GroupProcedureData,
}

impl RepartitionGroupProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::RepartitionGroup";

    pub fn new(
        entry: PlanEntry,
        table_id: TableId,
        route_snapshot: Vec<RegionRoute>,
        context: RepartitionContext,
    ) -> Self {
        Self {
            context,
            data: GroupProcedureData {
                table_id,
                entry,
                state: GroupState::Prepare,
                route_snapshot,
                prepare_result: None,
                freeze_completed: false,
                metadata_updated: false,
                staged_regions: Vec::new(),
            },
        }
    }

    pub async fn step(&mut self) -> MetaResult<Status> {
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
                self.data.state = GroupState::Finished;
                Ok(Status::done())
            }
            GroupState::Finished => Ok(Status::done()),
        }
    }

    async fn on_prepare(&mut self) -> MetaResult<()> {
        if self.data.prepare_result.is_some() {
            return Ok(());
        }

        let (_, latest_route) = self
            .context
            .table_metadata_manager
            .table_route_manager()
            .get_physical_table_route(self.data.table_id)
            .await?;

        self.data.route_snapshot = latest_route.region_routes.clone();

        let mut source_routes = Vec::with_capacity(self.data.entry.sources.len());
        for descriptor in &self.data.entry.sources {
            let Some(region_id) = descriptor.region_id else {
                return Err(error::UnsupportedSnafu {
                    operation: format!("group {} lacks source region id", self.data.entry.group_id),
                }
                .build());
            };

            let Some(route) = latest_route
                .region_routes
                .iter()
                .find(|route| route.region.id == region_id)
            else {
                return Err(error::UnsupportedSnafu {
                    operation: format!(
                        "group {} source region {region_id} missing from route snapshot",
                        self.data.entry.group_id
                    ),
                }
                .build());
            };
            source_routes.push(route.clone());
        }

        if source_routes.is_empty() {
            return Err(error::UnsupportedSnafu {
                operation: format!(
                    "group {} has no source regions after planning",
                    self.data.entry.group_id
                ),
            }
            .build());
        }

        let mut target_routes = Vec::with_capacity(self.data.entry.targets.len());
        for descriptor in &self.data.entry.targets {
            if let Some(region_id) = descriptor.region_id {
                let Some(route) = latest_route
                    .region_routes
                    .iter()
                    .find(|route| route.region.id == region_id)
                else {
                    return Err(error::UnsupportedSnafu {
                        operation: format!(
                            "group {} target region {region_id} missing from route snapshot",
                            self.data.entry.group_id
                        ),
                    }
                    .build());
                };

                target_routes.push(Some(route.clone()));
            } else {
                target_routes.push(None);
            }
        }

        let central_region = source_routes[0].region.id.as_u64();
        self.data.prepare_result = Some(GroupPrepareResult {
            source_routes,
            target_routes,
            central_region,
        });

        Ok(())
    }

    async fn on_freeze(&mut self) -> MetaResult<()> {
        if self.data.freeze_completed {
            return Ok(());
        }

        let prepare_result = self.prepare_result()?;

        for route in &prepare_result.source_routes {
            self.pause_region(route).await?;
        }

        for route in prepare_result.target_routes.iter().flatten() {
            self.pause_region(route).await?;
        }

        self.data.freeze_completed = true;
        Ok(())
    }

    async fn on_update_metadata(&mut self) -> MetaResult<()> {
        if self.data.metadata_updated {
            return Ok(());
        }

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
                    .await?;
            }
        }

        Self::mark_regions_staging(&mut self.data.route_snapshot, &region_ids);
        self.data.staged_regions = staged_set.into_iter().collect();

        self.data.metadata_updated = true;
        Ok(())
    }

    async fn on_update_region_rule(&mut self) -> MetaResult<()> {
        // TODO: Push new region rules to datanodes and reject outdated writes.
        Ok(())
    }

    async fn on_update_manifests(&mut self) -> MetaResult<()> {
        // TODO: Generate and submit new manifests for target regions.
        Ok(())
    }

    async fn on_confirm(&mut self) -> MetaResult<()> {
        // TODO: Confirm staged writes, resume ingestion, and record success.
        Ok(())
    }

    async fn on_cleanup(&mut self) -> MetaResult<()> {
        let prepare_result = self.prepare_result()?;

        for route in &prepare_result.source_routes {
            self.resume_region(route).await?;
        }
        for route in prepare_result.target_routes.iter().flatten() {
            self.resume_region(route).await?;
        }

        if !self.data.staged_regions.is_empty() {
            let route_manager = self.context.table_metadata_manager.table_route_manager();

            for region_id in &self.data.staged_regions {
                route_manager
                    .set_region_staging_state(*region_id, false)
                    .await?;
            }

            Self::clear_regions_staging(&mut self.data.route_snapshot, &self.data.staged_regions);
            self.data.staged_regions.clear();
            self.data.metadata_updated = false;
        }

        self.data.freeze_completed = false;
        Ok(())
    }

    fn prepare_result(&self) -> MetaResult<&GroupPrepareResult> {
        self.data.prepare_result.as_ref().ok_or_else(|| {
            error::UnsupportedSnafu {
                operation: format!(
                    "group {} is missing prepare context",
                    self.data.entry.group_id
                ),
            }
            .build()
        })
    }

    async fn pause_region(&self, _route: &RegionRoute) -> MetaResult<()> {
        // TODO: invoke datanode RPC to pause compaction and snapshot for the region.
        Ok(())
    }

    async fn resume_region(&self, _route: &RegionRoute) -> MetaResult<()> {
        // TODO: invoke datanode RPC to resume compaction and snapshot for the region.
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
}

#[async_trait::async_trait]
impl Procedure for RepartitionGroupProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let status = self.step().await.map_err(map_to_procedure_error)?;
        Ok(status)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        LockKey::default()
    }
}
