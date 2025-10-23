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

mod context;
mod group;
mod plan;

use common_catalog::format_full_table_name;
use common_meta::ddl::DdlContext;
use common_meta::ddl::utils::map_to_procedure_error;
use common_meta::error::{self, Result as MetaResult};
use common_meta::lock_key::{CatalogLock, SchemaLock, TableLock};
use common_procedure::error::{FromJsonSnafu, Result as ProcedureResult, ToJsonSnafu};
use common_procedure::{Context as ProcedureContext, LockKey, Procedure, ProcedureWithId, Status};
use partition::expr::PartitionExpr;
use partition::subtask;
use serde::{Deserialize, Serialize};
use serde_json;
use snafu::ResultExt;
use store_api::storage::TableId;
use strum::AsRefStr;
use table::table_reference::TableReference;
use uuid::Uuid;

use self::context::RepartitionContext;
use self::group::RepartitionGroupProcedure;
use self::plan::{
    PartitionRuleDiff, PlanEntry, PlanGroupId, RegionDescriptor, RepartitionPlan, ResourceDemand,
};

/// Procedure that orchestrates the repartition flow.
pub struct RepartitionProcedure {
    context: DdlContext,
    group_context: RepartitionContext,
    data: RepartitionData,
}

impl RepartitionProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::Repartition";

    pub fn new(task: RepartitionTask, context: DdlContext) -> MetaResult<Self> {
        let group_context = RepartitionContext::new(&context);
        Ok(Self {
            context,
            group_context,
            data: RepartitionData::new(task),
        })
    }

    pub fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data: RepartitionData = serde_json::from_str(json).context(FromJsonSnafu)?;
        let group_context = RepartitionContext::new(&context);
        Ok(Self {
            context,
            group_context,
            data,
        })
    }

    async fn on_prepare(&mut self) -> MetaResult<Status> {
        if self.data.plan.is_none() {
            self.build_plan().await?;
        }

        self.data.state = RepartitionState::AllocateResources;
        Ok(Status::executing(true))
    }

    async fn on_allocate_resources(&mut self) -> MetaResult<Status> {
        if !self.data.resource_allocated {
            let demand = self.data.resource_demand.unwrap_or_default();
            let allocated = self.allocate_resources(demand).await?;
            if !allocated {
                if let Some(plan) = &self.data.plan {
                    self.data
                        .failed_groups
                        .extend(plan.entries.iter().map(|entry| entry.group_id));
                }
                self.data.state = RepartitionState::Finalize;
                return Ok(Status::executing(true));
            }
            self.data.resource_allocated = true;
        }

        self.data.state = RepartitionState::DispatchSubprocedures;
        Ok(Status::executing(true))
    }

    async fn on_dispatch_subprocedures(&mut self) -> MetaResult<Status> {
        let plan = match self.data.plan.as_ref() {
            Some(plan) => plan,
            None => {
                self.data.state = RepartitionState::Finalize;
                return Ok(Status::executing(true));
            }
        };

        let groups_to_schedule: Vec<PlanGroupId> = plan
            .entries
            .iter()
            .filter(|entry| {
                !self.data.succeeded_groups.contains(&entry.group_id)
                    && !self.data.failed_groups.contains(&entry.group_id)
            })
            .map(|entry| entry.group_id)
            .collect();

        if groups_to_schedule.is_empty() {
            self.data.state = RepartitionState::Finalize;
            return Ok(Status::executing(true));
        }

        let subprocedures = self.spawn_group_procedures(plan, &groups_to_schedule);
        self.data.pending_groups = groups_to_schedule;
        self.data.state = RepartitionState::CollectSubprocedures;

        Ok(Status::suspended(subprocedures, true))
    }

    async fn on_collect_subprocedures(&mut self, _ctx: &ProcedureContext) -> MetaResult<Status> {
        self.data
            .succeeded_groups
            .append(&mut self.data.pending_groups);

        self.data.state = RepartitionState::Finalize;
        Ok(Status::executing(true))
    }

    async fn on_finalize(&mut self) -> MetaResult<Status> {
        self.data.summary = Some(RepartitionSummary {
            succeeded_groups: self.data.succeeded_groups.clone(),
            failed_groups: self.data.failed_groups.clone(),
        });
        self.data.state = RepartitionState::Finished;
        Ok(Status::done())
    }

    async fn build_plan(&mut self) -> MetaResult<()> {
        let table_id = self.data.task.table_id;
        let table_ref = self.data.task.table_ref();
        let table_name_str =
            format_full_table_name(table_ref.catalog, table_ref.schema, table_ref.table);

        self.context
            .table_metadata_manager
            .table_info_manager()
            .get(table_id)
            .await?
            .ok_or_else(|| {
                error::TableNotFoundSnafu {
                    table_name: table_name_str.clone(),
                }
                .build()
            })?;

        let (physical_table_id, physical_route) = self
            .context
            .table_metadata_manager
            .table_route_manager()
            .get_physical_table_route(table_id)
            .await?;

        let from_exprs_json = self.data.task.from_exprs_json.clone();
        let into_exprs_json = self.data.task.into_exprs_json.clone();

        let from_exprs = Self::deserialize_partition_exprs(&from_exprs_json)?;
        let into_exprs = Self::deserialize_partition_exprs(&into_exprs_json)?;

        let existing_regions = physical_route
            .region_routes
            .iter()
            .map(|route| (route.region.id, route.region.partition_expr()))
            .collect::<Vec<_>>();

        let mut used_regions = vec![false; existing_regions.len()];
        let mut source_descriptor_by_index = Vec::with_capacity(from_exprs_json.len());
        for expr_json in &from_exprs_json {
            let mut found = None;
            for (idx, (region_id, expr)) in existing_regions.iter().enumerate() {
                if !used_regions[idx] && expr == expr_json {
                    found = Some((*region_id, expr.clone()));
                    used_regions[idx] = true;
                    break;
                }
            }

            let (region_id, partition_expr_json) = found.ok_or_else(|| {
                error::UnsupportedSnafu {
                    operation: format!(
                        "repartition source expression '{}' does not match any existing region",
                        expr_json
                    ),
                }
                .build()
            })?;

            source_descriptor_by_index.push(RegionDescriptor {
                region_id: Some(region_id),
                partition_expr_json,
            });
        }

        let subtasks = subtask::create_subtasks(&from_exprs, &into_exprs).map_err(|err| {
            error::UnsupportedSnafu {
                operation: format!("create_subtasks failed: {err}"),
            }
            .build()
        })?;

        let mut plan = RepartitionPlan::empty(physical_table_id);
        let mut diff = PartitionRuleDiff::default();
        let mut demand = ResourceDemand::default();

        for subtask in subtasks {
            let group_id = Uuid::new_v4();

            let sources = subtask
                .from_expr_indices
                .iter()
                .map(|&idx| source_descriptor_by_index[idx].clone())
                .collect::<Vec<_>>();

            let targets = subtask
                .to_expr_indices
                .iter()
                .enumerate()
                .map(|(position, &idx)| {
                    let reused_region = if subtask.from_expr_indices.len() == 1 {
                        if position == 0 {
                            sources.get(0).and_then(|descriptor| descriptor.region_id)
                        } else {
                            None
                        }
                    } else if subtask.to_expr_indices.len() == 1 {
                        sources.first().and_then(|descriptor| descriptor.region_id)
                    } else {
                        sources
                            .get(position)
                            .and_then(|descriptor| descriptor.region_id)
                    };

                    RegionDescriptor {
                        region_id: reused_region,
                        partition_expr_json: into_exprs_json[idx].clone(),
                    }
                })
                .collect::<Vec<_>>();

            let entry = PlanEntry::new(group_id, subtask, sources, targets);
            demand.add_entry(&entry);
            diff.entries.push(group_id);
            plan.entries.push(entry);
        }

        plan.resource_demand = demand;
        plan.route_snapshot = physical_route.clone();
        plan.plan_hash = format!(
            "{}:{}->{}",
            physical_table_id,
            Self::expr_signature(&from_exprs_json),
            Self::expr_signature(&into_exprs_json)
        );

        self.data.plan = Some(plan);
        self.data.rule_diff = Some(diff);
        self.data.resource_demand = Some(demand);

        Ok(())
    }

    async fn allocate_resources(&self, _demand: ResourceDemand) -> MetaResult<bool> {
        Ok(true)
    }

    fn spawn_group_procedures(
        &self,
        plan: &RepartitionPlan,
        group_ids: &[PlanGroupId],
    ) -> Vec<ProcedureWithId> {
        group_ids
            .iter()
            .filter_map(|group_id| {
                plan.entries
                    .iter()
                    .find(|entry| entry.group_id == *group_id)
                    .cloned()
            })
            .map(|entry| {
                let route_snapshot = plan.route_snapshot.region_routes.clone();
                ProcedureWithId::with_random_id(Box::new(RepartitionGroupProcedure::new(
                    entry,
                    plan.table_id,
                    route_snapshot,
                    self.group_context.clone(),
                )))
            })
            .collect()
    }

    fn table_lock_key(&self) -> Vec<common_procedure::StringKey> {
        let mut lock_key = Vec::with_capacity(3);
        let table_ref = self.data.task.table_ref();
        lock_key.push(CatalogLock::Read(table_ref.catalog).into());
        lock_key.push(SchemaLock::read(table_ref.catalog, table_ref.schema).into());
        lock_key.push(TableLock::Write(self.data.task.table_id).into());

        lock_key
    }

    fn deserialize_partition_exprs(exprs_json: &[String]) -> MetaResult<Vec<PartitionExpr>> {
        exprs_json
            .iter()
            .map(|json| {
                let expr = PartitionExpr::from_json_str(json).map_err(|err| {
                    error::UnsupportedSnafu {
                        operation: format!(
                            "deserialize partition expression '{json}' failed: {err}"
                        ),
                    }
                    .build()
                })?;

                expr.ok_or_else(|| {
                    error::UnsupportedSnafu {
                        operation: format!("empty partition expression '{json}'"),
                    }
                    .build()
                })
            })
            .collect()
    }

    fn expr_signature(exprs: &[String]) -> String {
        exprs.join("|")
    }
}

#[async_trait::async_trait]
impl Procedure for RepartitionProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = self.data.state;
        let status = match state {
            RepartitionState::Prepare => self.on_prepare().await,
            RepartitionState::AllocateResources => self.on_allocate_resources().await,
            RepartitionState::DispatchSubprocedures => self.on_dispatch_subprocedures().await,
            RepartitionState::CollectSubprocedures => self.on_collect_subprocedures(ctx).await,
            RepartitionState::Finalize => self.on_finalize().await,
            RepartitionState::Finished => Ok(Status::done()),
        };

        status.map_err(map_to_procedure_error)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        LockKey::new(self.table_lock_key())
    }
}

/// Serialized data of the repartition procedure.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RepartitionData {
    state: RepartitionState,
    task: RepartitionTask,
    #[serde(default)]
    plan: Option<RepartitionPlan>,
    #[serde(default)]
    rule_diff: Option<PartitionRuleDiff>,
    #[serde(default)]
    resource_demand: Option<ResourceDemand>,
    #[serde(default)]
    resource_allocated: bool,
    #[serde(default)]
    pending_groups: Vec<PlanGroupId>,
    #[serde(default)]
    succeeded_groups: Vec<PlanGroupId>,
    #[serde(default)]
    failed_groups: Vec<PlanGroupId>,
    #[serde(default)]
    summary: Option<RepartitionSummary>,
}

impl RepartitionData {
    fn new(task: RepartitionTask) -> Self {
        Self {
            state: RepartitionState::Prepare,
            task,
            plan: None,
            rule_diff: None,
            resource_demand: None,
            resource_allocated: false,
            pending_groups: Vec::new(),
            succeeded_groups: Vec::new(),
            failed_groups: Vec::new(),
            summary: None,
        }
    }
}

/// High level states of the repartition procedure.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, AsRefStr)]
enum RepartitionState {
    Prepare,
    AllocateResources,
    DispatchSubprocedures,
    CollectSubprocedures,
    Finalize,
    Finished,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RepartitionSummary {
    succeeded_groups: Vec<PlanGroupId>,
    failed_groups: Vec<PlanGroupId>,
}

/// Task payload passed from the DDL entry point.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepartitionTask {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub table_id: TableId,
    /// Partition expressions of source regions (JSON encoded `PartitionExpr`).
    pub from_exprs_json: Vec<String>,
    /// Partition expressions of target regions (JSON encoded `PartitionExpr`).
    pub into_exprs_json: Vec<String>,
}

impl RepartitionTask {
    fn table_ref(&self) -> TableReference<'_> {
        TableReference {
            catalog: &self.catalog_name,
            schema: &self.schema_name,
            table: &self.table_name,
        }
    }
}
