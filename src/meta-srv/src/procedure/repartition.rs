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

use std::collections::HashMap;

use common_meta::ddl::DdlContext;
use common_meta::key::table_route::PhysicalTableRouteValue;
use common_meta::lock_key::{CatalogLock, SchemaLock, TableLock};
use common_procedure::error::{Error as ProcedureError, Result as ProcedureResult, ToJsonSnafu};
use common_procedure::{
    Context as ProcedureContext, LockKey, Procedure, ProcedureId, ProcedureWithId, Status,
};
use common_telemetry::error;
use partition::expr::PartitionExpr;
use partition::subtask::{self, RepartitionSubtask};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use store_api::storage::TableId;
use strum::AsRefStr;
use uuid::Uuid;

use self::context::{GroupManifestSummary, RepartitionContext};
use self::group::RepartitionGroupProcedure;
use self::plan::{PlanEntry, PlanGroupId, RegionDescriptor, RepartitionPlan, ResourceDemand};
use crate::error::{self, Result};

/// Task payload passed from the DDL entry point.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepartitionTask {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub table_id: TableId,
    /// Partition expressions representing the source regions.
    pub from_exprs: Vec<PartitionExpr>,
    /// Partition expressions representing the target regions.
    pub into_exprs: Vec<PartitionExpr>,
}

/// Procedure that orchestrates the repartition flow.
pub struct RepartitionProcedure {
    context: DdlContext,
    group_context: RepartitionContext,
    data: RepartitionData,
}

impl RepartitionProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::Repartition";

    /// Constructs a new procedure instance from a task payload.
    pub fn new(task: RepartitionTask, context: DdlContext) -> Result<Self> {
        let group_context = RepartitionContext::new(&context);
        Ok(Self {
            context,
            group_context,
            data: RepartitionData::new(task),
        })
    }

    /// Builds the repartition plan if we have not done so yet.
    async fn on_prepare(&mut self) -> Result<Status> {
        if self.data.plan.is_none() {
            self.build_plan().await?;
        }

        self.data.state = RepartitionState::AllocateResources;
        Ok(Status::executing(true))
    }

    /// Allocates target regions and decides whether the procedure can proceed.
    async fn on_allocate_resources(&mut self) -> Result<Status> {
        if !self.data.resource_allocated {
            let allocated = self.allocate_resources().await?;
            if !allocated {
                if let Some(plan) = &self.data.plan {
                    let failed_groups = plan.entries.iter().map(|entry| entry.group_id);
                    self.data.failed_groups.extend(failed_groups);
                }
                self.data.state = RepartitionState::Finalize;
                return Ok(Status::executing(true));
            }
            self.data.resource_allocated = true;
        }

        self.data.state = RepartitionState::DispatchSubprocedures;
        Ok(Status::executing(true))
    }

    /// Spawns group subprocedures for every pending plan entry.
    async fn on_dispatch_subprocedures(&mut self) -> Result<Status> {
        let plan = match self.data.plan.as_ref() {
            Some(plan) => plan,
            None => {
                self.data.state = RepartitionState::Finalize;
                return Ok(Status::executing(true));
            }
        };

        let entries_to_schedule: Vec<PlanEntry> = plan
            .entries
            .iter()
            .filter(|entry| {
                !self.data.succeeded_groups.contains(&entry.group_id)
                    && !self.data.failed_groups.contains(&entry.group_id)
            })
            .cloned()
            .collect();

        if entries_to_schedule.is_empty() {
            self.data.state = RepartitionState::Finalize;
            return Ok(Status::executing(true));
        }

        let groups_to_schedule: Vec<PlanGroupId> = entries_to_schedule
            .iter()
            .map(|entry| entry.group_id)
            .collect();

        let subprocedures = self.spawn_group_procedures(
            plan.table_id,
            plan.route_snapshot.clone(),
            entries_to_schedule,
        );
        self.data.pending_groups = groups_to_schedule;
        self.data.state = RepartitionState::CollectSubprocedures;

        Ok(Status::suspended(subprocedures, true))
    }

    /// Records the list of subprocedures that finished and move to finalisation.
    async fn on_collect_subprocedures(&mut self, ctx: &ProcedureContext) -> Result<Status> {
        let pending = std::mem::take(&mut self.data.pending_groups);
        let mut first_error: Option<error::Error> = None;
        let mut succeeded = Vec::new();

        for group_id in pending {
            let procedure_id = match self.data.group_subprocedures.remove(&group_id) {
                Some(id) => id,
                None => {
                    let err = error::RepartitionSubprocedureUnknownSnafu { group_id }.build();
                    self.data.failed_groups.push(group_id);
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                    continue;
                }
            };

            let state_opt = ctx.provider.procedure_state(procedure_id).await.context(
                error::RepartitionSubprocedureStateFetchSnafu {
                    group_id,
                    procedure_id,
                },
            )?;

            let state = match state_opt {
                Some(state) => state,
                None => {
                    let err = error::RepartitionSubprocedureStateMissingSnafu {
                        group_id,
                        procedure_id,
                    }
                    .build();
                    self.data.failed_groups.push(group_id);
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                    continue;
                }
            };

            if state.is_done() {
                succeeded.push(group_id);
                continue;
            }

            let reason = state
                .error()
                .map(|err| err.to_string())
                .unwrap_or_else(|| format!("subprocedure state {}", state.as_str_name()));
            let err = error::RepartitionSubprocedureFailedSnafu {
                group_id,
                procedure_id,
                reason,
            }
            .build();
            self.data.failed_groups.push(group_id);
            if first_error.is_none() {
                first_error = Some(err);
            }
        }

        self.data.succeeded_groups.extend(succeeded);
        self.data.state = RepartitionState::Finalize;

        if let Some(err) = first_error {
            return Err(err);
        }

        Ok(Status::executing(true))
    }

    /// Builds the summary that will be returned to the caller.
    async fn on_finalize(&mut self) -> Result<Status> {
        self.deallocate_resources().await?;

        self.data.summary = Some(RepartitionSummary {
            succeeded_groups: self.data.succeeded_groups.clone(),
            failed_groups: self.data.failed_groups.clone(),
            manifest_summaries: self.group_context.manifest_summaries(),
        });
        self.group_context.clear_group_records();
        self.data.state = RepartitionState::Finished;
        Ok(Status::done())
    }

    /// Constructs the repartition plan from the task specification.
    async fn build_plan(&mut self) -> Result<()> {
        let table_id = self.data.task.table_id;
        let from_exprs = &self.data.task.from_exprs;
        let into_exprs = &self.data.task.into_exprs;

        let (physical_table_id, physical_route) = self
            .context
            .table_metadata_manager
            .table_route_manager()
            .get_physical_table_route(table_id)
            .await
            .context(error::TableMetadataManagerSnafu)?;

        let src_descriptors = Self::source_region_descriptors(from_exprs, &physical_route)?;
        let subtasks = subtask::create_subtasks(from_exprs, into_exprs)
            .context(error::RepartitionCreateSubtasksSnafu)?;
        let entries = Self::build_plan_entries(subtasks, &src_descriptors, into_exprs);

        let demand = ResourceDemand::from_plan_entries(&entries);
        let plan = RepartitionPlan::new(physical_table_id, entries, demand, physical_route.clone());
        self.data.plan = Some(plan);

        Ok(())
    }

    fn source_region_descriptors(
        from_exprs: &[PartitionExpr],
        physical_route: &PhysicalTableRouteValue,
    ) -> Result<Vec<RegionDescriptor>> {
        let existing_regions = physical_route
            .region_routes
            .iter()
            .map(|route| (route.region.id, route.region.partition_expr()))
            .collect::<Vec<_>>();

        let descriptors = from_exprs
            .iter()
            .map(|expr| {
                let expr_json = expr
                    .as_json_str()
                    .context(error::RepartitionSerializePartitionExprSnafu)?;

                let matched_region_id = existing_regions
                    .iter()
                    .find_map(|(region_id, existing_expr)| {
                        (existing_expr == &expr_json).then_some(*region_id)
                    })
                    .with_context(|| error::RepartitionSourceExprMismatchSnafu {
                        expr: expr_json,
                    })?;

                Ok(RegionDescriptor {
                    region_id: Some(matched_region_id),
                    partition_expr: expr.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(descriptors)
    }

    fn build_plan_entries(
        subtasks: Vec<RepartitionSubtask>,
        source_index: &[RegionDescriptor],
        target_exprs: &[PartitionExpr],
    ) -> Vec<PlanEntry> {
        let plan_entries = subtasks
            .into_iter()
            .map(|subtask| {
                let group_id = Uuid::new_v4();
                let sources = subtask
                    .from_expr_indices
                    .iter()
                    .map(|&idx| source_index[idx].clone())
                    .collect::<Vec<_>>();

                let targets = subtask
                    .to_expr_indices
                    .iter()
                    .map(|&idx| RegionDescriptor {
                        region_id: None, // will be assigned later
                        partition_expr: target_exprs[idx].clone(),
                    })
                    .collect::<Vec<_>>();

                PlanEntry::new(group_id, subtask, sources, targets)
            })
            .collect::<Vec<_>>();

        plan_entries
    }

    /// Allocates resources required by the plan. Returning `false`
    /// indicates that the procedure should abort.
    async fn allocate_resources(&mut self) -> Result<bool> {
        todo!("allocate resources");
    }

    async fn deallocate_resources(&mut self) -> Result<()> {
        if !self.data.resource_allocated {
            return Ok(());
        }
        self.data.resource_allocated = false;

        todo!("deallocate resources");
    }

    /// Builds the child procedure list for the provided plan groups.
    fn spawn_group_procedures(
        &mut self,
        table_id: TableId,
        route_snapshot: PhysicalTableRouteValue,
        entries: Vec<PlanEntry>,
    ) -> Vec<ProcedureWithId> {
        let mut id_map = HashMap::new();

        let procedures = entries
            .into_iter()
            .map(|entry| {
                let group_id = entry.group_id;
                let group_procedure = RepartitionGroupProcedure::new(
                    entry,
                    table_id,
                    route_snapshot.clone(),
                    self.data.task.catalog_name.clone(),
                    self.data.task.schema_name.clone(),
                    self.group_context.clone(),
                );
                let procedure = ProcedureWithId::with_random_id(Box::new(group_procedure));
                id_map.insert(group_id, procedure.id);
                procedure
            })
            .collect::<Vec<_>>();

        self.data.group_subprocedures = id_map;
        procedures
    }

    /// Composes the set of locks required to safely mutate table metadata.
    fn table_lock_key(&self) -> Vec<common_procedure::StringKey> {
        let mut lock_key = Vec::with_capacity(3);
        let catalog = self.data.task.catalog_name.as_str();
        let schema = self.data.task.schema_name.as_str();
        lock_key.push(CatalogLock::Read(catalog).into());
        lock_key.push(SchemaLock::read(catalog, schema).into());
        lock_key.push(TableLock::Write(self.data.task.table_id).into());

        lock_key
    }

    async fn trigger_group_rollbacks(&mut self) {
        if self.data.rollback_triggered {
            return;
        }

        match self.group_context.rollback_registered_groups().await {
            Ok(_) => {
                self.data.rollback_triggered = true;
            }
            Err(err) => {
                error!(err; "repartition: rollback of successful groups failed");
                self.data.rollback_triggered = true;
            }
        }
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

        match status {
            Ok(status) => Ok(status),
            Err(err) => {
                self.trigger_group_rollbacks().await;
                if let Err(dealloc_err) = self.deallocate_resources().await {
                    error!(dealloc_err; "repartition: deallocating resources after failure failed");
                }
                Err(map_repartition_error(err))
            }
        }
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
    resource_allocated: bool,
    #[serde(default)]
    pending_groups: Vec<PlanGroupId>,
    #[serde(default)]
    succeeded_groups: Vec<PlanGroupId>,
    #[serde(default)]
    failed_groups: Vec<PlanGroupId>,
    #[serde(default)]
    summary: Option<RepartitionSummary>,
    #[serde(default)]
    rollback_triggered: bool,
    #[serde(default)]
    group_subprocedures: HashMap<PlanGroupId, ProcedureId>,
}

impl RepartitionData {
    /// Initialise the procedure data for a fresh run.
    fn new(task: RepartitionTask) -> Self {
        Self {
            state: RepartitionState::Prepare,
            task,
            plan: None,
            resource_allocated: false,
            pending_groups: Vec::new(),
            succeeded_groups: Vec::new(),
            failed_groups: Vec::new(),
            summary: None,
            rollback_triggered: false,
            group_subprocedures: HashMap::new(),
        }
    }
}

pub(super) fn map_repartition_error(err: error::Error) -> ProcedureError {
    match (err.is_retryable(), err.need_clean_poisons()) {
        (true, true) => ProcedureError::retry_later_and_clean_poisons(err),
        (true, false) => ProcedureError::retry_later(err),
        (false, true) => ProcedureError::external_and_clean_poisons(err),
        (false, false) => ProcedureError::external(err),
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

/// Information returned to the caller after the procedure finishes.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RepartitionSummary {
    succeeded_groups: Vec<PlanGroupId>,
    failed_groups: Vec<PlanGroupId>,
    #[serde(default)]
    manifest_summaries: Vec<GroupManifestSummary>,
}
