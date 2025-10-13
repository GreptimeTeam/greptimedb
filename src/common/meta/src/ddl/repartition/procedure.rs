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

//! High-level state machine for table repartitioning.
//!
//! The [`RepartitionProcedure`] coordinates the full flow described in the RFC:
//! 1. **Prepare** – validate the request, lock the table, compute the rule diff,
//!    and materialize a deterministic [`RepartitionPlan`].
//! 2. **AllocateResources** – estimate [`ResourceDemand`] and talk to PaaS to
//!    reserve the additional regions required by the plan.
//! 3. **DispatchSubprocedures** – split the plan into `PlanGroup`s, spawn
//!    sub-procedures that stop compaction/snapshot, enter the no-ingestion
//!    window, update metadata, push region-rule versions, generate new manifests,
//!    and stage/acknowledge versioned writes via `RegionEdit`.
//! 4. **Finalize** – aggregate results, roll failed groups back (metadata,
//!    manifests, staged data), restart unhealthy regions, unlock the table, and
//!    trigger cache reload / optional compaction in the background.
//!
//! Each step is designed to be idempotent so the framework can retry safely.
//! This file currently contains the skeleton states and will be filled out with
//! concrete logic in follow-up patches.

use common_procedure::error::{FromJsonSnafu, Result as ProcedureResult, ToJsonSnafu};
use common_procedure::{Context as ProcedureContext, LockKey, Procedure, ProcedureWithId, Status};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::storage::TableId;
use strum::AsRefStr;
use table::table_reference::TableReference;
use uuid::Uuid;

use crate::ddl::repartition::{PartitionRuleDiff, PlanGroup, PlanGroupId, RepartitionPlan, ResourceDemand};
use crate::ddl::utils::map_to_procedure_error;
use crate::ddl::DdlContext;
use crate::error::Result;
use crate::lock_key::{CatalogLock, SchemaLock, TableLock};

/// Procedure that orchestrates the repartition flow.
pub struct RepartitionProcedure {
    #[allow(dead_code)]
    context: DdlContext,
    data: RepartitionData,
}

impl RepartitionProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::Repartition";

    pub fn new(task: RepartitionTask, context: DdlContext) -> Result<Self> {
        Ok(Self {
            context,
            data: RepartitionData::new(task),
        })
    }

    pub fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data: RepartitionData = serde_json::from_str(json).context(FromJsonSnafu)?;
        Ok(Self { context, data })
    }

    async fn on_prepare(&mut self) -> Result<Status> {
        if self.data.plan.is_none() {
            let (mut plan, diff, demand) = self.generate_plan_stub()?;
            plan.plan_hash = format!(
                "{}:{}:{}",
                self.data.task.table_id,
                plan.groups.len(),
                self.data.task.new_rule_payload.len()
            );

            self.data.resource_demand = Some(demand);
            self.data.rule_diff = Some(diff);
            self.data.plan = Some(plan);
        }

        self.data.state = RepartitionState::AllocateResources;
        Ok(Status::executing(true))
    }

    async fn on_allocate_resources(&mut self) -> Result<Status> {
        if !self.data.resource_allocated {
            let demand = self.data.resource_demand.unwrap_or_default();
            let allocated = self.allocate_resources_stub(demand).await?;
            if !allocated {
                if let Some(plan) = &self.data.plan {
                    self.data
                        .failed_groups
                        .extend(plan.groups.iter().map(|group| group.group_id));
                }
                self.data.state = RepartitionState::Finalize;
                return Ok(Status::executing(true));
            }
            self.data.resource_allocated = true;
        }

        self.data.state = RepartitionState::DispatchSubprocedures;
        Ok(Status::executing(true))
    }

    async fn on_dispatch_subprocedures(&mut self) -> Result<Status> {
        let Some(plan) = self.data.plan.as_ref() else {
            self.data.state = RepartitionState::Finalize;
            return Ok(Status::executing(true));
        };

        let groups_to_schedule: Vec<PlanGroupId> = plan
            .groups
            .iter()
            .filter(|group| {
                !self.data.succeeded_groups.contains(&group.group_id)
                    && !self.data.failed_groups.contains(&group.group_id)
            })
            .map(|group| group.group_id)
            .collect();

        if groups_to_schedule.is_empty() {
            self.data.state = RepartitionState::Finalize;
            return Ok(Status::executing(true));
        }

        let subprocedures = self.spawn_group_stub_procedures(&groups_to_schedule);
        self.data.pending_groups = groups_to_schedule;
        self.data.state = RepartitionState::CollectSubprocedures;

        Ok(Status::suspended(subprocedures, true))
    }

    async fn on_collect_subprocedures(&mut self, _ctx: &ProcedureContext) -> Result<Status> {
        self.data
            .succeeded_groups
            .extend(self.data.pending_groups.drain(..));

        self.data.state = RepartitionState::Finalize;
        Ok(Status::executing(true))
    }

    async fn on_finalize(&mut self) -> Result<Status> {
        self.data.summary = Some(RepartitionSummary {
            succeeded_groups: self.data.succeeded_groups.clone(),
            failed_groups: self.data.failed_groups.clone(),
        });
        self.data.state = RepartitionState::Finished;
        Ok(Status::done())
    }

    fn generate_plan_stub(&self) -> Result<(RepartitionPlan, PartitionRuleDiff, ResourceDemand)> {
        let mut plan = RepartitionPlan::empty(self.data.task.table_id);
        let diff = PartitionRuleDiff::default();

        if !self.data.task.new_rule_payload.is_empty() {
            let group_id = Uuid::new_v4();
            plan.groups.push(PlanGroup::new(group_id));
        }

        let mut demand = ResourceDemand::default();
        demand.new_regions = plan.groups.len() as u32;
        plan.resource_demand = demand;

        Ok((plan, diff, demand))
    }

    async fn allocate_resources_stub(&self, _demand: ResourceDemand) -> Result<bool> {
        Ok(true)
    }

    fn spawn_group_stub_procedures(&self, groups: &[PlanGroupId]) -> Vec<ProcedureWithId> {
        groups
            .iter()
            .map(|group_id| {
                ProcedureWithId::with_random_id(Box::new(RepartitionGroupStubProcedure::new(
                    *group_id,
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
            RepartitionState::CollectSubprocedures => {
                self.on_collect_subprocedures(ctx).await
            }
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
    /// Validates request parameters and generates deterministic plan.
    Prepare,
    /// Coordinates with the resource provider (PaaS) for region allocation.
    AllocateResources,
    /// Submits work items to sub-procedures.
    DispatchSubprocedures,
    /// Collects and inspects sub-procedure results.
    CollectSubprocedures,
    /// Finalises metadata and emits summary.
    Finalize,
    /// Terminal state.
    Finished,
}

/// Task payload passed from the DDL entry point.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepartitionTask {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub table_id: TableId,
    /// Serialized representation of the new partition rule.
    ///
    /// The actual format will be determined later; currently we keep the raw
    /// payload so that the procedure can be wired end-to-end.
    pub new_rule_payload: Vec<u8>,
}

impl RepartitionTask {
    fn table_ref(&self) -> TableReference {
        TableReference {
            catalog: &self.catalog_name,
            schema: &self.schema_name,
            table: &self.table_name,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RepartitionSummary {
    succeeded_groups: Vec<PlanGroupId>,
    failed_groups: Vec<PlanGroupId>,
}

struct RepartitionGroupStubProcedure {
    group_id: PlanGroupId,
}

impl RepartitionGroupStubProcedure {
    fn new(group_id: PlanGroupId) -> Self {
        Self { group_id }
    }
}

#[async_trait::async_trait]
impl Procedure for RepartitionGroupStubProcedure {
    fn type_name(&self) -> &str {
        "metasrv-procedure::RepartitionGroupStub"
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        Ok(Status::done())
    }

    fn dump(&self) -> ProcedureResult<String> {
        Ok(self.group_id.to_string())
    }

    fn lock_key(&self) -> LockKey {
        LockKey::default()
    }
}
