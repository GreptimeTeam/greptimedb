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

pub(crate) mod drain_datanode_end;
pub(crate) mod drain_datanode_start;
pub(crate) mod manager;
mod migration;
pub(crate) mod utils;

#[cfg(test)]
mod test_util;

use std::any::Any;
use std::fmt::Debug;

use common_meta::DatanodeId;
use common_meta::key::TableMetadataManagerRef;
use common_procedure::error::{FromJsonSnafu, ToJsonSnafu};
use common_procedure::{
    Context as ProcedureContext, Error as ProcedureError, LockKey, Procedure, ProcedureId,
    Result as ProcedureResult, Status,
};
use common_telemetry::{error, info};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::error::Result;
use crate::metasrv::SelectorContext;
use crate::node_scheduling::NodeSchedulingManagerRef;
use crate::procedure::drain_datanode::drain_datanode_start::DrainDatanodeStart;
use crate::procedure::drain_datanode::manager::{
    DrainDatanodeProcedureGuard, DrainDatanodeProcedureTracker,
};
use crate::procedure::region_migration::manager::RegionMigrationManagerRef;

/// The strategy of a drain run when a region cannot be migrated.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum DrainDatanodeStrategy {
    /// Aborts the procedure once every migration of the batch is settled and at least one
    /// of them failed.
    FailFast,
    /// Drains every remaining region and records the failed ones.
    #[default]
    TryAll,
}

/// The regions placed on a datanode, split by role.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DrainDatanodePlan {
    /// The leader regions still to migrate, sorted and deduplicated.
    pub leader_regions: Vec<RegionId>,
    /// The follower regions of the datanode, sorted and deduplicated.
    pub follower_regions: Vec<RegionId>,
}

/// The persistent data of [`DrainDatanodeProcedure`].
///
/// Only what cannot be derived from the cluster metadata is persisted: the plan is rebuilt
/// from the authoritative table routes after a restart.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentContext {
    /// The datanode to drain.
    pub node_id: DatanodeId,
    /// The pinned destination of leader regions.
    pub target_node_id: Option<DatanodeId>,
}

/// A dispatched migration sub-procedure that is not settled yet.
#[derive(Debug, Clone, Copy)]
pub struct SubprocedureMeta {
    /// The region migrated by the sub-procedure.
    pub region_id: RegionId,
    /// The procedure id of the sub-procedure.
    pub procedure_id: ProcedureId,
}

/// The in-memory data of [`DrainDatanodeProcedure`].
///
/// It is not persisted: a restarted procedure rebuilds the plan by rescanning the datanode.
#[derive(Debug, Clone, Default)]
pub struct VolatileContext {
    /// The drain plan, `None` until the first state builds it.
    pub plan: Option<DrainDatanodePlan>,
    /// The migration sub-procedures of the current batch that are not settled yet.
    pub inflight_subprocedures: Vec<SubprocedureMeta>,
    /// The number of regions confirmed to have left the source while collecting batch results.
    pub migrated: usize,
    /// The regions given up by [`DrainDatanodeStrategy::TryAll`] in this run.
    pub failed_regions: Vec<RegionId>,
}

/// The task of [`DrainDatanodeProcedure`].
#[derive(Debug, Clone)]
pub struct DrainDatanodeProcedureTask {
    /// The datanode to drain.
    pub node_id: DatanodeId,
    /// The pinned destination of leader regions.
    pub target_node_id: Option<DatanodeId>,
}

/// The context of [`DrainDatanodeProcedure`].
#[derive(Clone)]
pub struct Context {
    /// The persistent data of the procedure.
    pub persistent_ctx: PersistentContext,
    /// The in-memory data of the procedure.
    pub volatile_ctx: VolatileContext,
    /// The table metadata manager.
    pub table_metadata_manager: TableMetadataManagerRef,
    /// The node scheduling manager, used to fence the drained datanode.
    pub node_scheduling_manager: NodeSchedulingManagerRef,
    /// Builds region migration procedures.
    pub region_migration_manager: RegionMigrationManagerRef,
    /// Resolves destinations for region migrations.
    pub selector_ctx: SelectorContext,
}

impl Context {
    /// Builds the context of a run.
    ///
    /// The per-run state starts empty: the plan is rebuilt from the table routes when the
    /// procedure needs it.
    pub(crate) fn new(
        table_metadata_manager: TableMetadataManagerRef,
        node_scheduling_manager: NodeSchedulingManagerRef,
        region_migration_manager: RegionMigrationManagerRef,
        selector_ctx: SelectorContext,
        persistent_ctx: PersistentContext,
    ) -> Self {
        Self {
            persistent_ctx,
            volatile_ctx: VolatileContext::default(),
            table_metadata_manager,
            node_scheduling_manager,
            region_migration_manager,
            selector_ctx,
        }
    }
}

impl Context {
    /// Builds the drain plan when it is not available yet.
    ///
    /// The plan is derived from the authoritative table routes, so a restarted procedure
    /// rebuilds it instead of reading a persisted copy.
    pub(crate) async fn ensure_plan(&mut self) -> Result<()> {
        if self.volatile_ctx.plan.is_some() {
            return Ok(());
        }

        let node_id = self.persistent_ctx.node_id;
        let plan = utils::build_datanode_plan(&self.table_metadata_manager, node_id).await?;
        self.volatile_ctx.plan = Some(plan);

        Ok(())
    }
}

impl Context {
    /// Removes `region_id` from the current plan. A later scan may add it back.
    pub(crate) fn forget_leader_region(&mut self, region_id: RegionId) {
        if let Some(plan) = self.volatile_ctx.plan.as_mut() {
            plan.leader_regions.retain(|region| *region != region_id);
        }
    }
}

impl VolatileContext {
    /// Returns the leader regions still to migrate.
    pub(crate) fn leader_regions(&self) -> &[RegionId] {
        self.plan
            .as_ref()
            .map_or(&[], |plan| plan.leader_regions.as_slice())
    }

    /// Returns the follower regions collected by the plan.
    pub(crate) fn follower_regions(&self) -> &[RegionId] {
        self.plan
            .as_ref()
            .map_or(&[], |plan| plan.follower_regions.as_slice())
    }
}

/// The persistent data of [`DrainDatanodeProcedure`] as read by `from_json`.
#[derive(Debug, Deserialize)]
pub struct DrainDatanodeDataOwned {
    persistent_ctx: PersistentContext,
    state: Box<dyn State>,
}

/// The persistent data of [`DrainDatanodeProcedure`] as written by `dump`.
#[derive(Debug, Serialize)]
pub struct DrainDatanodeData<'a> {
    persistent_ctx: &'a PersistentContext,
    state: &'a dyn State,
}

/// A state of [`DrainDatanodeProcedure`].
#[async_trait::async_trait]
#[typetag::serde(tag = "drain_datanode_state")]
pub(crate) trait State: Sync + Send + Debug {
    /// Returns the short name of this state.
    fn name(&self) -> &'static str {
        let type_name = std::any::type_name::<Self>();
        type_name.split("::").last().unwrap_or(type_name)
    }

    /// Yields the next [`State`] and [`Status`].
    async fn next(
        &mut self,
        ctx: &mut Context,
        procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)>;

    /// Returns as [`Any`].
    fn as_any(&self) -> &dyn Any;
}

/// The procedure draining a datanode.
///
/// It fences the datanode and collects the leader and follower regions placed on it.
/// Migrating the leader regions off the datanode lands in a follow-up.
pub(crate) struct DrainDatanodeProcedure {
    state: Box<dyn State>,
    context: Context,
    /// Keeps the drained datanode in the running set of the manager until the procedure ends,
    /// so a second drain of the datanode is rejected at submission instead of being queued.
    _guards: Vec<DrainDatanodeProcedureGuard>,
}

impl DrainDatanodeProcedure {
    const TYPE_NAME: &'static str = "metasrv-procedure::DrainDatanode";

    /// Returns a new [`DrainDatanodeProcedure`] in its initial state.
    pub fn new(context: Context, guards: Vec<DrainDatanodeProcedureGuard>) -> Self {
        Self {
            state: Box::new(DrainDatanodeStart),
            context,
            _guards: guards,
        }
    }

    /// Returns a new [`DrainDatanodeProcedure`] from the given serialized data.
    ///
    /// The procedure takes its place back in `tracker`, so a reloaded drain still rejects a
    /// second drain of the same datanode.
    pub fn from_json<F>(
        json: &str,
        tracker: &DrainDatanodeProcedureTracker,
        ctx_factory: F,
    ) -> ProcedureResult<Self>
    where
        F: FnOnce(PersistentContext) -> Context,
    {
        let DrainDatanodeDataOwned {
            persistent_ctx,
            state,
        } = serde_json::from_str(json).context(FromJsonSnafu)?;
        let guards = tracker
            .insert_running_procedure(&DrainDatanodeProcedureTask {
                node_id: persistent_ctx.node_id,
                target_node_id: persistent_ctx.target_node_id,
            })
            .into_iter()
            .collect();

        Ok(Self {
            state,
            context: ctx_factory(persistent_ctx),
            _guards: guards,
        })
    }
}

#[async_trait::async_trait]
impl Procedure for DrainDatanodeProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    #[tracing::instrument(skip_all, fields(
        state = %self.state.name(),
        node_id = %self.context.persistent_ctx.node_id
    ))]
    async fn execute(&mut self, ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state_name = self.state.name();
        info!(
            "Drain datanode procedure executing state: {}, node_id: {}",
            state_name, self.context.persistent_ctx.node_id
        );
        match self.state.next(&mut self.context, ctx).await {
            Ok((next, status)) => {
                self.state = next;
                Ok(status)
            }
            Err(e) => {
                if e.is_retryable() {
                    Err(ProcedureError::retry_later(e))
                } else {
                    error!(
                        e;
                        "Drain datanode procedure failed, node_id: {}",
                        self.context.persistent_ctx.node_id
                    );
                    Err(ProcedureError::external(e))
                }
            }
        }
    }

    fn dump(&self) -> ProcedureResult<String> {
        let data = DrainDatanodeData {
            persistent_ctx: &self.context.persistent_ctx,
            state: self.state.as_ref(),
        };
        serde_json::to_string(&data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        LockKey::single_exclusive(format!(
            "__drain_datanode/{}",
            self.context.persistent_ctx.node_id
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::NodeSchedulingRole;
    use crate::procedure::drain_datanode::test_util::{
        TestEnv, follower_region, leader_region, new_persistent_context, run_until_done,
    };
    use crate::procedure::region_migration::test_util::TestingEnv;

    #[tokio::test]
    async fn test_drain_datanode_fences_and_collects_the_plan() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        let follower_region_id = RegionId::new(1024, 2);
        env.create_table(vec![
            follower_region(follower_region_id, 2, 1),
            leader_region(region_id, 1),
        ])
        .await;

        let mut ctx = env.context(new_persistent_context(Some(2)));
        let procedure_ctx = TestingEnv::procedure_context();
        let mut state: Box<dyn State> = Box::new(DrainDatanodeStart);

        let status = run_until_done(&mut state, &mut ctx, &procedure_ctx)
            .await
            .unwrap();

        assert!(matches!(status, Status::Done { .. }));
        assert_eq!(vec![region_id], ctx.volatile_ctx.leader_regions());
        assert_eq!(
            vec![follower_region_id],
            ctx.volatile_ctx.follower_regions()
        );
        assert!(
            !ctx.node_scheduling_manager
                .is_schedulable(NodeSchedulingRole::Datanode, 1)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_drain_datanode_dump_roundtrip() {
        let env = TestEnv::new().await;
        let persistent_ctx = new_persistent_context(Some(2));

        let procedure = DrainDatanodeProcedure::new(env.context(persistent_ctx), vec![]);
        let json = procedure.dump().unwrap();
        let tracker = DrainDatanodeProcedureTracker::default();
        let reloaded = DrainDatanodeProcedure::from_json(&json, &tracker, |persistent_ctx| {
            env.context(persistent_ctx)
        })
        .unwrap();

        assert_eq!("DrainDatanodeStart", reloaded.state.name());
        let ctx = &reloaded.context.persistent_ctx;
        assert_eq!(1, ctx.node_id);
        assert_eq!(Some(2), ctx.target_node_id);
        // The plan is volatile, so it is not dumped.
        assert!(!json.contains("leader_regions"));
    }
}
