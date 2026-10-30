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

pub(crate) mod cursor;
pub(crate) mod dispatch;
pub(crate) mod drain_datanode_end;
pub(crate) mod drain_datanode_start;
pub(crate) mod manager;
mod migration;
mod scan;
pub(crate) mod utils;

#[cfg(test)]
mod test_util;

use std::any::Any;
use std::fmt::Debug;
use std::time::Duration;

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

use crate::error::{self, Result};
use crate::metasrv::SelectorContext;
use crate::node_scheduling::NodeSchedulingManagerRef;
use crate::procedure::drain_datanode::drain_datanode_start::DrainDatanodeStart;
use crate::procedure::drain_datanode::manager::{
    DrainDatanodeProcedureGuard, DrainDatanodeProcedureTracker,
};
use crate::procedure::region_migration::manager::RegionMigrationManagerRef;
use crate::procedure::region_migration::utils::RegionMigrationTaskBatch;

/// The default number of leader regions migrated concurrently.
pub const DEFAULT_PARALLELISM: usize = 4;
/// The maximum number of leader regions migrated concurrently.
pub const MAX_PARALLELISM: usize = 32;
/// The default budget of the whole drain.
pub const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(20 * 60);
/// The maximum number of plan build attempts, including the first scan and failed reads.
pub const MAX_RESCAN_ROUNDS: u32 = 3;

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

/// One bounded window of the datanode drain scan.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TopicBatch {
    /// The leader regions in this window, sorted and deduplicated.
    pub leader_regions: Vec<RegionId>,
    /// The shared Kafka topic, or `None` for singleton local WAL migrations.
    pub topic: Option<String>,
}

/// The persistent data of [`DrainDatanodeProcedure`].
///
/// Only what cannot be derived from the cluster metadata is persisted: the plan and the
/// progress of a run are rebuilt from the authoritative table routes after a restart.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentContext {
    /// The datanode to drain.
    pub node_id: DatanodeId,
    /// The pinned destination of leader regions. Resolved by the selector when `None`.
    pub target_node_id: Option<DatanodeId>,
    /// The number of leader regions migrated concurrently.
    pub parallelism: usize,
    /// The error handling strategy.
    pub strategy: DrainDatanodeStrategy,
    /// The absolute deadline of the whole drain, in milliseconds since the Unix epoch.
    pub deadline_ms: i64,
}

/// A dispatched migration sub-procedure that is not settled yet.
#[derive(Debug, Clone)]
pub struct SubprocedureMeta {
    /// The regions migrated by the sub-procedure.
    pub region_ids: Vec<RegionId>,
    /// The procedure id of the sub-procedure.
    pub procedure_id: ProcedureId,
}

/// The in-memory data of [`DrainDatanodeProcedure`].
///
/// It is not persisted: a restart rebuilds the plan by rescanning the datanode, which is
/// bounded by [`MAX_RESCAN_ROUNDS`], and re-dispatches the regions that are still led by
/// the drained datanode and have no running migration. Recovered migrations own their
/// lifecycle and may finish after this drain procedure ends.
#[derive(Debug, Default)]
pub struct VolatileContext {
    /// The current topic batch, retained until all its regions are settled.
    ///
    /// [`TopicBatch::leader_regions`] is the remaining work: a region leaves it when
    /// its migration succeeded, when it was given up by [`DrainDatanodeStrategy::TryAll`],
    /// when the drained datanode stopped leading it, or when another migration owns it.
    pub(crate) topic_batch: Option<TopicBatch>,
    /// Paused metadata scan; dropped and rebuilt after a metadata read error.
    pub(crate) scan: Option<scan::RegionScan>,
    /// Number of followers seen in the current scan.
    pub follower_count: usize,
    /// Prepared migrations from the current topic batch, consumed by Dispatch.
    pub(crate) migrations: Vec<RegionMigrationTaskBatch>,
    /// The migration sub-procedures of the current batch that are not settled yet.
    pub inflight_subprocedures: Vec<SubprocedureMeta>,
    /// The number of regions confirmed to have left the source while collecting batch results.
    pub migrated: usize,
    /// The regions given up by [`DrainDatanodeStrategy::TryAll`] in this run.
    pub failed_regions: Vec<RegionId>,
    /// The number of full scan attempts in this run, including failed reads.
    pub rescan_round: u32,
}

/// The task of [`DrainDatanodeProcedure`].
#[derive(Debug, Clone)]
pub struct DrainDatanodeProcedureTask {
    /// The datanode to drain.
    pub node_id: DatanodeId,
    /// The pinned destination of leader regions. Resolved by the selector when `None`.
    pub target_node_id: Option<DatanodeId>,
    /// The number of leader regions migrated concurrently.
    pub parallelism: usize,
    /// The error handling strategy.
    pub strategy: DrainDatanodeStrategy,
    /// The budget of the whole drain. `None` uses [`DEFAULT_DRAIN_TIMEOUT`].
    pub timeout: Option<Duration>,
}

/// The context of [`DrainDatanodeProcedure`].
pub struct Context {
    /// The persistent data of the procedure.
    pub persistent_ctx: PersistentContext,
    /// The in-memory data of the procedure.
    pub volatile_ctx: VolatileContext,
    /// The table metadata manager.
    pub table_metadata_manager: TableMetadataManagerRef,
    /// The node scheduling manager, used to fence the drained datanode.
    pub node_scheduling_manager: NodeSchedulingManagerRef,
    /// The region migration manager, used to build migration sub-procedures.
    pub region_migration_manager: RegionMigrationManagerRef,
    /// The selector context, used to resolve the destination of a leader region.
    pub selector_ctx: SelectorContext,
}

impl Context {
    /// Builds the context of a run.
    ///
    /// The per-run state starts empty: the plan and the progress are rebuilt from the
    /// table routes when the procedure needs them.
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
    /// Starts a fresh scan of the authoritative table routes.
    ///
    /// Scans are bounded by [`MAX_RESCAN_ROUNDS`]: a datanode whose leader regions keep
    /// coming back must not be rescanned forever.
    pub(crate) fn start_scan(&mut self) -> Result<()> {
        let node_id = self.persistent_ctx.node_id;
        if self.volatile_ctx.rescan_round >= MAX_RESCAN_ROUNDS {
            return error::DrainDatanodeRescanExceededSnafu {
                node_id,
                max_rounds: MAX_RESCAN_ROUNDS,
            }
            .fail();
        }

        self.volatile_ctx.rescan_round += 1;
        self.volatile_ctx.scan = Some(scan::RegionScan::new(
            &self.table_metadata_manager,
            node_id,
            self.persistent_ctx.parallelism,
        ));
        self.volatile_ctx.follower_count = 0;
        self.volatile_ctx.topic_batch = None;

        Ok(())
    }

    /// Reads only the next bounded window. A read failure invalidates the scan so
    /// retry starts from authoritative metadata instead of losing buffered entries.
    pub(crate) async fn read_next_topic(&mut self) -> Result<()> {
        let Some(mut scan) = self.volatile_ctx.scan.take() else {
            return Ok(());
        };
        let next = scan
            .next_batch(
                &self.table_metadata_manager,
                self.persistent_ctx.node_id,
                &self.volatile_ctx.failed_regions,
                self.region_migration_manager.tracker(),
            )
            .await?;
        self.volatile_ctx.follower_count = scan.follower_count;
        self.volatile_ctx.topic_batch = next;
        self.volatile_ctx.scan = Some(scan);
        Ok(())
    }

    /// Removes `region_id` from the current topic batch. A later scan may add it back.
    pub(crate) fn forget_leader_region(&mut self, region_id: RegionId) {
        if let Some(plan) = self.volatile_ctx.topic_batch.as_mut() {
            plan.leader_regions.retain(|region| *region != region_id);
        }
    }
}

impl VolatileContext {
    /// Returns the leader regions still to migrate.
    pub(crate) fn leader_regions(&self) -> &[RegionId] {
        self.topic_batch
            .as_ref()
            .map_or(&[], |plan| plan.leader_regions.as_slice())
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
/// It fences the datanode, migrates every leader region off it through
/// [`RegionMigrationProcedure`](crate::procedure::region_migration::RegionMigrationProcedure)
/// sub-procedures, and rescans for regions that still need a new migration. Migrations
/// already registered in the tracker continue independently and may outlive this
/// procedure. Follower regions are counted but not removed yet.
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
                parallelism: persistent_ctx.parallelism,
                strategy: persistent_ctx.strategy,
                timeout: None,
            })
            .into_iter()
            .collect();

        let context = ctx_factory(persistent_ctx);
        Ok(Self {
            state,
            context,
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
    use std::time::Duration;

    use common_error::ext::ErrorExt;
    use common_error::status_code::StatusCode;
    use common_meta::state_store::KvStateStore;
    use common_procedure::store::ProcedureMessage;
    use common_procedure::store::state_store::StateStore;
    use common_procedure::{Procedure, ProcedureId, ProcedureState, Status};
    use futures::TryStreamExt;
    use store_api::storage::RegionId;

    use crate::error;
    use crate::key::NodeSchedulingRole;
    use crate::procedure::drain_datanode::drain_datanode_start::DrainDatanodeStart;
    use crate::procedure::drain_datanode::manager::DrainDatanodeProcedureTracker;
    use crate::procedure::drain_datanode::test_util::{
        TestEnv, follower_region, leader_region, new_persistent_context, run_until_done,
        spawn_mock_datanode, wait_for_terminal_state,
    };
    use crate::procedure::drain_datanode::{
        DEFAULT_PARALLELISM, DrainDatanodeProcedure, DrainDatanodeProcedureTask,
        DrainDatanodeStrategy, State,
    };
    use crate::procedure::region_migration::test_util::TestingEnv;
    use crate::service::mailbox::Channel;

    #[tokio::test]
    async fn test_drain_datanode_fences_and_completes_without_leaders() {
        let env = TestEnv::new().await;
        env.create_table(vec![follower_region(RegionId::new(1024, 1), 2, 1)])
            .await;

        let mut ctx = env.context(new_persistent_context(
            None,
            DrainDatanodeStrategy::FailFast,
        ));
        let procedure_ctx = TestingEnv::procedure_context();
        let mut state: Box<dyn State> = Box::new(DrainDatanodeStart);

        let status = run_until_done(&mut state, &mut ctx, &procedure_ctx)
            .await
            .unwrap();

        assert!(matches!(status, Status::Done { .. }));
        assert_eq!(0, ctx.volatile_ctx.migrated);
        assert_eq!(1, ctx.volatile_ctx.follower_count);
        assert!(
            !env.node_scheduling_manager
                .is_schedulable(NodeSchedulingRole::Datanode, 1)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_drain_datanode_dump_roundtrip() {
        let env = TestEnv::new().await;
        let persistent_ctx = new_persistent_context(Some(2), DrainDatanodeStrategy::TryAll);

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
        assert_eq!(DEFAULT_PARALLELISM, ctx.parallelism);
        assert_eq!(DrainDatanodeStrategy::TryAll, ctx.strategy);
        // The plan and the progress of a run are volatile, so they are not dumped.
        assert!(!json.contains("leader_regions"));
        assert!(!json.contains("rescan_round"));
    }

    #[tokio::test]
    async fn test_drain_datanode_default_strategy_continues_after_batch_failures() {
        let env = TestEnv::new().await;
        let regions: Vec<_> = (1..=5).map(|number| RegionId::new(1024, number)).collect();
        env.create_table(
            regions
                .iter()
                .copied()
                .map(|region_id| leader_region(region_id, 1))
                .collect(),
        )
        .await;
        // The pinned target is unschedulable, so every region is given up.
        env.node_scheduling_manager
            .mark_unschedulable(NodeSchedulingRole::Datanode, 3)
            .await
            .unwrap();

        let mut persistent_ctx = new_persistent_context(Some(3), DrainDatanodeStrategy::default());
        persistent_ctx.parallelism = 2;
        let mut ctx = env.context(persistent_ctx);
        let procedure_ctx = TestingEnv::procedure_context();
        let mut state: Box<dyn State> = Box::new(DrainDatanodeStart);

        let error = run_until_done(&mut state, &mut ctx, &procedure_ctx)
            .await
            .unwrap_err();

        let error::Error::DrainDatanodeAbort { failed_regions, .. } = error else {
            panic!("expected the drain procedure to abort");
        };
        assert_eq!(regions, failed_regions);
        assert_eq!(regions, ctx.volatile_ctx.failed_regions);
        assert_eq!(0, ctx.volatile_ctx.migrated);
    }

    #[tokio::test]
    async fn test_drain_datanode_recovery_obeys_deadline() {
        for expired in [true, false] {
            let mut env = TestEnv::new().await;
            let first_id = RegionId::new(1024, 1);
            let second_id = RegionId::new(1024, 2);
            env.create_table(vec![
                leader_region(first_id, 1),
                leader_region(second_id, 1),
            ])
            .await;
            env.mock_datanodes().await;
            let mut persistent_ctx =
                new_persistent_context(Some(2), DrainDatanodeStrategy::FailFast);
            if expired {
                persistent_ctx.deadline_ms = 0;
            }
            let mut procedure =
                DrainDatanodeProcedure::new(env.context(persistent_ctx.clone()), vec![]);
            assert!(
                procedure
                    .execute(&TestingEnv::procedure_context())
                    .await
                    .unwrap()
                    .need_persist()
            );
            let checkpoint = procedure.dump().unwrap();
            let json: serde_json::Value = serde_json::from_str(&checkpoint).unwrap();
            assert_eq!(
                serde_json::json!({
                    "persistent_ctx": {
                        "node_id": 1, "target_node_id": 2,
                        "parallelism": DEFAULT_PARALLELISM, "strategy": "FailFast",
                        "deadline_ms": persistent_ctx.deadline_ms,
                    },
                    "state": {"drain_datanode_state": "DrainDatanodeCursor"},
                }),
                json
            );
            drop(procedure);
            let recovered = DrainDatanodeProcedure::from_json(
                &checkpoint,
                &DrainDatanodeProcedureTracker::default(),
                |persistent_ctx| env.context(persistent_ctx),
            )
            .unwrap();
            assert!(recovered.context.volatile_ctx.topic_batch.is_none());
            drop(recovered);
            let id = ProcedureId::random();
            let message = common_procedure::store::ProcedureMessage {
                type_name: DrainDatanodeProcedure::TYPE_NAME.to_string(),
                data: checkpoint,
                parent_id: None,
                subprocedure_recovery: Default::default(),
                step: 0,
                error: None,
                context: Default::default(),
            };
            let store = common_meta::state_store::KvStateStore::new(env.env.kv_backend());
            common_procedure::store::state_store::StateStore::put(
                &store,
                &format!("procedure/{id}/0000000000.step"),
                serde_json::to_vec(&message).unwrap(),
            )
            .await
            .unwrap();
            let manager = env.drain_manager();
            manager.try_start().unwrap();
            env.procedure_manager().start().await.unwrap();
            let state = wait_for_terminal_state(env.procedure_manager(), id).await;
            if expired {
                let ProcedureState::Failed { error } = state else {
                    panic!("expected expired drain: {state:?}");
                };
                assert_eq!(StatusCode::Cancelled, error.status_code());
            } else {
                assert!(state.is_done(), "{state:?}");
            }
            let table_route = env
                .table_metadata_manager
                .table_route_manager()
                .table_route_storage()
                .get(1024)
                .await
                .unwrap()
                .unwrap();
            let routes = table_route.region_routes().unwrap();
            assert_eq!(2, routes.len());
            for route in routes {
                assert_eq!(
                    Some(if expired { 1 } else { 2 }),
                    route.leader_peer.as_ref().map(|peer| peer.id)
                );
            }
        }
    }

    #[tokio::test]
    async fn test_drain_datanode_recovered_migration_outlives_parent() {
        let mut env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;
        let procedure_ctx = TestingEnv::procedure_context();
        let mut parent = DrainDatanodeProcedure::new(
            env.context(new_persistent_context(
                Some(2),
                DrainDatanodeStrategy::FailFast,
            )),
            vec![],
        );
        assert!(parent.execute(&procedure_ctx).await.unwrap().need_persist());
        let parent_checkpoint = parent.dump().unwrap();
        let Status::Suspended {
            subprocedures: mut children,
            recovery,
            ..
        } = run_until_done(&mut parent.state, &mut parent.context, &procedure_ctx)
            .await
            .unwrap()
        else {
            panic!("expected a migration batch");
        };
        assert_eq!(1, children.len());
        let mut child = children.pop().unwrap();
        // Persist the first migration checkpoint, before it opens the candidate region.
        assert!(
            child
                .procedure
                .execute(&procedure_ctx)
                .await
                .unwrap()
                .need_persist()
        );
        let parent_id = ProcedureId::random();
        let child_id = child.id;
        let store = KvStateStore::new(env.env.kv_backend());
        for (id, message) in [
            (
                parent_id,
                ProcedureMessage {
                    type_name: parent.type_name().to_string(),
                    data: parent_checkpoint,
                    parent_id: None,
                    subprocedure_recovery: Default::default(),
                    step: 0,
                    error: None,
                    context: Default::default(),
                },
            ),
            (
                child_id,
                ProcedureMessage {
                    type_name: child.procedure.type_name().to_string(),
                    data: child.procedure.dump().unwrap(),
                    parent_id: Some(parent_id),
                    subprocedure_recovery: recovery,
                    step: 0,
                    error: None,
                    context: child.context.clone(),
                },
            ),
        ] {
            store
                .put(
                    &format!("procedure/{id}/0000000000.step"),
                    serde_json::to_vec(&message).unwrap(),
                )
                .await
                .unwrap();
        }
        drop(child);
        drop(parent);
        assert!(!env.region_migration_manager.tracker().contains(region_id));

        // Queue mailbox requests without replying until the recovered parent has ended.
        let mailbox_ctx = env.env.mailbox_context();
        let mut receivers = Vec::new();
        for node_id in [1, 2] {
            let (tx, rx) = tokio::sync::mpsc::channel(8);
            mailbox_ctx
                .insert_heartbeat_response_receiver(Channel::Datanode(node_id), tx)
                .await;
            receivers.push(rx);
        }
        let mailbox = mailbox_ctx.mailbox().clone();
        env.region_migration_manager.try_start().unwrap();
        let drain_manager = env.drain_manager();
        drain_manager.try_start().unwrap();
        env.procedure_manager().start().await.unwrap();
        assert!(
            wait_for_terminal_state(env.procedure_manager(), parent_id)
                .await
                .is_done()
        );
        assert!(env.region_migration_manager.tracker().contains(region_id));
        assert!(matches!(
            env.procedure_manager()
                .procedure_state(child_id)
                .await
                .unwrap(),
            Some(ProcedureState::Running)
        ));
        // No replacement migration was submitted by the recovering drain.
        assert_eq!(
            2,
            env.procedure_manager()
                .list_procedures()
                .await
                .unwrap()
                .len()
        );

        for rx in receivers {
            spawn_mock_datanode(mailbox.clone(), rx);
        }
        assert!(
            wait_for_terminal_state(env.procedure_manager(), child_id)
                .await
                .is_done()
        );
        assert!(
            !crate::procedure::drain_datanode::utils::leads_region(
                &env.table_metadata_manager,
                1,
                region_id,
            )
            .await
            .unwrap()
        );
        // Completion must release the recovered guard and clean its own checkpoints.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut records = store.walk_top_down("procedure/").await.unwrap();
                if records.try_next().await.unwrap().is_none()
                    && !env.region_migration_manager.tracker().contains(region_id)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("recovered migration did not clean up");
        env.procedure_manager().stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_drain_datanode_migrates_leader_region() {
        assert_drain_migrates_leader_regions(1).await;
    }

    #[tokio::test]
    async fn test_drain_datanode_migrates_multiple_batches() {
        assert_drain_migrates_leader_regions(3).await;
    }

    async fn assert_drain_migrates_leader_regions(region_count: u32) {
        let mut env = TestEnv::new().await;
        let regions: Vec<_> = (1..=region_count)
            .map(|number| RegionId::new(1024, number))
            .collect();
        env.create_table(
            regions
                .iter()
                .copied()
                .map(|region_id| leader_region(region_id, 1))
                .collect(),
        )
        .await;

        env.mock_datanodes().await;

        env.procedure_manager().start().await.unwrap();
        let manager = env.drain_manager();
        let procedure_id = manager
            .submit_procedure(
                common_procedure::ProcedureContext::default(),
                DrainDatanodeProcedureTask {
                    node_id: 1,
                    target_node_id: Some(2),
                    parallelism: 1,
                    strategy: DrainDatanodeStrategy::FailFast,
                    timeout: None,
                },
            )
            .await
            .unwrap();

        let state = wait_for_terminal_state(env.procedure_manager(), procedure_id).await;
        assert!(
            matches!(state, ProcedureState::Done { .. }),
            "the drain procedure did not complete successfully: {state:?}"
        );

        let table_route = env
            .table_metadata_manager
            .table_route_manager()
            .table_route_storage()
            .get(1024)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            region_count as usize,
            table_route.region_routes().unwrap().len()
        );
        assert!(
            !env.node_scheduling_manager
                .is_schedulable(NodeSchedulingRole::Datanode, 1)
                .await
                .unwrap()
        );
        for region_route in table_route.region_routes().unwrap() {
            assert_eq!(
                Some(2),
                region_route.leader_peer.as_ref().map(|peer| peer.id)
            );
        }
    }
}
