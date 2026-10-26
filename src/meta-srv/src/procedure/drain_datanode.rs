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

pub(crate) mod check_drained;
pub(crate) mod dispatch;
pub(crate) mod drain_datanode_end;
pub(crate) mod drain_datanode_start;
pub(crate) mod manager;
pub(crate) mod utils;

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

use crate::error::Result;
use crate::metasrv::SelectorContext;
use crate::node_scheduling::NodeSchedulingManagerRef;
use crate::procedure::drain_datanode::drain_datanode_start::DrainDatanodeStart;
use crate::procedure::region_migration::manager::RegionMigrationManagerRef;

/// The default number of leader regions migrated concurrently.
pub const DEFAULT_PARALLELISM: usize = 4;
/// The maximum number of leader regions migrated concurrently.
pub const MAX_PARALLELISM: usize = 32;
/// The default budget of the whole drain.
pub const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(20 * 60);
/// The maximum number of times the procedure rescans the datanode before giving up.
pub const MAX_RESCAN_ROUNDS: u32 = 3;

/// The strategy of a drain run when a region cannot be migrated.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum DrainDatanodeStrategy {
    /// Aborts the procedure once every migration of the batch is settled and at least one
    /// of them failed.
    #[default]
    FailFast,
    /// Drains every remaining region and records the failed ones.
    TryAll,
}

/// The regions placed on a datanode, split by role.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct DrainDatanodePlan {
    /// The sorted and deduplicated leader regions.
    pub leader_regions: Vec<RegionId>,
    /// The number of follower occurrences of the datanode.
    pub follower_region_count: usize,
}

/// The persistent data of [`DrainDatanodeProcedure`].
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
    /// The leader regions on `node_id` that have not been migrated successfully yet.
    ///
    /// A region leaves this list after its migration sub-procedure succeeded, after it was
    /// given up by [`DrainDatanodeStrategy::TryAll`], or after `node_id` stopped leading it.
    ///
    /// Sub-procedures are not recorded here: they live in the volatile context, so a
    /// metasrv restart resumes from the last settled batch instead of waiting for
    /// sub-procedures that will never come back.
    pub leader_regions: Vec<RegionId>,
    /// The number of regions migrated successfully.
    pub migrated: usize,
    /// The regions given up by [`DrainDatanodeStrategy::TryAll`].
    pub failed_regions: Vec<RegionId>,
    /// The number of follower occurrences on `node_id`, counted when the plan is built.
    pub follower_region_count: usize,
    /// The number of rescans performed so far.
    pub rescan_round: u32,
    /// The absolute deadline of the whole drain, in milliseconds since the Unix epoch.
    pub deadline_ms: i64,
}

/// A dispatched migration sub-procedure that is not settled yet.
#[derive(Debug, Clone, Copy)]
pub struct InflightMigration {
    /// The region migrated by the sub-procedure.
    pub region_id: RegionId,
    /// The procedure id of the sub-procedure.
    pub procedure_id: ProcedureId,
}

/// The in-memory data of [`DrainDatanodeProcedure`].
///
/// It is lost on restart: the procedure then resumes from the last persisted batch and
/// re-dispatches the regions that are still led by the drained datanode.
#[derive(Debug, Clone, Default)]
pub struct VolatileContext {
    /// The migration sub-procedures of the current batch that are not settled yet.
    pub inflight: Vec<InflightMigration>,
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
    /// The region migration manager, used to build migration sub-procedures.
    pub region_migration_manager: RegionMigrationManagerRef,
    /// The selector context, used to resolve the destination of a leader region.
    pub selector_ctx: SelectorContext,
    /// The operation budget of each migration sub-procedure.
    pub migration_timeout: Duration,
}

/// Creates the [`Context`] of a [`DrainDatanodeProcedure`] for a given persistent state.
#[derive(Clone)]
pub struct ContextFactory {
    pub table_metadata_manager: TableMetadataManagerRef,
    pub node_scheduling_manager: NodeSchedulingManagerRef,
    pub region_migration_manager: RegionMigrationManagerRef,
    pub selector_ctx: SelectorContext,
    pub migration_timeout: Duration,
}

impl ContextFactory {
    /// Builds the [`Context`] of a procedure with `persistent_ctx`.
    pub fn build(&self, persistent_ctx: PersistentContext) -> Context {
        Context {
            persistent_ctx,
            volatile_ctx: VolatileContext::default(),
            table_metadata_manager: self.table_metadata_manager.clone(),
            node_scheduling_manager: self.node_scheduling_manager.clone(),
            region_migration_manager: self.region_migration_manager.clone(),
            selector_ctx: self.selector_ctx.clone(),
            migration_timeout: self.migration_timeout,
        }
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
/// sub-procedures, and rescans the datanode until no leader region is left. Follower
/// occurrences are only counted; removing them is not part of this procedure yet.
pub(crate) struct DrainDatanodeProcedure {
    state: Box<dyn State>,
    context: Context,
}

impl DrainDatanodeProcedure {
    const TYPE_NAME: &'static str = "metasrv-procedure::DrainDatanode";

    /// Returns a new [`DrainDatanodeProcedure`] in its initial state.
    pub fn new(context: Context) -> Self {
        Self {
            state: Box::new(DrainDatanodeStart),
            context,
        }
    }

    /// Returns a new [`DrainDatanodeProcedure`] from the given serialized data.
    pub fn from_json<F>(json: &str, ctx_factory: F) -> ProcedureResult<Self>
    where
        F: FnOnce(PersistentContext) -> Context,
    {
        let DrainDatanodeDataOwned {
            persistent_ctx,
            state,
        } = serde_json::from_str(json).context(FromJsonSnafu)?;

        Ok(Self {
            state,
            context: ctx_factory(persistent_ctx),
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
    use std::collections::HashMap;
    use std::sync::Arc;

    use api::v1::meta::MailboxMessage;
    use api::v1::meta::mailbox_message::Payload;
    use common_error::mock::MockError;
    use common_error::status_code::StatusCode;
    use common_meta::affinity::NoopAffinity;
    use common_meta::instruction::{
        DowngradeRegionReply, DowngradeRegionsReply, FlushRegionReply, Instruction,
        InstructionReply, SimpleReply, UpgradeRegionReply, UpgradeRegionsReply,
    };
    use common_meta::key::TableMetadataManagerRef;
    use common_meta::key::table_route::TableRouteValue;
    use common_meta::key::test_utils::new_test_table_info;
    use common_meta::peer::Peer;
    use common_meta::rpc::router::{Region, RegionRoute};
    use common_procedure::local::DynamicKeyLockGuard;
    use common_procedure::{
        Context as ExecContext, ContextProvider, Error as ProcedureError, PoisonKey,
        ProcedureManagerRef, ProcedureState, StringKey,
    };
    use common_procedure_test::MockContextProvider;
    use common_time::util::current_time_millis;
    use tokio::sync::watch;

    use super::*;
    use crate::error;
    use crate::key::NodeSchedulingRole;
    use crate::metasrv::SelectorContext;
    use crate::procedure::drain_datanode::check_drained::CheckDrained;
    use crate::procedure::drain_datanode::dispatch::Dispatch;
    use crate::procedure::drain_datanode::drain_datanode_end::DrainDatanodeEnd;
    use crate::procedure::drain_datanode::manager::DrainDatanodeManager;
    use crate::procedure::region_migration::manager::RegionMigrationManager;
    use crate::procedure::region_migration::test_util::TestingEnv;
    use crate::procedure::test_util::{MailboxContext, MockHeartbeatReceiver};
    use crate::service::mailbox::{Channel, MailboxRef};
    use crate::test_util::{create_meta_peer_client, new_node_scheduling_discovery, put_datanodes};

    fn new_persistent_context(
        target_node_id: Option<DatanodeId>,
        strategy: DrainDatanodeStrategy,
    ) -> PersistentContext {
        PersistentContext {
            node_id: 1,
            target_node_id,
            parallelism: DEFAULT_PARALLELISM,
            strategy,
            leader_regions: vec![],
            migrated: 0,
            failed_regions: vec![],
            follower_region_count: 0,
            rescan_round: 0,
            deadline_ms: current_time_millis() + 60 * 1000,
        }
    }

    fn leader_region(region_id: RegionId, leader_id: DatanodeId) -> RegionRoute {
        RegionRoute {
            region: Region::new_test(region_id),
            leader_peer: Some(Peer::empty(leader_id)),
            follower_peers: vec![],
            ..Default::default()
        }
    }

    fn follower_region(
        region_id: RegionId,
        leader_id: DatanodeId,
        follower_id: DatanodeId,
    ) -> RegionRoute {
        RegionRoute {
            region: Region::new_test(region_id),
            leader_peer: Some(Peer::empty(leader_id)),
            follower_peers: vec![Peer::empty(follower_id)],
            ..Default::default()
        }
    }

    struct TestEnv {
        env: TestingEnv,
        context_factory: ContextFactory,
        node_scheduling_manager: NodeSchedulingManagerRef,
    }

    impl TestEnv {
        async fn new() -> Self {
            let env = TestingEnv::new();
            // Shares one backend between the scheduling manager and the peer discovery, so
            // the selector observes the scheduling marks written by the procedure.
            let meta_peer_client = create_meta_peer_client();
            put_datanodes(
                &meta_peer_client,
                vec![
                    Peer {
                        id: 1,
                        addr: "dn1:3001".to_string(),
                    },
                    Peer {
                        id: 2,
                        addr: "dn2:3001".to_string(),
                    },
                    Peer {
                        id: 3,
                        addr: "dn3:3001".to_string(),
                    },
                ],
            )
            .await;
            let (node_scheduling_manager, discovery) =
                new_node_scheduling_discovery(&meta_peer_client);
            let selector_ctx = SelectorContext {
                affinity_fallback: Default::default(),
                affinity: Arc::new(NoopAffinity),
                peer_discovery: Arc::new(discovery),
            };
            let region_migration_manager = Arc::new(RegionMigrationManager::new(
                env.procedure_manager().clone(),
                env.context_factory(),
            ));
            let context_factory = ContextFactory {
                table_metadata_manager: env.table_metadata_manager().clone(),
                node_scheduling_manager: node_scheduling_manager.clone(),
                region_migration_manager,
                selector_ctx,
                migration_timeout: Duration::from_secs(10),
            };

            Self {
                env,
                context_factory,
                node_scheduling_manager,
            }
        }

        fn context(&self, persistent_ctx: PersistentContext) -> Context {
            self.context_factory.build(persistent_ctx)
        }

        async fn create_table(&self, region_routes: Vec<RegionRoute>) {
            create_table(&self.context_factory.table_metadata_manager, region_routes).await;
        }

        fn procedure_manager(&self) -> &ProcedureManagerRef {
            self.env.procedure_manager()
        }

        fn mailbox_context(&mut self) -> &mut MailboxContext {
            self.env.mailbox_context()
        }

        fn mailbox(&mut self) -> MailboxRef {
            self.env.mailbox_context().mailbox().clone()
        }
    }

    /// Spawns a mock datanode that answers every mailbox message of a migration.
    fn spawn_mock_datanode(mailbox: MailboxRef, mut rx: MockHeartbeatReceiver) {
        common_runtime::spawn_global(async move {
            while let Some(Ok(response)) = rx.recv().await {
                let message = response.mailbox_message.unwrap();
                if let Some(reply) = mock_instruction_reply(&message) {
                    mailbox.on_recv(message.id, Ok(reply)).await.unwrap();
                }
            }
        });
    }

    /// Returns the reply of `message`, or `None` if the instruction needs no reply.
    fn mock_instruction_reply(message: &MailboxMessage) -> Option<MailboxMessage> {
        let Some(Payload::Json(payload)) = message.payload.as_ref() else {
            panic!("unexpected mailbox payload of message {}", message.id);
        };
        let instruction: Instruction = serde_json::from_str(payload).unwrap();
        let reply = match instruction {
            Instruction::OpenRegions(_) => InstructionReply::OpenRegions(SimpleReply {
                result: true,
                error: None,
            }),
            Instruction::CloseRegions(_) => InstructionReply::CloseRegions(SimpleReply {
                result: true,
                error: None,
            }),
            Instruction::FlushRegions(flush) => {
                InstructionReply::FlushRegions(FlushRegionReply::from_results(
                    flush
                        .region_ids
                        .into_iter()
                        .map(|region_id| (region_id, Ok(())))
                        .collect(),
                ))
            }
            Instruction::DowngradeRegions(regions) => {
                InstructionReply::DowngradeRegions(DowngradeRegionsReply::new(
                    regions
                        .into_iter()
                        .map(|region| DowngradeRegionReply {
                            region_id: region.region_id,
                            last_entry_id: Some(1),
                            metadata_last_entry_id: Some(1),
                            exists: true,
                            error: None,
                        })
                        .collect(),
                ))
            }
            Instruction::UpgradeRegions(regions) => {
                InstructionReply::UpgradeRegions(UpgradeRegionsReply::new(
                    regions
                        .into_iter()
                        .map(|region| UpgradeRegionReply {
                            region_id: region.region_id,
                            ready: true,
                            exists: true,
                            error: None,
                        })
                        .collect(),
                ))
            }
            // One-way instruction: the meta server does not await a reply.
            Instruction::InvalidateCaches(_) => return None,
            other => panic!("unexpected instruction: {other:?}"),
        };

        Some(MailboxMessage {
            id: message.id,
            subject: "mock".to_string(),
            from: "datanode".to_string(),
            to: "meta".to_string(),
            timestamp_millis: current_time_millis(),
            payload: Some(Payload::Json(serde_json::to_string(&reply).unwrap())),
            header: None,
        })
    }

    async fn wait_for_terminal_state(
        procedure_manager: &ProcedureManagerRef,
        procedure_id: ProcedureId,
    ) -> ProcedureState {
        for _ in 0..500 {
            if let Some(state) = procedure_manager
                .procedure_state(procedure_id)
                .await
                .unwrap()
                && matches!(
                    state,
                    ProcedureState::Done { .. }
                        | ProcedureState::Failed { .. }
                        | ProcedureState::Poisoned { .. }
                )
            {
                return state;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        panic!("the drain procedure did not reach a terminal state in 10 seconds");
    }

    async fn create_table(
        table_metadata_manager: &TableMetadataManagerRef,
        region_routes: Vec<RegionRoute>,
    ) {
        table_metadata_manager
            .create_table_metadata(
                new_test_table_info(1024),
                TableRouteValue::physical(region_routes),
                HashMap::default(),
            )
            .await
            .unwrap();
    }

    async fn run_until_done(
        state: &mut Box<dyn State>,
        ctx: &mut Context,
        procedure_ctx: &ExecContext,
    ) -> Result<Status> {
        for _ in 0..50 {
            let (next, status) = state.next(ctx, procedure_ctx).await?;
            *state = next;
            if !matches!(status, Status::Executing { .. }) {
                return Ok(status);
            }
        }

        panic!("the drain procedure did not finish in 50 steps");
    }

    fn done_output(status: Status) -> serde_json::Value {
        let Status::Done { output } = status else {
            panic!("expected the drain procedure to be done");
        };
        let output = output.expect("the drain procedure should produce an output");
        output.downcast_ref::<serde_json::Value>().unwrap().clone()
    }

    fn exec_ctx_with(provider: Arc<dyn ContextProvider>) -> ExecContext {
        ExecContext {
            procedure_id: ProcedureId::random(),
            provider,
            event_context: None,
        }
    }

    #[tokio::test]
    async fn test_build_datanode_plan() {
        let env = TestEnv::new().await;
        let first = RegionId::new(1024, 1);
        let second = RegionId::new(1024, 2);
        env.create_table(vec![leader_region(first, 1), follower_region(second, 2, 1)])
            .await;

        let plan = utils::build_datanode_plan(&env.context_factory.table_metadata_manager, 1)
            .await
            .unwrap();

        assert_eq!(vec![first], plan.leader_regions);
        assert_eq!(1, plan.follower_region_count);
    }

    #[tokio::test]
    async fn test_build_datanode_plan_of_empty_datanode() {
        let env = TestEnv::new().await;

        let plan = utils::build_datanode_plan(&env.context_factory.table_metadata_manager, 1)
            .await
            .unwrap();

        assert!(plan.leader_regions.is_empty());
        assert_eq!(0, plan.follower_region_count);
    }

    #[tokio::test]
    async fn test_build_datanode_plan_reads_the_table_route() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        // The datanode is listed as a follower of the region, so it is not a leader even
        // though the table index of the datanode contains the table.
        env.create_table(vec![follower_region(region_id, 2, 1)])
            .await;

        let plan = utils::build_datanode_plan(&env.context_factory.table_metadata_manager, 1)
            .await
            .unwrap();

        assert!(plan.leader_regions.is_empty());
        assert_eq!(1, plan.follower_region_count);
    }

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
        let output = done_output(status);

        assert_eq!(0, output["migrated"]);
        assert_eq!(1, output["follower_region_count"]);
        assert_eq!(true, output["drained"]);
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
        let region_id = RegionId::new(1024, 1);
        let mut persistent_ctx = new_persistent_context(Some(2), DrainDatanodeStrategy::TryAll);
        persistent_ctx.leader_regions = vec![region_id];
        persistent_ctx.follower_region_count = 3;
        persistent_ctx.rescan_round = 1;

        let procedure = DrainDatanodeProcedure::new(env.context_factory.build(persistent_ctx));
        let json = procedure.dump().unwrap();
        let reloaded =
            DrainDatanodeProcedure::from_json(&json, |ctx| env.context_factory.build(ctx)).unwrap();

        assert_eq!("DrainDatanodeStart", reloaded.state.name());
        let ctx = &reloaded.context.persistent_ctx;
        assert_eq!(1, ctx.node_id);
        assert_eq!(Some(2), ctx.target_node_id);
        assert_eq!(vec![region_id], ctx.leader_regions);
        assert_eq!(3, ctx.follower_region_count);
        assert_eq!(1, ctx.rescan_round);
        assert_eq!(DrainDatanodeStrategy::TryAll, ctx.strategy);
    }

    #[tokio::test]
    async fn test_submit_procedure_rejects_invalid_task() {
        let env = TestEnv::new().await;
        let manager =
            DrainDatanodeManager::new(env.procedure_manager().clone(), env.context_factory.clone());

        // The pinned target must differ from the drained datanode.
        let error = manager
            .submit_procedure(
                common_procedure::ProcedureContext::default(),
                DrainDatanodeProcedureTask {
                    node_id: 1,
                    target_node_id: Some(1),
                    parallelism: DEFAULT_PARALLELISM,
                    strategy: DrainDatanodeStrategy::FailFast,
                    timeout: None,
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(error, error::Error::InvalidArguments { .. }));

        // The parallelism must be in range.
        let error = manager
            .submit_procedure(
                common_procedure::ProcedureContext::default(),
                DrainDatanodeProcedureTask {
                    node_id: 1,
                    target_node_id: Some(2),
                    parallelism: 0,
                    strategy: DrainDatanodeStrategy::FailFast,
                    timeout: None,
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(error, error::Error::InvalidArguments { .. }));

        // The timeout must be greater than zero.
        let error = manager
            .submit_procedure(
                common_procedure::ProcedureContext::default(),
                DrainDatanodeProcedureTask {
                    node_id: 1,
                    target_node_id: Some(2),
                    parallelism: DEFAULT_PARALLELISM,
                    strategy: DrainDatanodeStrategy::FailFast,
                    timeout: Some(Duration::ZERO),
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(error, error::Error::InvalidArguments { .. }));
    }

    #[tokio::test]
    async fn test_select_migration_target() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);

        let mut ctx = env.context(new_persistent_context(
            None,
            DrainDatanodeStrategy::FailFast,
        ));
        let peer = utils::select_migration_target(&ctx, region_id, &[])
            .await
            .unwrap();
        assert_eq!(2, peer.id);

        // The existing followers and the drained datanode are excluded.
        let peer = utils::select_migration_target(&ctx, region_id, &[Peer::empty(2)])
            .await
            .unwrap();
        assert_eq!(3, peer.id);
        assert_eq!("dn3:3001", peer.addr);

        // A pinned target is resolved to the active peer, so it carries its address.
        ctx.persistent_ctx.target_node_id = Some(2);
        let peer = utils::select_migration_target(&ctx, region_id, &[])
            .await
            .unwrap();
        assert_eq!(2, peer.id);
        assert_eq!("dn2:3001", peer.addr);

        // A target that is already a follower, the drained datanode itself or an
        // unschedulable datanode is rejected.
        ctx.persistent_ctx.target_node_id = Some(2);
        let error = utils::select_migration_target(&ctx, region_id, &[Peer::empty(2)])
            .await
            .unwrap_err();
        assert!(matches!(error, error::Error::DrainDatanodeAbort { .. }));

        ctx.persistent_ctx.target_node_id = Some(1);
        let error = utils::select_migration_target(&ctx, region_id, &[])
            .await
            .unwrap_err();
        assert!(matches!(error, error::Error::DrainDatanodeAbort { .. }));

        env.node_scheduling_manager
            .mark_unschedulable(NodeSchedulingRole::Datanode, 3)
            .await
            .unwrap();
        ctx.persistent_ctx.target_node_id = Some(3);
        let error = utils::select_migration_target(&ctx, region_id, &[])
            .await
            .unwrap_err();
        assert!(matches!(error, error::Error::DrainDatanodeAbort { .. }));
    }

    #[tokio::test]
    async fn test_drain_datanode_dispatches_migration() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut ctx = env.context(new_persistent_context(
            Some(2),
            DrainDatanodeStrategy::FailFast,
        ));
        let procedure_ctx = TestingEnv::procedure_context();

        let (mut next, _) = DrainDatanodeStart
            .next(&mut ctx, &procedure_ctx)
            .await
            .unwrap();
        let (next, status) = next.next(&mut ctx, &procedure_ctx).await.unwrap();

        let Status::Suspended {
            subprocedures,
            persist,
        } = status
        else {
            panic!("expected the dispatch state to suspend the procedure");
        };
        // The suspend must not be persisted, so a restart re-dispatches the batch.
        assert!(!persist);
        assert_eq!(1, subprocedures.len());
        assert_eq!(1, ctx.volatile_ctx.inflight.len());
        assert_eq!(region_id, ctx.volatile_ctx.inflight[0].region_id);
        // The region is only removed after its migration is settled.
        assert_eq!(vec![region_id], ctx.persistent_ctx.leader_regions);
        assert!(next.as_any().downcast_ref::<Dispatch>().is_some());
    }

    #[tokio::test]
    async fn test_drain_datanode_dispatch_skips_region_moved_away() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        // The plan lists the region, but the table route says another datanode leads it.
        env.create_table(vec![leader_region(region_id, 2)]).await;

        let mut persistent_ctx = new_persistent_context(Some(3), DrainDatanodeStrategy::FailFast);
        persistent_ctx.leader_regions = vec![region_id];
        let mut ctx = env.context(persistent_ctx);
        let procedure_ctx = TestingEnv::procedure_context();

        let (next, status) = Dispatch.next(&mut ctx, &procedure_ctx).await.unwrap();

        assert!(matches!(status, Status::Executing { .. }));
        assert!(next.as_any().downcast_ref::<Dispatch>().is_some());
        assert!(ctx.persistent_ctx.leader_regions.is_empty());
        assert!(ctx.volatile_ctx.inflight.is_empty());
    }

    #[tokio::test]
    async fn test_drain_datanode_fail_fast_on_unschedulable_target() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;
        env.node_scheduling_manager
            .mark_unschedulable(NodeSchedulingRole::Datanode, 3)
            .await
            .unwrap();

        let mut ctx = env.context(new_persistent_context(
            Some(3),
            DrainDatanodeStrategy::FailFast,
        ));
        let procedure_ctx = TestingEnv::procedure_context();

        let (mut state, _) = DrainDatanodeStart
            .next(&mut ctx, &procedure_ctx)
            .await
            .unwrap();
        let error = state.next(&mut ctx, &procedure_ctx).await.unwrap_err();

        assert!(matches!(error, error::Error::DrainDatanodeAbort { .. }));
        assert_eq!(vec![region_id], ctx.persistent_ctx.leader_regions);
    }

    #[tokio::test]
    async fn test_drain_datanode_try_all_completes_with_failed_region() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;
        env.node_scheduling_manager
            .mark_unschedulable(NodeSchedulingRole::Datanode, 3)
            .await
            .unwrap();

        let mut ctx = env.context(new_persistent_context(
            Some(3),
            DrainDatanodeStrategy::TryAll,
        ));
        let procedure_ctx = TestingEnv::procedure_context();
        let mut state: Box<dyn State> = Box::new(DrainDatanodeStart);

        let status = run_until_done(&mut state, &mut ctx, &procedure_ctx)
            .await
            .unwrap();
        let output = done_output(status);

        assert_eq!(vec![region_id], ctx.persistent_ctx.failed_regions);
        assert_eq!(0, output["migrated"]);
        assert_eq!(false, output["drained"]);
        let failed_regions: Vec<RegionId> =
            serde_json::from_value(output["failed_regions"].clone()).unwrap();
        assert_eq!(vec![region_id], failed_regions);
    }

    #[tokio::test]
    async fn test_drain_datanode_try_all_drains_beyond_one_batch() {
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

        let mut persistent_ctx = new_persistent_context(Some(3), DrainDatanodeStrategy::TryAll);
        persistent_ctx.parallelism = 2;
        let mut ctx = env.context(persistent_ctx);
        let procedure_ctx = TestingEnv::procedure_context();
        let mut state: Box<dyn State> = Box::new(DrainDatanodeStart);

        let status = run_until_done(&mut state, &mut ctx, &procedure_ctx)
            .await
            .unwrap();
        let output = done_output(status);

        assert_eq!(regions, ctx.persistent_ctx.failed_regions);
        assert_eq!(0, output["migrated"]);
        assert_eq!(0, ctx.persistent_ctx.rescan_round);
    }

    #[tokio::test]
    async fn test_drain_datanode_settles_migrated_batch() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut persistent_ctx = new_persistent_context(Some(2), DrainDatanodeStrategy::FailFast);
        persistent_ctx.leader_regions = vec![region_id];
        let mut ctx = env.context(persistent_ctx);
        ctx.volatile_ctx.inflight = vec![InflightMigration {
            region_id,
            procedure_id: ProcedureId::random(),
        }];
        let procedure_ctx = exec_ctx_with(Arc::new(ChildProvider::done()));

        let (mut next, status) = Dispatch.next(&mut ctx, &procedure_ctx).await.unwrap();

        assert!(matches!(status, Status::Executing { .. }));
        assert!(next.as_any().downcast_ref::<Dispatch>().is_some());
        assert_eq!(1, ctx.persistent_ctx.migrated);
        assert!(ctx.persistent_ctx.leader_regions.is_empty());
        assert!(ctx.volatile_ctx.inflight.is_empty());
        // The settled bookkeeping is persisted by the next step.
        let (next, _) = next.next(&mut ctx, &procedure_ctx).await.unwrap();
        assert!(next.as_any().downcast_ref::<CheckDrained>().is_some());
    }

    #[tokio::test]
    async fn test_drain_datanode_settle_failed_child_fail_fast() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut persistent_ctx = new_persistent_context(Some(2), DrainDatanodeStrategy::FailFast);
        persistent_ctx.leader_regions = vec![region_id];
        let mut ctx = env.context(persistent_ctx);
        ctx.volatile_ctx.inflight = vec![InflightMigration {
            region_id,
            procedure_id: ProcedureId::random(),
        }];
        let procedure_ctx = exec_ctx_with(Arc::new(ChildProvider::failed()));

        let error = Dispatch.next(&mut ctx, &procedure_ctx).await.unwrap_err();

        // The whole batch is settled before the procedure aborts.
        assert!(matches!(error, error::Error::DrainDatanodeAbort { .. }));
        assert_eq!(0, ctx.persistent_ctx.migrated);
        assert_eq!(vec![region_id], ctx.persistent_ctx.leader_regions);
        assert!(ctx.volatile_ctx.inflight.is_empty());
    }

    #[tokio::test]
    async fn test_drain_datanode_settle_failed_child_try_all() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut persistent_ctx = new_persistent_context(Some(2), DrainDatanodeStrategy::TryAll);
        persistent_ctx.leader_regions = vec![region_id];
        let mut ctx = env.context(persistent_ctx);
        ctx.volatile_ctx.inflight = vec![InflightMigration {
            region_id,
            procedure_id: ProcedureId::random(),
        }];
        let procedure_ctx = exec_ctx_with(Arc::new(ChildProvider::failed()));

        let (_, status) = Dispatch.next(&mut ctx, &procedure_ctx).await.unwrap();

        assert!(matches!(status, Status::Executing { .. }));
        assert_eq!(vec![region_id], ctx.persistent_ctx.failed_regions);
        assert_eq!(0, ctx.persistent_ctx.migrated);
        assert!(ctx.persistent_ctx.leader_regions.is_empty());
    }

    #[tokio::test]
    async fn test_drain_datanode_settle_failed_child_counts_migrated_when_moved() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        // The route reports another datanode as the leader, which is what a failover that
        // raced with the migration leaves behind.
        env.create_table(vec![leader_region(region_id, 2)]).await;

        let mut persistent_ctx = new_persistent_context(Some(2), DrainDatanodeStrategy::FailFast);
        persistent_ctx.leader_regions = vec![region_id];
        let mut ctx = env.context(persistent_ctx);
        ctx.volatile_ctx.inflight = vec![InflightMigration {
            region_id,
            procedure_id: ProcedureId::random(),
        }];
        let procedure_ctx = exec_ctx_with(Arc::new(ChildProvider::failed()));

        let (_, status) = Dispatch.next(&mut ctx, &procedure_ctx).await.unwrap();

        assert!(matches!(status, Status::Executing { .. }));
        assert_eq!(1, ctx.persistent_ctx.migrated);
        assert!(ctx.persistent_ctx.failed_regions.is_empty());
        assert!(ctx.persistent_ctx.leader_regions.is_empty());
    }

    #[tokio::test]
    async fn test_drain_datanode_settle_unknown_child_keeps_pending() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut persistent_ctx = new_persistent_context(Some(2), DrainDatanodeStrategy::FailFast);
        persistent_ctx.leader_regions = vec![region_id];
        let mut ctx = env.context(persistent_ctx);
        ctx.volatile_ctx.inflight = vec![InflightMigration {
            region_id,
            procedure_id: ProcedureId::random(),
        }];
        let procedure_ctx = exec_ctx_with(Arc::new(ChildProvider::unknown()));

        let (_, status) = Dispatch.next(&mut ctx, &procedure_ctx).await.unwrap();

        assert!(matches!(status, Status::Executing { .. }));
        // The region stays pending and is dispatched again instead of being reported as
        // failed.
        assert_eq!(vec![region_id], ctx.persistent_ctx.leader_regions);
        assert_eq!(0, ctx.persistent_ctx.migrated);
        assert!(ctx.persistent_ctx.failed_regions.is_empty());
    }

    #[tokio::test]
    async fn test_drain_datanode_check_drained_rescan_requeues() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        // The plan of the procedure is stale: a region still on the datanode is not listed.
        let mut ctx = env.context(new_persistent_context(
            None,
            DrainDatanodeStrategy::FailFast,
        ));
        let procedure_ctx = TestingEnv::procedure_context();

        let (next, status) = CheckDrained.next(&mut ctx, &procedure_ctx).await.unwrap();

        assert!(matches!(status, Status::Executing { .. }));
        assert!(next.as_any().downcast_ref::<Dispatch>().is_some());
        assert_eq!(vec![region_id], ctx.persistent_ctx.leader_regions);
        assert_eq!(1, ctx.persistent_ctx.rescan_round);
    }

    #[tokio::test]
    async fn test_drain_datanode_check_drained_exceeded_rounds() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut ctx = env.context(new_persistent_context(
            None,
            DrainDatanodeStrategy::FailFast,
        ));
        ctx.persistent_ctx.rescan_round = MAX_RESCAN_ROUNDS;
        let procedure_ctx = TestingEnv::procedure_context();

        let error = CheckDrained
            .next(&mut ctx, &procedure_ctx)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            error::Error::DrainDatanodeRescanExceeded { .. }
        ));
    }

    #[tokio::test]
    async fn test_drain_datanode_check_drained_ignores_failed_regions() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut ctx = env.context(new_persistent_context(None, DrainDatanodeStrategy::TryAll));
        ctx.persistent_ctx.failed_regions = vec![region_id];
        let procedure_ctx = TestingEnv::procedure_context();

        let (next, status) = CheckDrained.next(&mut ctx, &procedure_ctx).await.unwrap();

        assert!(matches!(status, Status::Executing { .. }));
        assert!(next.as_any().downcast_ref::<DrainDatanodeEnd>().is_some());
    }

    #[tokio::test]
    async fn test_drain_datanode_exceeded_deadline() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut persistent_ctx = new_persistent_context(Some(2), DrainDatanodeStrategy::FailFast);
        persistent_ctx.leader_regions = vec![region_id];
        persistent_ctx.deadline_ms = 0;
        let mut ctx = env.context(persistent_ctx);
        let procedure_ctx = TestingEnv::procedure_context();

        let error = Dispatch.next(&mut ctx, &procedure_ctx).await.unwrap_err();

        assert!(matches!(error, error::Error::ExceededDeadline { .. }));
        assert_eq!(vec![region_id], ctx.persistent_ctx.leader_regions);
    }

    #[tokio::test]
    async fn test_drain_datanode_deadline_does_not_interrupt_batch() {
        let env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        let mut persistent_ctx = new_persistent_context(Some(2), DrainDatanodeStrategy::FailFast);
        persistent_ctx.leader_regions = vec![region_id];
        persistent_ctx.deadline_ms = 0;
        let mut ctx = env.context(persistent_ctx);
        ctx.volatile_ctx.inflight = vec![InflightMigration {
            region_id,
            procedure_id: ProcedureId::random(),
        }];
        let procedure_ctx = exec_ctx_with(Arc::new(ChildProvider::done()));

        // The in-flight batch is settled before the deadline is checked.
        let (_, status) = Dispatch.next(&mut ctx, &procedure_ctx).await.unwrap();
        assert!(matches!(status, Status::Executing { .. }));
        assert_eq!(1, ctx.persistent_ctx.migrated);

        // The next step observes the exceeded deadline.
        let error = Dispatch.next(&mut ctx, &procedure_ctx).await.unwrap_err();
        assert!(matches!(error, error::Error::ExceededDeadline { .. }));
    }

    #[tokio::test]
    async fn test_backward_compatibility() {
        let env = TestEnv::new().await;
        // The serialized shape is an on-disk format: it must not be changed silently.
        let json = r#"{"persistent_ctx":{"node_id":1,"target_node_id":2,"parallelism":4,"strategy":"FailFast","leader_regions":[4398046511105],"migrated":0,"failed_regions":[],"follower_region_count":1,"rescan_round":0,"deadline_ms":0},"state":{"drain_datanode_state":"Dispatch"}}"#;

        let procedure =
            DrainDatanodeProcedure::from_json(json, |ctx| env.context_factory.build(ctx)).unwrap();
        assert_eq!("Dispatch", procedure.state.name());
        let persistent_ctx = &procedure.context.persistent_ctx;
        assert_eq!(1, persistent_ctx.node_id);
        assert_eq!(Some(2), persistent_ctx.target_node_id);
        assert_eq!(4, persistent_ctx.parallelism);
        assert_eq!(vec![RegionId::new(1024, 1)], persistent_ctx.leader_regions);
        assert_eq!(1, persistent_ctx.follower_region_count);

        // The restored procedure keeps the persisted deadline.
        let mut context = env.context_factory.build(persistent_ctx.clone());
        let error = Dispatch
            .next(&mut context, &TestingEnv::procedure_context())
            .await
            .unwrap_err();
        assert!(matches!(error, error::Error::ExceededDeadline { .. }));
    }

    #[tokio::test]
    async fn test_drain_datanode_migrates_leader_region() {
        let mut env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        for node_id in [1, 2] {
            let (tx, rx) = tokio::sync::mpsc::channel(8);
            env.mailbox_context()
                .insert_heartbeat_response_receiver(Channel::Datanode(node_id), tx)
                .await;
            spawn_mock_datanode(env.mailbox(), rx);
        }

        env.procedure_manager().start().await.unwrap();
        let manager =
            DrainDatanodeManager::new(env.procedure_manager().clone(), env.context_factory.clone());
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
        let ProcedureState::Done { output } = state else {
            panic!("the drain procedure did not complete successfully");
        };
        let output = output.expect("the drain procedure should produce an output");
        let output = output.downcast_ref::<serde_json::Value>().unwrap();

        assert_eq!(1, output["migrated"]);
        assert_eq!(0, output["follower_region_count"]);
        assert_eq!(true, output["drained"]);

        let table_route = env
            .context_factory
            .table_metadata_manager
            .table_route_manager()
            .table_route_storage()
            .get(1024)
            .await
            .unwrap()
            .unwrap();
        let region_routes = table_route.region_routes().unwrap();
        assert_eq!(
            Some(2),
            region_routes[0].leader_peer.as_ref().map(|peer| peer.id)
        );
        assert!(
            !env.node_scheduling_manager
                .is_schedulable(NodeSchedulingRole::Datanode, 1)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_drain_datanode_migrates_multiple_batches() {
        let mut env = TestEnv::new().await;
        let regions: Vec<_> = (1..=3).map(|number| RegionId::new(1024, number)).collect();
        env.create_table(
            regions
                .iter()
                .copied()
                .map(|region_id| leader_region(region_id, 1))
                .collect(),
        )
        .await;

        for node_id in [1, 2] {
            let (tx, rx) = tokio::sync::mpsc::channel(8);
            env.mailbox_context()
                .insert_heartbeat_response_receiver(Channel::Datanode(node_id), tx)
                .await;
            spawn_mock_datanode(env.mailbox(), rx);
        }

        env.procedure_manager().start().await.unwrap();
        let manager =
            DrainDatanodeManager::new(env.procedure_manager().clone(), env.context_factory.clone());
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
        let ProcedureState::Done { output } = state else {
            panic!("the drain procedure did not complete successfully");
        };
        let output = output.expect("the drain procedure should produce an output");
        let output = output.downcast_ref::<serde_json::Value>().unwrap();

        assert_eq!(3, output["migrated"]);
        assert_eq!(true, output["drained"]);

        let table_route = env
            .context_factory
            .table_metadata_manager
            .table_route_manager()
            .table_route_storage()
            .get(1024)
            .await
            .unwrap()
            .unwrap();
        for region_route in table_route.region_routes().unwrap() {
            assert_eq!(
                Some(2),
                region_route.leader_peer.as_ref().map(|peer| peer.id)
            );
        }
    }

    /// The outcome reported by the mocked migration sub-procedures.
    #[derive(Clone, Copy)]
    enum ChildOutcome {
        Done,
        Failed,
        Unknown,
    }

    /// A [`ContextProvider`] that reports the configured outcome of every sub-procedure.
    struct ChildProvider {
        inner: MockContextProvider,
        outcome: ChildOutcome,
    }

    impl ChildProvider {
        fn done() -> Self {
            Self {
                inner: MockContextProvider::default(),
                outcome: ChildOutcome::Done,
            }
        }

        fn failed() -> Self {
            Self {
                inner: MockContextProvider::default(),
                outcome: ChildOutcome::Failed,
            }
        }

        fn unknown() -> Self {
            Self {
                inner: MockContextProvider::default(),
                outcome: ChildOutcome::Unknown,
            }
        }
    }

    #[async_trait::async_trait]
    impl ContextProvider for ChildProvider {
        async fn procedure_state(
            &self,
            procedure_id: ProcedureId,
        ) -> common_procedure::Result<Option<ProcedureState>> {
            self.inner.procedure_state(procedure_id).await
        }

        async fn procedure_state_receiver(
            &self,
            _procedure_id: ProcedureId,
        ) -> common_procedure::Result<Option<watch::Receiver<ProcedureState>>> {
            let state = match self.outcome {
                ChildOutcome::Done => ProcedureState::Done { output: None },
                ChildOutcome::Failed => ProcedureState::failed(Arc::new(ProcedureError::external(
                    MockError::new(StatusCode::Internal),
                ))),
                ChildOutcome::Unknown => return Ok(None),
            };
            let (sender, receiver) = watch::channel(ProcedureState::Running);
            sender.send(state).unwrap();

            Ok(Some(receiver))
        }

        async fn try_put_poison(
            &self,
            key: &PoisonKey,
            procedure_id: ProcedureId,
        ) -> common_procedure::Result<()> {
            self.inner.try_put_poison(key, procedure_id).await
        }

        async fn acquire_lock(&self, key: &StringKey) -> DynamicKeyLockGuard {
            self.inner.acquire_lock(key).await
        }
    }
}
