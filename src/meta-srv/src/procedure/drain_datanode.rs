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
pub(crate) mod collect;
pub(crate) mod dispatch;
pub(crate) mod drain_datanode_end;
pub(crate) mod drain_datanode_start;
pub(crate) mod manager;
pub(crate) mod utils;

use std::any::Any;
use std::fmt::Debug;
use std::str::FromStr;
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

/// The default number of migration sub-procedures dispatched per round.
pub const DEFAULT_BATCH_SIZE: usize = 4;
/// The maximum number of migration sub-procedures dispatched per round.
pub const MAX_BATCH_SIZE: usize = 32;
/// The maximum number of times the procedure rescans the datanode before giving up.
pub const MAX_RESCAN_ROUNDS: u32 = 3;

/// Strategy of a drain run when a region cannot be migrated.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum DrainDatanodeStrategy {
    /// Aborts the procedure on the first failed region.
    #[default]
    FailFast,
    /// Drains every remaining region and records the failed ones.
    TryAll,
}

impl FromStr for DrainDatanodeStrategy {
    type Err = String;

    fn from_str(strategy: &str) -> std::result::Result<Self, Self::Err> {
        match strategy {
            "FailFast" => Ok(Self::FailFast),
            "TryAll" => Ok(Self::TryAll),
            _ => Err(strategy.to_string()),
        }
    }
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
    /// The number of migration sub-procedures dispatched per round.
    pub batch_size: usize,
    /// The error handling strategy.
    pub strategy: DrainDatanodeStrategy,
    /// The leader regions on `node_id` that have not been migrated successfully yet.
    ///
    /// A region leaves this list after its migration sub-procedure succeeded, after it was
    /// given up by [`DrainDatanodeStrategy::TryAll`], or after `node_id` stopped leading it.
    pub leader_regions: Vec<RegionId>,
    /// The submitted migration sub-procedures that are not settled yet.
    pub inflight_procedures: Vec<ProcedureMeta>,
    /// The number of regions migrated successfully.
    pub migrated: usize,
    /// The regions given up by [`DrainDatanodeStrategy::TryAll`].
    pub failed_regions: Vec<RegionId>,
    /// The number of follower occurrences on `node_id`, counted when the plan is built.
    pub follower_region_count: usize,
    /// The number of rescans performed so far.
    pub rescan_round: u32,
}

/// The metadata of a submitted migration sub-procedure.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ProcedureMeta {
    /// The region migrated by the sub-procedure.
    pub region_id: RegionId,
    /// The procedure id of the sub-procedure.
    pub procedure_id: ProcedureId,
}

/// The task of [`DrainDatanodeProcedure`].
#[derive(Debug, Clone)]
pub struct DrainDatanodeProcedureTask {
    /// The datanode to drain.
    pub node_id: DatanodeId,
    /// The pinned destination of leader regions. Resolved by the selector when `None`.
    pub target_node_id: Option<DatanodeId>,
    /// The number of migration sub-procedures dispatched per round.
    pub batch_size: usize,
    /// The error handling strategy.
    pub strategy: DrainDatanodeStrategy,
}

/// The context of [`DrainDatanodeProcedure`].
#[derive(Clone)]
pub struct Context {
    /// The persistent data of the procedure.
    pub persistent_ctx: PersistentContext,
    /// The table metadata manager.
    pub table_metadata_manager: TableMetadataManagerRef,
    /// The node scheduling manager, used to fence the drained datanode.
    pub node_scheduling_manager: NodeSchedulingManagerRef,
    /// The region migration manager, used to build migration sub-procedures.
    pub region_migration_manager: RegionMigrationManagerRef,
    /// The selector context, used to resolve the destination of a leader region.
    pub selector_ctx: SelectorContext,
    /// The timeout budget of each migration sub-procedure.
    pub timeout: Duration,
}

/// Creates the [`Context`] of a [`DrainDatanodeProcedure`] for a given persistent state.
#[derive(Clone)]
pub struct ContextFactory {
    pub table_metadata_manager: TableMetadataManagerRef,
    pub node_scheduling_manager: NodeSchedulingManagerRef,
    pub region_migration_manager: RegionMigrationManagerRef,
    pub selector_ctx: SelectorContext,
    pub timeout: Duration,
}

impl ContextFactory {
    /// Builds the [`Context`] of a procedure with `persistent_ctx`.
    pub fn build(&self, persistent_ctx: PersistentContext) -> Context {
        Context {
            persistent_ctx,
            table_metadata_manager: self.table_metadata_manager.clone(),
            node_scheduling_manager: self.node_scheduling_manager.clone(),
            region_migration_manager: self.region_migration_manager.clone(),
            selector_ctx: self.selector_ctx.clone(),
            timeout: self.timeout,
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
/// [`RegionMigrationProcedure`](crate::procedure::region_migration::RegionMigrationProcedure) sub-procedures,
/// and rescans the datanode until no leader region is left. Follower occurrences are
/// only counted; removing them is not part of this procedure yet.
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
    use std::sync::{Arc, Mutex};

    use api::v1::meta::MailboxMessage;
    use api::v1::meta::mailbox_message::Payload;
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
        Context as ExecContext, ContextProvider, PoisonKey, ProcedureManagerRef, ProcedureState,
        StringKey,
    };
    use common_procedure_test::MockContextProvider;
    use common_time::util::current_time_millis;
    use tokio::sync::watch;

    use super::*;
    use crate::error;
    use crate::key::NodeSchedulingRole;
    use crate::metasrv::SelectorContext;
    use crate::procedure::drain_datanode::check_drained::CheckDrained;
    use crate::procedure::drain_datanode::collect::Collect;
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
            batch_size: DEFAULT_BATCH_SIZE,
            strategy,
            leader_regions: vec![],
            inflight_procedures: vec![],
            migrated: 0,
            failed_regions: vec![],
            follower_region_count: 0,
            rescan_round: 0,
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
                timeout: Duration::from_secs(10),
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

    /// Spawns a mock datanode that answers every mailbox message of the migration.
    fn spawn_mock_datanode(
        mailbox: MailboxRef,
        mut rx: MockHeartbeatReceiver,
        region_id: RegionId,
    ) {
        common_runtime::spawn_global(async move {
            while let Some(Ok(response)) = rx.recv().await {
                let message = response.mailbox_message.unwrap();
                if let Some(reply) = mock_instruction_reply(&message, region_id) {
                    mailbox.on_recv(message.id, Ok(reply)).await.unwrap();
                }
            }
        });
    }

    /// Returns the reply of `message`, or `None` if the instruction needs no reply.
    fn mock_instruction_reply(
        message: &MailboxMessage,
        region_id: RegionId,
    ) -> Option<MailboxMessage> {
        let Some(Payload::Json(payload)) = message.payload.as_ref() else {
            panic!("unexpected mailbox payload of message {}", message.id);
        };
        let instruction: Instruction = serde_json::from_str(payload).unwrap();
        let reply = match instruction {
            Instruction::OpenRegions(_) => InstructionReply::OpenRegions(SimpleReply {
                result: true,
                error: None,
            }),
            Instruction::FlushRegions(_) => {
                InstructionReply::FlushRegions(FlushRegionReply::success_single(region_id))
            }
            Instruction::DowngradeRegions(_) => {
                InstructionReply::DowngradeRegions(DowngradeRegionsReply::new(vec![
                    DowngradeRegionReply {
                        region_id,
                        last_entry_id: Some(1),
                        metadata_last_entry_id: Some(1),
                        exists: true,
                        error: None,
                    },
                ]))
            }
            Instruction::UpgradeRegions(_) => {
                InstructionReply::UpgradeRegions(UpgradeRegionsReply::single(UpgradeRegionReply {
                    region_id,
                    ready: true,
                    exists: true,
                    error: None,
                }))
            }
            Instruction::CloseRegions(_) => InstructionReply::CloseRegions(SimpleReply {
                result: true,
                error: None,
            }),
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
        for _ in 0..10 {
            let (next, status) = state.next(ctx, procedure_ctx).await?;
            *state = next;
            if !matches!(status, Status::Executing { .. }) {
                return Ok(status);
            }
        }

        panic!("the drain procedure did not finish in 10 steps");
    }

    fn done_output(status: Status) -> serde_json::Value {
        let Status::Done { output } = status else {
            panic!("expected the drain procedure to be done");
        };
        let output = output.expect("the drain procedure should produce an output");
        output.downcast_ref::<serde_json::Value>().unwrap().clone()
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
        assert!(
            !env.node_scheduling_manager
                .is_schedulable(NodeSchedulingRole::Datanode, 1)
                .await
                .unwrap()
        );
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

        let (next, _) = DrainDatanodeStart
            .next(&mut ctx, &procedure_ctx)
            .await
            .unwrap();
        let mut state = next;

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
        let failed_regions: Vec<RegionId> =
            serde_json::from_value(output["failed_regions"].clone()).unwrap();
        assert_eq!(vec![region_id], failed_regions);
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
        assert!(persist);
        assert_eq!(1, subprocedures.len());
        assert_eq!(1, ctx.persistent_ctx.inflight_procedures.len());
        assert_eq!(
            region_id,
            ctx.persistent_ctx.inflight_procedures[0].region_id
        );
        assert_eq!(vec![region_id], ctx.persistent_ctx.leader_regions);
        assert!(next.as_any().downcast_ref::<Collect>().is_some());
    }

    #[tokio::test]
    async fn test_drain_datanode_collects_finished_subprocedure() {
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
        let _ = next.next(&mut ctx, &procedure_ctx).await.unwrap();

        let finished_ctx = ExecContext {
            procedure_id: ProcedureId::random(),
            provider: Arc::new(FinishedChildProvider::default()),
            event_context: None,
        };
        let (next, status) = Collect.next(&mut ctx, &finished_ctx).await.unwrap();

        assert!(matches!(status, Status::Executing { .. }));
        assert_eq!(1, ctx.persistent_ctx.migrated);
        assert!(ctx.persistent_ctx.leader_regions.is_empty());
        assert!(ctx.persistent_ctx.inflight_procedures.is_empty());
        assert!(next.as_any().downcast_ref::<CheckDrained>().is_some());
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
    async fn test_drain_datanode_migrates_leader_region() {
        let mut env = TestEnv::new().await;
        let region_id = RegionId::new(1024, 1);
        env.create_table(vec![leader_region(region_id, 1)]).await;

        for node_id in [1, 2] {
            let (tx, rx) = tokio::sync::mpsc::channel(8);
            env.mailbox_context()
                .insert_heartbeat_response_receiver(Channel::Datanode(node_id), tx)
                .await;
            spawn_mock_datanode(env.mailbox(), rx, region_id);
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
                    batch_size: DEFAULT_BATCH_SIZE,
                    strategy: DrainDatanodeStrategy::FailFast,
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
    async fn test_drain_datanode_dispatch_rolls_back_unsubmitted_children() {
        let env = TestEnv::new().await;
        let first = RegionId::new(1024, 1);
        let second = RegionId::new(1024, 2);
        // The second region already has the pinned target as a follower, so selecting the
        // target fails after the migration sub-procedure of the first region was built.
        env.create_table(vec![leader_region(first, 1), follower_region(second, 1, 2)])
            .await;

        let mut ctx = env.context(new_persistent_context(
            Some(2),
            DrainDatanodeStrategy::FailFast,
        ));
        let procedure_ctx = TestingEnv::procedure_context();

        let (mut state, _) = DrainDatanodeStart
            .next(&mut ctx, &procedure_ctx)
            .await
            .unwrap();
        let error = state.next(&mut ctx, &procedure_ctx).await.unwrap_err();

        assert!(matches!(error, error::Error::DrainDatanodeAbort { .. }));
        // The built child of `first` was dropped without being submitted, so nothing may be
        // recorded as inflight and no region may be considered handled.
        assert!(ctx.persistent_ctx.inflight_procedures.is_empty());
        assert_eq!(vec![first, second], ctx.persistent_ctx.leader_regions);
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
        persistent_ctx.batch_size = 2;
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

    /// A [`ContextProvider`] that reports every sub-procedure as finished.
    #[derive(Default)]
    struct FinishedChildProvider {
        inner: MockContextProvider,
        senders: Mutex<Vec<watch::Sender<ProcedureState>>>,
    }

    #[async_trait::async_trait]
    impl ContextProvider for FinishedChildProvider {
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
            let (sender, receiver) = watch::channel(ProcedureState::Running);
            sender.send(ProcedureState::Done { output: None }).unwrap();
            self.senders.lock().unwrap().push(sender);
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
