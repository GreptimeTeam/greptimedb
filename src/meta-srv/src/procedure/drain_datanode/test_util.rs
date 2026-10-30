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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use api::v1::meta::MailboxMessage;
use api::v1::meta::mailbox_message::Payload;
use common_meta::DatanodeId;
use common_meta::affinity::NoopAffinity;
use common_meta::instruction::{
    DowngradeRegionReply, DowngradeRegionsReply, FlushRegionReply, Instruction, InstructionReply,
    SimpleReply, UpgradeRegionReply, UpgradeRegionsReply,
};
use common_meta::key::TableMetadataManagerRef;
use common_meta::key::table_route::TableRouteValue;
use common_meta::key::test_utils::new_test_table_info;
use common_meta::peer::Peer;
use common_meta::rpc::router::{Region, RegionRoute};
use common_procedure::{
    Context as ExecContext, ProcedureId, ProcedureManagerRef, ProcedureState, Status,
};
use common_time::util::current_time_millis;
use common_wal::options::{KafkaWalOptions, WalOptions};
use store_api::storage::RegionId;

use crate::error::Result;
use crate::metasrv::SelectorContext;
use crate::node_scheduling::NodeSchedulingManagerRef;
use crate::procedure::drain_datanode::manager::DrainDatanodeManager;
use crate::procedure::drain_datanode::{
    Context, DEFAULT_PARALLELISM, DrainDatanodeStrategy, PersistentContext, State, TopicBatch,
};
use crate::procedure::region_migration::manager::{
    RegionMigrationManager, RegionMigrationManagerRef,
};
use crate::procedure::region_migration::test_util::TestingEnv;
use crate::procedure::test_util::{MockHeartbeatReceiver, new_open_region_reply};
use crate::service::mailbox::{Channel, MailboxRef};
use crate::test_util::{create_meta_peer_client, new_node_scheduling_discovery, put_datanodes};

pub(super) fn new_persistent_context(
    target_node_id: Option<DatanodeId>,
    strategy: DrainDatanodeStrategy,
) -> PersistentContext {
    PersistentContext {
        node_id: 1,
        target_node_id,
        parallelism: DEFAULT_PARALLELISM,
        strategy,
        deadline_ms: current_time_millis() + 60 * 1000,
    }
}

pub(super) fn leader_region(region_id: RegionId, leader_id: DatanodeId) -> RegionRoute {
    RegionRoute {
        region: Region::new_test(region_id),
        leader_peer: Some(Peer::empty(leader_id)),
        follower_peers: vec![],
        ..Default::default()
    }
}

pub(super) fn follower_region(
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

pub(super) struct TestEnv {
    pub(super) env: TestingEnv,
    pub(super) table_metadata_manager: TableMetadataManagerRef,
    pub(super) node_scheduling_manager: NodeSchedulingManagerRef,
    pub(super) region_migration_manager: RegionMigrationManagerRef,
    pub(super) selector_ctx: SelectorContext,
}

impl TestEnv {
    pub(super) async fn new() -> Self {
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
        let (node_scheduling_manager, discovery) = new_node_scheduling_discovery(&meta_peer_client);
        let selector_ctx = SelectorContext {
            affinity_fallback: Default::default(),
            affinity: Arc::new(NoopAffinity),
            peer_discovery: Arc::new(discovery),
        };
        let region_migration_manager = Arc::new(RegionMigrationManager::new(
            env.procedure_manager().clone(),
            env.context_factory(),
        ));

        let table_metadata_manager = env.table_metadata_manager().clone();

        Self {
            env,
            table_metadata_manager,
            node_scheduling_manager,
            region_migration_manager,
            selector_ctx,
        }
    }

    pub(super) fn context(&self, persistent_ctx: PersistentContext) -> Context {
        Context::new(
            self.table_metadata_manager.clone(),
            self.node_scheduling_manager.clone(),
            self.region_migration_manager.clone(),
            self.selector_ctx.clone(),
            persistent_ctx,
        )
    }

    pub(super) fn table_metadata_manager(&self) -> &TableMetadataManagerRef {
        &self.table_metadata_manager
    }

    pub(super) async fn create_table(&self, region_routes: Vec<RegionRoute>) {
        self.env
            .create_physical_table_metadata(new_test_table_info(1024), region_routes)
            .await;
    }

    pub(super) fn procedure_manager(&self) -> &ProcedureManagerRef {
        self.env.procedure_manager()
    }

    pub(super) fn drain_manager(&self) -> DrainDatanodeManager {
        DrainDatanodeManager::new(
            self.procedure_manager().clone(),
            self.table_metadata_manager.clone(),
            self.node_scheduling_manager.clone(),
            self.region_migration_manager.clone(),
            self.selector_ctx.clone(),
        )
    }

    pub(super) async fn mock_datanodes(&mut self) {
        let mailbox_ctx = self.env.mailbox_context();
        for node_id in [1, 2] {
            let (tx, rx) = tokio::sync::mpsc::channel(8);
            mailbox_ctx
                .insert_heartbeat_response_receiver(Channel::Datanode(node_id), tx)
                .await;
            spawn_mock_datanode(mailbox_ctx.mailbox().clone(), rx);
        }
    }
}

/// Spawns a mock datanode that answers every mailbox message of a migration.
pub(super) fn spawn_mock_datanode(mailbox: MailboxRef, mut rx: MockHeartbeatReceiver) {
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
        Instruction::OpenRegions(_) => {
            return Some(new_open_region_reply(message.id, true, None));
        }
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

pub(super) async fn wait_for_terminal_state(
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

pub(super) async fn run_until_done(
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

/// Seeds the volatile plan, as if `DrainDatanodeCursor` had built it.
pub(super) fn seed_plan(ctx: &mut Context, leader_regions: Vec<RegionId>) {
    ctx.volatile_ctx.topic_batch = Some(TopicBatch {
        leader_regions,
        ..Default::default()
    });
}

/// Returns the leader regions the procedure still has to migrate.
pub(super) fn leader_regions(ctx: &Context) -> Vec<RegionId> {
    ctx.volatile_ctx.leader_regions().to_vec()
}

/// Prepares the seeded topic as the cursor would, then executes the dispatch state.
pub(crate) async fn dispatch_next(
    ctx: &mut Context,
    procedure_ctx: &ExecContext,
) -> Result<(Box<dyn State>, Status)> {
    if ctx.volatile_ctx.inflight_subprocedures.is_empty() {
        ctx.prepare_migrations().await?;
    }
    crate::procedure::drain_datanode::dispatch::Dispatch
        .next(ctx, procedure_ctx)
        .await
}

/// Creates interleaved Kafka topic assignments for drain cursor tests.
pub(crate) async fn create_kafka_table(env: &TestEnv, table_id: u32, count: u32, topics: u32) {
    let routes = (0..count)
        .map(|i| leader_region(RegionId::new(table_id, i), 1))
        .collect();
    let options = (0..count)
        .map(|i| {
            (
                i,
                WalOptions::Kafka(KafkaWalOptions::new(format!("topic-{}", i % topics))),
            )
        })
        .collect::<HashMap<_, _>>();
    env.table_metadata_manager
        .create_table_metadata(
            new_test_table_info(table_id),
            TableRouteValue::physical(routes),
            options,
        )
        .await
        .unwrap();
}
