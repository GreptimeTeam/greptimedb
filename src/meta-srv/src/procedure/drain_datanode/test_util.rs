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

use crate::metasrv::SelectorContext;
use crate::procedure::drain_datanode::DrainDatanodePlan;
use crate::procedure::drain_datanode::manager::DrainDatanodeManager;
use crate::procedure::region_migration::manager::{
    RegionMigrationManager, RegionMigrationManagerRef,
};
use common_meta::affinity::NoopAffinity;
use common_procedure::ProcedureManagerRef;
use std::sync::Arc;

use common_meta::DatanodeId;
use common_meta::key::TableMetadataManagerRef;
use common_meta::key::test_utils::new_test_table_info;
use common_meta::peer::Peer;
use common_meta::rpc::router::{Region, RegionRoute};
use common_procedure::{Context as ProcedureContext, Status};
use store_api::storage::RegionId;

use crate::error::Result;
use crate::node_scheduling::NodeSchedulingManagerRef;
use crate::procedure::drain_datanode::{Context, PersistentContext, State};
use crate::procedure::region_migration::test_util::TestingEnv;
use crate::test_util::{create_meta_peer_client, new_node_scheduling_discovery, put_datanodes};

pub(super) fn new_persistent_context(target_node_id: Option<DatanodeId>) -> PersistentContext {
    PersistentContext {
        node_id: 1,
        target_node_id,
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
}

pub(super) async fn run_until_done(
    state: &mut Box<dyn State>,
    ctx: &mut Context,
    procedure_ctx: &ProcedureContext,
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

/// Seeds the volatile plan for migration batch tests.
pub(super) fn seed_plan(ctx: &mut Context, leader_regions: Vec<RegionId>) {
    ctx.volatile_ctx.plan = Some(DrainDatanodePlan {
        leader_regions,
        follower_regions: vec![],
    });
}

/// Returns the leader regions the procedure still has to migrate.
pub(super) fn leader_regions(ctx: &Context) -> Vec<RegionId> {
    ctx.volatile_ctx.leader_regions().to_vec()
}
