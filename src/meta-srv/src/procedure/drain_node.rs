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

mod migration;

use std::sync::Arc;
use std::time::Duration;

use common_meta::DatanodeId;
use common_meta::affinity::AffinityResolverRef;
use common_meta::key::TableMetadataManagerRef;
use common_procedure::error::{
    Error as ProcedureError, FromJsonSnafu, Result as ProcedureResult, ToJsonSnafu,
};
use common_procedure::{
    Context as ProcedureContext, LockKey, Procedure, ProcedureId, ProcedureManagerRef,
    ProcedureWithId, Status, watcher,
};
use common_telemetry::{error, info};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::error::{self, Result};
use crate::metasrv::{SelectorContext, SelectorRef};
use crate::procedure::region_migration::manager::RegionMigrationManagerRef;

/// Maximum number of leader regions migrated by one drain batch.
const DRAIN_NODE_BATCH_SIZE: usize = 16;
/// Delay before rescanning when every selected region is already being migrated.
const DRAIN_NODE_RESCAN_INTERVAL: Duration = Duration::from_secs(1);

pub type DrainNodeManagerRef = Arc<DrainNodeManager>;

/// Manages drain-node procedure submission and recovery.
pub struct DrainNodeManager {
    procedure_manager: ProcedureManagerRef,
    context: DrainNodeMigrationContext,
}

impl DrainNodeManager {
    // Wired into Metasrv after node scheduling state is available.
    #[allow(dead_code)]
    pub(crate) fn new(
        procedure_manager: ProcedureManagerRef,
        table_metadata_manager: TableMetadataManagerRef,
        selector_ctx: SelectorContext,
        selector: SelectorRef,
        region_migration_manager: RegionMigrationManagerRef,
        affinity_resolver: Option<AffinityResolverRef>,
    ) -> Self {
        Self {
            procedure_manager,
            context: DrainNodeMigrationContext {
                table_metadata_manager,
                selector_ctx,
                selector,
                region_migration_manager,
                affinity_resolver,
            },
        }
    }

    /// Registers the drain-node procedure loader.
    #[allow(dead_code)]
    pub(crate) fn try_start(&self) -> Result<()> {
        let context = self.context.clone();
        self.procedure_manager
            .register_loader(
                DrainNodeProcedure::TYPE_NAME,
                Box::new(move |json| {
                    DrainNodeProcedure::from_json(json, context.clone()).map(|p| Box::new(p) as _)
                }),
            )
            .context(error::RegisterProcedureLoaderSnafu {
                type_name: DrainNodeProcedure::TYPE_NAME,
            })
    }

    /// Submits an internal drain-node procedure.
    pub async fn submit(&self, datanode_id: DatanodeId) -> Result<ProcedureId> {
        // Reject an unknown or expired source before creating durable procedure state. The
        // procedure resolves it again for each batch because its address can change while drain
        // is running.
        self.context.source_peer(datanode_id).await?;

        let procedure = DrainNodeProcedure::new(datanode_id, self.context.clone());
        let procedure = ProcedureWithId::with_random_id(Box::new(procedure));
        let procedure_id = procedure.id;
        info!("Starting drain-node procedure {procedure_id} for datanode {datanode_id}");
        let mut receiver = self
            .procedure_manager
            .submit(procedure)
            .await
            .context(error::SubmitProcedureSnafu)?;

        common_runtime::spawn_global(async move {
            if let Err(e) = watcher::wait(&mut receiver).await {
                error!(e; "Drain-node procedure {procedure_id} for datanode {datanode_id} failed");
                return;
            }
            info!("Drain-node procedure {procedure_id} for datanode {datanode_id} completed");
        });

        Ok(procedure_id)
    }
}

#[derive(Clone)]
struct DrainNodeMigrationContext {
    table_metadata_manager: TableMetadataManagerRef,
    selector_ctx: SelectorContext,
    selector: SelectorRef,
    region_migration_manager: RegionMigrationManagerRef,
    affinity_resolver: Option<AffinityResolverRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum DrainNodeState {
    Prepare,
    Scan,
    Wait { procedure_ids: Vec<ProcedureId> },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct DrainNodeData {
    datanode_id: DatanodeId,
    state: DrainNodeState,
}

/// A recoverable procedure that repeatedly migrates batches of leader regions off one datanode.
struct DrainNodeProcedure {
    data: DrainNodeData,
    context: DrainNodeMigrationContext,
}

impl DrainNodeProcedure {
    const TYPE_NAME: &'static str = "metasrv-procedure::DrainNode";

    fn new(datanode_id: DatanodeId, context: DrainNodeMigrationContext) -> Self {
        Self {
            data: DrainNodeData {
                datanode_id,
                state: DrainNodeState::Prepare,
            },
            context,
        }
    }

    #[allow(dead_code)]
    fn from_json(json: &str, context: DrainNodeMigrationContext) -> ProcedureResult<Self> {
        let data = serde_json::from_str(json).context(FromJsonSnafu)?;
        Ok(Self { data, context })
    }

    async fn execute_inner(&mut self, procedure_ctx: &ProcedureContext) -> Result<Status> {
        match self.data.state.clone() {
            DrainNodeState::Prepare => self.prepare(),
            DrainNodeState::Scan => self.scan().await,
            DrainNodeState::Wait { procedure_ids } => {
                self.wait(procedure_ctx, &procedure_ids).await
            }
        }
    }

    fn prepare(&mut self) -> Result<Status> {
        self.data.state = DrainNodeState::Scan;
        Ok(Status::executing(true))
    }

    async fn scan(&mut self) -> Result<Status> {
        // TODO(Aetherance): Mark the source unschedulable before migrating its regions.
        let region_ids = self.context.leader_regions(self.data.datanode_id).await?;
        if region_ids.is_empty() {
            return Ok(Status::done());
        }

        let batch = &region_ids[..region_ids.len().min(DRAIN_NODE_BATCH_SIZE)];
        let procedure_ids = match self
            .context
            .submit_migrations(self.data.datanode_id, batch)
            .await
        {
            Ok(batch) => batch,
            Err(
                error::Error::NoEnoughAvailableNode { .. } | error::Error::PeerUnavailable { .. },
            ) => {
                tokio::time::sleep(DRAIN_NODE_RESCAN_INTERVAL).await;
                return Ok(Status::executing(false));
            }
            Err(err) => return Err(err),
        };
        if procedure_ids.is_empty() {
            // In-flight migrations, concurrent route changes and stale metadata must not
            // exhaust the procedure framework's bounded error retry budget or spin hot.
            tokio::time::sleep(DRAIN_NODE_RESCAN_INTERVAL).await;
            // Rescan current metadata when the batch submitted no new migrations.
            return Ok(Status::executing(false));
        }

        self.data.state = DrainNodeState::Wait { procedure_ids };
        // These migrations are independent root procedures. Persist their IDs before
        // waiting; the next scan reconciles any task submitted before a crash.
        Ok(Status::executing(true))
    }

    async fn wait(
        &mut self,
        procedure_ctx: &ProcedureContext,
        procedure_ids: &[ProcedureId],
    ) -> Result<Status> {
        let mut known_procedures = Vec::with_capacity(procedure_ids.len());
        let mut migration_failures = Vec::new();
        for &procedure_id in procedure_ids {
            let receiver = procedure_ctx
                .provider
                .procedure_state_receiver(procedure_id)
                .await
                .context(error::QueryProcedureSnafu)?;
            if let Some(receiver) = receiver {
                known_procedures.push((procedure_id, receiver));
            }
        }

        for (procedure_id, mut receiver) in known_procedures {
            if let Err(source) = watcher::wait(&mut receiver).await {
                migration_failures.push(format!(
                    "Drain-node migration procedure {procedure_id} failed: {source}"
                ));
            }
        }
        if !migration_failures.is_empty() {
            error!(
                "Drain-node migration batch failed, rescanning routes: {}",
                migration_failures.join("; ")
            );
            tokio::time::sleep(DRAIN_NODE_RESCAN_INTERVAL).await;
        }

        // Root procedure states may have been collected during recovery. Rescan durable
        // routes after all known tasks finish, including when some IDs are unknown.
        self.data.state = DrainNodeState::Scan;
        Ok(Status::executing(true))
    }
}

#[async_trait::async_trait]
impl Procedure for DrainNodeProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, ctx: &ProcedureContext) -> ProcedureResult<Status> {
        self.execute_inner(ctx).await.map_err(|e| {
            if e.is_retryable() {
                ProcedureError::retry_later(e)
            } else {
                ProcedureError::external(e)
            }
        })
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        // Serialize drains of one node without blocking child region-migration locks.
        LockKey::single_exclusive(format!("drain-node/{}", self.data.datanode_id))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use common_error::mock::MockError;
    use common_error::status_code::StatusCode;
    use common_meta::affinity::{
        Affinity, AffinityReference, AffinityRequirements, AffinityResolver, NoopAffinity,
    };
    use common_meta::cluster::NodeInfo;
    use common_meta::key::MetadataKey;
    use common_meta::key::datanode_table::DatanodeTableKey;
    use common_meta::key::test_utils::new_test_table_info_with_name;
    use common_meta::peer::Peer;
    use common_meta::rpc::router::{Region, RegionRoute};
    use common_procedure::ProcedureState;
    use common_procedure_test::MockContextProvider;
    use store_api::storage::RegionId;
    use tokio::sync::watch;

    use super::*;
    use crate::cluster::MetaPeerClientRef;
    use crate::metasrv::SelectTarget;
    use crate::procedure::region_migration::manager::RegionMigrationManager;
    use crate::procedure::region_migration::test_util::TestingEnv as MigrationTestingEnv;
    use crate::procedure::repartition::test_util::{
        procedure_context_with_receivers, procedure_state_receiver,
    };
    use crate::selector::round_robin::RoundRobinSelector;
    use crate::test_util::{create_meta_peer_client, put_datanodes};

    struct TestingEnv {
        migration_env: MigrationTestingEnv,
        peer_client: MetaPeerClientRef,
        context: DrainNodeMigrationContext,
    }

    impl TestingEnv {
        fn new() -> Self {
            let migration_env = MigrationTestingEnv::new();
            let peer_client = create_meta_peer_client();
            let region_migration_manager = Arc::new(RegionMigrationManager::new(
                migration_env.procedure_manager().clone(),
                migration_env.context_factory(),
            ));
            let context = DrainNodeMigrationContext {
                table_metadata_manager: migration_env.table_metadata_manager().clone(),
                selector_ctx: SelectorContext {
                    peer_discovery: peer_client.clone(),
                    affinity: Arc::new(NoopAffinity),
                    affinity_fallback: Default::default(),
                },
                selector: Arc::new(RoundRobinSelector::new(SelectTarget::Datanode)),
                region_migration_manager: region_migration_manager.clone(),
                affinity_resolver: None,
            };
            Self {
                migration_env,
                peer_client,
                context,
            }
        }

        async fn put_peers(&self, ids: &[u64]) {
            let peers = ids
                .iter()
                .map(|id| Peer {
                    id: *id,
                    addr: format!("node-{id}"),
                })
                .collect();
            put_datanodes(&self.peer_client, peers).await;
        }

        async fn add_table(&self, table_id: u32, name: &str, routes: Vec<RegionRoute>) {
            self.migration_env
                .create_physical_table_metadata(
                    new_test_table_info_with_name(table_id, name),
                    routes,
                )
                .await;
        }

        fn procedure(&self, datanode_id: DatanodeId) -> DrainNodeProcedure {
            DrainNodeProcedure::new(datanode_id, self.context.clone())
        }

        fn scan_procedure(&self, datanode_id: DatanodeId) -> DrainNodeProcedure {
            let mut procedure = self.procedure(datanode_id);
            procedure.data.state = DrainNodeState::Scan;
            procedure
        }
    }

    fn route(region_id: RegionId, leader: u64, followers: &[u64]) -> RegionRoute {
        RegionRoute {
            region: Region::new_test(region_id),
            leader_peer: Some(Peer::empty(leader)),
            follower_peers: followers.iter().map(|id| Peer::empty(*id)).collect(),
            ..Default::default()
        }
    }

    fn default_procedure_context() -> ProcedureContext {
        ProcedureContext {
            procedure_id: ProcedureId::random(),
            provider: Arc::new(MockContextProvider::default()),
            event_context: None,
        }
    }

    #[test]
    fn test_restore_initial_scan_state() {
        let json = r#"{"datanode_id":7,"state":"Scan"}"#;
        let data: DrainNodeData = serde_json::from_str(json).unwrap();
        assert_eq!(data.datanode_id, 7);
        assert_eq!(data.state, DrainNodeState::Scan);
    }

    #[tokio::test]
    async fn test_prepare_then_empty_scan_completes() {
        let env = TestingEnv::new();
        let mut procedure = env.procedure(1);

        let status = procedure
            .execute(&default_procedure_context())
            .await
            .unwrap();

        assert!(matches!(status, Status::Executing { persist: true, .. }));
        assert_eq!(procedure.data.state, DrainNodeState::Scan);

        assert!(
            procedure
                .execute(&default_procedure_context())
                .await
                .unwrap()
                .is_done()
        );
    }

    #[tokio::test]
    async fn test_no_available_target_waits_to_rescan() {
        let env = TestingEnv::new();
        env.put_peers(&[1]).await;
        env.add_table(100, "table_100", vec![route(RegionId::new(100, 1), 1, &[])])
            .await;
        let mut procedure = env.scan_procedure(1);

        let status = procedure
            .execute(&default_procedure_context())
            .await
            .unwrap();

        assert!(matches!(status, Status::Executing { persist: false, .. }));
    }

    #[tokio::test]
    async fn test_target_selection_uses_source_affinity() {
        struct SourceAffinity;
        #[async_trait::async_trait]
        impl AffinityResolver for SourceAffinity {
            async fn resolve(
                &self,
                references: &[AffinityReference],
            ) -> common_meta::error::Result<Vec<Option<AffinityRequirements>>> {
                assert_eq!(references, &[AffinityReference::Datanode(1)]);
                Ok(vec![AffinityRequirements::new(HashMap::from([(
                    "pool".into(),
                    "a".into(),
                )]))])
            }
        }

        struct OnlyThirdNode;
        impl Affinity for OnlyThirdNode {
            fn matches(&self, _: &AffinityRequirements, candidate: &NodeInfo) -> bool {
                candidate.peer.id == 3
            }
        }

        let mut env = TestingEnv::new();
        env.put_peers(&[1, 2, 3]).await;
        env.context.affinity_resolver = Some(Arc::new(SourceAffinity));
        env.context.selector_ctx.affinity = Arc::new(OnlyThirdNode);

        let groups = env
            .context
            .select_migration_targets(1, &[RegionId::new(100, 1)])
            .await
            .unwrap();
        assert_eq!(groups.keys().copied().collect::<Vec<_>>(), vec![3]);
    }

    #[tokio::test]
    async fn test_follower_regions_are_rejected() {
        let env = TestingEnv::new();
        env.add_table(
            100,
            "table_100",
            vec![route(RegionId::new(100, 1), 2, &[1])],
        )
        .await;
        let mut procedure = env.scan_procedure(1);

        let err = procedure
            .execute(&default_procedure_context())
            .await
            .unwrap_err();

        assert!(
            format!("{err:?}").contains("follower regions is unsupported"),
            "unexpected error: {err:?}"
        );
        assert!(!err.is_retry_later());
    }

    #[tokio::test]
    async fn test_recovered_batch_rescans_remaining_region() {
        let env = TestingEnv::new();
        env.migration_env.procedure_manager().start().await.unwrap();
        env.put_peers(&[1, 2, 3]).await;
        env.add_table(
            100,
            "table_100",
            (0..DRAIN_NODE_BATCH_SIZE as u32)
                .map(|number| route(RegionId::new(100, number), 1, &[]))
                .collect(),
        )
        .await;
        env.add_table(200, "table_200", vec![route(RegionId::new(200, 7), 1, &[])])
            .await;
        let region_ids = env.context.leader_regions(1).await.unwrap();
        assert_eq!(region_ids.len(), DRAIN_NODE_BATCH_SIZE + 1);
        assert_eq!(region_ids[0], RegionId::new(100, 0));
        assert_eq!(region_ids[DRAIN_NODE_BATCH_SIZE], RegionId::new(200, 7));

        let mut procedure = env.scan_procedure(1);
        let first_status = procedure
            .execute(&default_procedure_context())
            .await
            .unwrap();
        assert!(matches!(
            first_status,
            Status::Executing { persist: true, .. }
        ));
        let DrainNodeState::Wait { procedure_ids } = &procedure.data.state else {
            panic!("expected wait state");
        };
        assert_eq!(procedure_ids.len(), 2);
        let first_migration_ids = procedure_ids.clone();
        let json = procedure.dump().unwrap();

        let key = DatanodeTableKey::new(1, 100).to_bytes();
        env.migration_env
            .kv_backend()
            .delete(&key, false)
            .await
            .unwrap();
        let mut recovered = DrainNodeProcedure::from_json(&json, env.context.clone()).unwrap();
        assert_eq!(recovered.dump().unwrap(), json);
        let status = recovered
            .execute(&default_procedure_context())
            .await
            .unwrap();
        assert!(matches!(status, Status::Executing { persist: true, .. }));
        assert_eq!(recovered.data.state, DrainNodeState::Scan);

        let status = recovered
            .execute(&default_procedure_context())
            .await
            .unwrap();
        assert!(matches!(status, Status::Executing { persist: true, .. }));
        let DrainNodeState::Wait { procedure_ids } = &recovered.data.state else {
            panic!("expected wait state");
        };
        assert_eq!(procedure_ids.len(), 1);
        assert!(!first_migration_ids.contains(&procedure_ids[0]));
        env.migration_env.procedure_manager().stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_failed_migration_does_not_complete() {
        let env = TestingEnv::new();
        let migration_id = ProcedureId::random();
        let running_migration_id = ProcedureId::random();
        let mut failed = env.scan_procedure(1);
        failed.data.state = DrainNodeState::Wait {
            procedure_ids: vec![migration_id, running_migration_id],
        };
        let migration_error =
            common_procedure::Error::external(MockError::new(StatusCode::Internal));
        let failed_state = ProcedureState::failed(Arc::new(migration_error));
        let failed_receiver = procedure_state_receiver(failed_state);
        let (running_sender, running_receiver) = watch::channel(ProcedureState::Running);
        let context = procedure_context_with_receivers(HashMap::from([
            (migration_id, failed_receiver),
            (running_migration_id, running_receiver),
        ]));
        let mut execution = tokio::spawn(async move { failed.execute(&context).await });

        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut execution)
                .await
                .is_err(),
            "drain returned before the running migration reached a terminal state"
        );
        running_sender
            .send(ProcedureState::Done { output: None })
            .unwrap();
        let status = execution.await.unwrap().unwrap();
        assert!(matches!(status, Status::Executing { persist: true, .. }));
    }
}
