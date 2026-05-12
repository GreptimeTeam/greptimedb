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
use std::time::{Duration, Instant};

use common_meta::ddl::DdlContext;
use common_meta::key::table_route::TableRouteValue;
use common_meta::key::test_utils::{new_test_table_info, new_test_table_info_with_name};
use common_meta::key::{TableMetadataManager, TableMetadataManagerRef};
use common_meta::kv_backend::KvBackendRef;
use common_meta::kv_backend::memory::MemoryKvBackend;
use common_meta::node_manager::NodeManagerRef;
use common_meta::peer::Peer;
use common_meta::rpc::router::{Region, RegionRoute};
use common_meta::sequence::SequenceBuilder;
use common_meta::test_util::new_ddl_context_with_kv_backend;
use common_procedure::{
    Context as ProcedureContext, ContextProvider, ProcedureId, ProcedureState, Status,
};
use common_procedure_test::MockContextProvider;
use common_wal::options::{KafkaWalOptions, WalOptions};
use datatypes::value::Value;
use partition::expr::{PartitionExpr, col};
use store_api::storage::{RegionId, RegionNumber, TableId};
use table::table_name::TableName;
use tokio::sync::watch;
use uuid::Uuid;

use crate::cache_invalidator::MetasrvCacheInvalidator;
use crate::metasrv::MetasrvInfo;
use crate::procedure::repartition::group::{Context, PersistentContext, VolatileContext};
use crate::procedure::repartition::plan::RegionDescriptor;
use crate::procedure::repartition::{
    Context as ParentContext, PersistentContext as ParentPersistentContext, RepartitionProcedure,
};
use crate::procedure::test_util::MailboxContext;

/// `TestingEnv` provides components during the tests.
pub struct TestingEnv {
    pub kv_backend: KvBackendRef,
    pub table_metadata_manager: TableMetadataManagerRef,
    pub mailbox_ctx: MailboxContext,
    pub server_addr: String,
}

impl Default for TestingEnv {
    fn default() -> Self {
        Self::new()
    }
}

impl TestingEnv {
    pub fn new() -> Self {
        let kv_backend = Arc::new(MemoryKvBackend::new()) as KvBackendRef;
        let table_metadata_manager = Arc::new(TableMetadataManager::new(kv_backend.clone()));
        let mailbox_sequence =
            SequenceBuilder::new("test_heartbeat_mailbox", kv_backend.clone()).build();
        let mailbox_ctx = MailboxContext::new(mailbox_sequence);

        Self {
            kv_backend,
            table_metadata_manager,
            mailbox_ctx,
            server_addr: "localhost".to_string(),
        }
    }

    pub fn create_context(&self, persistent_context: PersistentContext) -> Context {
        let cache_invalidator = Arc::new(MetasrvCacheInvalidator::new(
            self.mailbox_ctx.mailbox().clone(),
            MetasrvInfo {
                server_addr: String::new(),
            },
        ));

        Context {
            persistent_ctx: persistent_context,
            table_metadata_manager: self.table_metadata_manager.clone(),
            cache_invalidator,
            mailbox: self.mailbox_ctx.mailbox().clone(),
            server_addr: self.server_addr.clone(),
            start_time: Instant::now(),
            volatile_ctx: VolatileContext::default(),
        }
    }

    pub fn procedure_context() -> ProcedureContext {
        ProcedureContext {
            procedure_id: ProcedureId::random(),
            provider: Arc::new(MockContextProvider::default()),
        }
    }

    pub async fn create_physical_table_metadata(
        &self,
        table_id: TableId,
        region_routes: Vec<RegionRoute>,
    ) {
        self.create_physical_table_metadata_with_wal_options(
            table_id,
            region_routes,
            HashMap::default(),
        )
        .await;
    }

    pub async fn create_physical_table_metadata_with_wal_options(
        &self,
        table_id: TableId,
        region_routes: Vec<RegionRoute>,
        region_wal_options: HashMap<RegionNumber, String>,
    ) {
        self.table_metadata_manager
            .create_table_metadata(
                new_test_table_info(table_id),
                TableRouteValue::physical(region_routes),
                region_wal_options,
            )
            .await
            .unwrap();
    }

    pub async fn create_physical_table_metadata_for_repartition(
        &self,
        table_id: TableId,
        region_routes: Vec<RegionRoute>,
        region_wal_options: HashMap<RegionNumber, String>,
    ) {
        let mut table_info = new_test_table_info_with_name(table_id, "test_table");
        table_info.meta.column_ids = vec![0, 1, 2];

        self.table_metadata_manager
            .create_table_metadata(
                table_info,
                TableRouteValue::physical(region_routes),
                region_wal_options,
            )
            .await
            .unwrap();
    }

    pub fn ddl_context(&self, node_manager: NodeManagerRef) -> DdlContext {
        new_ddl_context_with_kv_backend(node_manager, self.kv_backend.clone())
    }
}

pub fn range_expr(col_name: &str, start: i64, end: i64) -> PartitionExpr {
    col(col_name)
        .gt_eq(Value::Int64(start))
        .and(col(col_name).lt(Value::Int64(end)))
}

pub fn test_region_wal_options(region_numbers: &[RegionNumber]) -> HashMap<RegionNumber, String> {
    let wal_options = serde_json::to_string(&WalOptions::Kafka(KafkaWalOptions {
        topic: "test_topic".to_string(),
    }))
    .unwrap();

    region_numbers
        .iter()
        .map(|region_number| (*region_number, wal_options.clone()))
        .collect()
}

pub fn new_persistent_context(
    table_id: TableId,
    sources: Vec<RegionDescriptor>,
    targets: Vec<RegionDescriptor>,
) -> PersistentContext {
    PersistentContext {
        group_id: Uuid::new_v4(),
        catalog_name: "test_catalog".to_string(),
        schema_name: "test_schema".to_string(),
        table_id,
        sources,
        targets,
        region_mapping: HashMap::new(),
        group_prepare_result: None,
        staging_manifest_paths: HashMap::new(),
        sync_region: false,
        allocated_region_ids: vec![],
        pending_deallocate_region_ids: vec![],
        timeout: Duration::from_secs(120),
    }
}

pub fn test_region_route(region_id: RegionId, partition_expr: &str) -> RegionRoute {
    RegionRoute {
        region: Region {
            id: region_id,
            partition_expr: partition_expr.to_string(),
            ..Default::default()
        },
        leader_peer: Some(Peer::empty(1)),
        ..Default::default()
    }
}

pub async fn current_parent_region_routes(ctx: &ParentContext) -> Vec<RegionRoute> {
    let table_route_value = ctx.get_table_route_value().await.unwrap().into_inner();
    table_route_value.region_routes().unwrap().clone()
}

pub fn new_parent_context(
    env: &TestingEnv,
    node_manager: NodeManagerRef,
    table_id: TableId,
) -> ParentContext {
    let ddl_ctx = env.ddl_context(node_manager);
    let persistent_ctx = ParentPersistentContext::new(
        TableName::new("test_catalog", "test_schema", "test_table"),
        table_id,
        None,
    );

    ParentContext::new(
        &ddl_ctx,
        env.mailbox_ctx.mailbox().clone(),
        env.server_addr.clone(),
        persistent_ctx,
    )
}

pub fn assert_parent_state<T: 'static>(procedure: &RepartitionProcedure) {
    assert!(procedure.state.as_any().is::<T>());
}

pub fn extract_subprocedure_ids(status: Status) -> Vec<ProcedureId> {
    let Status::Suspended { subprocedures, .. } = status else {
        panic!("expected suspended status");
    };

    subprocedures
        .into_iter()
        .map(|procedure| procedure.id)
        .collect()
}

pub fn procedure_state_receiver(state: ProcedureState) -> watch::Receiver<ProcedureState> {
    let (tx, rx) = watch::channel(ProcedureState::Running);
    tx.send(state).unwrap();
    rx
}

pub fn procedure_context_with_receivers(
    receivers: HashMap<ProcedureId, watch::Receiver<ProcedureState>>,
) -> ProcedureContext {
    ProcedureContext {
        procedure_id: ProcedureId::random(),
        provider: Arc::new(ProcedureStateReceiverProvider {
            receivers,
            inner: MockContextProvider::default(),
        }),
    }
}

struct ProcedureStateReceiverProvider {
    receivers: HashMap<ProcedureId, watch::Receiver<ProcedureState>>,
    inner: MockContextProvider,
}

#[async_trait::async_trait]
impl ContextProvider for ProcedureStateReceiverProvider {
    async fn procedure_state(
        &self,
        procedure_id: ProcedureId,
    ) -> common_procedure::Result<Option<ProcedureState>> {
        self.inner.procedure_state(procedure_id).await
    }

    async fn procedure_state_receiver(
        &self,
        procedure_id: ProcedureId,
    ) -> common_procedure::Result<Option<watch::Receiver<ProcedureState>>> {
        Ok(self.receivers.get(&procedure_id).cloned())
    }

    async fn try_put_poison(
        &self,
        key: &common_procedure::PoisonKey,
        procedure_id: ProcedureId,
    ) -> common_procedure::Result<()> {
        self.inner.try_put_poison(key, procedure_id).await
    }

    async fn acquire_lock(
        &self,
        key: &common_procedure::StringKey,
    ) -> common_procedure::local::DynamicKeyLockGuard {
        self.inner.acquire_lock(key).await
    }
}
