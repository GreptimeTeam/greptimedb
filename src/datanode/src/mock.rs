// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use catalog::remote::MetaKvBackend;
use common_catalog::consts::MIN_USER_TABLE_ID;
use meta_client::client::{MetaClient, MetaClientBuilder};
use meta_srv::mocks::MockInfo;
use query::QueryEngineFactory;
use storage::config::EngineConfig as StorageEngineConfig;
use storage::EngineImpl;
use table::metadata::TableId;
use table::table::{TableIdProvider, TableIdProviderRef};
use table_engine::config::EngineConfig as TableEngineConfig;

use crate::datanode::DatanodeOptions;
use crate::error::Result;
use crate::heartbeat::HeartbeatTask;
use crate::instance::{create_local_file_log_store, new_object_store, DefaultEngine, Instance};
use crate::script::ScriptExecutor;
use crate::server::grpc::plan::PhysicalPlanner;
use crate::sql::SqlHandler;

impl Instance {
    // This method is used in other crate's testing codes, so move it out of "cfg(test)".
    // TODO(LFC): Delete it when callers no longer need it.
    pub async fn new_mock() -> Result<Self> {
        use table_engine::table::test_util::{new_test_object_store, MockEngine, MockMitoEngine};

        let mock_info = meta_srv::mocks::mock_with_memstore().await;
        let meta_client = Some(Arc::new(mock_meta_client(mock_info, 0).await));
        let (_dir, object_store) = new_test_object_store("setup_mock_engine_and_table").await;
        let mock_engine = Arc::new(MockMitoEngine::new(
            TableEngineConfig::default(),
            MockEngine::default(),
            object_store,
        ));

        let catalog_manager = Arc::new(
            catalog::local::manager::LocalCatalogManager::try_new(mock_engine.clone())
                .await
                .unwrap(),
        );

        let factory = QueryEngineFactory::new(catalog_manager.clone());
        let query_engine = factory.query_engine();

        let sql_handler = SqlHandler::new(mock_engine.clone(), catalog_manager.clone());
        let physical_planner = PhysicalPlanner::new(query_engine.clone());
        let script_executor = ScriptExecutor::new(catalog_manager.clone(), query_engine.clone())
            .await
            .unwrap();

        let heartbeat_task = Some(HeartbeatTask::new(
            0,
            "127.0.0.1:3302".to_string(),
            meta_client.as_ref().unwrap().clone(),
        ));

        let table_id_provider = Some(catalog_manager.clone() as TableIdProviderRef);
        Ok(Self {
            query_engine,
            sql_handler,
            catalog_manager,
            physical_planner,
            script_executor,
            meta_client,
            heartbeat_task,
            table_id_provider,
        })
    }

    pub async fn with_mock_meta_client(opts: &DatanodeOptions) -> Result<Self> {
        let mock_info = meta_srv::mocks::mock_with_memstore().await;
        Self::with_mock_meta_server(opts, mock_info).await
    }

    pub async fn with_mock_meta_server(opts: &DatanodeOptions, meta_srv: MockInfo) -> Result<Self> {
        let object_store = new_object_store(&opts.storage).await?;
        let log_store = create_local_file_log_store(opts).await?;
        let meta_client = Arc::new(mock_meta_client(meta_srv, opts.node_id).await);
        let table_engine = Arc::new(DefaultEngine::new(
            TableEngineConfig::default(),
            EngineImpl::new(
                StorageEngineConfig::default(),
                Arc::new(log_store),
                object_store.clone(),
            ),
            object_store,
        ));

        // create remote catalog manager
        let catalog_manager = Arc::new(catalog::remote::RemoteCatalogManager::new(
            table_engine.clone(),
            opts.node_id,
            Arc::new(MetaKvBackend {
                client: meta_client.clone(),
            }),
        ));

        let factory = QueryEngineFactory::new(catalog_manager.clone());
        let query_engine = factory.query_engine();
        let script_executor =
            ScriptExecutor::new(catalog_manager.clone(), query_engine.clone()).await?;

        let heartbeat_task =
            HeartbeatTask::new(opts.node_id, opts.rpc_addr.clone(), meta_client.clone());
        Ok(Self {
            query_engine: query_engine.clone(),
            sql_handler: SqlHandler::new(table_engine, catalog_manager.clone()),
            catalog_manager,
            physical_planner: PhysicalPlanner::new(query_engine),
            script_executor,
            table_id_provider: Some(Arc::new(LocalTableIdProvider::default())),
            meta_client: Some(meta_client),
            heartbeat_task: Some(heartbeat_task),
        })
    }
}

struct LocalTableIdProvider {
    inner: Arc<AtomicU32>,
}

impl Default for LocalTableIdProvider {
    fn default() -> Self {
        Self {
            inner: Arc::new(AtomicU32::new(MIN_USER_TABLE_ID)),
        }
    }
}

#[async_trait::async_trait]
impl TableIdProvider for LocalTableIdProvider {
    async fn next_table_id(&self) -> table::Result<TableId> {
        Ok(self.inner.fetch_add(1, Ordering::Relaxed))
    }
}

async fn mock_meta_client(mock_info: MockInfo, node_id: u64) -> MetaClient {
    let MockInfo {
        server_addr,
        channel_manager,
    } = mock_info;

    let id = (1000u64, 2000u64);
    let mut meta_client = MetaClientBuilder::new(id.0, node_id)
        .enable_heartbeat()
        .enable_router()
        .enable_store()
        .channel_manager(channel_manager)
        .build();
    meta_client.start(&[&server_addr]).await.unwrap();
    // // required only when the heartbeat_client is enabled
    meta_client.ask_leader().await.unwrap();

    meta_client
}
