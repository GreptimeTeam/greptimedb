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

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use catalog::remote::MetaKvBackend;
use catalog::CatalogManagerRef;
use common_catalog::consts::MIN_USER_TABLE_ID;
use common_wrcu::WrcuStat;
use meta_client::client::{MetaClient, MetaClientBuilder};
use meta_srv::mocks::MockInfo;
use mito::config::EngineConfig as TableEngineConfig;
use query::QueryEngineFactory;
use servers::Mode;
use snafu::ResultExt;
use storage::compaction::noop::NoopCompactionScheduler;
use storage::config::EngineConfig as StorageEngineConfig;
use storage::EngineImpl;
use table::metadata::TableId;
use table::table::TableIdProvider;

use crate::datanode::DatanodeOptions;
use crate::error::{CatalogSnafu, RecoverProcedureSnafu, Result};
use crate::heartbeat::HeartbeatTask;
use crate::instance::{
    create_log_store, create_procedure_manager, new_object_store, DefaultEngine, Instance,
};
use crate::script::ScriptExecutor;
use crate::sql::SqlHandler;

impl Instance {
    pub async fn with_mock_meta_client(opts: &DatanodeOptions) -> Result<Self> {
        let mock_info = meta_srv::mocks::mock_with_memstore().await;
        Self::with_mock_meta_server(opts, mock_info).await
    }

    pub async fn with_mock_meta_server(opts: &DatanodeOptions, meta_srv: MockInfo) -> Result<Self> {
        let object_store = new_object_store(&opts.storage).await?;
        let logstore = Arc::new(create_log_store(&opts.wal).await?);
        let meta_client = Arc::new(mock_meta_client(meta_srv, opts.node_id.unwrap_or(42)).await);
        let compaction_scheduler = Arc::new(NoopCompactionScheduler::default());
        let table_engine = Arc::new(DefaultEngine::new(
            TableEngineConfig::default(),
            EngineImpl::new(
                StorageEngineConfig::default(),
                logstore.clone(),
                object_store.clone(),
                compaction_scheduler,
            ),
            object_store,
        ));

        // By default, catalog manager and factory are created in standalone mode
        let (catalog_manager, factory, heartbeat_task) = match opts.mode {
            Mode::Standalone => {
                let catalog = Arc::new(
                    catalog::local::LocalCatalogManager::try_new(table_engine.clone())
                        .await
                        .context(CatalogSnafu)?,
                );
                let factory = QueryEngineFactory::new(catalog.clone());
                (catalog as CatalogManagerRef, factory, None)
            }
            Mode::Distributed => {
                let catalog = Arc::new(catalog::remote::RemoteCatalogManager::new(
                    table_engine.clone(),
                    opts.node_id.unwrap_or(42),
                    Arc::new(MetaKvBackend {
                        client: meta_client.clone(),
                    }),
                ));
                let factory = QueryEngineFactory::new(catalog.clone());
                let heartbeat_task = HeartbeatTask::new(
                    opts.node_id.unwrap_or(42),
                    opts.rpc_addr.clone(),
                    None,
                    meta_client.clone(),
                    catalog.clone(),
                    WrcuStat::default(),
                );
                (catalog as CatalogManagerRef, factory, Some(heartbeat_task))
            }
        };
        let query_engine = factory.query_engine();
        let script_executor =
            ScriptExecutor::new(catalog_manager.clone(), query_engine.clone()).await?;

        let procedure_manager = create_procedure_manager(&opts.procedure).await?;
        if let Some(procedure_manager) = &procedure_manager {
            table_engine.register_procedure_loaders(&**procedure_manager);
            // Recover procedures.
            procedure_manager
                .recover()
                .await
                .context(RecoverProcedureSnafu)?;
        }

        Ok(Self {
            query_engine: query_engine.clone(),
            sql_handler: SqlHandler::new(
                table_engine.clone(),
                catalog_manager.clone(),
                query_engine.clone(),
                table_engine,
                procedure_manager,
            ),
            catalog_manager,
            script_executor,
            table_id_provider: Some(Arc::new(LocalTableIdProvider::default())),
            heartbeat_task,
            wrcu_stat: None,
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
