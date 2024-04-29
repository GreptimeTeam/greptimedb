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

use std::sync::Arc;

use catalog::CatalogManagerRef;
use common_base::Plugins;
use common_meta::cache_invalidator::{CacheInvalidatorRef, DummyCacheInvalidator};
use common_meta::ddl::ProcedureExecutorRef;
use common_meta::key::TableMetadataManager;
use common_meta::kv_backend::KvBackendRef;
use common_meta::node_manager::NodeManagerRef;
use operator::delete::Deleter;
use operator::insert::Inserter;
use operator::procedure::ProcedureServiceOperator;
use operator::request::Requester;
use operator::statement::StatementExecutor;
use operator::table::TableMutationOperator;
use partition::manager::PartitionRuleManager;
use query::QueryEngineFactory;
use servers::server::ServerHandlers;

use crate::error::Result;
use crate::heartbeat::HeartbeatTask;
use crate::instance::region_query::FrontendRegionQueryHandler;
use crate::instance::{Instance, StatementExecutorRef};
use crate::script::ScriptExecutor;

/// The frontend [`Instance`] builder.
pub struct FrontendBuilder {
    kv_backend: KvBackendRef,
    cache_invalidator: Option<CacheInvalidatorRef>,
    catalog_manager: CatalogManagerRef,
    node_manager: NodeManagerRef,
    plugins: Option<Plugins>,
    procedure_executor: ProcedureExecutorRef,
    heartbeat_task: Option<HeartbeatTask>,
}

impl FrontendBuilder {
    pub fn new(
        kv_backend: KvBackendRef,
        catalog_manager: CatalogManagerRef,
        node_manager: NodeManagerRef,
        procedure_executor: ProcedureExecutorRef,
    ) -> Self {
        Self {
            kv_backend,
            cache_invalidator: None,
            catalog_manager,
            node_manager,
            plugins: None,
            procedure_executor,
            heartbeat_task: None,
        }
    }

    pub fn with_cache_invalidator(self, cache_invalidator: CacheInvalidatorRef) -> Self {
        Self {
            cache_invalidator: Some(cache_invalidator),
            ..self
        }
    }

    pub fn with_plugin(self, plugins: Plugins) -> Self {
        Self {
            plugins: Some(plugins),
            ..self
        }
    }

    pub fn with_heartbeat_task(self, heartbeat_task: HeartbeatTask) -> Self {
        Self {
            heartbeat_task: Some(heartbeat_task),
            ..self
        }
    }

    pub async fn try_build(self) -> Result<Instance> {
        let kv_backend = self.kv_backend;
        let node_manager = self.node_manager;
        let plugins = self.plugins.unwrap_or_default();

        let partition_manager = Arc::new(PartitionRuleManager::new(kv_backend.clone()));

        let cache_invalidator = self
            .cache_invalidator
            .unwrap_or_else(|| Arc::new(DummyCacheInvalidator));

        let region_query_handler =
            FrontendRegionQueryHandler::arc(partition_manager.clone(), node_manager.clone());

        let inserter = Arc::new(Inserter::new(
            self.catalog_manager.clone(),
            partition_manager.clone(),
            node_manager.clone(),
        ));
        let deleter = Arc::new(Deleter::new(
            self.catalog_manager.clone(),
            partition_manager.clone(),
            node_manager.clone(),
        ));
        let requester = Arc::new(Requester::new(
            self.catalog_manager.clone(),
            partition_manager,
            node_manager.clone(),
        ));
        let table_mutation_handler = Arc::new(TableMutationOperator::new(
            inserter.clone(),
            deleter.clone(),
            requester,
        ));

        let procedure_service_handler = Arc::new(ProcedureServiceOperator::new(
            self.procedure_executor.clone(),
        ));

        let query_engine = QueryEngineFactory::new_with_plugins(
            self.catalog_manager.clone(),
            Some(region_query_handler.clone()),
            Some(table_mutation_handler),
            Some(procedure_service_handler),
            true,
            plugins.clone(),
        )
        .query_engine();

        let script_executor = Arc::new(
            ScriptExecutor::new(self.catalog_manager.clone(), query_engine.clone()).await?,
        );

        let statement_executor = Arc::new(StatementExecutor::new(
            self.catalog_manager.clone(),
            query_engine.clone(),
            self.procedure_executor,
            kv_backend.clone(),
            cache_invalidator,
            inserter.clone(),
        ));

        plugins.insert::<StatementExecutorRef>(statement_executor.clone());

        Ok(Instance {
            catalog_manager: self.catalog_manager,
            script_executor,
            statement_executor,
            query_engine,
            plugins,
            servers: ServerHandlers::default(),
            heartbeat_task: self.heartbeat_task,
            inserter,
            deleter,
            export_metrics_task: None,
            table_metadata_manager: Arc::new(TableMetadataManager::new(kv_backend)),
        })
    }
}
