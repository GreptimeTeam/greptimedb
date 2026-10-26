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
use std::sync::atomic::AtomicBool;

use auth::PermissionCheckerRef;
use cache::{PARTITION_INFO_CACHE_NAME, TABLE_FLOWNODE_SET_CACHE_NAME, TABLE_ROUTE_CACHE_NAME};
use catalog::CatalogManagerRef;
use catalog::kvbackend::KvBackendCatalogManager;
use catalog::process_manager::ProcessManagerRef;
use catalog::system_schema::semantic_graph::EntityGraphProviderRef;
use common_base::Plugins;
use common_batcher::request_limiter::RequestLimiter;
use common_datasource::object_store::LocalFileAccess;
use common_event_recorder::{EventRecorderImpl, EventRecorderRef};
use common_meta::cache::{LayeredCacheRegistryRef, TableFlownodeSetCacheRef, TableRouteCacheRef};
use common_meta::cache_invalidator::CacheInvalidatorRef;
use common_meta::key::TableMetadataManager;
use common_meta::key::flow::FlowMetadataManager;
use common_meta::kv_backend::KvBackendRef;
use common_meta::node_manager::NodeManagerRef;
use common_meta::procedure_executor::ProcedureExecutorRef;
use dashmap::DashMap;
use operator::batcher::PendingRowsBatcher;
use operator::delete::Deleter;
use operator::flow::FlowServiceOperator;
use operator::insert::Inserter;
use operator::procedure::ProcedureServiceOperator;
use operator::request::Requester;
#[cfg(feature = "enterprise")]
use operator::statement::CreateDatabaseHandlerRef;
use operator::statement::{
    AdminEventRecorderHandle, AdminFunctionRecordingLayer, ExecutorConfigureContext,
    StatementExecutor, StatementExecutorConfiguratorRef, StatementExecutorRef,
};
use operator::table::TableMutationOperator;
use operator::write_dispatcher::WriteDispatcher;
use partition::cache::PartitionInfoCacheRef;
use partition::manager::PartitionRuleManager;
use pipeline::pipeline_operator::PipelineOperator;
use query::QueryEngineFactory;
use query::region_query::RegionQueryHandlerFactoryRef;
use servers::batcher::table::TablePendingRowsBatcher;
use servers::batcher::{BatchingProtocol, pending_rows_batch_sync_enabled};
use snafu::{OptionExt, ResultExt};

use crate::error::{self, DataFusionSnafu, ExternalSnafu, Result};
use crate::events::EventHandlerImpl;
use crate::frontend::FrontendOptions;
use crate::heartbeat::frontend_peer_addr;
use crate::instance::Instance;
use crate::instance::entity_graph::EntityGraphProviderImpl;
use crate::instance::otlp::TraceAuxCache;
use crate::instance::region_query::FrontendRegionQueryHandler;
use crate::service_config::BatcherOptions;

/// The frontend [`Instance`] builder.
pub struct FrontendBuilder {
    options: FrontendOptions,
    kv_backend: KvBackendRef,
    layered_cache_registry: LayeredCacheRegistryRef,
    local_cache_invalidator: CacheInvalidatorRef,
    catalog_manager: CatalogManagerRef,
    node_manager: NodeManagerRef,
    plugins: Option<Plugins>,
    procedure_executor: ProcedureExecutorRef,
    process_manager: ProcessManagerRef,
    local_file_access: LocalFileAccess,
}

impl FrontendBuilder {
    /// Creates a frontend builder from its configuration and shared services.
    ///
    /// Uses the layered cache registry for local invalidation unless overridden.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        options: FrontendOptions,
        kv_backend: KvBackendRef,
        layered_cache_registry: LayeredCacheRegistryRef,
        catalog_manager: CatalogManagerRef,
        node_manager: NodeManagerRef,
        procedure_executor: ProcedureExecutorRef,
        process_manager: ProcessManagerRef,
    ) -> Self {
        Self {
            options,
            kv_backend,
            local_cache_invalidator: layered_cache_registry.clone(),
            layered_cache_registry,
            catalog_manager,
            node_manager,
            plugins: None,
            procedure_executor,
            process_manager,
            local_file_access: LocalFileAccess::Disabled,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_test(
        options: &FrontendOptions,
        meta_client: meta_client::MetaClientRef,
    ) -> Self {
        use cache::{build_fundamental_cache_registry, with_default_composite_cache_registry};
        use common_meta::cache::LayeredCacheRegistryBuilder;
        use common_meta::kv_backend::memory::MemoryKvBackend;

        let kv_backend = Arc::new(MemoryKvBackend::new());

        // Builds cache registry
        let layered_cache_builder = LayeredCacheRegistryBuilder::default();
        let fundamental_cache_registry = build_fundamental_cache_registry(kv_backend.clone());
        let layered_cache_registry = Arc::new(
            with_default_composite_cache_registry(
                layered_cache_builder.add_cache_registry(fundamental_cache_registry),
            )
            .unwrap()
            .build(),
        );

        Self::new(
            options.clone(),
            kv_backend,
            layered_cache_registry,
            catalog::memory::MemoryCatalogManager::with_default_setup(),
            Arc::new(client::client_manager::NodeClients::default()),
            meta_client,
            Arc::new(catalog::process_manager::ProcessManager::new(
                "".to_string(),
                None,
            )),
        )
    }

    /// Overrides the default local cache invalidator.
    pub fn with_local_cache_invalidator(self, cache_invalidator: CacheInvalidatorRef) -> Self {
        Self {
            local_cache_invalidator: cache_invalidator,
            ..self
        }
    }

    pub fn with_local_file_access(mut self, local_file_access: LocalFileAccess) -> Self {
        self.local_file_access = local_file_access;
        self
    }

    pub fn options(&self) -> &FrontendOptions {
        &self.options
    }

    pub fn kv_backend(&self) -> &KvBackendRef {
        &self.kv_backend
    }

    pub fn layered_cache_registry(&self) -> &LayeredCacheRegistryRef {
        &self.layered_cache_registry
    }

    pub fn catalog_manager(&self) -> &CatalogManagerRef {
        &self.catalog_manager
    }

    pub fn node_manager(&self) -> &NodeManagerRef {
        &self.node_manager
    }

    pub fn procedure_executor(&self) -> &ProcedureExecutorRef {
        &self.procedure_executor
    }

    pub fn process_manager(&self) -> &ProcessManagerRef {
        &self.process_manager
    }

    pub fn with_plugin(self, plugins: Plugins) -> Self {
        Self {
            plugins: Some(plugins),
            ..self
        }
    }

    /// Validates configuration and assembles the frontend [`Instance`].
    pub async fn try_build(self) -> Result<Instance> {
        if let Err(reason) = self.options.write_retry.validate() {
            return error::InvalidWriteRetryConfigSnafu { reason }.fail();
        }
        let kv_backend = self.kv_backend;
        let node_manager = self.node_manager;
        let plugins = self.plugins.unwrap_or_default();
        let process_manager = self.process_manager;
        let table_route_cache: TableRouteCacheRef =
            self.layered_cache_registry
                .get()
                .context(error::CacheRequiredSnafu {
                    name: TABLE_ROUTE_CACHE_NAME,
                })?;
        let partition_info_cache: PartitionInfoCacheRef = self
            .layered_cache_registry
            .get()
            .context(error::CacheRequiredSnafu {
                name: PARTITION_INFO_CACHE_NAME,
            })?;
        let partition_manager = Arc::new(PartitionRuleManager::new(
            kv_backend.clone(),
            table_route_cache.clone(),
            partition_info_cache.clone(),
        ));

        let local_cache_invalidator = self.local_cache_invalidator;

        let mut write_dispatcher =
            WriteDispatcher::new(node_manager.clone(), partition_manager.clone());
        if self.options.write_retry.max_retries > 0 {
            write_dispatcher = write_dispatcher.with_retry(
                self.options.write_retry.clone(),
                self.catalog_manager.clone(),
                local_cache_invalidator.clone(),
            );
        }
        let write_dispatcher = Arc::new(write_dispatcher);

        let region_query_handler =
            if let Some(factory) = plugins.get::<RegionQueryHandlerFactoryRef>() {
                factory.build(partition_manager.clone(), node_manager.clone())
            } else {
                FrontendRegionQueryHandler::arc(partition_manager.clone(), node_manager.clone())
            };

        let table_flownode_cache: TableFlownodeSetCacheRef = self
            .layered_cache_registry
            .get()
            .context(error::CacheRequiredSnafu {
                name: TABLE_FLOWNODE_SET_CACHE_NAME,
            })?;

        let create_inserter = || {
            Inserter::new(
                self.catalog_manager.clone(),
                partition_manager.clone(),
                node_manager.clone(),
                table_flownode_cache.clone(),
                self.options.auto_create_table,
            )
            .with_write_dispatcher(write_dispatcher.clone())
        };
        // The execution-only inserter owns no batchers, avoiding an Arc cycle.
        let bulk_inserter = Arc::new(create_inserter());
        let build_batcher = |options: &BatcherOptions| -> Option<Arc<dyn PendingRowsBatcher>> {
            if !options.pending_rows_batching_enabled()
                || (self.options.prom_store.with_metric_engine
                    && options
                        .protocols
                        .iter()
                        .all(|protocol| *protocol == BatchingProtocol::Prom))
            {
                return None;
            }
            TablePendingRowsBatcher::try_new(
                options.pending_rows_flush_interval,
                options.max_batch_rows,
                options.max_concurrent_flushes,
                options.worker_channel_capacity,
                options.max_inflight_requests,
                options.flow_notification_queue_capacity,
                bulk_inserter.clone(),
            )
            .map(|batcher| batcher as Arc<dyn PendingRowsBatcher>)
        };
        let inserter = Arc::new(
            create_inserter()
                .with_pending_rows_batcher(build_batcher(self.options.table_batcher_options())),
        );
        let deleter = Arc::new(
            Deleter::new(
                self.catalog_manager.clone(),
                partition_manager.clone(),
                node_manager.clone(),
            )
            .with_write_dispatcher(write_dispatcher),
        );
        let requester = Arc::new(Requester::new(
            self.catalog_manager.clone(),
            partition_manager.clone(),
            node_manager.clone(),
        ));
        let table_mutation_handler = Arc::new(TableMutationOperator::new(
            inserter.clone(),
            deleter.clone(),
            requester,
        ));

        let table_metadata_manager = Arc::new(TableMetadataManager::new(kv_backend.clone()));
        let procedure_service_handler = Arc::new(ProcedureServiceOperator::new(
            self.procedure_executor.clone(),
            self.catalog_manager.clone(),
            table_metadata_manager.clone(),
        ));

        let flow_metadata_manager: Arc<FlowMetadataManager> =
            Arc::new(FlowMetadataManager::new(kv_backend.clone()));
        let flow_service = FlowServiceOperator::new(flow_metadata_manager, node_manager.clone());

        let mut query_options = self.options.query.clone();
        query_options.enable_per_region_metrics = self.options.logging.enable_per_region_metrics;
        let query_engine = QueryEngineFactory::try_new_with_plugins(
            self.catalog_manager.clone(),
            Some(partition_manager.clone()),
            Some(region_query_handler.clone()),
            Some(table_mutation_handler),
            Some(procedure_service_handler),
            Some(Arc::new(flow_service)),
            true,
            plugins.clone(),
            query_options,
        )
        .context(DataFusionSnafu)?
        .query_engine();

        // Inject the entity-graph provider now that the query engine exists, so the
        // computed `greptime_private.semantic_entities` / `semantic_relationships`
        // tables can derive rows. Late binding here breaks the `catalog -> query`
        // dependency cycle.
        if let Some(kv_catalog) = self
            .catalog_manager
            .as_any()
            .downcast_ref::<KvBackendCatalogManager>()
        {
            let provider: EntityGraphProviderRef = Arc::new(EntityGraphProviderImpl::new(
                query_engine.clone(),
                Arc::downgrade(&self.catalog_manager),
                plugins.get::<PermissionCheckerRef>(),
            ));
            kv_catalog.set_entity_graph_provider(provider);
        }

        let frontend_peer_addr = frontend_peer_addr(&self.options);
        let statement_executor = StatementExecutor::new(
            self.catalog_manager.clone(),
            query_engine.clone(),
            self.procedure_executor,
            kv_backend.clone(),
            local_cache_invalidator,
            inserter.clone(),
            partition_manager,
            Some(process_manager.clone()),
            frontend_peer_addr.clone(),
            self.local_file_access,
        );

        let statement_executor =
            if let Some(configurator) = plugins.get::<StatementExecutorConfiguratorRef>() {
                let ctx = ExecutorConfigureContext {
                    kv_backend: kv_backend.clone(),
                };
                configurator
                    .configure(statement_executor, ctx)
                    .await
                    .context(ExternalSnafu)?
            } else {
                statement_executor
            };

        #[cfg(feature = "enterprise")]
        let statement_executor = if let Some(handler) = plugins.get::<CreateDatabaseHandlerRef>() {
            statement_executor.with_create_database_handler(handler)
        } else {
            statement_executor
        };

        let admin_event_recorder = AdminEventRecorderHandle::default();
        let statement_executor = statement_executor.with_admin_function_layer(Arc::new(
            AdminFunctionRecordingLayer::new(admin_event_recorder.clone()),
        ));
        let statement_executor = Arc::new(statement_executor);

        let pipeline_operator = Arc::new(PipelineOperator::new(
            inserter.clone(),
            statement_executor.clone(),
            self.catalog_manager.clone(),
            query_engine.clone(),
            &self.options.pipeline,
        ));

        plugins.insert::<StatementExecutorRef>(statement_executor.clone());

        let slow_query_recorder = Arc::new(EventRecorderImpl::new(Box::new(
            EventHandlerImpl::new(statement_executor.clone(), self.options.slow_query.ttl),
        )));
        let event_recorder: EventRecorderRef = Arc::new(EventRecorderImpl::with_event_type_filter(
            Box::new(EventHandlerImpl::new(
                statement_executor.clone(),
                self.options.event_recorder.ttl,
            )),
            self.options.event_recorder.event_types.clone(),
            self.options.event_recorder.flush_interval,
        ));
        admin_event_recorder.install(&event_recorder);

        let batcher_options = self.options.table_batcher_options();
        let trace_aux_limiter = if batcher_options.pending_rows_batching_enabled()
            && !pending_rows_batch_sync_enabled()
        {
            RequestLimiter::try_new(batcher_options.max_inflight_requests)
        } else {
            None
        };

        Ok(Instance {
            logical_batcher: Default::default(),
            frontend_peer_addr,
            catalog_manager: self.catalog_manager,
            pipeline_operator,
            statement_executor,
            query_engine,
            plugins,
            inserter,
            deleter,
            table_metadata_manager,
            event_recorder,
            slow_query_recorder,
            process_manager,
            otlp_metrics_table_legacy_cache: Arc::new(DashMap::new()),
            slow_query_options: self.options.slow_query.clone(),
            influxdb_default_merge_mode: self.options.influxdb.default_merge_mode,
            trace_ingest_chunk_size: self.options.otlp.trace_ingest_chunk_size,
            trace_aux_cache: TraceAuxCache::new(self.options.otlp.trace_aux_cache_size.as_bytes()),
            trace_aux_limiter,
            otlp_resource_info: self.options.otlp.experimental_enable_resource_info,
            suspend: Arc::new(AtomicBool::new(false)),
        })
    }
}

#[cfg(test)]
mod tests {
    use api::v1::meta::Role;
    use common_meta::cache_invalidator::{Context, DummyCacheInvalidator};
    use common_meta::instruction::CacheIdent;
    use common_meta::key::table_route::{TableRouteKey, TableRouteValue};
    use common_meta::key::{MetadataKey, MetadataValue};
    use common_meta::rpc::router::{Region, RegionRoute};
    use common_meta::rpc::store::PutRequest;
    use meta_client::client::MetaClientBuilder;
    use store_api::storage::RegionId;

    use super::*;

    #[tokio::test]
    async fn test_local_cache_invalidator() {
        for custom_invalidator in [false, true] {
            let mut builder = FrontendBuilder::new_test(
                &FrontendOptions::default(),
                Arc::new(MetaClientBuilder::new(0, Role::Frontend).build()),
            );
            if custom_invalidator {
                builder = builder.with_local_cache_invalidator(Arc::new(DummyCacheInvalidator));
            }
            let backend = builder.kv_backend.clone();
            let routes: TableRouteCacheRef = builder.layered_cache_registry.get().unwrap();
            let partitions: PartitionInfoCacheRef = builder.layered_cache_registry.get().unwrap();
            let instance = builder.try_build().await.unwrap();
            let table_id = 1024;
            for region_number in [1, 2] {
                let route = TableRouteValue::physical(vec![RegionRoute {
                    region: Region {
                        id: RegionId::new(table_id, region_number),
                        ..Default::default()
                    },
                    ..Default::default()
                }]);
                backend
                    .put(
                        PutRequest::new()
                            .with_key(TableRouteKey::new(table_id).to_bytes())
                            .with_value(route.try_as_raw_value().unwrap()),
                    )
                    .await
                    .unwrap();
                if region_number == 1 {
                    routes.get(table_id).await.unwrap().unwrap();
                    partitions.get(table_id).await.unwrap().unwrap();
                }
            }
            assert!(routes.contains_key(&table_id));
            assert!(partitions.contains_key(&table_id));

            instance
                .cache_invalidator()
                .invalidate(&Context::default(), &[CacheIdent::TableId(table_id)])
                .await
                .unwrap();

            assert_eq!(custom_invalidator, routes.contains_key(&table_id));
            assert_eq!(custom_invalidator, partitions.contains_key(&table_id));
            let route = routes.get(table_id).await.unwrap().unwrap();
            let region = route.as_physical_table_route_ref().unwrap().region_routes[0]
                .region
                .id;
            assert_eq!(
                if custom_invalidator { 1 } else { 2 },
                region.region_number()
            );
        }
    }
}
