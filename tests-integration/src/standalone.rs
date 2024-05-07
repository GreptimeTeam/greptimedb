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

use catalog::kvbackend::KvBackendCatalogManager;
use cmd::options::MixOptions;
use common_base::Plugins;
use common_catalog::consts::{MIN_USER_FLOW_ID, MIN_USER_TABLE_ID};
use common_config::KvBackendConfig;
use common_meta::cache_invalidator::MultiCacheInvalidator;
use common_meta::ddl::flow_meta::FlowMetadataAllocator;
use common_meta::ddl::table_meta::TableMetadataAllocator;
use common_meta::ddl::DdlContext;
use common_meta::ddl_manager::DdlManager;
use common_meta::key::flow::FlowMetadataManager;
use common_meta::key::TableMetadataManager;
use common_meta::kv_backend::KvBackendRef;
use common_meta::region_keeper::MemoryRegionKeeper;
use common_meta::sequence::SequenceBuilder;
use common_meta::wal_options_allocator::WalOptionsAllocator;
use common_procedure::options::ProcedureConfig;
use common_procedure::ProcedureManagerRef;
use common_telemetry::logging::LoggingOptions;
use common_wal::config::{DatanodeWalConfig, MetasrvWalConfig};
use datanode::datanode::DatanodeBuilder;
use flow::FlownodeBuilder;
use frontend::frontend::FrontendOptions;
use frontend::instance::builder::FrontendBuilder;
use frontend::instance::{FrontendInstance, Instance, StandaloneDatanodeManager};
use meta_srv::metasrv::{FLOW_ID_SEQ, TABLE_ID_SEQ};
use servers::Mode;

use crate::test_util::{self, create_tmp_dir_and_datanode_opts, StorageType, TestGuard};

pub struct GreptimeDbStandalone {
    pub instance: Arc<Instance>,
    pub mix_options: MixOptions,
    pub guard: TestGuard,
    // Used in rebuild.
    pub kv_backend: KvBackendRef,
    pub procedure_manager: ProcedureManagerRef,
}

pub struct GreptimeDbStandaloneBuilder {
    instance_name: String,
    datanode_wal_config: DatanodeWalConfig,
    metasrv_wal_config: MetasrvWalConfig,
    store_providers: Option<Vec<StorageType>>,
    default_store: Option<StorageType>,
    plugin: Option<Plugins>,
}

impl GreptimeDbStandaloneBuilder {
    pub fn new(instance_name: &str) -> Self {
        Self {
            instance_name: instance_name.to_string(),
            store_providers: None,
            plugin: None,
            default_store: None,
            datanode_wal_config: DatanodeWalConfig::default(),
            metasrv_wal_config: MetasrvWalConfig::default(),
        }
    }

    #[must_use]
    pub fn with_default_store_type(self, store_type: StorageType) -> Self {
        Self {
            default_store: Some(store_type),
            ..self
        }
    }

    #[cfg(test)]
    #[must_use]
    pub fn with_store_providers(self, store_providers: Vec<StorageType>) -> Self {
        Self {
            store_providers: Some(store_providers),
            ..self
        }
    }

    #[cfg(test)]
    #[must_use]
    pub fn with_plugin(self, plugin: Plugins) -> Self {
        Self {
            plugin: Some(plugin),
            ..self
        }
    }

    #[must_use]
    pub fn with_datanode_wal_config(mut self, datanode_wal_config: DatanodeWalConfig) -> Self {
        self.datanode_wal_config = datanode_wal_config;
        self
    }

    #[must_use]
    pub fn with_metasrv_wal_config(mut self, metasrv_wal_config: MetasrvWalConfig) -> Self {
        self.metasrv_wal_config = metasrv_wal_config;
        self
    }

    pub async fn build_with(
        &self,
        kv_backend: KvBackendRef,
        guard: TestGuard,
        mix_options: MixOptions,
        procedure_manager: ProcedureManagerRef,
        register_procedure_loaders: bool,
    ) -> GreptimeDbStandalone {
        let plugins = self.plugin.clone().unwrap_or_default();

        let datanode = DatanodeBuilder::new(mix_options.datanode.clone(), plugins.clone())
            .with_kv_backend(kv_backend.clone())
            .build()
            .await
            .unwrap();

        let table_metadata_manager = Arc::new(TableMetadataManager::new(kv_backend.clone()));
        table_metadata_manager.init().await.unwrap();

        let flow_builder = FlownodeBuilder::new(
            Default::default(),
            plugins.clone(),
            table_metadata_manager.clone(),
        )
        .with_kv_backend(kv_backend.clone());
        let flownode = Arc::new(flow_builder.build());

        let flow_metadata_manager = Arc::new(FlowMetadataManager::new(kv_backend.clone()));
        let multi_cache_invalidator = Arc::new(MultiCacheInvalidator::default());
        let catalog_manager = KvBackendCatalogManager::new(
            Mode::Standalone,
            None,
            kv_backend.clone(),
            multi_cache_invalidator.clone(),
        )
        .await;

        let node_manager = Arc::new(StandaloneDatanodeManager {
            region_server: datanode.region_server(),
            flow_server: flownode.clone(),
        });

        let table_id_sequence = Arc::new(
            SequenceBuilder::new(TABLE_ID_SEQ, kv_backend.clone())
                .initial(MIN_USER_TABLE_ID as u64)
                .step(10)
                .build(),
        );
        let flow_id_sequence = Arc::new(
            SequenceBuilder::new(FLOW_ID_SEQ, kv_backend.clone())
                .initial(MIN_USER_FLOW_ID as u64)
                .step(10)
                .build(),
        );
        let wal_options_allocator = Arc::new(WalOptionsAllocator::new(
            mix_options.wal_meta.clone(),
            kv_backend.clone(),
        ));
        let table_metadata_allocator = Arc::new(TableMetadataAllocator::new(
            table_id_sequence,
            wal_options_allocator.clone(),
        ));
        let flow_metadata_allocator = Arc::new(FlowMetadataAllocator::with_noop_peer_allocator(
            flow_id_sequence,
        ));

        let ddl_task_executor = Arc::new(
            DdlManager::try_new(
                DdlContext {
                    node_manager: node_manager.clone(),
                    cache_invalidator: multi_cache_invalidator,
                    memory_region_keeper: Arc::new(MemoryRegionKeeper::default()),
                    table_metadata_manager,
                    table_metadata_allocator,
                    flow_metadata_manager,
                    flow_metadata_allocator,
                },
                procedure_manager.clone(),
                register_procedure_loaders,
            )
            .unwrap(),
        );

        let instance = FrontendBuilder::new(
            kv_backend.clone(),
            catalog_manager,
            node_manager,
            ddl_task_executor,
        )
        .with_plugin(plugins)
        .try_build()
        .await
        .unwrap();

        flownode
            .set_frontend_invoker(Box::new(instance.clone()))
            .await;

        procedure_manager.start().await.unwrap();
        wal_options_allocator.start().await.unwrap();

        test_util::prepare_another_catalog_and_schema(&instance).await;

        instance.start().await.unwrap();

        GreptimeDbStandalone {
            instance: Arc::new(instance),
            mix_options,
            guard,
            kv_backend,
            procedure_manager,
        }
    }

    pub async fn build(&self) -> GreptimeDbStandalone {
        let default_store_type = self.default_store.unwrap_or(StorageType::File);
        let store_types = self.store_providers.clone().unwrap_or_default();

        let (opts, guard) = create_tmp_dir_and_datanode_opts(
            Mode::Standalone,
            default_store_type,
            store_types,
            &self.instance_name,
            self.datanode_wal_config.clone(),
        );

        let kv_backend_config = KvBackendConfig::default();
        let procedure_config = ProcedureConfig::default();
        let (kv_backend, procedure_manager) = Instance::try_build_standalone_components(
            format!("{}/kv", &opts.storage.data_home),
            kv_backend_config.clone(),
            procedure_config.clone(),
        )
        .await
        .unwrap();

        let wal_meta = self.metasrv_wal_config.clone();
        let mix_options = MixOptions {
            data_home: opts.storage.data_home.to_string(),
            procedure: procedure_config,
            metadata_store: kv_backend_config,
            frontend: FrontendOptions::default(),
            datanode: opts,
            logging: LoggingOptions::default(),
            wal_meta,
        };

        self.build_with(kv_backend, guard, mix_options, procedure_manager, true)
            .await
    }
}
