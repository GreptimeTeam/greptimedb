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

use cmd::options::MixOptions;
use common_base::Plugins;
use common_catalog::consts::MIN_USER_TABLE_ID;
use common_config::KvBackendConfig;
use common_meta::cache_invalidator::DummyCacheInvalidator;
use common_meta::ddl::table_meta::TableMetadataAllocator;
use common_meta::ddl_manager::DdlManager;
use common_meta::key::TableMetadataManager;
use common_meta::kv_backend::KvBackendRef;
use common_meta::region_keeper::MemoryRegionKeeper;
use common_meta::sequence::SequenceBuilder;
use common_meta::wal_options_allocator::WalOptionsAllocator;
use common_procedure::options::ProcedureConfig;
use common_procedure::ProcedureManagerRef;
use common_telemetry::logging::LoggingOptions;
use common_wal::config::{DatanodeWalConfig, MetaSrvWalConfig};
use datanode::config::DatanodeOptions;
use datanode::datanode::DatanodeBuilder;
use frontend::frontend::FrontendOptions;
use frontend::instance::builder::FrontendBuilder;
use frontend::instance::{FrontendInstance, Instance, StandaloneDatanodeManager};
use servers::Mode;

use crate::test_util::{self, create_tmp_dir_and_datanode_opts, StorageType, TestGuard};

pub struct GreptimeDbStandalone {
    pub instance: Arc<Instance>,
    pub datanode_opts: DatanodeOptions,
    pub mix_opts: MixOptions,
    pub guard: TestGuard,
}

/// Static state that will be preserved across rebuilds.
#[derive(Clone)]
struct StaticState {
    kv_backend: KvBackendRef,
    guard: TestGuard,
    // The following state are actually no need to be preserved across rebuilds.
    procedure_manager: ProcedureManagerRef,
    datanode_opts: DatanodeOptions,
}

#[derive(Clone)]
pub struct GreptimeDbStandaloneBuilder {
    instance_name: String,
    datanode_wal_config: DatanodeWalConfig,
    metasrv_wal_config: MetaSrvWalConfig,
    kv_backend_config: KvBackendConfig,
    procedure_config: ProcedureConfig,
    store_providers: Option<Vec<StorageType>>,
    default_store: Option<StorageType>,
    plugin: Option<Plugins>,
    // The builder retrieves and reuses the static state if the `static_state` is not None.
    static_state: Option<StaticState>,
}

impl GreptimeDbStandaloneBuilder {
    pub fn new(instance_name: &str) -> Self {
        Self {
            instance_name: instance_name.to_string(),
            store_providers: None,
            plugin: None,
            default_store: None,
            static_state: None,
            datanode_wal_config: DatanodeWalConfig::default(),
            metasrv_wal_config: MetaSrvWalConfig::default(),
            kv_backend_config: KvBackendConfig::default(),
            procedure_config: ProcedureConfig::default(),
        }
    }

    #[must_use]
    pub async fn with_static_state(self) -> Self {
        let (opts, guard) = self.build_datanode_opts_and_guard();

        let (kv_backend, procedure_manager) = Instance::try_build_standalone_components(
            format!("{}/kv", &opts.storage.data_home),
            self.kv_backend_config.clone(),
            self.procedure_config.clone(),
        )
        .await
        .unwrap();

        Self {
            static_state: Some(StaticState {
                kv_backend,
                procedure_manager,
                datanode_opts: opts,
                guard,
            }),
            ..self
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
    pub fn with_metasrv_wal_config(mut self, metasrv_wal_config: MetaSrvWalConfig) -> Self {
        self.metasrv_wal_config = metasrv_wal_config;
        self
    }

    pub async fn build(self) -> GreptimeDbStandalone {
        let procedure_config = ProcedureConfig::default();

        let (kv_backend, opts, guard, procedure_manager) = match self.static_state {
            Some(state) => (
                state.kv_backend,
                state.datanode_opts,
                state.guard,
                state.procedure_manager,
            ),
            None => {
                let (opts, guard) = self.build_datanode_opts_and_guard();
                let (kv_backend, procedure_manager) = Instance::try_build_standalone_components(
                    format!("{}/kv", &opts.storage.data_home),
                    self.kv_backend_config.clone(),
                    procedure_config.clone(),
                )
                .await
                .unwrap();

                (kv_backend, opts, guard, procedure_manager)
            }
        };

        let plugins = self.plugin.unwrap_or_default();

        let datanode = DatanodeBuilder::new(opts.clone(), plugins.clone())
            .with_kv_backend(kv_backend.clone())
            .build()
            .await
            .unwrap();

        let table_metadata_manager = Arc::new(TableMetadataManager::new(kv_backend.clone()));
        table_metadata_manager.init().await.unwrap();

        let datanode_manager = Arc::new(StandaloneDatanodeManager(datanode.region_server()));

        let table_id_sequence = Arc::new(
            SequenceBuilder::new("table_id", kv_backend.clone())
                .initial(MIN_USER_TABLE_ID as u64)
                .step(10)
                .build(),
        );
        let metasrv_wal_config = self.metasrv_wal_config.clone();
        let wal_options_allocator = Arc::new(WalOptionsAllocator::new(
            metasrv_wal_config.clone(),
            kv_backend.clone(),
        ));
        let table_meta_allocator = TableMetadataAllocator::new(
            table_id_sequence,
            wal_options_allocator.clone(),
            table_metadata_manager.clone(),
        );

        let ddl_task_executor = Arc::new(
            DdlManager::try_new(
                procedure_manager.clone(),
                datanode_manager.clone(),
                Arc::new(DummyCacheInvalidator),
                table_metadata_manager,
                table_meta_allocator,
                Arc::new(MemoryRegionKeeper::default()),
            )
            .unwrap(),
        );

        let instance = FrontendBuilder::new(kv_backend, datanode_manager, ddl_task_executor)
            .with_plugin(plugins)
            .try_build()
            .await
            .unwrap();

        procedure_manager.start().await.unwrap();
        wal_options_allocator.start().await.unwrap();

        test_util::prepare_another_catalog_and_schema(&instance).await;

        instance.start().await.unwrap();

        GreptimeDbStandalone {
            instance: Arc::new(instance),
            datanode_opts: opts.clone(),
            mix_opts: MixOptions {
                data_home: opts.storage.data_home.to_string(),
                procedure: procedure_config,
                metadata_store: self.kv_backend_config,
                frontend: FrontendOptions::default(),
                datanode: opts,
                logging: LoggingOptions::default(),
                wal_meta: metasrv_wal_config,
            },
            guard,
        }
    }

    fn build_datanode_opts_and_guard(&self) -> (DatanodeOptions, TestGuard) {
        let default_store_type = self.default_store.unwrap_or(StorageType::File);
        let store_types = self.store_providers.clone().unwrap_or_default();
        let (opts, guard) = create_tmp_dir_and_datanode_opts(
            Mode::Standalone,
            default_store_type,
            store_types,
            &self.instance_name,
            self.datanode_wal_config.clone(),
        );
        (opts, guard)
    }
}
