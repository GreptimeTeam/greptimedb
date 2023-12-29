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
use common_config::{KvBackendConfig, WalConfig};
use common_meta::cache_invalidator::DummyCacheInvalidator;
use common_meta::ddl::table_meta::TableMetadataAllocator;
use common_meta::ddl_manager::DdlManager;
use common_meta::key::TableMetadataManager;
use common_meta::region_keeper::MemoryRegionKeeper;
use common_meta::sequence::SequenceBuilder;
use common_meta::wal::{WalConfig as MetaWalConfig, WalOptionsAllocator};
use common_procedure::options::ProcedureConfig;
use common_telemetry::logging::LoggingOptions;
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
    pub mix_options: MixOptions,
    pub guard: TestGuard,
}

#[derive(Clone)]
pub struct GreptimeDbStandaloneBuilder {
    instance_name: String,
    wal_config: WalConfig,
    meta_wal_config: MetaWalConfig,
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
            wal_config: WalConfig::default(),
            meta_wal_config: MetaWalConfig::default(),
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
    pub fn with_wal_config(mut self, wal_config: WalConfig) -> Self {
        self.wal_config = wal_config;
        self
    }

    #[must_use]
    pub fn with_meta_wal_config(mut self, wal_meta: MetaWalConfig) -> Self {
        self.meta_wal_config = wal_meta;
        self
    }

    pub async fn build(self) -> GreptimeDbStandalone {
        let default_store_type = self.default_store.unwrap_or(StorageType::File);
        let store_types = self.store_providers.unwrap_or_default();

        let (opts, guard) = create_tmp_dir_and_datanode_opts(
            Mode::Standalone,
            default_store_type,
            store_types,
            &self.instance_name,
            self.wal_config.clone(),
        );

        let procedure_config = ProcedureConfig::default();
        let kv_backend_config = KvBackendConfig::default();
        let (kv_backend, procedure_manager) = Instance::try_build_standalone_components(
            format!("{}/kv", &opts.storage.data_home),
            kv_backend_config.clone(),
            procedure_config.clone(),
        )
        .await
        .unwrap();

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
        let wal_meta = self.meta_wal_config.clone();
        let wal_options_allocator = Arc::new(WalOptionsAllocator::new(
            wal_meta.clone(),
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
            mix_options: MixOptions {
                data_home: opts.storage.data_home.to_string(),
                procedure: procedure_config,
                metadata_store: kv_backend_config,
                frontend: FrontendOptions::default(),
                datanode: opts,
                logging: LoggingOptions::default(),
                wal_meta,
            },
            guard,
        }
    }
}
