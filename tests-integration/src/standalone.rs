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
use common_config::KvBackendConfig;
use common_meta::cache_invalidator::DummyKvCacheInvalidator;
use common_procedure::options::ProcedureConfig;
use common_telemetry::logging::LoggingOptions;
use datanode::config::DatanodeOptions;
use datanode::datanode::DatanodeBuilder;
use frontend::frontend::FrontendOptions;
use frontend::instance::{FrontendInstance, Instance, StandaloneDatanodeManager};

use crate::test_util::{self, create_tmp_dir_and_datanode_opts, StorageType, TestGuard};

pub struct GreptimeDbStandalone {
    pub instance: Arc<Instance>,
    pub datanode_opts: DatanodeOptions,
    pub mix_options: MixOptions,
    pub guard: TestGuard,
}

pub struct GreptimeDbStandaloneBuilder {
    instance_name: String,
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
        }
    }

    pub fn with_default_store_type(self, store_type: StorageType) -> Self {
        Self {
            default_store: Some(store_type),
            ..self
        }
    }

    #[cfg(test)]
    pub fn with_store_providers(self, store_providers: Vec<StorageType>) -> Self {
        Self {
            store_providers: Some(store_providers),
            ..self
        }
    }

    #[cfg(test)]
    pub fn with_plugin(self, plugin: Plugins) -> Self {
        Self {
            plugin: Some(plugin),
            ..self
        }
    }

    pub async fn build(self) -> GreptimeDbStandalone {
        let default_store_type = self.default_store.unwrap_or(StorageType::File);
        let store_types = self.store_providers.unwrap_or_default();

        let (opts, guard) =
            create_tmp_dir_and_datanode_opts(default_store_type, store_types, &self.instance_name);

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

        let catalog_manager = KvBackendCatalogManager::new(
            kv_backend.clone(),
            Arc::new(DummyKvCacheInvalidator),
            Arc::new(StandaloneDatanodeManager(datanode.region_server())),
        );

        catalog_manager
            .table_metadata_manager_ref()
            .init()
            .await
            .unwrap();
        procedure_manager.start().await.unwrap();
        let instance = Instance::try_new_standalone(
            kv_backend,
            procedure_manager,
            catalog_manager,
            plugins,
            datanode.region_server(),
        )
        .await
        .unwrap();

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
            },
            guard,
        }
    }
}
