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

use common_catalog::consts::DEFAULT_CATALOG_NAME;
use common_error::ext::BoxedError;
use common_meta::cache::LayeredCacheRegistryRef;
use common_meta::key::TableMetadataManager;
use common_meta::key::flow::FlowMetadataManager;
use common_meta::kv_backend::KvBackendRef;
use common_procedure::ProcedureManagerRef;
use moka::sync::Cache;
use partition::manager::PartitionRuleManager;

use crate::information_schema::{
    InformationExtensionRef, InformationSchemaProvider, InformationSchemaTableFactoryRef,
};
use crate::kvbackend::KvBackendCatalogManager;
use crate::kvbackend::manager::{CATALOG_CACHE_MAX_CAPACITY, SystemCatalog};
use crate::process_manager::ProcessManagerRef;
use crate::system_schema::numbers_table_provider::NumbersTableProvider;
use crate::system_schema::pg_catalog::PGCatalogProvider;

#[async_trait::async_trait]
pub trait CatalogManagerBuilderConfigrator<C>: Send + Sync {
    async fn configure(
        &self,
        builder: KvBackendCatalogManagerBuilder,
        ctx: C,
    ) -> std::result::Result<KvBackendCatalogManagerBuilder, BoxedError>;
}

pub type CatalogManagerBuilderConfigratorRef<C> = Arc<dyn CatalogManagerBuilderConfigrator<C>>;

pub struct KvBackendCatalogManagerBuilder {
    information_extension: InformationExtensionRef,
    backend: KvBackendRef,
    cache_registry: LayeredCacheRegistryRef,
    procedure_manager: Option<ProcedureManagerRef>,
    process_manager: Option<ProcessManagerRef>,
    extra_information_table_factories: HashMap<String, InformationSchemaTableFactoryRef>,
}

impl KvBackendCatalogManagerBuilder {
    pub fn new(
        information_extension: InformationExtensionRef,
        backend: KvBackendRef,
        cache_registry: LayeredCacheRegistryRef,
    ) -> Self {
        Self {
            information_extension,
            backend,
            cache_registry,
            procedure_manager: None,
            process_manager: None,
            extra_information_table_factories: HashMap::new(),
        }
    }

    pub fn with_procedure_manager(mut self, procedure_manager: ProcedureManagerRef) -> Self {
        self.procedure_manager = Some(procedure_manager);
        self
    }

    pub fn with_process_manager(mut self, process_manager: ProcessManagerRef) -> Self {
        self.process_manager = Some(process_manager);
        self
    }

    /// Sets the extra information tables.
    pub fn with_extra_information_table_factories(
        mut self,
        factories: HashMap<String, InformationSchemaTableFactoryRef>,
    ) -> Self {
        self.extra_information_table_factories = factories;
        self
    }

    pub fn build(self) -> Arc<KvBackendCatalogManager> {
        let Self {
            information_extension,
            backend,
            cache_registry,
            procedure_manager,
            process_manager,
            extra_information_table_factories,
        } = self;
        Arc::new_cyclic(|me| KvBackendCatalogManager {
            information_extension,
            partition_manager: Arc::new(PartitionRuleManager::new(
                backend.clone(),
                cache_registry
                    .get()
                    .expect("Failed to get table_route_cache"),
            )),
            table_metadata_manager: Arc::new(TableMetadataManager::new(backend.clone())),
            system_catalog: SystemCatalog {
                catalog_manager: me.clone(),
                catalog_cache: Cache::new(CATALOG_CACHE_MAX_CAPACITY),
                pg_catalog_cache: Cache::new(CATALOG_CACHE_MAX_CAPACITY),
                information_schema_provider: {
                    let provider = InformationSchemaProvider::new(
                        DEFAULT_CATALOG_NAME.to_string(),
                        me.clone(),
                        Arc::new(FlowMetadataManager::new(backend.clone())),
                        process_manager.clone(),
                        backend.clone(),
                    );
                    let provider = provider
                        .with_extra_table_factories(extra_information_table_factories.clone());
                    Arc::new(provider)
                },
                pg_catalog_provider: Arc::new(PGCatalogProvider::new(
                    DEFAULT_CATALOG_NAME.to_string(),
                    me.clone(),
                )),
                numbers_table_provider: NumbersTableProvider,
                backend,
                process_manager,
                extra_information_table_factories,
            },
            cache_registry,
            procedure_manager,
        })
    }
}
