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

use common_catalog::consts::DEFAULT_CATALOG_NAME;
use common_meta::cache::LayeredCacheRegistryRef;
use common_meta::key::TableMetadataManager;
use common_meta::key::flow::FlowMetadataManager;
use common_meta::kv_backend::KvBackendRef;
use common_procedure::ProcedureManagerRef;
use moka::sync::Cache;
use partition::manager::PartitionRuleManager;

#[cfg(feature = "enterprise")]
use crate::information_schema::InformationSchemaTableFactoryRef;
use crate::information_schema::{InformationExtensionRef, InformationSchemaProvider};
use crate::kvbackend::KvBackendCatalogManager;
use crate::kvbackend::manager::{CATALOG_CACHE_MAX_CAPACITY, SystemCatalog};
use crate::process_manager::ProcessManagerRef;
use crate::system_schema::pg_catalog::PGCatalogProvider;

pub struct KvBackendCatalogManagerBuilder {
    information_extension: InformationExtensionRef,
    backend: KvBackendRef,
    cache_registry: LayeredCacheRegistryRef,
    procedure_manager: Option<ProcedureManagerRef>,
    process_manager: Option<ProcessManagerRef>,
    #[cfg(feature = "enterprise")]
    extra_information_table_factories:
        std::collections::HashMap<String, InformationSchemaTableFactoryRef>,
    enable_numbers_table: bool,
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
            #[cfg(feature = "enterprise")]
            extra_information_table_factories: std::collections::HashMap::new(),
            enable_numbers_table: true,
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

    /// Sets whether to enable the builtin `numbers` table.
    pub fn with_enable_numbers_table(mut self, enable: bool) -> Self {
        self.enable_numbers_table = enable;
        self
    }

    /// Sets the extra information tables.
    #[cfg(feature = "enterprise")]
    pub fn with_extra_information_table_factories(
        mut self,
        factories: std::collections::HashMap<String, InformationSchemaTableFactoryRef>,
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
            #[cfg(feature = "enterprise")]
            extra_information_table_factories,
            enable_numbers_table,
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
                    #[cfg(feature = "enterprise")]
                    let provider = provider
                        .with_extra_table_factories(extra_information_table_factories.clone());
                    Arc::new(provider)
                },
                pg_catalog_provider: Arc::new(PGCatalogProvider::new(
                    DEFAULT_CATALOG_NAME.to_string(),
                    me.clone(),
                )),
                backend,
                process_manager,
                #[cfg(feature = "enterprise")]
                extra_information_table_factories,
                enable_numbers_table,
            },
            cache_registry,
            procedure_manager,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cache::{build_fundamental_cache_registry, with_default_composite_cache_registry};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_meta::cache::{CacheRegistryBuilder, LayeredCacheRegistryBuilder};
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use table::table::numbers::NUMBERS_TABLE_NAME;

    use super::*;
    use crate::CatalogManager;
    use crate::information_schema::NoopInformationExtension;

    #[tokio::test]
    async fn test_numbers_table_enabled_by_default() {
        let backend = Arc::new(MemoryKvBackend::new());
        let layered_cache_builder = LayeredCacheRegistryBuilder::default()
            .add_cache_registry(CacheRegistryBuilder::default().build());
        let fundamental_cache_registry = build_fundamental_cache_registry(backend.clone());
        let cache_registry = Arc::new(
            with_default_composite_cache_registry(
                layered_cache_builder.add_cache_registry(fundamental_cache_registry),
            )
            .unwrap()
            .build(),
        );

        let catalog_manager = KvBackendCatalogManagerBuilder::new(
            Arc::new(NoopInformationExtension),
            backend,
            cache_registry,
        )
        .build();

        // Numbers table should be enabled by default
        let table_names = catalog_manager
            .table_names(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, None)
            .await
            .unwrap();
        assert!(table_names.contains(&NUMBERS_TABLE_NAME.to_string()));

        let table = catalog_manager
            .table(
                DEFAULT_CATALOG_NAME,
                DEFAULT_SCHEMA_NAME,
                NUMBERS_TABLE_NAME,
                None,
            )
            .await
            .unwrap();
        assert!(table.is_some());

        let exists = catalog_manager
            .table_exists(
                DEFAULT_CATALOG_NAME,
                DEFAULT_SCHEMA_NAME,
                NUMBERS_TABLE_NAME,
                None,
            )
            .await
            .unwrap();
        assert!(exists);
    }

    #[tokio::test]
    async fn test_numbers_table_disabled() {
        let backend = Arc::new(MemoryKvBackend::new());
        let layered_cache_builder = LayeredCacheRegistryBuilder::default()
            .add_cache_registry(CacheRegistryBuilder::default().build());
        let fundamental_cache_registry = build_fundamental_cache_registry(backend.clone());
        let cache_registry = Arc::new(
            with_default_composite_cache_registry(
                layered_cache_builder.add_cache_registry(fundamental_cache_registry),
            )
            .unwrap()
            .build(),
        );

        let catalog_manager = KvBackendCatalogManagerBuilder::new(
            Arc::new(NoopInformationExtension),
            backend,
            cache_registry,
        )
        .with_enable_numbers_table(false)
        .build();

        // Numbers table should be disabled
        let table_names = catalog_manager
            .table_names(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, None)
            .await
            .unwrap();
        assert!(!table_names.contains(&NUMBERS_TABLE_NAME.to_string()));

        let table = catalog_manager
            .table(
                DEFAULT_CATALOG_NAME,
                DEFAULT_SCHEMA_NAME,
                NUMBERS_TABLE_NAME,
                None,
            )
            .await
            .unwrap();
        assert!(table.is_none());

        let exists = catalog_manager
            .table_exists(
                DEFAULT_CATALOG_NAME,
                DEFAULT_SCHEMA_NAME,
                NUMBERS_TABLE_NAME,
                None,
            )
            .await
            .unwrap();
        assert!(!exists);
    }
}
