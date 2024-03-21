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

use std::any::Any;
use std::collections::BTreeSet;
use std::sync::{Arc, Weak};
use std::time::Duration;

use async_stream::try_stream;
use common_catalog::consts::{
    DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, INFORMATION_SCHEMA_NAME, NUMBERS_TABLE_ID,
};
use common_catalog::format_full_table_name;
use common_error::ext::BoxedError;
use common_meta::cache_invalidator::{CacheInvalidator, CacheInvalidatorRef, Context};
use common_meta::error::Result as MetaResult;
use common_meta::instruction::CacheIdent;
use common_meta::key::catalog_name::CatalogNameKey;
use common_meta::key::schema_name::SchemaNameKey;
use common_meta::key::table_info::TableInfoValue;
use common_meta::key::table_name::TableNameKey;
use common_meta::key::{TableMetadataManager, TableMetadataManagerRef};
use common_meta::kv_backend::KvBackendRef;
use futures_util::stream::BoxStream;
use futures_util::{StreamExt, TryStreamExt};
use moka::future::{Cache as AsyncCache, CacheBuilder};
use moka::sync::Cache;
use partition::manager::{PartitionRuleManager, PartitionRuleManagerRef};
use snafu::prelude::*;
use table::dist_table::DistTable;
use table::table::numbers::{NumbersTable, NUMBERS_TABLE_NAME};
use table::TableRef;

use crate::error::Error::{GetTableCache, TableCacheNotGet};
use crate::error::{
    self as catalog_err, ListCatalogsSnafu, ListSchemasSnafu, ListTablesSnafu,
    Result as CatalogResult, TableCacheNotGetSnafu, TableMetadataManagerSnafu,
};
use crate::information_schema::InformationSchemaProvider;
use crate::CatalogManager;

/// Access all existing catalog, schema and tables.
///
/// The result comes from two source, all the user tables are presented in
/// a kv-backend which persists the metadata of a table. And system tables
/// comes from `SystemCatalog`, which is static and read-only.
#[derive(Clone)]
pub struct KvBackendCatalogManager {
    // TODO(LFC): Maybe use a real implementation for Standalone mode.
    // Now we use `NoopKvCacheInvalidator` for Standalone mode. In Standalone mode, the KV backend
    // is implemented by RaftEngine. Maybe we need a cache for it?
    cache_invalidator: CacheInvalidatorRef,
    partition_manager: PartitionRuleManagerRef,
    table_metadata_manager: TableMetadataManagerRef,
    /// A sub-CatalogManager that handles system tables
    system_catalog: SystemCatalog,
    table_cache: AsyncCache<String, TableRef>,
}

fn make_table(table_info_value: TableInfoValue) -> CatalogResult<TableRef> {
    let table_info = table_info_value
        .table_info
        .try_into()
        .context(catalog_err::InvalidTableInfoInCatalogSnafu)?;
    Ok(DistTable::table(Arc::new(table_info)))
}

#[async_trait::async_trait]
impl CacheInvalidator for KvBackendCatalogManager {
    async fn invalidate(&self, ctx: &Context, caches: Vec<CacheIdent>) -> MetaResult<()> {
        for cache in &caches {
            if let CacheIdent::TableName(table_name) = cache {
                let table_cache_key = format_full_table_name(
                    &table_name.catalog_name,
                    &table_name.schema_name,
                    &table_name.table_name,
                );
                self.table_cache.invalidate(&table_cache_key).await;
            }
        }
        self.cache_invalidator.invalidate(ctx, caches).await
    }
}

const CATALOG_CACHE_MAX_CAPACITY: u64 = 128;
const TABLE_CACHE_MAX_CAPACITY: u64 = 65536;
const TABLE_CACHE_TTL: Duration = Duration::from_secs(10 * 60);
const TABLE_CACHE_TTI: Duration = Duration::from_secs(5 * 60);

impl KvBackendCatalogManager {
    pub fn new(backend: KvBackendRef, cache_invalidator: CacheInvalidatorRef) -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            partition_manager: Arc::new(PartitionRuleManager::new(backend.clone())),
            table_metadata_manager: Arc::new(TableMetadataManager::new(backend)),
            cache_invalidator,
            system_catalog: SystemCatalog {
                catalog_manager: me.clone(),
                catalog_cache: Cache::new(CATALOG_CACHE_MAX_CAPACITY),
                information_schema_provider: Arc::new(InformationSchemaProvider::new(
                    DEFAULT_CATALOG_NAME.to_string(),
                    me.clone(),
                )),
            },
            table_cache: CacheBuilder::new(TABLE_CACHE_MAX_CAPACITY)
                .time_to_live(TABLE_CACHE_TTL)
                .time_to_idle(TABLE_CACHE_TTI)
                .build(),
        })
    }

    pub fn partition_manager(&self) -> PartitionRuleManagerRef {
        self.partition_manager.clone()
    }

    pub fn table_metadata_manager_ref(&self) -> &TableMetadataManagerRef {
        &self.table_metadata_manager
    }
}

#[async_trait::async_trait]
impl CatalogManager for KvBackendCatalogManager {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn catalog_names(&self) -> CatalogResult<Vec<String>> {
        let stream = self
            .table_metadata_manager
            .catalog_manager()
            .catalog_names()
            .await;

        let keys = stream
            .try_collect::<Vec<_>>()
            .await
            .map_err(BoxedError::new)
            .context(ListCatalogsSnafu)?;

        Ok(keys)
    }

    async fn schema_names(&self, catalog: &str) -> CatalogResult<Vec<String>> {
        let stream = self
            .table_metadata_manager
            .schema_manager()
            .schema_names(catalog)
            .await;
        let mut keys = stream
            .try_collect::<BTreeSet<_>>()
            .await
            .map_err(BoxedError::new)
            .context(ListSchemasSnafu { catalog })?;

        keys.extend(self.system_catalog.schema_names());

        Ok(keys.into_iter().collect())
    }

    async fn table_names(&self, catalog: &str, schema: &str) -> CatalogResult<Vec<String>> {
        let stream = self
            .table_metadata_manager
            .table_name_manager()
            .tables(catalog, schema)
            .await;
        let mut tables = stream
            .try_collect::<Vec<_>>()
            .await
            .map_err(BoxedError::new)
            .context(ListTablesSnafu { catalog, schema })?
            .into_iter()
            .map(|(k, _)| k)
            .collect::<Vec<_>>();
        tables.extend_from_slice(&self.system_catalog.table_names(schema));

        Ok(tables.into_iter().collect())
    }

    async fn catalog_exists(&self, catalog: &str) -> CatalogResult<bool> {
        self.table_metadata_manager
            .catalog_manager()
            .exists(CatalogNameKey::new(catalog))
            .await
            .context(TableMetadataManagerSnafu)
    }

    async fn schema_exists(&self, catalog: &str, schema: &str) -> CatalogResult<bool> {
        if self.system_catalog.schema_exist(schema) {
            return Ok(true);
        }

        self.table_metadata_manager
            .schema_manager()
            .exists(SchemaNameKey::new(catalog, schema))
            .await
            .context(TableMetadataManagerSnafu)
    }

    async fn table_exists(&self, catalog: &str, schema: &str, table: &str) -> CatalogResult<bool> {
        if self.system_catalog.table_exist(schema, table) {
            return Ok(true);
        }

        let key = TableNameKey::new(catalog, schema, table);
        self.table_metadata_manager
            .table_name_manager()
            .get(key)
            .await
            .context(TableMetadataManagerSnafu)
            .map(|x| x.is_some())
    }

    async fn table(
        &self,
        catalog: &str,
        schema: &str,
        table_name: &str,
    ) -> CatalogResult<Option<TableRef>> {
        if let Some(table) = self.system_catalog.table(catalog, schema, table_name) {
            return Ok(Some(table));
        }

        let init = async {
            let table_name_key = TableNameKey::new(catalog, schema, table_name);
            let Some(table_name_value) = self
                .table_metadata_manager
                .table_name_manager()
                .get(table_name_key)
                .await
                .context(TableMetadataManagerSnafu)?
            else {
                return TableCacheNotGetSnafu {
                    key: table_name_key.to_string(),
                }
                .fail();
            };
            let table_id = table_name_value.table_id();

            let Some(table_info_value) = self
                .table_metadata_manager
                .table_info_manager()
                .get(table_id)
                .await
                .context(TableMetadataManagerSnafu)?
                .map(|v| v.into_inner())
            else {
                return TableCacheNotGetSnafu {
                    key: table_name_key.to_string(),
                }
                .fail();
            };
            make_table(table_info_value)
        };

        match self
            .table_cache
            .try_get_with_by_ref(&format_full_table_name(catalog, schema, table_name), init)
            .await
        {
            Ok(table) => Ok(Some(table)),
            Err(err) => match err.as_ref() {
                TableCacheNotGet { .. } => Ok(None),
                _ => Err(err),
            },
        }
        .map_err(|err| GetTableCache {
            err_msg: err.to_string(),
        })
    }

    async fn tables<'a>(
        &'a self,
        catalog: &'a str,
        schema: &'a str,
    ) -> BoxStream<'a, CatalogResult<TableRef>> {
        let sys_tables = try_stream!({
            // System tables
            let sys_table_names = self.system_catalog.table_names(schema);
            for table_name in sys_table_names {
                if let Some(table) = self.system_catalog.table(catalog, schema, &table_name) {
                    yield table;
                }
            }
        });

        let table_id_stream = self
            .table_metadata_manager
            .table_name_manager()
            .tables(catalog, schema)
            .await
            .map_ok(|(_, v)| v.table_id());
        const BATCH_SIZE: usize = 128;
        let user_tables = try_stream!({
            // Split table ids into chunks
            let mut table_id_chunks = table_id_stream.ready_chunks(BATCH_SIZE);

            while let Some(table_ids) = table_id_chunks.next().await {
                let table_ids = table_ids
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(BoxedError::new)
                    .context(ListTablesSnafu { catalog, schema })?;

                let table_info_values = self
                    .table_metadata_manager
                    .table_info_manager()
                    .batch_get(&table_ids)
                    .await
                    .context(TableMetadataManagerSnafu)?;

                for table_info_value in table_info_values.into_values() {
                    yield make_table(table_info_value)?;
                }
            }
        });

        Box::pin(sys_tables.chain(user_tables))
    }
}

// TODO: This struct can hold a static map of all system tables when
// the upper layer (e.g., procedure) can inform the catalog manager
// a new catalog is created.
/// Existing system tables:
/// - public.numbers
/// - information_schema.{tables}
#[derive(Clone)]
struct SystemCatalog {
    catalog_manager: Weak<KvBackendCatalogManager>,
    catalog_cache: Cache<String, Arc<InformationSchemaProvider>>,
    information_schema_provider: Arc<InformationSchemaProvider>,
}

impl SystemCatalog {
    fn schema_names(&self) -> Vec<String> {
        vec![INFORMATION_SCHEMA_NAME.to_string()]
    }

    fn table_names(&self, schema: &str) -> Vec<String> {
        if schema == INFORMATION_SCHEMA_NAME {
            self.information_schema_provider.table_names()
        } else if schema == DEFAULT_SCHEMA_NAME {
            vec![NUMBERS_TABLE_NAME.to_string()]
        } else {
            vec![]
        }
    }

    fn schema_exist(&self, schema: &str) -> bool {
        schema == INFORMATION_SCHEMA_NAME
    }

    fn table_exist(&self, schema: &str, table: &str) -> bool {
        if schema == INFORMATION_SCHEMA_NAME {
            self.information_schema_provider.table(table).is_some()
        } else if schema == DEFAULT_SCHEMA_NAME {
            table == NUMBERS_TABLE_NAME
        } else {
            false
        }
    }

    fn table(&self, catalog: &str, schema: &str, table_name: &str) -> Option<TableRef> {
        if schema == INFORMATION_SCHEMA_NAME {
            let information_schema_provider =
                self.catalog_cache.get_with_by_ref(catalog, move || {
                    Arc::new(InformationSchemaProvider::new(
                        catalog.to_string(),
                        self.catalog_manager.clone(),
                    ))
                });
            information_schema_provider.table(table_name)
        } else if schema == DEFAULT_SCHEMA_NAME && table_name == NUMBERS_TABLE_NAME {
            Some(NumbersTable::table(NUMBERS_TABLE_ID))
        } else {
            None
        }
    }
}
