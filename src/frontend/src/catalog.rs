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
use std::sync::Arc;

use catalog::error::{
    self as catalog_err, ListCatalogsSnafu, ListSchemasSnafu, Result as CatalogResult,
    TableMetadataManagerSnafu,
};
use catalog::information_schema::{InformationSchemaProvider, COLUMNS, TABLES};
use catalog::local::MemoryCatalogManager;
use catalog::remote::KvCacheInvalidatorRef;
use catalog::{
    CatalogManager, DeregisterSchemaRequest, DeregisterTableRequest, RegisterSchemaRequest,
    RegisterTableRequest,
};
use common_catalog::consts::{
    DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, INFORMATION_SCHEMA_NAME, NUMBERS_TABLE_ID,
};
use common_error::ext::BoxedError;
use common_meta::cache_invalidator::{CacheInvalidator, Context};
use common_meta::datanode_manager::DatanodeManagerRef;
use common_meta::error::Result as MetaResult;
use common_meta::ident::TableIdent;
use common_meta::key::catalog_name::CatalogNameKey;
use common_meta::key::schema_name::SchemaNameKey;
use common_meta::key::table_info::TableInfoKey;
use common_meta::key::table_name::TableNameKey;
use common_meta::key::table_route::TableRouteKey;
use common_meta::key::{TableMetaKey, TableMetadataManager, TableMetadataManagerRef};
use common_meta::kv_backend::KvBackendRef;
use common_telemetry::debug;
use futures_util::TryStreamExt;
use partition::manager::{PartitionRuleManager, PartitionRuleManagerRef};
use snafu::prelude::*;
use table::table::numbers::{NumbersTable, NUMBERS_TABLE_NAME};
use table::TableRef;

use crate::table::DistTable;

// There are two sources for finding a table: the `local_catalog_manager` and the
// `table_metadata_manager`.
//
// The `local_catalog_manager` is for storing tables that are often transparent, not saving any
// real data. For example, our system tables, the `numbers` table and the "information_schema"
// table.
//
// The `table_metadata_manager`, on the other hand, is for storing tables that are created by users,
// obviously.
//
// For now, separating the two makes the code simpler, at least in the retrieval site. Now we have
// `numbers` and `information_schema` system tables. Both have their special implementations. If we
// put them with other ordinary tables that are created by users, we need to check the table name
// to decide which `TableRef` to return. Like this:
//
// ```rust
// match table_name {
//   "numbers" => ... // return NumbersTable impl
//   "information_schema" => ... // return InformationSchemaTable impl
//   _ => .. // return DistTable impl
// }
// ```
//
// On the other hand, because we use `MemoryCatalogManager` for system tables, we can easily store
// and retrieve the concrete implementation of the system tables by their names, no more "if-else"s.
//
// However, if the system table is designed to have more features in the future, we may revisit
// the implementation here.
#[derive(Clone)]
pub struct FrontendCatalogManager {
    // TODO(LFC): Maybe use a real implementation for Standalone mode.
    // Now we use `NoopKvCacheInvalidator` for Standalone mode. In Standalone mode, the KV backend
    // is implemented by RaftEngine. Maybe we need a cache for it?
    backend_cache_invalidator: KvCacheInvalidatorRef,
    partition_manager: PartitionRuleManagerRef,
    table_metadata_manager: TableMetadataManagerRef,
    datanode_manager: DatanodeManagerRef,
    local_catalog_manager: Arc<MemoryCatalogManager>,
}

#[async_trait::async_trait]
impl CacheInvalidator for FrontendCatalogManager {
    async fn invalidate_table(&self, _ctx: &Context, table_ident: TableIdent) -> MetaResult<()> {
        let table_id = table_ident.table_id;
        let key = TableNameKey::new(
            &table_ident.catalog,
            &table_ident.schema,
            &table_ident.table,
        );
        self.backend_cache_invalidator
            .invalidate_key(&key.as_raw_key())
            .await;
        debug!(
            "invalidated cache key: {}",
            String::from_utf8_lossy(&key.as_raw_key())
        );

        let key = TableInfoKey::new(table_id);
        self.backend_cache_invalidator
            .invalidate_key(&key.as_raw_key())
            .await;
        debug!(
            "invalidated cache key: {}",
            String::from_utf8_lossy(&key.as_raw_key())
        );

        let key = &TableRouteKey { table_id };
        self.backend_cache_invalidator
            .invalidate_key(&key.as_raw_key())
            .await;
        debug!(
            "invalidated cache key: {}",
            String::from_utf8_lossy(&key.as_raw_key())
        );

        Ok(())
    }
}

impl FrontendCatalogManager {
    pub fn new(
        backend: KvBackendRef,
        backend_cache_invalidator: KvCacheInvalidatorRef,
        datanode_manager: DatanodeManagerRef,
    ) -> Self {
        Self {
            backend_cache_invalidator,
            partition_manager: Arc::new(PartitionRuleManager::new(backend.clone())),
            table_metadata_manager: Arc::new(TableMetadataManager::new(backend)),
            datanode_manager,
            local_catalog_manager: MemoryCatalogManager::new(),
        }
    }

    pub fn partition_manager(&self) -> PartitionRuleManagerRef {
        self.partition_manager.clone()
    }

    pub fn table_metadata_manager_ref(&self) -> &TableMetadataManagerRef {
        &self.table_metadata_manager
    }

    pub fn datanode_manager(&self) -> DatanodeManagerRef {
        self.datanode_manager.clone()
    }

    pub async fn invalidate_schema(&self, catalog: &str, schema: &str) {
        let key = SchemaNameKey::new(catalog, schema).as_raw_key();

        self.backend_cache_invalidator.invalidate_key(&key).await;
    }
}

#[async_trait::async_trait]
impl CatalogManager for FrontendCatalogManager {
    fn register_catalog(&self, name: &str) -> CatalogResult<bool> {
        self.local_catalog_manager.register_catalog(name)
    }

    fn register_table(&self, request: RegisterTableRequest) -> CatalogResult<bool> {
        self.local_catalog_manager.register_table(request)
    }

    fn deregister_table(&self, _request: DeregisterTableRequest) -> CatalogResult<()> {
        Ok(())
    }

    fn register_schema(&self, _request: RegisterSchemaRequest) -> catalog::error::Result<bool> {
        unimplemented!("FrontendCatalogManager does not support registering schema")
    }

    fn deregister_schema(&self, _request: DeregisterSchemaRequest) -> catalog_err::Result<bool> {
        unimplemented!("FrontendCatalogManager does not support deregistering schema")
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

        keys.insert(INFORMATION_SCHEMA_NAME.to_string());

        Ok(keys.into_iter().collect::<Vec<_>>())
    }

    async fn table_names(&self, catalog: &str, schema: &str) -> CatalogResult<Vec<String>> {
        let mut tables = self
            .table_metadata_manager
            .table_name_manager()
            .tables(catalog, schema)
            .await
            .context(TableMetadataManagerSnafu)?
            .into_iter()
            .map(|(k, _)| k)
            .collect::<Vec<String>>();
        if catalog == DEFAULT_CATALOG_NAME && schema == DEFAULT_SCHEMA_NAME {
            tables.push(NUMBERS_TABLE_NAME.to_string());
        }
        if schema == INFORMATION_SCHEMA_NAME {
            tables.push(TABLES.to_string());
            tables.push(COLUMNS.to_string());
        }

        Ok(tables)
    }

    async fn catalog_exist(&self, catalog: &str) -> CatalogResult<bool> {
        self.table_metadata_manager
            .catalog_manager()
            .exist(CatalogNameKey::new(catalog))
            .await
            .context(TableMetadataManagerSnafu)
    }

    async fn schema_exist(&self, catalog: &str, schema: &str) -> CatalogResult<bool> {
        if schema == INFORMATION_SCHEMA_NAME {
            return Ok(true);
        }
        self.table_metadata_manager
            .schema_manager()
            .exist(SchemaNameKey::new(catalog, schema))
            .await
            .context(TableMetadataManagerSnafu)
    }

    async fn table_exist(&self, catalog: &str, schema: &str, table: &str) -> CatalogResult<bool> {
        if schema == INFORMATION_SCHEMA_NAME && (table == TABLES || table == COLUMNS) {
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
        if catalog == DEFAULT_CATALOG_NAME
            && schema == DEFAULT_SCHEMA_NAME
            && table_name == NUMBERS_TABLE_NAME
        {
            return Ok(Some(NumbersTable::table(NUMBERS_TABLE_ID)));
        }

        if schema == INFORMATION_SCHEMA_NAME {
            let manager = Arc::new(self.clone()) as _;

            let provider =
                InformationSchemaProvider::new(catalog.to_string(), Arc::downgrade(&manager));
            return Ok(provider.table(table_name));
        }

        let key = TableNameKey::new(catalog, schema, table_name);
        let Some(table_name_value) = self
            .table_metadata_manager
            .table_name_manager()
            .get(key)
            .await
            .context(TableMetadataManagerSnafu)?
        else {
            return Ok(None);
        };
        let table_id = table_name_value.table_id();

        let Some(table_info_value) = self
            .table_metadata_manager
            .table_info_manager()
            .get(table_id)
            .await
            .context(TableMetadataManagerSnafu)?
        else {
            return Ok(None);
        };
        let table_info = Arc::new(
            table_info_value
                .table_info
                .try_into()
                .context(catalog_err::InvalidTableInfoInCatalogSnafu)?,
        );
        Ok(Some(DistTable::table(table_info)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
