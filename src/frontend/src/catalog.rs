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

use catalog::error::{
    self as catalog_err, ListCatalogsSnafu, ListSchemasSnafu, Result as CatalogResult,
    TableMetadataManagerSnafu,
};
use catalog::information_schema::{InformationSchemaProvider, COLUMNS, TABLES};
use catalog::remote::KvCacheInvalidatorRef;
use catalog::CatalogManager;
use common_catalog::consts::{DEFAULT_SCHEMA_NAME, INFORMATION_SCHEMA_NAME, NUMBERS_TABLE_ID};
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

/// Access all existing catalog, schema and tables.
///
/// The result comes from two source, all the user tables are presented in
/// a kv-backend which persists the metadata of a table. And system tables
/// comes from [SystemCatalog], which is static and read-only.
#[derive(Clone)]
pub struct FrontendCatalogManager {
    // TODO(LFC): Maybe use a real implementation for Standalone mode.
    // Now we use `NoopKvCacheInvalidator` for Standalone mode. In Standalone mode, the KV backend
    // is implemented by RaftEngine. Maybe we need a cache for it?
    backend_cache_invalidator: KvCacheInvalidatorRef,
    partition_manager: PartitionRuleManagerRef,
    table_metadata_manager: TableMetadataManagerRef,
    datanode_manager: DatanodeManagerRef,
    /// A sub-CatalogManager that handles system tables
    system_catalog: SystemCatalog,
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
    ) -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            backend_cache_invalidator,
            partition_manager: Arc::new(PartitionRuleManager::new(backend.clone())),
            table_metadata_manager: Arc::new(TableMetadataManager::new(backend)),
            datanode_manager,
            system_catalog: SystemCatalog {
                catalog_manager: me.clone(),
            },
        })
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
            .context(ListSchemasSnafu { catalog })?
            .into_iter()
            .collect::<Vec<_>>();

        keys.extend_from_slice(&self.system_catalog.schema_names());

        Ok(keys)
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
        tables.extend_from_slice(&self.system_catalog.table_names(schema));

        Ok(tables)
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

// TODO: This struct can hold a static map of all system tables when
// the upper layer (e.g., procedure) can inform the catalog manager
// a new catalog is created.
/// Existing system tables:
/// - public.numbers
/// - information_schema.tables
/// - information_schema.columns
#[derive(Clone)]
struct SystemCatalog {
    catalog_manager: Weak<FrontendCatalogManager>,
}

impl SystemCatalog {
    fn schema_names(&self) -> Vec<String> {
        vec![INFORMATION_SCHEMA_NAME.to_string()]
    }

    fn table_names(&self, schema: &str) -> Vec<String> {
        if schema == INFORMATION_SCHEMA_NAME {
            vec![TABLES.to_string(), COLUMNS.to_string()]
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
            table == TABLES || table == COLUMNS
        } else if schema == DEFAULT_SCHEMA_NAME {
            table == NUMBERS_TABLE_NAME
        } else {
            false
        }
    }

    fn table(&self, catalog: &str, schema: &str, table_name: &str) -> Option<TableRef> {
        if schema == INFORMATION_SCHEMA_NAME {
            let information_schema_provider =
                InformationSchemaProvider::new(catalog.to_string(), self.catalog_manager.clone());
            information_schema_provider.table(table_name)
        } else if schema == DEFAULT_SCHEMA_NAME && table_name == NUMBERS_TABLE_NAME {
            Some(NumbersTable::table(NUMBERS_TABLE_ID))
        } else {
            None
        }
    }
}
