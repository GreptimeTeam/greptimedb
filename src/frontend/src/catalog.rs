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
use catalog::remote::KvCacheInvalidatorRef;
use catalog::{
    CatalogManager, DeregisterSchemaRequest, DeregisterTableRequest, RegisterTableRequest,
    RenameTableRequest, RegisterSchemaRequest,
};
use client::client_manager::DatanodeClients;
use common_catalog::consts::{
    DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, INFORMATION_SCHEMA_NAME, NUMBERS_TABLE_ID,
};
use common_error::ext::BoxedError;
use common_meta::cache_invalidator::{CacheInvalidator, Context};
use common_meta::error::Result as MetaResult;
use common_meta::ident::TableIdent;
use common_meta::key::catalog_name::CatalogNameKey;
use common_meta::key::schema_name::SchemaNameKey;
use common_meta::key::table_info::TableInfoKey;
use common_meta::key::table_name::TableNameKey;
use common_meta::key::{TableMetaKey, TableMetadataManagerRef};
use common_meta::kv_backend::KvBackendRef;
use common_telemetry::debug;
use futures_util::TryStreamExt;
use partition::manager::PartitionRuleManagerRef;
use snafu::prelude::*;
use table::table::numbers::{NumbersTable, NUMBERS_TABLE_NAME};
use table::TableRef;

use crate::table::DistTable;

#[derive(Clone)]
pub struct FrontendCatalogManager {
    backend: KvBackendRef,
    backend_cache_invalidator: KvCacheInvalidatorRef,
    partition_manager: PartitionRuleManagerRef,
    datanode_clients: Arc<DatanodeClients>,
    table_metadata_manager: TableMetadataManagerRef,
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

        self.partition_manager
            .table_routes()
            .invalidate_table_route(table_id)
            .await;

        Ok(())
    }
}

impl FrontendCatalogManager {
    pub fn new(
        backend: KvBackendRef,
        backend_cache_invalidator: KvCacheInvalidatorRef,
        partition_manager: PartitionRuleManagerRef,
        datanode_clients: Arc<DatanodeClients>,
        table_metadata_manager: TableMetadataManagerRef,
    ) -> Self {
        Self {
            backend,
            backend_cache_invalidator,
            partition_manager,
            datanode_clients,
            table_metadata_manager,
        }
    }

    pub fn backend(&self) -> KvBackendRef {
        self.backend.clone()
    }

    pub fn partition_manager(&self) -> PartitionRuleManagerRef {
        self.partition_manager.clone()
    }

    pub fn table_metadata_manager_ref(&self) -> &TableMetadataManagerRef {
        &self.table_metadata_manager
    }

    pub fn datanode_clients(&self) -> Arc<DatanodeClients> {
        self.datanode_clients.clone()
    }

    pub async fn invalidate_schema(&self, catalog: &str, schema: &str) {
        let key = SchemaNameKey::new(catalog, schema).as_raw_key();

        self.backend_cache_invalidator.invalidate_key(&key).await;
    }
}

// FIXME(hl): Frontend only needs a CatalogList, should replace with trait upcasting
// as soon as it's stable: https://github.com/rust-lang/rust/issues/65991
#[async_trait::async_trait]
impl CatalogManager for FrontendCatalogManager {
    async fn start(&self) -> catalog::error::Result<()> {
        Ok(())
    }

    async fn register_catalog(self: Arc<Self>, _name: String) -> CatalogResult<bool> {
        unimplemented!("FrontendCatalogManager does not support registering catalog")
    }

    // TODO(LFC): Handle the table caching in (de)register_table.
    async fn register_table(&self, _request: RegisterTableRequest) -> CatalogResult<bool> {
        Ok(true)
    }

    async fn deregister_table(&self, _request: DeregisterTableRequest) -> CatalogResult<()> {
        Ok(())
    }
    
    async fn register_schema(
        &self,
        _request: RegisterSchemaRequest,
    ) -> catalog::error::Result<bool> {
        unimplemented!("FrontendCatalogManager does not support registering schema")
    }

    async fn deregister_schema(
        &self,
        _request: DeregisterSchemaRequest,
    ) -> catalog_err::Result<bool> {
        unimplemented!("FrontendCatalogManager does not support deregistering schema")
    }

    async fn rename_table(&self, _request: RenameTableRequest) -> catalog_err::Result<bool> {
        unimplemented!()
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
