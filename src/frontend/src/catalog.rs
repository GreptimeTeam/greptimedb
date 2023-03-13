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
use std::collections::HashSet;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use catalog::error::Error::GetCache;
use catalog::error::{self as catalog_err, Result as CatalogResult};
use catalog::helper::{
    build_catalog_prefix, build_schema_prefix, build_table_global_prefix, CatalogKey, SchemaKey,
    TableGlobalKey, TableGlobalValue,
};
use catalog::remote::{Kv, KvBackendRef};
use catalog::{
    CatalogList, CatalogManager, CatalogProvider, CatalogProviderRef, DeregisterTableRequest,
    RegisterSchemaRequest, RegisterSystemTableRequest, RegisterTableRequest, RenameTableRequest,
    SchemaProvider, SchemaProviderRef,
};
use common_telemetry::error;
use futures::StreamExt;
use meta_client::rpc::TableName;
use moka::sync::{Cache, CacheBuilder};
use partition::manager::PartitionRuleManagerRef;
use snafu::prelude::*;
use snafu::{Backtrace, GenerateImplicitData};
use table::TableRef;

use crate::datanode::DatanodeClients;
use crate::table::DistTable;

#[derive(Clone)]
pub struct FrontendCatalogManager {
    backend: KvBackendRef,
    partition_manager: PartitionRuleManagerRef,
    datanode_clients: Arc<DatanodeClients>,
    cache: Arc<Cache<String, CatalogProviderRef>>,
}

impl FrontendCatalogManager {
    pub fn new(
        backend: KvBackendRef,
        partition_manager: PartitionRuleManagerRef,
        datanode_clients: Arc<DatanodeClients>,
    ) -> Self {
        let cache = Arc::new(
            CacheBuilder::new(1024)
                .time_to_live(Duration::from_secs(30 * 60))
                .time_to_idle(Duration::from_secs(5 * 60))
                .build(),
        );

        Self {
            backend,
            partition_manager,
            datanode_clients,
            cache,
        }
    }

    pub(crate) fn backend(&self) -> KvBackendRef {
        self.backend.clone()
    }

    pub(crate) fn partition_manager(&self) -> PartitionRuleManagerRef {
        self.partition_manager.clone()
    }

    pub(crate) fn datanode_clients(&self) -> Arc<DatanodeClients> {
        self.datanode_clients.clone()
    }
}

// FIXME(hl): Frontend only needs a CatalogList, should replace with trait upcasting
// as soon as it's stable: https://github.com/rust-lang/rust/issues/65991
#[async_trait::async_trait]
impl CatalogManager for FrontendCatalogManager {
    async fn start(&self) -> CatalogResult<()> {
        Ok(())
    }

    // TODO(LFC): Handle the table caching in (de)register_table.
    async fn register_table(&self, _request: RegisterTableRequest) -> CatalogResult<bool> {
        Ok(true)
    }

    async fn deregister_table(&self, request: DeregisterTableRequest) -> CatalogResult<bool> {
        let table_name = TableName::new(request.catalog, request.schema, request.table_name);
        self.partition_manager
            .table_routes()
            .invalidate_table_route(&table_name)
            .await;
        Ok(true)
    }

    async fn register_schema(&self, _request: RegisterSchemaRequest) -> CatalogResult<bool> {
        unimplemented!()
    }

    async fn rename_table(&self, _request: RenameTableRequest) -> catalog_err::Result<bool> {
        unimplemented!()
    }

    async fn register_system_table(
        &self,
        _request: RegisterSystemTableRequest,
    ) -> CatalogResult<()> {
        unimplemented!()
    }

    fn schema(&self, catalog: &str, schema: &str) -> CatalogResult<Option<SchemaProviderRef>> {
        self.catalog(catalog)?
            .context(catalog::error::CatalogNotFoundSnafu {
                catalog_name: catalog,
            })?
            .schema(schema)
    }

    async fn table(
        &self,
        catalog: &str,
        schema: &str,
        table_name: &str,
    ) -> CatalogResult<Option<TableRef>> {
        self.schema(catalog, schema)?
            .context(catalog::error::SchemaNotFoundSnafu { catalog, schema })?
            .table(table_name)
            .await
    }
}

impl FrontendCatalogManager {
    fn refresh_catalogs(&self) -> CatalogResult<()> {
        // TODO(fys): waiting for datafusion asynchronous api about catalog
        std::thread::scope(|s| {
            let join = s.spawn(|| {
                common_runtime::block_on_read(async move { self.async_refresh_catalogs().await })
            });
            join.join().unwrap()
        })
    }

    async fn async_refresh_catalogs(&self) -> CatalogResult<()> {
        let backend = self.backend.clone();
        let key = build_catalog_prefix();
        let mut iter = backend.range(key.as_bytes());

        let mut catalog_names = HashSet::new();
        while let Some(r) = iter.next().await {
            let Kv(k, _) = r?;

            let catalog_key = String::from_utf8_lossy(&k);
            if let Ok(key) = CatalogKey::parse(catalog_key.as_ref()) {
                catalog_names.insert(key.catalog_name);
            } else {
                error!("invalid catalog key: {:?}", catalog_key);
            }
        }

        for catalog_name in catalog_names {
            if self.cache.get(&catalog_name).is_some() {
                continue;
            }

            let cache = Arc::new(
                CacheBuilder::new(1024)
                    .time_to_live(Duration::from_secs(30 * 60))
                    .time_to_idle(Duration::from_secs(5 * 60))
                    .build(),
            );

            let catalog_provider = Arc::new(FrontendCatalogProvider {
                catalog_name: catalog_name.clone(),
                backend: self.backend().clone(),
                partition_manager: self.partition_manager.clone(),
                datanode_clients: self.datanode_clients.clone(),
                cache,
            });

            self.cache.insert(catalog_name, catalog_provider);
        }
        Ok(())
    }
}

impl CatalogList for FrontendCatalogManager {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        _name: String,
        _catalog: CatalogProviderRef,
    ) -> CatalogResult<Option<CatalogProviderRef>> {
        unimplemented!("Frontend catalog list does not support register catalog")
    }

    fn catalog_names(&self) -> CatalogResult<Vec<String>> {
        self.refresh_catalogs()?;

        let catalog_names: Vec<String> = self
            .cache
            .iter()
            .map(|(name, _)| name.to_string())
            .collect();

        Ok(catalog_names)
    }

    fn catalog(&self, name: &str) -> CatalogResult<Option<CatalogProviderRef>> {
        // TODO(fys): Moka does not seem to have an "try_optionally_get_with" api.
        let catalog = self.cache.try_get_with_by_ref(name, || {
            self.refresh_catalogs()?;

            self.cache
                .get(name)
                .context(catalog_err::CatalogNotFoundSnafu { catalog_name: name })
        });

        if let Err(e) = catalog.as_ref() {
            if let catalog_err::Error::CatalogNotFound { .. } = e.deref() {
                return Ok(None);
            }
        }

        catalog.map(Some).map_err(|e| GetCache {
            err_msg: e.to_string(),
            backtrace: Backtrace::generate(),
        })
    }
}

pub struct FrontendCatalogProvider {
    catalog_name: String,
    backend: KvBackendRef,
    partition_manager: PartitionRuleManagerRef,
    datanode_clients: Arc<DatanodeClients>,
    cache: Arc<Cache<String, SchemaProviderRef>>,
}

impl FrontendCatalogProvider {
    fn refresh_schemas(&self) -> CatalogResult<()> {
        // TODO(fys): waiting for datafusion asynchronous api about catalog
        std::thread::scope(|s| {
            let join = s.spawn(|| {
                common_runtime::block_on_read(async move { self.async_refresh_schemas().await })
            });
            join.join().unwrap()
        })
    }

    async fn async_refresh_schemas(&self) -> CatalogResult<()> {
        let backend = self.backend.clone();
        let catalog_name = self.catalog_name.clone();
        let key = build_schema_prefix(&catalog_name);
        let mut iter = backend.range(key.as_bytes());
        let mut schema_names = HashSet::new();

        while let Some(r) = iter.next().await {
            let Kv(k, _) = r?;
            let key = SchemaKey::parse(String::from_utf8_lossy(&k))
                .context(catalog_err::InvalidCatalogValueSnafu)?;
            schema_names.insert(key.schema_name);
        }

        for schema_name in schema_names {
            if self.cache.get(&schema_name).is_some() {
                continue;
            }

            let cache = Arc::new(
                CacheBuilder::new(1024)
                    .time_to_live(Duration::from_secs(30 * 60))
                    .time_to_idle(Duration::from_secs(5 * 60))
                    .build(),
            );

            let schema_provider = Arc::new(FrontendSchemaProvider {
                catalog_name: self.catalog_name.clone(),
                schema_name: schema_name.clone(),
                backend: self.backend.clone(),
                partition_manager: self.partition_manager.clone(),
                datanode_clients: self.datanode_clients.clone(),
                cache,
            });

            self.cache.insert(schema_name, schema_provider);
        }

        Ok(())
    }
}

impl CatalogProvider for FrontendCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> CatalogResult<Vec<String>> {
        self.refresh_schemas()?;

        let schema_names: Vec<String> = self
            .cache
            .iter()
            .map(|(name, _)| name.to_string())
            .collect();

        Ok(schema_names)
    }

    fn register_schema(
        &self,
        _name: String,
        _schema: SchemaProviderRef,
    ) -> CatalogResult<Option<SchemaProviderRef>> {
        unimplemented!("Frontend catalog provider does not support register schema")
    }

    fn schema(&self, name: &str) -> CatalogResult<Option<SchemaProviderRef>> {
        let catalog = &self.catalog_name;

        // TODO(fys): Moka does not seem to have an "try_optionally_get_with" api.
        let schema = self.cache.try_get_with_by_ref(name, || {
            self.refresh_schemas()?;

            self.cache
                .get(name)
                .context(catalog_err::SchemaNotFoundSnafu {
                    catalog,
                    schema: name.to_string(),
                })
        });

        if let Err(e) = schema.as_ref() {
            if let catalog_err::Error::SchemaNotFound { .. } = e.deref() {
                return Ok(None);
            }
        }

        schema.map(Some).map_err(|e| GetCache {
            err_msg: e.to_string(),
            backtrace: Backtrace::generate(),
        })
    }
}

pub struct FrontendSchemaProvider {
    catalog_name: String,
    schema_name: String,
    backend: KvBackendRef,
    partition_manager: PartitionRuleManagerRef,
    datanode_clients: Arc<DatanodeClients>,
    cache: Arc<Cache<String, TableRef>>,
}

impl FrontendSchemaProvider {
    fn refresh_tables(&self) -> CatalogResult<()> {
        // TODO(fys): waiting for datafusion asynchronous api about catalog
        std::thread::scope(|s| {
            let join = s.spawn(|| {
                common_runtime::block_on_read(async move { self.async_refresh_tables().await })
            });
            join.join().unwrap()
        })
    }

    async fn async_refresh_tables(&self) -> CatalogResult<()> {
        let backend = self.backend.clone();
        let catalog_name = self.catalog_name.clone();
        let schema_name = self.schema_name.clone();

        let key = build_table_global_prefix(catalog_name, schema_name);
        let mut iter = backend.range(key.as_bytes());
        let mut table_names = HashSet::new();

        while let Some(r) = iter.next().await {
            let Kv(k, _) = r?;
            let key = TableGlobalKey::parse(String::from_utf8_lossy(&k))
                .context(catalog_err::InvalidCatalogValueSnafu)?;
            table_names.insert(key.table_name);
        }

        for table_name in table_names {
            if self.cache.get(&table_name).is_some() {
                continue;
            }

            let table_global_key = TableGlobalKey {
                catalog_name: self.catalog_name.clone(),
                schema_name: self.schema_name.clone(),
                table_name: table_name.clone(),
            };

            let Some(kv) = self.backend.get(table_global_key.to_string().as_bytes()).await? else { continue };

            let tg_val = TableGlobalValue::from_bytes(kv.1)
                .context(catalog_err::InvalidCatalogValueSnafu)?;

            let table_info = Arc::new(
                tg_val
                    .table_info
                    .try_into()
                    .context(catalog_err::InvalidTableInfoInCatalogSnafu)?,
            );

            let table = Arc::new(DistTable::new(
                TableName::new(&self.catalog_name, &self.schema_name, table_name.clone()),
                table_info,
                self.partition_manager.clone(),
                self.datanode_clients.clone(),
                self.backend.clone(),
            ));

            self.cache.insert(table_name, table);
        }

        Ok(())
    }
}

#[async_trait]
impl SchemaProvider for FrontendSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> CatalogResult<Vec<String>> {
        self.refresh_tables()?;

        let table_names: Vec<String> = self
            .cache
            .iter()
            .map(|(name, _)| name.to_string())
            .collect();

        Ok(table_names)
    }

    async fn table(&self, name: &str) -> CatalogResult<Option<TableRef>> {
        // TODO(fys): Moka does not seem to have an "try_optionally_get_with" api.
        let table = self.cache.try_get_with_by_ref(name, || {
            self.refresh_tables()?;

            self.cache
                .get(name)
                .context(catalog_err::TableNotExistSnafu {
                    table: name.to_string(),
                })
        });

        if let Err(e) = table.as_ref() {
            if let catalog_err::Error::TableNotExist { .. } = e.deref() {
                return Ok(None);
            }
        }

        table.map(Some).map_err(|e| GetCache {
            err_msg: e.to_string(),
            backtrace: Backtrace::generate(),
        })
    }

    fn register_table(&self, _name: String, _table: TableRef) -> CatalogResult<Option<TableRef>> {
        unimplemented!("Frontend schema provider does not support register table")
    }

    fn rename_table(&self, _name: &str, _new_name: String) -> catalog_err::Result<TableRef> {
        unimplemented!("Frontend schema provider does not support rename table")
    }

    fn deregister_table(&self, _name: &str) -> CatalogResult<Option<TableRef>> {
        unimplemented!("Frontend schema provider does not support deregister table")
    }

    fn table_exist(&self, name: &str) -> CatalogResult<bool> {
        Ok(self.table_names()?.contains(&name.to_string()))
    }
}
