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
use std::sync::Arc;

use catalog::error::{self as catalog_err, InvalidCatalogValueSnafu, Result as CatalogResult};
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
use partition::manager::PartitionRuleManagerRef;
use snafu::prelude::*;
use table::TableRef;

use crate::datanode::DatanodeClients;
use crate::table::DistTable;

#[derive(Clone)]
pub struct FrontendCatalogManager {
    backend: KvBackendRef,
    partition_manager: PartitionRuleManagerRef,
    datanode_clients: Arc<DatanodeClients>,
}

impl FrontendCatalogManager {
    pub(crate) fn new(
        backend: KvBackendRef,
        partition_manager: PartitionRuleManagerRef,
        datanode_clients: Arc<DatanodeClients>,
    ) -> Self {
        Self {
            backend,
            partition_manager,
            datanode_clients,
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
    async fn start(&self) -> catalog::error::Result<()> {
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

    async fn register_schema(
        &self,
        _request: RegisterSchemaRequest,
    ) -> catalog::error::Result<bool> {
        unimplemented!()
    }

    async fn rename_table(&self, _request: RenameTableRequest) -> catalog_err::Result<bool> {
        unimplemented!()
    }

    async fn register_system_table(
        &self,
        _request: RegisterSystemTableRequest,
    ) -> catalog::error::Result<()> {
        unimplemented!()
    }

    fn schema(
        &self,
        catalog: &str,
        schema: &str,
    ) -> catalog::error::Result<Option<SchemaProviderRef>> {
        self.catalog(catalog)?
            .context(catalog::error::CatalogNotFoundSnafu {
                catalog_name: catalog,
            })?
            .schema(schema)
    }

    fn table(
        &self,
        catalog: &str,
        schema: &str,
        table_name: &str,
    ) -> catalog::error::Result<Option<TableRef>> {
        self.schema(catalog, schema)?
            .context(catalog::error::SchemaNotFoundSnafu { catalog, schema })?
            .table(table_name)
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
    ) -> catalog::error::Result<Option<CatalogProviderRef>> {
        unimplemented!("Frontend catalog list does not support register catalog")
    }

    fn catalog_names(&self) -> catalog::error::Result<Vec<String>> {
        let backend = self.backend.clone();
        let res = std::thread::spawn(|| {
            common_runtime::block_on_read(async move {
                let key = build_catalog_prefix();
                let mut iter = backend.range(key.as_bytes());
                let mut res = HashSet::new();

                while let Some(r) = iter.next().await {
                    let Kv(k, _) = r?;

                    let catalog_key = String::from_utf8_lossy(&k);
                    if let Ok(key) = CatalogKey::parse(catalog_key.as_ref()) {
                        res.insert(key.catalog_name);
                    } else {
                        error!("invalid catalog key: {:?}", catalog_key);
                    }
                }
                Ok(res.into_iter().collect())
            })
        })
        .join()
        .unwrap();
        res
    }

    fn catalog(&self, name: &str) -> catalog::error::Result<Option<CatalogProviderRef>> {
        let all_catalogs = self.catalog_names()?;
        if all_catalogs.contains(&name.to_string()) {
            Ok(Some(Arc::new(FrontendCatalogProvider {
                catalog_name: name.to_string(),
                backend: self.backend.clone(),
                partition_manager: self.partition_manager.clone(),
                datanode_clients: self.datanode_clients.clone(),
            })))
        } else {
            Ok(None)
        }
    }
}

pub struct FrontendCatalogProvider {
    catalog_name: String,
    backend: KvBackendRef,
    partition_manager: PartitionRuleManagerRef,
    datanode_clients: Arc<DatanodeClients>,
}

impl CatalogProvider for FrontendCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> catalog::error::Result<Vec<String>> {
        let backend = self.backend.clone();
        let catalog_name = self.catalog_name.clone();
        let res = std::thread::spawn(|| {
            common_runtime::block_on_read(async move {
                let key = build_schema_prefix(&catalog_name);
                let mut iter = backend.range(key.as_bytes());
                let mut res = HashSet::new();

                while let Some(r) = iter.next().await {
                    let Kv(k, _) = r?;
                    let key = SchemaKey::parse(String::from_utf8_lossy(&k))
                        .context(InvalidCatalogValueSnafu)?;
                    res.insert(key.schema_name);
                }
                Ok(res.into_iter().collect())
            })
        })
        .join()
        .unwrap();
        res
    }

    fn register_schema(
        &self,
        _name: String,
        _schema: SchemaProviderRef,
    ) -> catalog::error::Result<Option<SchemaProviderRef>> {
        unimplemented!("Frontend catalog provider does not support register schema")
    }

    fn schema(&self, name: &str) -> catalog::error::Result<Option<SchemaProviderRef>> {
        let all_schemas = self.schema_names()?;
        if all_schemas.contains(&name.to_string()) {
            Ok(Some(Arc::new(FrontendSchemaProvider {
                catalog_name: self.catalog_name.clone(),
                schema_name: name.to_string(),
                backend: self.backend.clone(),
                partition_manager: self.partition_manager.clone(),
                datanode_clients: self.datanode_clients.clone(),
            })))
        } else {
            Ok(None)
        }
    }
}

pub struct FrontendSchemaProvider {
    catalog_name: String,
    schema_name: String,
    backend: KvBackendRef,
    partition_manager: PartitionRuleManagerRef,
    datanode_clients: Arc<DatanodeClients>,
}

impl SchemaProvider for FrontendSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> catalog::error::Result<Vec<String>> {
        let backend = self.backend.clone();
        let catalog_name = self.catalog_name.clone();
        let schema_name = self.schema_name.clone();

        std::thread::spawn(|| {
            common_runtime::block_on_read(async move {
                let key = build_table_global_prefix(catalog_name, schema_name);
                let mut iter = backend.range(key.as_bytes());
                let mut res = HashSet::new();

                while let Some(r) = iter.next().await {
                    let Kv(k, _) = r?;
                    let key = TableGlobalKey::parse(String::from_utf8_lossy(&k))
                        .context(InvalidCatalogValueSnafu)?;
                    res.insert(key.table_name);
                }
                Ok(res.into_iter().collect())
            })
        })
        .join()
        .unwrap()
    }

    fn table(&self, name: &str) -> catalog::error::Result<Option<TableRef>> {
        let table_global_key = TableGlobalKey {
            catalog_name: self.catalog_name.clone(),
            schema_name: self.schema_name.clone(),
            table_name: name.to_string(),
        };

        let backend = self.backend.clone();
        let partition_manager = self.partition_manager.clone();
        let datanode_clients = self.datanode_clients.clone();
        let table_name = TableName::new(&self.catalog_name, &self.schema_name, name);
        let result: CatalogResult<Option<TableRef>> = std::thread::spawn(|| {
            common_runtime::block_on_read(async move {
                let res = match backend.get(table_global_key.to_string().as_bytes()).await? {
                    None => {
                        return Ok(None);
                    }
                    Some(r) => r,
                };
                let val = TableGlobalValue::from_bytes(res.1).context(InvalidCatalogValueSnafu)?;

                let table = Arc::new(DistTable::new(
                    table_name,
                    Arc::new(
                        val.table_info
                            .try_into()
                            .context(catalog_err::InvalidTableInfoInCatalogSnafu)?,
                    ),
                    partition_manager,
                    datanode_clients,
                    backend,
                ));
                Ok(Some(table as _))
            })
        })
        .join()
        .unwrap();
        result
    }

    fn register_table(
        &self,
        _name: String,
        _table: TableRef,
    ) -> catalog::error::Result<Option<TableRef>> {
        unimplemented!("Frontend schema provider does not support register table")
    }

    fn rename_table(&self, _name: &str, _new_name: String) -> catalog_err::Result<TableRef> {
        unimplemented!("Frontend schema provider does not support rename table")
    }

    fn deregister_table(&self, _name: &str) -> catalog::error::Result<Option<TableRef>> {
        unimplemented!("Frontend schema provider does not support deregister table")
    }

    fn table_exist(&self, name: &str) -> catalog::error::Result<bool> {
        Ok(self.table_names()?.contains(&name.to_string()))
    }
}
