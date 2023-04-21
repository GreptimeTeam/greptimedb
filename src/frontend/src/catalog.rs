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

use api::v1::CreateTableExpr;
use async_trait::async_trait;
use catalog::error::{
    self as catalog_err, InternalSnafu, InvalidCatalogValueSnafu, InvalidSystemTableDefSnafu,
    Result as CatalogResult, UnimplementedSnafu,
};
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
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_error::prelude::BoxedError;
use common_telemetry::error;
use futures::StreamExt;
use meta_client::rpc::TableName;
use partition::manager::PartitionRuleManagerRef;
use snafu::prelude::*;
use table::table::numbers::NumbersTable;
use table::TableRef;

use crate::datanode::DatanodeClients;
use crate::expr_factory;
use crate::instance::distributed::DistInstance;
use crate::table::DistTable;

#[derive(Clone)]
pub struct FrontendCatalogManager {
    backend: KvBackendRef,
    partition_manager: PartitionRuleManagerRef,
    datanode_clients: Arc<DatanodeClients>,

    // TODO(LFC): Remove this field.
    // DistInstance in FrontendCatalogManager is only used for creating distributed script table now.
    // Once we have some standalone distributed table creator (like create distributed table procedure),
    // we should use that.
    dist_instance: Option<Arc<DistInstance>>,
}

impl FrontendCatalogManager {
    pub fn new(
        backend: KvBackendRef,
        partition_manager: PartitionRuleManagerRef,
        datanode_clients: Arc<DatanodeClients>,
    ) -> Self {
        Self {
            backend,
            partition_manager,
            datanode_clients,
            dist_instance: None,
        }
    }

    pub(crate) fn set_dist_instance(&mut self, dist_instance: Arc<DistInstance>) {
        self.dist_instance = Some(dist_instance)
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
        request: RegisterSystemTableRequest,
    ) -> catalog::error::Result<()> {
        if let Some(dist_instance) = &self.dist_instance {
            let open_hook = request.open_hook;
            let request = request.create_table_request;

            if let Some(table) = self
                .table(
                    &request.catalog_name,
                    &request.schema_name,
                    &request.table_name,
                )
                .await?
            {
                if let Some(hook) = open_hook {
                    (hook)(table)?;
                }
                return Ok(());
            }

            let time_index = request
                .schema
                .column_schemas
                .iter()
                .find_map(|x| {
                    if x.is_time_index() {
                        Some(x.name.clone())
                    } else {
                        None
                    }
                })
                .context(InvalidSystemTableDefSnafu {
                    err_msg: "Time index is not defined.",
                })?;

            let primary_keys = request
                .schema
                .column_schemas
                .iter()
                .enumerate()
                .filter_map(|(i, x)| {
                    if request.primary_key_indices.contains(&i) {
                        Some(x.name.clone())
                    } else {
                        None
                    }
                })
                .collect();

            let column_defs = expr_factory::column_schemas_to_defs(request.schema.column_schemas)
                .map_err(|e| {
                InvalidSystemTableDefSnafu {
                    err_msg: e.to_string(),
                }
                .build()
            })?;

            let mut create_table = CreateTableExpr {
                catalog_name: request.catalog_name,
                schema_name: request.schema_name,
                table_name: request.table_name,
                desc: request.desc.unwrap_or("".to_string()),
                column_defs,
                time_index,
                primary_keys,
                create_if_not_exists: request.create_if_not_exists,
                table_options: (&request.table_options).into(),
                table_id: None, // Should and will be assigned by Meta.
                region_ids: vec![0],
                engine: request.engine,
            };

            let table = dist_instance
                .create_table(&mut create_table, None)
                .await
                .map_err(BoxedError::new)
                .context(InternalSnafu)?;

            if let Some(hook) = open_hook {
                (hook)(table)?;
            }
            Ok(())
        } else {
            UnimplementedSnafu {
                operation: "register system table",
            }
            .fail()
        }
    }

    async fn catalog_async(&self, catalog: &str) -> CatalogResult<Option<CatalogProviderRef>> {
        // get from kv
        let key = CatalogKey {
            catalog_name: catalog.to_string(),
        }
        .to_string();
        let res = self.backend.get(key.as_bytes()).await?.map(|_| {
            Arc::new(FrontendCatalogProvider {
                catalog_name: catalog.to_string(),
                backend: self.backend.clone(),
                partition_manager: self.partition_manager.clone(),
                datanode_clients: self.datanode_clients.clone(),
            }) as Arc<_>
        });
        Ok(res)
    }

    async fn schema_async(
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

    async fn table(
        &self,
        catalog: &str,
        schema: &str,
        table_name: &str,
    ) -> catalog::error::Result<Option<TableRef>> {
        self.schema_async(catalog, schema)
            .await?
            .context(catalog::error::SchemaNotFoundSnafu { catalog, schema })?
            .table(table_name)
            .await
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

#[async_trait]
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
                let mut tables = vec![];
                if catalog_name == DEFAULT_CATALOG_NAME && schema_name == DEFAULT_SCHEMA_NAME {
                    tables.push("numbers".to_string());
                }

                let key = build_table_global_prefix(catalog_name, schema_name);
                let mut iter = backend.range(key.as_bytes());

                while let Some(r) = iter.next().await {
                    let Kv(k, _) = r?;
                    let key = TableGlobalKey::parse(String::from_utf8_lossy(&k))
                        .context(InvalidCatalogValueSnafu)?;
                    tables.push(key.table_name);
                }
                Ok(tables)
            })
        })
        .join()
        .unwrap()
    }

    async fn table(&self, name: &str) -> catalog::error::Result<Option<TableRef>> {
        if self.catalog_name == DEFAULT_CATALOG_NAME
            && self.schema_name == DEFAULT_SCHEMA_NAME
            && name == "numbers"
        {
            return Ok(Some(Arc::new(NumbersTable::default())));
        }

        let table_global_key = TableGlobalKey {
            catalog_name: self.catalog_name.clone(),
            schema_name: self.schema_name.clone(),
            table_name: name.to_string(),
        };
        let Some(kv) = self.backend.get(table_global_key.to_string().as_bytes()).await? else { return Ok(None) };
        let v = TableGlobalValue::from_bytes(kv.1).context(InvalidCatalogValueSnafu)?;
        let table_info = Arc::new(
            v.table_info
                .try_into()
                .context(catalog_err::InvalidTableInfoInCatalogSnafu)?,
        );
        let table = Arc::new(DistTable::new(
            TableName::new(&self.catalog_name, &self.schema_name, name),
            table_info,
            self.partition_manager.clone(),
            self.datanode_clients.clone(),
            self.backend.clone(),
        ));
        Ok(Some(table))
    }

    fn table_exist(&self, name: &str) -> catalog::error::Result<bool> {
        Ok(self.table_names()?.contains(&name.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, MITO_ENGINE};
    use script::table::{build_scripts_schema, SCRIPTS_TABLE_NAME};
    use table::requests::{CreateTableRequest, TableOptions};

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_register_system_table() {
        let instance =
            crate::tests::create_distributed_instance("test_register_system_table").await;

        let catalog_name = DEFAULT_CATALOG_NAME;
        let schema_name = DEFAULT_SCHEMA_NAME;
        let table_name = SCRIPTS_TABLE_NAME;
        let request = CreateTableRequest {
            id: 1,
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
            desc: Some("Scripts table".to_string()),
            schema: build_scripts_schema(),
            region_numbers: vec![0],
            primary_key_indices: vec![0, 1],
            create_if_not_exists: true,
            table_options: TableOptions::default(),
            engine: MITO_ENGINE.to_string(),
        };

        let result = instance
            .catalog_manager
            .register_system_table(RegisterSystemTableRequest {
                create_table_request: request,
                open_hook: None,
            })
            .await;
        assert!(result.is_ok());

        assert!(
            instance
                .catalog_manager
                .table(catalog_name, schema_name, table_name)
                .await
                .unwrap()
                .is_some(),
            "the registered system table cannot be found in catalog"
        );

        let mut actually_created_table_in_datanode = 0;
        for datanode in instance.datanodes.values() {
            if datanode
                .catalog_manager()
                .table(catalog_name, schema_name, table_name)
                .await
                .unwrap()
                .is_some()
            {
                actually_created_table_in_datanode += 1;
            }
        }
        assert_eq!(
            actually_created_table_in_datanode, 1,
            "system table should be actually created at one and only one datanode"
        )
    }
}
