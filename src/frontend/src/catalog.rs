use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use catalog::error::{
    DeserializePartitionRuleSnafu, InvalidCatalogValueSnafu, InvalidSchemaInCatalogSnafu,
};
use catalog::remote::{Kv, KvBackendRef};
use catalog::{
    CatalogList, CatalogProvider, CatalogProviderRef, SchemaProvider, SchemaProviderRef,
};
use common_catalog::{CatalogKey, SchemaKey, TableGlobalKey, TableGlobalValue};
use futures::StreamExt;
use meta_client::rpc::TableName;
use snafu::prelude::*;
use table::TableRef;

use crate::datanode::DatanodeClients;
use crate::partitioning::range::RangePartitionRule;
use crate::table::route::TableRoutes;
use crate::table::DistTable;

#[derive(Clone)]
pub struct FrontendCatalogManager {
    backend: KvBackendRef,
    table_routes: Arc<TableRoutes>,
    datanode_clients: Arc<DatanodeClients>,
}

impl FrontendCatalogManager {
    pub(crate) fn new(
        backend: KvBackendRef,
        table_routes: Arc<TableRoutes>,
        datanode_clients: Arc<DatanodeClients>,
    ) -> Self {
        Self {
            backend,
            table_routes,
            datanode_clients,
        }
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
                let key = common_catalog::build_catalog_prefix();
                let mut iter = backend.range(key.as_bytes());
                let mut res = HashSet::new();

                while let Some(r) = iter.next().await {
                    let Kv(k, _) = r?;
                    let key = CatalogKey::parse(String::from_utf8_lossy(&k))
                        .context(InvalidCatalogValueSnafu)?;
                    res.insert(key.catalog_name);
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
                table_routes: self.table_routes.clone(),
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
    table_routes: Arc<TableRoutes>,
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
                let key = common_catalog::build_schema_prefix(&catalog_name);
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
                table_routes: self.table_routes.clone(),
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
    table_routes: Arc<TableRoutes>,
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
                let key = common_catalog::build_table_global_prefix(catalog_name, schema_name);
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
        let table_routes = self.table_routes.clone();
        let datanode_clients = self.datanode_clients.clone();
        let table_name = TableName::new(&self.catalog_name, &self.schema_name, name);
        let result: Result<Option<TableRef>, catalog::error::Error> = std::thread::spawn(|| {
            common_runtime::block_on_read(async move {
                let res = match backend.get(table_global_key.to_string().as_bytes()).await? {
                    None => {
                        return Ok(None);
                    }
                    Some(r) => r,
                };
                let val = TableGlobalValue::parse(String::from_utf8_lossy(&res.1))
                    .context(InvalidCatalogValueSnafu)?;

                // TODO(hl): We need to deserialize string to PartitionRule trait object
                let partition_rule: Arc<RangePartitionRule> =
                    Arc::new(serde_json::from_str(&val.partition_rules).context(
                        DeserializePartitionRuleSnafu {
                            data: &val.partition_rules,
                        },
                    )?);

                let table = Arc::new(DistTable {
                    table_name,
                    schema: Arc::new(
                        val.meta
                            .schema
                            .try_into()
                            .context(InvalidSchemaInCatalogSnafu)?,
                    ),
                    partition_rule,
                    table_routes,
                    datanode_clients,
                });
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

    fn deregister_table(&self, _name: &str) -> catalog::error::Result<Option<TableRef>> {
        unimplemented!("Frontend schema provider does not support deregister table")
    }

    fn table_exist(&self, name: &str) -> catalog::error::Result<bool> {
        Ok(self.table_names()?.contains(&name.to_string()))
    }
}
