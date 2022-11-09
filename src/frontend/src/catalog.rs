use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use catalog::error::{
    DeserializePartitionRuleSnafu, InvalidCatalogValueSnafu, InvalidSchemaInCatalogSnafu,
};
use catalog::remote::{Kv, KvBackendRef};
use catalog::{
    CatalogList, CatalogManagerRef, CatalogProvider, CatalogProviderRef, SchemaProvider,
    SchemaProviderRef,
};
use common_catalog::{CatalogKey, SchemaKey, TableGlobalKey, TableGlobalValue};
use common_error::ext::BoxedError;
use futures::StreamExt;
use snafu::{OptionExt, ResultExt};
use table::TableRef;
use tokio::sync::RwLock;

use crate::error::DatanodeNotAvailableSnafu;
use crate::mock::{DatanodeId, DatanodeInstance};
use crate::partitioning::range::RangePartitionRule;
use crate::table::DistTable;

pub type DatanodeInstances = HashMap<DatanodeId, DatanodeInstance>;

pub struct FrontendCatalogManager {
    backend: KvBackendRef,
    datanode_instances: Arc<RwLock<DatanodeInstances>>,
}

impl FrontendCatalogManager {
    #[allow(dead_code)]
    pub fn new(backend: KvBackendRef, datanode_instances: Arc<RwLock<DatanodeInstances>>) -> Self {
        Self {
            backend,
            datanode_instances,
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
                datanode_instances: self.datanode_instances.clone(),
            })))
        } else {
            Ok(None)
        }
    }
}

pub struct FrontendCatalogProvider {
    catalog_name: String,
    backend: KvBackendRef,
    datanode_instances: Arc<RwLock<DatanodeInstances>>,
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
                datanode_instances: self.datanode_instances.clone(),
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
    datanode_instances: Arc<RwLock<DatanodeInstances>>,
}

impl FrontendSchemaProvider {
    fn build_query_catalog(&self) -> CatalogManagerRef {
        let catalog_list = catalog::local::new_memory_catalog_list().unwrap();
        let catalog = Arc::new(catalog::local::memory::MemoryCatalogProvider::new());
        let schema = Arc::new(catalog::local::memory::MemorySchemaProvider::new());
        catalog
            .register_schema(self.schema_name.clone(), schema)
            .unwrap();
        catalog_list
            .register_catalog(self.catalog_name.clone(), catalog)
            .unwrap();
        catalog_list
    }
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

        let catalog_ref = self.build_query_catalog();
        let instances = self.datanode_instances.clone();
        let backend = self.backend.clone();
        let table_name = name.to_string();
        let result: Result<Option<TableRef>, catalog::error::Error> = std::thread::spawn(|| {
            common_runtime::block_on_read(async move {
                let mut datanode_instances = HashMap::new();
                let res = match backend.get(table_global_key.to_string().as_bytes()).await? {
                    None => {
                        return Ok(None);
                    }
                    Some(r) => r,
                };

                let mut region_to_datanode_map = HashMap::new();

                let val = TableGlobalValue::parse(String::from_utf8_lossy(&res.1))
                    .context(InvalidCatalogValueSnafu)?;
                let node_id: DatanodeId = val.node_id;

                // TODO(hl): We need to deserialize string to PartitionRule trait object
                let partition_rule: Arc<RangePartitionRule> =
                    Arc::new(serde_json::from_str(&val.partition_rules).context(
                        DeserializePartitionRuleSnafu {
                            data: &val.partition_rules,
                        },
                    )?);

                for (node_id, region_ids) in val.regions_id_map {
                    for region_id in region_ids {
                        region_to_datanode_map.insert(region_id, node_id);
                    }
                }

                datanode_instances.insert(
                    node_id,
                    instances
                        .read()
                        .await
                        .get(&node_id)
                        .context(DatanodeNotAvailableSnafu { node_id })
                        .map_err(BoxedError::new)
                        .context(catalog::error::InternalSnafu)?
                        .clone(),
                );

                let table = Arc::new(DistTable {
                    table_name: table_name.clone(),
                    schema: Arc::new(
                        val.meta
                            .schema
                            .try_into()
                            .context(InvalidSchemaInCatalogSnafu)?,
                    ),
                    partition_rule,
                    region_dist_map: region_to_datanode_map,
                    datanode_instances,
                });

                catalog_ref
                    .catalog(&table_global_key.catalog_name)
                    .unwrap()
                    .unwrap()
                    .schema(&table_global_key.schema_name)
                    .unwrap()
                    .unwrap()
                    .register_table(table_name, table.clone())
                    .unwrap();
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
