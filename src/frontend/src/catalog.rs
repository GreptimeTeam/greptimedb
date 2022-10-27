use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use catalog::remote::{Kv, KvBackendRef};
use catalog::{
    CatalogList, CatalogListRef, CatalogProvider, CatalogProviderRef, SchemaProvider,
    SchemaProviderRef,
};
use client::Database;
use common_catalog::{CatalogKey, SchemaKey, TableKey, TableValue};
use futures::StreamExt;
use table::TableRef;
use tokio::sync::{Mutex, RwLock};

use crate::error::Result;
use crate::mock::{Datanode, DatanodeInstance, RangePartitionRule, Region};
use crate::table::DistTable;

pub struct FrontendCatalogList {
    backend: KvBackendRef,
    range_rules: Arc<RwLock<HashMap<String, RangePartitionRule>>>,
    datanode_instances: Arc<RwLock<HashMap<u64, Database>>>,
}

impl FrontendCatalogList {
    pub fn new(
        backend: KvBackendRef,
        range_rules: Arc<RwLock<HashMap<String, RangePartitionRule>>>,
        datanode_instances: Arc<RwLock<HashMap<u64, Database>>>,
    ) -> Self {
        Self {
            backend,
            range_rules,
            datanode_instances,
        }
    }
}

impl CatalogList for FrontendCatalogList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: CatalogProviderRef,
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
                    let Kv(k, _) = r.unwrap();
                    if !k.starts_with(key.as_bytes()) {
                        continue;
                    }
                    let key = CatalogKey::parse(String::from_utf8_lossy(&k)).unwrap();
                    res.insert(key.catalog_name);
                }
                res
            })
        })
        .join()
        .unwrap();
        Ok(res.into_iter().collect())
    }

    fn catalog(&self, name: &str) -> catalog::error::Result<Option<CatalogProviderRef>> {
        let all_catalogs = self.catalog_names()?;
        if all_catalogs.contains(&name.to_string()) {
            Ok(Some(Arc::new(FrontendCatalogProvider {
                catalog_name: name.to_string(),
                backend: self.backend.clone(),
                range_rules: self.range_rules.clone(),
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
    range_rules: Arc<RwLock<HashMap<String, RangePartitionRule>>>,
    datanode_instances: Arc<RwLock<HashMap<u64, Database>>>,
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
                    let Kv(k, _) = r.unwrap();
                    if !k.starts_with(key.as_bytes()) {
                        continue;
                    }
                    let key = SchemaKey::parse(String::from_utf8_lossy(&k)).unwrap();
                    res.insert(key.schema_name);
                }
                res
            })
        })
        .join()
        .unwrap();
        Ok(res.into_iter().collect())
    }

    fn register_schema(
        &self,
        name: String,
        schema: SchemaProviderRef,
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
                range_rules: self.range_rules.clone(),
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
    range_rules: Arc<RwLock<HashMap<String, RangePartitionRule>>>,
    datanode_instances: Arc<RwLock<HashMap<u64, Database>>>,
}

impl FrontendSchemaProvider {
    fn build_query_catalog(&self) -> CatalogListRef {
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

        let res = std::thread::spawn(|| {
            common_runtime::block_on_read(async move {
                let key = common_catalog::build_table_prefix(catalog_name, schema_name);
                let mut iter = backend.range(key.as_bytes());
                let mut res = HashSet::new();

                while let Some(r) = iter.next().await {
                    let Kv(k, _) = r.unwrap();
                    if !k.starts_with(key.as_bytes()) {
                        continue;
                    }
                    let key = TableKey::parse(String::from_utf8_lossy(&k)).unwrap();
                    res.insert(key.table_name);
                }
                res
            })
        })
        .join()
        .unwrap();
        Ok(res.into_iter().collect())
    }

    fn table(&self, name: &str) -> catalog::error::Result<Option<TableRef>> {
        let mut table_prefix =
            common_catalog::build_table_prefix(&self.catalog_name, &self.schema_name);
        table_prefix.push_str(name);
        table_prefix.push_str("-");

        let catalog_name = self.catalog_name.clone();
        let schema_name = self.schema_name.clone();
        let catalog_ref = self.build_query_catalog();
        let instances = self.datanode_instances.clone();
        let backend = self.backend.clone();

        let range_rules = self.range_rules.clone();

        let table_name = name.to_string();
        let result: Result<Option<TableRef>> = std::thread::spawn(|| {
            common_runtime::block_on_read(async move {
                let range_rules = match range_rules.read().await.get(&table_name) {
                    None => panic!("Please call create table first!"),
                    Some(r) => r.clone(),
                };

                let mut datanode_instances = HashMap::new();
                let mut iter = backend.range(table_prefix.as_bytes());

                let mut region_to_datanode_map = HashMap::new();
                let mut schema_opt = None;
                while let Some(r) = iter.next().await {
                    let Kv(k, v) = r.unwrap();
                    if !k.starts_with(table_prefix.as_bytes()) {
                        continue;
                    }
                    let key = TableKey::parse(String::from_utf8_lossy(&k)).unwrap();
                    let val = TableValue::parse(String::from_utf8_lossy(&v)).unwrap();
                    let node_id = val.node_id;
                    let region_ids = val.regions_ids;
                    let database = match instances.read().await.get(&node_id) {
                        None => {
                            return Ok(None);
                        }
                        Some(datanode) => datanode.clone(),
                    };

                    for region_id in region_ids {
                        region_to_datanode_map
                            .insert(Region::new(region_id), Datanode::new(node_id));
                    }
                    schema_opt = Some(val.meta.schema.clone());
                    let instance = DatanodeInstance::new(catalog_ref.clone(), database);
                    datanode_instances.insert(Datanode::new(node_id), instance);
                }

                let table = Arc::new(DistTable {
                    table_name: table_name.clone(),
                    schema: schema_opt.unwrap(),
                    partition_rule: range_rules,
                    region_dist_map: region_to_datanode_map,
                    datanode_instances: Arc::new(Mutex::new(datanode_instances)),
                });

                catalog_ref
                    .catalog(&catalog_name)
                    .unwrap()
                    .unwrap()
                    .schema(&schema_name)
                    .unwrap()
                    .unwrap()
                    .register_table(table_name, table.clone())
                    .unwrap();
                Ok(Some(table as _))
            })
        })
        .join()
        .unwrap();
        Ok(result.unwrap())
    }

    fn register_table(
        &self,
        name: String,
        table: TableRef,
    ) -> catalog::error::Result<Option<TableRef>> {
        unimplemented!("Frontend schema provider does not support register table")
    }

    fn deregister_table(&self, name: &str) -> catalog::error::Result<Option<TableRef>> {
        unimplemented!("Frontend schema provider does not support deregister table")
    }

    fn table_exist(&self, name: &str) -> catalog::error::Result<bool> {
        Ok(self.table_names()?.contains(&name.to_string()))
    }
}
