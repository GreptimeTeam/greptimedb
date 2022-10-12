use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use futures_util::StreamExt;
use table::metadata::TableId;
use table::TableRef;
use tokio::sync::{Mutex, RwLock};

use crate::error::Error;
use crate::remote::client::MetaKvBackend;
use crate::remote::helper::{
    build_catalog_prefix, build_schema_prefix, build_table_prefix, CatalogKey, SchemaKey, TableKey,
};
use crate::remote::KvBackend;
use crate::{
    CatalogManager, CatalogProviderRef, RegisterSystemTableRequest, RegisterTableRequest,
    SchemaProvider, SchemaProviderRef,
};

pub struct RemoteCatalogManager {
    node_id: String,
    backend: Arc<MetaKvBackend>,
    catalogs: Arc<RwLock<HashMap<String, CatalogProviderRef>>>,
    #[allow(unused)]
    table_id: Mutex<AtomicU32>, // table id should be calculated on startup
}

impl RemoteCatalogManager {
    fn catalog_key(&self, catalog_name: impl AsRef<str>) -> CatalogKey {
        CatalogKey {
            catalog_name: catalog_name.as_ref().to_string(),
            node_id: self.node_id.clone(),
        }
    }
}

impl crate::CatalogList for RemoteCatalogManager {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: CatalogProviderRef,
    ) -> Result<Option<CatalogProviderRef>, Error> {
        futures::executor::block_on(async move {
            let key = self.catalog_key(&name).to_string();
            let prev = match self.backend.get(key.as_bytes()).await? {
                None => None,
                Some(_) => self.catalogs.read().await.get(&name).cloned(),
            };

            // TODO(hl): change value
            self.backend.set(key.as_bytes(), "".as_bytes()).await?;
            let mut catalogs = self.catalogs.write().await;
            catalogs.insert(name, catalog);
            Ok(prev)
        })
    }

    fn catalog_names(&self) -> Result<Vec<String>, Error> {
        futures::executor::block_on(async move {
            let mut res = HashSet::new();
            while let Some(v) = self
                .backend
                .range(build_catalog_prefix().as_bytes())
                .next()
                .await
            {
                let CatalogKey {
                    node_id,
                    catalog_name,
                } = CatalogKey::parse(&String::from_utf8_lossy(&v?.0))?;

                if node_id == self.node_id {
                    res.insert(catalog_name);
                }
            }
            Ok(res.into_iter().collect())
        })
    }

    fn catalog(&self, name: &str) -> Result<Option<CatalogProviderRef>, Error> {
        futures::executor::block_on(async move {
            let key = CatalogKey {
                catalog_name: name.to_string(),
                node_id: self.node_id.clone(),
            }
            .to_string();

            match self.backend.get(key.as_bytes()).await? {
                None => Ok(None),
                Some(_) => Ok(self.catalogs.read().await.get(name).cloned()),
            }
        })
    }
}

#[async_trait::async_trait]
impl CatalogManager for RemoteCatalogManager {
    async fn start(&self) -> crate::error::Result<()> {
        todo!()
    }

    async fn next_table_id(&self) -> TableId {
        todo!()
    }

    async fn register_table(&self, request: RegisterTableRequest) -> crate::error::Result<usize> {
        let _ = request;
        todo!()
    }

    async fn register_system_table(
        &self,
        request: RegisterSystemTableRequest,
    ) -> crate::error::Result<()> {
        let _ = request;
        todo!()
    }

    fn table(
        &self,
        catalog: Option<&str>,
        schema: Option<&str>,
        table_name: &str,
    ) -> crate::error::Result<Option<TableRef>> {
        let _ = catalog;
        let _ = schema;
        let _ = table_name;
        todo!()
    }
}

pub struct RemoteCatalogProvider {
    catalog_name: String,
    node_id: String,
    backend: Arc<MetaKvBackend>,
    schemas: Arc<RwLock<HashMap<String, SchemaProviderRef>>>,
}

impl RemoteCatalogProvider {
    fn schema_key(&self, schema_name: impl AsRef<str>) -> SchemaKey {
        SchemaKey {
            catalog_name: self.catalog_name.clone(),
            schema_name: schema_name.as_ref().to_string(),
            node_id: self.node_id.clone(),
        }
    }
}

impl crate::CatalogProvider for RemoteCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Result<Vec<String>, Error> {
        let key_prefix = build_schema_prefix(&self.catalog_name);
        futures::executor::block_on(async move {
            let mut res = HashSet::new();
            let mut iter = self.backend.range(key_prefix.as_bytes());
            while let Some(r) = iter.next().await {
                let kv = r?;
                let key = String::from_utf8_lossy(&kv.0).to_string();
                let SchemaKey {
                    node_id,
                    schema_name,
                    catalog_name,
                } = SchemaKey::parse(&key)?;
                assert_eq!(self.catalog_name, catalog_name);
                if node_id == self.node_id {
                    res.insert(schema_name);
                }
            }
            Ok(res.into_iter().collect())
        })
    }

    fn register_schema(
        &self,
        name: String,
        schema: SchemaProviderRef,
    ) -> Result<Option<SchemaProviderRef>, Error> {
        let _ = schema;
        let key = self.schema_key(&name).to_string();
        futures::executor::block_on(async move {
            let prev = match self.backend.get(key.as_bytes()).await? {
                None => None,
                Some(_) => self.schemas.read().await.get(&name).cloned(),
            };

            // TODO(hl): Schema entry value
            self.backend.set(key.as_bytes(), "".as_bytes()).await?;
            Ok(prev)
        })
    }

    fn schema(&self, name: &str) -> Result<Option<Arc<dyn SchemaProvider>>, Error> {
        futures::executor::block_on(async move {
            let key = self.schema_key(name).to_string();
            match self.backend.get(key.as_bytes()).await? {
                None => Ok(None),
                Some(_) => Ok(self.schemas.read().await.get(name).cloned()),
            }
        })
    }
}

pub struct RemoteSchemaProvider {
    catalog_name: String,
    schema_name: String,
    node_id: String,
    backend: Arc<MetaKvBackend>,
    tables: Arc<RwLock<HashMap<String, TableRef>>>,
}

impl RemoteSchemaProvider {
    pub fn table_key(&self, table_name: impl AsRef<str>) -> TableKey {
        TableKey {
            catalog_name: self.catalog_name.clone(),
            schema_name: self.schema_name.clone(),
            table_name: table_name.as_ref().to_string(),
            node_id: self.node_id.clone(),
        }
    }
}

impl SchemaProvider for RemoteSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Result<Vec<String>, Error> {
        futures::executor::block_on(async move {
            let prefix = build_table_prefix(&self.catalog_name, &self.schema_name);
            let mut iter = self.backend.range(prefix.as_bytes());
            let mut res = HashSet::new();
            while let Some(r) = iter.next().await {
                let kv = r?;
                let key = String::from_utf8_lossy(&kv.0).to_string();
                let TableKey {
                    node_id,
                    schema_name,
                    catalog_name,
                    table_name,
                } = TableKey::parse(key)?;

                assert_eq!(self.schema_name, schema_name);
                assert_eq!(self.catalog_name, catalog_name);

                if node_id == self.node_id {
                    res.insert(table_name);
                }
            }
            Ok(res.into_iter().collect())
        })
    }

    fn table(&self, name: &str) -> crate::error::Result<Option<TableRef>> {
        futures::executor::block_on(async move {
            let key = self.table_key(&name).to_string();
            match self.backend.get(key.as_bytes()).await? {
                None => Ok(None),
                Some(_) => Ok(self.tables.read().await.get(name).cloned()),
            }
        })
    }

    fn register_table(
        &self,
        name: String,
        table: TableRef,
    ) -> crate::error::Result<Option<TableRef>> {
        futures::executor::block_on(async move {
            let key = self.table_key(name.clone()).to_string();
            let prev = match self.backend.get(key.as_bytes()).await? {
                None => None,
                Some(_) => self.tables.read().await.get(&key).cloned(),
            };
            // TODO(hl): table values
            self.backend.set(key.as_bytes(), "".as_bytes()).await?;
            let mut tables = self.tables.write().await;
            tables.insert(name, table);
            Ok(prev)
        })
    }

    fn deregister_table(&self, name: &str) -> crate::error::Result<Option<TableRef>> {
        futures::executor::block_on(async move {
            let key = self.table_key(&name).to_string();
            let table_ref = match self.backend.get(key.as_bytes()).await? {
                None => None,
                Some(_) => self.tables.write().await.remove(name),
            };
            self.backend.delete_range(key.as_bytes(), &[]).await?;
            Ok(table_ref)
        })
    }

    fn table_exist(&self, name: &str) -> Result<bool, Error> {
        futures::executor::block_on(async move {
            let key = self.table_key(&name).to_string();
            Ok(self.backend.get(key.as_bytes()).await?.is_some())
        })
    }
}
