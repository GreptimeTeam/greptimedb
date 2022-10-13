use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use common_telemetry::info;
use futures_util::StreamExt;
use snafu::{OptionExt, ResultExt};
use table::engine::{EngineContext, TableEngineRef};
use table::metadata::TableId;
use table::requests::{CreateTableRequest, OpenTableRequest};
use table::TableRef;
use tokio::sync::{Mutex, RwLock};

use crate::error::{
    CatalogNotFoundSnafu, CreateTableSnafu, Error, OpenTableSnafu, SchemaNotFoundSnafu,
    TableExistsSnafu,
};
use crate::remote::client::MetaKvBackend;
use crate::remote::helper::{
    build_catalog_prefix, build_schema_prefix, build_table_prefix, CatalogKey, SchemaKey, TableKey,
    TableValue,
};
use crate::remote::{Kv, KvBackend};
use crate::{
    handle_system_table_request, CatalogList, CatalogManager, CatalogProviderRef,
    RegisterSystemTableRequest, RegisterTableRequest, SchemaProvider, SchemaProviderRef,
    DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME,
};

pub struct RemoteCatalogManager {
    node_id: String,
    backend: Arc<MetaKvBackend>,
    catalogs: Arc<RwLock<HashMap<String, CatalogProviderRef>>>,
    #[allow(unused)]
    next_table_id: AtomicU32, // table id should be calculated on startup
    engine: TableEngineRef,
    system_table_requests: Mutex<Vec<RegisterSystemTableRequest>>,
}

impl RemoteCatalogManager {
    fn catalog_key(&self, catalog_name: impl AsRef<str>) -> CatalogKey {
        CatalogKey {
            catalog_name: catalog_name.as_ref().to_string(),
            node_id: self.node_id.clone(),
        }
    }

    fn new_catalog_provider(&self, catalog_name: &str) -> CatalogProviderRef {
        Arc::new(RemoteCatalogProvider {
            catalog_name: catalog_name.to_string(),
            schemas: Default::default(),
            node_id: self.node_id.clone(),
            backend: self.backend.clone(),
        }) as _
    }

    fn new_schema_provider(&self, catalog_name: &str, schema_name: &str) -> SchemaProviderRef {
        Arc::new(RemoteSchemaProvider {
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
            tables: Default::default(),
            node_id: self.node_id.clone(),
            backend: self.backend.clone(),
        }) as _
    }

    /// Fetch catalogs/schemas/tables from remote catalog manager along with max table id allocated.
    async fn initiate_catalogs(
        &self,
    ) -> Result<(HashMap<String, CatalogProviderRef>, TableId), Error> {
        let mut res = HashMap::new();
        let mut max_table_id = TableId::MIN;

        let mut catalogs = self.backend.range(build_catalog_prefix().as_bytes());
        while let Some(r) = catalogs.next().await {
            let CatalogKey { catalog_name, .. } =
                CatalogKey::parse(&String::from_utf8_lossy(&r?.0))?;

            info!("Fetch catalog from metasrv: {}", &catalog_name);
            let catalog = res
                .entry(catalog_name.clone())
                .or_insert_with(|| self.new_catalog_provider(&catalog_name));

            let mut schemas = self
                .backend
                .range(build_schema_prefix(&catalog_name).as_bytes());
            while let Some(r) = schemas.next().await {
                let SchemaKey { schema_name, .. } =
                    SchemaKey::parse(&String::from_utf8_lossy(&r?.0))?;
                let schema = match catalog.schema(&schema_name)? {
                    None => {
                        let schema = self.new_schema_provider(&catalog_name, &schema_name);
                        catalog.register_schema(schema_name.clone(), schema.clone())?;
                        schema
                    }
                    Some(schema) => schema,
                };

                info!(
                    "Fetch schema from metasrv: {}.{}",
                    &catalog_name, &schema_name
                );

                let mut tables = self
                    .backend
                    .range(build_table_prefix(&catalog_name, &schema_name).as_bytes());
                while let Some(r) = tables.next().await {
                    let Kv(k, v) = r?;
                    let table_key = TableKey::parse(&String::from_utf8_lossy(&k))?;
                    let table_value = TableValue::parse(&String::from_utf8_lossy(&v))?;

                    let table_ref = self.open_or_create_table(&table_key, &table_value).await?;
                    schema.register_table(table_key.table_name.to_string(), table_ref)?;
                    max_table_id = max_table_id.max(table_value.id);
                }
            }
        }

        Ok((res, max_table_id))
    }

    async fn open_or_create_table(
        &self,
        table_key: &TableKey,
        table_value: &TableValue,
    ) -> Result<TableRef, Error> {
        let context = EngineContext {};

        let request = OpenTableRequest {
            catalog_name: table_key.catalog_name.clone(),
            schema_name: table_key.schema_name.clone(),
            table_name: table_key.table_name.clone(),
            table_id: table_value.id,
        };
        match self
            .engine
            .open_table(&context, request)
            .await
            .with_context(|_| OpenTableSnafu {
                table_info: format!(
                    "{}.{}.{}, id:{}",
                    &table_key.catalog_name, &table_key.schema_name, &table_key.table_name, 1
                ),
            })? {
            Some(table) => Ok(table),
            None => {
                let req = CreateTableRequest {
                    id: table_value.id,
                    catalog_name: Some(table_key.catalog_name.clone()),
                    schema_name: Some(table_key.schema_name.clone()),
                    table_name: table_key.table_name.clone(),
                    desc: None,
                    schema: table_value.meta.schema.clone(),
                    primary_key_indices: table_value.meta.primary_key_indices.clone(),
                    create_if_not_exists: true,
                    table_options: table_value.meta.options.clone(),
                };

                self.engine
                    .create_table(&context, req)
                    .await
                    .context(CreateTableSnafu {
                        table_info: format!(
                            "{}.{}.{}, id:{}",
                            &table_key.catalog_name,
                            &table_key.schema_name,
                            &table_key.table_name,
                            table_value.id
                        ),
                    })
            }
        }
    }
}

#[async_trait::async_trait]
impl CatalogManager for RemoteCatalogManager {
    async fn start(&self) -> crate::error::Result<()> {
        let (catalogs, max_table_id) = self.initiate_catalogs().await?;
        info!("Max table id allocated: {}", max_table_id);
        *(self.catalogs.write().await) = catalogs;
        self.next_table_id
            .store(max_table_id + 1, Ordering::Relaxed);

        let mut system_table_requests = self.system_table_requests.lock().await;
        handle_system_table_request(self, self.engine.clone(), &mut system_table_requests).await?;
        Ok(())
    }

    async fn next_table_id(&self) -> TableId {
        self.next_table_id.fetch_add(1, Ordering::Relaxed)
    }

    async fn register_table(&self, request: RegisterTableRequest) -> crate::error::Result<usize> {
        let catalog_name = request
            .catalog
            .unwrap_or_else(|| DEFAULT_CATALOG_NAME.to_string());
        let schema_name = request
            .schema
            .unwrap_or_else(|| DEFAULT_SCHEMA_NAME.to_string());
        let catalog_provider = self.catalog(&catalog_name)?.context(CatalogNotFoundSnafu {
            catalog_name: &catalog_name,
        })?;
        let schema_provider =
            catalog_provider
                .schema(&schema_name)?
                .with_context(|| SchemaNotFoundSnafu {
                    schema_info: format!("{}.{}", &catalog_name, &schema_name),
                })?;
        if schema_provider.table_exist(&request.table_name)? {
            return TableExistsSnafu {
                table: format!("{}.{}.{}", &catalog_name, &schema_name, &request.table_name),
            }
            .fail();
        }
        schema_provider.register_table(request.table_name, request.table)?;
        Ok(1)
    }

    async fn register_system_table(
        &self,
        request: RegisterSystemTableRequest,
    ) -> crate::error::Result<()> {
        let mut requests = self.system_table_requests.lock().await;
        requests.push(request);
        Ok(())
    }

    fn table(
        &self,
        catalog: Option<&str>,
        schema: Option<&str>,
        table_name: &str,
    ) -> crate::error::Result<Option<TableRef>> {
        let catalog_name = catalog.unwrap_or(DEFAULT_CATALOG_NAME);
        let schema_name = schema.unwrap_or(DEFAULT_SCHEMA_NAME);

        let catalog = self
            .catalog(catalog_name)?
            .with_context(|| CatalogNotFoundSnafu { catalog_name })?;
        let schema = catalog
            .schema(schema_name)?
            .with_context(|| SchemaNotFoundSnafu {
                schema_info: format!("{}.{}", catalog_name, schema_name),
            })?;
        schema.table(table_name)
    }
}

impl CatalogList for RemoteCatalogManager {
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

    /// List all catalogs from metasrv
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

    /// Read catalog info of given name from metsrv.
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
