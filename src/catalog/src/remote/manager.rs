use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_catalog::{
    build_catalog_prefix, build_schema_prefix, build_table_prefix, CatalogKey, CatalogValue,
    SchemaKey, SchemaValue, TableKey, TableValue,
};
use common_telemetry::{debug, info};
use futures_util::StreamExt;
use snafu::{OptionExt, ResultExt};
use table::engine::{EngineContext, TableEngineRef};
use table::metadata::{TableId, TableIdent, TableVersion};
use table::requests::{CreateTableRequest, OpenTableRequest};
use table::TableRef;
use tokio::sync::{Mutex, RwLock};

use crate::error::Result;
use crate::error::{
    CatalogNotFoundSnafu, CreateTableSnafu, InvalidCatalogValueSnafu, OpenTableSnafu,
    SchemaNotFoundSnafu, TableExistsSnafu,
};
use crate::remote::{Kv, KvBackendRef};
use crate::{
    handle_system_table_request, CatalogList, CatalogManager, CatalogProvider, CatalogProviderRef,
    RegisterSystemTableRequest, RegisterTableRequest, SchemaProvider, SchemaProviderRef,
};

/// Catalog manager based on metasrv.
pub struct RemoteCatalogManager {
    node_id: String,
    pub backend: KvBackendRef,
    catalogs: Arc<RwLock<HashMap<String, CatalogProviderRef>>>,
    next_table_id: Arc<AtomicU32>,
    engine: TableEngineRef,
    system_table_requests: Mutex<Vec<RegisterSystemTableRequest>>,
}

impl RemoteCatalogManager {
    pub fn new(engine: TableEngineRef, node_id: String, backend: KvBackendRef) -> Self {
        Self {
            engine,
            node_id,
            backend,
            catalogs: Arc::new(Default::default()),
            next_table_id: Arc::new(Default::default()),
            system_table_requests: Default::default(),
        }
    }

    async fn fetch_remote_catalogs(&self) -> Result<HashSet<String>> {
        let mut remote_catalogs = HashSet::new();
        let mut iter = self.backend.range(build_catalog_prefix().as_bytes());
        while let Some(r) = iter.next().await {
            let CatalogKey {
                node_id,
                catalog_name,
            } = CatalogKey::parse(&String::from_utf8_lossy(&r?.0))
                .context(InvalidCatalogValueSnafu)?;
            if node_id == self.node_id {
                remote_catalogs.insert(catalog_name);
            }
        }
        Ok(remote_catalogs)
    }

    #[inline]
    async fn local_catalogs(&self) -> HashSet<String> {
        self.catalogs
            .read()
            .await
            .keys()
            .cloned()
            .collect::<HashSet<_>>()
    }

    pub async fn sync(&self) -> Result<()> {
        let remote_catalogs = self.fetch_remote_catalogs().await?;
        let local = self.local_catalogs().await;

        for catalog_to_register in local.difference(&remote_catalogs) {
            let key = self.build_catalog_key(catalog_to_register).to_string();
            self.backend
                .set(
                    key.as_bytes(),
                    &CatalogValue {}
                        .to_bytes()
                        .context(InvalidCatalogValueSnafu)?,
                )
                .await?
        }

        for (_, catalog) in self.catalogs.read().await.iter() {
            let catalog_provider = catalog
                .as_any()
                .downcast_ref::<RemoteCatalogProvider>()
                .unwrap();
            catalog_provider.sync().await?;
        }
        Ok(())
    }

    fn build_catalog_key(&self, catalog_name: impl AsRef<str>) -> CatalogKey {
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
            mutex: Default::default(),
        }) as _
    }

    /// Fetch catalogs/schemas/tables from remote catalog manager along with max table id allocated.
    async fn initiate_catalogs(&self) -> Result<(HashMap<String, CatalogProviderRef>, TableId)> {
        let mut res = HashMap::new();
        let mut max_table_id = TableId::MIN;

        // initiate default catalog and schema
        self.initiate_default_catalog().await?;
        info!("Default catalog and schema registered");

        let mut catalogs = self.backend.range(build_catalog_prefix().as_bytes());
        while let Some(r) = catalogs.next().await {
            let CatalogKey { catalog_name, .. } =
                CatalogKey::parse(&String::from_utf8_lossy(&r?.0))
                    .context(InvalidCatalogValueSnafu)?;

            info!("Fetch catalog from metasrv: {}", &catalog_name);
            let catalog = res
                .entry(catalog_name.clone())
                .or_insert_with(|| self.new_catalog_provider(&catalog_name));
            info!("Found catalog: {}", &catalog_name);

            let mut schemas = self
                .backend
                .range(build_schema_prefix(&catalog_name).as_bytes());

            info!("List schema from metasrv");
            while let Some(r) = schemas.next().await {
                let SchemaKey { schema_name, .. } =
                    SchemaKey::parse(&String::from_utf8_lossy(&r?.0))
                        .context(InvalidCatalogValueSnafu)?;
                info!("Found schema: {}", &schema_name);
                let schema = match catalog.schema(&schema_name)? {
                    None => {
                        let schema = self.new_schema_provider(&catalog_name, &schema_name);
                        info!("Register schema: {}", &schema_name);
                        catalog.register_schema(schema_name.clone(), schema.clone())?;
                        info!("Registered schema: {}", &schema_name);
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
                    let table_key = TableKey::parse(&String::from_utf8_lossy(&k))
                        .context(InvalidCatalogValueSnafu)?;
                    let table_value = TableValue::parse(&String::from_utf8_lossy(&v))
                        .context(InvalidCatalogValueSnafu)?;

                    let table_ref = self.open_or_create_table(&table_key, &table_value).await?;
                    info!("Try to register table: {}", &table_key.table_name);
                    schema.register_table(table_key.table_name.to_string(), table_ref)?;
                    info!("Table {} registered", &table_key.table_name);
                    max_table_id = max_table_id.max(table_value.id);
                }
            }
        }

        Ok((res, max_table_id))
    }

    async fn initiate_default_catalog(&self) -> Result<CatalogProviderRef> {
        let default_catalog = self.new_catalog_provider(DEFAULT_CATALOG_NAME);
        let default_schema = self.new_schema_provider(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME);
        default_catalog.register_schema(DEFAULT_SCHEMA_NAME.to_string(), default_schema)?;
        let schema_key = SchemaKey {
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            node_id: self.node_id.clone(),
        }
        .to_string();
        self.backend
            .set(
                schema_key.as_bytes(),
                &SchemaValue {}
                    .to_bytes()
                    .context(InvalidCatalogValueSnafu)?,
            )
            .await?;
        info!("Registered default schema");

        let catalog_key = CatalogKey {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            node_id: self.node_id.clone(),
        }
        .to_string();
        self.backend
            .set(
                catalog_key.as_bytes(),
                &CatalogValue {}
                    .to_bytes()
                    .context(InvalidCatalogValueSnafu)?,
            )
            .await?;
        info!("Registered default catalog");
        Ok(default_catalog)
    }

    async fn open_or_create_table(
        &self,
        table_key: &TableKey,
        table_value: &TableValue,
    ) -> Result<TableRef> {
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
                    catalog_name: table_key.catalog_name.clone(),
                    schema_name: table_key.schema_name.clone(),
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
    async fn start(&self) -> Result<()> {
        let (catalogs, max_table_id) = self.initiate_catalogs().await?;
        *(self.catalogs.write().await) = catalogs;
        self.next_table_id
            .store(max_table_id + 1, Ordering::Relaxed);
        info!("Max table id allocated: {}", max_table_id);

        let mut system_table_requests = self.system_table_requests.lock().await;
        handle_system_table_request(self, self.engine.clone(), &mut system_table_requests).await?;
        info!("All system table opened");
        Ok(())
    }

    fn next_table_id(&self) -> TableId {
        self.next_table_id.fetch_add(1, Ordering::Relaxed)
    }

    async fn register_table(&self, request: RegisterTableRequest) -> Result<usize> {
        let catalog_name = request.catalog;
        let schema_name = request.schema;
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

    async fn register_system_table(&self, request: RegisterSystemTableRequest) -> Result<()> {
        let mut requests = self.system_table_requests.lock().await;
        requests.push(request);
        Ok(())
    }

    fn table(
        &self,
        catalog: Option<&str>,
        schema: Option<&str>,
        table_name: &str,
    ) -> Result<Option<TableRef>> {
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
    ) -> Result<Option<CatalogProviderRef>> {
        futures::executor::block_on(async move {
            let key = self.build_catalog_key(&name).to_string();
            let prev = match self.backend.get(key.as_bytes()).await? {
                None => None,
                Some(_) => self.catalogs.read().await.get(&name).cloned(),
            };
            self.backend
                .set(
                    key.as_bytes(),
                    &CatalogValue {}
                        .to_bytes()
                        .context(InvalidCatalogValueSnafu)?,
                )
                .await?;
            let mut catalogs = self.catalogs.write().await;
            catalogs.insert(name, catalog);
            Ok(prev)
        })
    }

    /// List all catalogs from metasrv
    fn catalog_names(&self) -> Result<Vec<String>> {
        futures::executor::block_on(async move {
            let mut res = HashSet::new();
            let mut catalog_iter = self.backend.range(build_catalog_prefix().as_bytes());
            while let Some(v) = catalog_iter.next().await {
                let CatalogKey {
                    node_id,
                    catalog_name,
                } = CatalogKey::parse(&String::from_utf8_lossy(&v?.0))
                    .context(InvalidCatalogValueSnafu)?;

                if node_id == self.node_id {
                    res.insert(catalog_name);
                }
            }
            Ok(res.into_iter().collect())
        })
    }

    /// Read catalog info of given name from metasrv.
    fn catalog(&self, name: &str) -> Result<Option<CatalogProviderRef>> {
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
    backend: KvBackendRef,
    schemas: Arc<RwLock<HashMap<String, SchemaProviderRef>>>,
}

impl RemoteCatalogProvider {
    pub fn new(catalog_name: String, node_id: String, backend: KvBackendRef) -> Self {
        Self {
            catalog_name,
            node_id,
            backend,
            schemas: Default::default(),
        }
    }

    async fn fetch_remote_schemas(&self) -> Result<HashSet<String>> {
        let mut remote_schemas = HashSet::new();
        let mut iter = self
            .backend
            .range(build_schema_prefix(&self.catalog_name).as_bytes());
        while let Some(r) = iter.next().await {
            let kv = r?;
            let SchemaKey {
                catalog_name,
                schema_name,
                node_id,
            } = SchemaKey::parse(&String::from_utf8_lossy(&kv.0))
                .context(InvalidCatalogValueSnafu)?;
            debug_assert_eq!(catalog_name, self.catalog_name);
            if node_id == self.node_id {
                remote_schemas.insert(schema_name);
            }
        }
        Ok(remote_schemas)
    }

    #[inline]
    async fn local_schemas(&self) -> HashSet<String> {
        self.schemas
            .read()
            .await
            .keys()
            .cloned()
            .collect::<HashSet<_>>()
    }

    pub async fn sync(&self) -> Result<()> {
        let remote_schemas = self.fetch_remote_schemas().await?;
        let local = self.local_schemas().await;

        for schema_to_register in local.difference(&remote_schemas) {
            let key = self.build_schema_key(schema_to_register).to_string();
            let val = SchemaValue {}
                .to_bytes()
                .context(InvalidCatalogValueSnafu)?;
            self.backend.set(key.as_bytes(), &val).await?;
        }

        for schema in self.schemas.read().await.values() {
            let schema = schema
                .as_any()
                .downcast_ref::<RemoteSchemaProvider>()
                .unwrap();
            schema.sync().await?;
        }
        Ok(())
    }

    fn build_schema_key(&self, schema_name: impl AsRef<str>) -> SchemaKey {
        SchemaKey {
            catalog_name: self.catalog_name.clone(),
            schema_name: schema_name.as_ref().to_string(),
            node_id: self.node_id.clone(),
        }
    }
}

impl CatalogProvider for RemoteCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Result<Vec<String>> {
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
                } = SchemaKey::parse(&key).context(InvalidCatalogValueSnafu)?;
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
    ) -> Result<Option<SchemaProviderRef>> {
        let _ = schema;
        let key = self.build_schema_key(&name).to_string();
        futures::executor::block_on(async move {
            let prev = match self.backend.get(key.as_bytes()).await? {
                None => None,
                Some(_) => self.schemas.read().await.get(&name).cloned(),
            };

            self.backend
                .set(
                    key.as_bytes(),
                    &SchemaValue {}
                        .to_bytes()
                        .context(InvalidCatalogValueSnafu)?,
                )
                .await?;
            let mut schemas = self.schemas.write().await;
            schemas.insert(name, schema);
            Ok(prev)
        })
    }

    fn schema(&self, name: &str) -> Result<Option<Arc<dyn SchemaProvider>>> {
        futures::executor::block_on(async move {
            let key = self.build_schema_key(name).to_string();
            match self.backend.get(key.as_bytes()).await? {
                None => {
                    info!("Schema key does not exist on backend: {}", key);
                    Ok(None)
                }
                Some(_) => Ok(self.schemas.read().await.get(name).cloned()),
            }
        })
    }
}

pub struct RemoteSchemaProvider {
    catalog_name: String,
    schema_name: String,
    node_id: String,
    backend: KvBackendRef,
    tables: Arc<ArcSwap<HashMap<String, TableRef>>>,
    mutex: Arc<Mutex<()>>,
}

impl RemoteSchemaProvider {
    pub fn new(
        catalog_name: String,
        schema_name: String,
        node_id: String,
        backend: KvBackendRef,
    ) -> Self {
        Self {
            catalog_name,
            schema_name,
            node_id,
            backend,
            tables: Default::default(),
            mutex: Default::default(),
        }
    }

    /// Fetch remote table entries with highest table version.
    async fn fetch_remote_tables(&self) -> Result<HashMap<String, TableVersion>> {
        let mut res: HashMap<String, TableVersion> = HashMap::new();
        let mut iter = self
            .backend
            .range(build_table_prefix(&self.catalog_name, &self.schema_name).as_bytes());
        while let Some(r) = iter.next().await {
            let kv = r?;
            let TableKey {
                catalog_name,
                schema_name,
                table_name,
                version,
                node_id,
            } = TableKey::parse(&String::from_utf8_lossy(&kv.0))
                .context(InvalidCatalogValueSnafu)?;
            debug_assert_eq!(catalog_name, self.catalog_name);
            debug_assert_eq!(schema_name, self.schema_name);

            if node_id == self.node_id {
                if match res.get(&table_name) {
                    None => true,
                    Some(prev) => version > (*prev),
                } {
                    res.insert(table_name, version);
                }
            }
        }

        Ok(res)
    }

    /// Refresh tables in schema according to table list from metasrv
    pub async fn sync(&self) -> Result<()> {
        let remote_tables = self.fetch_remote_tables().await?;
        let local_tables = self.tables.load();
        for (table_name, table_ref) in local_tables.iter() {
            let TableIdent { table_id, version } = table_ref.table_info().ident;
            let need_register = match remote_tables.get(table_name) {
                None => true,
                Some(prev_ver) => *prev_ver < version, // remote version obsolete
            };

            if need_register {
                let key = self.build_table_key(table_name, version).to_string();
                let value = TableValue {
                    id: table_id,
                    node_id: self.node_id.clone(),
                    meta: table_ref.table_info().meta.clone(),
                };
                self.backend
                    .set(
                        key.as_bytes(),
                        &value.as_bytes().context(InvalidCatalogValueSnafu)?,
                    )
                    .await?;
            }
        }
        Ok(())
    }

    fn build_table_key(
        &self,
        table_name: impl AsRef<str>,
        table_version: TableVersion,
    ) -> TableKey {
        TableKey {
            catalog_name: self.catalog_name.clone(),
            schema_name: self.schema_name.clone(),
            table_name: table_name.as_ref().to_string(),
            version: table_version,
            node_id: self.node_id.clone(),
        }
    }
}

impl SchemaProvider for RemoteSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Result<Vec<String>> {
        Ok(self.tables.load().keys().cloned().collect::<Vec<_>>())
    }

    fn table(&self, name: &str) -> Result<Option<TableRef>> {
        Ok(self.tables.load().get(name).cloned())
    }

    fn register_table(&self, name: String, table: TableRef) -> Result<Option<TableRef>> {
        let table_info = table.table_info();
        let table_version = table_info.ident.version;
        let table_value = TableValue {
            meta: table_info.meta.clone(),
            id: table_info.ident.table_id,
            node_id: self.node_id.clone(),
        };
        let backend = self.backend.clone();
        let mutex = self.mutex.clone();
        let tables = self.tables.clone();

        let table_key = self
            .build_table_key(name.clone(), table_version)
            .to_string();

        let prev = std::thread::spawn(move || {
            common_runtime::block_on_read(async move {
                let _guard = mutex.lock().await;
                backend
                    .set(
                        table_key.as_bytes(),
                        &table_value
                            .as_bytes()
                            .context(InvalidCatalogValueSnafu)
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                debug!(
                    "Successfully set catalog table entry, key: {}, table value: {:?}",
                    table_key, table_value
                );

                let prev_tables = tables.load();
                let mut new_tables = HashMap::with_capacity(prev_tables.len() + 1);
                for (k, v) in prev_tables.iter() {
                    new_tables.insert(k.clone(), v.clone());
                }
                let prev = new_tables.insert(name, table);
                tables.store(Arc::new(new_tables));
                prev
            })
        })
        .join()
        .unwrap();
        Ok(prev)
    }

    fn deregister_table(&self, name: &str) -> Result<Option<TableRef>> {
        let table_version = match self.tables.load().get(name) {
            None => return Ok(None),
            Some(t) => t.table_info().ident.version,
        };

        let table_name = name.to_string();
        let table_key = self.build_table_key(&table_name, table_version).to_string();

        let backend = self.backend.clone();
        let mutex = self.mutex.clone();
        let tables = self.tables.clone();

        let prev = std::thread::spawn(move || {
            common_runtime::block_on_read(async move {
                let _guard = mutex.lock().await;
                backend.delete(table_key.as_bytes()).await.unwrap();
                debug!(
                    "Successfully deleted catalog table entry, key: {}",
                    table_key
                );

                let prev_tables = tables.load();
                let mut new_tables = HashMap::with_capacity(prev_tables.len() + 1);
                for (k, v) in prev_tables.iter() {
                    new_tables.insert(k.clone(), v.clone());
                }
                let prev = new_tables.remove(&table_name);
                tables.store(Arc::new(new_tables));
                prev
            })
        })
        .join()
        .unwrap();
        Ok(prev)
    }

    /// Checks if table exists in schema provider based on locally opened table map.
    fn table_exist(&self, name: &str) -> Result<bool> {
        Ok(self.tables.load().contains_key(name))
    }
}
