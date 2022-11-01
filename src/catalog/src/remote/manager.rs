use std::any::Any;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use async_stream::stream;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, MIN_USER_TABLE_ID};
use common_catalog::{
    build_catalog_prefix, build_schema_prefix, build_table_prefix, CatalogKey, CatalogValue,
    SchemaKey, SchemaValue, TableKey, TableValue,
};
use common_telemetry::{debug, info};
use futures::Stream;
use futures_util::StreamExt;
use snafu::{OptionExt, ResultExt};
use table::engine::{EngineContext, TableEngineRef};
use table::metadata::{TableId, TableVersion};
use table::requests::{CreateTableRequest, OpenTableRequest};
use table::TableRef;
use tokio::sync::Mutex;

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
    node_id: u64,
    backend: KvBackendRef,
    catalogs: Arc<ArcSwap<HashMap<String, CatalogProviderRef>>>,
    next_table_id: Arc<AtomicU32>,
    engine: TableEngineRef,
    system_table_requests: Mutex<Vec<RegisterSystemTableRequest>>,
    mutex: Arc<Mutex<()>>,
}

impl RemoteCatalogManager {
    pub fn new(engine: TableEngineRef, node_id: u64, backend: KvBackendRef) -> Self {
        Self {
            engine,
            node_id,
            backend,
            catalogs: Default::default(),
            next_table_id: Default::default(),
            system_table_requests: Default::default(),
            mutex: Default::default(),
        }
    }

    fn build_catalog_key(&self, catalog_name: impl AsRef<str>) -> CatalogKey {
        CatalogKey {
            catalog_name: catalog_name.as_ref().to_string(),
            node_id: self.node_id,
        }
    }

    fn new_catalog_provider(&self, catalog_name: &str) -> CatalogProviderRef {
        Arc::new(RemoteCatalogProvider {
            catalog_name: catalog_name.to_string(),
            node_id: self.node_id,
            backend: self.backend.clone(),
            schemas: Default::default(),
            mutex: Default::default(),
        }) as _
    }

    fn new_schema_provider(&self, catalog_name: &str, schema_name: &str) -> SchemaProviderRef {
        Arc::new(RemoteSchemaProvider {
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
            tables: Default::default(),
            node_id: self.node_id,
            backend: self.backend.clone(),
            mutex: Default::default(),
        }) as _
    }

    async fn iter_remote_catalogs(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<CatalogKey>> + Send + '_>> {
        let catalog_range_prefix = build_catalog_prefix();
        info!("catalog_range_prefix: {}", catalog_range_prefix);
        let mut catalogs = self.backend.range(catalog_range_prefix.as_bytes());
        Box::pin(stream!({
            while let Some(r) = catalogs.next().await {
                let Kv(k, _) = r?;
                if !k.starts_with(catalog_range_prefix.as_bytes()) {
                    debug!("Ignoring non-catalog key: {}", String::from_utf8_lossy(&k));
                    continue;
                }
                let key = CatalogKey::parse(&String::from_utf8_lossy(&k))
                    .context(InvalidCatalogValueSnafu)?;
                if key.node_id == self.node_id {
                    yield Ok(key)
                }
            }
        }))
    }

    async fn iter_remote_schemas(
        &self,
        catalog_name: &str,
    ) -> Pin<Box<dyn Stream<Item = Result<SchemaKey>> + Send + '_>> {
        let schema_prefix = build_schema_prefix(catalog_name);
        let mut schemas = self.backend.range(schema_prefix.as_bytes());

        Box::pin(stream!({
            while let Some(r) = schemas.next().await {
                let Kv(k, _) = r?;
                if !k.starts_with(schema_prefix.as_bytes()) {
                    debug!("Ignoring non-schema key: {}", String::from_utf8_lossy(&k));
                    continue;
                }

                let schema_key = SchemaKey::parse(&String::from_utf8_lossy(&k))
                    .context(InvalidCatalogValueSnafu)?;

                if schema_key.node_id == self.node_id {
                    yield Ok(schema_key)
                }
            }
        }))
    }

    /// Iterate over all table entries on metasrv
    /// TODO(hl): table entries with different version is not currently considered.
    /// Ideally deprecated table entry must be deleted when deregistering from catalog.
    async fn iter_remote_tables(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> Pin<Box<dyn Stream<Item = Result<(TableKey, TableValue)>> + Send + '_>> {
        let table_prefix = build_table_prefix(catalog_name, schema_name);
        let mut tables = self.backend.range(table_prefix.as_bytes());
        Box::pin(stream!({
            while let Some(r) = tables.next().await {
                let Kv(k, v) = r?;
                if !k.starts_with(table_prefix.as_bytes()) {
                    debug!("Ignoring non-table prefix: {}", String::from_utf8_lossy(&k));
                    continue;
                }
                let table_key = TableKey::parse(&String::from_utf8_lossy(&k))
                    .context(InvalidCatalogValueSnafu)?;
                let table_value = TableValue::parse(&String::from_utf8_lossy(&v))
                    .context(InvalidCatalogValueSnafu)?;

                if table_value.node_id == self.node_id {
                    yield Ok((table_key, table_value))
                }
            }
        }))
    }

    /// Fetch catalogs/schemas/tables from remote catalog manager along with max table id allocated.
    async fn initiate_catalogs(&self) -> Result<(HashMap<String, CatalogProviderRef>, TableId)> {
        let mut res = HashMap::new();
        let max_table_id = MIN_USER_TABLE_ID;

        // initiate default catalog and schema
        let default_catalog = self.initiate_default_catalog().await?;
        res.insert(DEFAULT_CATALOG_NAME.to_string(), default_catalog);
        info!("Default catalog and schema registered");

        let mut catalogs = self.iter_remote_catalogs().await;
        while let Some(r) = catalogs.next().await {
            let CatalogKey { catalog_name, .. } = r?;
            info!("Fetch catalog from metasrv: {}", catalog_name);
            let catalog = res
                .entry(catalog_name.clone())
                .or_insert_with(|| self.new_catalog_provider(&catalog_name))
                .clone();

            self.initiate_schemas(catalog_name, catalog, max_table_id)
                .await?;
        }

        Ok((res, max_table_id))
    }

    async fn initiate_schemas(
        &self,
        catalog_name: String,
        catalog: CatalogProviderRef,
        max_table_id: TableId,
    ) -> Result<()> {
        let mut schemas = self.iter_remote_schemas(&catalog_name).await;
        while let Some(r) = schemas.next().await {
            let SchemaKey {
                catalog_name,
                schema_name,
                ..
            } = r?;
            info!("Found schema: {}.{}", catalog_name, schema_name);
            let schema = match catalog.schema(&schema_name)? {
                None => {
                    let schema = self.new_schema_provider(&catalog_name, &schema_name);
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
            self.initiate_tables(&catalog_name, &schema_name, schema, max_table_id)
                .await?;
        }
        Ok(())
    }

    /// Initiates all tables inside a catalog by fetching data from metasrv.
    async fn initiate_tables<'a>(
        &'a self,
        catalog_name: &'a str,
        schema_name: &'a str,
        schema: SchemaProviderRef,
        mut max_table_id: TableId,
    ) -> Result<()> {
        let mut tables = self.iter_remote_tables(catalog_name, schema_name).await;
        while let Some(r) = tables.next().await {
            let (table_key, table_value) = r?;
            let table_ref = self.open_or_create_table(&table_key, &table_value).await?;
            schema.register_table(table_key.table_name.to_string(), table_ref)?;
            info!("Registered table {}", &table_key.table_name);
            if table_value.id > max_table_id {
                info!("Max table id: {} -> {}", max_table_id, table_value.id);
                max_table_id = table_value.id;
            }
        }
        Ok(())
    }

    async fn initiate_default_catalog(&self) -> Result<CatalogProviderRef> {
        let default_catalog = self.new_catalog_provider(DEFAULT_CATALOG_NAME);
        let default_schema = self.new_schema_provider(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME);
        default_catalog.register_schema(DEFAULT_SCHEMA_NAME.to_string(), default_schema)?;
        let schema_key = SchemaKey {
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            node_id: self.node_id,
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
            node_id: self.node_id,
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
        let TableKey {
            catalog_name,
            schema_name,
            table_name,
            ..
        } = table_key;

        let TableValue { id, meta, .. } = table_value;

        let request = OpenTableRequest {
            catalog_name: catalog_name.clone(),
            schema_name: schema_name.clone(),
            table_name: table_name.clone(),
            table_id: *id,
        };
        match self
            .engine
            .open_table(&context, request)
            .await
            .with_context(|_| OpenTableSnafu {
                table_info: format!("{}.{}.{}, id:{}", catalog_name, schema_name, table_name, id,),
            })? {
            Some(table) => Ok(table),
            None => {
                let req = CreateTableRequest {
                    id: *id,
                    catalog_name: catalog_name.clone(),
                    schema_name: schema_name.clone(),
                    table_name: table_name.clone(),
                    desc: None,
                    schema: meta.schema.clone(),
                    region_numbers: meta.region_numbers.clone(),
                    primary_key_indices: meta.primary_key_indices.clone(),
                    create_if_not_exists: true,
                    table_options: meta.options.clone(),
                };

                self.engine
                    .create_table(&context, req)
                    .await
                    .context(CreateTableSnafu {
                        table_info: format!(
                            "{}.{}.{}, id:{}",
                            &catalog_name, &schema_name, &table_name, id
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
        info!(
            "Initialized catalogs: {:?}",
            catalogs.keys().cloned().collect::<Vec<_>>()
        );
        self.catalogs.store(Arc::new(catalogs));
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
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<TableRef>> {
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
        let key = self.build_catalog_key(&name).to_string();
        let backend = self.backend.clone();
        let mutex = self.mutex.clone();
        let catalogs = self.catalogs.clone();

        std::thread::spawn(|| {
            common_runtime::block_on_write(async move {
                let _guard = mutex.lock().await;
                backend
                    .set(
                        key.as_bytes(),
                        &CatalogValue {}
                            .to_bytes()
                            .context(InvalidCatalogValueSnafu)?,
                    )
                    .await?;
                let prev_catalogs = catalogs.load();
                let mut new_catalogs = HashMap::with_capacity(prev_catalogs.len() + 1);
                new_catalogs.clone_from(&prev_catalogs);
                let prev = new_catalogs.insert(name, catalog);
                catalogs.store(Arc::new(new_catalogs));
                Ok(prev)
            })
        })
        .join()
        .unwrap()
    }

    /// List all catalogs from metasrv
    fn catalog_names(&self) -> Result<Vec<String>> {
        Ok(self.catalogs.load().keys().cloned().collect::<Vec<_>>())
    }

    /// Read catalog info of given name from metasrv.
    fn catalog(&self, name: &str) -> Result<Option<CatalogProviderRef>> {
        Ok(self.catalogs.load().get(name).cloned())
    }
}

pub struct RemoteCatalogProvider {
    catalog_name: String,
    node_id: u64,
    backend: KvBackendRef,
    schemas: Arc<ArcSwap<HashMap<String, SchemaProviderRef>>>,
    mutex: Arc<Mutex<()>>,
}

impl RemoteCatalogProvider {
    pub fn new(catalog_name: String, node_id: u64, backend: KvBackendRef) -> Self {
        Self {
            catalog_name,
            node_id,
            backend,
            schemas: Default::default(),
            mutex: Default::default(),
        }
    }

    fn build_schema_key(&self, schema_name: impl AsRef<str>) -> SchemaKey {
        SchemaKey {
            catalog_name: self.catalog_name.clone(),
            schema_name: schema_name.as_ref().to_string(),
            node_id: self.node_id,
        }
    }
}

impl CatalogProvider for RemoteCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Result<Vec<String>> {
        Ok(self.schemas.load().keys().cloned().collect::<Vec<_>>())
    }

    fn register_schema(
        &self,
        name: String,
        schema: SchemaProviderRef,
    ) -> Result<Option<SchemaProviderRef>> {
        let key = self.build_schema_key(&name).to_string();
        let backend = self.backend.clone();
        let mutex = self.mutex.clone();
        let schemas = self.schemas.clone();

        std::thread::spawn(|| {
            common_runtime::block_on_write(async move {
                let _guard = mutex.lock().await;
                backend
                    .set(
                        key.as_bytes(),
                        &SchemaValue {}
                            .to_bytes()
                            .context(InvalidCatalogValueSnafu)?,
                    )
                    .await?;
                let prev_schemas = schemas.load();
                let mut new_schemas = HashMap::with_capacity(prev_schemas.len() + 1);
                new_schemas.clone_from(&prev_schemas);
                let prev_schema = new_schemas.insert(name, schema);
                schemas.store(Arc::new(new_schemas));
                Ok(prev_schema)
            })
        })
        .join()
        .unwrap()
    }

    fn schema(&self, name: &str) -> Result<Option<Arc<dyn SchemaProvider>>> {
        Ok(self.schemas.load().get(name).cloned())
    }
}

pub struct RemoteSchemaProvider {
    catalog_name: String,
    schema_name: String,
    node_id: u64,
    backend: KvBackendRef,
    tables: Arc<ArcSwap<HashMap<String, TableRef>>>,
    mutex: Arc<Mutex<()>>,
}

impl RemoteSchemaProvider {
    pub fn new(
        catalog_name: String,
        schema_name: String,
        node_id: u64,
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
            node_id: self.node_id,
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
            node_id: self.node_id,
            regions_ids: table
                .table_info()
                .meta
                .region_numbers
                .iter()
                .map(|v| *v as u64)
                .collect(),
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
                        &table_value.as_bytes().context(InvalidCatalogValueSnafu)?,
                    )
                    .await?;
                debug!(
                    "Successfully set catalog table entry, key: {}, table value: {:?}",
                    table_key, table_value
                );

                let prev_tables = tables.load();
                let mut new_tables = HashMap::with_capacity(prev_tables.len() + 1);
                new_tables.clone_from(&prev_tables);
                let prev = new_tables.insert(name, table);
                tables.store(Arc::new(new_tables));
                Ok(prev)
            })
        })
        .join()
        .unwrap();
        prev
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
                backend.delete(table_key.as_bytes()).await?;
                debug!(
                    "Successfully deleted catalog table entry, key: {}",
                    table_key
                );

                let prev_tables = tables.load();
                let mut new_tables = HashMap::with_capacity(prev_tables.len() + 1);
                new_tables.clone_from(&prev_tables);
                let prev = new_tables.remove(&table_name);
                tables.store(Arc::new(new_tables));
                Ok(prev)
            })
        })
        .join()
        .unwrap();
        prev
    }

    /// Checks if table exists in schema provider based on locally opened table map.
    fn table_exist(&self, name: &str) -> Result<bool> {
        Ok(self.tables.load().contains_key(name))
    }
}
