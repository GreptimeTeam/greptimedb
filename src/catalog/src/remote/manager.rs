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
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;

use arc_swap::ArcSwap;
use async_stream::stream;
use async_trait::async_trait;
use common_catalog::consts::{
    DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, MIN_USER_TABLE_ID, MITO_ENGINE,
};
use common_telemetry::{debug, error, info};
use dashmap::DashMap;
use futures::Stream;
use futures_util::StreamExt;
use key_lock::KeyLock;
use parking_lot::RwLock;
use snafu::{OptionExt, ResultExt};
use table::engine::manager::TableEngineManagerRef;
use table::engine::EngineContext;
use table::metadata::TableId;
use table::requests::{CreateTableRequest, OpenTableRequest};
use table::table::numbers::NumbersTable;
use table::TableRef;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::error::Error::ParallelOpenTable;
use crate::error::{
    CatalogNotFoundSnafu, CreateTableSnafu, InvalidCatalogValueSnafu, OpenTableSnafu, Result,
    SchemaNotFoundSnafu, TableEngineNotFoundSnafu, TableExistsSnafu, UnimplementedSnafu,
};
use crate::helper::{
    build_catalog_prefix, build_schema_prefix, build_table_global_prefix, CatalogKey, CatalogValue,
    SchemaKey, SchemaValue, TableGlobalKey, TableGlobalValue, TableRegionalKey, TableRegionalValue,
    CATALOG_KEY_PREFIX,
};
use crate::remote::{Kv, KvBackendRef};
use crate::{
    handle_system_table_request, CatalogList, CatalogManager, CatalogProvider, CatalogProviderRef,
    DeregisterTableRequest, RegisterSchemaRequest, RegisterSystemTableRequest,
    RegisterTableRequest, RenameTableRequest, SchemaProvider, SchemaProviderRef,
};

/// Catalog manager based on metasrv.
pub struct RemoteCatalogManager {
    node_id: u64,
    backend: KvBackendRef,
    catalogs: Arc<RwLock<DashMap<String, CatalogProviderRef>>>,
    engine_manager: TableEngineManagerRef,
    system_table_requests: Mutex<Vec<RegisterSystemTableRequest>>,
}

impl RemoteCatalogManager {
    pub fn new(engine_manager: TableEngineManagerRef, node_id: u64, backend: KvBackendRef) -> Self {
        Self {
            engine_manager,
            node_id,
            backend,
            catalogs: Default::default(),
            system_table_requests: Default::default(),
        }
    }

    fn build_catalog_key(&self, catalog_name: impl AsRef<str>) -> CatalogKey {
        CatalogKey {
            catalog_name: catalog_name.as_ref().to_string(),
        }
    }

    fn new_catalog_provider(&self, catalog_name: &str) -> CatalogProviderRef {
        Arc::new(RemoteCatalogProvider {
            node_id: self.node_id,
            catalog_name: catalog_name.to_string(),
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

                let catalog_key = String::from_utf8_lossy(&k);
                if let Ok(key) = CatalogKey::parse(&catalog_key) {
                    yield Ok(key)
                } else {
                    error!("Invalid catalog key: {:?}", catalog_key);
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
                yield Ok(schema_key)
            }
        }))
    }

    /// Iterate over all table entries on metasrv
    async fn iter_remote_tables(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> Pin<Box<dyn Stream<Item = Result<(TableGlobalKey, TableGlobalValue)>> + Send + '_>> {
        let table_prefix = build_table_global_prefix(catalog_name, schema_name);
        let mut tables = self.backend.range(table_prefix.as_bytes());
        Box::pin(stream!({
            while let Some(r) = tables.next().await {
                let Kv(k, v) = r?;
                if !k.starts_with(table_prefix.as_bytes()) {
                    debug!("Ignoring non-table prefix: {}", String::from_utf8_lossy(&k));
                    continue;
                }
                let table_key = TableGlobalKey::parse(&String::from_utf8_lossy(&k))
                    .context(InvalidCatalogValueSnafu)?;
                let table_value =
                    TableGlobalValue::from_bytes(&v).context(InvalidCatalogValueSnafu)?;

                info!(
                    "Found catalog table entry, key: {}, value: {:?}",
                    table_key, table_value
                );
                // metasrv has allocated region ids to current datanode
                if table_value
                    .regions_id_map
                    .get(&self.node_id)
                    .map(|v| !v.is_empty())
                    .unwrap_or(false)
                {
                    yield Ok((table_key, table_value))
                }
            }
        }))
    }

    /// Fetch catalogs/schemas/tables from remote catalog manager along with max table id allocated.
    async fn initiate_catalogs(&self) -> Result<(HashMap<String, CatalogProviderRef>, TableId)> {
        let mut res = HashMap::new();
        let max_table_id = MIN_USER_TABLE_ID - 1;

        // initiate default catalog and schema
        let default_catalog = self
            .create_catalog_and_schema(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME)
            .await?;
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
        info!("initializing tables in {}.{}", catalog_name, schema_name);
        let mut table_num = 0;
        let tables = self.iter_remote_tables(catalog_name, schema_name).await;
        let kvs = tables.collect::<Vec<_>>().await;
        let mut joins = Vec::with_capacity(kvs.len());

        let node_id = self.node_id;
        for kv in kvs {
            let (table_key, table_value) = kv?;
            let engine_manager = self.engine_manager.clone();
            let join: JoinHandle<Result<TableRef>> = tokio::spawn(async move {
                let table_ref =
                    open_or_create_table(node_id, engine_manager, &table_key, &table_value).await?;
                Ok(table_ref)
            });
            joins.push(join);
        }

        while !joins.is_empty() {
            match futures::future::select_all(joins).await {
                (Ok(join_re), _, remaining) => {
                    joins = remaining;
                    let table_ref = join_re?;
                    let table_info = table_ref.table_info();

                    let table_name = table_info.name.clone();
                    let table_id = table_info.ident.table_id;
                    schema.register_table(table_name.clone(), table_ref)?;
                    info!("Registered table {}", &table_name);
                    max_table_id = max_table_id.max(table_id);
                    table_num += 1;
                }
                (Err(source), _, _) => {
                    return Err(ParallelOpenTable { source });
                }
            }
        }

        info!(
            "initialized tables in {}.{}, total: {}",
            catalog_name, schema_name, table_num
        );
        Ok(())
    }

    pub async fn create_catalog_and_schema(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<CatalogProviderRef> {
        let schema_provider = self.new_schema_provider(catalog_name, schema_name);

        let catalog_provider = self.new_catalog_provider(catalog_name);
        catalog_provider.register_schema(schema_name.to_string(), schema_provider.clone())?;

        let schema_key = SchemaKey {
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
        }
        .to_string();
        self.backend
            .set(
                schema_key.as_bytes(),
                &SchemaValue {}
                    .as_bytes()
                    .context(InvalidCatalogValueSnafu)?,
            )
            .await?;
        info!("Created schema '{schema_key}'");

        let catalog_key = CatalogKey {
            catalog_name: catalog_name.to_string(),
        }
        .to_string();
        self.backend
            .set(
                catalog_key.as_bytes(),
                &CatalogValue {}
                    .as_bytes()
                    .context(InvalidCatalogValueSnafu)?,
            )
            .await?;
        info!("Created catalog '{catalog_key}");
        Ok(catalog_provider)
    }
}

async fn open_or_create_table(
    node_id: u64,
    engine_manager: TableEngineManagerRef,
    table_key: &TableGlobalKey,
    table_value: &TableGlobalValue,
) -> Result<TableRef> {
    let context = EngineContext {};
    let TableGlobalKey {
        catalog_name,
        schema_name,
        table_name,
        ..
    } = table_key;

    let table_id = table_value.table_id();

    let TableGlobalValue {
        table_info,
        regions_id_map,
        ..
    } = table_value;

    // unwrap safety: checked in yielding this table when `iter_remote_tables`
    let region_numbers = regions_id_map.get(&node_id).unwrap();

    let request = OpenTableRequest {
        catalog_name: catalog_name.clone(),
        schema_name: schema_name.clone(),
        table_name: table_name.clone(),
        table_id,
    };
    let engine =
        engine_manager
            .engine(&table_info.meta.engine)
            .context(TableEngineNotFoundSnafu {
                engine_name: &table_info.meta.engine,
            })?;
    match engine
        .open_table(&context, request)
        .await
        .with_context(|_| OpenTableSnafu {
            table_info: format!("{catalog_name}.{schema_name}.{table_name}, id:{table_id}"),
        })? {
        Some(table) => {
            info!(
                "Table opened: {}.{}.{}",
                catalog_name, schema_name, table_name
            );
            Ok(table)
        }
        None => {
            info!(
                "Try create table: {}.{}.{}",
                catalog_name, schema_name, table_name
            );

            let meta = &table_info.meta;
            let req = CreateTableRequest {
                id: table_id,
                catalog_name: catalog_name.clone(),
                schema_name: schema_name.clone(),
                table_name: table_name.clone(),
                desc: None,
                schema: meta.schema.clone(),
                region_numbers: region_numbers.clone(),
                primary_key_indices: meta.primary_key_indices.clone(),
                create_if_not_exists: true,
                table_options: meta.options.clone(),
                engine: engine.name().to_string(),
            };

            engine
                .create_table(&context, req)
                .await
                .context(CreateTableSnafu {
                    table_info: format!(
                        "{}.{}.{}, id:{}",
                        &catalog_name, &schema_name, &table_name, table_id
                    ),
                })
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

        {
            let self_catalogs = self.catalogs.read();
            catalogs.into_iter().for_each(|(k, v)| {
                self_catalogs.insert(k, v);
            });
        }

        info!("Max table id allocated: {}", max_table_id);

        let mut system_table_requests = self.system_table_requests.lock().await;
        let engine = self
            .engine_manager
            .engine(MITO_ENGINE)
            .context(TableEngineNotFoundSnafu {
                engine_name: MITO_ENGINE,
            })?;
        handle_system_table_request(self, engine, &mut system_table_requests).await?;
        info!("All system table opened");

        self.catalog(DEFAULT_CATALOG_NAME)
            .unwrap()
            .unwrap()
            .schema(DEFAULT_SCHEMA_NAME)
            .unwrap()
            .unwrap()
            .register_table("numbers".to_string(), Arc::new(NumbersTable::default()))
            .unwrap();
        Ok(())
    }

    async fn register_table(&self, request: RegisterTableRequest) -> Result<bool> {
        let catalog_name = request.catalog;
        let schema_name = request.schema;
        let catalog_provider = self.catalog(&catalog_name)?.context(CatalogNotFoundSnafu {
            catalog_name: &catalog_name,
        })?;
        let schema_provider =
            catalog_provider
                .schema(&schema_name)?
                .with_context(|| SchemaNotFoundSnafu {
                    catalog: &catalog_name,
                    schema: &schema_name,
                })?;
        if schema_provider.table_exist(&request.table_name)? {
            return TableExistsSnafu {
                table: format!("{}.{}.{}", &catalog_name, &schema_name, &request.table_name),
            }
            .fail();
        }
        schema_provider.register_table(request.table_name, request.table)?;
        Ok(true)
    }

    async fn deregister_table(&self, request: DeregisterTableRequest) -> Result<bool> {
        let catalog_name = &request.catalog;
        let schema_name = &request.schema;
        let schema = self
            .schema(catalog_name, schema_name)?
            .context(SchemaNotFoundSnafu {
                catalog: catalog_name,
                schema: schema_name,
            })?;

        let result = schema.deregister_table(&request.table_name)?;
        Ok(result.is_none())
    }

    async fn register_schema(&self, request: RegisterSchemaRequest) -> Result<bool> {
        let catalog_name = request.catalog;
        let schema_name = request.schema;
        let catalog_provider = self.catalog(&catalog_name)?.context(CatalogNotFoundSnafu {
            catalog_name: &catalog_name,
        })?;
        let schema_provider = self.new_schema_provider(&catalog_name, &schema_name);
        catalog_provider.register_schema(schema_name, schema_provider)?;
        Ok(true)
    }

    async fn rename_table(&self, _request: RenameTableRequest) -> Result<bool> {
        UnimplementedSnafu {
            operation: "rename table",
        }
        .fail()
    }

    async fn register_system_table(&self, request: RegisterSystemTableRequest) -> Result<()> {
        let mut requests = self.system_table_requests.lock().await;
        requests.push(request);
        Ok(())
    }

    fn schema(&self, catalog: &str, schema: &str) -> Result<Option<SchemaProviderRef>> {
        self.catalog(catalog)?
            .context(CatalogNotFoundSnafu {
                catalog_name: catalog,
            })?
            .schema(schema)
    }

    async fn table(
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
                catalog: catalog_name,
                schema: schema_name,
            })?;
        schema.table(table_name).await
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
        let catalogs = self.catalogs.clone();

        std::thread::spawn(|| {
            common_runtime::block_on_write(async move {
                backend
                    .set(
                        key.as_bytes(),
                        &CatalogValue {}
                            .as_bytes()
                            .context(InvalidCatalogValueSnafu)?,
                    )
                    .await?;

                let catalogs = catalogs.read();
                let prev = catalogs.insert(name, catalog.clone());

                Ok(prev)
            })
        })
        .join()
        .unwrap()
    }

    /// List all catalogs from metasrv
    fn catalog_names(&self) -> Result<Vec<String>> {
        let catalogs = self.catalogs.read();
        Ok(catalogs.iter().map(|k| k.key().to_string()).collect())
    }

    /// Read catalog info of given name from metasrv.
    fn catalog(&self, name: &str) -> Result<Option<CatalogProviderRef>> {
        {
            let catalogs = self.catalogs.read();
            let catalog = catalogs.get(name);

            if let Some(catalog) = catalog {
                return Ok(Some(catalog.clone()));
            }
        }

        let catalogs = self.catalogs.write();

        let catalog = catalogs.get(name);
        if let Some(catalog) = catalog {
            return Ok(Some(catalog.clone()));
        }

        // It's for lack of incremental catalog syncing between datanode and meta. Here we fetch catalog
        // from meta on demand. This can be removed when incremental catalog syncing is done in datanode.

        let backend = self.backend.clone();

        let catalogs_from_meta: HashSet<String> = std::thread::spawn(|| {
            common_runtime::block_on_read(async move {
                let mut stream = backend.range(CATALOG_KEY_PREFIX.as_bytes());
                let mut catalogs = HashSet::new();

                while let Some(catalog) = stream.next().await {
                    if let Ok(catalog) = catalog {
                        let catalog_key = String::from_utf8_lossy(&catalog.0);

                        if let Ok(key) = CatalogKey::parse(&catalog_key) {
                            catalogs.insert(key.catalog_name);
                        }
                    }
                }

                catalogs
            })
        })
        .join()
        .unwrap();

        catalogs.retain(|catalog_name, _| catalogs_from_meta.get(catalog_name).is_some());

        for catalog in catalogs_from_meta {
            catalogs
                .entry(catalog.clone())
                .or_insert(self.new_catalog_provider(&catalog));
        }

        let catalog = catalogs.get(name);

        Ok(catalog.as_deref().cloned())
    }
}

pub struct RemoteCatalogProvider {
    node_id: u64,
    catalog_name: String,
    backend: KvBackendRef,
    schemas: Arc<ArcSwap<HashMap<String, SchemaProviderRef>>>,
    mutex: Arc<Mutex<()>>,
}

impl RemoteCatalogProvider {
    pub fn new(catalog_name: String, backend: KvBackendRef, node_id: u64) -> Self {
        Self {
            node_id,
            catalog_name,
            backend,
            schemas: Default::default(),
            mutex: Default::default(),
        }
    }

    pub fn refresh_schemas(&self) -> Result<()> {
        let schemas = self.schemas.clone();
        let schema_prefix = build_schema_prefix(&self.catalog_name);
        let catalog_name = self.catalog_name.clone();
        let mutex = self.mutex.clone();
        let backend = self.backend.clone();
        let node_id = self.node_id;

        std::thread::spawn(move || {
            common_runtime::block_on_write(async move {
                let _guard = mutex.lock().await;
                let prev_schemas = schemas.load();
                let mut new_schemas = HashMap::with_capacity(prev_schemas.len() + 1);
                new_schemas.clone_from(&prev_schemas);

                let mut remote_schemas = backend.range(schema_prefix.as_bytes());
                while let Some(r) = remote_schemas.next().await {
                    let Kv(k, _) = r?;
                    let schema_key = SchemaKey::parse(&String::from_utf8_lossy(&k))
                        .context(InvalidCatalogValueSnafu)?;
                    if !new_schemas.contains_key(&schema_key.schema_name) {
                        new_schemas.insert(
                            schema_key.schema_name.clone(),
                            Arc::new(RemoteSchemaProvider::new(
                                catalog_name.clone(),
                                schema_key.schema_name,
                                node_id,
                                backend.clone(),
                            )),
                        );
                    }
                }
                schemas.store(Arc::new(new_schemas));
                Ok(())
            })
        })
        .join()
        .unwrap()?;

        Ok(())
    }

    fn build_schema_key(&self, schema_name: impl AsRef<str>) -> SchemaKey {
        SchemaKey {
            catalog_name: self.catalog_name.clone(),
            schema_name: schema_name.as_ref().to_string(),
        }
    }
}

impl CatalogProvider for RemoteCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Result<Vec<String>> {
        self.refresh_schemas()?;
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
                            .as_bytes()
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
        // TODO(hl): We should refresh whole catalog before calling datafusion's query engine.
        self.refresh_schemas()?;
        Ok(self.schemas.load().get(name).cloned())
    }
}

pub struct RemoteSchemaProvider {
    catalog_name: String,
    schema_name: String,
    node_id: u64,
    backend: KvBackendRef,
    tables: Arc<ArcSwap<DashMap<String, TableRef>>>,
    mutex: Arc<KeyLock<String>>,
    // mutex: Arc<Mutex<()>>,
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

    fn build_regional_table_key(&self, table_name: impl AsRef<str>) -> TableRegionalKey {
        TableRegionalKey {
            catalog_name: self.catalog_name.clone(),
            schema_name: self.schema_name.clone(),
            table_name: table_name.as_ref().to_string(),
            node_id: self.node_id,
        }
    }
}

#[async_trait]
impl SchemaProvider for RemoteSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Result<Vec<String>> {
        Ok(self
            .tables
            .load()
            .iter()
            .map(|en| en.key().clone())
            .collect::<Vec<_>>())
    }

    async fn table(&self, name: &str) -> Result<Option<TableRef>> {
        Ok(self.tables.load().get(name).map(|en| en.value().clone()))
    }

    fn register_table(&self, name: String, table: TableRef) -> Result<Option<TableRef>> {
        let table_info = table.table_info();
        let table_version = table_info.ident.version;
        let table_value = TableRegionalValue {
            version: table_version,
            regions_ids: table.table_info().meta.region_numbers.clone(),
        };
        let backend = self.backend.clone();
        let mutex = self.mutex.clone();
        let tables = self.tables.clone();
        let table_key = self.build_regional_table_key(&name).to_string();

        let prev = std::thread::spawn(move || {
            common_runtime::block_on_read(async move {
                let _guard = mutex.lock(table_key.clone()).await;
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

                let tables = tables.load();
                let prev = tables.insert(name, table);
                Ok(prev)
            })
        })
        .join()
        .unwrap();
        prev
    }

    fn rename_table(&self, _name: &str, _new_name: String) -> Result<TableRef> {
        UnimplementedSnafu {
            operation: "rename table",
        }
        .fail()
    }

    fn deregister_table(&self, name: &str) -> Result<Option<TableRef>> {
        let table_name = name.to_string();
        let table_key = self.build_regional_table_key(&table_name).to_string();
        let backend = self.backend.clone();
        let mutex = self.mutex.clone();
        let tables = self.tables.clone();
        let prev = std::thread::spawn(move || {
            common_runtime::block_on_read(async move {
                let _guard = mutex.lock(table_key.clone()).await;
                backend.delete(table_key.as_bytes()).await?;
                debug!(
                    "Successfully deleted catalog table entry, key: {}",
                    table_key
                );

                let tables = tables.load();
                let prev = tables.remove(&table_name).map(|en| en.1);
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
