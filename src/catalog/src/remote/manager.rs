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

use async_stream::stream;
use async_trait::async_trait;
use common_catalog::consts::{MAX_SYS_TABLE_ID, MITO_ENGINE};
use common_telemetry::{debug, error, info, warn};
use dashmap::DashMap;
use futures::Stream;
use futures_util::{StreamExt, TryStreamExt};
use metrics::{decrement_gauge, increment_gauge};
use parking_lot::RwLock;
use snafu::ResultExt;
use table::engine::manager::TableEngineManagerRef;
use table::engine::{EngineContext, TableReference};
use table::requests::{CreateTableRequest, OpenTableRequest};
use table::TableRef;
use tokio::sync::Mutex;

use crate::error::{
    CatalogNotFoundSnafu, CreateTableSnafu, InvalidCatalogValueSnafu, OpenTableSnafu,
    ParallelOpenTableSnafu, Result, SchemaNotFoundSnafu, TableEngineNotFoundSnafu,
};
use crate::helper::{
    build_catalog_prefix, build_schema_prefix, build_table_global_prefix,
    build_table_regional_prefix, CatalogKey, CatalogValue, SchemaKey, SchemaValue, TableGlobalKey,
    TableGlobalValue, TableRegionalKey, TableRegionalValue, CATALOG_KEY_PREFIX,
};
use crate::remote::{Kv, KvBackendRef};
use crate::{
    handle_system_table_request, CatalogManager, CatalogProvider, CatalogProviderRef,
    DeregisterTableRequest, RegisterSchemaRequest, RegisterSystemTableRequest,
    RegisterTableRequest, RenameTableRequest,
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

    fn new_catalog_provider(&self, catalog_name: &str) -> CatalogProviderRef {
        Arc::new(RemoteCatalogProvider {
            node_id: self.node_id,
            catalog_name: catalog_name.to_string(),
            backend: self.backend.clone(),
            engine_manager: self.engine_manager.clone(),
        }) as _
    }

    async fn iter_remote_catalogs(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<CatalogKey>> + Send + '_>> {
        let catalog_range_prefix = build_catalog_prefix();
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

    /// Fetch catalogs/schemas/tables from remote catalog manager along with max table id allocated.
    async fn initiate_catalogs(&self) -> Result<HashMap<String, CatalogProviderRef>> {
        let mut res = HashMap::new();

        let mut catalogs = self.iter_remote_catalogs().await;
        let mut joins = Vec::new();
        while let Some(r) = catalogs.next().await {
            let CatalogKey { catalog_name, .. } = r?;
            info!("Fetch catalog from metasrv: {}", catalog_name);
            let catalog = res
                .entry(catalog_name.clone())
                .or_insert_with(|| self.new_catalog_provider(&catalog_name))
                .clone();

            let node_id = self.node_id;
            let backend = self.backend.clone();
            let engine_manager = self.engine_manager.clone();

            increment_gauge!(crate::metrics::METRIC_CATALOG_MANAGER_CATALOG_COUNT, 1.0);

            joins.push(self.initiate_schemas(node_id, backend, engine_manager, catalog_name));
        }

        futures::future::try_join_all(joins).await?;
        // .context(ParallelOpenTableSnafu)?
        // .into_iter()
        // .collect::<Result<Vec<_>>>()?;

        Ok(res)
    }

    pub async fn create_catalog_and_schema(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<()> {
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
        Ok(())
    }

    fn build_schema_key(&self, catalog_name: String, schema_name: String) -> SchemaKey {
        SchemaKey {
            catalog_name,
            schema_name,
        }
    }

    /// Initiates all tables inside the catalog by fetching data from metasrv.
    /// Return maximum table id in the schema.
    async fn initiate_tables(
        &self,
        node_id: u64,
        backend: KvBackendRef,
        engine_manager: TableEngineManagerRef,
        catalog_name: String,
        schema_name: String,
    ) -> Result<u32> {
        info!("initializing tables in {}.{}", catalog_name, schema_name);
        let tables = iter_remote_tables(node_id, &backend, &catalog_name, &schema_name).await;

        let kvs = tables.try_collect::<Vec<_>>().await?;
        let table_num = kvs.len();
        let joins = kvs
            .into_iter()
            .map(|(table_key, table_value)| {
                let engine_manager = engine_manager.clone();
                let backend = backend.clone();
                common_runtime::spawn_bg(async move {
                    match open_or_create_table(node_id, engine_manager, &table_key, &table_value)
                        .await
                    {
                        Ok(table_ref) => {
                            let table_info = table_ref.table_info();
                            let table_name = &table_info.name;
                            info!("Registered table {}", table_name);
                            Ok(Some(table_info.ident.table_id))
                        }
                        Err(err) => {
                            warn!(
                                "Node id: {}, failed to open table: {}, source: {}",
                                node_id, table_key, err
                            );
                            debug!(
                                "Node id: {}, TableGlobalKey: {}, value: {:?},",
                                node_id, table_key, table_value
                            );
                            print_regional_key_debug_info(node_id, backend, &table_key).await;

                            Ok(None)
                        }
                    }
                })
            })
            .collect::<Vec<_>>();

        let opened_table_ids = futures::future::try_join_all(joins)
            .await
            .context(ParallelOpenTableSnafu)?
            .into_iter()
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        let opened = opened_table_ids.len();

        let max_table_id = opened_table_ids
            .into_iter()
            .max()
            .unwrap_or(MAX_SYS_TABLE_ID);

        increment_gauge!(
            crate::metrics::METRIC_CATALOG_MANAGER_TABLE_COUNT,
            table_num as f64,
            &[crate::metrics::db_label(&catalog_name, &schema_name)],
        );
        info!(
            "initialized tables in {}.{}, total: {}, opened: {}, failed: {}",
            catalog_name,
            schema_name,
            table_num,
            opened,
            table_num - opened
        );

        Ok(max_table_id)
    }

    /// Initiates all schemas inside the catalog by fetching data from metasrv.
    /// Return maximum table id in the catalog.
    async fn initiate_schemas(
        &self,
        node_id: u64,
        backend: KvBackendRef,
        engine_manager: TableEngineManagerRef,
        catalog_name: String,
    ) -> Result<()> {
        let mut schemas = iter_remote_schemas(&backend, &catalog_name).await;
        let mut joins = Vec::new();
        while let Some(r) = schemas.next().await {
            let SchemaKey {
                catalog_name,
                schema_name,
                ..
            } = r?;

            info!(
                "Fetch schema from metasrv: {}.{}",
                &catalog_name, &schema_name
            );
            increment_gauge!(crate::metrics::METRIC_CATALOG_MANAGER_SCHEMA_COUNT, 1.0);

            let backend = backend.clone();
            let engine_manager = engine_manager.clone();

            joins.push(self.initiate_tables(
                node_id,
                backend,
                engine_manager,
                catalog_name,
                schema_name,
            ));
        }

        let mut max_table_id = MAX_SYS_TABLE_ID;
        if let Some(found_max_table_id) = futures::future::try_join_all(joins)
            .await?
            .into_iter()
            .max()
        {
            max_table_id = max_table_id.max(found_max_table_id);
            info!(
                "Catalog name: {}, max table id allocated: {}",
                catalog_name, max_table_id
            );
        }

        Ok(())
    }

    async fn register_table(
        &self,
        catalog_name: String,
        schema_name: String,
        table_name: String,
        table: TableRef,
    ) -> Result<Option<TableRef>> {
        let table_info = table.table_info();
        let table_version = table_info.ident.version;
        let table_value = TableRegionalValue {
            version: table_version,
            regions_ids: table.table_info().meta.region_numbers.clone(),
            engine_name: Some(table_info.meta.engine.clone()),
        };
        let table_key = self
            .build_regional_table_key(catalog_name, schema_name, table_name)
            .to_string();
        self.backend
            .set(
                table_key.as_bytes(),
                &table_value.as_bytes().context(InvalidCatalogValueSnafu)?,
            )
            .await?;
        debug!(
            "Successfully set catalog table entry, key: {}, table value: {:?}",
            table_key, table_value
        );

        // TODO(hl): retrieve prev table info using cas
        Ok(None)
    }

    async fn deregister_table(
        &self,
        catalog_name: String,
        schema_name: String,
        table_name: String,
    ) -> Result<Option<TableRef>> {
        let table_key = self
            .build_regional_table_key(
                catalog_name.clone(),
                schema_name.clone(),
                table_name.clone(),
            )
            .to_string();

        let engine_opt = self
            .backend
            .get(table_key.as_bytes())
            .await?
            .map(|Kv(_, v)| {
                let TableRegionalValue { engine_name, .. } =
                    TableRegionalValue::parse(String::from_utf8_lossy(&v))
                        .context(InvalidCatalogValueSnafu)?;
                Ok(engine_name)
            })
            .transpose()?
            .flatten();

        let engine_name = engine_opt.as_deref().unwrap_or_else(|| {
            warn!("Cannot find table engine name for {table_key}");
            MITO_ENGINE
        });

        self.backend.delete(table_key.as_bytes()).await?;
        debug!(
            "Successfully deleted catalog table entry, key: {}",
            table_key
        );

        let reference = TableReference {
            catalog: &catalog_name,
            schema: &schema_name,
            table: &table_name,
        };
        // deregistering table does not necessarily mean dropping the table
        let table = self
            .engine_manager
            .engine(engine_name)
            .context(TableEngineNotFoundSnafu { engine_name })?
            .get_table(&EngineContext {}, &reference)
            .with_context(|_| OpenTableSnafu {
                table_info: reference.to_string(),
            })?;
        Ok(table)
    }

    fn build_regional_table_key(
        &self,
        catalog_name: String,
        schema_name: String,
        table_name: String,
    ) -> TableRegionalKey {
        TableRegionalKey {
            catalog_name,
            schema_name,
            table_name,
            node_id: self.node_id,
        }
    }

    async fn check_catalog_schema_exist(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<()> {
        if !self.catalog_exist(catalog_name).await? {
            return CatalogNotFoundSnafu { catalog_name }.fail()?;
        }
        if !self.schema_exist(catalog_name, schema_name).await? {
            return SchemaNotFoundSnafu {
                catalog: catalog_name,
                schema: schema_name,
            }
            .fail()?;
        }
        Ok(())
    }
}

async fn iter_remote_schemas<'a>(
    backend: &'a KvBackendRef,
    catalog_name: &'a str,
) -> Pin<Box<dyn Stream<Item = Result<SchemaKey>> + Send + 'a>> {
    let schema_prefix = build_schema_prefix(catalog_name);
    let mut schemas = backend.range(schema_prefix.as_bytes());

    Box::pin(stream!({
        while let Some(r) = schemas.next().await {
            let Kv(k, _) = r?;
            if !k.starts_with(schema_prefix.as_bytes()) {
                debug!("Ignoring non-schema key: {}", String::from_utf8_lossy(&k));
                continue;
            }

            let schema_key =
                SchemaKey::parse(&String::from_utf8_lossy(&k)).context(InvalidCatalogValueSnafu)?;
            yield Ok(schema_key)
        }
    }))
}

/// Iterate over all table entries on metasrv
async fn iter_remote_tables<'a>(
    node_id: u64,
    backend: &'a KvBackendRef,
    catalog_name: &'a str,
    schema_name: &'a str,
) -> Pin<Box<dyn Stream<Item = Result<(TableGlobalKey, TableGlobalValue)>> + Send + 'a>> {
    let table_prefix = build_table_regional_prefix(catalog_name, schema_name);
    let mut tables = backend.range(table_prefix.as_bytes());
    Box::pin(stream!({
        while let Some(r) = tables.next().await {
            let Kv(k, v) = r?;
            if !k.starts_with(table_prefix.as_bytes()) {
                debug!("Ignoring non-table prefix: {}", String::from_utf8_lossy(&k));
                continue;
            }
            let table_key = TableGlobalKey::parse(&String::from_utf8_lossy(&k))
                .context(InvalidCatalogValueSnafu)?;
            let table_value = TableGlobalValue::from_bytes(&v).context(InvalidCatalogValueSnafu)?;

            info!(
                "Found catalog table entry, key: {}, value: {:?}",
                table_key, table_value
            );
            // metasrv has allocated region ids to current datanode
            if table_value
                .regions_id_map
                .get(&node_id)
                .map(|v| !v.is_empty())
                .unwrap_or(false)
            {
                yield Ok((table_key, table_value))
            }
        }
    }))
}

async fn print_regional_key_debug_info(
    node_id: u64,
    backend: KvBackendRef,
    table_key: &TableGlobalKey,
) {
    let regional_key = TableRegionalKey {
        catalog_name: table_key.catalog_name.clone(),
        schema_name: table_key.schema_name.clone(),
        table_name: table_key.table_name.clone(),
        node_id,
    }
    .to_string();

    match backend.get(regional_key.as_bytes()).await {
        Ok(Some(Kv(_, values_bytes))) => {
            debug!(
                "Node id: {}, TableRegionalKey: {}, value: {},",
                node_id,
                table_key,
                String::from_utf8_lossy(&values_bytes),
            );
        }
        Ok(None) => {
            debug!(
                "Node id: {}, TableRegionalKey: {}, value: None",
                node_id, table_key,
            );
        }
        Err(err) => {
            debug!(
                "Node id: {}, failed to fetch TableRegionalKey: {}, source: {}",
                node_id, regional_key, err
            );
        }
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
        region_numbers: region_numbers.clone(),
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

#[async_trait]
impl CatalogManager for RemoteCatalogManager {
    async fn start(&self) -> Result<()> {
        let catalogs = self.initiate_catalogs().await?;
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

        let mut system_table_requests = self.system_table_requests.lock().await;
        let engine = self
            .engine_manager
            .engine(MITO_ENGINE)
            .context(TableEngineNotFoundSnafu {
                engine_name: MITO_ENGINE,
            })?;
        handle_system_table_request(self, engine, &mut system_table_requests).await?;
        info!("All system table opened");
        Ok(())
    }

    async fn register_table(&self, request: RegisterTableRequest) -> Result<bool> {
        let catalog_name = request.catalog;
        let schema_name = request.schema;
        self.check_catalog_schema_exist(&catalog_name, &schema_name)
            .await?;

        self.register_table(
            catalog_name.clone(),
            schema_name.clone(),
            request.table_name,
            request.table,
        )
        .await?;

        increment_gauge!(
            crate::metrics::METRIC_CATALOG_MANAGER_TABLE_COUNT,
            1.0,
            &[crate::metrics::db_label(&catalog_name, &schema_name)],
        );
        Ok(true)
    }

    async fn deregister_table(&self, request: DeregisterTableRequest) -> Result<bool> {
        let catalog_name = request.catalog;
        let schema_name = request.schema;
        self.check_catalog_schema_exist(&catalog_name, &schema_name)
            .await?;

        let result = self
            .deregister_table(
                catalog_name.clone(),
                schema_name.clone(),
                request.table_name,
            )
            .await?;
        decrement_gauge!(
            crate::metrics::METRIC_CATALOG_MANAGER_TABLE_COUNT,
            1.0,
            &[crate::metrics::db_label(&catalog_name, &schema_name)],
        );
        Ok(result.is_none())
    }

    async fn register_schema(&self, request: RegisterSchemaRequest) -> Result<bool> {
        let catalog_name = request.catalog;
        let schema_name = request.schema;
        let key = self.build_schema_key(catalog_name, schema_name).to_string();
        self.backend
            .set(
                key.as_bytes(),
                &SchemaValue {}
                    .as_bytes()
                    .context(InvalidCatalogValueSnafu)?,
            )
            .await?;

        increment_gauge!(crate::metrics::METRIC_CATALOG_MANAGER_SCHEMA_COUNT, 1.0);
        Ok(true)
    }

    async fn rename_table(&self, request: RenameTableRequest) -> Result<bool> {
        let catalog_name = request.catalog.clone();
        let schema_name = request.schema.clone();
        self.check_catalog_schema_exist(&catalog_name, &schema_name)
            .await?;

        let old_table_key = TableRegionalKey {
            catalog_name: request.catalog.clone(),
            schema_name: request.schema.clone(),
            table_name: request.table_name.clone(),
            node_id: self.node_id,
        }
        .to_string();
        let Some(Kv(_, value_bytes)) = self.backend.get(old_table_key.as_bytes()).await? else {
            return Ok(false)
        };
        let new_table_key = TableRegionalKey {
            catalog_name: request.catalog.clone(),
            schema_name: request.schema.clone(),
            table_name: request.new_table_name,
            node_id: self.node_id,
        };
        self.backend
            .set(new_table_key.to_string().as_bytes(), &value_bytes)
            .await?;
        self.backend
            .delete(old_table_key.to_string().as_bytes())
            .await?;
        Ok(true)
    }

    async fn register_system_table(&self, request: RegisterSystemTableRequest) -> Result<()> {
        let catalog_name = request.create_table_request.catalog_name.clone();
        let schema_name = request.create_table_request.schema_name.clone();

        let mut requests = self.system_table_requests.lock().await;
        requests.push(request);
        increment_gauge!(
            crate::metrics::METRIC_CATALOG_MANAGER_TABLE_COUNT,
            1.0,
            &[crate::metrics::db_label(&catalog_name, &schema_name)],
        );
        Ok(())
    }

    async fn schema_exist(&self, catalog: &str, schema: &str) -> Result<bool> {
        let key = self
            .build_schema_key(catalog.to_string(), schema.to_string())
            .to_string();
        Ok(self.backend.get(key.as_bytes()).await?.is_some())
    }

    async fn table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<TableRef>> {
        self.check_catalog_schema_exist(catalog_name, schema_name)
            .await?;

        let key = self
            .build_regional_table_key(
                catalog_name.to_string(),
                schema_name.to_string(),
                table_name.to_string(),
            )
            .to_string();
        let table_opt = self
            .backend
            .get(key.as_bytes())
            .await?
            .map(|Kv(_, v)| {
                let TableRegionalValue { engine_name, .. } =
                    TableRegionalValue::parse(String::from_utf8_lossy(&v))
                        .context(InvalidCatalogValueSnafu)?;
                let reference = TableReference {
                    catalog: catalog_name,
                    schema: schema_name,
                    table: table_name,
                };
                let engine_name = engine_name.as_deref().unwrap_or(MITO_ENGINE);
                let engine = self
                    .engine_manager
                    .engine(engine_name)
                    .context(TableEngineNotFoundSnafu { engine_name })?;
                let table = engine
                    .get_table(&EngineContext {}, &reference)
                    .with_context(|_| OpenTableSnafu {
                        table_info: reference.to_string(),
                    })?;
                Ok(table)
            })
            .transpose()?
            .flatten();

        Ok(table_opt)
    }

    async fn catalog_exist(&self, catalog: &str) -> Result<bool> {
        let key = CatalogKey {
            catalog_name: catalog.to_string(),
        };
        Ok(self
            .backend
            .get(key.to_string().as_bytes())
            .await?
            .is_some())
    }

    async fn table_exist(&self, catalog: &str, schema: &str, table: &str) -> Result<bool> {
        self.check_catalog_schema_exist(catalog, schema).await?;

        let key = TableRegionalKey {
            catalog_name: catalog.to_string(),
            schema_name: schema.to_string(),
            table_name: table.to_string(),
            node_id: self.node_id,
        }
        .to_string();

        Ok(self.backend.get(key.as_bytes()).await?.is_some())
    }

    async fn catalog_names(&self) -> Result<Vec<String>> {
        let mut stream = self.backend.range(CATALOG_KEY_PREFIX.as_bytes());
        let mut catalogs = HashSet::new();

        while let Some(catalog) = stream.next().await {
            if let Ok(catalog) = catalog {
                let catalog_key = String::from_utf8_lossy(&catalog.0);

                if let Ok(key) = CatalogKey::parse(&catalog_key) {
                    catalogs.insert(key.catalog_name);
                }
            }
        }

        Ok(catalogs.into_iter().collect())
    }

    async fn schema_names(&self, catalog_name: &str) -> Result<Vec<String>> {
        let mut stream = self
            .backend
            .range(build_schema_prefix(catalog_name).as_bytes());
        let mut schemas = HashSet::new();

        while let Some(schema) = stream.next().await {
            if let Ok(schema) = schema {
                let schema_key = String::from_utf8_lossy(&schema.0);

                if let Ok(key) = SchemaKey::parse(&schema_key) {
                    schemas.insert(key.schema_name);
                }
            }
        }
        Ok(schemas.into_iter().collect())
    }

    async fn table_names(&self, catalog_name: &str, schema_name: &str) -> Result<Vec<String>> {
        self.check_catalog_schema_exist(catalog_name, schema_name)
            .await?;

        let mut stream = self
            .backend
            .range(build_table_regional_prefix(catalog_name, schema_name).as_bytes());
        let mut tables = HashSet::new();

        while let Some(table) = stream.next().await {
            if let Ok(table) = table {
                let table_key = String::from_utf8_lossy(&table.0);

                if let Ok(key) = TableRegionalKey::parse(&table_key) {
                    tables.insert(key.table_name);
                }
            }
        }
        Ok(tables.into_iter().collect())
    }

    async fn register_catalog(&self, name: String) -> Result<bool> {
        let key = CatalogKey { catalog_name: name }.to_string();
        // TODO(hl): use compare_and_swap to prevent concurrent update
        self.backend
            .set(
                key.as_bytes(),
                &CatalogValue {}
                    .as_bytes()
                    .context(InvalidCatalogValueSnafu)?,
            )
            .await?;
        increment_gauge!(crate::metrics::METRIC_CATALOG_MANAGER_CATALOG_COUNT, 1.0);
        Ok(false)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct RemoteCatalogProvider {
    node_id: u64,
    catalog_name: String,
    backend: KvBackendRef,
    engine_manager: TableEngineManagerRef,
}

impl RemoteCatalogProvider {
    pub fn new(
        catalog_name: String,
        backend: KvBackendRef,
        engine_manager: TableEngineManagerRef,
        node_id: u64,
    ) -> Self {
        Self {
            node_id,
            catalog_name,
            backend,
            engine_manager,
        }
    }

    fn build_schema_key(&self, schema_name: impl AsRef<str>) -> SchemaKey {
        SchemaKey {
            catalog_name: self.catalog_name.clone(),
            schema_name: schema_name.as_ref().to_string(),
        }
    }
}

#[async_trait::async_trait]
impl CatalogProvider for RemoteCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn schema_names(&self) -> Result<Vec<String>> {
        let schema_prefix = build_schema_prefix(&self.catalog_name);

        let remote_schemas = self.backend.range(schema_prefix.as_bytes());
        let res = remote_schemas
            .map(|kv| {
                let Kv(k, _) = kv?;
                let schema_key = SchemaKey::parse(String::from_utf8_lossy(&k))
                    .context(InvalidCatalogValueSnafu)?;
                Ok(schema_key.schema_name)
            })
            .try_collect()
            .await?;
        Ok(res)
    }
}

pub struct RemoteSchemaProvider {
    catalog_name: String,
    schema_name: String,
    node_id: u64,
    backend: KvBackendRef,
    engine_manager: TableEngineManagerRef,
}

impl RemoteSchemaProvider {
    pub fn new(
        catalog_name: String,
        schema_name: String,
        node_id: u64,
        engine_manager: TableEngineManagerRef,
        backend: KvBackendRef,
    ) -> Self {
        Self {
            catalog_name,
            schema_name,
            node_id,
            backend,
            engine_manager,
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
