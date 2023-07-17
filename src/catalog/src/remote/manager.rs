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
use std::sync::Arc;

use async_trait::async_trait;
use common_catalog::consts::MITO_ENGINE;
use common_meta::helper::{
    build_catalog_prefix, build_schema_prefix, build_table_global_prefix, CatalogKey, SchemaKey,
    TableGlobalKey, TableGlobalValue, TableRegionalKey, TableRegionalValue,
};
use common_meta::ident::TableIdent;
use common_meta::key::TableMetadataManagerRef;
use common_meta::kv_backend::KvBackendRef;
use common_meta::rpc::store::{PutRequest, RangeRequest};
use common_meta::rpc::KeyValue;
use common_telemetry::{debug, error, info, warn};
use metrics::increment_gauge;
use snafu::{ensure, OptionExt, ResultExt};
use table::engine::manager::TableEngineManagerRef;
use table::engine::{EngineContext, TableReference};
use table::requests::OpenTableRequest;
use table::TableRef;
use tokio::sync::Mutex;

use crate::error::{
    InvalidCatalogValueSnafu, OpenTableSnafu, ParallelOpenTableSnafu, Result,
    TableEngineNotFoundSnafu, TableExistsSnafu, TableMetadataManagerSnafu, TableNotFoundSnafu,
    UnimplementedSnafu,
};
use crate::local::MemoryCatalogManager;
use crate::remote::region_alive_keeper::RegionAliveKeepers;
use crate::{
    handle_system_table_request, CatalogManager, DeregisterSchemaRequest, DeregisterTableRequest,
    RegisterSchemaRequest, RegisterSystemTableRequest, RegisterTableRequest, RenameTableRequest,
};

/// Catalog manager based on metasrv.
pub struct RemoteCatalogManager {
    node_id: u64,
    backend: KvBackendRef,
    engine_manager: TableEngineManagerRef,
    system_table_requests: Mutex<Vec<RegisterSystemTableRequest>>,
    region_alive_keepers: Arc<RegionAliveKeepers>,
    memory_catalog_manager: Arc<MemoryCatalogManager>,
    table_metadata_manager: TableMetadataManagerRef,
}

impl RemoteCatalogManager {
    pub fn new(
        engine_manager: TableEngineManagerRef,
        node_id: u64,
        backend: KvBackendRef,
        region_alive_keepers: Arc<RegionAliveKeepers>,
        table_metadata_manager: TableMetadataManagerRef,
    ) -> Self {
        Self {
            engine_manager,
            node_id,
            backend,
            system_table_requests: Default::default(),
            region_alive_keepers,
            memory_catalog_manager: Arc::new(MemoryCatalogManager::default()),
            table_metadata_manager,
        }
    }

    async fn iter_remote_catalogs(&self) -> Result<Vec<CatalogKey>> {
        let catalog_range_prefix = build_catalog_prefix();
        let req = RangeRequest::new().with_prefix(catalog_range_prefix.as_bytes());

        let kvs = self
            .backend
            .range(req)
            .await
            .context(TableMetadataManagerSnafu)?
            .kvs;

        let catalogs = kvs
            .into_iter()
            .filter_map(|kv| {
                let catalog_key = String::from_utf8_lossy(kv.key());

                match CatalogKey::parse(&catalog_key) {
                    Ok(x) => Some(x),
                    Err(e) => {
                        error!(e; "Ignore invalid catalog key {:?}", catalog_key);
                        None
                    }
                }
            })
            .collect();
        Ok(catalogs)
    }

    /// Fetch catalogs/schemas/tables from remote catalog manager along with max table id allocated.
    async fn initiate_catalogs(&self) -> Result<()> {
        let catalogs = self.iter_remote_catalogs().await?;
        let mut joins = Vec::new();
        for CatalogKey { catalog_name } in catalogs {
            info!("Fetch catalog from metasrv: {}", catalog_name);

            let node_id = self.node_id;
            let backend = self.backend.clone();
            let engine_manager = self.engine_manager.clone();

            increment_gauge!(crate::metrics::METRIC_CATALOG_MANAGER_CATALOG_COUNT, 1.0);
            joins.push(self.initiate_schemas(node_id, backend, engine_manager, catalog_name));
        }

        futures::future::try_join_all(joins).await?;

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
    ) -> Result<()> {
        info!("initializing tables in {}.{}", catalog_name, schema_name);
        let kvs = iter_remote_tables(node_id, &backend, &catalog_name, &schema_name).await?;
        let joins = kvs
            .into_iter()
            .map(|(_, table_value)| {
                let engine_manager = engine_manager.clone();
                let memory_catalog_manager = self.memory_catalog_manager.clone();
                let table_metadata_manager = self.table_metadata_manager.clone();
                let table_id = table_value.table_id();
                common_runtime::spawn_bg(async move {
                    if let Err(e) = open_and_register_table(
                        node_id,
                        engine_manager,
                        &table_value,
                        memory_catalog_manager,
                        table_metadata_manager,
                    )
                    .await
                    {
                        // Note that we don't return error here if table opened failed. This is because
                        // we don't want those broken tables to impede the startup of Datanode.
                        // However, this could be changed in the future.
                        error!(e; "Failed to open or register table, id = {table_id}")
                    }
                })
            })
            .collect::<Vec<_>>();

        futures::future::try_join_all(joins)
            .await
            .context(ParallelOpenTableSnafu)?;
        Ok(())
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
        let schemas = iter_remote_schemas(&backend, &catalog_name).await?;

        let mut joins = Vec::new();
        for SchemaKey {
            catalog_name,
            schema_name,
        } in schemas
        {
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

        futures::future::try_join_all(joins).await?;
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
            table_id: Some(table.table_info().ident.table_id),
            version: table_version,
            regions_ids: table.table_info().meta.region_numbers.clone(),
            engine_name: Some(table_info.meta.engine.clone()),
        };
        let table_key = self
            .build_regional_table_key(catalog_name, schema_name, table_name)
            .to_string();
        let req = PutRequest::new()
            .with_key(table_key.as_bytes())
            .with_value(table_value.as_bytes().context(InvalidCatalogValueSnafu)?);
        self.backend
            .put(req)
            .await
            .context(TableMetadataManagerSnafu)?;
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
            .await
            .context(TableMetadataManagerSnafu)?
            .map(|KeyValue { key: _, value: v }| {
                let TableRegionalValue {
                    table_id,
                    engine_name,
                    ..
                } = TableRegionalValue::parse(String::from_utf8_lossy(&v))
                    .context(InvalidCatalogValueSnafu)?;
                Ok(engine_name.and_then(|name| table_id.map(|id| (name, id))))
            })
            .transpose()?
            .flatten();

        let Some((engine_name, table_id)) = engine_opt else {
                warn!("Cannot find table id and engine name for {table_key}");
                return Ok(None);
            };

        self.backend
            .delete(table_key.as_bytes(), false)
            .await
            .context(TableMetadataManagerSnafu)?;
        debug!(
            "Successfully deleted catalog table entry, key: {}",
            table_key
        );

        // deregistering table does not necessarily mean dropping the table
        let table = self
            .engine_manager
            .engine(&engine_name)
            .context(TableEngineNotFoundSnafu { engine_name })?
            .get_table(&EngineContext {}, table_id)
            .with_context(|_| {
                let reference = TableReference {
                    catalog: &catalog_name,
                    schema: &schema_name,
                    table: &table_name,
                };
                OpenTableSnafu {
                    table_info: reference.to_string(),
                }
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
}

async fn iter_remote_schemas<'a>(
    backend: &'a KvBackendRef,
    catalog_name: &'a str,
) -> Result<Vec<SchemaKey>> {
    let schema_prefix = build_schema_prefix(catalog_name);
    let req = RangeRequest::new().with_prefix(schema_prefix.as_bytes());

    let kvs = backend
        .range(req)
        .await
        .context(TableMetadataManagerSnafu)?
        .kvs;

    let schemas = kvs
        .into_iter()
        .filter_map(|kv| {
            let schema_key = String::from_utf8_lossy(kv.key());
            match SchemaKey::parse(&schema_key) {
                Ok(x) => Some(x),
                Err(e) => {
                    warn!("Ignore invalid schema key {:?}: {e}", schema_key);
                    None
                }
            }
        })
        .collect();
    Ok(schemas)
}

/// Iterate over all table entries on metasrv
async fn iter_remote_tables<'a>(
    node_id: u64,
    backend: &'a KvBackendRef,
    catalog_name: &'a str,
    schema_name: &'a str,
) -> Result<Vec<(TableGlobalKey, TableGlobalValue)>> {
    let table_prefix = build_table_global_prefix(catalog_name, schema_name);
    let req = RangeRequest::new().with_prefix(table_prefix.as_bytes());

    let kvs = backend
        .range(req)
        .await
        .context(TableMetadataManagerSnafu)?
        .kvs;

    let mut tables = Vec::with_capacity(kvs.len());
    for kv in kvs {
        let tgk = &String::from_utf8_lossy(kv.key());
        let Ok(table_key) = TableGlobalKey::parse(tgk) else {
            warn!("Ignore invalid table global key {:?}", tgk);
            continue;
        };

        let Ok(table_value) = TableGlobalValue::from_bytes(kv.value()) else {
            warn!("Ignore invalid table global value {:?}", String::from_utf8_lossy(kv.value()));
            continue;
        };

        info!("Found catalog table entry, key: {table_key}, value: {table_value:?}");

        // metasrv has allocated region ids to current datanode
        if table_value
            .regions_id_map
            .get(&node_id)
            .map(|v| !v.is_empty())
            .unwrap_or(false)
        {
            tables.push((table_key, table_value))
        }
    }
    Ok(tables)
}

async fn open_and_register_table(
    node_id: u64,
    engine_manager: TableEngineManagerRef,
    table_value: &TableGlobalValue,
    memory_catalog_manager: Arc<MemoryCatalogManager>,
    _table_metadata_manager: TableMetadataManagerRef,
) -> Result<()> {
    let context = EngineContext {};

    let table_id = table_value.table_id();

    let TableGlobalValue {
        table_info,
        regions_id_map,
        ..
    } = table_value;

    let catalog_name = table_info.catalog_name.clone();
    let schema_name = table_info.schema_name.clone();
    let table_name = table_info.name.clone();

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

    let table_ident = TableIdent {
        catalog: catalog_name,
        schema: schema_name,
        table: table_name,
        table_id,
        engine: table_info.meta.engine.clone(),
    };

    let table = engine
        .open_table(&context, request)
        .await
        .with_context(|_| OpenTableSnafu {
            table_info: table_ident.to_string(),
        })?
        .with_context(|| TableNotFoundSnafu {
            table_info: table_ident.to_string(),
        })?;
    info!("Successfully opened table, {table_ident}");

    if !memory_catalog_manager
        .catalog_exist(&table_ident.catalog)
        .await?
    {
        memory_catalog_manager.register_catalog_sync(table_ident.catalog.clone())?;
    }

    if !memory_catalog_manager
        .schema_exist(&table_ident.catalog, &table_ident.schema)
        .await?
    {
        memory_catalog_manager.register_schema_sync(RegisterSchemaRequest {
            catalog: table_ident.catalog.clone(),
            schema: table_ident.schema.clone(),
        })?;
    }

    let request = RegisterTableRequest {
        catalog: table_ident.catalog.clone(),
        schema: table_ident.schema.clone(),
        table_name: table_ident.table.clone(),
        table_id,
        table,
    };
    let registered = memory_catalog_manager.register_table_sync(request)?;
    ensure!(
        registered,
        TableExistsSnafu {
            table: table_ident.to_string(),
        }
    );
    info!("Successfully registered table, {table_ident}");
    Ok(())
}

#[async_trait]
impl CatalogManager for RemoteCatalogManager {
    async fn start(&self) -> Result<()> {
        self.initiate_catalogs().await?;

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
        let catalog = request.catalog.clone();
        let schema = request.schema.clone();
        let table_name = request.table_name.clone();
        let table = request.table.clone();

        let registered = self.memory_catalog_manager.register_table_sync(request)?;

        if registered {
            self.register_table(catalog, schema, table_name, table.clone())
                .await?;

            let table_info = table.table_info();
            let table_ident = TableIdent {
                catalog: table_info.catalog_name.clone(),
                schema: table_info.schema_name.clone(),
                table: table_info.name.clone(),
                table_id: table_info.table_id(),
                engine: table_info.meta.engine.clone(),
            };
            self.region_alive_keepers
                .register_table(table_ident, table)
                .await?;
        }

        Ok(registered)
    }

    async fn deregister_table(&self, request: DeregisterTableRequest) -> Result<()> {
        let Some(table) = self
            .memory_catalog_manager
            .table(&request.catalog, &request.schema, &request.table_name)
            .await? else { return Ok(()) };

        self.deregister_table(
            request.catalog.clone(),
            request.schema.clone(),
            request.table_name.clone(),
        )
        .await?;

        let table_info = table.table_info();
        let table_ident = TableIdent {
            catalog: request.catalog.clone(),
            schema: request.schema.clone(),
            table: request.table_name.clone(),
            table_id: table_info.ident.table_id,
            engine: table_info.meta.engine.clone(),
        };
        if let Some(keeper) = self
            .region_alive_keepers
            .deregister_table(&table_ident)
            .await
        {
            warn!(
                "Table {} is deregistered from region alive keepers",
                keeper.table_ident(),
            );
        }

        self.memory_catalog_manager.deregister_table(request).await
    }

    async fn register_schema(&self, request: RegisterSchemaRequest) -> Result<bool> {
        self.memory_catalog_manager.register_schema_sync(request)
    }

    async fn deregister_schema(&self, _request: DeregisterSchemaRequest) -> Result<bool> {
        UnimplementedSnafu {
            operation: "deregister schema",
        }
        .fail()
    }

    async fn rename_table(&self, request: RenameTableRequest) -> Result<bool> {
        self.memory_catalog_manager.rename_table(request).await
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
        if !self.catalog_exist(catalog).await? {
            return Ok(false);
        }

        if self
            .memory_catalog_manager
            .schema_exist(catalog, schema)
            .await?
        {
            return Ok(true);
        }

        let key = self
            .build_schema_key(catalog.to_string(), schema.to_string())
            .to_string();
        let remote_schema_exists = self
            .backend
            .get(key.as_bytes())
            .await
            .context(TableMetadataManagerSnafu)?
            .is_some();

        // Create schema locally if remote schema exists. Since local schema is managed by memory
        // catalog manager, creating a local schema is relatively cheap (just a HashMap).
        // Besides, if this method ("schema_exist) is called, it's very likely that someone wants to
        // create a table in this schema. We should create the schema now.
        if remote_schema_exists
            && self
                .memory_catalog_manager
                .register_schema(RegisterSchemaRequest {
                    catalog: catalog.to_string(),
                    schema: schema.to_string(),
                })
                .await?
        {
            info!("register schema '{catalog}/{schema}' on demand");
        }

        Ok(remote_schema_exists)
    }

    async fn table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<TableRef>> {
        self.memory_catalog_manager
            .table(catalog_name, schema_name, table_name)
            .await
    }

    async fn catalog_exist(&self, catalog: &str) -> Result<bool> {
        if self.memory_catalog_manager.catalog_exist(catalog).await? {
            return Ok(true);
        }

        let key = CatalogKey {
            catalog_name: catalog.to_string(),
        };

        let remote_catalog_exists = self
            .backend
            .get(key.to_string().as_bytes())
            .await
            .context(TableMetadataManagerSnafu)?
            .is_some();

        // Create catalog locally if remote catalog exists. Since local catalog is managed by memory
        // catalog manager, creating a local catalog is relatively cheap (just a HashMap).
        // Besides, if this method ("catalog_exist) is called, it's very likely that someone wants to
        // create a table in this catalog. We should create the catalog now.
        if remote_catalog_exists
            && self
                .memory_catalog_manager
                .register_catalog(catalog.to_string())
                .await?
        {
            info!("register catalog '{catalog}' on demand");
        }

        Ok(remote_catalog_exists)
    }

    async fn table_exist(&self, catalog: &str, schema: &str, table: &str) -> Result<bool> {
        if !self.catalog_exist(catalog).await? {
            return Ok(false);
        }

        if !self.schema_exist(catalog, schema).await? {
            return Ok(false);
        }

        self.memory_catalog_manager
            .table_exist(catalog, schema, table)
            .await
    }

    async fn catalog_names(&self) -> Result<Vec<String>> {
        self.memory_catalog_manager.catalog_names().await
    }

    async fn schema_names(&self, catalog_name: &str) -> Result<Vec<String>> {
        self.memory_catalog_manager.schema_names(catalog_name).await
    }

    async fn table_names(&self, catalog_name: &str, schema_name: &str) -> Result<Vec<String>> {
        self.memory_catalog_manager
            .table_names(catalog_name, schema_name)
            .await
    }

    async fn register_catalog(&self, name: String) -> Result<bool> {
        self.memory_catalog_manager.register_catalog_sync(name)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
