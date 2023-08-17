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
use common_meta::ident::TableIdent;
use common_meta::key::catalog_name::CatalogNameKey;
use common_meta::key::datanode_table::DatanodeTableValue;
use common_meta::key::schema_name::SchemaNameKey;
use common_meta::key::TableMetadataManagerRef;
use common_telemetry::{error, info, warn};
use futures_util::TryStreamExt;
use metrics::increment_gauge;
use snafu::{ensure, OptionExt, ResultExt};
use table::engine::manager::TableEngineManagerRef;
use table::engine::EngineContext;
use table::requests::OpenTableRequest;
use table::TableRef;
use tokio::sync::Mutex;

use crate::error::{
    OpenTableSnafu, ParallelOpenTableSnafu, Result, TableEngineNotFoundSnafu, TableExistsSnafu,
    TableMetadataManagerSnafu, TableNotFoundSnafu, UnimplementedSnafu,
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
        region_alive_keepers: Arc<RegionAliveKeepers>,
        table_metadata_manager: TableMetadataManagerRef,
    ) -> Self {
        Self {
            engine_manager,
            node_id,
            system_table_requests: Default::default(),
            region_alive_keepers,
            memory_catalog_manager: MemoryCatalogManager::with_default_setup(),
            table_metadata_manager,
        }
    }

    async fn initiate_catalogs(&self) -> Result<()> {
        let tables = self
            .table_metadata_manager
            .datanode_table_manager()
            .tables(self.node_id)
            .try_collect::<Vec<_>>()
            .await
            .context(TableMetadataManagerSnafu)?;

        let joins = tables
            .into_iter()
            .map(|datanode_table_value| {
                let engine_manager = self.engine_manager.clone();
                let memory_catalog_manager = self.memory_catalog_manager.clone();
                let table_metadata_manager = self.table_metadata_manager.clone();
                common_runtime::spawn_bg(async move {
                    let table_id = datanode_table_value.table_id;
                    if let Err(e) = open_and_register_table(
                        engine_manager,
                        datanode_table_value,
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

        let _ = futures::future::try_join_all(joins)
            .await
            .context(ParallelOpenTableSnafu)?;
        Ok(())
    }
}

async fn open_and_register_table(
    engine_manager: TableEngineManagerRef,
    datanode_table_value: DatanodeTableValue,
    memory_catalog_manager: Arc<MemoryCatalogManager>,
    table_metadata_manager: TableMetadataManagerRef,
) -> Result<()> {
    let context = EngineContext {};

    let table_id = datanode_table_value.table_id;
    let region_numbers = datanode_table_value.regions;

    let table_info_value = table_metadata_manager
        .table_info_manager()
        .get(table_id)
        .await
        .context(TableMetadataManagerSnafu)?
        .context(TableNotFoundSnafu {
            table_info: format!("table id: {table_id}"),
        })?;
    let table_info = &table_info_value.table_info;
    let catalog_name = table_info.catalog_name.clone();
    let schema_name = table_info.schema_name.clone();
    let table_name = table_info.name.clone();

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
        let table = request.table.clone();

        let registered = self.memory_catalog_manager.register_table_sync(request)?;

        if registered {
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
            .await?
        else {
            return Ok(());
        };

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

        let remote_schema_exists = self
            .table_metadata_manager
            .schema_manager()
            .exist(SchemaNameKey::new(catalog, schema))
            .await
            .context(TableMetadataManagerSnafu)?;
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

        let key = CatalogNameKey::new(catalog);
        let remote_catalog_exists = self
            .table_metadata_manager
            .catalog_manager()
            .exist(key)
            .await
            .context(TableMetadataManagerSnafu)?;

        // Create catalog locally if remote catalog exists. Since local catalog is managed by memory
        // catalog manager, creating a local catalog is relatively cheap (just a HashMap).
        // Besides, if this method ("catalog_exist) is called, it's very likely that someone wants to
        // create a table in this catalog. We should create the catalog now.
        if remote_catalog_exists
            && self
                .memory_catalog_manager
                .clone()
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

    async fn register_catalog(self: Arc<Self>, name: String) -> Result<bool> {
        self.memory_catalog_manager.register_catalog_sync(name)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
