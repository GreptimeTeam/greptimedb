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

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use common_catalog::consts::IMMUTABLE_FILE_ENGINE;
use common_error::ext::BoxedError;
use common_procedure::{BoxedProcedure, ProcedureManager};
use common_telemetry::{debug, logging};
use datatypes::schema::Schema;
use object_store::ObjectStore;
use snafu::ResultExt;
use table::engine::{table_dir, EngineContext, TableEngine, TableEngineProcedure, TableReference};
use table::error::TableOperationSnafu;
use table::metadata::{TableId, TableInfo, TableInfoBuilder, TableMetaBuilder, TableType};
use table::requests::{AlterTableRequest, CreateTableRequest, DropTableRequest, OpenTableRequest};
use table::{error as table_error, Result as TableResult, Table, TableRef};
use tokio::sync::Mutex;

use crate::config::EngineConfig;
use crate::engine::procedure::{self, CreateImmutableFileTable, DropImmutableFileTable};
use crate::engine::INIT_TABLE_VERSION;
use crate::error::{
    BuildTableInfoSnafu, BuildTableMetaSnafu, DropTableSnafu, InvalidRawSchemaSnafu, Result,
    TableExistsSnafu,
};
use crate::manifest::immutable::{delete_table_manifest, ImmutableMetadata};
use crate::manifest::table_manifest_dir;
use crate::table::immutable::{ImmutableFileTable, ImmutableFileTableRef};

/// [TableEngine] implementation.
#[derive(Clone)]
pub struct ImmutableFileTableEngine {
    inner: Arc<EngineInner>,
}

#[async_trait]
impl TableEngine for ImmutableFileTableEngine {
    fn name(&self) -> &str {
        IMMUTABLE_FILE_ENGINE
    }

    async fn create_table(
        &self,
        ctx: &EngineContext,
        request: CreateTableRequest,
    ) -> TableResult<TableRef> {
        self.inner
            .create_table(ctx, request)
            .await
            .map_err(BoxedError::new)
            .context(table_error::TableOperationSnafu)
    }

    async fn open_table(
        &self,
        ctx: &EngineContext,
        request: OpenTableRequest,
    ) -> TableResult<Option<TableRef>> {
        self.inner
            .open_table(ctx, request)
            .await
            .map_err(BoxedError::new)
            .context(table_error::TableOperationSnafu)
    }

    async fn alter_table(
        &self,
        _ctx: &EngineContext,
        _req: AlterTableRequest,
    ) -> TableResult<TableRef> {
        table_error::UnsupportedSnafu {
            operation: "ALTER TABLE",
        }
        .fail()
    }

    fn get_table(&self, _ctx: &EngineContext, table_id: TableId) -> TableResult<Option<TableRef>> {
        Ok(self.inner.get_table(table_id))
    }

    fn table_exists(&self, _ctx: &EngineContext, table_id: TableId) -> bool {
        self.inner.get_table(table_id).is_some()
    }

    async fn drop_table(
        &self,
        _ctx: &EngineContext,
        request: DropTableRequest,
    ) -> TableResult<bool> {
        self.inner
            .drop_table(request)
            .await
            .map_err(BoxedError::new)
            .context(table_error::TableOperationSnafu)
    }

    async fn close(&self) -> TableResult<()> {
        self.inner.close().await
    }
}

#[async_trait]
impl TableEngineProcedure for ImmutableFileTableEngine {
    fn create_table_procedure(
        &self,
        _ctx: &EngineContext,
        request: CreateTableRequest,
    ) -> TableResult<BoxedProcedure> {
        let procedure = Box::new(CreateImmutableFileTable::new(request, self.clone()));
        Ok(procedure)
    }

    fn alter_table_procedure(
        &self,
        _ctx: &EngineContext,
        _request: AlterTableRequest,
    ) -> TableResult<BoxedProcedure> {
        table_error::UnsupportedSnafu {
            operation: "ALTER TABLE",
        }
        .fail()
    }

    fn drop_table_procedure(
        &self,
        _ctx: &EngineContext,
        request: DropTableRequest,
    ) -> TableResult<BoxedProcedure> {
        let procedure = Box::new(DropImmutableFileTable::new(request, self.clone()));
        Ok(procedure)
    }
}

#[cfg(test)]
impl ImmutableFileTableEngine {
    pub async fn close_table(&self, table_id: TableId) -> TableResult<()> {
        self.inner.close_table(table_id).await
    }
}

impl ImmutableFileTableEngine {
    pub fn new(config: EngineConfig, object_store: ObjectStore) -> Self {
        ImmutableFileTableEngine {
            inner: Arc::new(EngineInner::new(config, object_store)),
        }
    }

    /// Register all procedure loaders to the procedure manager.
    ///
    /// # Panics
    /// Panics on error.
    pub fn register_procedure_loaders(&self, procedure_manager: &dyn ProcedureManager) {
        procedure::register_procedure_loaders(self.clone(), procedure_manager);
    }
}

struct EngineInner {
    /// All tables opened by the engine.
    ///
    /// Writing to `tables` should also hold the `table_mutex`.
    tables: RwLock<HashMap<TableId, ImmutableFileTableRef>>,
    object_store: ObjectStore,

    /// Table mutex is used to protect the operations such as creating/opening/closing
    /// a table, to avoid things like opening the same table simultaneously.
    table_mutex: Mutex<()>,
}

impl EngineInner {
    pub fn new(_config: EngineConfig, object_store: ObjectStore) -> Self {
        EngineInner {
            tables: RwLock::new(HashMap::default()),
            object_store,
            table_mutex: Mutex::new(()),
        }
    }

    async fn create_table(
        &self,
        _ctx: &EngineContext,
        request: CreateTableRequest,
    ) -> Result<TableRef> {
        let CreateTableRequest {
            id: table_id,
            catalog_name,
            schema_name,
            table_name,
            create_if_not_exists,
            table_options,
            ..
        } = request;
        let table_ref = TableReference {
            catalog: &catalog_name,
            schema: &schema_name,
            table: &table_name,
        };

        if let Some(table) = self.get_table(table_id) {
            return if create_if_not_exists {
                Ok(table)
            } else {
                TableExistsSnafu { table_name }.fail()
            };
        }

        let table_schema =
            Arc::new(Schema::try_from(request.schema).context(InvalidRawSchemaSnafu)?);

        let table_dir = table_dir(&catalog_name, &schema_name, table_id);

        let table_full_name = table_ref.to_string();

        let _lock = self.table_mutex.lock().await;
        // Checks again, read lock should be enough since we are guarded by the mutex.
        if let Some(table) = self.get_table(table_id) {
            return if request.create_if_not_exists {
                Ok(table)
            } else {
                // If the procedure retry this method. It is possible to return error
                // when the table is already created.
                // TODO(yingwen): Refactor this like the mito engine.
                TableExistsSnafu { table_name }.fail()
            };
        }

        let table_meta = TableMetaBuilder::new_external_table()
            .schema(table_schema)
            .engine(IMMUTABLE_FILE_ENGINE)
            .options(table_options)
            .build()
            .context(BuildTableMetaSnafu {
                table_name: &table_full_name,
            })?;

        let table_info = TableInfoBuilder::new(&table_name, table_meta)
            .ident(table_id)
            .table_version(INIT_TABLE_VERSION)
            .table_type(TableType::Base)
            .catalog_name(catalog_name.to_string())
            .schema_name(schema_name.to_string())
            .desc(request.desc)
            .build()
            .context(BuildTableInfoSnafu {
                table_name: &table_full_name,
            })?;

        let table = Arc::new(
            ImmutableFileTable::create(
                &table_full_name,
                &table_dir,
                table_info,
                self.object_store.clone(),
            )
            .await?,
        );

        logging::info!(
            "Immutable file engine created table: {} in schema: {}, table_id: {}.",
            table_name,
            schema_name,
            table_id
        );

        let _ = self.tables.write().unwrap().insert(table_id, table.clone());

        Ok(table)
    }

    fn get_table(&self, table_id: TableId) -> Option<TableRef> {
        self.tables
            .read()
            .unwrap()
            .get(&table_id)
            .cloned()
            .map(|table| table as _)
    }

    async fn open_table(
        &self,
        _ctx: &EngineContext,
        request: OpenTableRequest,
    ) -> TableResult<Option<TableRef>> {
        let OpenTableRequest {
            catalog_name,
            schema_name,
            table_name,
            table_id,
            ..
        } = request;
        let table_ref = TableReference {
            catalog: &catalog_name,
            schema: &schema_name,
            table: &table_name,
        };

        if let Some(table) = self.get_table(table_id) {
            return Ok(Some(table));
        }

        let table_full_name = table_ref.to_string();
        let table = {
            let _lock = self.table_mutex.lock().await;
            // Checks again, read lock should be enough since we are guarded by the mutex.
            if let Some(table) = self.get_table(table_id) {
                return Ok(Some(table));
            }

            let table_id = request.table_id;
            let table_dir = table_dir(&catalog_name, &schema_name, table_id);

            let (metadata, table_info) = self
                .recover_table_manifest_and_info(&table_full_name, &table_dir)
                .await
                .map_err(BoxedError::new)
                .context(TableOperationSnafu)?;

            debug!(
                "Opening table {}, table info recovered: {:?}",
                table_id, table_info
            );

            let table = Arc::new(
                ImmutableFileTable::new(table_info, metadata)
                    .map_err(BoxedError::new)
                    .context(table_error::TableOperationSnafu)?,
            );

            let _ = self.tables.write().unwrap().insert(table_id, table.clone());
            Some(table as _)
        };

        logging::info!(
            "Immutable file engine opened table: {} in schema: {}",
            table_name,
            schema_name
        );

        Ok(table)
    }

    async fn drop_table(&self, req: DropTableRequest) -> Result<bool> {
        let table_ref = TableReference {
            catalog: &req.catalog_name,
            schema: &req.schema_name,
            table: &req.table_name,
        };

        let table_full_name = table_ref.to_string();
        let _lock = self.table_mutex.lock().await;
        if let Some(table) = self.get_table(req.table_id) {
            let table_id = table.table_info().ident.table_id;
            let table_dir = table_dir(&req.catalog_name, &req.schema_name, table_id);

            delete_table_manifest(
                &table_full_name,
                &table_manifest_dir(&table_dir),
                &self.object_store,
            )
            .await
            .map_err(BoxedError::new)
            .context(DropTableSnafu {
                table_name: &table_full_name,
            })?;
            let _ = self.tables.write().unwrap().remove(&req.table_id);

            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn close(&self) -> TableResult<()> {
        let _lock = self.table_mutex.lock().await;

        let tables = self.tables.read().unwrap().clone();

        let _ = futures::future::try_join_all(tables.values().map(|t| t.close(&[])))
            .await
            .map_err(BoxedError::new)
            .context(table_error::TableOperationSnafu)?;

        // Releases all closed table
        self.tables.write().unwrap().clear();

        Ok(())
    }

    async fn recover_table_manifest_and_info(
        &self,
        table_name: &str,
        table_dir: &str,
    ) -> Result<(ImmutableMetadata, TableInfo)> {
        ImmutableFileTable::recover_table_info(
            table_name,
            &table_manifest_dir(table_dir),
            &self.object_store,
        )
        .await
    }
}

#[cfg(test)]
impl EngineInner {
    pub async fn close_table(&self, table_id: TableId) -> TableResult<()> {
        let _lock = self.table_mutex.lock().await;

        if let Some(table) = self.get_table(table_id) {
            let regions = Vec::new();
            table
                .close(&regions)
                .await
                .map_err(BoxedError::new)
                .context(table_error::TableOperationSnafu)?;
        }

        let _ = self.tables.write().unwrap().remove(&table_id);

        Ok(())
    }
}
