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

mod procedure;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
pub use common_catalog::consts::MITO_ENGINE;
use common_error::ext::BoxedError;
use common_procedure::{BoxedProcedure, ProcedureManager};
use common_telemetry::{debug, logging};
use dashmap::DashMap;
use datatypes::schema::Schema;
use key_lock::KeyLock;
use object_store::ObjectStore;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::{
    ColumnDescriptorBuilder, ColumnFamilyDescriptor, ColumnFamilyDescriptorBuilder, ColumnId,
    EngineContext as StorageEngineContext, OpenOptions, RowKeyDescriptor, RowKeyDescriptorBuilder,
    StorageEngine,
};
use table::engine::{
    region_name, table_dir, EngineContext, TableEngine, TableEngineProcedure, TableReference,
};
use table::metadata::{TableInfo, TableVersion};
use table::requests::{
    AlterKind, AlterTableRequest, CreateTableRequest, DropTableRequest, OpenTableRequest,
};
use table::{error as table_error, Result as TableResult, Table, TableRef};

use crate::config::EngineConfig;
use crate::engine::procedure::{AlterMitoTable, CreateMitoTable, DropMitoTable, TableCreator};
use crate::error::{
    BuildColumnDescriptorSnafu, BuildColumnFamilyDescriptorSnafu, BuildRowKeyDescriptorSnafu,
    InvalidPrimaryKeySnafu, MissingTimestampIndexSnafu, RegionNotFoundSnafu, Result,
    TableExistsSnafu,
};
use crate::manifest::TableManifest;
use crate::metrics;
use crate::table::MitoTable;
pub const INIT_COLUMN_ID: ColumnId = 0;
const INIT_TABLE_VERSION: TableVersion = 0;

/// [TableEngine] implementation.
///
/// About mito <https://en.wikipedia.org/wiki/Alfa_Romeo_MiTo>.
/// "You can't be a true petrolhead until you've owned an Alfa Romeo." -- by Jeremy Clarkson
#[derive(Clone)]
pub struct MitoEngine<S: StorageEngine> {
    inner: Arc<MitoEngineInner<S>>,
}

impl<S: StorageEngine> MitoEngine<S> {
    pub fn new(config: EngineConfig, storage_engine: S, object_store: ObjectStore) -> Self {
        Self {
            inner: Arc::new(MitoEngineInner::new(config, storage_engine, object_store)),
        }
    }

    /// Register all procedure loaders to the procedure manager.
    ///
    /// # Panics
    /// Panics on error.
    pub fn register_procedure_loaders(&self, procedure_manager: &dyn ProcedureManager) {
        procedure::register_procedure_loaders(self.inner.clone(), procedure_manager);
    }
}

#[async_trait]
impl<S: StorageEngine> TableEngine for MitoEngine<S> {
    fn name(&self) -> &str {
        MITO_ENGINE
    }

    async fn create_table(
        &self,
        _ctx: &EngineContext,
        request: CreateTableRequest,
    ) -> TableResult<TableRef> {
        let _timer = common_telemetry::timer!(metrics::MITO_CREATE_TABLE_ELAPSED);

        validate_create_table_request(&request)
            .map_err(BoxedError::new)
            .context(table_error::TableOperationSnafu)?;

        let table_ref = request.table_ref();
        let _lock = self.inner.table_mutex.lock(table_ref.to_string()).await;
        if let Some(table) = self.inner.get_mito_table(&table_ref) {
            if request.create_if_not_exists {
                return Ok(table);
            } else {
                return TableExistsSnafu {
                    table_name: request.table_name,
                }
                .fail()
                .map_err(BoxedError::new)
                .context(table_error::TableOperationSnafu)?;
            }
        }

        let mut creator = TableCreator::new(request, self.inner.clone())
            .map_err(BoxedError::new)
            .context(table_error::TableOperationSnafu)?;

        creator
            .create_table()
            .await
            .map_err(BoxedError::new)
            .context(table_error::TableOperationSnafu)
    }

    async fn open_table(
        &self,
        ctx: &EngineContext,
        request: OpenTableRequest,
    ) -> TableResult<Option<TableRef>> {
        let _timer = common_telemetry::timer!(metrics::MITO_OPEN_TABLE_ELAPSED);
        self.inner
            .open_table(ctx, request)
            .await
            .map_err(BoxedError::new)
            .context(table_error::TableOperationSnafu)
    }

    async fn alter_table(
        &self,
        _ctx: &EngineContext,
        req: AlterTableRequest,
    ) -> TableResult<TableRef> {
        let _timer = common_telemetry::timer!(metrics::MITO_ALTER_TABLE_ELAPSED);

        if let AlterKind::RenameTable { new_table_name } = &req.alter_kind {
            let mut table_ref = req.table_ref();
            table_ref.table = new_table_name;
            if self.inner.get_mito_table(&table_ref).is_some() {
                return TableExistsSnafu {
                    table_name: table_ref.to_string(),
                }
                .fail()
                .map_err(BoxedError::new)
                .context(table_error::TableOperationSnafu)?;
            }
        }

        let mut procedure = AlterMitoTable::new(req, self.inner.clone())
            .map_err(BoxedError::new)
            .context(table_error::TableOperationSnafu)?;

        // TODO(yingwen): Rename has concurrent issue without the procedure runtime. But
        // users can't use this method to alter a table so it is still safe. We should
        // refactor the table engine to avoid using table name as key.
        procedure
            .engine_alter_table()
            .await
            .map_err(BoxedError::new)
            .context(table_error::TableOperationSnafu)
    }

    fn get_table(
        &self,
        _ctx: &EngineContext,
        table_ref: &TableReference,
    ) -> TableResult<Option<TableRef>> {
        Ok(self.inner.get_table(table_ref))
    }

    fn table_exists(&self, _ctx: &EngineContext, table_ref: &TableReference) -> bool {
        self.inner.get_table(table_ref).is_some()
    }

    async fn drop_table(
        &self,
        _ctx: &EngineContext,
        request: DropTableRequest,
    ) -> TableResult<bool> {
        self.inner.drop_table(request).await
    }

    async fn close(&self) -> TableResult<()> {
        self.inner.close().await
    }
}

impl<S: StorageEngine> TableEngineProcedure for MitoEngine<S> {
    fn create_table_procedure(
        &self,
        _ctx: &EngineContext,
        request: CreateTableRequest,
    ) -> TableResult<BoxedProcedure> {
        validate_create_table_request(&request)
            .map_err(BoxedError::new)
            .context(table_error::TableOperationSnafu)?;

        let procedure = Box::new(
            CreateMitoTable::new(request, self.inner.clone())
                .map_err(BoxedError::new)
                .context(table_error::TableOperationSnafu)?,
        );
        Ok(procedure)
    }

    fn alter_table_procedure(
        &self,
        _ctx: &EngineContext,
        request: AlterTableRequest,
    ) -> TableResult<BoxedProcedure> {
        let procedure = Box::new(
            AlterMitoTable::new(request, self.inner.clone())
                .map_err(BoxedError::new)
                .context(table_error::TableOperationSnafu)?,
        );
        Ok(procedure)
    }

    fn drop_table_procedure(
        &self,
        _ctx: &EngineContext,
        request: DropTableRequest,
    ) -> TableResult<BoxedProcedure> {
        let procedure = Box::new(
            DropMitoTable::new(request, self.inner.clone())
                .map_err(BoxedError::new)
                .context(table_error::TableOperationSnafu)?,
        );
        Ok(procedure)
    }
}

pub(crate) struct MitoEngineInner<S: StorageEngine> {
    /// All tables opened by the engine. Map key is formatted [TableReference].
    ///
    /// Writing to `tables` should also hold the `table_mutex`.
    tables: DashMap<String, Arc<MitoTable<S::Region>>>,
    object_store: ObjectStore,
    storage_engine: S,
    /// Table mutex is used to protect the operations such as creating/opening/closing
    /// a table, to avoid things like opening the same table simultaneously.
    table_mutex: Arc<KeyLock<String>>,
}

fn build_row_key_desc(
    mut column_id: ColumnId,
    table_name: &str,
    table_schema: &Schema,
    primary_key_indices: &Vec<usize>,
) -> Result<(ColumnId, RowKeyDescriptor)> {
    let ts_column_schema = table_schema
        .timestamp_column()
        .context(MissingTimestampIndexSnafu { table_name })?;
    // `unwrap` is safe because we've checked the `timestamp_column` above
    let timestamp_index = table_schema.timestamp_index().unwrap();

    let ts_column = ColumnDescriptorBuilder::new(
        column_id,
        ts_column_schema.name.clone(),
        ts_column_schema.data_type.clone(),
    )
    .default_constraint(ts_column_schema.default_constraint().cloned())
    .is_nullable(ts_column_schema.is_nullable())
    .is_time_index(true)
    .build()
    .context(BuildColumnDescriptorSnafu {
        column_name: &ts_column_schema.name,
        table_name,
    })?;
    column_id += 1;

    let column_schemas = &table_schema.column_schemas();

    //TODO(boyan): enable version column by table option?
    let mut builder = RowKeyDescriptorBuilder::new(ts_column);

    for index in primary_key_indices {
        if *index == timestamp_index {
            continue;
        }

        let column_schema = &column_schemas[*index];

        let column = ColumnDescriptorBuilder::new(
            column_id,
            column_schema.name.clone(),
            column_schema.data_type.clone(),
        )
        .default_constraint(column_schema.default_constraint().cloned())
        .is_nullable(column_schema.is_nullable())
        .build()
        .context(BuildColumnDescriptorSnafu {
            column_name: &column_schema.name,
            table_name,
        })?;

        builder = builder.push_column(column);
        column_id += 1;
    }

    Ok((
        column_id,
        builder
            .build()
            .context(BuildRowKeyDescriptorSnafu { table_name })?,
    ))
}

fn build_column_family(
    mut column_id: ColumnId,
    table_name: &str,
    table_schema: &Schema,
    primary_key_indices: &[usize],
) -> Result<(ColumnId, ColumnFamilyDescriptor)> {
    let mut builder = ColumnFamilyDescriptorBuilder::default();

    let ts_index = table_schema
        .timestamp_index()
        .context(MissingTimestampIndexSnafu { table_name })?;
    let column_schemas = table_schema
        .column_schemas()
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != ts_index && !primary_key_indices.contains(index));

    for (_, column_schema) in column_schemas {
        let column = ColumnDescriptorBuilder::new(
            column_id,
            column_schema.name.clone(),
            column_schema.data_type.clone(),
        )
        .default_constraint(column_schema.default_constraint().cloned())
        .is_nullable(column_schema.is_nullable())
        .build()
        .context(BuildColumnDescriptorSnafu {
            column_name: &column_schema.name,
            table_name,
        })?;

        builder = builder.push_column(column);
        column_id += 1;
    }

    Ok((
        column_id,
        builder
            .build()
            .context(BuildColumnFamilyDescriptorSnafu { table_name })?,
    ))
}

fn validate_create_table_request(request: &CreateTableRequest) -> Result<()> {
    let ts_index = request
        .schema
        .timestamp_index
        .context(MissingTimestampIndexSnafu {
            table_name: &request.table_name,
        })?;

    ensure!(
        !request
            .primary_key_indices
            .iter()
            .any(|index| *index == ts_index),
        InvalidPrimaryKeySnafu {
            msg: "time index column can't be included in primary key"
        }
    );

    Ok(())
}

impl<S: StorageEngine> MitoEngineInner<S> {
    async fn open_table(
        &self,
        _ctx: &EngineContext,
        request: OpenTableRequest,
    ) -> TableResult<Option<TableRef>> {
        let catalog_name = &request.catalog_name;
        let schema_name = &request.schema_name;
        let table_name = &request.table_name;
        let table_ref = TableReference {
            catalog: catalog_name,
            schema: schema_name,
            table: table_name,
        };

        if let Some(table) = self.get_table(&table_ref) {
            // Table has already been opened.
            return Ok(Some(table));
        }

        // Acquires the mutex before opening a new table.
        let table = {
            let table_name_key = table_ref.to_string();
            let _lock = self.table_mutex.lock(table_name_key.clone()).await;

            // Checks again, read lock should be enough since we are guarded by the mutex.
            if let Some(table) = self.get_table(&table_ref) {
                return Ok(Some(table));
            }

            let table_id = request.table_id;
            let engine_ctx = StorageEngineContext::default();
            let table_dir = table_dir(catalog_name, schema_name, table_id);

            let Some((manifest, table_info)) = self
                .recover_table_manifest_and_info(table_name, &table_dir)
                .await.map_err(BoxedError::new)
                .context(table_error::TableOperationSnafu)? else { return Ok(None) };

            let opts = OpenOptions {
                parent_dir: table_dir.to_string(),
                write_buffer_size: table_info
                    .meta
                    .options
                    .write_buffer_size
                    .map(|s| s.0 as usize),
                ttl: table_info.meta.options.ttl,
                compaction_time_window: table_info.meta.options.compaction_time_window,
            };

            debug!(
                "Opening table {}, table info recovered: {:?}",
                table_id, table_info
            );

            let mut regions = HashMap::with_capacity(table_info.meta.region_numbers.len());
            for region_number in &table_info.meta.region_numbers {
                let region_name = region_name(table_id, *region_number);
                let region = self
                    .storage_engine
                    .open_region(&engine_ctx, &region_name, &opts)
                    .await
                    .map_err(BoxedError::new)
                    .context(table_error::TableOperationSnafu)?
                    .with_context(|| RegionNotFoundSnafu {
                        table: format!(
                            "{}.{}.{}",
                            request.catalog_name, request.schema_name, request.table_name
                        ),
                        region: *region_number,
                    })
                    .map_err(BoxedError::new)
                    .context(table_error::TableOperationSnafu)?;
                regions.insert(*region_number, region);
            }

            let table = Arc::new(MitoTable::new(table_info, regions, manifest));

            // already locked
            self.tables.insert(table_ref.to_string(), table.clone());
            Some(table as _)
        };

        logging::info!(
            "Mito engine opened table: {} in schema: {}",
            table_name,
            schema_name
        );

        Ok(table)
    }

    async fn drop_table(&self, request: DropTableRequest) -> TableResult<bool> {
        // Remove the table from the engine to avoid further access from users.
        let table_ref = request.table_ref();

        let _lock = self.table_mutex.lock(table_ref.to_string()).await;
        let removed_table = self.tables.remove(&table_ref.to_string());

        // Close the table to close all regions. Closing a region is idempotent.
        if let Some((_, table)) = &removed_table {
            table
                .close()
                .await
                .map_err(BoxedError::new)
                .context(table_error::TableOperationSnafu)?;

            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn recover_table_manifest_and_info(
        &self,
        table_name: &str,
        table_dir: &str,
    ) -> Result<Option<(TableManifest, TableInfo)>> {
        let manifest = MitoTable::<<S as StorageEngine>::Region>::build_manifest(
            table_dir,
            self.object_store.clone(),
        );
        let  Some(table_info) =
            MitoTable::<<S as StorageEngine>::Region>::recover_table_info(table_name, &manifest)
                .await? else { return Ok(None) };

        Ok(Some((manifest, table_info)))
    }

    fn get_table(&self, table_ref: &TableReference) -> Option<TableRef> {
        self.tables
            .get(&table_ref.to_string())
            .map(|en| en.value().clone() as _)
    }

    /// Returns the [MitoTable].
    fn get_mito_table(&self, table_ref: &TableReference) -> Option<Arc<MitoTable<S::Region>>> {
        self.tables
            .get(&table_ref.to_string())
            .map(|en| en.value().clone())
    }

    async fn close(&self) -> TableResult<()> {
        futures::future::try_join_all(
            self.tables
                .iter()
                .map(|item| close_table(self.table_mutex.clone(), item.value().clone())),
        )
        .await
        .map_err(BoxedError::new)
        .context(table_error::TableOperationSnafu)?;

        Ok(())
    }
}

async fn close_table(lock: Arc<KeyLock<String>>, table: TableRef) -> TableResult<()> {
    let info = table.table_info();
    let table_ref = TableReference {
        catalog: &info.catalog_name,
        schema: &info.schema_name,
        table: &info.name,
    };
    let _lock = lock.lock(table_ref.to_string()).await;
    table.close().await
}

impl<S: StorageEngine> MitoEngineInner<S> {
    fn new(_config: EngineConfig, storage_engine: S, object_store: ObjectStore) -> Self {
        Self {
            tables: DashMap::new(),
            storage_engine,
            object_store,
            table_mutex: Arc::new(KeyLock::new()),
        }
    }
}
