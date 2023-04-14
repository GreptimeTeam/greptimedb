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
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
pub use common_catalog::consts::MITO_ENGINE;
use common_catalog::format_full_table_name;
use common_error::ext::BoxedError;
use common_procedure::{BoxedProcedure, ProcedureManager};
use common_telemetry::tracing::log::info;
use common_telemetry::{debug, logging};
use datatypes::schema::Schema;
use object_store::ObjectStore;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::{
    ColumnDescriptorBuilder, ColumnFamilyDescriptor, ColumnFamilyDescriptorBuilder, ColumnId,
    CreateOptions, EngineContext as StorageEngineContext, OpenOptions, Region,
    RegionDescriptorBuilder, RowKeyDescriptor, RowKeyDescriptorBuilder, StorageEngine,
};
use table::engine::{
    region_id, region_name, table_dir, EngineContext, TableEngine, TableEngineProcedure,
    TableReference,
};
use table::error::TableOperationSnafu;
use table::metadata::{TableInfo, TableInfoBuilder, TableMetaBuilder, TableType, TableVersion};
use table::requests::{
    AlterKind, AlterTableRequest, CreateTableRequest, DropTableRequest, OpenTableRequest,
};
use table::table::{AlterContext, TableRef};
use table::{error as table_error, Result as TableResult, Table};
use tokio::sync::Mutex;

use crate::config::EngineConfig;
use crate::engine::procedure::{AlterMitoTable, CreateMitoTable, DropMitoTable};
use crate::error::{
    self, BuildColumnDescriptorSnafu, BuildColumnFamilyDescriptorSnafu, BuildRegionDescriptorSnafu,
    BuildRowKeyDescriptorSnafu, InvalidPrimaryKeySnafu, InvalidRawSchemaSnafu,
    MissingTimestampIndexSnafu, RegionNotFoundSnafu, Result, TableExistsSnafu,
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
        ctx: &EngineContext,
        request: CreateTableRequest,
    ) -> TableResult<TableRef> {
        let _timer = common_telemetry::timer!(metrics::MITO_CREATE_TABLE_ELAPSED);
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
        let _timer = common_telemetry::timer!(metrics::MITO_OPEN_TABLE_ELAPSED);
        self.inner
            .open_table(ctx, request)
            .await
            .map_err(BoxedError::new)
            .context(table_error::TableOperationSnafu)
    }

    async fn alter_table(
        &self,
        ctx: &EngineContext,
        req: AlterTableRequest,
    ) -> TableResult<TableRef> {
        let _timer = common_telemetry::timer!(metrics::MITO_ALTER_TABLE_ELAPSED);
        self.inner
            .alter_table(ctx, req)
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
    tables: RwLock<HashMap<String, Arc<MitoTable<S::Region>>>>,
    object_store: ObjectStore,
    storage_engine: S,
    /// Table mutex is used to protect the operations such as creating/opening/closing
    /// a table, to avoid things like opening the same table simultaneously.
    table_mutex: Mutex<()>,
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
    async fn create_table(
        &self,
        _ctx: &EngineContext,
        request: CreateTableRequest,
    ) -> Result<TableRef> {
        let catalog_name = &request.catalog_name;
        let schema_name = &request.schema_name;
        let table_name = &request.table_name;
        let table_ref = TableReference {
            catalog: catalog_name,
            schema: schema_name,
            table: table_name,
        };

        validate_create_table_request(&request)?;

        if let Some(table) = self.get_table(&table_ref) {
            if request.create_if_not_exists {
                return Ok(table);
            } else {
                return TableExistsSnafu {
                    table_name: format_full_table_name(catalog_name, schema_name, table_name),
                }
                .fail();
            }
        }

        let table_schema =
            Arc::new(Schema::try_from(request.schema).context(InvalidRawSchemaSnafu)?);
        let primary_key_indices = &request.primary_key_indices;
        let (next_column_id, default_cf) = build_column_family(
            INIT_COLUMN_ID,
            table_name,
            &table_schema,
            primary_key_indices,
        )?;
        let (next_column_id, row_key) = build_row_key_desc(
            next_column_id,
            table_name,
            &table_schema,
            primary_key_indices,
        )?;

        let table_id = request.id;
        let table_dir = table_dir(catalog_name, schema_name, table_id);
        let mut regions = HashMap::with_capacity(request.region_numbers.len());

        let _lock = self.table_mutex.lock().await;
        // Checks again, read lock should be enough since we are guarded by the mutex.
        if let Some(table) = self.get_table(&table_ref) {
            return if request.create_if_not_exists {
                Ok(table)
            } else {
                TableExistsSnafu { table_name }.fail()
            };
        }

        for region_number in &request.region_numbers {
            let region_id = region_id(table_id, *region_number);

            let region_name = region_name(table_id, *region_number);
            let region_descriptor = RegionDescriptorBuilder::default()
                .id(region_id)
                .name(&region_name)
                .row_key(row_key.clone())
                .compaction_time_window(request.table_options.compaction_time_window)
                .default_cf(default_cf.clone())
                .build()
                .context(BuildRegionDescriptorSnafu {
                    table_name,
                    region_name,
                })?;
            let opts = CreateOptions {
                parent_dir: table_dir.clone(),
                write_buffer_size: request
                    .table_options
                    .write_buffer_size
                    .map(|size| size.0 as usize),
                ttl: request.table_options.ttl,
                compaction_time_window: request.table_options.compaction_time_window,
            };

            {
                let _timer = common_telemetry::timer!(crate::metrics::MITO_CREATE_REGION_ELAPSED);
                let region = self
                    .storage_engine
                    .create_region(&StorageEngineContext::default(), region_descriptor, &opts)
                    .await
                    .map_err(BoxedError::new)
                    .context(error::CreateRegionSnafu)?;
            }
            info!(
                "Mito engine created region: {}, id: {}",
                region.name(),
                region.id()
            );
            regions.insert(*region_number, region);
        }

        let table_meta = TableMetaBuilder::default()
            .schema(table_schema)
            .engine(MITO_ENGINE)
            .next_column_id(next_column_id)
            .primary_key_indices(request.primary_key_indices.clone())
            .options(request.table_options)
            .region_numbers(request.region_numbers)
            .build()
            .context(error::BuildTableMetaSnafu { table_name })?;

        let table_info = TableInfoBuilder::new(table_name.clone(), table_meta)
            .ident(table_id)
            .table_version(INIT_TABLE_VERSION)
            .table_type(TableType::Base)
            .catalog_name(catalog_name.to_string())
            .schema_name(schema_name.to_string())
            .desc(request.desc)
            .build()
            .context(error::BuildTableInfoSnafu { table_name })?;

        let table = Arc::new(
            MitoTable::create(
                table_name,
                &table_dir,
                table_info,
                regions,
                self.object_store.clone(),
            )
            .await?,
        );

        logging::info!(
            "Mito engine created table: {} in schema: {}, table_id: {}.",
            table_name,
            schema_name,
            table_id
        );

        self.tables
            .write()
            .unwrap()
            .insert(table_ref.to_string(), table.clone());

        Ok(table)
    }

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
            let _lock = self.table_mutex.lock().await;
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
                .context(TableOperationSnafu)? else { return Ok(None) };

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

            self.tables
                .write()
                .unwrap()
                .insert(table_ref.to_string(), table.clone());
            Some(table as _)
        };

        logging::info!(
            "Mito engine opened table: {} in schema: {}",
            table_name,
            schema_name
        );

        Ok(table)
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
            .read()
            .unwrap()
            .get(&table_ref.to_string())
            .cloned()
            .map(|table| table as _)
    }

    /// Returns the [MitoTable].
    fn get_mito_table(&self, table_ref: &TableReference) -> Option<Arc<MitoTable<S::Region>>> {
        self.tables
            .read()
            .unwrap()
            .get(&table_ref.to_string())
            .cloned()
    }

    async fn alter_table(&self, _ctx: &EngineContext, req: AlterTableRequest) -> Result<TableRef> {
        let catalog_name = &req.catalog_name;
        let schema_name = &req.schema_name;
        let table_name = &req.table_name;

        if let AlterKind::RenameTable { new_table_name } = &req.alter_kind {
            let table_ref = TableReference {
                catalog: catalog_name,
                schema: schema_name,
                table: new_table_name,
            };

            if self.get_table(&table_ref).is_some() {
                return TableExistsSnafu {
                    table_name: table_ref.to_string(),
                }
                .fail();
            }
        }

        let mut table_ref = TableReference {
            catalog: catalog_name,
            schema: schema_name,
            table: table_name,
        };
        let table = self
            .get_mito_table(&table_ref)
            .context(error::TableNotFoundSnafu { table_name })?;

        logging::info!("start altering table {} with request {:?}", table_name, req);
        table
            .alter(AlterContext::new(), &req)
            .await
            .context(error::AlterTableSnafu { table_name })?;

        if let AlterKind::RenameTable { new_table_name } = &req.alter_kind {
            let mut tables = self.tables.write().unwrap();
            tables.remove(&table_ref.to_string());
            table_ref.table = new_table_name.as_str();
            tables.insert(table_ref.to_string(), table.clone());
        }
        Ok(table)
    }

    /// Drop table. Returns whether a table is dropped (true) or not exist (false).
    async fn drop_table(&self, req: DropTableRequest) -> Result<bool> {
        let table_reference = TableReference {
            catalog: &req.catalog_name,
            schema: &req.schema_name,
            table: &req.table_name,
        };
        // todo(ruihang): reclaim persisted data
        Ok(self
            .tables
            .write()
            .unwrap()
            .remove(&table_reference.to_string())
            .is_some())
    }

    async fn close(&self) -> TableResult<()> {
        let _lock = self.table_mutex.lock().await;

        let tables = self.tables.write().unwrap().clone();

        futures::future::try_join_all(tables.values().map(|t| t.close()))
            .await
            .map_err(BoxedError::new)
            .context(table_error::TableOperationSnafu)?;

        Ok(())
    }
}

impl<S: StorageEngine> MitoEngineInner<S> {
    fn new(_config: EngineConfig, storage_engine: S, object_store: ObjectStore) -> Self {
        Self {
            tables: RwLock::new(HashMap::default()),
            storage_engine,
            object_store,
            table_mutex: Mutex::new(()),
        }
    }
}
