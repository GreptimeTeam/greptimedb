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
use common_datasource::compression::CompressionType;
use common_error::ext::BoxedError;
use common_procedure::{BoxedProcedure, ProcedureManager};
use common_telemetry::{debug, logging};
use dashmap::DashMap;
use datatypes::schema::Schema;
use key_lock::KeyLock;
use object_store::ObjectStore;
use snafu::{ensure, OptionExt, ResultExt};
use storage::manifest::manifest_compress_type;
use store_api::path_utils::{region_name, table_dir_with_catalog_and_schema};
use store_api::storage::{
    CloseOptions, ColumnDescriptorBuilder, ColumnFamilyDescriptor, ColumnFamilyDescriptorBuilder,
    ColumnId, CompactionStrategy, EngineContext as StorageEngineContext, OpenOptions, RegionNumber,
    RowKeyDescriptor, RowKeyDescriptorBuilder, StorageEngine,
};
use table::engine::{
    CloseTableResult, EngineContext, TableEngine, TableEngineProcedure, TableReference,
};
use table::metadata::{TableId, TableInfo, TableVersion};
use table::requests::{
    AlterTableRequest, CloseTableRequest, CreateTableRequest, DropTableRequest, OpenTableRequest,
    TruncateTableRequest,
};
use table::{error as table_error, Result as TableResult, Table, TableRef};

use self::procedure::TruncateMitoTable;
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

        let _lock = self.inner.table_mutex.lock(request.id).await;
        if let Some(table) = self.inner.get_mito_table(request.id) {
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

        let mut procedure = AlterMitoTable::new(req, self.inner.clone())
            .map_err(BoxedError::new)
            .context(table_error::TableOperationSnafu)?;

        procedure
            .engine_alter_table()
            .await
            .map_err(BoxedError::new)
            .context(table_error::TableOperationSnafu)
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
        self.inner.drop_table(request).await
    }

    async fn close_table(
        &self,
        _ctx: &EngineContext,
        request: CloseTableRequest,
    ) -> TableResult<CloseTableResult> {
        self.inner.close_table(request).await
    }

    async fn close(&self) -> TableResult<()> {
        self.inner.close().await
    }

    async fn truncate_table(
        &self,
        _ctx: &EngineContext,
        request: TruncateTableRequest,
    ) -> TableResult<bool> {
        self.inner.truncate_table(request).await
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

    fn truncate_table_procedure(
        &self,
        _ctx: &EngineContext,
        request: TruncateTableRequest,
    ) -> TableResult<BoxedProcedure> {
        let procedure = Box::new(
            TruncateMitoTable::new(request, self.inner.clone())
                .map_err(BoxedError::new)
                .context(table_error::TableOperationSnafu)?,
        );
        Ok(procedure)
    }
}

pub(crate) struct MitoEngineInner<S: StorageEngine> {
    /// All tables opened by the engine.
    ///
    /// Writing to `tables` should also hold the `table_mutex`.
    tables: DashMap<TableId, Arc<MitoTable<S::Region>>>,
    object_store: ObjectStore,
    compress_type: CompressionType,
    storage_engine: S,
    /// Table mutex is used to protect the operations such as creating/opening/closing
    /// a table, to avoid things like opening the same table simultaneously.
    table_mutex: Arc<KeyLock<TableId>>,
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

    //TODO(dennis): enable version column by table option?
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

fn all_regions_open(table: TableRef, regions: &[RegionNumber]) -> TableResult<bool> {
    for r in regions {
        let region_exist = table.contains_region(*r)?;
        if !region_exist {
            return Ok(false);
        }
    }
    Ok(true)
}

impl<S: StorageEngine> MitoEngineInner<S> {
    /// Returns Some(table) contains all specific regions
    fn check_regions(
        &self,
        table: TableRef,
        regions: &[RegionNumber],
    ) -> TableResult<Option<TableRef>> {
        if all_regions_open(table.clone(), regions)? {
            // If all regions have been opened
            Ok(Some(table))
        } else {
            Ok(None)
        }
    }

    /// Builds table from scratch.
    /// Returns None if failed to recover manifest.
    async fn recover_table(
        &self,
        _ctx: &EngineContext,
        request: OpenTableRequest,
    ) -> TableResult<Option<Arc<MitoTable<S::Region>>>> {
        let catalog_name = &request.catalog_name;
        let schema_name = &request.schema_name;
        let table_name = &request.table_name;

        let table_id = request.table_id;
        let engine_ctx = StorageEngineContext::default();
        let table_dir = table_dir_with_catalog_and_schema(catalog_name, schema_name, table_id);

        let Some((manifest, table_info)) = self
            .recover_table_manifest_and_info(table_name, &table_dir)
            .await
            .map_err(BoxedError::new)
            .context(table_error::TableOperationSnafu)?
        else {
            return Ok(None);
        };

        let compaction_strategy = CompactionStrategy::from(&table_info.meta.options.extra_options);
        let opts = OpenOptions {
            parent_dir: table_dir.to_string(),
            write_buffer_size: table_info
                .meta
                .options
                .write_buffer_size
                .map(|s| s.0 as usize),
            ttl: table_info.meta.options.ttl,
            compaction_strategy,
        };

        debug!(
            "Opening table {}, table info recovered: {:?}",
            table_id, table_info
        );

        // FIXME: We cannot trust the region numbers in the manifest because other datanodes might overwrite the manifest.

        let mut regions = HashMap::with_capacity(table_info.meta.region_numbers.len());

        let table_ref = TableReference {
            catalog: catalog_name,
            schema: schema_name,
            table: table_name,
        };
        for region_number in &request.region_numbers {
            let region = self
                .open_region(&engine_ctx, table_id, *region_number, &table_ref, &opts)
                .await?;
            let _ = regions.insert(*region_number, region);
        }

        let table = Arc::new(MitoTable::new(table_info, regions, manifest));

        Ok(Some(table))
    }

    async fn open_region(
        &self,
        engine_ctx: &StorageEngineContext,
        table_id: TableId,
        region_number: RegionNumber,
        table_ref: &TableReference<'_>,
        opts: &OpenOptions,
    ) -> TableResult<S::Region> {
        let region_name = region_name(table_id, region_number);
        let region = self
            .storage_engine
            .open_region(engine_ctx, &region_name, opts)
            .await
            .map_err(BoxedError::new)
            .context(table_error::TableOperationSnafu)?
            .with_context(|| RegionNotFoundSnafu {
                table: format!(
                    "{}.{}.{}",
                    table_ref.catalog, table_ref.schema, table_ref.table
                ),
                region: region_number,
            })
            .map_err(BoxedError::new)
            .context(table_error::TableOperationSnafu)?;

        Ok(region)
    }

    /// Loads regions
    async fn load_missing_regions(
        &self,
        _ctx: &EngineContext,
        table: Arc<MitoTable<S::Region>>,
        region_numbers: &[RegionNumber],
    ) -> TableResult<()> {
        let table_info = table.table_info();
        let catalog = &table_info.catalog_name;
        let schema = &table_info.schema_name;
        let name = &table_info.name;
        let table_id = table_info.ident.table_id;

        let table_dir = table_dir_with_catalog_and_schema(catalog, schema, table_id);
        let table_ref = TableReference {
            catalog,
            schema,
            table: name,
        };

        let compaction_strategy = CompactionStrategy::from(&table_info.meta.options.extra_options);
        let opts = OpenOptions {
            parent_dir: table_dir.to_string(),
            write_buffer_size: table_info
                .meta
                .options
                .write_buffer_size
                .map(|s| s.0 as usize),
            ttl: table_info.meta.options.ttl,
            compaction_strategy,
        };

        // TODO(weny): Returns an error earlier if the target region does not exist in the meta.
        for region_number in region_numbers {
            if table.contains_region(*region_number)? {
                continue;
            }

            let engine_ctx = StorageEngineContext::default();

            let region = self
                .open_region(&engine_ctx, table_id, *region_number, &table_ref, &opts)
                .await?;

            table.load_region(*region_number, region).await?;
        }

        Ok(())
    }

    async fn open_table(
        &self,
        ctx: &EngineContext,
        request: OpenTableRequest,
    ) -> TableResult<Option<TableRef>> {
        if let Some(table) = self.get_table(request.table_id) {
            if let Some(table) = self.check_regions(table, &request.region_numbers)? {
                return Ok(Some(table));
            }
        }

        // Acquires the mutex before opening a new table.
        let table = {
            let _lock = self.table_mutex.lock(request.table_id).await;

            // Checks again, read lock should be enough since we are guarded by the mutex.
            if let Some(table) = self.get_mito_table(request.table_id) {
                // Contains all regions or target region
                if let Some(table) = self.check_regions(table.clone(), &request.region_numbers)? {
                    Some(table)
                } else {
                    // Loads missing regions
                    self.load_missing_regions(ctx, table.clone(), &request.region_numbers)
                        .await?;

                    Some(table as _)
                }
            } else {
                // Builds table from scratch
                let table = self.recover_table(ctx, request.clone()).await?;
                if let Some(table) = table {
                    // already locked
                    let _ = self.tables.insert(request.table_id, table.clone());

                    Some(table as _)
                } else {
                    None
                }
            }
        };

        logging::info!(
            "Mito engine opened table: {} in schema: {}",
            request.table_name,
            request.schema_name
        );

        Ok(table)
    }

    async fn drop_table(&self, request: DropTableRequest) -> TableResult<bool> {
        // Remove the table from the engine to avoid further access from users.
        let _lock = self.table_mutex.lock(request.table_id).await;
        let removed_table = self.tables.remove(&request.table_id);

        // Close the table to close all regions. Closing a region is idempotent.
        if let Some((_, table)) = &removed_table {
            let mut regions = table.remove_regions(&table.region_ids()).await?;

            let ctx = StorageEngineContext::default();

            let _ = futures::future::try_join_all(
                regions
                    .drain()
                    .map(|(_, region)| self.storage_engine.drop_region(&ctx, region)),
            )
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
            self.compress_type,
        );
        let Some(table_info) =
            MitoTable::<<S as StorageEngine>::Region>::recover_table_info(table_name, &manifest)
                .await?
        else {
            return Ok(None);
        };

        Ok(Some((manifest, table_info)))
    }

    fn get_table(&self, table_id: TableId) -> Option<TableRef> {
        self.tables.get(&table_id).map(|en| en.value().clone() as _)
    }

    /// Returns the [MitoTable].
    fn get_mito_table(&self, table_id: TableId) -> Option<Arc<MitoTable<S::Region>>> {
        self.tables.get(&table_id).map(|en| en.value().clone())
    }

    async fn close(&self) -> TableResult<()> {
        let _ = futures::future::try_join_all(
            self.tables
                .iter()
                .map(|item| self.close_table_inner(item.value().clone(), None, false)),
        )
        .await
        .map_err(BoxedError::new)
        .context(table_error::TableOperationSnafu)?;

        self.storage_engine
            .close(&StorageEngineContext::default())
            .await
            .map_err(BoxedError::new)
            .context(table_error::TableOperationSnafu)?;

        Ok(())
    }

    async fn close_table(&self, request: CloseTableRequest) -> TableResult<CloseTableResult> {
        if let Some(table) = self.get_mito_table(request.table_id) {
            return self
                .close_table_inner(table, Some(&request.region_numbers), request.flush)
                .await;
        }
        // table doesn't exist
        Ok(CloseTableResult::NotFound)
    }

    async fn close_table_inner(
        &self,
        table: Arc<MitoTable<S::Region>>,
        regions: Option<&[RegionNumber]>,
        flush: bool,
    ) -> TableResult<CloseTableResult> {
        let info = table.table_info();
        let table_id = info.ident.table_id;
        let _lock = self.table_mutex.lock(table_id).await;

        let all_regions = table.region_ids();
        let regions = regions.unwrap_or(&all_regions);
        let removed = table.remove_regions(regions).await?;
        let removed_regions = removed.keys().cloned().collect::<Vec<_>>();
        let ctx = StorageEngineContext::default();

        let opts = CloseOptions { flush };
        // Releases regions in storage engine
        for region_number in regions {
            self.storage_engine
                .close_region(&ctx, &region_name(table_id, *region_number), &opts)
                .await
                .map_err(BoxedError::new)
                .context(table_error::TableOperationSnafu)?;
        }

        if table.is_releasable() {
            let _ = self.tables.remove(&table_id);

            logging::info!(
                "Mito engine closed table: {} in schema: {}",
                info.name,
                info.schema_name,
            );
            return Ok(CloseTableResult::Released(removed_regions));
        }

        // Partial closed
        Ok(CloseTableResult::PartialClosed(removed_regions))
    }

    async fn truncate_table(&self, request: TruncateTableRequest) -> TableResult<bool> {
        let _lock = self.table_mutex.lock(request.table_id).await;

        let table_id = request.table_id;
        if let Some(table) = self.get_mito_table(table_id) {
            table
                .truncate()
                .await
                .map_err(BoxedError::new)
                .context(table_error::TableOperationSnafu)?;

            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl<S: StorageEngine> MitoEngineInner<S> {
    fn new(config: EngineConfig, storage_engine: S, object_store: ObjectStore) -> Self {
        Self {
            tables: DashMap::new(),
            storage_engine,
            object_store,
            compress_type: manifest_compress_type(config.compress_manifest),
            table_mutex: Arc::new(KeyLock::new()),
        }
    }
}
