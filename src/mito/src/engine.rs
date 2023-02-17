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

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_procedure::BoxedProcedure;
use common_telemetry::tracing::log::info;
use common_telemetry::{debug, logging};
use datatypes::schema::Schema;
use object_store::ObjectStore;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::{
    ColumnDescriptorBuilder, ColumnFamilyDescriptor, ColumnFamilyDescriptorBuilder, ColumnId,
    CreateOptions, EngineContext as StorageEngineContext, OpenOptions, Region,
    RegionDescriptorBuilder, RegionId, RowKeyDescriptor, RowKeyDescriptorBuilder, StorageEngine,
};
use table::engine::{EngineContext, TableEngine, TableReference};
use table::error::TableOperationSnafu;
use table::metadata::{
    TableId, TableInfo, TableInfoBuilder, TableMetaBuilder, TableType, TableVersion,
};
use table::requests::{
    AlterKind, AlterTableRequest, CreateTableRequest, DropTableRequest, OpenTableRequest,
};
use table::table::{AlterContext, TableRef};
use table::{error as table_error, Result as TableResult, Table};
use tokio::sync::Mutex;

use crate::config::EngineConfig;
use crate::engine::procedure::CreateMitoTable;
use crate::error::{
    self, BuildColumnDescriptorSnafu, BuildColumnFamilyDescriptorSnafu, BuildRegionDescriptorSnafu,
    BuildRowKeyDescriptorSnafu, InvalidPrimaryKeySnafu, InvalidRawSchemaSnafu,
    MissingTimestampIndexSnafu, RegionNotFoundSnafu, Result, TableExistsSnafu,
};
use crate::manifest::TableManifest;
use crate::table::MitoTable;

pub const MITO_ENGINE: &str = "mito";
pub const INIT_COLUMN_ID: ColumnId = 0;
const INIT_TABLE_VERSION: TableVersion = 0;

/// Generate region name in the form of "{TABLE_ID}_{REGION_NUMBER}"
#[inline]
fn region_name(table_id: TableId, n: u32) -> String {
    format!("{table_id}_{n:010}")
}

#[inline]
fn region_id(table_id: TableId, n: u32) -> RegionId {
    (u64::from(table_id) << 32) | u64::from(n)
}

#[inline]
fn table_dir(catalog_name: &str, schema_name: &str, table_id: TableId) -> String {
    format!("{catalog_name}/{schema_name}/{table_id}/")
}

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
        self.inner
            .create_table(ctx, request)
            .await
            .map_err(BoxedError::new)
            .context(table_error::TableOperationSnafu)
    }

    async fn create_table_procedure(
        &self,
        _ctx: &EngineContext,
        request: CreateTableRequest,
    ) -> TableResult<BoxedProcedure> {
        validate_create_table_request(&request)
            .map_err(BoxedError::new)
            .context(table_error::TableOperationSnafu)?;

        let procedure = Box::new(CreateMitoTable::new(request, self.inner.clone()));
        Ok(procedure)
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
        ctx: &EngineContext,
        req: AlterTableRequest,
    ) -> TableResult<TableRef> {
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
}

pub(crate) struct MitoEngineInner<S: StorageEngine> {
    /// All tables opened by the engine. Map key is formatted [TableReference].
    ///
    /// Writing to `tables` should also hold the `table_mutex`.
    tables: RwLock<HashMap<String, TableRef>>,
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
                    table_name: format!("{catalog_name}.{schema_name}.{table_name}"),
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
                .default_cf(default_cf.clone())
                .build()
                .context(BuildRegionDescriptorSnafu {
                    table_name,
                    region_name,
                })?;
            let opts = CreateOptions {
                parent_dir: table_dir.clone(),
            };

            let region = self
                .storage_engine
                .create_region(&StorageEngineContext::default(), region_descriptor, &opts)
                .await
                .map_err(BoxedError::new)
                .context(error::CreateRegionSnafu)?;
            info!("Mito engine created region: {:?}", region.id());
            regions.insert(*region_number, region);
        }

        let table_meta = TableMetaBuilder::default()
            .schema(table_schema)
            .engine(MITO_ENGINE)
            .next_column_id(next_column_id)
            .primary_key_indices(request.primary_key_indices.clone())
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

        logging::info!("Mito engine created table: {:?}.", table.table_info());

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
            let opts = OpenOptions {
                parent_dir: table_dir.to_string(),
            };

            let Some((manifest, table_info)) = self
                .recover_table_manifest_and_info(table_name, &table_dir)
                .await.map_err(BoxedError::new)
                .context(TableOperationSnafu)? else { return Ok(None) };

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

        logging::info!("Mito engine opened table {}", table_name);

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
            .get_table(&table_ref)
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

#[cfg(test)]
mod tests {
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_query::physical_plan::SessionContext;
    use common_recordbatch::util;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema, RawSchema};
    use datatypes::value::Value;
    use datatypes::vectors::{
        Float64Vector, Int32Vector, StringVector, TimestampMillisecondVector, VectorRef,
    };
    use log_store::NoopLogStore;
    use storage::compaction::noop::NoopCompactionScheduler;
    use storage::config::EngineConfig as StorageEngineConfig;
    use storage::region::RegionImpl;
    use storage::EngineImpl;
    use store_api::manifest::Manifest;
    use store_api::storage::ReadContext;
    use table::requests::{AddColumnRequest, AlterKind, DeleteRequest};
    use tempdir::TempDir;

    use super::*;
    use crate::table::test_util;
    use crate::table::test_util::{
        new_insert_request, schema_for_test, TestEngineComponents, TABLE_NAME,
    };

    async fn setup_table_with_column_default_constraint() -> (TempDir, String, TableRef) {
        let table_name = "test_default_constraint";
        let column_schemas = vec![
            ColumnSchema::new("name", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("n", ConcreteDataType::int32_datatype(), true)
                .with_default_constraint(Some(ColumnDefaultConstraint::Value(Value::from(42i32))))
                .unwrap(),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_datatype(common_time::timestamp::TimeUnit::Millisecond),
                true,
            )
            .with_time_index(true),
        ];

        let schema = RawSchema::new(column_schemas);

        let (dir, object_store) =
            test_util::new_test_object_store("test_insert_with_column_default_constraint").await;
        let compaction_scheduler = Arc::new(NoopCompactionScheduler::default());
        let table_engine = MitoEngine::new(
            EngineConfig::default(),
            EngineImpl::new(
                StorageEngineConfig::default(),
                Arc::new(NoopLogStore::default()),
                object_store.clone(),
                compaction_scheduler,
            ),
            object_store,
        );

        let table = table_engine
            .create_table(
                &EngineContext::default(),
                CreateTableRequest {
                    id: 1,
                    catalog_name: "greptime".to_string(),
                    schema_name: "public".to_string(),
                    table_name: table_name.to_string(),
                    desc: Some("a test table".to_string()),
                    schema,
                    create_if_not_exists: true,
                    primary_key_indices: Vec::default(),
                    table_options: HashMap::new(),
                    region_numbers: vec![0],
                },
            )
            .await
            .unwrap();

        (dir, table_name.to_string(), table)
    }

    #[tokio::test]
    async fn test_column_default_constraint() {
        let (_dir, table_name, table) = setup_table_with_column_default_constraint().await;

        let mut columns_values: HashMap<String, VectorRef> = HashMap::with_capacity(4);
        let names: VectorRef = Arc::new(StringVector::from(vec!["first", "second"]));
        let tss: VectorRef = Arc::new(TimestampMillisecondVector::from_vec(vec![1, 2]));

        columns_values.insert("name".to_string(), names.clone());
        columns_values.insert("ts".to_string(), tss.clone());

        let insert_req = new_insert_request(table_name.to_string(), columns_values);
        assert_eq!(2, table.insert(insert_req).await.unwrap());

        let session_ctx = SessionContext::new();
        let stream = table.scan(None, &[], None).await.unwrap();
        let stream = stream.execute(0, session_ctx.task_ctx()).unwrap();
        let batches = util::collect(stream).await.unwrap();
        assert_eq!(1, batches.len());

        let record = &batches[0];
        assert_eq!(record.num_columns(), 3);
        assert_eq!(names, *record.column(0));
        assert_eq!(
            Arc::new(Int32Vector::from_vec(vec![42, 42])) as VectorRef,
            *record.column(1)
        );
        assert_eq!(tss, *record.column(2));
    }

    #[tokio::test]
    async fn test_insert_with_column_default_constraint() {
        let (_dir, table_name, table) = setup_table_with_column_default_constraint().await;

        let mut columns_values: HashMap<String, VectorRef> = HashMap::with_capacity(4);
        let names: VectorRef = Arc::new(StringVector::from(vec!["first", "second"]));
        let nums: VectorRef = Arc::new(Int32Vector::from(vec![None, Some(66)]));
        let tss: VectorRef = Arc::new(TimestampMillisecondVector::from_vec(vec![1, 2]));

        columns_values.insert("name".to_string(), names.clone());
        columns_values.insert("n".to_string(), nums.clone());
        columns_values.insert("ts".to_string(), tss.clone());

        let insert_req = new_insert_request(table_name.to_string(), columns_values);
        assert_eq!(2, table.insert(insert_req).await.unwrap());

        let session_ctx = SessionContext::new();
        let stream = table.scan(None, &[], None).await.unwrap();
        let stream = stream.execute(0, session_ctx.task_ctx()).unwrap();
        let batches = util::collect(stream).await.unwrap();
        assert_eq!(1, batches.len());

        let record = &batches[0];
        assert_eq!(record.num_columns(), 3);
        assert_eq!(names, *record.column(0));
        assert_eq!(nums, *record.column(1));
        assert_eq!(tss, *record.column(2));
    }

    #[test]
    fn test_region_name() {
        assert_eq!("1_0000000000", region_name(1, 0));
        assert_eq!("1_0000000001", region_name(1, 1));
        assert_eq!("99_0000000100", region_name(99, 100));
        assert_eq!("1000_0000009999", region_name(1000, 9999));
    }

    #[test]
    fn test_table_dir() {
        assert_eq!(
            "greptime/public/1024/",
            table_dir("greptime", "public", 1024)
        );
        assert_eq!(
            "0x4354a1/prometheus/1024/",
            table_dir("0x4354a1", "prometheus", 1024)
        );
    }

    #[test]
    fn test_validate_create_table_request() {
        let table_name = "test_validate_create_table_request";
        let column_schemas = vec![
            ColumnSchema::new("name", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_datatype(common_time::timestamp::TimeUnit::Millisecond),
                true,
            )
            .with_time_index(true),
        ];

        let schema = RawSchema::new(column_schemas);

        let mut request = CreateTableRequest {
            id: 1,
            catalog_name: "greptime".to_string(),
            schema_name: "public".to_string(),
            table_name: table_name.to_string(),
            desc: Some("a test table".to_string()),
            schema,
            create_if_not_exists: true,
            // put ts into primary keys
            primary_key_indices: vec![0, 1],
            table_options: HashMap::new(),
            region_numbers: vec![0],
        };

        let err = validate_create_table_request(&request).unwrap_err();
        assert!(err
            .to_string()
            .contains("Invalid primary key: time index column can't be included in primary key"));

        request.primary_key_indices = vec![0];
        assert!(validate_create_table_request(&request).is_ok());
    }

    #[tokio::test]
    async fn test_create_table_insert_scan() {
        let TestEngineComponents {
            table_ref: table,
            schema_ref,
            dir: _dir,
            ..
        } = test_util::setup_test_engine_and_table().await;
        assert_eq!(TableType::Base, table.table_type());
        assert_eq!(schema_ref, table.schema());

        let insert_req = new_insert_request("demo".to_string(), HashMap::default());
        assert_eq!(0, table.insert(insert_req).await.unwrap());

        let mut columns_values: HashMap<String, VectorRef> = HashMap::with_capacity(4);
        let hosts: VectorRef = Arc::new(StringVector::from(vec!["host1", "host2"]));
        let cpus: VectorRef = Arc::new(Float64Vector::from_vec(vec![55.5, 66.6]));
        let memories: VectorRef = Arc::new(Float64Vector::from_vec(vec![1024f64, 4096f64]));
        let tss: VectorRef = Arc::new(TimestampMillisecondVector::from_vec(vec![1, 2]));

        columns_values.insert("host".to_string(), hosts.clone());
        columns_values.insert("cpu".to_string(), cpus.clone());
        columns_values.insert("memory".to_string(), memories.clone());
        columns_values.insert("ts".to_string(), tss.clone());

        let insert_req = new_insert_request("demo".to_string(), columns_values);
        assert_eq!(2, table.insert(insert_req).await.unwrap());

        let session_ctx = SessionContext::new();
        let stream = table.scan(None, &[], None).await.unwrap();
        let stream = stream.execute(0, session_ctx.task_ctx()).unwrap();
        let batches = util::collect(stream).await.unwrap();
        assert_eq!(1, batches.len());
        assert_eq!(batches[0].num_columns(), 4);

        let batch_schema = &batches[0].schema;
        assert_eq!(batch_schema.num_columns(), 4);
        assert_eq!(batch_schema.column_schemas()[0].name, "host");
        assert_eq!(batch_schema.column_schemas()[1].name, "cpu");
        assert_eq!(batch_schema.column_schemas()[2].name, "memory");
        assert_eq!(batch_schema.column_schemas()[3].name, "ts");

        let batch = &batches[0];
        assert_eq!(4, batch.num_columns());
        assert_eq!(hosts, *batch.column(0));
        assert_eq!(cpus, *batch.column(1));
        assert_eq!(memories, *batch.column(2));
        assert_eq!(tss, *batch.column(3));

        // Scan with projections: cpu and memory
        let stream = table.scan(Some(&vec![1, 2]), &[], None).await.unwrap();
        let stream = stream.execute(0, session_ctx.task_ctx()).unwrap();
        let batches = util::collect(stream).await.unwrap();
        assert_eq!(1, batches.len());
        assert_eq!(batches[0].num_columns(), 2);

        let batch_schema = &batches[0].schema;
        assert_eq!(batch_schema.num_columns(), 2);

        assert_eq!(batch_schema.column_schemas()[0].name, "cpu");
        assert_eq!(batch_schema.column_schemas()[1].name, "memory");

        let batch = &batches[0];
        assert_eq!(2, batch.num_columns());
        assert_eq!(cpus, *batch.column(0));
        assert_eq!(memories, *batch.column(1));

        // Scan with projections: only ts
        let stream = table.scan(Some(&vec![3]), &[], None).await.unwrap();
        let stream = stream.execute(0, session_ctx.task_ctx()).unwrap();
        let batches = util::collect(stream).await.unwrap();
        assert_eq!(1, batches.len());
        assert_eq!(batches[0].num_columns(), 1);

        let batch_schema = &batches[0].schema;
        assert_eq!(batch_schema.num_columns(), 1);

        assert_eq!(batch_schema.column_schemas()[0].name, "ts");

        let record = &batches[0];
        assert_eq!(1, record.num_columns());
        assert_eq!(tss, *record.column(0));
    }

    #[tokio::test]
    async fn test_create_table_scan_batches() {
        common_telemetry::init_default_ut_logging();

        let TestEngineComponents {
            table_ref: table,
            dir: _dir,
            ..
        } = test_util::setup_test_engine_and_table().await;

        // TODO(yingwen): Custom batch size once the table support setting batch_size.
        let default_batch_size = ReadContext::default().batch_size;
        // Insert more than batch size rows to the table.
        let test_batch_size = default_batch_size * 4;
        let mut columns_values: HashMap<String, VectorRef> = HashMap::with_capacity(4);
        let hosts: VectorRef = Arc::new(StringVector::from(vec!["host1"; test_batch_size]));
        let cpus: VectorRef = Arc::new(Float64Vector::from_vec(vec![55.5; test_batch_size]));
        let memories: VectorRef = Arc::new(Float64Vector::from_vec(vec![1024f64; test_batch_size]));
        let tss: VectorRef = Arc::new(TimestampMillisecondVector::from_values(
            (0..test_batch_size).map(|v| v as i64),
        ));

        columns_values.insert("host".to_string(), hosts);
        columns_values.insert("cpu".to_string(), cpus);
        columns_values.insert("memory".to_string(), memories);
        columns_values.insert("ts".to_string(), tss.clone());

        let insert_req = new_insert_request("demo".to_string(), columns_values);
        assert_eq!(test_batch_size, table.insert(insert_req).await.unwrap());

        let session_ctx = SessionContext::new();
        let stream = table.scan(None, &[], None).await.unwrap();
        let stream = stream.execute(0, session_ctx.task_ctx()).unwrap();
        let batches = util::collect(stream).await.unwrap();
        let mut total = 0;
        for batch in batches {
            assert_eq!(batch.num_columns(), 4);
            let ts = batch.column(3);
            let expect = tss.slice(total, ts.len());
            assert_eq!(expect, *ts);
            total += ts.len();
        }
        assert_eq!(test_batch_size, total);
    }

    #[tokio::test]
    async fn test_create_if_not_exists() {
        common_telemetry::init_default_ut_logging();
        let ctx = EngineContext::default();

        let (_engine, table_engine, table, _object_store, _dir) =
            test_util::setup_mock_engine_and_table().await;

        let table_info = table.table_info();

        let request = CreateTableRequest {
            id: 1,
            catalog_name: "greptime".to_string(),
            schema_name: "public".to_string(),
            table_name: table_info.name.to_string(),
            schema: RawSchema::from(&*table_info.meta.schema),
            create_if_not_exists: true,
            desc: None,
            primary_key_indices: Vec::default(),
            table_options: HashMap::new(),
            region_numbers: vec![0],
        };

        let created_table = table_engine.create_table(&ctx, request).await.unwrap();
        assert_eq!(table_info, created_table.table_info());

        // test create_if_not_exists=false
        let request = CreateTableRequest {
            id: 1,
            catalog_name: "greptime".to_string(),
            schema_name: "public".to_string(),
            table_name: table_info.name.to_string(),
            schema: RawSchema::from(&*table_info.meta.schema),
            create_if_not_exists: false,
            desc: None,
            primary_key_indices: Vec::default(),
            table_options: HashMap::new(),
            region_numbers: vec![0],
        };

        let result = table_engine.create_table(&ctx, request).await;

        assert!(result.is_err());
        assert!(matches!(result, Err(e) if format!("{e:?}").contains("Table already exists")));
    }

    #[tokio::test]
    async fn test_open_table() {
        common_telemetry::init_default_ut_logging();

        let ctx = EngineContext::default();
        let open_req = OpenTableRequest {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: test_util::TABLE_NAME.to_string(),
            // the test table id is 1
            table_id: 1,
        };

        let (_engine, storage_engine, table, object_store, _dir) = {
            let TestEngineComponents {
                table_engine,
                storage_engine,
                table_ref: table,
                object_store,
                dir,
                ..
            } = test_util::setup_test_engine_and_table().await;

            assert_eq!(MITO_ENGINE, table_engine.name());
            // Now try to open the table again.
            let reopened = table_engine
                .open_table(&ctx, open_req.clone())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(table.schema(), reopened.schema());

            (table_engine, storage_engine, table, object_store, dir)
        };

        // Construct a new table engine, and try to open the table.
        let table_engine = MitoEngine::new(EngineConfig::default(), storage_engine, object_store);
        let reopened = table_engine
            .open_table(&ctx, open_req.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(table.schema(), reopened.schema());

        let reopened = reopened
            .as_any()
            .downcast_ref::<MitoTable<RegionImpl<NoopLogStore>>>()
            .unwrap();

        let left = table.table_info();
        // assert recovered table_info is correct
        let right = reopened.table_info();
        assert_eq!(left, right);
        assert_eq!(reopened.manifest().last_version(), 1);
    }

    #[test]
    fn test_region_id() {
        assert_eq!(1, region_id(0, 1));
        assert_eq!(4294967296, region_id(1, 0));
        assert_eq!(4294967297, region_id(1, 1));
        assert_eq!(4294967396, region_id(1, 100));
        assert_eq!(8589934602, region_id(2, 10));
        assert_eq!(18446744069414584330, region_id(u32::MAX, 10));
    }

    fn new_add_columns_req(new_tag: &ColumnSchema, new_field: &ColumnSchema) -> AlterTableRequest {
        AlterTableRequest {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: TABLE_NAME.to_string(),
            alter_kind: AlterKind::AddColumns {
                columns: vec![
                    AddColumnRequest {
                        column_schema: new_tag.clone(),
                        is_key: true,
                    },
                    AddColumnRequest {
                        column_schema: new_field.clone(),
                        is_key: false,
                    },
                ],
            },
        }
    }

    #[tokio::test]
    async fn test_alter_table_add_column() {
        let (_engine, table_engine, table, _object_store, _dir) =
            test_util::setup_mock_engine_and_table().await;

        let old_info = table.table_info();
        let old_meta = &old_info.meta;
        let old_schema = &old_meta.schema;

        let new_tag = ColumnSchema::new("my_tag", ConcreteDataType::string_datatype(), true);
        let new_field = ColumnSchema::new("my_field", ConcreteDataType::string_datatype(), true);
        let req = new_add_columns_req(&new_tag, &new_field);
        let table = table_engine
            .alter_table(&EngineContext::default(), req)
            .await
            .unwrap();

        let new_info = table.table_info();
        let new_meta = &new_info.meta;
        let new_schema = &new_meta.schema;

        assert_eq!(&[0, 4], &new_meta.primary_key_indices[..]);
        assert_eq!(&[1, 2, 3, 5], &new_meta.value_indices[..]);
        assert_eq!(new_schema.num_columns(), old_schema.num_columns() + 2);
        assert_eq!(
            &new_schema.column_schemas()[..old_schema.num_columns()],
            old_schema.column_schemas()
        );
        assert_eq!(
            &new_schema.column_schemas()[old_schema.num_columns()],
            &new_tag
        );
        assert_eq!(
            &new_schema.column_schemas()[old_schema.num_columns() + 1],
            &new_field
        );
        assert_eq!(new_schema.timestamp_column(), old_schema.timestamp_column());
        assert_eq!(new_schema.version(), old_schema.version() + 1);
        assert_eq!(new_meta.next_column_id, old_meta.next_column_id + 2);
    }

    #[tokio::test]
    async fn test_alter_table_remove_column() {
        let (_engine, table_engine, _table, _object_store, _dir) =
            test_util::setup_mock_engine_and_table().await;

        // Add two columns to the table first.
        let new_tag = ColumnSchema::new("my_tag", ConcreteDataType::string_datatype(), true);
        let new_field = ColumnSchema::new("my_field", ConcreteDataType::string_datatype(), true);
        let req = new_add_columns_req(&new_tag, &new_field);
        let table = table_engine
            .alter_table(&EngineContext::default(), req)
            .await
            .unwrap();

        let old_info = table.table_info();
        let old_meta = &old_info.meta;
        let old_schema = &old_meta.schema;

        // Then remove memory and my_field from the table.
        let req = AlterTableRequest {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: TABLE_NAME.to_string(),
            alter_kind: AlterKind::DropColumns {
                names: vec![String::from("memory"), String::from("my_field")],
            },
        };
        let table = table_engine
            .alter_table(&EngineContext::default(), req)
            .await
            .unwrap();

        let new_info = table.table_info();
        let new_meta = &new_info.meta;
        let new_schema = &new_meta.schema;

        assert_eq!(new_schema.num_columns(), old_schema.num_columns() - 2);
        let remaining_names: Vec<String> = new_schema
            .column_schemas()
            .iter()
            .map(|column_schema| column_schema.name.clone())
            .collect();
        assert_eq!(&["host", "cpu", "ts", "my_tag"], &remaining_names[..]);
        assert_eq!(&[0, 3], &new_meta.primary_key_indices[..]);
        assert_eq!(&[1, 2], &new_meta.value_indices[..]);
        assert_eq!(new_schema.timestamp_column(), old_schema.timestamp_column());
        assert_eq!(new_schema.version(), old_schema.version() + 1);
    }

    #[tokio::test]
    async fn test_alter_rename_table() {
        let TestEngineComponents {
            table_engine,
            storage_engine,
            object_store,
            dir: _dir,
            ..
        } = test_util::setup_test_engine_and_table().await;
        let ctx = EngineContext::default();

        // register another table
        let another_name = "another_table";
        let req = CreateTableRequest {
            id: 1024,
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: another_name.to_string(),
            desc: Some("another test table".to_string()),
            schema: RawSchema::from(&schema_for_test()),
            region_numbers: vec![0],
            primary_key_indices: vec![0],
            create_if_not_exists: true,
            table_options: HashMap::new(),
        };
        table_engine
            .create_table(&ctx, req)
            .await
            .expect("create table must succeed");
        // test renaming a table with an existing name.
        let req = AlterTableRequest {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: TABLE_NAME.to_string(),
            alter_kind: AlterKind::RenameTable {
                new_table_name: another_name.to_string(),
            },
        };
        let err = table_engine.alter_table(&ctx, req).await.err().unwrap();
        assert!(
            err.to_string().contains("Table already exists"),
            "Unexpected error: {err}"
        );

        let new_table_name = "test_table";
        // test rename table
        let req = AlterTableRequest {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: TABLE_NAME.to_string(),
            alter_kind: AlterKind::RenameTable {
                new_table_name: new_table_name.to_string(),
            },
        };
        let table = table_engine.alter_table(&ctx, req).await.unwrap();

        assert_eq!(table.table_info().name, new_table_name);

        let table_engine = MitoEngine::new(EngineConfig::default(), storage_engine, object_store);
        let open_req = OpenTableRequest {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: new_table_name.to_string(),
            table_id: 1,
        };

        // test reopen table
        let reopened = table_engine
            .open_table(&ctx, open_req.clone())
            .await
            .unwrap()
            .unwrap();
        let reopened = reopened
            .as_any()
            .downcast_ref::<MitoTable<RegionImpl<NoopLogStore>>>()
            .unwrap();
        assert_eq!(reopened.table_info(), table.table_info());
        assert_eq!(reopened.table_info().name, new_table_name);
        assert_eq!(reopened.manifest().last_version(), 2);
    }

    #[tokio::test]
    async fn test_drop_table() {
        common_telemetry::init_default_ut_logging();
        let ctx = EngineContext::default();

        let (_engine, table_engine, table, _object_store, _dir) =
            test_util::setup_mock_engine_and_table().await;
        let engine_ctx = EngineContext {};

        let table_info = table.table_info();
        let table_reference = TableReference {
            catalog: DEFAULT_CATALOG_NAME,
            schema: DEFAULT_SCHEMA_NAME,
            table: &table_info.name,
        };

        let create_table_request = CreateTableRequest {
            id: 1,
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: table_info.name.to_string(),
            schema: RawSchema::from(&*table_info.meta.schema),
            create_if_not_exists: true,
            desc: None,
            primary_key_indices: Vec::default(),
            table_options: HashMap::new(),
            region_numbers: vec![0],
        };

        let created_table = table_engine
            .create_table(&ctx, create_table_request)
            .await
            .unwrap();
        assert_eq!(table_info, created_table.table_info());
        assert!(table_engine.table_exists(&engine_ctx, &table_reference));

        let drop_table_request = DropTableRequest {
            catalog_name: table_reference.catalog.to_string(),
            schema_name: table_reference.schema.to_string(),
            table_name: table_reference.table.to_string(),
        };
        let table_dropped = table_engine
            .drop_table(&engine_ctx, drop_table_request)
            .await
            .unwrap();
        assert!(table_dropped);
        assert!(!table_engine.table_exists(&engine_ctx, &table_reference));

        // should be able to re-create
        let request = CreateTableRequest {
            id: 2,
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: table_info.name.to_string(),
            schema: RawSchema::from(&*table_info.meta.schema),
            create_if_not_exists: false,
            desc: None,
            primary_key_indices: Vec::default(),
            table_options: HashMap::new(),
            region_numbers: vec![0],
        };
        table_engine.create_table(&ctx, request).await.unwrap();
        assert!(table_engine.table_exists(&engine_ctx, &table_reference));
    }

    #[tokio::test]
    async fn test_table_delete_rows() {
        let TestEngineComponents {
            table_ref: table,
            dir: _dir,
            ..
        } = test_util::setup_test_engine_and_table().await;

        let mut columns_values: HashMap<String, VectorRef> = HashMap::with_capacity(4);
        let hosts: VectorRef =
            Arc::new(StringVector::from(vec!["host1", "host2", "host3", "host4"]));
        let cpus: VectorRef = Arc::new(Float64Vector::from_vec(vec![1.0, 2.0, 3.0, 4.0]));
        let memories: VectorRef = Arc::new(Float64Vector::from_vec(vec![1.0, 2.0, 3.0, 4.0]));
        let tss: VectorRef = Arc::new(TimestampMillisecondVector::from_vec(vec![1, 2, 2, 1]));

        columns_values.insert("host".to_string(), hosts.clone());
        columns_values.insert("cpu".to_string(), cpus.clone());
        columns_values.insert("memory".to_string(), memories.clone());
        columns_values.insert("ts".to_string(), tss.clone());

        let insert_req = new_insert_request("demo".to_string(), columns_values);
        assert_eq!(4, table.insert(insert_req).await.unwrap());

        let del_hosts: VectorRef = Arc::new(StringVector::from(vec!["host1", "host3"]));
        let del_tss: VectorRef = Arc::new(TimestampMillisecondVector::from_vec(vec![1, 2]));
        let mut key_column_values = HashMap::with_capacity(2);
        key_column_values.insert("host".to_string(), del_hosts);
        key_column_values.insert("ts".to_string(), del_tss);
        let del_req = DeleteRequest { key_column_values };
        table.delete(del_req).await.unwrap();

        let session_ctx = SessionContext::new();
        let stream = table.scan(None, &[], None).await.unwrap();
        let stream = stream.execute(0, session_ctx.task_ctx()).unwrap();
        let batches = util::collect_batches(stream).await.unwrap();

        assert_eq!(
            batches.pretty_print().unwrap(),
            "\
+-------+-----+--------+-------------------------+
| host  | cpu | memory | ts                      |
+-------+-----+--------+-------------------------+
| host2 | 2   | 2      | 1970-01-01T00:00:00.002 |
| host4 | 4   | 4      | 1970-01-01T00:00:00.001 |
+-------+-----+--------+-------------------------+"
        );
    }
}
