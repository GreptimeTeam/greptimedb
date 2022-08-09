use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::RwLock;

use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_telemetry::logging;
use snafu::{OptionExt, ResultExt};
use store_api::storage::{
    self, ColumnDescriptorBuilder, ColumnFamilyDescriptor, ColumnFamilyDescriptorBuilder, ColumnId,
    OpenOptions, Region, RegionDescriptorBuilder, RegionMeta, RowKeyDescriptor,
    RowKeyDescriptorBuilder, StorageEngine,
};
use table::engine::{EngineContext, TableEngine};
use table::requests::{AlterTableRequest, CreateTableRequest, DropTableRequest, OpenTableRequest};
use table::Result as TableResult;
use table::{
    metadata::{TableId, TableInfoBuilder, TableMetaBuilder, TableType},
    table::TableRef,
};
use tokio::sync::Mutex;

use crate::error::{
    self, BuildColumnDescriptorSnafu, BuildColumnFamilyDescriptorSnafu, BuildRegionDescriptorSnafu,
    BuildRowKeyDescriptorSnafu, MissingTimestampIndexSnafu, Result,
};
use crate::table::MitoTable;

pub const DEFAULT_ENGINE: &str = "mito";
const INIT_COLUMN_ID: ColumnId = 0;

/// [TableEngine] implementation.
///
/// About mito <https://en.wikipedia.org/wiki/Alfa_Romeo_MiTo>.
/// "you can't be a true petrolhead until you've owned an Alfa Romeo" -- by Jeremy Clarkson
#[derive(Clone)]
pub struct MitoEngine<S: StorageEngine> {
    inner: Arc<MitoEngineInner<S>>,
}

impl<S: StorageEngine> MitoEngine<S> {
    pub fn new(storage_engine: S) -> Self {
        Self {
            inner: Arc::new(MitoEngineInner::new(storage_engine)),
        }
    }
}

#[async_trait]
impl<S: StorageEngine> TableEngine for MitoEngine<S> {
    fn name(&self) -> &str {
        DEFAULT_ENGINE
    }

    async fn create_table(
        &self,
        ctx: &EngineContext,
        request: CreateTableRequest,
    ) -> TableResult<TableRef> {
        Ok(self.inner.create_table(ctx, request).await?)
    }

    async fn open_table(
        &self,
        ctx: &EngineContext,
        request: OpenTableRequest,
    ) -> TableResult<Option<TableRef>> {
        Ok(self.inner.open_table(ctx, request).await?)
    }

    async fn alter_table(
        &self,
        _ctx: &EngineContext,
        _request: AlterTableRequest,
    ) -> TableResult<TableRef> {
        unimplemented!();
    }

    fn get_table(&self, _ctx: &EngineContext, name: &str) -> TableResult<Option<TableRef>> {
        Ok(self.inner.get_table(name))
    }

    fn table_exists(&self, _ctx: &EngineContext, _name: &str) -> bool {
        unimplemented!();
    }

    async fn drop_table(
        &self,
        _ctx: &EngineContext,
        _request: DropTableRequest,
    ) -> TableResult<()> {
        unimplemented!();
    }
}

/// FIXME(dennis) impl system catalog to keep table metadata.
struct MitoEngineInner<S: StorageEngine> {
    /// All tables opened by the engine.
    ///
    /// Writing to `tables` should also hold the `table_mutex`.
    tables: RwLock<HashMap<String, TableRef>>,
    storage_engine: S,
    // FIXME(yingwen): Remove `next_table_id`. Table id should be assigned by other module (maybe catalog).
    next_table_id: AtomicU64,
    /// Table mutex is used to protect the operations such as creating/opening/closing
    /// a table, to avoid things like opening the same table simultaneously.
    table_mutex: Mutex<()>,
}

impl<S: StorageEngine> MitoEngineInner<S> {
    fn new(storage_engine: S) -> Self {
        Self {
            tables: RwLock::new(HashMap::default()),
            storage_engine,
            next_table_id: AtomicU64::new(0),
            table_mutex: Mutex::new(()),
        }
    }

    fn next_table_id(&self) -> TableId {
        self.next_table_id.fetch_add(1, Ordering::Relaxed)
    }
}

fn build_row_key_desc_from_schema(
    mut column_id: ColumnId,
    request: &CreateTableRequest,
) -> Result<(ColumnId, RowKeyDescriptor)> {
    let ts_column_schema =
        request
            .schema
            .timestamp_column()
            .context(MissingTimestampIndexSnafu {
                table_name: &request.name,
            })?;
    let timestamp_index = request.schema.timestamp_index().unwrap();

    let ts_column = ColumnDescriptorBuilder::new(
        column_id,
        ts_column_schema.name.clone(),
        ts_column_schema.data_type.clone(),
    )
    .is_nullable(ts_column_schema.is_nullable)
    .build()
    .context(BuildColumnDescriptorSnafu {
        column_name: &ts_column_schema.name,
        table_name: &request.name,
    })?;
    column_id += 1;

    let column_schemas = &request.schema.column_schemas();

    //TODO(boyan): enable version column by table option?
    let mut builder = RowKeyDescriptorBuilder::new(ts_column);

    for index in &request.primary_key_indices {
        if *index == timestamp_index {
            continue;
        }

        let column_schema = &column_schemas[*index];

        let column = ColumnDescriptorBuilder::new(
            column_id,
            column_schema.name.clone(),
            column_schema.data_type.clone(),
        )
        .is_nullable(column_schema.is_nullable)
        .build()
        .context(BuildColumnDescriptorSnafu {
            column_name: &column_schema.name,
            table_name: &request.name,
        })?;

        builder = builder.push_column(column);
        column_id += 1;
    }

    Ok((
        column_id,
        builder.build().context(BuildRowKeyDescriptorSnafu {
            table_name: &request.name,
        })?,
    ))
}

fn build_column_family_from_request(
    mut column_id: ColumnId,
    request: &CreateTableRequest,
) -> Result<(ColumnId, ColumnFamilyDescriptor)> {
    let mut builder = ColumnFamilyDescriptorBuilder::default();

    let primary_key_indices = &request.primary_key_indices;
    let ts_index = request
        .schema
        .timestamp_index()
        .context(MissingTimestampIndexSnafu {
            table_name: &request.name,
        })?;
    let column_schemas = request
        .schema
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
        .is_nullable(column_schema.is_nullable)
        .build()
        .context(BuildColumnDescriptorSnafu {
            column_name: &column_schema.name,
            table_name: &request.name,
        })?;

        builder = builder.push_column(column);
        column_id += 1;
    }

    Ok((
        column_id,
        builder.build().context(BuildColumnFamilyDescriptorSnafu {
            table_name: &request.name,
        })?,
    ))
}

impl<S: StorageEngine> MitoEngineInner<S> {
    async fn create_table(
        &self,
        _ctx: &EngineContext,
        request: CreateTableRequest,
    ) -> Result<TableRef> {
        let table_name = &request.name;

        if let Some(table) = self.get_table(table_name) {
            return Ok(table);
        }

        let (next_column_id, default_cf) =
            build_column_family_from_request(INIT_COLUMN_ID, &request)?;
        let (next_column_id, row_key) = build_row_key_desc_from_schema(next_column_id, &request)?;

        // Now we just use table name as region name. TODO(yingwen): Naming pattern of region.
        let region_name = table_name.clone();
        let region_descriptor = RegionDescriptorBuilder::default()
            .id(0)
            .name(&region_name)
            .row_key(row_key)
            .default_cf(default_cf)
            .build()
            .context(BuildRegionDescriptorSnafu {
                table_name,
                region_name,
            })?;

        let _lock = self.table_mutex.lock().await;
        // Checks again, read lock should be enough since we are guarded by the mutex.
        if let Some(table) = self.get_table(table_name) {
            return Ok(table);
        }

        let region = self
            .storage_engine
            .create_region(&storage::EngineContext::default(), region_descriptor)
            .await
            .map_err(BoxedError::new)
            .context(error::CreateRegionSnafu)?;

        // Use region meta schema instead of request schema
        let table_meta = TableMetaBuilder::default()
            .schema(region.in_memory_metadata().schema().clone())
            .engine(DEFAULT_ENGINE)
            .next_column_id(next_column_id)
            .primary_key_indices(request.primary_key_indices.clone())
            .build()
            .context(error::BuildTableMetaSnafu { table_name })?;

        let table_info = TableInfoBuilder::new(table_name.clone(), table_meta)
            .ident(self.next_table_id())
            .table_version(0u64)
            .table_type(TableType::Base)
            .desc(request.desc)
            .build()
            .context(error::BuildTableInfoSnafu { table_name })?;

        //TODO(dennis): persist table info to table manifest service.
        logging::info!("Mito engine created table: {:?}.", table_info);

        let table = Arc::new(MitoTable::new(table_info, region));

        self.tables
            .write()
            .unwrap()
            .insert(table_name.clone(), table.clone());

        Ok(table)
    }

    // TODO(yingwen): Support catalog and schema name.
    async fn open_table(
        &self,
        _ctx: &EngineContext,
        request: OpenTableRequest,
    ) -> TableResult<Option<TableRef>> {
        let table_name = &request.table_name;
        if let Some(table) = self.get_table(table_name) {
            // Table has already been opened.
            return Ok(Some(table));
        }

        // Acquires the mutex before opening a new table.
        let table = {
            let _lock = self.table_mutex.lock().await;
            // Checks again, read lock should be enough since we are guarded by the mutex.
            if let Some(table) = self.get_table(table_name) {
                return Ok(Some(table));
            }

            let engine_ctx = storage::EngineContext::default();
            let opts = OpenOptions::default();
            let region_name = table_name;
            // Now we just use table name as region name. TODO(yingwen): Naming pattern of region.

            let region = match self
                .storage_engine
                .open_region(&engine_ctx, region_name, &opts)
                .await
                .map_err(BoxedError::new)
                .context(error::OpenRegionSnafu { region_name })?
            {
                None => return Ok(None),
                Some(region) => region,
            };

            //FIXME(boyan): recover table meta from table manifest
            let table_meta = TableMetaBuilder::default()
                .schema(region.in_memory_metadata().schema().clone())
                .engine(DEFAULT_ENGINE)
                .next_column_id(INIT_COLUMN_ID)
                .primary_key_indices(Vec::default())
                .build()
                .context(error::BuildTableMetaSnafu { table_name })?;

            let table_info = TableInfoBuilder::new(table_name.clone(), table_meta)
                .ident(request.table_id)
                .table_version(0u64)
                .table_type(TableType::Base)
                .build()
                .context(error::BuildTableInfoSnafu { table_name })?;

            let table = Arc::new(MitoTable::new(table_info, region));

            self.tables
                .write()
                .unwrap()
                .insert(table_name.to_string(), table.clone());
            Some(table as _)
        };

        logging::info!("Mito engine opened table {}", table_name);

        Ok(table)
    }

    fn get_table(&self, name: &str) -> Option<TableRef> {
        self.tables.read().unwrap().get(name).cloned()
    }
}

#[cfg(test)]
mod tests {
    use common_recordbatch::util;
    use datafusion_common::field_util::FieldExt;
    use datafusion_common::field_util::SchemaExt;
    use datatypes::vectors::*;
    use table::requests::InsertRequest;

    use super::*;
    use crate::table::test_util;

    #[tokio::test]
    async fn test_create_table_insert_scan() {
        let (_engine, table, schema, _dir) = test_util::setup_test_engine_and_table().await;

        assert_eq!(TableType::Base, table.table_type());
        assert_eq!(schema, table.schema());

        let insert_req = InsertRequest {
            table_name: "demo".to_string(),
            columns_values: HashMap::default(),
        };
        assert_eq!(0, table.insert(insert_req).await.unwrap());

        let mut columns_values: HashMap<String, VectorRef> = HashMap::with_capacity(4);
        let hosts = StringVector::from(vec!["host1", "host2"]);
        let cpus = Float64Vector::from_vec(vec![55.5, 66.6]);
        let memories = Float64Vector::from_vec(vec![1024f64, 4096f64]);
        let tss = Int64Vector::from_vec(vec![1, 2]);

        columns_values.insert("host".to_string(), Arc::new(hosts.clone()));
        columns_values.insert("cpu".to_string(), Arc::new(cpus.clone()));
        columns_values.insert("memory".to_string(), Arc::new(memories.clone()));
        columns_values.insert("ts".to_string(), Arc::new(tss.clone()));

        let insert_req = InsertRequest {
            table_name: "demo".to_string(),
            columns_values,
        };
        assert_eq!(2, table.insert(insert_req).await.unwrap());

        let stream = table.scan(&None, &[], None).await.unwrap();
        let batches = util::collect(stream).await.unwrap();
        assert_eq!(1, batches.len());
        assert_eq!(batches[0].df_recordbatch.num_columns(), 4);

        let arrow_schema = batches[0].schema.arrow_schema();
        assert_eq!(arrow_schema.fields().len(), 4);
        assert_eq!(arrow_schema.field(0).name(), "ts");
        assert_eq!(arrow_schema.field(1).name(), "host");
        assert_eq!(arrow_schema.field(2).name(), "cpu");
        assert_eq!(arrow_schema.field(3).name(), "memory");

        let columns = batches[0].df_recordbatch.columns();
        assert_eq!(4, columns.len());
        assert_eq!(tss.to_arrow_array(), columns[0]);
        assert_eq!(hosts.to_arrow_array(), columns[1]);
        assert_eq!(cpus.to_arrow_array(), columns[2]);
        assert_eq!(memories.to_arrow_array(), columns[3]);
    }

    #[tokio::test]
    async fn test_open_table() {
        common_telemetry::init_default_ut_logging();

        let ctx = EngineContext::default();
        let open_req = OpenTableRequest {
            catalog_name: String::new(),
            schema_name: String::new(),
            table_name: test_util::TABLE_NAME.to_string(),
            // Currently the first table has id 0.
            table_id: 0,
        };

        let (engine, table) = {
            let (engine, table_engine, table) = test_util::setup_mock_engine_and_table().await;
            // Now try to open the table again.
            let reopened = table_engine
                .open_table(&ctx, open_req.clone())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(table.schema(), reopened.schema());

            (engine, table)
        };

        // Construct a new table engine, and try to open the table.
        let table_engine = MitoEngine::new(engine);
        let reopened = table_engine
            .open_table(&ctx, open_req.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(table.schema(), reopened.schema());
    }
}
