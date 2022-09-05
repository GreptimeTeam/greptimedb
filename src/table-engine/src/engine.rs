use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_telemetry::logging;
use datatypes::schema::{ColumnSchema, SchemaBuilder, SchemaRef};
use object_store::ObjectStore;
use snafu::{OptionExt, ResultExt};
use store_api::storage::{
    ColumnDescriptorBuilder, ColumnFamilyDescriptor, ColumnFamilyDescriptorBuilder, ColumnId,
    CreateOptions, EngineContext as StorageEngineContext, OpenOptions, Region,
    RegionDescriptorBuilder, RegionId, RowKeyDescriptor, RowKeyDescriptorBuilder, StorageEngine,
};
use table::engine::{EngineContext, TableEngine};
use table::metadata::{TableInfo, TableMeta};
use table::requests::{
    AlterKind, AlterTableRequest, CreateTableRequest, DropTableRequest, OpenTableRequest,
};
use table::Result as TableResult;
use table::{
    metadata::{TableId, TableInfoBuilder, TableMetaBuilder, TableType, TableVersion},
    table::TableRef,
};
use tokio::sync::Mutex;

use crate::config::EngineConfig;
use crate::error::{
    self, BuildColumnDescriptorSnafu, BuildColumnFamilyDescriptorSnafu, BuildRegionDescriptorSnafu,
    BuildRowKeyDescriptorSnafu, MissingTimestampIndexSnafu, Result, TableExistsSnafu,
};
use crate::table::MitoTable;

pub const MITO_ENGINE: &str = "mito";
const INIT_COLUMN_ID: ColumnId = 0;
const INIT_TABLE_VERSION: TableVersion = 0;

/// Generate region name in the form of "{TABLE_ID}_{REGION_NUMBER}"
#[inline]
fn region_name(table_id: TableId, n: u32) -> String {
    format!("{}_{:010}", table_id, n)
}

#[inline]
fn region_id(table_id: TableId, n: u32) -> RegionId {
    (u64::from(table_id) << 32) | u64::from(n)
}

#[inline]
fn table_dir(table_name: &str) -> String {
    format!("{}/", table_name)
}

/// [TableEngine] implementation.
///
/// About mito <https://en.wikipedia.org/wiki/Alfa_Romeo_MiTo>.
/// "you can't be a true petrolhead until you've owned an Alfa Romeo" -- by Jeremy Clarkson
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
        ctx: &EngineContext,
        req: AlterTableRequest,
    ) -> TableResult<TableRef> {
        Ok(self.inner.alter_table(ctx, req).await?)
    }

    fn get_table(&self, _ctx: &EngineContext, name: &str) -> TableResult<Option<TableRef>> {
        Ok(self.inner.get_table(name))
    }

    fn table_exists(&self, _ctx: &EngineContext, name: &str) -> bool {
        self.inner.get_table(name).is_some()
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
    object_store: ObjectStore,
    storage_engine: S,
    /// Table mutex is used to protect the operations such as creating/opening/closing
    /// a table, to avoid things like opening the same table simultaneously.
    table_mutex: Mutex<()>,
}

fn build_row_key_desc(
    mut column_id: ColumnId,
    table_name: &str,
    table_schema: &SchemaRef,
    primary_key_indices: &Vec<usize>,
) -> Result<(ColumnId, RowKeyDescriptor)> {
    let ts_column_schema = table_schema
        .timestamp_column()
        .context(MissingTimestampIndexSnafu { table_name })?;
    let timestamp_index = table_schema.timestamp_index().unwrap();

    let ts_column = ColumnDescriptorBuilder::new(
        column_id,
        ts_column_schema.name.clone(),
        ts_column_schema.data_type.clone(),
    )
    .is_nullable(ts_column_schema.is_nullable)
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
        .is_nullable(column_schema.is_nullable)
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
    table_schema: &SchemaRef,
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
        .is_nullable(column_schema.is_nullable)
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

impl<S: StorageEngine> MitoEngineInner<S> {
    async fn create_table(
        &self,
        _ctx: &EngineContext,
        request: CreateTableRequest,
    ) -> Result<TableRef> {
        let table_name = &request.table_name;

        if let Some(table) = self.get_table(table_name) {
            if request.create_if_not_exists {
                return Ok(table);
            } else {
                return TableExistsSnafu { table_name }.fail();
            }
        }

        let table_schema = &request.schema;
        let primary_key_indices = &request.primary_key_indices;
        let (next_column_id, default_cf) = build_column_family(
            INIT_COLUMN_ID,
            table_name,
            table_schema,
            primary_key_indices,
        )?;
        let (next_column_id, row_key) = build_row_key_desc(
            next_column_id,
            table_name,
            table_schema,
            primary_key_indices,
        )?;

        let table_id = request.id;
        // TODO(dennis): supports multi regions;
        let region_number = 0;
        let region_id = region_id(table_id, region_number);

        let region_name = region_name(table_id, region_number);
        let region_descriptor = RegionDescriptorBuilder::default()
            .id(region_id)
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
            if request.create_if_not_exists {
                return Ok(table);
            } else {
                return TableExistsSnafu { table_name }.fail();
            }
        }

        let opts = CreateOptions {
            parent_dir: table_dir(table_name),
        };

        let region = self
            .storage_engine
            .create_region(&StorageEngineContext::default(), region_descriptor, &opts)
            .await
            .map_err(BoxedError::new)
            .context(error::CreateRegionSnafu)?;

        let table_meta = TableMetaBuilder::default()
            .schema(request.schema)
            .engine(MITO_ENGINE)
            .next_column_id(next_column_id)
            .primary_key_indices(request.primary_key_indices.clone())
            .build()
            .context(error::BuildTableMetaSnafu { table_name })?;

        let table_info = TableInfoBuilder::new(table_name.clone(), table_meta)
            .ident(table_id)
            .table_version(INIT_TABLE_VERSION)
            .table_type(TableType::Base)
            .desc(request.desc)
            .build()
            .context(error::BuildTableInfoSnafu { table_name })?;

        let table = Arc::new(
            MitoTable::create(table_name, table_info, region, self.object_store.clone()).await?,
        );

        logging::info!("Mito engine created table: {:?}.", table.table_info());

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

            let engine_ctx = StorageEngineContext::default();
            let opts = OpenOptions {
                parent_dir: table_dir(table_name),
            };

            let table_id = request.table_id;
            // TODO(dennis): supports multi regions;
            let region_number = 0;
            let region_name = region_name(table_id, region_number);

            let region = match self
                .storage_engine
                .open_region(&engine_ctx, &region_name, &opts)
                .await
                .map_err(BoxedError::new)
                .context(error::OpenRegionSnafu { region_name })?
            {
                None => return Ok(None),
                Some(region) => region,
            };

            let table =
                Arc::new(MitoTable::open(table_name, region, self.object_store.clone()).await?);

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

    // Alter table changes the schemas of the table. The altering happens as cloning a new schema,
    // change the new one, and swap the old. Though we can change the schema in place, considering
    // the complex interwinding of inner data representation of schema, I think it's safer to
    // change it like this to avoid partial inconsistent during the altering. For example, schema's
    // `name_to_index` field must changed with `column_schemas` synchronously. If we add or remove
    // columns from `column_schemas` *and then* update the `name_to_index`, there's a slightly time
    // window of an inconsistency of the two field, which might bring some hard to trace down
    // concurrency related bugs or failures. (Of course we could introduce some guards like readwrite
    // lock to protect the consistency of schema altering, but that would hurt the performance of
    // schema reads, and the reads are the dominant operation of schema. At last, altering is
    // performed far lesser frequent.)
    async fn alter_table(&self, _ctx: &EngineContext, req: AlterTableRequest) -> Result<TableRef> {
        let _lock = self.table_mutex.lock().await;

        let table_name = &req.table_name;
        let table = self
            .get_table(table_name)
            .context(error::TableNotFoundSnafu { table_name })?;
        let mito_table = table
            .as_any()
            .downcast_ref::<MitoTable<S::Region>>()
            .context(error::TableDowncastSnafu {
                table_name,
                table_engine: MITO_ENGINE,
            })?;

        let table_info = mito_table.table_info();
        let table_meta = &table_info.meta;
        let table_schema = match &req.alter_kind {
            AlterKind::AddColumn { new_column } => {
                build_table_schema_with_new_column(table_name, &table_meta.schema, new_column)?
            }
            _ => table_meta.schema.clone(),
        };

        let primary_key_indices = &table_meta.primary_key_indices;
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

        let new_meta = TableMetaBuilder::default()
            .schema(table_schema.clone())
            .engine(&table_meta.engine)
            .next_column_id(next_column_id)
            .primary_key_indices(primary_key_indices.clone())
            .build()
            .context(error::BuildTableMetaSnafu { table_name })?;

        let mut new_info = TableInfo::clone(&*table_info);
        new_info.ident.version = table_info.ident.version + 1;
        new_info.meta = new_meta;

        // first alter region
        let region = mito_table.region();
        self.alter_region(table_name, region, row_key, default_cf)
            .await?;

        // then alter table info
        let _manifest_version = mito_table.alter(new_info.clone()).await?;
        mito_table.set_table_info(new_info);

        // TODO(LFC): Think of a way to properly handle the metadata integrity between region and table.
        // Currently there are no "transactions" to alter the metadata of region and table together,
        // they are altered in sequence. That means there might be cases where the metadata of region
        // is altered while the table's is not. Then the metadata integrity between region and
        // table cannot be hold.
        Ok(table)
    }

    async fn alter_region(
        &self,
        table_name: &str,
        region: &S::Region,
        row_key: RowKeyDescriptor,
        default_cf: ColumnFamilyDescriptor,
    ) -> Result<()> {
        let region_id = region.id();
        let region_name = region.name();
        let region_descriptor = RegionDescriptorBuilder::default()
            .id(region_id)
            .name(region_name)
            .row_key(row_key)
            .default_cf(default_cf)
            .build()
            .context(error::BuildRegionDescriptorSnafu {
                table_name,
                region_name,
            })?;
        self.storage_engine
            .alter_region(&StorageEngineContext::default(), region_descriptor)
            .await
            .map_err(BoxedError::new)
            .context(error::AlterRegionSnafu { region_name })?;
        Ok(())
    }
}

fn build_table_schema_with_new_column(
    table_name: &str,
    table_schema: &SchemaRef,
    new_column: &ColumnSchema,
) -> Result<SchemaRef> {
    if table_schema
        .column_schema_by_name(&new_column.name)
        .is_some()
    {
        return error::ColumnExistsSnafu {
            column_name: &new_column.name,
            table_name,
        }
        .fail()?;
    }

    let mut columns = table_schema.column_schemas().to_vec();
    columns.push(new_column.clone());

    // Right now we are not support adding a timestamp index column or adding the column
    // before or after some column, so just clone a new schema like this.
    // TODO(LFC): support adding timestamp index column
    //   maybe a custom statement syntax like "ALTER TABLE ADD TIME INDEX ts BIGINT"?
    // TODO(LFC): support adding column before or after some column
    let mut builder = SchemaBuilder::from_columns(columns).version(table_schema.version() + 1);

    if let Some(index) = table_schema.timestamp_index() {
        builder = builder.timestamp_index(index);
    }
    for (k, v) in table_schema.arrow_schema().metadata.iter() {
        builder = builder.add_metadata(k, v);
    }
    let new_schema = Arc::new(builder.build().context(error::SchemaBuildSnafu {
        msg: format!("cannot add new column {:?}", new_column),
    })?);
    Ok(new_schema)
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
    use common_recordbatch::util;
    use datafusion_common::field_util::FieldExt;
    use datafusion_common::field_util::SchemaExt;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::vectors::*;
    use store_api::manifest::Manifest;
    use table::requests::InsertRequest;

    use super::*;
    use crate::table::test_util;
    use crate::table::test_util::{MockRegion, TABLE_NAME};

    #[test]
    fn test_region_name() {
        assert_eq!("1_0000000000", region_name(1, 0));
        assert_eq!("1_0000000001", region_name(1, 1));
        assert_eq!("99_0000000100", region_name(99, 100));
        assert_eq!("1000_0000009999", region_name(1000, 9999));
    }

    #[test]
    fn test_table_dir() {
        assert_eq!("test_table/", table_dir("test_table"));
        assert_eq!("demo/", table_dir("demo"));
    }

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

        assert_eq!(arrow_schema.field(0).name(), "host");
        assert_eq!(arrow_schema.field(1).name(), "cpu");
        assert_eq!(arrow_schema.field(2).name(), "memory");
        assert_eq!(arrow_schema.field(3).name(), "ts");

        let columns = batches[0].df_recordbatch.columns();
        assert_eq!(4, columns.len());
        assert_eq!(hosts.to_arrow_array(), columns[0]);
        assert_eq!(cpus.to_arrow_array(), columns[1]);
        assert_eq!(memories.to_arrow_array(), columns[2]);
        assert_eq!(tss.to_arrow_array(), columns[3]);

        // Scan with projections: cpu and memory
        let stream = table.scan(&Some(vec![1, 2]), &[], None).await.unwrap();
        let batches = util::collect(stream).await.unwrap();
        assert_eq!(1, batches.len());
        assert_eq!(batches[0].df_recordbatch.num_columns(), 2);

        let arrow_schema = batches[0].schema.arrow_schema();
        assert_eq!(arrow_schema.fields().len(), 2);

        assert_eq!(arrow_schema.field(0).name(), "cpu");
        assert_eq!(arrow_schema.field(1).name(), "memory");

        let columns = batches[0].df_recordbatch.columns();
        assert_eq!(2, columns.len());
        assert_eq!(cpus.to_arrow_array(), columns[0]);
        assert_eq!(memories.to_arrow_array(), columns[1]);

        // Scan with projections: only ts
        let stream = table.scan(&Some(vec![3]), &[], None).await.unwrap();
        let batches = util::collect(stream).await.unwrap();
        assert_eq!(1, batches.len());
        assert_eq!(batches[0].df_recordbatch.num_columns(), 1);

        let arrow_schema = batches[0].schema.arrow_schema();
        assert_eq!(arrow_schema.fields().len(), 1);

        assert_eq!(arrow_schema.field(0).name(), "ts");

        let columns = batches[0].df_recordbatch.columns();
        assert_eq!(1, columns.len());
        assert_eq!(tss.to_arrow_array(), columns[0]);
    }

    #[tokio::test]
    async fn test_create_if_not_exists() {
        common_telemetry::init_default_ut_logging();
        let ctx = EngineContext::default();

        let (_engine, table_engine, table, _object_store, _dir) =
            test_util::setup_mock_engine_and_table().await;

        let table = table
            .as_any()
            .downcast_ref::<MitoTable<MockRegion>>()
            .unwrap();
        let table_info = table.table_info();

        let request = CreateTableRequest {
            id: 1,
            catalog_name: None,
            schema_name: None,
            table_name: table_info.name.to_string(),
            schema: table_info.meta.schema.clone(),
            create_if_not_exists: true,
            desc: None,
            primary_key_indices: Vec::default(),
            table_options: HashMap::new(),
        };

        let created_table = table_engine.create_table(&ctx, request).await.unwrap();
        assert_eq!(
            table_info,
            created_table
                .as_any()
                .downcast_ref::<MitoTable<MockRegion>>()
                .unwrap()
                .table_info()
        );

        // test create_if_not_exists=false
        let request = CreateTableRequest {
            id: 1,
            catalog_name: None,
            schema_name: None,
            table_name: table_info.name.to_string(),
            schema: table_info.meta.schema.clone(),
            create_if_not_exists: false,
            desc: None,
            primary_key_indices: Vec::default(),
            table_options: HashMap::new(),
        };

        let result = table_engine.create_table(&ctx, request).await;

        assert!(result.is_err());
        assert!(matches!(result, Err(e) if format!("{:?}", e).contains("Table already exists")));
    }

    #[tokio::test]
    async fn test_open_table() {
        common_telemetry::init_default_ut_logging();

        let ctx = EngineContext::default();
        let open_req = OpenTableRequest {
            catalog_name: String::new(),
            schema_name: String::new(),
            table_name: test_util::TABLE_NAME.to_string(),
            // the test table id is 1
            table_id: 1,
        };

        let (engine, table, object_store, _dir) = {
            let (engine, table_engine, table, object_store, dir) =
                test_util::setup_mock_engine_and_table().await;
            assert_eq!(MITO_ENGINE, table_engine.name());
            // Now try to open the table again.
            let reopened = table_engine
                .open_table(&ctx, open_req.clone())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(table.schema(), reopened.schema());

            (engine, table, object_store, dir)
        };

        // Construct a new table engine, and try to open the table.
        let table_engine = MitoEngine::new(EngineConfig::default(), engine, object_store);
        let reopened = table_engine
            .open_table(&ctx, open_req.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(table.schema(), reopened.schema());

        let table = table
            .as_any()
            .downcast_ref::<MitoTable<MockRegion>>()
            .unwrap();
        let reopened = reopened
            .as_any()
            .downcast_ref::<MitoTable<MockRegion>>()
            .unwrap();

        // assert recovered table_info is correct
        assert_eq!(table.table_info(), reopened.table_info());
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

    #[tokio::test]
    async fn test_alter_table_add_column() {
        let (_engine, table_engine, table, _object_store, _dir) =
            test_util::setup_mock_engine_and_table().await;

        let table = table
            .as_any()
            .downcast_ref::<MitoTable<MockRegion>>()
            .unwrap();
        let table_info = table.table_info();
        let old_info = (&*table_info).clone();
        let old_meta = &old_info.meta;
        let old_schema = &old_meta.schema;

        let new_column = ColumnSchema::new("my_tag", ConcreteDataType::string_datatype(), true);
        let req = AlterTableRequest {
            catalog_name: None,
            schema_name: None,
            table_name: TABLE_NAME.to_string(),
            alter_kind: AlterKind::AddColumn {
                new_column: new_column.clone(),
            },
        };
        let table = table_engine
            .alter_table(&EngineContext::default(), req)
            .await
            .unwrap();

        let table = table
            .as_any()
            .downcast_ref::<MitoTable<MockRegion>>()
            .unwrap();
        let new_info = table.table_info();
        let new_meta = &new_info.meta;
        let new_schema = &new_meta.schema;

        assert_eq!(new_schema.num_columns(), old_schema.num_columns() + 1);
        assert_eq!(
            new_schema.column_schemas().split_last().unwrap(),
            (&new_column, old_schema.column_schemas())
        );
        assert_eq!(new_schema.timestamp_column(), old_schema.timestamp_column());
        assert_eq!(new_schema.version(), old_schema.version() + 1);
        assert_eq!(new_meta.next_column_id, old_meta.next_column_id + 1);
    }

    #[tokio::test]
    async fn test_alter_region() {
        let (_engine, table_engine, table, _object_store, _dir) =
            test_util::setup_mock_engine_and_table().await;

        let table = table
            .as_any()
            .downcast_ref::<MitoTable<MockRegion>>()
            .unwrap();
        let table_info = table.table_info();
        let table_name = &table_info.name;
        let table_meta = &table_info.meta;
        let table_schema = &table_meta.schema;

        // columns = (host, cpu, memory, ts)
        let (next_column_id, default_cf) =
            build_column_family(INIT_COLUMN_ID, table_name, table_schema, &[0, 3]).unwrap();
        let (_next_column_id, row_key) =
            build_row_key_desc(next_column_id, table_name, table_schema, &vec![0, 3]).unwrap();

        let inner = table_engine.inner;
        let region = table.region();
        let metadata = region.inner.metadata.load();
        // assert in case the testing data are changed
        assert_eq!(
            vec!["ts"],
            metadata
                .schema()
                .row_key_columns()
                .map(|x| x.name())
                .collect::<Vec<&str>>(),
        );
        let expect_region_id = region.id();
        let expect_region_name = region.name().to_string();

        inner
            .alter_region(table_name, region, row_key, default_cf)
            .await
            .unwrap();

        let new_region = inner
            .storage_engine
            .get_region(&StorageEngineContext::default(), region.name())
            .unwrap()
            .unwrap();
        assert_eq!(new_region.id(), expect_region_id);
        assert_eq!(new_region.name(), expect_region_name);

        let new_metadata = new_region.inner.metadata.load();
        assert_eq!(
            vec!["host", "ts"],
            new_metadata
                .schema()
                .row_key_columns()
                .map(|x| x.name())
                .collect::<Vec<&str>>()
        );
    }

    #[test]
    fn test_build_table_schema_with_new_column() {
        let table_info = test_util::build_test_table_info();
        let table_name = &table_info.name;
        let table_meta = &table_info.meta;
        let table_schema = &table_meta.schema;

        let new_column = ColumnSchema::new("host", ConcreteDataType::string_datatype(), true);
        let result = build_table_schema_with_new_column(table_name, table_schema, &new_column);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Column host already exists in table demo"));

        let new_column = ColumnSchema::new("my_tag", ConcreteDataType::string_datatype(), true);
        let new_schema =
            build_table_schema_with_new_column(table_name, table_schema, &new_column).unwrap();

        assert_eq!(new_schema.num_columns(), table_schema.num_columns() + 1);
        assert_eq!(
            new_schema.column_schemas().split_last().unwrap(),
            (&new_column, table_schema.column_schemas())
        );

        assert_eq!(
            new_schema.timestamp_column(),
            table_schema.timestamp_column()
        );
        assert_eq!(new_schema.version(), table_schema.version() + 1);
    }
}
