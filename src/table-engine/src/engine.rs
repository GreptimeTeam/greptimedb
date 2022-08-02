use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::RwLock;

use async_trait::async_trait;
use common_error::ext::BoxedError;
use snafu::ResultExt;
use store_api::storage::{
    self, ColumnDescriptorBuilder, ColumnFamilyDescriptorBuilder, ConcreteDataType, OpenOptions,
    Region, RegionDescriptor, RegionId, RegionMeta, RowKeyDescriptorBuilder, StorageEngine,
};
use table::engine::{EngineContext, TableEngine};
use table::requests::{AlterTableRequest, CreateTableRequest, DropTableRequest, OpenTableRequest};
use table::Result as TableResult;
use table::{
    metadata::{TableId, TableInfoBuilder, TableMetaBuilder, TableType},
    table::TableRef,
};
use tokio::sync::Mutex;

use crate::error::{self, Result};
use crate::table::MitoTable;

pub const DEFAULT_ENGINE: &str = "mito";

/// [TableEngine] implementation.
///
/// About mito <https://en.wikipedia.org/wiki/Alfa_Romeo_MiTo>.
/// "you can't be a true petrolhead until you've owned an Alfa Romeo" -- by Jeremy Clarkson
#[derive(Clone)]
pub struct MitoEngine<Store: StorageEngine> {
    inner: Arc<MitoEngineInner<Store>>,
}

impl<Store: StorageEngine> MitoEngine<Store> {
    pub fn new(storage_engine: Store) -> Self {
        Self {
            inner: Arc::new(MitoEngineInner::new(storage_engine)),
        }
    }
}

#[async_trait]
impl<Store: StorageEngine> TableEngine for MitoEngine<Store> {
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
    ) -> TableResult<TableRef> {
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
struct MitoEngineInner<Store: StorageEngine> {
    /// All tables opened by the engine.
    ///
    /// Writing to `tables` should also hold the `table_mutex`.
    tables: RwLock<HashMap<String, TableRef>>,
    storage_engine: Store,
    // FIXME(yingwen): Remove `next_table_id`. Table id should be assigned by other module (maybe catalog).
    next_table_id: AtomicU64,
    /// Table mutex is used to protect the operations such as creating/opening/closing
    /// a table, to avoid things like opening the same table simultaneously.
    table_mutex: Mutex<()>,
}

impl<Store: StorageEngine> MitoEngineInner<Store> {
    fn new(storage_engine: Store) -> Self {
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

impl<Store: StorageEngine> MitoEngineInner<Store> {
    async fn create_table(
        &self,
        _ctx: &EngineContext,
        request: CreateTableRequest,
    ) -> Result<TableRef> {
        //FIXME(dennis): we only supports creating a demo table right now
        //The create table sql is like:
        //    create table demo(host string,
        //                                  ts int64,
        //                                  cpu float64,
        //                                  memory float64,
        //                                  PRIMARY KEY(ts, host)) with regions=1;

        //TODO(dennis): supports multi regions
        let region_id: RegionId = 0;
        // TODO(yingwen): Maybe we should use table name as part of region name.
        let name = storage::gen_region_name(region_id);

        let host_column =
            ColumnDescriptorBuilder::new(0, "host", ConcreteDataType::string_datatype())
                .is_nullable(false)
                .build()
                .unwrap();
        let cpu_column =
            ColumnDescriptorBuilder::new(1, "cpu", ConcreteDataType::float64_datatype())
                .is_nullable(true)
                .build()
                .unwrap();
        let memory_column =
            ColumnDescriptorBuilder::new(2, "memory", ConcreteDataType::float64_datatype())
                .is_nullable(true)
                .build()
                .unwrap();
        let ts_column = ColumnDescriptorBuilder::new(0, "ts", ConcreteDataType::int64_datatype())
            .build()
            .unwrap();

        let row_key = RowKeyDescriptorBuilder::new(ts_column)
            .push_column(host_column)
            .enable_version_column(false)
            .build()
            .unwrap();

        let default_cf = ColumnFamilyDescriptorBuilder::default()
            .push_column(cpu_column)
            .push_column(memory_column)
            .build()
            .unwrap();

        let region = self
            .storage_engine
            .create_region(
                &storage::EngineContext::default(),
                RegionDescriptor {
                    id: region_id,
                    name,
                    row_key,
                    default_cf,
                    extra_cfs: Vec::default(),
                },
            )
            .await
            .map_err(BoxedError::new)
            .context(error::CreateRegionSnafu)?;

        // Use region meta schema instead of request schema
        let table_meta = TableMetaBuilder::default()
            .schema(region.in_memory_metadata().schema().clone())
            .engine(DEFAULT_ENGINE)
            .build()
            .unwrap();

        let table_name = request.name;
        let table_info = TableInfoBuilder::new(table_name.clone(), table_meta)
            .ident(self.next_table_id())
            .table_version(0u64)
            .table_type(TableType::Base)
            .desc(request.desc)
            .build()
            .unwrap();

        let table = Arc::new(MitoTable::new(table_info, region));

        self.tables
            .write()
            .unwrap()
            .insert(table_name, table.clone());

        Ok(table)
    }

    // TODO(yingwen): Support catalog and schema name.
    async fn open_table(
        &self,
        _ctx: &EngineContext,
        request: OpenTableRequest,
    ) -> TableResult<TableRef> {
        let table_name = &request.table_name;
        if let Some(table) = self.get_table(table_name) {
            // Table has already been opened.
            return Ok(table);
        }

        // Acquires the mutex before opening a new table.
        let _lock = self.table_mutex.lock().await;
        // Checks again, read lock should be enough since we are guarded by the mutex.
        if let Some(table) = self.get_table(table_name) {
            return Ok(table);
        }

        let engine_ctx = storage::EngineContext::default();
        let opts = OpenOptions::default();
        let region_name = table_name;
        // Now we just use table name as region name. TODO(yingwen): Naming pattern of region.
        let region = self
            .storage_engine
            .open_region(&engine_ctx, region_name, &opts)
            .await
            .map_err(BoxedError::new)
            .context(error::OpenRegionSnafu { region_name })?;

        let table_meta = TableMetaBuilder::default()
            .schema(region.in_memory_metadata().schema().clone())
            .engine(DEFAULT_ENGINE)
            .build()
            .context(error::BuildTableMetaSnafu)?;
        let table_info = TableInfoBuilder::new(table_name.clone(), table_meta)
            .ident(request.table_id)
            .table_version(0u64)
            .table_type(TableType::Base)
            .build()
            .context(error::BuildTableInfoSnafu)?;

        let table = Arc::new(MitoTable::new(table_info, region));

        self.tables
            .write()
            .unwrap()
            .insert(table_name.to_string(), table.clone());

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
    use crate::table::test;

    #[tokio::test]
    async fn test_create_table_insert_scan() {
        let (_engine, table, schema, _dir) = test::setup_test_engine_and_table().await;

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
        assert_eq!(arrow_schema.field(1).name(), "ts");
        assert_eq!(arrow_schema.field(2).name(), "cpu");
        assert_eq!(arrow_schema.field(3).name(), "memory");

        let columns = batches[0].df_recordbatch.columns();
        assert_eq!(4, columns.len());
        assert_eq!(hosts.to_arrow_array(), columns[0]);
        assert_eq!(tss.to_arrow_array(), columns[1]);
        assert_eq!(cpus.to_arrow_array(), columns[2]);
        assert_eq!(memories.to_arrow_array(), columns[3]);
    }
}
