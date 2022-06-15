use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::RwLock;

use async_trait::async_trait;
use snafu::Backtrace;
use snafu::GenerateImplicitData;
use store_api::storage::ConcreteDataType;
use store_api::storage::{
    self, ColumnDescriptorBuilder, ColumnFamilyDescriptorBuilder, EngineContext as StorageContext,
    Region, RegionDescriptor, RegionId, RegionMeta, RowKeyDescriptorBuilder, StorageEngine,
};
use table::engine::{EngineContext, TableEngine};
use table::requests::{AlterTableRequest, CreateTableRequest, DropTableRequest};
use table::{
    metadata::{TableId, TableInfoBuilder, TableMetaBuilder, TableType},
    table::TableRef,
};

use crate::error::{Error, Result};
use crate::table::MitoTable;

pub const DEFAULT_ENGINE: &str = "mito";

/// [TableEgnine] implementation.
///
/// About mito <https://en.wikipedia.org/wiki/Alfa_Romeo_MiTo>.
/// "you can't be a true petrolhead until you've owned an Alfa Romeo" -- by Jeremy Clarkson
#[derive(Clone)]
pub struct MitoEngine<Store: StorageEngine + 'static> {
    inner: Arc<MitoEngineInner<Store>>,
}

impl<Store: StorageEngine + 'static> MitoEngine<Store> {
    pub fn new(storage_engine: Store) -> Self {
        Self {
            inner: Arc::new(MitoEngineInner::new(storage_engine)),
        }
    }
}

#[async_trait]
impl<Store: StorageEngine> TableEngine for MitoEngine<Store> {
    type Error = Error;

    async fn create_table(
        &self,
        ctx: &EngineContext,
        request: CreateTableRequest,
    ) -> Result<TableRef> {
        self.inner.create_table(ctx, request).await
    }

    async fn alter_table(
        &self,
        _ctx: &EngineContext,
        _request: AlterTableRequest,
    ) -> Result<TableRef> {
        unimplemented!();
    }

    fn get_table(&self, ctx: &EngineContext, name: &str) -> Result<Option<TableRef>> {
        self.inner.get_table(ctx, name)
    }

    fn table_exists(&self, _ctx: &EngineContext, _name: &str) -> bool {
        unimplemented!();
    }

    async fn drop_table(&self, _ctx: &EngineContext, _request: DropTableRequest) -> Result<()> {
        unimplemented!();
    }
}

/// FIXME(boyan) impl system catalog to keep table metadata.
struct MitoEngineInner<Store: StorageEngine + 'static> {
    tables: RwLock<HashMap<String, TableRef>>,
    storage_engine: Store,
    next_table_id: AtomicU64,
}

impl<Store: StorageEngine> MitoEngineInner<Store> {
    fn new(storage_engine: Store) -> Self {
        Self {
            tables: RwLock::new(HashMap::default()),
            storage_engine,
            next_table_id: AtomicU64::new(0),
        }
    }

    fn next_table_id(&self) -> TableId {
        self.next_table_id.fetch_add(1, Ordering::Relaxed)
    }
}

impl<Store: StorageEngine + 'static> MitoEngineInner<Store> {
    async fn create_table(
        &self,
        _ctx: &EngineContext,
        request: CreateTableRequest,
    ) -> Result<TableRef> {
        //FIXME(boyan): we only supports creating a demo table right now
        //The create table sql is like:
        //    create table demo(host string,
        //                                  ts int64,
        //                                  cpu float64,
        //                                  memory float64,
        //                                  TIMESTAMP KEY(ts, host)) with regions=1;

        //TODO(boyan): supports multi regions
        let region_id: RegionId = 0;
        let name = storage::gen_region_name(region_id);

        let host_column =
            ColumnDescriptorBuilder::new(0, "host", ConcreteDataType::string_datatype())
                .is_nullable(false)
                .build();
        let cpu_column =
            ColumnDescriptorBuilder::new(1, "cpu", ConcreteDataType::float64_datatype())
                .is_nullable(true)
                .build();
        let memory_column =
            ColumnDescriptorBuilder::new(2, "memory", ConcreteDataType::float64_datatype())
                .is_nullable(true)
                .build();
        let ts_column =
            ColumnDescriptorBuilder::new(0, "ts", ConcreteDataType::int64_datatype()).build();

        let row_key = RowKeyDescriptorBuilder::new(ts_column)
            .push_column(host_column)
            .enable_version_column(false)
            .build();

        let default_cf = ColumnFamilyDescriptorBuilder::default()
            .push_column(cpu_column)
            .push_column(memory_column)
            .build();

        let region = self
            .storage_engine
            .create_region(
                &StorageContext::default(),
                RegionDescriptor {
                    id: region_id,
                    name,
                    row_key,
                    default_cf,
                    extra_cfs: Vec::default(),
                },
            )
            .await
            .map_err(|e| Error::CreateTable {
                source: Box::new(e),
                backtrace: Backtrace::generate(),
            })?;

        // Use region meta schema instead of request schema
        let table_meta = TableMetaBuilder::new(region.in_memory_metadata().schema().clone())
            .engine(DEFAULT_ENGINE)
            .build();

        let table_name = request.name;
        let table_info = TableInfoBuilder::new(table_name.clone(), table_meta)
            .table_id(self.next_table_id())
            .table_version(0u64)
            .table_type(TableType::Base)
            .desc(request.desc)
            .build();

        let table = Arc::new(MitoTable::new(table_info, region));

        self.tables
            .write()
            .unwrap()
            .insert(table_name, table.clone());

        Ok(table)
    }

    fn get_table(&self, _ctx: &EngineContext, name: &str) -> Result<Option<TableRef>> {
        Ok(self.tables.read().unwrap().get(name).cloned())
    }
}
