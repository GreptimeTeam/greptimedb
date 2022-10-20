use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::error::Result;
use crate::requests::{AlterTableRequest, CreateTableRequest, DropTableRequest, OpenTableRequest};
use crate::test_util::EmptyTable;
use crate::TableRef;

/// Table engine abstraction.
#[async_trait::async_trait]
pub trait TableEngine: Send + Sync {
    /// Return engine name
    fn name(&self) -> &str;

    /// Create a table by given request.
    ///
    /// Return the created table.
    async fn create_table(
        &self,
        ctx: &EngineContext,
        request: CreateTableRequest,
    ) -> Result<TableRef>;

    /// Open an existing table by given `request`, returns the opened table. If the table does not
    /// exist, returns an `Ok(None)`.
    async fn open_table(
        &self,
        ctx: &EngineContext,
        request: OpenTableRequest,
    ) -> Result<Option<TableRef>>;

    /// Alter table schema, options etc. by given request,
    ///
    /// Returns the table after altered.
    async fn alter_table(
        &self,
        ctx: &EngineContext,
        request: AlterTableRequest,
    ) -> Result<TableRef>;

    /// Returns the table by it's name.
    fn get_table(&self, ctx: &EngineContext, name: &str) -> Result<Option<TableRef>>;

    /// Returns true when the given table is exists.
    fn table_exists(&self, ctx: &EngineContext, name: &str) -> bool;

    /// Drops the given table.
    async fn drop_table(&self, ctx: &EngineContext, request: DropTableRequest) -> Result<()>;
}

pub type TableEngineRef = Arc<dyn TableEngine>;

/// Storage engine context.
#[derive(Debug, Clone, Default)]
pub struct EngineContext {}

#[derive(Default)]
pub struct MockTableEngine {
    tables: Mutex<HashMap<(String, String, String), TableRef>>,
}

impl MockTableEngine {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl TableEngine for MockTableEngine {
    fn name(&self) -> &str {
        "MockTableEngine"
    }

    async fn create_table(
        &self,
        _ctx: &EngineContext,
        request: CreateTableRequest,
    ) -> Result<TableRef> {
        let catalog_name = request.catalog_name.clone();
        let schema_name = request.schema_name.clone();
        let table_name = request.table_name.clone();

        let table_ref = Arc::new(EmptyTable::new(request));

        self.tables
            .lock()
            .await
            .insert((catalog_name, schema_name, table_name), table_ref.clone());
        Ok(table_ref)
    }

    async fn open_table(
        &self,
        _ctx: &EngineContext,
        request: OpenTableRequest,
    ) -> Result<Option<TableRef>> {
        let catalog_name = request.catalog_name;
        let schema_name = request.schema_name;
        let table_name = request.table_name;

        let res = self
            .tables
            .lock()
            .await
            .get(&(catalog_name, schema_name, table_name))
            .cloned();

        Ok(res)
    }

    async fn alter_table(
        &self,
        _ctx: &EngineContext,
        _request: AlterTableRequest,
    ) -> Result<TableRef> {
        unimplemented!()
    }

    fn get_table(&self, _ctx: &EngineContext, _name: &str) -> Result<Option<TableRef>> {
        unimplemented!()
    }

    fn table_exists(&self, _ctx: &EngineContext, _name: &str) -> bool {
        unimplemented!()
    }

    async fn drop_table(&self, _ctx: &EngineContext, _request: DropTableRequest) -> Result<()> {
        unimplemented!()
    }
}
