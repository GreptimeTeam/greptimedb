use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::test_util::EmptyTable;
use crate::{
    engine::{EngineContext, TableEngine},
    requests::{AlterTableRequest, CreateTableRequest, DropTableRequest, OpenTableRequest},
    Result, TableRef,
};

#[derive(Default)]
pub struct MockTableEngine {
    tables: Mutex<HashMap<(String, String, String), TableRef>>,
}

impl MockTableEngine {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
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
