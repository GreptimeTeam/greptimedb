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

use std::collections::HashMap;
use std::sync::{Arc, RwLock as StdRwLock};

use common_recordbatch::RecordBatch;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::StringVector;
use table::engine::{CloseTableResult, EngineContext, TableEngine};
use table::metadata::TableId;
use table::requests::{
    AlterTableRequest, CloseTableRequest, CreateTableRequest, DropTableRequest, OpenTableRequest,
    TruncateTableRequest,
};
use table::test_util::MemTable;
use table::TableRef;

#[derive(Default)]
pub struct MockTableEngine {
    tables: StdRwLock<HashMap<TableId, TableRef>>,
}

#[async_trait::async_trait]
impl TableEngine for MockTableEngine {
    fn name(&self) -> &str {
        "MockTableEngine"
    }

    /// Create a table with only one column
    async fn create_table(
        &self,
        _ctx: &EngineContext,
        request: CreateTableRequest,
    ) -> table::Result<TableRef> {
        let table_id = request.id;

        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "name",
            ConcreteDataType::string_datatype(),
            true,
        )]));

        let data = vec![Arc::new(StringVector::from(vec!["a", "b", "c"])) as _];
        let record_batch = RecordBatch::new(schema, data).unwrap();
        let table = MemTable::new_with_catalog(
            &request.table_name,
            record_batch,
            table_id,
            request.catalog_name,
            request.schema_name,
            vec![0],
        );

        let mut tables = self.tables.write().unwrap();
        let _ = tables.insert(table_id, table.clone() as TableRef);
        Ok(table)
    }

    async fn open_table(
        &self,
        _ctx: &EngineContext,
        request: OpenTableRequest,
    ) -> table::Result<Option<TableRef>> {
        Ok(self.tables.read().unwrap().get(&request.table_id).cloned())
    }

    async fn alter_table(
        &self,
        _ctx: &EngineContext,
        _request: AlterTableRequest,
    ) -> table::Result<TableRef> {
        unimplemented!()
    }

    fn get_table(
        &self,
        _ctx: &EngineContext,
        table_id: TableId,
    ) -> table::Result<Option<TableRef>> {
        Ok(self.tables.read().unwrap().get(&table_id).cloned())
    }

    fn table_exists(&self, _ctx: &EngineContext, table_id: TableId) -> bool {
        self.tables.read().unwrap().contains_key(&table_id)
    }

    async fn drop_table(
        &self,
        _ctx: &EngineContext,
        _request: DropTableRequest,
    ) -> table::Result<bool> {
        unimplemented!()
    }

    async fn close_table(
        &self,
        _ctx: &EngineContext,
        request: CloseTableRequest,
    ) -> table::Result<CloseTableResult> {
        let _ = self.tables.write().unwrap().remove(&request.table_id);
        Ok(CloseTableResult::Released(vec![]))
    }

    async fn close(&self) -> table::Result<()> {
        Ok(())
    }

    async fn truncate_table(
        &self,
        _ctx: &EngineContext,
        _request: TruncateTableRequest,
    ) -> table::Result<bool> {
        Ok(true)
    }
}
