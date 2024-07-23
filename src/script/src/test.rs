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

use std::sync::Arc;

use api::v1::greptime_request::Request;
use async_trait::async_trait;
use catalog::memory::MemoryCatalogManager;
use common_query::Output;
use common_recordbatch::RecordBatch;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::{StringVector, VectorRef};
use query::QueryEngineFactory;
use servers::query_handler::grpc::GrpcQueryHandler;
use session::context::QueryContextRef;
use table::test_util::MemTable;

use crate::error::{Error, Result};
use crate::manager::ScriptManager;

/// Setup the scripts table and create a script manager.
pub async fn setup_scripts_manager(
    catalog: &str,
    schema: &str,
    name: &str,
    script: &str,
) -> ScriptManager<Error> {
    let column_schemas = vec![
        ColumnSchema::new("script", ConcreteDataType::string_datatype(), false),
        ColumnSchema::new("schema", ConcreteDataType::string_datatype(), false),
        ColumnSchema::new("name", ConcreteDataType::string_datatype(), false),
    ];

    let columns: Vec<VectorRef> = vec![
        Arc::new(StringVector::from(vec![script])),
        Arc::new(StringVector::from(vec![schema])),
        Arc::new(StringVector::from(vec![name])),
    ];

    let schema = Arc::new(Schema::new(column_schemas));
    let recordbatch = RecordBatch::new(schema, columns).unwrap();

    let table = MemTable::table("scripts", recordbatch);

    let catalog_manager = MemoryCatalogManager::new_with_table(table.clone());

    let factory = QueryEngineFactory::new(catalog_manager.clone(), None, None, None, None, false);
    let query_engine = factory.query_engine();
    let mgr = ScriptManager::new(Arc::new(MockGrpcQueryHandler {}) as _, query_engine)
        .await
        .unwrap();
    mgr.insert_scripts_table(catalog, table);

    mgr
}

struct MockGrpcQueryHandler {}

#[async_trait]
impl GrpcQueryHandler for MockGrpcQueryHandler {
    type Error = Error;

    async fn do_query(&self, _query: Request, _ctx: QueryContextRef) -> Result<Output> {
        Ok(Output::new_with_affected_rows(1))
    }
}
