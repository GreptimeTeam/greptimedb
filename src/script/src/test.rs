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

/// Setup the scripts table
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

    let factory = QueryEngineFactory::new(catalog_manager.clone(), None, false);
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
        Ok(Output::AffectedRows(1))
    }
}
