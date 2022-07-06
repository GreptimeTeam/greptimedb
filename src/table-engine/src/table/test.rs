use std::sync::Arc;

use datatypes::prelude::ConcreteDataType;
use datatypes::schema::SchemaRef;
use datatypes::schema::{ColumnSchema, Schema};
use log_store::fs::log::NoopLogStore;
use storage::EngineImpl;
use table::engine::{EngineContext, TableEngine};
use table::requests::CreateTableRequest;
use table::TableRef;

use crate::engine::MitoEngine;

pub async fn setup_test_engine_and_table(
) -> (MitoEngine<EngineImpl<NoopLogStore>>, TableRef, SchemaRef) {
    let column_schemas = vec![
        ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
        ColumnSchema::new("ts", ConcreteDataType::int64_datatype(), true),
        ColumnSchema::new("cpu", ConcreteDataType::float64_datatype(), true),
        ColumnSchema::new("memory", ConcreteDataType::float64_datatype(), true),
    ];

    let table_engine = MitoEngine::<EngineImpl<NoopLogStore>>::new(EngineImpl::new(Arc::new(
        NoopLogStore::default(),
    )));

    let table_name = "demo";
    let schema = Arc::new(Schema::new(column_schemas));
    let table = table_engine
        .create_table(
            &EngineContext::default(),
            CreateTableRequest {
                name: table_name.to_string(),
                desc: Some(" a test table".to_string()),
                schema: schema.clone(),
            },
        )
        .await
        .unwrap();

    (table_engine, table, schema)
}
