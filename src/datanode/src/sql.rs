//! sql handler

mod insert;
use query::catalog::schema::SchemaProviderRef;
use query::query_engine::Output;
use snafu::{OptionExt, ResultExt};
use sql::statements::statement::Statement;
use table::engine::{EngineContext, TableEngine};
use table::requests::*;
use table::TableRef;

use crate::error::{GetTableSnafu, Result, TableNotFoundSnafu};

pub enum SqlRequest {
    Insert(InsertRequest),
}

// Handler to execute SQL except query
pub struct SqlHandler<Engine: TableEngine> {
    table_engine: Engine,
}

impl<Engine: TableEngine> SqlHandler<Engine> {
    pub fn new(table_engine: Engine) -> Self {
        Self { table_engine }
    }

    pub async fn execute(&self, request: SqlRequest) -> Result<Output> {
        match request {
            SqlRequest::Insert(req) => self.insert(req).await,
        }
    }

    pub(crate) fn get_table(&self, table_name: &str) -> Result<TableRef> {
        self.table_engine
            .get_table(&EngineContext::default(), table_name)
            .map_err(|e| Box::new(e) as _)
            .context(GetTableSnafu { table_name })?
            .context(TableNotFoundSnafu { table_name })
    }

    // Cast sql statement into sql request
    pub(crate) fn statement_to_request(
        &self,
        schema_provider: SchemaProviderRef,
        statement: Statement,
    ) -> Result<SqlRequest> {
        match statement {
            Statement::Insert(stmt) => self.insert_to_request(schema_provider, *stmt),
            _ => unimplemented!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::Arc;

    use common_query::logical_plan::Expr;
    use common_recordbatch::SendableRecordBatchStream;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
    use datatypes::value::Value;
    use query::catalog::memory;
    use query::catalog::schema::SchemaProvider;
    use query::error::Result as QueryResult;
    use query::QueryEngineFactory;
    use storage::EngineImpl;
    use table::error::Result as TableResult;
    use table::{Table, TableRef};
    use table_engine::engine::MitoEngine;

    use super::*;

    struct DemoTable;

    #[async_trait::async_trait]
    impl Table for DemoTable {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            let column_schemas = vec![
                ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
                ColumnSchema::new("cpu", ConcreteDataType::float64_datatype(), true),
                ColumnSchema::new("memory", ConcreteDataType::float64_datatype(), true),
                ColumnSchema::new("ts", ConcreteDataType::int64_datatype(), true),
            ];

            Arc::new(Schema::new(column_schemas))
        }
        async fn scan(
            &self,
            _projection: &Option<Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> TableResult<SendableRecordBatchStream> {
            unimplemented!();
        }
    }

    struct MockSchemaProvider;

    impl SchemaProvider for MockSchemaProvider {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn table_names(&self) -> Vec<String> {
            vec!["demo".to_string()]
        }

        fn table(&self, name: &str) -> Option<TableRef> {
            assert_eq!(name, "demo");
            Some(Arc::new(DemoTable {}))
        }

        fn register_table(&self, _name: String, _table: TableRef) -> QueryResult<Option<TableRef>> {
            unimplemented!();
        }
        fn deregister_table(&self, _name: &str) -> QueryResult<Option<TableRef>> {
            unimplemented!();
        }
        fn table_exist(&self, name: &str) -> bool {
            name == "demo"
        }
    }

    #[test]
    fn test_statement_to_request() {
        let catalog_list = memory::new_memory_catalog_list().unwrap();
        let factory = QueryEngineFactory::new(catalog_list);
        let query_engine = factory.query_engine().clone();

        let sql = r#"insert into demo(host, cpu, memory, ts) values
                           ('host1', 66.6, 1024, 1655276557000),
                           ('host2', 88.8,  333.3, 1655276558000)
                           "#;

        let table_engine = MitoEngine::<EngineImpl>::new(EngineImpl::new());
        let sql_handler = SqlHandler::new(table_engine);

        let stmt = query_engine.sql_to_statement(sql).unwrap();
        let schema_provider = Arc::new(MockSchemaProvider {});
        let request = sql_handler
            .statement_to_request(schema_provider, stmt)
            .unwrap();

        match request {
            SqlRequest::Insert(req) => {
                assert_eq!(req.table_name, "demo");
                let columns_values = req.columns_values;
                assert_eq!(4, columns_values.len());

                let hosts = &columns_values["host"];
                assert_eq!(2, hosts.len());
                assert_eq!(Value::from("host1"), hosts.get(0));
                assert_eq!(Value::from("host2"), hosts.get(1));

                let cpus = &columns_values["cpu"];
                assert_eq!(2, cpus.len());
                assert_eq!(Value::from(66.6f64), cpus.get(0));
                assert_eq!(Value::from(88.8f64), cpus.get(1));

                let memories = &columns_values["memory"];
                assert_eq!(2, memories.len());
                assert_eq!(Value::from(1024f64), memories.get(0));
                assert_eq!(Value::from(333.3f64), memories.get(1));

                let ts = &columns_values["ts"];
                assert_eq!(2, ts.len());
                assert_eq!(Value::from(1655276557000i64), ts.get(0));
                assert_eq!(Value::from(1655276558000i64), ts.get(1));
            }
        }
    }
}
