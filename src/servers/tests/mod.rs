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
use std::sync::{Arc, RwLock};

use api::v1::greptime_request::{Request as GreptimeRequest, Request};
use api::v1::query_request::Query;
use async_trait::async_trait;
use catalog::local::{MemoryCatalogManager, MemoryCatalogProvider, MemorySchemaProvider};
use catalog::{CatalogList, CatalogProvider, SchemaProvider};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_query::Output;
use datatypes::schema::Schema;
use query::parser::{PromQuery, QueryLanguageParser, QueryStatement};
use query::{QueryEngineFactory, QueryEngineRef};
use script::engine::{CompileContext, EvalContext, Script, ScriptEngine};
use script::python::{PyEngine, PyScript};
use servers::error::{Error, NotSupportedSnafu, Result};
use servers::query_handler::grpc::{GrpcQueryHandler, ServerGrpcQueryHandlerRef};
use servers::query_handler::sql::{ServerSqlQueryHandlerRef, SqlQueryHandler};
use servers::query_handler::{ScriptHandler, ScriptHandlerRef};
use session::context::QueryContextRef;
use snafu::ensure;
use sql::statements::statement::Statement;
use table::test_util::MemTable;

mod auth;
mod grpc;
mod http;
mod interceptor;
mod mysql;
mod opentsdb;
mod postgres;
mod py_script;

const LOCALHOST_WITH_0: &str = "127.0.0.1:0";

struct DummyInstance {
    query_engine: QueryEngineRef,
    py_engine: Arc<PyEngine>,
    scripts: RwLock<HashMap<String, Arc<PyScript>>>,
}

impl DummyInstance {
    fn new(query_engine: QueryEngineRef) -> Self {
        Self {
            py_engine: Arc::new(PyEngine::new(query_engine.clone())),
            scripts: RwLock::new(HashMap::new()),
            query_engine,
        }
    }
}

#[async_trait]
impl SqlQueryHandler for DummyInstance {
    type Error = Error;

    async fn do_query(&self, query: &str, query_ctx: QueryContextRef) -> Vec<Result<Output>> {
        let stmt = QueryLanguageParser::parse_sql(query).unwrap();
        let plan = self
            .query_engine
            .statement_to_plan(stmt, query_ctx)
            .unwrap();
        let output = self.query_engine.execute(&plan).await.unwrap();
        vec![Ok(output)]
    }

    async fn do_promql_query(
        &self,
        _: &PromQuery,
        _: QueryContextRef,
    ) -> Vec<std::result::Result<Output, Self::Error>> {
        unimplemented!()
    }

    async fn do_statement_query(
        &self,
        _stmt: Statement,
        _query_ctx: QueryContextRef,
    ) -> Result<Output> {
        unimplemented!()
    }

    fn do_describe(&self, stmt: Statement, query_ctx: QueryContextRef) -> Result<Option<Schema>> {
        if let Statement::Query(_) = stmt {
            let schema = self
                .query_engine
                .describe(QueryStatement::Sql(stmt), query_ctx)
                .unwrap();
            Ok(Some(schema))
        } else {
            Ok(None)
        }
    }

    fn is_valid_schema(&self, catalog: &str, schema: &str) -> Result<bool> {
        Ok(catalog == DEFAULT_CATALOG_NAME && schema == DEFAULT_SCHEMA_NAME)
    }
}

#[async_trait]
impl ScriptHandler for DummyInstance {
    async fn insert_script(&self, schema: &str, name: &str, script: &str) -> Result<()> {
        let script = self
            .py_engine
            .compile(script, CompileContext::default())
            .await
            .unwrap();
        script.register_udf();
        self.scripts
            .write()
            .unwrap()
            .insert(format!("{schema}_{name}"), Arc::new(script));

        Ok(())
    }

    async fn execute_script(
        &self,
        schema: &str,
        name: &str,
        params: HashMap<String, String>,
    ) -> Result<Output> {
        let key = format!("{schema}_{name}");

        let py_script = self.scripts.read().unwrap().get(&key).unwrap().clone();

        Ok(py_script
            .execute(params, EvalContext::default())
            .await
            .unwrap())
    }
}

#[async_trait]
impl GrpcQueryHandler for DummyInstance {
    type Error = Error;

    async fn do_query(
        &self,
        request: GreptimeRequest,
        ctx: QueryContextRef,
    ) -> std::result::Result<Output, Self::Error> {
        let output = match request {
            Request::Insert(_) => unimplemented!(),
            Request::Query(query_request) => {
                let query = query_request.query.unwrap();
                match query {
                    Query::Sql(sql) => {
                        let mut result = SqlQueryHandler::do_query(self, &sql, ctx).await;
                        ensure!(
                            result.len() == 1,
                            NotSupportedSnafu {
                                feat: "execute multiple statements in SQL query string through GRPC interface"
                            }
                        );
                        result.remove(0)?
                    }
                    Query::LogicalPlan(_) => unimplemented!(),
                }
            }
            Request::Ddl(_) => unimplemented!(),
        };
        Ok(output)
    }
}

fn create_testing_instance(table: MemTable) -> DummyInstance {
    let table_name = table.table_name().to_string();
    let table = Arc::new(table);

    let schema_provider = Arc::new(MemorySchemaProvider::new());
    let catalog_provider = Arc::new(MemoryCatalogProvider::new());
    let catalog_list = Arc::new(MemoryCatalogManager::default());
    schema_provider.register_table(table_name, table).unwrap();
    catalog_provider
        .register_schema(DEFAULT_SCHEMA_NAME.to_string(), schema_provider)
        .unwrap();
    catalog_list
        .register_catalog(DEFAULT_CATALOG_NAME.to_string(), catalog_provider)
        .unwrap();

    let factory = QueryEngineFactory::new(catalog_list);
    let query_engine = factory.query_engine();
    DummyInstance::new(query_engine)
}

fn create_testing_script_handler(table: MemTable) -> ScriptHandlerRef {
    Arc::new(create_testing_instance(table)) as _
}

fn create_testing_sql_query_handler(table: MemTable) -> ServerSqlQueryHandlerRef {
    Arc::new(create_testing_instance(table)) as _
}

fn create_testing_grpc_query_handler(table: MemTable) -> ServerGrpcQueryHandlerRef {
    Arc::new(create_testing_instance(table)) as _
}
