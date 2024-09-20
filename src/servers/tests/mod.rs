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

use api::v1::greptime_request::Request;
use api::v1::query_request::Query;
use async_trait::async_trait;
use catalog::memory::MemoryCatalogManager;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_query::Output;
use datafusion_expr::LogicalPlan;
use query::parser::{PromQuery, QueryLanguageParser, QueryStatement};
use query::query_engine::DescribeResult;
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
use table::TableRef;

mod grpc;
mod http;
mod interceptor;
mod mysql;
mod postgres;
mod py_script;

const LOCALHOST_WITH_0: &str = "127.0.0.1:0";

pub struct DummyInstance {
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
        let stmt = QueryLanguageParser::parse_sql(query, &query_ctx).unwrap();
        let plan = self
            .query_engine
            .planner()
            .plan(stmt, query_ctx.clone())
            .await
            .unwrap();
        let output = self.query_engine.execute(plan, query_ctx).await.unwrap();
        vec![Ok(output)]
    }

    async fn do_exec_plan(&self, plan: LogicalPlan, query_ctx: QueryContextRef) -> Result<Output> {
        Ok(self.query_engine.execute(plan, query_ctx).await.unwrap())
    }

    async fn do_promql_query(
        &self,
        _: &PromQuery,
        _: QueryContextRef,
    ) -> Vec<std::result::Result<Output, Self::Error>> {
        unimplemented!()
    }

    async fn do_describe(
        &self,
        stmt: Statement,
        query_ctx: QueryContextRef,
    ) -> Result<Option<DescribeResult>> {
        if let Statement::Query(_) = stmt {
            let plan = self
                .query_engine
                .planner()
                .plan(QueryStatement::Sql(stmt), query_ctx.clone())
                .await
                .unwrap();
            let schema = self.query_engine.describe(plan, query_ctx).await.unwrap();
            Ok(Some(schema))
        } else {
            Ok(None)
        }
    }

    async fn is_valid_schema(&self, catalog: &str, schema: &str) -> Result<bool> {
        Ok(catalog == DEFAULT_CATALOG_NAME && schema == DEFAULT_SCHEMA_NAME)
    }
}

#[async_trait]
impl ScriptHandler for DummyInstance {
    async fn insert_script(
        &self,
        query_ctx: QueryContextRef,
        name: &str,
        script: &str,
    ) -> Result<()> {
        let catalog = query_ctx.current_catalog();
        let schema = query_ctx.current_schema();

        let script = self
            .py_engine
            .compile(script, CompileContext::default())
            .await
            .unwrap();
        script.register_udf().await;
        let _ = self
            .scripts
            .write()
            .unwrap()
            .insert(format!("{catalog}_{schema}_{name}"), Arc::new(script));

        Ok(())
    }

    async fn execute_script(
        &self,
        query_ctx: QueryContextRef,
        name: &str,
        params: HashMap<String, String>,
    ) -> Result<Output> {
        let catalog = query_ctx.current_catalog();
        let schema = query_ctx.current_schema();
        let key = format!("{catalog}_{schema}_{name}");

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
        request: Request,
        ctx: QueryContextRef,
    ) -> std::result::Result<Output, Self::Error> {
        let output = match request {
            Request::Inserts(_)
            | Request::Deletes(_)
            | Request::RowInserts(_)
            | Request::RowDeletes(_) => unimplemented!(),
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
                    Query::PromRangeQuery(promql) => {
                        let prom_query = PromQuery {
                            query: promql.query,
                            start: promql.start,
                            end: promql.end,
                            step: promql.step,
                            lookback: promql.lookback,
                        };
                        let mut result =
                            SqlQueryHandler::do_promql_query(self, &prom_query, ctx).await;
                        ensure!(
                            result.len() == 1,
                            NotSupportedSnafu {
                                feat: "execute multiple statements in PromQL query string through GRPC interface"
                            }
                        );
                        result.remove(0)?
                    }
                }
            }
            Request::Ddl(_) => unimplemented!(),
        };
        Ok(output)
    }
}

fn create_testing_instance(table: TableRef) -> DummyInstance {
    let catalog_manager = MemoryCatalogManager::new_with_table(table);
    let query_engine =
        QueryEngineFactory::new(catalog_manager, None, None, None, None, false).query_engine();
    DummyInstance::new(query_engine)
}

fn create_testing_script_handler(table: TableRef) -> ScriptHandlerRef {
    Arc::new(create_testing_instance(table)) as _
}

fn create_testing_sql_query_handler(table: TableRef) -> ServerSqlQueryHandlerRef {
    Arc::new(create_testing_instance(table)) as _
}

fn create_testing_grpc_query_handler(table: TableRef) -> ServerGrpcQueryHandlerRef {
    Arc::new(create_testing_instance(table)) as _
}
