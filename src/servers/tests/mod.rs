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
use api::v1::query_request::Query;
use async_trait::async_trait;
use catalog::memory::MemoryCatalogManager;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_error::ext::BoxedError;
use common_grpc::flight::do_put::DoPutResponse;
use common_query::Output;
use datafusion_expr::LogicalPlan;
use futures_util::TryFutureExt;
use query::options::QueryOptions;
use query::parser::{PromQuery, QueryStatement};
use query::query_engine::DescribeResult;
use query::{QueryEngineFactory, QueryEngineRef};
use servers::error::{ExecuteQuerySnafu, NotSupportedSnafu, Result};
use servers::query_handler::grpc::GrpcQueryHandler;
use servers::query_handler::sql::{ServerSqlQueryHandlerRef, SqlQueryHandler};
use session::context::QueryContextRef;
use snafu::{ResultExt, ensure};
use sql::parser::{ParseOptions, ParserContext};
use sql::statements::statement::Statement;
use table::TableRef;

mod http;
mod interceptor;
mod mysql;
mod postgres;

pub struct DummyInstance {
    query_engine: QueryEngineRef,
}

impl DummyInstance {
    fn new(query_engine: QueryEngineRef) -> Self {
        Self { query_engine }
    }
}

#[async_trait]
impl SqlQueryHandler for DummyInstance {
    async fn do_query(&self, query: &str, query_ctx: QueryContextRef) -> Vec<Result<Output>> {
        let mut results = vec![];

        let statements = ParserContext::create_with_dialect(
            query,
            query_ctx.sql_dialect(),
            ParseOptions::default(),
        )
        .map(|x| x.into_iter().map(QueryStatement::Sql).collect::<Vec<_>>())
        .unwrap();

        for statement in &statements {
            let result = self
                .query_engine
                .planner()
                .plan(statement, query_ctx.clone())
                .and_then(|plan| self.query_engine.execute(plan, query_ctx.clone()))
                .await
                .map_err(BoxedError::new)
                .context(ExecuteQuerySnafu);
            results.push(result);
        }

        results
    }

    async fn do_exec_plan(
        &self,
        plan: LogicalPlan,
        _stmt: Option<Statement>,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        Ok(self.query_engine.execute(plan, query_ctx).await.unwrap())
    }

    async fn do_promql_query(&self, _: &PromQuery, _: QueryContextRef) -> Vec<Result<Output>> {
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
                .plan(&QueryStatement::Sql(stmt), query_ctx.clone())
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
impl GrpcQueryHandler for DummyInstance {
    async fn do_query(&self, request: Request, ctx: QueryContextRef) -> Result<Output> {
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
                    Query::LogicalPlan(_) | Query::InsertIntoPlan(_) => unimplemented!(),
                    Query::PromRangeQuery(promql) => {
                        let prom_query = PromQuery {
                            query: promql.query,
                            start: promql.start,
                            end: promql.end,
                            step: promql.step,
                            lookback: promql.lookback,
                            alias: None,
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

    fn handle_put_record_batch_stream(
        &self,
        _stream: servers::grpc::flight::PutRecordBatchRequestStream,
        _ctx: QueryContextRef,
    ) -> std::pin::Pin<Box<dyn futures::Stream<Item = Result<DoPutResponse>> + Send>> {
        unimplemented!()
    }
}

fn create_testing_instance(table: TableRef) -> DummyInstance {
    let catalog_manager = MemoryCatalogManager::new_with_table(table);
    let query_engine = QueryEngineFactory::new(
        catalog_manager,
        None,
        None,
        None,
        None,
        false,
        QueryOptions::default(),
    )
    .query_engine();
    DummyInstance::new(query_engine)
}

fn create_testing_sql_query_handler(table: TableRef) -> ServerSqlQueryHandlerRef {
    Arc::new(create_testing_instance(table)) as _
}
