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

use api::v1::ddl_request::Expr as DdlExpr;
use api::v1::greptime_request::Request;
use api::v1::query_request::Query;
use api::v1::{DeleteRequests, InsertRequests, RowDeleteRequests, RowInsertRequests};
use async_trait::async_trait;
use auth::{PermissionChecker, PermissionCheckerRef, PermissionReq};
use common_meta::table_name::TableName;
use common_query::Output;
use query::parser::PromQuery;
use servers::interceptor::{GrpcQueryInterceptor, GrpcQueryInterceptorRef};
use servers::query_handler::grpc::GrpcQueryHandler;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};

use crate::delete::Deleter;
use crate::error::{
    self, Error, IncompleteGrpcResultSnafu, NotSupportedSnafu, PermissionSnafu, Result,
};
use crate::insert::Inserter;
use crate::instance::Instance;

#[async_trait]
impl GrpcQueryHandler for Instance {
    type Error = Error;

    async fn do_query(&self, request: Request, ctx: QueryContextRef) -> Result<Output> {
        let interceptor_ref = self.plugins.get::<GrpcQueryInterceptorRef<Error>>();
        let interceptor = interceptor_ref.as_ref();
        interceptor.pre_execute(&request, ctx.clone())?;

        self.plugins
            .get::<PermissionCheckerRef>()
            .as_ref()
            .check_permission(ctx.current_user(), PermissionReq::GrpcRequest(&request))
            .context(PermissionSnafu)?;

        let output = match request {
            Request::Inserts(requests) => self.handle_inserts(requests, ctx.clone()).await?,
            Request::RowInserts(requests) => self.handle_row_inserts(requests, ctx.clone()).await?,
            Request::Deletes(requests) => self.handle_deletes(requests, ctx.clone()).await?,
            Request::RowDeletes(requests) => self.handle_row_deletes(requests, ctx.clone()).await?,
            Request::Query(query_request) => {
                let query = query_request.query.context(IncompleteGrpcResultSnafu {
                    err_msg: "Missing field 'QueryRequest.query'",
                })?;
                match query {
                    Query::Sql(sql) => {
                        let mut result = SqlQueryHandler::do_query(self, &sql, ctx.clone()).await;
                        ensure!(
                            result.len() == 1,
                            NotSupportedSnafu {
                                feat: "execute multiple statements in SQL query string through GRPC interface"
                            }
                        );
                        result.remove(0)?
                    }
                    Query::LogicalPlan(_) => {
                        return NotSupportedSnafu {
                            feat: "Execute LogicalPlan in Frontend",
                        }
                        .fail();
                    }
                    Query::PromRangeQuery(promql) => {
                        let prom_query = PromQuery {
                            query: promql.query,
                            start: promql.start,
                            end: promql.end,
                            step: promql.step,
                        };
                        let mut result =
                            SqlQueryHandler::do_promql_query(self, &prom_query, ctx.clone()).await;
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
            Request::Ddl(request) => {
                let expr = request.expr.context(error::UnexpectedSnafu {
                    violated: "expected expr",
                })?;

                match expr {
                    DdlExpr::CreateTable(mut expr) => {
                        // TODO(weny): supports to create multiple region table.
                        let _ = self
                            .statement_executor
                            .create_table_inner(&mut expr, None)
                            .await?;
                        Output::AffectedRows(0)
                    }
                    DdlExpr::Alter(expr) => self.statement_executor.alter_table_inner(expr).await?,
                    DdlExpr::CreateDatabase(expr) => {
                        self.statement_executor
                            .create_database(
                                ctx.current_catalog(),
                                &expr.database_name,
                                expr.create_if_not_exists,
                            )
                            .await?
                    }
                    DdlExpr::DropTable(expr) => {
                        let table_name =
                            TableName::new(&expr.catalog_name, &expr.schema_name, &expr.table_name);
                        self.statement_executor.drop_table(table_name).await?
                    }
                    DdlExpr::TruncateTable(expr) => {
                        let table_name =
                            TableName::new(&expr.catalog_name, &expr.schema_name, &expr.table_name);
                        self.statement_executor.truncate_table(table_name).await?
                    }
                }
            }
        };

        let output = interceptor.post_execute(output, ctx)?;
        Ok(output)
    }
}

impl Instance {
    pub async fn handle_inserts(
        &self,
        requests: InsertRequests,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        let inserter = Inserter::new(
            self.catalog_manager.as_ref(),
            self.statement_executor.as_ref(),
            self.region_request_handler.as_ref(),
        );
        inserter.handle_column_inserts(requests, ctx).await
    }

    pub async fn handle_row_inserts(
        &self,
        requests: RowInsertRequests,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        let inserter = Inserter::new(
            self.catalog_manager.as_ref(),
            self.statement_executor.as_ref(),
            self.region_request_handler.as_ref(),
        );
        inserter.handle_row_inserts(requests, ctx).await
    }

    pub async fn handle_deletes(
        &self,
        requests: DeleteRequests,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        let deleter = Deleter::new(
            self.catalog_manager.as_ref(),
            self.region_request_handler.as_ref(),
        );
        deleter.handle_column_deletes(requests, ctx).await
    }

    pub async fn handle_row_deletes(
        &self,
        requests: RowDeleteRequests,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        let deleter = Deleter::new(
            self.catalog_manager.as_ref(),
            self.region_request_handler.as_ref(),
        );
        deleter.handle_row_deletes(requests, ctx).await
    }
}
