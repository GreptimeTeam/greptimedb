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

use api::v1::greptime_request::Request;
use api::v1::query_request::Query;
use async_trait::async_trait;
use common_query::Output;
use query::parser::PromQuery;
use servers::query_handler::grpc::GrpcQueryHandler;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt};

use crate::error::{self, Result};
use crate::instance::Instance;

#[async_trait]
impl GrpcQueryHandler for Instance {
    type Error = error::Error;

    async fn do_query(&self, request: Request, ctx: QueryContextRef) -> Result<Output> {
        let output = match request {
            Request::Insert(request) => self.handle_insert(request, ctx).await?,
            Request::Query(query_request) => {
                let query = query_request
                    .query
                    .context(error::IncompleteGrpcResultSnafu {
                        err_msg: "Missing field 'QueryRequest.query'",
                    })?;
                match query {
                    Query::Sql(sql) => {
                        let mut result = SqlQueryHandler::do_query(self, &sql, ctx).await;
                        ensure!(
                            result.len() == 1,
                            error::NotSupportedSnafu {
                                feat: "execute multiple statements in SQL query string through GRPC interface"
                            }
                        );
                        result.remove(0)?
                    }
                    Query::LogicalPlan(_) => {
                        return error::NotSupportedSnafu {
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
                            SqlQueryHandler::do_promql_query(self, &prom_query, ctx).await;
                        ensure!(
                            result.len() == 1,
                            error::NotSupportedSnafu {
                                feat: "execute multiple statements in PromQL query string through GRPC interface"
                            }
                        );
                        result.remove(0)?
                    }
                }
            }
            Request::Ddl(_) | Request::Delete(_) => {
                GrpcQueryHandler::do_query(self.grpc_query_handler.as_ref(), request, ctx).await?
            }
        };
        Ok(output)
    }
}
