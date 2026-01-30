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

use async_trait::async_trait;
use common_query::Output;
use datafusion_expr::LogicalPlan;
use query::parser::PromQuery;
use query::query_engine::DescribeResult;
use session::context::QueryContextRef;
use sql::statements::statement::Statement;

use crate::error::Result;

pub type ServerSqlQueryHandlerRef = Arc<dyn SqlQueryHandler + Send + Sync>;

#[async_trait]
pub trait SqlQueryHandler {
    async fn do_query(&self, query: &str, query_ctx: QueryContextRef) -> Vec<Result<Output>>;

    async fn do_exec_plan(
        &self,
        stmt: Option<Statement>,
        plan: LogicalPlan,
        query_ctx: QueryContextRef,
    ) -> Result<Output>;

    async fn do_promql_query(
        &self,
        query: &PromQuery,
        query_ctx: QueryContextRef,
    ) -> Vec<Result<Output>>;

    async fn do_describe(
        &self,
        stmt: Statement,
        query_ctx: QueryContextRef,
    ) -> Result<Option<DescribeResult>>;

    async fn is_valid_schema(&self, catalog: &str, schema: &str) -> Result<bool>;
}
