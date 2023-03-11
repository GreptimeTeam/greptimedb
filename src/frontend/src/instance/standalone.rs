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

use api::v1::greptime_request::Request as GreptimeRequest;
use async_trait::async_trait;
use common_query::Output;
use datanode::error::Error as DatanodeError;
use datatypes::schema::Schema;
use query::parser::PromQuery;
use query::plan::LogicalPlan;
use servers::query_handler::grpc::{GrpcQueryHandler, GrpcQueryHandlerRef};
use servers::query_handler::sql::{SqlQueryHandler, SqlQueryHandlerRef};
use session::context::QueryContextRef;
use snafu::ResultExt;
use sql::statements::statement::Statement;

use crate::error::{self, Result};

pub(crate) struct StandaloneSqlQueryHandler(SqlQueryHandlerRef<DatanodeError>);

impl StandaloneSqlQueryHandler {
    pub(crate) fn arc(handler: SqlQueryHandlerRef<DatanodeError>) -> Arc<Self> {
        Arc::new(Self(handler))
    }
}

#[async_trait]
impl SqlQueryHandler for StandaloneSqlQueryHandler {
    type Error = error::Error;

    async fn do_query(&self, query: &str, query_ctx: QueryContextRef) -> Vec<Result<Output>> {
        self.0
            .do_query(query, query_ctx)
            .await
            .into_iter()
            .map(|x| x.context(error::InvokeDatanodeSnafu))
            .collect()
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
        stmt: Statement,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        self.0
            .do_statement_query(stmt, query_ctx)
            .await
            .context(error::InvokeDatanodeSnafu)
    }

    async fn do_describe(
        &self,
        stmt: Statement,
        query_ctx: QueryContextRef,
    ) -> Result<Option<(Schema, LogicalPlan)>> {
        self.0
            .do_describe(stmt, query_ctx)
            .await
            .context(error::InvokeDatanodeSnafu)
    }

    fn is_valid_schema(&self, catalog: &str, schema: &str) -> Result<bool> {
        self.0
            .is_valid_schema(catalog, schema)
            .context(error::InvokeDatanodeSnafu)
    }
}

pub(crate) struct StandaloneGrpcQueryHandler(GrpcQueryHandlerRef<DatanodeError>);

impl StandaloneGrpcQueryHandler {
    pub(crate) fn arc(handler: GrpcQueryHandlerRef<DatanodeError>) -> Arc<Self> {
        Arc::new(Self(handler))
    }
}

#[async_trait]
impl GrpcQueryHandler for StandaloneGrpcQueryHandler {
    type Error = error::Error;

    async fn do_query(&self, query: GreptimeRequest, ctx: QueryContextRef) -> Result<Output> {
        self.0
            .do_query(query, ctx)
            .await
            .context(error::InvokeDatanodeSnafu)
    }
}
