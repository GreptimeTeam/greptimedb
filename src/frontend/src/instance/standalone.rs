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
use common_error::prelude::BoxedError;
use common_query::Output;
use datanode::error::Error as DatanodeError;
use datatypes::schema::Schema;
use query::parser::QueryStatement;
use servers::error as server_error;
use servers::query_handler::grpc::{GrpcQueryHandler, GrpcQueryHandlerRef};
use servers::query_handler::sql::{QueryHandler, QueryHandlerRef};
use session::context::QueryContextRef;
use snafu::ResultExt;

use crate::error::{self, Result};

pub(crate) struct StandaloneSqlQueryHandler(QueryHandlerRef);

#[async_trait]
impl QueryHandler for StandaloneSqlQueryHandler {
    async fn statement_query(
        &self,
        stmt: QueryStatement,
        query_ctx: QueryContextRef,
    ) -> server_error::Result<Output> {
        self.0
            .statement_query(stmt, query_ctx)
            .await
            .map_err(BoxedError::new)
            .context(server_error::ExecuteQueryStatementSnafu)
    }

    fn is_valid_schema(&self, catalog: &str, schema: &str) -> server_error::Result<bool> {
        self.0.is_valid_schema(catalog, schema)
    }

    fn describe(
        &self,
        stmt: QueryStatement,
        query_ctx: QueryContextRef,
    ) -> server_error::Result<Option<Schema>> {
        self.0.describe(stmt, query_ctx)
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
