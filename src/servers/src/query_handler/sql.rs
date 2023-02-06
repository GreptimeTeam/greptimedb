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
use common_error::prelude::*;
use common_query::Output;
use query::parser::{QueryLanguage, QueryLanguageParser, QueryStatement};
use session::context::QueryContextRef;

use crate::error::{self, Result};

pub type QueryHandlerRef = Arc<dyn QueryHandler + Send + Sync>;
pub type ServerQueryHandlerRef = QueryHandlerRef;

#[async_trait]
pub trait QueryHandler {
    /// Execute a [QueryStatement].
    async fn statement_query(
        &self,
        stmt: QueryStatement,
        query_ctx: QueryContextRef,
    ) -> Result<Output>;

    /// Check if the given catalog and schema are valid.
    fn is_valid_schema(&self, catalog: &str, schema: &str) -> Result<bool>;

    async fn query(&self, query: QueryLanguage, query_ctx: QueryContextRef) -> Result<Output> {
        let stmt = QueryLanguageParser::parse(query)
            .map_err(BoxedError::new)
            .context(error::ParseQuerySnafu)?;
        self.statement_query(stmt, query_ctx)
            .await
            .map_err(BoxedError::new)
            .context(error::ExecuteQueryStatementSnafu)
    }

    /// Execute a [QueryLanguage] that may return multiple [Output]s.
    async fn query_multiple(
        &self,
        query: QueryLanguage,
        query_ctx: QueryContextRef,
    ) -> Vec<Result<Output>> {
        match query {
            QueryLanguage::Sql(_) => {
                let stmts = QueryLanguageParser::parse_multiple(query)
                    .map_err(BoxedError::new)
                    .context(error::ParseQuerySnafu);
                if let Err(e) = stmts {
                    return vec![Err(e)];
                }

                let stmts = stmts.unwrap();
                let mut outputs = Vec::with_capacity(stmts.len());
                for stmt in stmts {
                    let output = self
                        .statement_query(stmt, query_ctx.clone())
                        .await
                        .map_err(BoxedError::new)
                        .context(error::ExecuteQueryStatementSnafu);
                    outputs.push(output);
                }

                outputs
            }
            QueryLanguage::Promql(_) => vec![self.query(query, query_ctx).await],
        }
    }
}

pub struct ServerQueryHandlerAdaptor(QueryHandlerRef);

impl ServerQueryHandlerAdaptor {
    pub fn arc(handler: QueryHandlerRef) -> Arc<Self> {
        Arc::new(Self(handler))
    }
}

#[async_trait]
impl QueryHandler for ServerQueryHandlerAdaptor {
    async fn statement_query(
        &self,
        stmt: QueryStatement,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        self.0
            .statement_query(stmt, query_ctx)
            .await
            .map_err(BoxedError::new)
            .context(error::ExecuteStatementSnafu)
    }

    fn is_valid_schema(&self, catalog: &str, schema: &str) -> Result<bool> {
        self.0
            .is_valid_schema(catalog, schema)
            .map_err(BoxedError::new)
            .context(error::CheckDatabaseValiditySnafu)
    }
}
