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

//! prom supply the prometheus HTTP API Server compliance

use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use auth::PermissionTableTargets;
use catalog::CatalogManagerRef;
use common_query::Output;
use promql_parser::label::{MatchOp, Matcher};
use promql_parser::parser::Expr as PromqlExpr;
use query::parser::{PromQuery, QueryLanguageParser, QueryStatement};
use session::context::QueryContextRef;
use snafu::ResultExt;

use crate::error::{InvalidQuerySnafu, ParsePromQLSnafu, Result};

pub const PROMETHEUS_API_VERSION: &str = "v1";

const PROMQL_SCHEMA_MATCHERS: [&str; 2] = ["__database__", "__schema__"];

pub type PrometheusHandlerRef = Arc<dyn PrometheusHandler + Send + Sync>;

/// A Prometheus query paired with its parsed statement.
#[derive(Debug, Clone)]
pub struct ParsedPromQuery {
    query: PromQuery,
    statement: QueryStatement,
}

impl ParsedPromQuery {
    /// Parses a Prometheus query once for permission checking and execution.
    pub fn parse(query: PromQuery, query_ctx: &QueryContextRef) -> Result<Self> {
        let statement =
            QueryLanguageParser::parse_promql(&query, query_ctx).with_context(|_| {
                ParsePromQLSnafu {
                    query: query.clone(),
                }
            })?;
        Ok(Self { query, statement })
    }

    /// Returns the original query parameters.
    pub fn query(&self) -> &PromQuery {
        &self.query
    }

    /// Returns the parsed query statement.
    pub fn statement(&self) -> &QueryStatement {
        &self.statement
    }

    /// Returns the parsed PromQL expression.
    pub fn expr(&self) -> &PromqlExpr {
        let QueryStatement::Promql(eval_stmt, _) = &self.statement else {
            unreachable!("query is parsed from PromQL")
        };
        &eval_stmt.expr
    }

    pub(crate) fn update_expr(&mut self, update: impl FnOnce(&mut PromqlExpr)) {
        let QueryStatement::Promql(eval_stmt, _) = &mut self.statement else {
            unreachable!("query is parsed from PromQL")
        };
        update(&mut eval_stmt.expr);
        self.query.query = eval_stmt.expr.to_string();
    }

    /// Splits the parsed query into its original parameters and statement.
    pub fn into_parts(self) -> (PromQuery, QueryStatement) {
        (self.query, self.statement)
    }
}

/// Resolves the optional schema selector shared by Prometheus query paths.
///
/// Like the PromQL planner, the last equality matcher selects the schema.
pub fn resolve_schema_from_matchers(matchers: &[Matcher]) -> Result<Option<String>> {
    let mut schema = None;
    for matcher in matchers
        .iter()
        .filter(|matcher| PROMQL_SCHEMA_MATCHERS.contains(&matcher.name.as_str()))
    {
        if matcher.op != MatchOp::Equal || matcher.value.is_empty() {
            return Err(InvalidQuerySnafu {
                reason: format!(
                    "expected a non-empty equality matcher for '{}'",
                    matcher.name
                ),
            }
            .build());
        }
        schema = Some(matcher.value.clone());
    }
    Ok(schema)
}

#[async_trait]
pub trait PrometheusHandler {
    async fn do_query(&self, query: &PromQuery, query_ctx: QueryContextRef) -> Result<Output>;

    /// Executes a query whose statement has already been parsed.
    async fn do_query_parsed(
        &self,
        query: ParsedPromQuery,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        self.do_query(query.query(), query_ctx).await
    }

    /// Checks access to every table referenced by a batch of PromQL queries.
    ///
    /// An empty batch still checks the operation privilege.
    async fn check_query_permission(
        &self,
        queries: &[PromQuery],
        query_ctx: &QueryContextRef,
    ) -> Result<()>;

    /// Checks access without parsing the queries again.
    async fn check_query_permission_parsed(
        &self,
        queries: &[ParsedPromQuery],
        query_ctx: &QueryContextRef,
    ) -> Result<()> {
        let queries = queries
            .iter()
            .map(|query| query.query().clone())
            .collect::<Vec<_>>();
        self.check_query_permission(&queries, query_ctx).await
    }

    /// Checks access to targets resolved by Prometheus metadata APIs.
    async fn check_query_target_permission(
        &self,
        targets: PermissionTableTargets,
        query_ctx: &QueryContextRef,
    ) -> Result<()>;

    /// Removes inaccessible metric names from logical-table metadata enumeration results.
    async fn filter_metadata_metric_names(
        &self,
        metric_names: Vec<String>,
        schema: &str,
        query_ctx: &QueryContextRef,
    ) -> Result<Vec<String>>;

    /// Query metric table names by the `__name__` matchers.
    async fn query_metric_names(
        &self,
        matchers: Vec<Matcher>,
        schema: &str,
        ctx: &QueryContextRef,
    ) -> Result<Vec<String>>;

    async fn query_label_values(
        &self,
        metric: String,
        label_name: String,
        matchers: Vec<Matcher>,
        start: SystemTime,
        end: SystemTime,
        ctx: &QueryContextRef,
    ) -> Result<Vec<String>>;

    fn catalog_manager(&self) -> CatalogManagerRef;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_schema_from_matchers() {
        assert_eq!(resolve_schema_from_matchers(&[]).unwrap(), None);

        for label in ["__database__", "__schema__"] {
            let matchers = [Matcher::new(MatchOp::Equal, label, "private")];
            assert_eq!(
                resolve_schema_from_matchers(&matchers).unwrap(),
                Some("private".to_string())
            );
        }

        let matchers = [Matcher::new(
            MatchOp::Equal,
            "x_greptime_database",
            "private",
        )];
        assert_eq!(resolve_schema_from_matchers(&matchers).unwrap(), None);

        let matchers = [
            Matcher::new(MatchOp::Equal, "__database__", "private"),
            Matcher::new(MatchOp::Equal, "__schema__", "public"),
        ];
        assert_eq!(
            resolve_schema_from_matchers(&matchers).unwrap(),
            Some("public".to_string())
        );
    }

    #[test]
    fn test_resolve_schema_from_matchers_rejects_unsafe_matchers() {
        assert!(
            resolve_schema_from_matchers(&[Matcher::new(
                MatchOp::NotEqual,
                "__database__",
                "private",
            )])
            .is_err()
        );
        assert!(
            resolve_schema_from_matchers(&[Matcher::new(MatchOp::Equal, "__database__", "",)])
                .is_err()
        );
    }
}
