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
use query::parser::PromQuery;
use session::context::QueryContextRef;

use crate::error::{InvalidQuerySnafu, Result};

pub const PROMETHEUS_API_VERSION: &str = "v1";

const PROMQL_SCHEMA_MATCHERS: [&str; 2] = ["__database__", "__schema__"];

pub type PrometheusHandlerRef = Arc<dyn PrometheusHandler + Send + Sync>;

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

    /// Checks access to every table referenced by a batch of PromQL queries.
    ///
    /// An empty batch still checks the operation privilege.
    async fn check_query_permission(
        &self,
        queries: &[PromQuery],
        query_ctx: &QueryContextRef,
    ) -> Result<()>;

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
