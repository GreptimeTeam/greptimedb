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

use sqlness::QueryContext;
use sqlness::SqlnessError;
use sqlness::interceptor::{Interceptor, InterceptorFactory, InterceptorRef};

pub const PREFIX: &str = "HINT";

/// Prefix of the keys under which the hints are stored in the [`QueryContext`].
pub const HINT_KEY_PREFIX: &str = "hint.";

/// Pass per-query hints to the next statement.
///
/// # Example
/// ```sql
/// -- SQLNESS HINT query_parallelism=2 query.enable_remote_dynamic_filter_pushdown=false
/// SELECT * FROM t;
/// ```
///
/// # Format
/// The hints are `key=value` pairs separated by spaces and are only attached to
/// the statement that directly follows the interceptor. The pairs are collected
/// from the statement's [`QueryContext`] and forwarded over the gRPC
/// `x-greptime-hints` header. Protocols that cannot carry hints reject the
/// statement instead of silently ignoring them.
#[derive(Debug)]
pub struct HintInterceptor {
    hints: Vec<(String, String)>,
}

impl Interceptor for HintInterceptor {
    fn before_execute(&self, _: &mut Vec<String>, context: &mut QueryContext) {
        for (key, value) in &self.hints {
            context
                .context
                .insert(format!("{HINT_KEY_PREFIX}{key}"), value.clone());
        }
    }
}

pub struct HintInterceptorFactory;

impl InterceptorFactory for HintInterceptorFactory {
    fn try_new(&self, ctx: &str) -> Result<InterceptorRef, SqlnessError> {
        let mut hints = Vec::new();

        for pair in ctx.split_whitespace() {
            let Some((key, value)) = pair.split_once('=') else {
                return Err(SqlnessError::InvalidContext {
                    prefix: PREFIX.to_string(),
                    msg: format!("Expected key=value pairs separated by spaces, got: {ctx}"),
                });
            };

            if key.is_empty() {
                return Err(SqlnessError::InvalidContext {
                    prefix: PREFIX.to_string(),
                    msg: format!("Hint name should not be empty, got: {ctx}"),
                });
            }

            hints.push((key.to_string(), value.to_string()));
        }

        if hints.is_empty() {
            return Err(SqlnessError::InvalidContext {
                prefix: PREFIX.to_string(),
                msg: "Expected at least one key=value pair".to_string(),
            });
        }

        Ok(Box::new(HintInterceptor { hints }))
    }
}

/// Returns the hints attached to the statement by the [`HintInterceptor`].
///
/// The pairs are sorted by hint name so that a statement always carries the
/// same hints in the same order.
pub fn hints_from_context(ctx: &QueryContext) -> Vec<(&str, &str)> {
    let mut hints: Vec<(&str, &str)> = ctx
        .context
        .iter()
        .filter_map(|(key, value)| {
            key.strip_prefix(HINT_KEY_PREFIX)
                .map(|name| (name, value.as_str()))
        })
        .collect();
    hints.sort_unstable();
    hints
}

/// Error message for a statement that declares hints over a protocol that
/// cannot carry them.
pub fn unsupported_hint_error(protocol: &str) -> String {
    format!(
        "Error: HINT is not supported with the {protocol} protocol, \
         only the default gRPC protocol forwards query hints"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_key_value_pairs() {
        let interceptor = HintInterceptorFactory
            .try_new("query_parallelism=2 query.enable_remote_dynamic_filter_pushdown=false")
            .unwrap();
        let mut context = QueryContext::default();
        interceptor.before_execute(&mut Vec::new(), &mut context);
        assert_eq!(
            context.context.get("hint.query_parallelism"),
            Some(&"2".to_string())
        );
        assert_eq!(
            context
                .context
                .get("hint.query.enable_remote_dynamic_filter_pushdown"),
            Some(&"false".to_string())
        );
    }

    #[test]
    fn hints_from_context_ignores_other_entries() {
        let interceptor = HintInterceptorFactory.try_new("b=2 a=1").unwrap();
        let mut context = QueryContext::default();
        context
            .context
            .insert("protocol".to_string(), "mysql".to_string());
        interceptor.before_execute(&mut Vec::new(), &mut context);

        assert_eq!(hints_from_context(&context), vec![("a", "1"), ("b", "2")]);
    }

    #[test]
    fn factory_rejects_malformed_pairs() {
        assert!(HintInterceptorFactory.try_new("query_parallelism").is_err());
        assert!(HintInterceptorFactory.try_new("=2").is_err());
        assert!(HintInterceptorFactory.try_new("").is_err());
    }
}
