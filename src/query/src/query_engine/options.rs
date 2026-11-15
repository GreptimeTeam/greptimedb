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

use std::collections::HashMap;

use datafusion::optimizer::analyzer::AnalyzerRule;
use session::context::QueryContextRef;
use session::query_options;
use snafu::ensure;

use crate::dist_plan::{DistPlannerAnalyzer, DistPlannerOptions};
use crate::error::{QueryAccessDeniedSnafu, Result};
use crate::options;
use crate::query_engine::QueryEngineContext;

#[derive(Default, Clone)]
pub struct QueryOptions {
    pub disallow_cross_catalog_query: bool,
}

/// DataFusion optimizer option whose effective default depends on distributed planning.
pub const FILTER_NULL_JOIN_KEYS: &str = "datafusion.optimizer.filter_null_join_keys";

/// Returns effective public query-option values for SHOW and hint discovery.
/// Session overrides are retained; a fresh context yields the engine defaults.
pub fn effective_query_options(context: &QueryEngineContext) -> Result<HashMap<String, String>> {
    let config = context.state().config_options();
    let mut values: HashMap<_, _> = config
        .entries()
        .into_iter()
        .filter(|entry| {
            entry
                .key
                .starts_with(query_options::DATAFUSION_OPTIMIZER_PREFIX)
        })
        .filter_map(|entry| entry.value.map(|value| (entry.key, value)))
        .collect();
    for name in query_options::QUERY_OPTIONS
        .iter()
        .map(|option| option.name)
        .chain(std::iter::once(FILTER_NULL_JOIN_KEYS))
    {
        if let Some(value) = effective_query_option(context, name)? {
            values.insert(name.to_string(), value);
        }
    }
    Ok(values)
}

/// Returns one canonical option without evaluating unrelated query settings.
pub fn effective_query_option(context: &QueryEngineContext, name: &str) -> Result<Option<String>> {
    let value = match name {
        query_options::QUERY_PARALLELISM => {
            context.state().config().target_partitions().to_string()
        }
        query_options::QUERY_ALLOW_QUERY_FALLBACK => context
            .state()
            .config_options()
            .extensions
            .get::<DistPlannerOptions>()
            .is_some_and(|options| options.allow_query_fallback)
            .to_string(),
        query_options::QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN => {
            // SHOW reads the request override; discovery supplies a hint-free context.
            options::remote_dyn_filter_pushdown_enabled_from_extensions(
                &context.query_ctx().extensions(),
            )?
            .to_string()
        }
        FILTER_NULL_JOIN_KEYS
            if context
                .query_ctx()
                .extension(FILTER_NULL_JOIN_KEYS)
                .is_none()
                && context
                    .state()
                    .analyzer()
                    .rules
                    .iter()
                    .any(|rule| rule.name() == DistPlannerAnalyzer.name()) =>
        {
            true.to_string()
        }
        _ => {
            return Ok(context
                .state()
                .config_options()
                .entries()
                .into_iter()
                .find(|entry| entry.key == name)
                .and_then(|entry| entry.value));
        }
    };
    Ok(Some(value))
}

// TODO(shuiyisong): remove one method after #559 is done
pub fn validate_catalog_and_schema(
    catalog: &str,
    schema: &str,
    query_ctx: &QueryContextRef,
) -> Result<()> {
    ensure!(
        catalog == query_ctx.current_catalog(),
        QueryAccessDeniedSnafu {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
        }
    );

    Ok(())
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use session::context::QueryContext;

    use super::*;

    #[tokio::test]
    async fn test_effective_query_options_preserves_overrides() {
        for distributed in [false, true] {
            let engine = crate::query_engine::QueryEngineFactory::new(
                catalog::memory::new_memory_catalog_manager().unwrap(),
                None,
                None,
                None,
                None,
                distributed,
                crate::options::QueryOptions::default(),
            )
            .query_engine();
            for explicit in [None, Some(false), Some(true)] {
                let mut query_ctx = QueryContext::with("greptime", "public");
                if let Some(value) = explicit {
                    query_ctx.set_extension(FILTER_NULL_JOIN_KEYS, value.to_string());
                }
                let context = engine.engine_context(Arc::new(query_ctx)).unwrap();
                let values = effective_query_options(&context).unwrap();
                assert_eq!(
                    values[FILTER_NULL_JOIN_KEYS],
                    explicit.unwrap_or(distributed).to_string(),
                    "distributed={distributed}, explicit={explicit:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_effective_query_option_does_not_evaluate_unrelated_values() {
        let engine = crate::query_engine::QueryEngineFactory::new(
            catalog::memory::new_memory_catalog_manager().unwrap(),
            None,
            None,
            None,
            None,
            false,
            crate::options::QueryOptions {
                parallelism: 7,
                ..Default::default()
            },
        )
        .query_engine();
        let mut query_ctx = QueryContext::with("greptime", "public");
        query_ctx.set_extension(
            query_options::QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN,
            "invalid",
        );
        // Construct an internal context directly: the public engine boundary validates
        // every query option before reaching this accessor.
        let context =
            QueryEngineContext::new(engine.engine_state().session_state(), Arc::new(query_ctx));
        assert_eq!(
            effective_query_option(&context, query_options::QUERY_PARALLELISM)
                .unwrap()
                .as_deref(),
            Some("7")
        );
        assert!(
            effective_query_option(&context, FILTER_NULL_JOIN_KEYS)
                .unwrap()
                .is_some()
        );
        assert!(
            effective_query_option(
                &context,
                query_options::QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN
            )
            .is_err()
        );
    }

    #[test]
    fn test_validate_catalog_and_schema() {
        let context = Arc::new(QueryContext::with("greptime", "public"));

        validate_catalog_and_schema("greptime", "public", &context).unwrap();
        let re = validate_catalog_and_schema("greptime", "private_schema", &context);
        assert!(re.is_ok());
        let re = validate_catalog_and_schema("wrong_catalog", "public", &context);
        assert!(re.is_err());
        let re = validate_catalog_and_schema("wrong_catalog", "wrong_schema", &context);
        assert!(re.is_err());

        validate_catalog_and_schema("greptime", "information_schema", &context).unwrap();
    }
}
