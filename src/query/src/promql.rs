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

pub mod error;
pub mod label_values;
pub mod planner;

use datafusion_common::tree_node::{TreeNode as _, TreeNodeRecursion};
use datafusion_expr::{Extension, LogicalPlan};
use promql::extension_plan::{
    Absent, EmptyMetric, HistogramFold, InstantManipulate, RangeManipulate, ScalarCalculate,
    SeriesDivide, SeriesNormalize, UnionDistinctOn,
};
use serde::{Deserialize, Serialize};
use session::context::QueryContextRef;
use session::hints::PROMQL_METRIC_NAMES_EXTENSION_KEY;

/// Metric tables resolved by the protocol layer for a PromQL `__name__` non-equality matcher.
///
/// Stored in the query context extension [`PROMQL_METRIC_NAMES_EXTENSION_KEY`], which is a
/// reserved key: it can only be set by the database itself, never by a client request. The
/// tables were already filtered and authorized by the caller, so both the planner and the
/// run-time permission check can rely on them without widening the caller's access.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricNameCandidates {
    /// Schema the metric tables were resolved in. It may differ from the current schema when
    /// the selector carries a `__schema__`/`__database__` matcher.
    pub schema: String,
    /// Resolved metric table names.
    pub metric_names: Vec<String>,
}

/// Encodes resolved metric tables for the query context extension.
pub fn encode_metric_name_candidates(candidates: &MetricNameCandidates) -> String {
    // Serializing these fields cannot fail.
    serde_json::to_string(candidates).unwrap_or_else(|_| {
        serde_json::to_string(&MetricNameCandidates {
            schema: candidates.schema.clone(),
            metric_names: Vec::new(),
        })
        .expect("an empty candidate list serializes")
    })
}

/// Returns the metric tables resolved for this query, if the caller resolved any.
///
/// `None` means nothing was resolved for this query, while an empty `metric_names` means the
/// matcher resolved to zero metric tables.
pub fn query_context_metric_name_candidates(
    query_ctx: &QueryContextRef,
) -> Option<MetricNameCandidates> {
    let value = query_ctx.extension(PROMQL_METRIC_NAMES_EXTENSION_KEY)?;
    serde_json::from_str(value).ok()
}

/// Returns the metric table names resolved for this query, if any.
pub fn query_context_metric_names(query_ctx: &QueryContextRef) -> Option<Vec<String>> {
    query_context_metric_name_candidates(query_ctx).map(|candidates| candidates.metric_names)
}

/// Returns true if the plan contains PromQL-specific extension plan nodes.
pub fn plan_contains_promql_extension(plan: &LogicalPlan) -> bool {
    let mut found = false;
    let _ = plan.apply(|node| {
        if is_promql_extension_plan(node) {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    });
    found
}

fn is_promql_extension_plan(plan: &LogicalPlan) -> bool {
    let LogicalPlan::Extension(Extension { node }) = plan else {
        return false;
    };

    node.as_any().is::<Absent>()
        || node.as_any().is::<EmptyMetric>()
        || node.as_any().is::<HistogramFold>()
        || node.as_any().is::<InstantManipulate>()
        || node.as_any().is::<RangeManipulate>()
        || node.as_any().is::<ScalarCalculate>()
        || node.as_any().is::<SeriesDivide>()
        || node.as_any().is::<SeriesNormalize>()
        || node.as_any().is::<UnionDistinctOn>()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion_common::DFSchema;
    use datafusion_expr::{EmptyRelation, Extension, LogicalPlanBuilder, col};
    use session::context::{QueryContext, QueryContextBuilder};

    use super::*;

    #[test]
    fn plan_contains_promql_extension_returns_true_for_promql_extension() {
        let plan = empty_metric_plan();

        assert!(plan_contains_promql_extension(&plan));
    }

    #[test]
    fn plan_contains_promql_extension_returns_true_for_nested_promql_extension() {
        let plan = LogicalPlanBuilder::from(empty_metric_plan())
            .project(vec![col("ts")])
            .unwrap()
            .build()
            .unwrap();

        assert!(plan_contains_promql_extension(&plan));
    }

    #[test]
    fn plan_contains_promql_extension_returns_false_for_non_promql_plan() {
        let plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });

        assert!(!plan_contains_promql_extension(&plan));
    }

    fn empty_metric_plan() -> LogicalPlan {
        let empty_metric = EmptyMetric::new(
            0,
            10_000,
            5_000,
            "ts".to_string(),
            "greptime_value".to_string(),
            None,
        )
        .unwrap();

        LogicalPlan::Extension(Extension {
            node: Arc::new(empty_metric),
        })
    }

    #[test]
    fn metric_names_extension_round_trips() {
        let candidates = MetricNameCandidates {
            schema: "public".to_string(),
            metric_names: vec!["cpu_user".to_string(), "cpu\"system\"".to_string()],
        };

        let context_with = |value: String| {
            let mut query_ctx = QueryContextBuilder::default().build();
            query_ctx.set_extension(PROMQL_METRIC_NAMES_EXTENSION_KEY, value);
            Arc::new(query_ctx) as QueryContextRef
        };

        let encoded = encode_metric_name_candidates(&candidates);
        let query_ctx = context_with(encoded);
        assert_eq!(
            query_context_metric_name_candidates(&query_ctx),
            Some(candidates.clone())
        );
        assert_eq!(
            query_context_metric_names(&query_ctx),
            Some(candidates.metric_names)
        );

        assert_eq!(
            query_context_metric_names(&QueryContext::arc()),
            None,
            "an absent extension means nothing was resolved"
        );
        assert_eq!(
            query_context_metric_names(&context_with("not json".to_string())),
            None,
            "a malformed extension must be ignored"
        );

        // An empty candidate set is different from an absent one: the caller resolved the
        // matcher to zero metric tables.
        let empty = context_with(encode_metric_name_candidates(&MetricNameCandidates {
            schema: "public".to_string(),
            metric_names: Vec::new(),
        }));
        assert_eq!(query_context_metric_names(&empty), Some(Vec::new()));
    }
}
