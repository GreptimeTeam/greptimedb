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
}
