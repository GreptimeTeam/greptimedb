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

use datafusion_common::Result;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::{LogicalPlan, SortExpr};
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};
use tokio::task_local;

use crate::dist_plan::{MergeScanLogicalPlan, MergeSortLogicalPlan};
use crate::vector_search::plan::AdaptiveVectorTopKLogicalPlan;
use crate::vector_search::utils::{extract_limit_info, is_vector_sort, is_vector_sort_exprs};

#[derive(Debug)]
/// Rewrites vector distance `Sort + Limit` queries into `AdaptiveVectorTopKLogicalPlan`.
///
/// This enables adaptive top-k execution: start with `k = fetch + skip`, then retry with
/// larger k when needed to stabilize tie groups and return correct global top-k results.
/// Non-vector sorts or plans without concrete limit info are left unchanged.
pub struct AdaptiveVectorTopKRule;

task_local! {
    // Prevent recursive rewrite when adaptive execution rebuilds a logical plan per round.
    // The rebuilt plan still contains Sort + Limit shape; without this guard, the optimizer
    // would keep wrapping it with AdaptiveVectorTopK again.
    //
    // Safety: relies on `SessionState::create_physical_plan` running entirely within the
    // current tokio task, which holds for the single-threaded planning pipeline.
    static SKIP_REWRITE: bool;
}

pub async fn with_adaptive_topk_disabled<F, T>(fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    SKIP_REWRITE.scope(true, fut).await
}

impl OptimizerRule for AdaptiveVectorTopKRule {
    fn name(&self) -> &str {
        "AdaptiveVectorTopKRule"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let skip_rewrite = SKIP_REWRITE.try_with(|flag| *flag).unwrap_or(false);
        if skip_rewrite {
            // This branch is entered only from adaptive runtime re-planning.
            // User-submitted queries should still be rewritten normally.
            return Ok(Transformed::no(plan));
        }
        plan.transform_down(&mut |plan| Self::rewrite_limit_sort(plan))
    }
}

impl AdaptiveVectorTopKRule {
    fn rewrite_limit_sort(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        let Some((input, expr, fetch, skip)) = Self::extract_adaptive_topk_target(&plan) else {
            return Ok(Transformed::no(plan));
        };
        let new_plan =
            AdaptiveVectorTopKLogicalPlan::new(input, expr, Some(fetch), skip).into_logical_plan();
        Ok(Transformed::yes(new_plan))
    }

    fn extract_adaptive_topk_target(
        plan: &LogicalPlan,
    ) -> Option<(std::sync::Arc<LogicalPlan>, Vec<SortExpr>, usize, usize)> {
        match plan {
            LogicalPlan::Limit(limit) => {
                let (input, expr) = Self::extract_vector_sort(limit.input.as_ref())?;
                let (fetch, skip) = extract_limit_info(limit)?;
                Some((input, expr, fetch, skip))
            }
            LogicalPlan::Extension(ext) => {
                let merge_sort = ext.node.as_any().downcast_ref::<MergeSortLogicalPlan>()?;
                let supports_vector =
                    is_vector_sort_exprs(merge_sort.input.as_ref(), &merge_sort.expr)
                        || Self::merge_scan_input_has_vector_sort(merge_sort.input.as_ref());
                if !supports_vector {
                    return None;
                }
                let fetch = merge_sort.fetch?;
                Some((merge_sort.input.clone(), merge_sort.expr.clone(), fetch, 0))
            }
            _ => None,
        }
    }

    fn extract_vector_sort(
        plan: &LogicalPlan,
    ) -> Option<(std::sync::Arc<LogicalPlan>, Vec<SortExpr>)> {
        match plan {
            LogicalPlan::Sort(sort) if is_vector_sort(sort) => {
                Some((sort.input.clone(), sort.expr.clone()))
            }
            LogicalPlan::Limit(limit) => {
                let LogicalPlan::Sort(sort) = limit.input.as_ref() else {
                    return None;
                };
                if !is_vector_sort(sort) {
                    return None;
                }
                Some((sort.input.clone(), sort.expr.clone()))
            }
            LogicalPlan::Extension(ext) => {
                let merge_sort = ext.node.as_any().downcast_ref::<MergeSortLogicalPlan>()?;
                (is_vector_sort_exprs(merge_sort.input.as_ref(), &merge_sort.expr)
                    || Self::merge_scan_input_has_vector_sort(merge_sort.input.as_ref()))
                .then(|| (merge_sort.input.clone(), merge_sort.expr.clone()))
            }
            _ => None,
        }
    }

    fn merge_scan_input_has_vector_sort(plan: &LogicalPlan) -> bool {
        let LogicalPlan::Extension(ext) = plan else {
            return false;
        };
        let Some(merge_scan) = ext.node.as_any().downcast_ref::<MergeScanLogicalPlan>() else {
            return false;
        };
        Self::extract_vector_sort(merge_scan.input()).is_some()
    }
}

#[cfg(all(test, feature = "vector_index"))]
mod tests {
    use std::sync::Arc;

    use api::v1::SemanticType;
    use common_function::scalars::vector::distance::VEC_L2SQ_DISTANCE;
    use datafusion::datasource::DefaultTableSource;
    use datafusion_expr::{LogicalPlan, LogicalPlanBuilder, col};
    use datafusion_optimizer::{OptimizerContext, OptimizerRule};
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::ConcreteDataType;

    use crate::dist_plan::{MergeScanLogicalPlan, MergeSortLogicalPlan};
    use crate::dummy_catalog::DummyTableProvider;
    use crate::optimizer::adaptive_vector_topk::AdaptiveVectorTopKRule;
    use crate::optimizer::scan_hint::ScanHintRule;
    use crate::optimizer::test_util::MetaRegionEngine;
    use crate::vector_search::test_utils::vec_distance_expr;

    fn build_dummy_provider(column_id: u32) -> Arc<DummyTableProvider> {
        let mut builder = RegionMetadataBuilder::new(0.into());
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("k0", ConcreteDataType::string_datatype(), true),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("v", ConcreteDataType::vector_datatype(2), false),
                semantic_type: SemanticType::Field,
                column_id,
            })
            .primary_key(vec![1]);
        let metadata = Arc::new(builder.build().unwrap());
        let engine = Arc::new(MetaRegionEngine::with_metadata(metadata.clone()));
        Arc::new(DummyTableProvider::new(0.into(), engine, metadata))
    }

    #[test]
    fn adaptive_vector_topk_rewrites_limit_sort() {
        let provider = build_dummy_provider(10);
        let source = DefaultTableSource::new(provider);

        let plan = LogicalPlanBuilder::scan_with_filters("t", Arc::new(source), None, vec![])
            .unwrap()
            .sort(vec![vec_distance_expr(VEC_L2SQ_DISTANCE).sort(true, false)])
            .unwrap()
            .limit(0, Some(5))
            .unwrap()
            .build()
            .unwrap();

        let context = OptimizerContext::new();
        let plan = ScanHintRule.rewrite(plan, &context).unwrap().data;
        let result = AdaptiveVectorTopKRule.rewrite(plan, &context).unwrap().data;
        let plan_str = result.display().to_string();
        assert!(plan_str.contains("AdaptiveVectorTopK"));
    }

    #[test]
    fn adaptive_vector_topk_does_not_rewrite_plain_sort_fetch() {
        let provider = build_dummy_provider(10);
        let source = DefaultTableSource::new(provider);

        let sort_plan = LogicalPlanBuilder::scan_with_filters("t", Arc::new(source), None, vec![])
            .unwrap()
            .sort(vec![vec_distance_expr(VEC_L2SQ_DISTANCE).sort(true, false)])
            .unwrap()
            .build()
            .unwrap();
        let LogicalPlan::Sort(mut sort) = sort_plan else {
            panic!("expected sort plan");
        };
        sort.fetch = Some(5);
        let plan = LogicalPlan::Sort(sort);

        let context = OptimizerContext::new();
        let plan = ScanHintRule.rewrite(plan, &context).unwrap().data;
        let result = AdaptiveVectorTopKRule.rewrite(plan, &context).unwrap().data;
        let plan_str = result.display().to_string();
        assert!(!plan_str.contains("AdaptiveVectorTopK"));
    }

    #[test]
    fn adaptive_vector_topk_rewrites_limit_merge_sort() {
        let provider = build_dummy_provider(10);
        let source = DefaultTableSource::new(provider);

        let sort_plan = LogicalPlanBuilder::scan_with_filters("t", Arc::new(source), None, vec![])
            .unwrap()
            .sort(vec![vec_distance_expr(VEC_L2SQ_DISTANCE).sort(true, false)])
            .unwrap()
            .build()
            .unwrap();
        let LogicalPlan::Sort(sort) = sort_plan else {
            panic!("expected sort plan");
        };

        let merge_sort =
            MergeSortLogicalPlan::new(sort.input, sort.expr, sort.fetch).into_logical_plan();
        let plan = LogicalPlanBuilder::from(merge_sort)
            .limit(0, Some(5))
            .unwrap()
            .build()
            .unwrap();

        let context = OptimizerContext::new();
        let plan = ScanHintRule.rewrite(plan, &context).unwrap().data;
        let result = AdaptiveVectorTopKRule.rewrite(plan, &context).unwrap().data;
        let plan_str = result.display().to_string();
        assert!(plan_str.contains("AdaptiveVectorTopK"));
    }

    #[test]
    fn adaptive_vector_topk_rewrites_by_alias() {
        let provider = build_dummy_provider(10);
        let source = DefaultTableSource::new(provider);

        // Case 1: MergeSort with fetch (alias "d")
        let remote_sort = LogicalPlanBuilder::scan_with_filters(
            "t",
            Arc::new(DefaultTableSource::new(build_dummy_provider(10))),
            None,
            vec![],
        )
        .unwrap()
        .project(vec![
            col("k0"),
            vec_distance_expr(VEC_L2SQ_DISTANCE).alias("d"),
        ])
        .unwrap()
        .sort(vec![col("d").sort(true, false)])
        .unwrap()
        .build()
        .unwrap();
        let merge_scan =
            MergeScanLogicalPlan::new(remote_sort, false, Default::default()).into_logical_plan();
        let merge_sort = MergeSortLogicalPlan::new(
            Arc::new(merge_scan),
            vec![col("d").sort(true, false)],
            Some(7),
        )
        .into_logical_plan();

        let context = OptimizerContext::new();
        let plan = ScanHintRule.rewrite(merge_sort, &context).unwrap().data;
        let result = AdaptiveVectorTopKRule.rewrite(plan, &context).unwrap().data;
        assert!(result.display().to_string().contains("AdaptiveVectorTopK"));

        // Case 2: Limit + MergeSort (alias "d")
        let remote_sort =
            LogicalPlanBuilder::scan_with_filters("t", Arc::new(source), None, vec![])
                .unwrap()
                .project(vec![
                    col("k0"),
                    vec_distance_expr(VEC_L2SQ_DISTANCE).alias("d"),
                ])
                .unwrap()
                .sort(vec![col("d").sort(true, false)])
                .unwrap()
                .build()
                .unwrap();
        let merge_scan =
            MergeScanLogicalPlan::new(remote_sort, false, Default::default()).into_logical_plan();
        let merge_sort =
            MergeSortLogicalPlan::new(Arc::new(merge_scan), vec![col("d").sort(true, false)], None)
                .into_logical_plan();
        let plan = LogicalPlanBuilder::from(merge_sort)
            .limit(20, Some(2))
            .unwrap()
            .build()
            .unwrap();

        let context = OptimizerContext::new();
        let plan = ScanHintRule.rewrite(plan, &context).unwrap().data;
        let result = AdaptiveVectorTopKRule.rewrite(plan, &context).unwrap().data;
        assert!(result.display().to_string().contains("AdaptiveVectorTopK"));
    }

    #[test]
    fn adaptive_vector_topk_rewrites_limit_merge_sort_by_unaliased_projection_distance() {
        let provider = build_dummy_provider(10);
        let source = DefaultTableSource::new(provider);
        let distance_expr = vec_distance_expr(VEC_L2SQ_DISTANCE);
        let projected = LogicalPlanBuilder::scan_with_filters("t", Arc::new(source), None, vec![])
            .unwrap()
            .project(vec![col("k0"), distance_expr.clone()])
            .unwrap()
            .build()
            .unwrap();
        let distance_col = projected.expressions()[1].schema_name().to_string();
        let remote_sort = LogicalPlanBuilder::from(projected)
            .sort(vec![col(distance_col).sort(true, false)])
            .unwrap()
            .build()
            .unwrap();
        let merge_scan =
            MergeScanLogicalPlan::new(remote_sort, false, Default::default()).into_logical_plan();
        let merge_sort = MergeSortLogicalPlan::new(
            Arc::new(merge_scan),
            vec![distance_expr.sort(true, false)],
            None,
        )
        .into_logical_plan();
        let plan = LogicalPlanBuilder::from(merge_sort)
            .limit(20, Some(2))
            .unwrap()
            .build()
            .unwrap();

        let context = OptimizerContext::new();
        let plan = ScanHintRule.rewrite(plan, &context).unwrap().data;
        let result = AdaptiveVectorTopKRule.rewrite(plan, &context).unwrap().data;
        let plan_str = result.display().to_string();
        assert!(plan_str.contains("AdaptiveVectorTopK"));
    }

    #[test]
    fn adaptive_vector_topk_rewrites_limit_merge_sort_when_remote_input_is_limit_sort() {
        let provider = build_dummy_provider(10);
        let source = DefaultTableSource::new(provider);
        let remote_sort =
            LogicalPlanBuilder::scan_with_filters("t", Arc::new(source), None, vec![])
                .unwrap()
                .sort(vec![vec_distance_expr(VEC_L2SQ_DISTANCE).sort(true, false)])
                .unwrap()
                .limit(0, Some(9))
                .unwrap()
                .build()
                .unwrap();

        let merge_scan =
            MergeScanLogicalPlan::new(remote_sort, false, Default::default()).into_logical_plan();
        let merge_sort = MergeSortLogicalPlan::new(
            Arc::new(merge_scan),
            vec![vec_distance_expr(VEC_L2SQ_DISTANCE).sort(true, false)],
            None,
        )
        .into_logical_plan();
        let plan = LogicalPlanBuilder::from(merge_sort)
            .limit(2, Some(3))
            .unwrap()
            .build()
            .unwrap();

        let context = OptimizerContext::new();
        let plan = ScanHintRule.rewrite(plan, &context).unwrap().data;
        let result = AdaptiveVectorTopKRule.rewrite(plan, &context).unwrap().data;
        let plan_str = result.display().to_string();
        assert!(plan_str.contains("AdaptiveVectorTopK"));
    }
}
