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

use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion_common::Result as DfResult;
use datafusion_physical_expr::Distribution;
use datafusion_physical_expr::utils::map_columns_before_projection;

use crate::dist_plan::MergeScanExec;

/// This is a [`PhysicalOptimizerRule`] to pass distribution requirement to
/// [`MergeScanExec`] to avoid unnecessary shuffling.
///
/// This rule is expected to be run before [`EnforceDistribution`].
///
/// [`EnforceDistribution`]: datafusion::physical_optimizer::enforce_distribution::EnforceDistribution
/// [`MergeScanExec`]: crate::dist_plan::MergeScanExec
#[derive(Debug)]
pub struct PassDistribution;

impl PhysicalOptimizerRule for PassDistribution {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Self::do_optimize(plan, config)
    }

    fn name(&self) -> &str {
        "PassDistributionRule"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

impl PassDistribution {
    fn do_optimize(
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Start from root with no requirement
        Self::rewrite_with_distribution(plan, None)
    }

    /// Top-down rewrite that propagates distribution requirements to children.
    fn rewrite_with_distribution(
        plan: Arc<dyn ExecutionPlan>,
        current_req: Option<Distribution>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // If this is a MergeScanExec, try to apply the current requirement.
        if let Some(merge_scan) = plan.as_any().downcast_ref::<MergeScanExec>()
            && let Some(distribution) = current_req.as_ref()
            && let Some(new_plan) = merge_scan.try_with_new_distribution(distribution.clone())
        {
            // Leaf node; no children to process
            return Ok(Arc::new(new_plan) as _);
        }

        // Compute per-child requirements from the current node.
        let children = plan.children();
        if children.is_empty() {
            return Ok(plan);
        }

        let required = plan.required_input_distribution();
        let mut new_children = Vec::with_capacity(children.len());
        for (idx, child) in children.into_iter().enumerate() {
            let child_req = match required.get(idx) {
                Some(Distribution::UnspecifiedDistribution) if idx == 0 => {
                    Self::map_hash_requirement_through_projection(plan.as_ref(), &current_req)
                }
                Some(Distribution::UnspecifiedDistribution) => None,
                None => current_req.clone(),
                Some(req) => Some(req.clone()),
            };
            let new_child = Self::rewrite_with_distribution(child.clone(), child_req)?;
            new_children.push(new_child);
        }

        // Rebuild the node only if any child changed (pointer inequality)
        let unchanged = plan
            .children()
            .into_iter()
            .zip(new_children.iter())
            .all(|(old, new)| Arc::ptr_eq(old, new));
        if unchanged {
            Ok(plan)
        } else {
            plan.with_new_children(new_children)
        }
    }

    fn map_hash_requirement_through_projection(
        plan: &dyn ExecutionPlan,
        current_req: &Option<Distribution>,
    ) -> Option<Distribution> {
        let Some(Distribution::HashPartitioned(required_exprs)) = current_req else {
            return None;
        };

        let projection = plan.as_any().downcast_ref::<ProjectionExec>()?;
        let proj_exprs = projection
            .expr()
            .iter()
            .map(|expr| (Arc::clone(&expr.expr), expr.alias.clone()))
            .collect::<Vec<_>>();
        let mapped = map_columns_before_projection(required_exprs, &proj_exprs);

        (mapped.len() == required_exprs.len()).then_some(Distribution::HashPartitioned(mapped))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use async_trait::async_trait;
    use common_query::request::QueryRequest;
    use common_recordbatch::SendableRecordBatchStream;
    use datafusion::common::NullEquality;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
    use datafusion::physical_plan::{ExecutionPlanProperties, Partitioning};
    use datafusion_expr::{JoinType, LogicalPlanBuilder};
    use datafusion_physical_expr::PhysicalExpr;
    use datafusion_physical_expr::expressions::Column as PhysicalColumn;
    use session::ReadPreference;
    use session::context::QueryContext;
    use store_api::metric_engine_consts::DATA_SCHEMA_TSID_COLUMN_NAME;
    use store_api::storage::RegionId;
    use table::table_name::TableName;

    use super::*;
    use crate::error::Result as QueryResult;
    use crate::region_query::RegionQueryHandler;

    struct NoopRegionQueryHandler;

    #[async_trait]
    impl RegionQueryHandler for NoopRegionQueryHandler {
        async fn do_get(
            &self,
            _read_preference: ReadPreference,
            _request: QueryRequest,
        ) -> QueryResult<SendableRecordBatchStream> {
            unreachable!("pass distribution tests should not execute remote queries")
        }
    }

    #[test]
    fn passes_hash_requirement_through_projection_to_merge_scan() {
        let schema = test_schema();
        let left_merge_scan = Arc::new(test_merge_scan_exec(schema.clone()));
        let right_merge_scan = Arc::new(test_merge_scan_exec(schema.clone()));
        let left_projection = Arc::new(
            ProjectionExec::try_new(
                vec![
                    ProjectionExpr::new(partition_column("greptime_value", 3), "greptime_value"),
                    ProjectionExpr::new(
                        partition_column(DATA_SCHEMA_TSID_COLUMN_NAME, 1),
                        DATA_SCHEMA_TSID_COLUMN_NAME,
                    ),
                    ProjectionExpr::new(
                        partition_column("greptime_timestamp", 2),
                        "greptime_timestamp",
                    ),
                ],
                left_merge_scan,
            )
            .unwrap(),
        ) as Arc<dyn datafusion::physical_plan::ExecutionPlan>;
        let join = Arc::new(
            HashJoinExec::try_new(
                left_projection,
                right_merge_scan,
                vec![
                    (
                        partition_column(DATA_SCHEMA_TSID_COLUMN_NAME, 1),
                        partition_column(DATA_SCHEMA_TSID_COLUMN_NAME, 1),
                    ),
                    (
                        partition_column("greptime_timestamp", 2),
                        partition_column("greptime_timestamp", 2),
                    ),
                ],
                None,
                &JoinType::Inner,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNull,
                false,
            )
            .unwrap(),
        ) as Arc<dyn datafusion::physical_plan::ExecutionPlan>;

        let optimized = PassDistribution
            .optimize(join, &ConfigOptions::default())
            .unwrap();
        let hash_join = optimized.as_any().downcast_ref::<HashJoinExec>().unwrap();
        let left_projection = hash_join
            .left()
            .as_any()
            .downcast_ref::<ProjectionExec>()
            .unwrap();
        let left_partitioning = left_projection.input().output_partitioning();
        let right_partitioning = hash_join.right().output_partitioning();

        let Partitioning::Hash(left_exprs, left_count) = left_partitioning else {
            panic!("expected left merge scan hash partitioning");
        };
        let Partitioning::Hash(right_exprs, right_count) = right_partitioning else {
            panic!("expected right merge scan hash partitioning");
        };

        assert_eq!(*left_count, 32);
        assert_eq!(*right_count, 32);
        assert_eq!(
            column_names(left_exprs),
            vec![DATA_SCHEMA_TSID_COLUMN_NAME, "greptime_timestamp"]
        );
        assert_eq!(
            column_names(right_exprs),
            vec![DATA_SCHEMA_TSID_COLUMN_NAME, "greptime_timestamp"]
        );
    }

    fn test_merge_scan_exec(schema: SchemaRef) -> MergeScanExec {
        let session_state = SessionStateBuilder::new().with_default_features().build();
        let partition_cols = BTreeMap::from([
            (
                DATA_SCHEMA_TSID_COLUMN_NAME.to_string(),
                BTreeSet::from([datafusion_common::Column::from_name(
                    DATA_SCHEMA_TSID_COLUMN_NAME,
                )]),
            ),
            (
                "greptime_timestamp".to_string(),
                BTreeSet::from([datafusion_common::Column::from_name("greptime_timestamp")]),
            ),
        ]);
        let plan = LogicalPlanBuilder::empty(false).build().unwrap();

        MergeScanExec::new(
            &session_state,
            TableName::new("greptime", "public", "test"),
            vec![RegionId::new(1, 0), RegionId::new(1, 1)],
            plan,
            schema.as_ref(),
            Arc::new(NoopRegionQueryHandler),
            QueryContext::arc(),
            32,
            partition_cols,
        )
        .unwrap()
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, true),
            Field::new(DATA_SCHEMA_TSID_COLUMN_NAME, DataType::UInt64, false),
            Field::new(
                "greptime_timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("greptime_value", DataType::Float64, true),
        ]))
    }

    fn partition_column(name: &str, index: usize) -> Arc<dyn PhysicalExpr> {
        Arc::new(PhysicalColumn::new(name, index))
    }

    fn column_names(exprs: &[Arc<dyn PhysicalExpr>]) -> Vec<&str> {
        exprs
            .iter()
            .map(|expr| {
                expr.as_any()
                    .downcast_ref::<PhysicalColumn>()
                    .unwrap()
                    .name()
            })
            .collect()
    }
}
