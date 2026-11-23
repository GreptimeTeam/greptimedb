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
use datafusion_common::tree_node::Transformed;
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion_optimizer::optimizer::ApplyOrder;
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};

use crate::dist_plan::MergeScanLogicalPlan;

#[derive(Debug)]
pub(crate) struct MergeScanProjection;

impl OptimizerRule for MergeScanProjection {
    fn name(&self) -> &str {
        "merge_scan_projection"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Projection(projection) = &plan else {
            return Ok(Transformed::no(plan));
        };
        // Remote batches do not yet preserve the row count of empty projections.
        if projection.expr.is_empty()
            || !projection
                .expr
                .iter()
                .all(|expr| matches!(expr, Expr::Column(_)))
        {
            return Ok(Transformed::no(plan));
        }
        let LogicalPlan::Extension(extension) = projection.input.as_ref() else {
            return Ok(Transformed::no(plan));
        };
        let Some(scan) = extension
            .node
            .as_any()
            .downcast_ref::<MergeScanLogicalPlan>()
        else {
            return Ok(Transformed::no(plan));
        };
        if scan.is_placeholder() {
            return Ok(Transformed::no(plan));
        }
        // Project after the remote subtree, preserving its sort, limit, and aggregation scope.
        let remote = LogicalPlanBuilder::from(scan.input().clone())
            .project(projection.expr.clone())?
            .build()?;
        let mut projected =
            MergeScanLogicalPlan::new(remote, false).with_output_schema(projection.schema.clone());
        if let Some(id) = scan.remote_dyn_filter_producer_id() {
            projected = projected.with_remote_dyn_filter_producer_id(id);
        }
        Ok(Transformed::yes(projected.into_logical_plan()))
    }
}

#[cfg(test)]
mod tests {
    use datafusion_expr::{col, lit};
    use datafusion_optimizer::optimizer::OptimizerContext;

    use super::*;

    #[test]
    fn projects_remote_columns_without_changing_limit_scope() {
        let input = LogicalPlanBuilder::values(vec![vec![lit(1), lit(2)]])
            .unwrap()
            .limit(0, Some(1))
            .unwrap()
            .build()
            .unwrap();
        let producer = crate::dist_plan::RemoteDynFilterProducerId::new(42);
        let scan = MergeScanLogicalPlan::new(input.clone(), false)
            .with_remote_dyn_filter_producer_id(producer);
        let plan = LogicalPlanBuilder::from(scan.into_logical_plan())
            .project(vec![col("column2")])
            .unwrap()
            .build()
            .unwrap();
        let schema = plan.schema().clone();
        let result = MergeScanProjection
            .rewrite(plan, &OptimizerContext::new())
            .unwrap();
        assert!(result.transformed);
        assert_eq!(result.data.schema(), &schema);
        let LogicalPlan::Extension(extension) = result.data else {
            panic!("expected MergeScan")
        };
        let scan = extension
            .node
            .as_any()
            .downcast_ref::<MergeScanLogicalPlan>()
            .unwrap();
        assert_eq!(scan.remote_dyn_filter_producer_id(), Some(producer));
        let LogicalPlan::Projection(projection) = scan.input() else {
            panic!("expected remote projection")
        };
        assert_eq!(projection.expr, vec![col("column2")]);
        assert_eq!(*projection.input, input);
    }

    #[test]
    fn keeps_computed_expressions_local() {
        let input = LogicalPlanBuilder::values(vec![vec![lit(1), lit(2)]])
            .unwrap()
            .build()
            .unwrap();
        let plan =
            LogicalPlanBuilder::from(MergeScanLogicalPlan::new(input, false).into_logical_plan())
                .project(vec![col("column2") + lit(1)])
                .unwrap()
                .build()
                .unwrap();
        let result = MergeScanProjection
            .rewrite(plan.clone(), &OptimizerContext::new())
            .unwrap();
        assert!(!result.transformed);
        assert_eq!(result.data, plan);
    }
}
