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

use common_query::DfPhysicalPlanRef;
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::Result as DfResult;
/// This is [PhysicalOptimizerRule] to eliminate unneeded [SeriesDivideExec] nodes.
///
/// A [SeriesDivideExec] can be eliminated when one of the following conditions is met:
/// - No upper plan will depend on the time series distribution.
/// - No child plan will change the time series distribution.
///
/// "Time Series Distribution" means the data batch in [RecordBatchStream] is divided by
/// time series. One batch only contains data of one time series and all data of one time
/// series is in one batch.
///
/// **Pre-requirement**: storage engine returns data in time series distribution. This is
/// the current behavior of mito engine.
pub struct RemoveDuplicate;

impl PhysicalOptimizerRule for RemoveDuplicate {
    fn optimize(
        &self,
        plan: DfPhysicalPlanRef,
        _config: &ConfigOptions,
    ) -> DfResult<DfPhysicalPlanRef> {
        Self::do_optimize(plan)
    }

    fn name(&self) -> &str {
        "RemoveDuplicateRule"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

impl RemoveDuplicate {
    fn do_optimize(plan: DfPhysicalPlanRef) -> DfResult<DfPhysicalPlanRef> {
        let result = plan
            .transform_down_mut(&mut |plan| {
                if plan.as_any().is::<CoalesceBatchesExec>()
                    || plan.as_any().is::<RepartitionExec>()
                {
                    // check child
                    let child = plan.children()[0].clone();
                    if child.as_any().type_id() == plan.as_any().type_id() {
                        // remove child
                        let grand_child = child.children()[0].clone();
                        let new_plan = plan.with_new_children(vec![grand_child])?;
                        return Ok(Transformed::yes(new_plan));
                    }
                }

                Ok(Transformed::no(plan))
            })?
            .data;

        Ok(result)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_schema::Schema;
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion_physical_expr::Partitioning;

    use super::*;

    #[test]
    fn remove_coalesce_batches() {
        let empty = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        let coalesce_batches = Arc::new(CoalesceBatchesExec::new(empty, 1024));
        let another_coalesce_batches = Arc::new(CoalesceBatchesExec::new(coalesce_batches, 8192));

        let optimized = RemoveDuplicate::do_optimize(another_coalesce_batches).unwrap();
        let formatted = displayable(optimized.as_ref()).indent(true).to_string();
        let expected = "CoalesceBatchesExec: target_batch_size=8192\
        \n  EmptyExec\n";

        assert_eq!(expected, formatted);
    }

    #[test]
    fn non_continuous_coalesce_batches() {
        let empty = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        let coalesce_batches = Arc::new(CoalesceBatchesExec::new(empty, 1024));
        let repartition = Arc::new(
            RepartitionExec::try_new(coalesce_batches, Partitioning::UnknownPartitioning(1))
                .unwrap(),
        );
        let another_coalesce_batches = Arc::new(CoalesceBatchesExec::new(repartition, 8192));

        let optimized = RemoveDuplicate::do_optimize(another_coalesce_batches).unwrap();
        let formatted = displayable(optimized.as_ref()).indent(true).to_string();
        let expected = "CoalesceBatchesExec: target_batch_size=8192\
        \n  RepartitionExec: partitioning=UnknownPartitioning(1), input_partitions=1\
        \n    CoalesceBatchesExec: target_batch_size=1024\
        \n      EmptyExec\n";

        assert_eq!(expected, formatted);
    }
}
