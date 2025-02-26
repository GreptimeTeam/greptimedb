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
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::Result as DfResult;

/// This is [PhysicalOptimizerRule] to remove duplicate physical plans such as two
/// adjoining [CoalesceBatchesExec] or [RepartitionExec]. They won't have any effect
/// if one runs right after another.
///
/// This rule is expected to be run in the final stage of the optimization process.
#[derive(Debug)]
pub struct RemoveDuplicate;

impl PhysicalOptimizerRule for RemoveDuplicate {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
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
    fn do_optimize(plan: Arc<dyn ExecutionPlan>) -> DfResult<Arc<dyn ExecutionPlan>> {
        let result = plan
            .transform_down(|plan| {
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
