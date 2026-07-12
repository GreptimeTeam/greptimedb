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
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion_common::Result as DfResult;
use datafusion_common::tree_node::{Transformed, TreeNode};

/// This is [PhysicalOptimizerRule] to remove duplicate physical plans such as two
/// adjoining [RepartitionExec]. They won't have any effect
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
                if plan.as_any().is::<RepartitionExec>() {
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
