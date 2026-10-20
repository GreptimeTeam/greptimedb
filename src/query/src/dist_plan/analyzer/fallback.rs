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

//! Fallback dist plan analyzer, which will only push down table scan node
//! This is used when `PlanRewriter` produce errors when trying to rewrite the plan
//! This is a temporary solution, and will be removed once we have a more robust plan rewriter
//!

use datafusion_common::Result as DfResult;
use datafusion_common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion_expr::LogicalPlan;

use crate::dist_plan::MergeScanLogicalPlan;

/// FallbackPlanRewriter is a plan rewriter that will only push down table scan node
/// This is used when `PlanRewriter` produce errors when trying to rewrite the plan
/// This is a temporary solution, and will be removed once we have a more robust plan rewriter
/// It will traverse the logical plan and rewrite table scan node to merge scan node
#[derive(Debug, Clone, Default)]
pub struct FallbackPlanRewriter;

impl TreeNodeRewriter for FallbackPlanRewriter {
    type Node = LogicalPlan;

    fn f_down(
        &mut self,
        plan: Self::Node,
    ) -> DfResult<datafusion_common::tree_node::Transformed<Self::Node>> {
        if matches!(plan, LogicalPlan::TableScan(_)) {
            let node = MergeScanLogicalPlan::new(plan, false).into_logical_plan();
            Ok(Transformed::yes(node))
        } else {
            Ok(Transformed::no(plan))
        }
    }
}
