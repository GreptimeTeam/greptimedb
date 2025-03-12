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

//! some utils for helping with recording rule

use std::collections::BTreeSet;

use datafusion_common::tree_node::{TreeNodeRecursion, TreeNodeVisitor};
use datafusion_expr::LogicalPlan;

/// Helper to find the innermost group by expr in schema, return None if no group by expr
#[derive(Debug, Clone, Default)]
pub struct FindGroupByFinalName {
    pub group_expr_names: Option<BTreeSet<String>>,
}

impl TreeNodeVisitor<'_> for FindGroupByFinalName {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> datafusion_common::Result<TreeNodeRecursion> {
        if let LogicalPlan::Aggregate(aggregate) = node {
            self.group_expr_names = Some(
                aggregate
                    .group_expr
                    .iter()
                    .map(|e| e.name_for_alias())
                    .collect::<Result<_, _>>()?,
            );
        };

        Ok(TreeNodeRecursion::Continue)
    }

    /// deal with projection when going up with group exprs
    fn f_up(&mut self, node: &Self::Node) -> datafusion_common::Result<TreeNodeRecursion> {
        if let LogicalPlan::Projection(projection) = node {
            for expr in &projection.expr {
                if let datafusion_expr::Expr::Alias(alias) = expr {
                    // if a alias exist, replace it
                    let old_name = alias.expr.name_for_alias()?;
                    let new_name = alias.name.clone();
                    let Some(group_expr_names) = &mut self.group_expr_names else {
                        return Ok(TreeNodeRecursion::Continue);
                    };

                    if group_expr_names.contains(&old_name) {
                        group_expr_names.remove(&old_name);
                        group_expr_names.insert(new_name);
                    }
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    }
}
