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

use crate::physical_plan::PhysicalPlan;

/// Trait that implements the [Visitor
/// pattern](https://en.wikipedia.org/wiki/Visitor_pattern) for a
/// depth first walk of `PhysicalPlan` nodes. `pre_visit` is called
/// before any children are visited, and then `post_visit` is called
/// after all children have been visited.
///
/// To use, define a struct that implements this trait and then invoke
/// ['accept'].
///
/// For example, for an physical plan that looks like:
///
/// ```text
/// ProjectionExec: id
///    FilterExec: state = CO
///       CsvExec:
/// ```
///
/// The sequence of visit operations would be:
/// ```text
/// visitor.pre_visit(ProjectionExec)
/// visitor.pre_visit(FilterExec)
/// visitor.pre_visit(CsvExec)
/// visitor.post_visit(CsvExec)
/// visitor.post_visit(FilterExec)
/// visitor.post_visit(ProjectionExec)
/// ```
pub trait PhysicalPlanVisitor {
    /// The type of error returned by this visitor.
    type Error;

    /// Invoked on an `PhysicalPlan` plan before any of its child
    /// inputs have been visited. If Ok(true) is returned, the
    /// recursion continues. If Err(..) or Ok(false) are returned, the
    /// recursion stops immediately and the error, if any, is returned
    /// to `accept`.
    fn pre_visit(&mut self, plan: &dyn PhysicalPlan) -> Result<bool, Self::Error>;

    /// Invoked on an `PhysicalPlan` plan *after* all of its child
    /// inputs have been visited. The return value is handled the same
    /// as the return value of `pre_visit`. The provided default
    /// implementation returns `Ok(true)`.
    fn post_visit(&mut self, _plan: &dyn PhysicalPlan) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

/// Recursively calls `pre_visit` and `post_visit` for this node and
/// all of its children, as described on [`PhysicalPlanVisitor`]
pub fn visit_physical_plan<V: PhysicalPlanVisitor>(
    plan: &dyn PhysicalPlan,
    visitor: &mut V,
) -> Result<(), V::Error> {
    visitor.pre_visit(plan)?;

    for child in plan.children() {
        visit_physical_plan(child.as_ref(), visitor)?;
    }

    visitor.post_visit(plan)?;

    Ok(())
}
