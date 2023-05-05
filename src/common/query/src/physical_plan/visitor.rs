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

pub trait PhysicalPlanVisitor {
    type Error;

    fn pre_visit(&mut self, plan: &dyn PhysicalPlan) -> Result<bool, Self::Error>;

    fn post_visit(&mut self, _plan: &dyn PhysicalPlan) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

pub fn visit_execution_plan<V: PhysicalPlanVisitor>(
    plan: &dyn PhysicalPlan,
    visitor: &mut V,
) -> Result<(), V::Error> {
    visitor.pre_visit(plan)?;

    for child in plan.children() {
        visit_execution_plan(child.as_ref(), visitor)?;
    }

    visitor.post_visit(plan)?;

    Ok(())
}

pub fn accept<V: PhysicalPlanVisitor>(
    plan: &dyn PhysicalPlan,
    visitor: &mut V,
) -> Result<(), V::Error> {
    visitor.pre_visit(plan)?;

    for child in plan.children() {
        visit_execution_plan(child.as_ref(), visitor)?;
    }

    visitor.post_visit(plan)?;

    Ok(())
}
