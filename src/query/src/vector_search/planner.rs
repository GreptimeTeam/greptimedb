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

//! Extension planner for adaptive vector top-k logical plan.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::DataFusionError;
use datafusion_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion_physical_expr::create_physical_sort_expr;

use crate::vector_search::exec::AdaptiveVectorTopKExec;
use crate::vector_search::plan::AdaptiveVectorTopKLogicalPlan;

/// Builds physical exec for `AdaptiveVectorTopKLogicalPlan`.
///
/// It converts logical sort expressions into physical sort expressions and
/// preserves the logical input for adaptive re-planning in execution rounds.
pub struct AdaptiveVectorTopKPlanner;

#[async_trait]
impl ExtensionPlanner for AdaptiveVectorTopKPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // Ignore unrelated extension nodes and let other extension planners handle them.
        let Some(topk) = node
            .as_any()
            .downcast_ref::<AdaptiveVectorTopKLogicalPlan>()
        else {
            return Ok(None);
        };

        // AdaptiveVectorTopKLogicalPlan is unary. Enforce exactly one input here so
        // future planner wiring bugs fail fast instead of silently dropping inputs.
        if logical_inputs.len() != 1 || physical_inputs.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "AdaptiveVectorTopKPlanner expects exactly one input, logical_inputs={}, physical_inputs={}",
                logical_inputs.len(),
                physical_inputs.len()
            )));
        }
        let logical_input = logical_inputs[0];
        let plan = physical_inputs[0].clone();
        let exprs = topk
            .expr
            .iter()
            .map(|expr| {
                create_physical_sort_expr(
                    expr,
                    logical_input.schema().as_ref(),
                    session_state.execution_props(),
                )
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Some(Arc::new(AdaptiveVectorTopKExec::new(
            plan,
            exprs,
            topk.expr.clone(),
            (*logical_input).clone(),
            Arc::new(session_state.clone()),
            topk.fetch,
            topk.skip,
            true,
        ))))
    }
}
