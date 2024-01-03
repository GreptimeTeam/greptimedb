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

use async_trait::async_trait;
use datafusion::error::Result as DfResult;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};

use super::{HistogramFold, UnionDistinctOn};
use crate::extension_plan::{
    EmptyMetric, InstantManipulate, RangeManipulate, SeriesDivide, SeriesNormalize,
};

pub struct PromExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for PromExtensionPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(node) = node.as_any().downcast_ref::<SeriesNormalize>() {
            Ok(Some(node.to_execution_plan(physical_inputs[0].clone())))
        } else if let Some(node) = node.as_any().downcast_ref::<InstantManipulate>() {
            Ok(Some(node.to_execution_plan(physical_inputs[0].clone())))
        } else if let Some(node) = node.as_any().downcast_ref::<RangeManipulate>() {
            Ok(Some(node.to_execution_plan(physical_inputs[0].clone())))
        } else if let Some(node) = node.as_any().downcast_ref::<SeriesDivide>() {
            Ok(Some(node.to_execution_plan(physical_inputs[0].clone())))
        } else if let Some(node) = node.as_any().downcast_ref::<EmptyMetric>() {
            Ok(Some(node.to_execution_plan(session_state, planner)?))
        } else if let Some(node) = node.as_any().downcast_ref::<HistogramFold>() {
            Ok(Some(node.to_execution_plan(physical_inputs[0].clone())))
        } else if let Some(node) = node.as_any().downcast_ref::<UnionDistinctOn>() {
            Ok(Some(node.to_execution_plan(
                physical_inputs[0].clone(),
                physical_inputs[1].clone(),
            )))
        } else {
            Ok(None)
        }
    }
}
