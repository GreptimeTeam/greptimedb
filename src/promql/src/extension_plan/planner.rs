use std::sync::Arc;

use async_trait::async_trait;
use datafusion::error::Result as DfResult;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::planner::ExtensionPlanner;
use datafusion::physical_plan::{ExecutionPlan, PhysicalPlanner};

use super::{InstantManipulate, RangeManipulate};
use crate::extension_plan::SeriesNormalize;

pub struct PromExtensionPlanner {}

#[async_trait]
impl ExtensionPlanner for PromExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(node) = node.as_any().downcast_ref::<SeriesNormalize>() {
            println!("{node:?}");
            Ok(Some(node.to_execution_plan(physical_inputs[0].clone())))
        } else if let Some(node) = node.as_any().downcast_ref::<InstantManipulate>() {
            Ok(Some(node.to_execution_plan(physical_inputs[0].clone())))
        } else if let Some(node) = node.as_any().downcast_ref::<RangeManipulate>() {
            Ok(Some(node.to_execution_plan(physical_inputs[0].clone())))
        } else {
            Ok(None)
        }
    }
}
