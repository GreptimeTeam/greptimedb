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

//! [ExtensionPlanner] impl for distributed planner

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::planner::ExtensionPlanner;
use datafusion::physical_plan::{ExecutionPlan, PhysicalPlanner};
use datafusion_common::DataFusionError;
use datafusion_expr::{LogicalPlan, UserDefinedLogicalNode};

use crate::dist_plan::merge_scan::MergeScanLogicalPlan;

pub struct DistExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for DistExtensionPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let maybe_merge_scan = { node.as_any().downcast_ref::<MergeScanLogicalPlan>() };
        if let Some(merge_scan) = maybe_merge_scan {
            if merge_scan.is_placeholder() {
                let input = merge_scan.input().clone();
                planner
                    .create_physical_plan(&input, session_state)
                    .await
                    .map(Some)
            } else {
                Err(DataFusionError::NotImplemented("MergeScan".to_string()))
            }
        } else {
            Ok(None)
        }
    }
}
