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

//! Execution-time decision for the materialized broadcast join.
//!
//! Plain `EXPLAIN` only needs the shape of the plan, so the analyzer inserts a
//! schema-only [`MaterializedBroadcastNode`] placeholder instead of evaluating
//! the small side. When the query actually runs, [`MaterializedBroadcastExecution`]
//! wraps the whole physical plan, evaluates the build side once, and then:
//!
//! * within the row/byte budget, materializes the batches and re-plans the probe
//!   side with the data embedded, so the payload is encoded once per query;
//! * beyond the budget, re-plans the original analyzed plan (the frontend join)
//!   instead of materializing.
//!
//! Re-planning runs on `SessionState` directly, so the engine's analyzer hook
//! (and its placeholder insertion) does not run twice.

use std::fmt::{self, Debug};
use std::sync::Arc;

use datafusion::common::DataFusionError;
use datafusion::execution::session_state::SessionState;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion_common::Result;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_expr::LogicalPlan;
use futures::StreamExt;

use crate::dist_plan::materialized_broadcast::{
    MaterializedBroadcastNode, find_placeholder, materialize_placeholder,
};

/// Wraps the probe-side physical plan and decides at execution time whether the
/// build side is materialized.
pub struct MaterializedBroadcastExecution {
    session_state: SessionState,
    /// Logical plan with the schema-only placeholder (within the budget).
    placeholder_plan: LogicalPlan,
    /// The analyzed plan before inserting the placeholder (beyond the budget).
    fallback_plan: LogicalPlan,
    properties: Arc<PlanProperties>,
}

impl Debug for MaterializedBroadcastExecution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MaterializedBroadcastExecution")
            .field("placeholder_plan", &self.placeholder_plan)
            .finish_non_exhaustive()
    }
}

impl MaterializedBroadcastExecution {
    pub fn new(
        session_state: SessionState,
        placeholder_plan: LogicalPlan,
        fallback_plan: LogicalPlan,
    ) -> Self {
        let schema = Arc::new(placeholder_plan.schema().as_arrow().clone());
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            session_state,
            placeholder_plan,
            fallback_plan,
            properties,
        }
    }

    /// Whether a logical plan carries a schema-only placeholder.
    pub fn has_placeholder(plan: &LogicalPlan) -> bool {
        find_placeholder(plan).is_some()
    }
}

impl DisplayAs for MaterializedBroadcastExecution {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MaterializedBroadcastExecution")
    }
}

impl ExecutionPlan for MaterializedBroadcastExecution {
    fn name(&self) -> &str {
        "MaterializedBroadcastExecution"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = self.schema();
        if partition != 0 {
            return Ok(Box::pin(
                datafusion::physical_plan::EmptyRecordBatchStream::new(schema),
            ));
        }

        let this = self.clone_for_execution();
        let stream = async_stream::try_stream! {
            let (build_plan, placeholder_plan) = this.build_and_probe_plans()?;
            let build_physical = this.session_state.create_physical_plan(&build_plan).await?;
            // The build side is distributed by region: coalesce all of its
            // partitions, otherwise only the first region is broadcast.
            let build_physical = CoalescePartitionsExec::new(build_physical);
            let mut build_stream = build_physical.execute(0, context.clone())?;

            let mut batches = Vec::new();
            let mut rows = 0usize;
            let mut bytes = 0usize;
            let mut overflow = false;
            while let Some(batch) = build_stream.next().await {
                let batch = batch?;
                rows += batch.num_rows();
                bytes += batch.get_array_memory_size();
                if rows > MaterializedBroadcastNode::MAX_ROWS
                    || bytes > MaterializedBroadcastNode::MAX_BYTES
                {
                    overflow = true;
                    break;
                }
                batches.push(batch);
            }

            // Beyond the budget the already read batches are dropped and the
            // original plan re-reads the tables as a frontend join; keeping the
            // partially read stream would duplicate the read.
            let plan = if overflow {
                this.fallback_plan.clone()
            } else {
                materialize_placeholder(placeholder_plan, batches)?
            };
            let physical = this.session_state.create_physical_plan(&plan).await?;
            // The decision node exposes a single partition, so the probe side's
            // partitions have to be merged here.
            let physical = CoalescePartitionsExec::new(physical);
            let mut stream = physical.execute(0, context)?;
            while let Some(batch) = stream.next().await {
                yield batch?;
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

impl MaterializedBroadcastExecution {
    fn clone_for_execution(&self) -> Self {
        Self {
            session_state: self.session_state.clone(),
            placeholder_plan: self.placeholder_plan.clone(),
            fallback_plan: self.fallback_plan.clone(),
            properties: self.properties.clone(),
        }
    }

    fn build_and_probe_plans(&self) -> Result<(LogicalPlan, LogicalPlan)> {
        let placeholder = find_placeholder(&self.placeholder_plan).ok_or_else(|| {
            DataFusionError::Internal(
                "materialized broadcast execution without a placeholder".to_string(),
            )
        })?;
        let build_plan = placeholder.build_plan().cloned().ok_or_else(|| {
            DataFusionError::Internal(
                "materialized broadcast placeholder without a build plan".to_string(),
            )
        })?;
        Ok((build_plan, self.placeholder_plan.clone()))
    }
}
