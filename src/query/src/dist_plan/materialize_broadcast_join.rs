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

//! The broadcast variant of the distributed merge stage.
//!
//! A [`MaterializeBroadcastJoin`] replaces the pair of per-side `MergeScan`s of a join
//! whose build side is small enough to be materialized once on the frontend:
//!
//! ```text
//! MaterializeBroadcastJoin [remote_input=[
//! Join: probe.number = <slot>.number
//!   MaterializedInput rows=0
//!   <probe fragment>
//! ]]
//!   Materialize(build)
//! ```
//!
//! - `remote_input` is the join template the regions run: its left input is a
//!   [`MaterializedInput`] data slot, its right input is the probe fragment. The probe
//!   fragment is the only table scan of `remote_input`, so it also selects the region
//!   route of the merge stage.
//! - `build_input` is the local child: a [`MaterializeLogicalPlan`] over the build side.
//!   The frontend plans and executes it, binds the produced batches into the data slot,
//!   and ships the plan with the data to the regions.
//!
//! [`MaterializedInput`]: crate::dist_plan::MaterializedInput

use std::sync::Arc;

use datafusion_common::{DFSchemaRef, Result};
use datafusion_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};

use crate::dist_plan::RemoteDynFilterProducerId;

/// A merge stage whose remote join reads its build side from a materialized data slot.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct MaterializeBroadcastJoin {
    /// The remote join template: a [`MaterializedInput`] data slot joined with the probe
    /// fragment. It is hidden from plan traversal and runs in the regions after the
    /// frontend binds the data slot.
    ///
    /// [`MaterializedInput`]: crate::dist_plan::MaterializedInput
    remote_input: LogicalPlan,
    /// The local child: the [`Materialize`] stage over the build side. The frontend plans
    /// and executes it, which is why it is the only input visible to plan traversal.
    ///
    /// [`Materialize`]: crate::dist_plan::MaterializeLogicalPlan
    build_input: LogicalPlan,
    /// Schema exposed to the local stage, i.e. the schema of `remote_input`.
    output_schema: DFSchemaRef,
    /// Assigned after dist-plan rewriting so rewriters only deal with plan shape.
    remote_dyn_filter_producer_id: Option<RemoteDynFilterProducerId>,
}

impl PartialOrd for MaterializeBroadcastJoin {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let Self {
            remote_input,
            build_input,
            output_schema,
            remote_dyn_filter_producer_id,
        } = self;
        let Self {
            remote_input: other_remote_input,
            build_input: other_build_input,
            output_schema: other_output_schema,
            remote_dyn_filter_producer_id: other_remote_dyn_filter_producer_id,
        } = other;

        let ordering = (remote_input, build_input, remote_dyn_filter_producer_id).partial_cmp(&(
            other_remote_input,
            other_build_input,
            other_remote_dyn_filter_producer_id,
        ));
        match ordering {
            Some(std::cmp::Ordering::Equal) if output_schema != other_output_schema => None,
            ordering => ordering,
        }
    }
}

impl UserDefinedLogicalNodeCore for MaterializeBroadcastJoin {
    fn name(&self) -> &str {
        Self::name()
    }

    /// Only the local materialization of the build side is visible: the remote join
    /// template is hidden, like the remote input of a `MergeScan`.
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.build_input()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.output_schema
    }

    // Prevent further optimization
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "MaterializeBroadcastJoin [remote_input=[\n{}\n]]",
            self.remote_input()
        )
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        let mut inputs = inputs.into_iter();
        let Some(build_input) = inputs.next() else {
            return Ok(self.clone());
        };
        if inputs.next().is_some() {
            return Err(datafusion_common::DataFusionError::Internal(
                "Expect MaterializeBroadcastJoin to have at most one input".to_string(),
            ));
        }
        Ok(Self {
            build_input,
            ..self.clone()
        })
    }
}

impl MaterializeBroadcastJoin {
    /// Creates a broadcast merge stage. `remote_input` runs in the regions and starts with
    /// a [`MaterializedInput`] data slot on its left; `build_input` is the local
    /// `Materialize` stage over the build side.
    ///
    /// [`MaterializedInput`]: crate::dist_plan::MaterializedInput
    pub fn new(remote_input: LogicalPlan, build_input: LogicalPlan) -> Self {
        Self {
            output_schema: remote_input.schema().clone(),
            remote_input,
            build_input,
            remote_dyn_filter_producer_id: None,
        }
    }

    pub fn name() -> &'static str {
        "MaterializeBroadcastJoin"
    }

    /// Create a [LogicalPlan::Extension] node from this broadcast merge plan
    pub fn into_logical_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }

    /// The remote join template, to be executed in the regions.
    pub fn remote_input(&self) -> &LogicalPlan {
        &self.remote_input
    }

    /// The local child, i.e. the `Materialize` stage over the build side.
    pub fn build_input(&self) -> &LogicalPlan {
        &self.build_input
    }

    /// The remote dynamic filter producer assigned after dist-plan rewriting, if any.
    pub(crate) fn remote_dyn_filter_producer_id(&self) -> Option<RemoteDynFilterProducerId> {
        self.remote_dyn_filter_producer_id
    }

    pub fn with_remote_dyn_filter_producer_id(
        mut self,
        remote_dyn_filter_producer_id: RemoteDynFilterProducerId,
    ) -> Self {
        self.remote_dyn_filter_producer_id = Some(remote_dyn_filter_producer_id);
        self
    }
}

#[cfg(test)]
mod tests {
    use datafusion_expr::{LogicalPlanBuilder, col, lit};

    use super::*;

    fn literal_plan(alias: &str) -> LogicalPlan {
        LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias(alias)])
            .unwrap()
            .build()
            .unwrap()
    }

    fn broadcast_join() -> MaterializeBroadcastJoin {
        let remote_input = LogicalPlanBuilder::from(literal_plan("slot"))
            .join_on(
                literal_plan("probe"),
                datafusion_expr::JoinType::Inner,
                vec![col("probe").eq(col("slot"))],
            )
            .unwrap()
            .build()
            .unwrap();
        MaterializeBroadcastJoin::new(remote_input, literal_plan("build"))
    }

    #[test]
    fn only_the_build_side_is_a_visible_input() {
        let node = broadcast_join();

        assert_eq!(vec![node.build_input()], node.inputs());
        assert_eq!(node.remote_input().schema(), node.schema());
    }

    #[test]
    fn explain_shows_the_remote_template_and_the_local_child() {
        let explained = broadcast_join()
            .into_logical_plan()
            .display_indent()
            .to_string();

        assert!(
            explained.starts_with("MaterializeBroadcastJoin [remote_input=[\nInner Join:"),
            "{explained}"
        );
        assert!(
            explained.contains("]]\n  Projection: Int64(1) AS build"),
            "{explained}"
        );
    }

    #[test]
    fn with_exprs_and_inputs_rebuilds_the_local_child() {
        let node = broadcast_join();
        let replacement = literal_plan("another_build");

        let rebuilt = node
            .with_exprs_and_inputs(vec![], vec![replacement.clone()])
            .unwrap();
        assert_eq!(&replacement, rebuilt.build_input());
        assert_eq!(node.remote_input(), rebuilt.remote_input());
        assert_eq!(node.schema(), rebuilt.schema());
        assert_eq!(
            node.clone(),
            node.with_exprs_and_inputs(vec![], vec![]).unwrap()
        );
    }
}
