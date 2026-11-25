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

//! The local half of a materialized broadcast join.
//!
//! A [`MaterializeLogicalPlan`] wraps the build side of a frontend/region join: it runs
//! on the frontend and produces the batches that the remote join reads through
//! [`MaterializedInput`](crate::dist_plan::MaterializedInput). It is the local child of
//! [`MaterializeBroadcastJoin`](crate::dist_plan::MaterializeBroadcastJoin), so the
//! materialization is part of the plan tree that only the frontend executes.

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion_common::{DFSchemaRef, Result};
use datafusion_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};

/// Marks the frontend stage that materializes the build side of a remote join.
#[derive(Debug, Clone)]
pub struct MaterializeLogicalPlan {
    /// The build side, evaluated by the frontend.
    input: LogicalPlan,
    /// Schema of `input`, i.e. the schema of the materialized batches.
    schema: DFSchemaRef,
}

impl PartialEq for MaterializeLogicalPlan {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input
    }
}
impl Eq for MaterializeLogicalPlan {}

impl Hash for MaterializeLogicalPlan {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
    }
}

impl PartialOrd for MaterializeLogicalPlan {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.input.partial_cmp(&other.input)
    }
}

impl MaterializeLogicalPlan {
    pub const NAME: &'static str = "greptime.materialize";

    /// Wraps `input` in a materialization stage. The schema follows `input`.
    pub fn new(input: LogicalPlan) -> Self {
        let schema = input.schema().clone();
        Self { input, schema }
    }

    /// Create a [LogicalPlan::Extension] node from this materialize plan
    pub fn into_logical_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }
}

impl UserDefinedLogicalNodeCore for MaterializeLogicalPlan {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Materialize")
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        let mut inputs = inputs.into_iter();
        let input = inputs.next().ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(
                "Expect Materialize to have exactly one input".to_string(),
            )
        })?;
        if inputs.next().is_some() {
            return Err(datafusion_common::DataFusionError::Internal(
                "Expect Materialize to have exactly one input".to_string(),
            ));
        }
        Ok(Self::new(input))
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

    #[test]
    fn schema_tracks_input() {
        let input = literal_plan("a");
        let node = MaterializeLogicalPlan::new(input.clone());

        assert_eq!(input.schema(), node.schema());
        assert_eq!(1, node.inputs().len());
        assert_eq!(&input, node.inputs()[0]);
        assert_eq!(
            Some("Materialize"),
            node.clone()
                .into_logical_plan()
                .display_indent()
                .to_string()
                .lines()
                .next()
        );
    }

    #[test]
    fn with_exprs_and_inputs_rebuilds_from_the_new_input() {
        let node = MaterializeLogicalPlan::new(literal_plan("a"));
        let replacement = LogicalPlanBuilder::from(literal_plan("b"))
            .project(vec![col("b")])
            .unwrap()
            .build()
            .unwrap();

        let rebuilt = node
            .with_exprs_and_inputs(vec![], vec![replacement.clone()])
            .unwrap();
        assert_eq!(replacement.schema(), rebuilt.schema());
        assert_eq!(vec![&replacement], rebuilt.inputs());
        assert!(node.with_exprs_and_inputs(vec![], vec![]).is_err());
        assert!(
            node.with_exprs_and_inputs(vec![], vec![replacement.clone(), replacement])
                .is_err()
        );
    }
}
