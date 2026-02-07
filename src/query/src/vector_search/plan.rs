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

//! Logical plan for adaptive vector top-k search.

use std::fmt;
use std::sync::Arc;

use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{Extension, LogicalPlan, SortExpr, UserDefinedLogicalNodeCore};

/// Adaptive vector top-k logical plan.
#[derive(Hash, PartialOrd, PartialEq, Eq, Clone)]
pub struct AdaptiveVectorTopKLogicalPlan {
    pub expr: Vec<SortExpr>,
    pub input: Arc<LogicalPlan>,
    pub fetch: Option<usize>,
    pub skip: usize,
}

impl fmt::Debug for AdaptiveVectorTopKLogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl AdaptiveVectorTopKLogicalPlan {
    pub fn new(
        input: Arc<LogicalPlan>,
        expr: Vec<SortExpr>,
        fetch: Option<usize>,
        skip: usize,
    ) -> Self {
        Self {
            expr,
            input,
            fetch,
            skip,
        }
    }

    pub fn name() -> &'static str {
        "AdaptiveVectorTopK"
    }

    /// Create a [`LogicalPlan::Extension`] node from this plan.
    pub fn into_logical_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }
}

impl UserDefinedLogicalNodeCore for AdaptiveVectorTopKLogicalPlan {
    fn name(&self) -> &str {
        Self::name()
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &datafusion_common::DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<datafusion_expr::Expr> {
        self.expr.iter().map(|sort| sort.expr.clone()).collect()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AdaptiveVectorTopK: ")?;
        for (i, expr_item) in self.expr.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{expr_item}")?;
        }
        if let Some(fetch) = self.fetch {
            write!(f, ", fetch={fetch}")?;
        }
        if self.skip > 0 {
            write!(f, ", skip={}", self.skip)?;
        }
        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<datafusion_expr::Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        let mut zelf = self.clone();
        zelf.expr = zelf
            .expr
            .into_iter()
            .zip(exprs)
            .map(|(sort, expr)| sort.with_expr(expr))
            .collect();
        zelf.input = Arc::new(inputs.pop().ok_or_else(|| {
            DataFusionError::Internal(
                "Expected exactly one input with AdaptiveVectorTopK".to_string(),
            )
        })?);
        Ok(zelf)
    }
}
