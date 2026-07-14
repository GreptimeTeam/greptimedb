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

//! Merge sort logical plan for distributed query execution, roughly corresponding to the
//! `SortPreservingMergeExec` operator in datafusion
//!

use std::fmt;
use std::sync::Arc;

use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{Extension, LogicalPlan, SortExpr, UserDefinedLogicalNodeCore};

/// MergeSort Logical Plan, have same field as `Sort`, but indicate it is a merge sort,
/// which assume each input partition is a sorted stream, and will use `SortPreserveingMergeExec`
/// to merge them into a single sorted stream.
#[derive(Hash, PartialOrd, PartialEq, Eq, Clone)]
pub struct MergeSortLogicalPlan {
    pub expr: Vec<SortExpr>,
    pub input: Arc<LogicalPlan>,
    pub fetch: Option<usize>,
}

impl fmt::Debug for MergeSortLogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl MergeSortLogicalPlan {
    pub fn new(input: Arc<LogicalPlan>, expr: Vec<SortExpr>, fetch: Option<usize>) -> Self {
        Self { input, expr, fetch }
    }

    pub fn name() -> &'static str {
        "MergeSort"
    }

    /// Create a [`LogicalPlan::Extension`] node from this merge sort plan
    pub fn into_logical_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }

    /// Convert self to a [`Sort`] logical plan with same input and expressions
    pub fn into_sort(self) -> LogicalPlan {
        LogicalPlan::Sort(datafusion::logical_expr::Sort {
            input: self.input.clone(),
            expr: self.expr,
            fetch: self.fetch,
        })
    }
}

impl UserDefinedLogicalNodeCore for MergeSortLogicalPlan {
    fn name(&self) -> &str {
        Self::name()
    }

    // Allow optimization here
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &datafusion_common::DFSchemaRef {
        self.input.schema()
    }

    // Allow further optimization
    fn expressions(&self) -> Vec<datafusion_expr::Expr> {
        self.expr.iter().map(|sort| sort.expr.clone()).collect()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MergeSort: ")?;
        for (i, expr_item) in self.expr.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{expr_item}")?;
        }
        if let Some(a) = self.fetch {
            write!(f, ", fetch={a}")?;
        }
        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<datafusion::prelude::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        let [input] = inputs.as_slice() else {
            return Err(DataFusionError::Internal(
                "Expected exactly one input with MergeSort".to_string(),
            ));
        };

        let mut zelf = self.clone();
        zelf.expr = zelf
            .expr
            .into_iter()
            .zip(exprs)
            .map(|(sort, expr)| sort.with_expr(expr))
            .collect();
        zelf.input = Arc::new(input.clone());
        Ok(zelf)
    }
}

/// Turn `Sort` into `MergeSort` if possible
pub fn merge_sort_transformer(plan: &LogicalPlan) -> Option<LogicalPlan> {
    if let LogicalPlan::Sort(sort) = plan {
        Some(
            MergeSortLogicalPlan::new(sort.input.clone(), sort.expr.clone(), sort.fetch)
                .into_logical_plan(),
        )
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::DFSchema;
    use datafusion_expr::{EmptyRelation, UserDefinedLogicalNodeCore, col, lit};

    use super::*;

    fn empty_relation(produce_one_row: bool) -> LogicalPlan {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "sort_key",
            DataType::Int64,
            true,
        )]));
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row,
            schema: Arc::new(DFSchema::try_from(schema).unwrap()),
        })
    }

    fn merge_sort_plan() -> MergeSortLogicalPlan {
        MergeSortLogicalPlan::new(
            Arc::new(empty_relation(false)),
            vec![col("sort_key").sort(true, true)],
            Some(17),
        )
    }

    #[test]
    fn qbs_merge_sort_rejects_zero_inputs() {
        let plan = merge_sort_plan();

        match UserDefinedLogicalNodeCore::with_exprs_and_inputs(&plan, plan.expressions(), vec![]) {
            Err(DataFusionError::Internal(message)) => {
                assert_eq!(message, "Expected exactly one input with MergeSort");
            }
            Err(error) => panic!("expected an internal exact-one-input error, got {error:?}"),
            Ok(_) => panic!("MergeSort accepted zero inputs instead of rejecting them"),
        }
    }

    #[test]
    fn qbs_merge_sort_accepts_one_input() {
        let plan = merge_sort_plan();
        let replacement = empty_relation(true);
        let replacement_expr = lit(1_i64);

        let rebuilt = UserDefinedLogicalNodeCore::with_exprs_and_inputs(
            &plan,
            vec![replacement_expr.clone()],
            vec![replacement.clone()],
        )
        .unwrap();

        assert_eq!(rebuilt.fetch, plan.fetch);
        assert_eq!(rebuilt.expr, vec![replacement_expr.sort(true, true)]);
        assert_eq!(rebuilt.input.as_ref(), &replacement);
        assert_eq!(rebuilt.input.schema(), replacement.schema());
        assert_eq!(rebuilt.input.schema().field(0).name(), "sort_key");
    }

    #[test]
    fn qbs_merge_sort_rejects_multiple_inputs() {
        let plan = merge_sort_plan();
        let first = empty_relation(false);
        let second = empty_relation(true);

        match UserDefinedLogicalNodeCore::with_exprs_and_inputs(
            &plan,
            plan.expressions(),
            vec![first, second],
        ) {
            Err(DataFusionError::Internal(message)) => {
                assert_eq!(message, "Expected exactly one input with MergeSort");
            }
            Err(error) => panic!("expected an internal exact-one-input error, got {error:?}"),
            Ok(_) => panic!("MergeSort accepted multiple inputs instead of rejecting them"),
        }
    }
}
