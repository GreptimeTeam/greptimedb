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

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::projection::{ProjectionExec, make_with_child, update_ordering};
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    Statistics,
};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{Extension, LogicalPlan, SortExpr, UserDefinedLogicalNodeCore};
use datafusion_physical_expr::{Distribution, LexOrdering, OrderingRequirements};

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
}

/// An opaque physical execution node for [`MergeSortLogicalPlan`].
///
/// It delegates execution and physical properties to DataFusion's
/// [`SortPreservingMergeExec`], but intentionally does not expose itself as a
/// `SortPreservingMergeExec`. `EnforceSorting` is allowed to replace a bare
/// `SortPreservingMergeExec` with `CoalescePartitionsExec` when the parent does
/// not require ordering. `MergeSortExec` represents the distributed TopK merge
/// stage itself, so later physical optimizer rules must not rewrite it into an
/// unordered fetch.
#[derive(Debug, Clone)]
pub(crate) struct MergeSortExec {
    inner: SortPreservingMergeExec,
}

impl MergeSortExec {
    pub(crate) fn new(
        ordering: LexOrdering,
        input: Arc<dyn ExecutionPlan>,
        fetch: Option<usize>,
    ) -> Self {
        Self {
            inner: SortPreservingMergeExec::new(ordering, input).with_fetch(fetch),
        }
    }
}

impl DisplayAs for MergeSortExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "MergeSortExec: [{}]", self.inner.expr())?;
                if let Some(fetch) = self.inner.fetch() {
                    write!(f, ", fetch={fetch}")?;
                }
                Ok(())
            }
            DisplayFormatType::TreeRender => {
                if let Some(fetch) = self.inner.fetch() {
                    writeln!(f, "limit={fetch}")?;
                }

                for (i, expr) in self.inner.expr().iter().enumerate() {
                    expr.fmt_sql(f)?;
                    if i != self.inner.expr().len() - 1 {
                        write!(f, ", ")?;
                    }
                }

                Ok(())
            }
        }
    }
}

impl ExecutionPlan for MergeSortExec {
    fn name(&self) -> &str {
        "MergeSortExec"
    }

    /// Keeps this node intentionally opaque to DataFusion's type-specialized
    /// optimizer rewrites.
    ///
    /// `MergeSortExec` delegates most behavior to DataFusion's
    /// `SortPreservingMergeExec`, but it must not expose itself as that type.
    /// DataFusion's `EnforceSorting` optimizer recognizes a bare
    /// `SortPreservingMergeExec` via `as_any().downcast_ref::<...>()` and may
    /// replace it with an unordered `CoalescePartitionsExec(fetch)` when the
    /// parent does not require sorted output.
    ///
    /// That rewrite is valid for an ordinary SPM used only to satisfy parent
    /// ordering, but not for GreptimeDB's distributed TopK merge stage. In a
    /// scalar-subquery shape like `ORDER BY ts DESC LIMIT 1`, this node is the
    /// operator that merges region-local TopK streams into the global TopK.
    /// Replacing it with unordered coalescing can return a partial/latest row
    /// from one region instead of the global latest row.
    ///
    /// `required_input_ordering()` separately tells DataFusion what ordering this
    /// node needs from its child, so `EnforceSorting` can insert a `SortExec`
    /// below `MergeSortExec` when `MergeScanExec` cannot preserve per-partition
    /// ordering. This opacity is specifically about protecting the merge stage
    /// itself from the `EnforceSorting` rewrite above.
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn with_preserve_order(&self, preserve_order: bool) -> Option<Arc<dyn ExecutionPlan>> {
        self.inner
            .input()
            .with_preserve_order(preserve_order)
            .map(|new_input| {
                Arc::new(Self::new(
                    self.inner.expr().clone(),
                    new_input,
                    self.inner.fetch(),
                )) as Arc<dyn ExecutionPlan>
            })
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.inner.required_input_distribution()
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        self.inner.benefits_from_input_partitioning()
    }

    /// Tells DataFusion that `MergeSortExec` requires each input partition to be
    /// ordered. This is the contract that makes `EnforceSorting` insert a
    /// `SortExec` below `MergeSortExec` when the input cannot preserve ordering.
    ///
    /// The opacity of `MergeSortExec::as_any`, not this requirement, is what
    /// prevents DataFusion from rewriting the merge stage itself as a bare
    /// `SortPreservingMergeExec`.
    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![Some(OrderingRequirements::from(self.inner.expr().clone()))]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.inner.maintains_input_order()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inner.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "MergeSortExec expects exactly one child, got {}",
                children.len()
            )));
        }

        Ok(Arc::new(Self::new(
            self.inner.expr().clone(),
            children.swap_remove(0),
            self.inner.fetch(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.inner.partition_statistics(partition)
    }

    // Do not mirror `SortPreservingMergeExec::supports_limit_pushdown()` here.
    // DataFusion's limit pushdown rules know how to treat a bare SPM as a
    // partition-combining node, but `MergeSortExec` is intentionally opaque to
    // those SPM-specific downcasts. Enabling generic limit pushdown without also
    // teaching the optimizer about this wrapper could push a limit below the
    // required distributed TopK merge and return partition-local rows.

    fn fetch(&self) -> Option<usize> {
        self.inner.fetch()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        Some(Arc::new(Self::new(
            self.inner.expr().clone(),
            Arc::clone(self.inner.input()),
            limit,
        )))
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if projection.expr().len() >= projection.input().schema().fields().len() {
            return Ok(None);
        }

        let Some(updated_exprs) = update_ordering(self.inner.expr().clone(), projection.expr())?
        else {
            return Ok(None);
        };

        Ok(Some(Arc::new(Self::new(
            updated_exprs,
            make_with_child(projection, self.inner.input())?,
            self.inner.fetch(),
        ))))
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
            DataFusionError::Internal("Expected exactly one input with MergeSort".to_string())
        })?);
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
    use arrow_schema::{DataType, Field, Schema, SortOptions};
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion_physical_expr::PhysicalSortExpr;
    use datafusion_physical_expr::expressions::col as physical_col;

    use super::*;

    #[test]
    fn merge_sort_exec_is_opaque_and_preserves_topk_requirements() {
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)]));
        let input = Arc::new(EmptyExec::new(schema.clone()).with_partitions(2)) as _;
        let ordering = LexOrdering::new([PhysicalSortExpr::new(
            physical_col("ts", schema.as_ref()).unwrap(),
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        )])
        .unwrap();

        let merge_sort =
            Arc::new(MergeSortExec::new(ordering, input, Some(1))) as Arc<dyn ExecutionPlan>;

        assert_eq!(merge_sort.name(), "MergeSortExec");
        assert!(
            merge_sort
                .as_any()
                .downcast_ref::<SortPreservingMergeExec>()
                .is_none(),
            "MergeSortExec must stay opaque to EnforceSorting's bare SortPreservingMerge rewrite"
        );
        assert_eq!(merge_sort.fetch(), Some(1));
        assert!(merge_sort.required_input_ordering()[0].is_some());

        let tree = displayable(merge_sort.as_ref()).tree_render().to_string();
        assert!(tree.contains("MergeSortExec"));
        assert!(!tree.contains("SortPreservingMergeExec"));

        let fetched = merge_sort.with_fetch(Some(2)).unwrap();
        assert!(fetched.as_any().downcast_ref::<MergeSortExec>().is_some());
        assert_eq!(fetched.fetch(), Some(2));
    }
}
