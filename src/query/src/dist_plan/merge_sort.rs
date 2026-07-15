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
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::projection::{ProjectionExec, make_with_child, update_ordering};
use datafusion::physical_plan::sorts::sort::SortExec;
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

    fn input_with_fetch(&self, fetch: Option<usize>) -> Arc<dyn ExecutionPlan> {
        let input = Arc::clone(self.inner.input());
        if let Some(sort) = input.as_any().downcast_ref::<SortExec>()
            && sort.preserve_partitioning()
            && sort.expr() == self.inner.expr()
        {
            // Mirror DataFusion's bare SPM plan quality for distributed TopK:
            // keep the parent `MergeSortExec(fetch)` as the global merge, and
            // bound the partition-preserving child sort to the same local TopK.
            // Local top-K is safe because every global top-K row must be within
            // the top-K rows of its own input partition.
            Arc::new(sort.with_fetch(fetch))
        } else {
            input
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

    /// Forwards DataFusion's order-preserving scan hint through this wrapper.
    ///
    /// This mirrors `SortPreservingMergeExec::with_preserve_order()`: if the
    /// child can produce an order-preserving variant, rebuild the same merge
    /// stage on top of that child. The returned plan must stay a
    /// `MergeSortExec`, not a bare SPM, so the distributed TopK merge remains
    /// opaque to `EnforceSorting`'s SPM-specific rewrite.
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

    fn cardinality_effect(&self) -> CardinalityEffect {
        self.inner.cardinality_effect()
    }

    /// Intentionally keeps DataFusion's generic limit pushdown disabled.
    ///
    /// `MergeSortExec` still supports its own global fetch through
    /// `with_fetch()`. What we must not allow is pushing an external limit below
    /// this required distributed TopK merge. DataFusion's limit pushdown rules
    /// know how to treat a bare `SortPreservingMergeExec` as a
    /// partition-combining node, but `MergeSortExec` is intentionally opaque to
    /// those SPM-specific downcasts. Enabling generic limit pushdown without also
    /// teaching the optimizer about this wrapper could return partition-local
    /// rows instead of the global TopK.
    fn supports_limit_pushdown(&self) -> bool {
        false
    }

    fn fetch(&self) -> Option<usize> {
        self.inner.fetch()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        Some(Arc::new(Self::new(
            self.inner.expr().clone(),
            self.input_with_fetch(limit),
            limit,
        )))
    }

    /// Lets DataFusion push a projection below this merge when it can rewrite
    /// the ordering expressions safely.
    ///
    /// This mirrors `SortPreservingMergeExec::try_swapping_with_projection()`
    /// for plan quality, but re-wraps the result as `MergeSortExec` so the
    /// distributed merge stage keeps its type identity and opacity.
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
    use datafusion::physical_optimizer::enforce_sorting::replace_with_order_preserving_variants::{
        OrderPreservationContext, plan_with_order_breaking_variants,
    };
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion_physical_expr::PhysicalSortExpr;
    use datafusion_physical_expr::expressions::col as physical_col;

    use super::*;

    /// Test double that records DataFusion's preserve-order signal while
    /// otherwise behaving like a transparent wrapper around its child.
    #[derive(Debug, Clone)]
    struct PreserveOrderProbeExec {
        inner: Arc<dyn ExecutionPlan>,
        preserve_order: bool,
    }

    impl PreserveOrderProbeExec {
        fn new(inner: Arc<dyn ExecutionPlan>) -> Self {
            Self {
                inner,
                preserve_order: false,
            }
        }
    }

    impl DisplayAs for PreserveOrderProbeExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(
                f,
                "PreserveOrderProbeExec: preserve_order={}",
                self.preserve_order
            )
        }
    }

    impl ExecutionPlan for PreserveOrderProbeExec {
        fn name(&self) -> &str {
            "PreserveOrderProbeExec"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            self.inner.properties()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![&self.inner]
        }

        fn with_new_children(
            self: Arc<Self>,
            mut children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            if children.len() != 1 {
                return Err(DataFusionError::Internal(format!(
                    "PreserveOrderProbeExec expects exactly one child, got {}",
                    children.len()
                )));
            }

            Ok(Arc::new(Self {
                inner: children.swap_remove(0),
                preserve_order: self.preserve_order,
            }))
        }

        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            self.inner.execute(partition, context)
        }

        fn with_preserve_order(&self, preserve_order: bool) -> Option<Arc<dyn ExecutionPlan>> {
            Some(Arc::new(Self {
                inner: Arc::clone(&self.inner),
                preserve_order,
            }))
        }
    }

    fn test_ordering(schema: &Schema) -> LexOrdering {
        LexOrdering::new([PhysicalSortExpr::new(
            physical_col("ts", schema).unwrap(),
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        )])
        .unwrap()
    }

    #[test]
    fn merge_sort_exec_is_opaque_and_preserves_topk_requirements() {
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)]));
        let input = Arc::new(EmptyExec::new(schema.clone()).with_partitions(2)) as _;
        let ordering = test_ordering(schema.as_ref());

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
        assert!(!merge_sort.supports_limit_pushdown());
        assert!(merge_sort.required_input_ordering()[0].is_some());

        let tree = displayable(merge_sort.as_ref()).tree_render().to_string();
        assert!(tree.contains("MergeSortExec"));
        assert!(!tree.contains("SortPreservingMergeExec"));

        let fetched = merge_sort.with_fetch(Some(2)).unwrap();
        assert!(fetched.as_any().downcast_ref::<MergeSortExec>().is_some());
        assert_eq!(fetched.fetch(), Some(2));
    }

    #[test]
    fn merge_sort_exec_required_input_ordering_matches_spm() {
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)]));
        let input = Arc::new(EmptyExec::new(schema.clone()).with_partitions(2)) as _;
        let ordering = test_ordering(schema.as_ref());

        let merge_sort = MergeSortExec::new(ordering.clone(), Arc::clone(&input), Some(1));
        let bare_spm =
            SortPreservingMergeExec::new(ordering.clone(), Arc::clone(&input)).with_fetch(Some(1));

        assert_eq!(
            merge_sort.required_input_ordering(),
            vec![Some(OrderingRequirements::from(ordering))],
            "MergeSortExec must require locally sorted input partitions for the merge key"
        );
        assert_eq!(
            merge_sort.required_input_ordering(),
            bare_spm.required_input_ordering(),
            "MergeSortExec's child ordering contract should mirror SortPreservingMergeExec"
        );
        assert_eq!(
            merge_sort.maintains_input_order(),
            bare_spm.maintains_input_order()
        );
    }

    #[test]
    fn merge_sort_exec_with_fetch_pushes_fetch_to_child_sort() {
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)]));
        let input = Arc::new(EmptyExec::new(schema.clone()).with_partitions(2)) as _;
        let ordering = test_ordering(schema.as_ref());
        let child_sort =
            Arc::new(SortExec::new(ordering.clone(), input).with_preserve_partitioning(true))
                as Arc<dyn ExecutionPlan>;
        let merge_sort = MergeSortExec::new(ordering, child_sort, None);

        let fetched = merge_sort.with_fetch(Some(2)).unwrap();

        assert!(fetched.as_any().downcast_ref::<MergeSortExec>().is_some());
        assert_eq!(fetched.fetch(), Some(2));
        let child_sort = fetched.children()[0]
            .as_any()
            .downcast_ref::<SortExec>()
            .unwrap();
        assert_eq!(child_sort.fetch(), Some(2));
        assert!(child_sort.preserve_partitioning());
    }

    #[test]
    fn merge_sort_exec_with_preserve_order_matches_spm_but_keeps_wrapper() {
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)]));
        let input = Arc::new(PreserveOrderProbeExec::new(Arc::new(
            EmptyExec::new(schema.clone()).with_partitions(2),
        ))) as _;
        let ordering = test_ordering(schema.as_ref());

        let bare_spm =
            SortPreservingMergeExec::new(ordering.clone(), Arc::clone(&input)).with_fetch(Some(1));
        let preserved_spm = bare_spm.with_preserve_order(true).unwrap();
        assert!(
            preserved_spm
                .as_any()
                .downcast_ref::<SortPreservingMergeExec>()
                .is_some(),
            "bare SPM should rebuild as bare SPM"
        );
        assert!(
            preserved_spm.children()[0]
                .as_any()
                .downcast_ref::<PreserveOrderProbeExec>()
                .unwrap()
                .preserve_order
        );

        let merge_sort = MergeSortExec::new(ordering, input, Some(1));
        let preserved_merge_sort = merge_sort.with_preserve_order(true).unwrap();
        assert!(
            preserved_merge_sort
                .as_any()
                .downcast_ref::<MergeSortExec>()
                .is_some(),
            "MergeSortExec must rewrap the preserve-order child as MergeSortExec"
        );
        assert!(
            preserved_merge_sort
                .as_any()
                .downcast_ref::<SortPreservingMergeExec>()
                .is_none(),
            "MergeSortExec must not expose a bare SPM after with_preserve_order"
        );
        assert_eq!(preserved_merge_sort.fetch(), Some(1));
        assert_eq!(
            preserved_merge_sort.required_input_ordering(),
            preserved_spm.required_input_ordering(),
            "preserve-order rewrite should keep the same SPM child-ordering contract"
        );
        assert!(
            preserved_merge_sort.children()[0]
                .as_any()
                .downcast_ref::<PreserveOrderProbeExec>()
                .unwrap()
                .preserve_order
        );
    }

    #[test]
    fn merge_sort_exec_projection_swap_matches_spm_but_keeps_wrapper() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("tag", DataType::Utf8, false),
        ]));
        let input = Arc::new(EmptyExec::new(schema.clone()).with_partitions(2)) as _;
        let ordering = test_ordering(schema.as_ref());

        let bare_spm = Arc::new(
            SortPreservingMergeExec::new(ordering.clone(), Arc::clone(&input)).with_fetch(Some(1)),
        ) as Arc<dyn ExecutionPlan>;
        let spm_projection = ProjectionExec::try_new(
            vec![
                (physical_col("ts", schema.as_ref())?, "ts".to_string()),
                (physical_col("tag", schema.as_ref())?, "tag".to_string()),
            ],
            Arc::clone(&bare_spm),
        )?;
        let swapped_spm = bare_spm
            .try_swapping_with_projection(&spm_projection)?
            .expect("SPM should accept a narrowing projection that preserves the sort key");
        assert!(
            swapped_spm
                .as_any()
                .downcast_ref::<SortPreservingMergeExec>()
                .is_some(),
            "bare SPM should rebuild as bare SPM"
        );

        let merge_sort =
            Arc::new(MergeSortExec::new(ordering, input, Some(1))) as Arc<dyn ExecutionPlan>;
        let merge_projection = ProjectionExec::try_new(
            vec![
                (physical_col("ts", schema.as_ref())?, "ts".to_string()),
                (physical_col("tag", schema.as_ref())?, "tag".to_string()),
            ],
            Arc::clone(&merge_sort),
        )?;
        let swapped_merge_sort = merge_sort
            .try_swapping_with_projection(&merge_projection)?
            .expect("MergeSortExec should accept the same projection swap as SPM");

        assert!(
            swapped_merge_sort
                .as_any()
                .downcast_ref::<MergeSortExec>()
                .is_some(),
            "MergeSortExec must rewrap projection swaps as MergeSortExec"
        );
        assert!(
            swapped_merge_sort
                .as_any()
                .downcast_ref::<SortPreservingMergeExec>()
                .is_none(),
            "MergeSortExec must not expose a bare SPM after projection swap"
        );
        assert_eq!(swapped_merge_sort.fetch(), Some(1));
        assert!(
            swapped_merge_sort.children()[0]
                .as_any()
                .downcast_ref::<ProjectionExec>()
                .is_some(),
            "the projection should move below MergeSortExec"
        );
        let swapped_schema = swapped_merge_sort.schema();
        assert_eq!(
            swapped_schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec!["ts", "tag"],
            "swapped MergeSortExec should expose the projected schema"
        );

        let projected_ordering = LexOrdering::new([PhysicalSortExpr::new(
            physical_col("ts", swapped_merge_sort.children()[0].schema().as_ref())?,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        )])
        .unwrap();
        assert_eq!(
            swapped_merge_sort.required_input_ordering(),
            vec![Some(OrderingRequirements::from(projected_ordering))],
            "projection swap must rewrite the ordering to the child projection's schema"
        );
        assert_eq!(
            swapped_merge_sort.required_input_ordering(),
            swapped_spm.required_input_ordering(),
            "MergeSortExec projection swap should mirror SPM's ordering rewrite"
        );

        let spm_projection_without_sort_key = ProjectionExec::try_new(
            vec![(physical_col("tag", schema.as_ref())?, "tag".to_string())],
            Arc::clone(&bare_spm),
        )?;
        let merge_projection_without_sort_key = ProjectionExec::try_new(
            vec![(physical_col("tag", schema.as_ref())?, "tag".to_string())],
            Arc::clone(&merge_sort),
        )?;
        assert!(
            bare_spm
                .try_swapping_with_projection(&spm_projection_without_sort_key)?
                .is_none(),
            "SPM must reject projection swaps that drop the sort key"
        );
        assert!(
            merge_sort
                .try_swapping_with_projection(&merge_projection_without_sort_key)?
                .is_none(),
            "MergeSortExec should reject the same projection swap as SPM"
        );

        Ok(())
    }

    #[test]
    fn enforce_sorting_rewrite_keeps_merge_sort_exec_opaque() {
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)]));
        let input = Arc::new(EmptyExec::new(schema.clone()).with_partitions(2)) as _;
        let ordering = test_ordering(schema.as_ref());

        let bare_spm = Arc::new(
            SortPreservingMergeExec::new(ordering.clone(), Arc::clone(&input)).with_fetch(Some(1)),
        ) as Arc<dyn ExecutionPlan>;
        let optimized_spm = plan_with_order_breaking_variants(OrderPreservationContext::new(
            bare_spm,
            false,
            vec![OrderPreservationContext::new(
                Arc::clone(&input),
                false,
                vec![],
            )],
        ))
        .unwrap()
        .plan;
        assert!(
            optimized_spm
                .as_any()
                .downcast_ref::<CoalescePartitionsExec>()
                .is_some(),
            "this regression test must exercise EnforceSorting's bare SPM -> CoalescePartitionsExec rewrite"
        );

        let merge_sort =
            Arc::new(MergeSortExec::new(ordering, input, Some(1))) as Arc<dyn ExecutionPlan>;
        let optimized_merge_sort =
            plan_with_order_breaking_variants(OrderPreservationContext::new(
                Arc::clone(&merge_sort),
                false,
                vec![OrderPreservationContext::new(
                    Arc::clone(merge_sort.children()[0]),
                    false,
                    vec![],
                )],
            ))
            .unwrap()
            .plan;
        assert!(
            optimized_merge_sort
                .as_any()
                .downcast_ref::<MergeSortExec>()
                .is_some(),
            "MergeSortExec must stay opaque to the bare SPM rewrite"
        );
        assert!(
            optimized_merge_sort
                .as_any()
                .downcast_ref::<CoalescePartitionsExec>()
                .is_none(),
            "MergeSortExec(fetch) is the required distributed TopK merge stage, not an unordered coalesce"
        );
        assert_eq!(optimized_merge_sort.fetch(), Some(1));
    }
}
