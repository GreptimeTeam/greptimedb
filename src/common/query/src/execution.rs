use std::any::Any;
use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use common_recordbatch::adapter::{DfRecordBatchStreamAdapter, RecordBatchStreamAdapter};
use common_recordbatch::SendableRecordBatchStream;
use datafusion::arrow::datatypes::SchemaRef as DfSchemaRef;
use datafusion::error::Result as DfResult;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::ExecutionPlan as DfExecutionPlan;
use datafusion::physical_plan::SendableRecordBatchStream as DfSendableRecordBatchStream;
use datafusion::physical_plan::{DisplayFormatType, Distribution, Partitioning, Statistics};
use datatypes::schema::SchemaRef;
use snafu::ResultExt;

use crate::error::{self, Result};

/// `ExecutionPlan` represent nodes in the Physical Plan.
///
/// Each `ExecutionPlan` is Partition-aware and is responsible for
/// creating the actual `async` [`SendableRecordBatchStream`]s
/// of [`RecordBatch`] that incrementally compute the operator's
/// output from its input partition.
///
/// [`ExecutionPlan`] can be displayed in an simplified form using the
/// return value from [`displayable`] in addition to the (normally
/// quite verbose) `Debug` output.
#[async_trait]
pub trait ExecutionPlan: Debug + Send + Sync {
    /// Returns the execution plan as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef;

    /// Specifies the output partitioning scheme of this plan
    fn output_partitioning(&self) -> Partitioning;

    /// If the output of this operator is sorted, returns `Some(keys)`
    /// with the description of how it was sorted.
    ///
    /// For example, Sort, (obviously) produces sorted output as does
    /// SortPreservingMergeStream. Less obviously `Projection`
    /// produces sorted output if its input was sorted as it does not
    /// reorder the input rows,
    ///
    /// It is safe to return `None` here if your operator does not
    /// have any particular output order here
    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]>;

    /// Specifies the data distribution requirements of all the
    /// children for this operator
    fn required_child_distribution(&self) -> Distribution {
        Distribution::UnspecifiedDistribution
    }

    /// Returns `true` if this operator relies on its inputs being
    /// produced in a certain order (for example that they are sorted
    /// a particular way) for correctness.
    ///
    /// If `true` is returned, DataFusion will not apply certain
    /// optimizations which might reorder the inputs (such as
    /// repartitioning to increase concurrency).
    ///
    /// The default implementation returns `true`
    ///
    /// WARNING: if you override this default and return `false`, your
    /// operator can not rely on datafusion preserving the input order
    /// as it will likely not.
    fn relies_on_input_order(&self) -> bool {
        true
    }

    /// Returns `false` if this operator's implementation may reorder
    /// rows within or between partitions.
    ///
    /// For example, Projection, Filter, and Limit maintain the order
    /// of inputs -- they may transform values (Projection) or not
    /// produce the same number of rows that went in (Filter and
    /// Limit), but the rows that are produced go in the same way.
    ///
    /// DataFusion uses this metadata to apply certain optimizations
    /// such as automatically repartitioning correctly.
    ///
    /// The default implementation returns `false`
    ///
    /// WARNING: if you override this default, you *MUST* ensure that
    /// the operator's maintains the ordering invariant or else
    /// DataFusion may produce incorrect results.
    fn maintains_input_order(&self) -> bool {
        false
    }

    /// Returns `true` if this operator would benefit from
    /// partitioning its input (and thus from more parallelism). For
    /// operators that do very little work the overhead of extra
    /// parallelism may outweigh any benefits
    ///
    /// The default implementation returns `true` unless this operator
    /// has signalled it requiers a single child input partition.
    fn benefits_from_input_partitioning(&self) -> bool {
        // By default try to maximize parallelism with more CPUs if
        // possible
        !matches!(
            self.required_child_distribution(),
            Distribution::SinglePartition
        )
    }

    /// Get a list of child execution plans that provide the input for this plan. The returned list
    /// will be empty for leaf nodes, will contain a single value for unary nodes, or two
    /// values for binary nodes (such as joins).
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>>;

    /// Returns a new plan where all children were replaced by new plans.
    /// The size of `children` must be equal to the size of `ExecutionPlan::children()`.
    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Creates an RecordBatch stream.
    async fn execute(
        &self,
        partition: usize,
        runtime: Arc<RuntimeEnv>,
    ) -> Result<SendableRecordBatchStream>;

    /// Return a snapshot of the set of [`Metric`]s for this
    /// [`ExecutionPlan`].
    ///
    /// While the values of the metrics in the returned
    /// [`MetricsSet`]s may change as execution progresses, the
    /// specific metrics will not.
    ///
    /// Once `self.execute()` has returned (technically the future is
    /// resolved) for all available partitions, the set of metrics
    /// should be complete. If this function is called prior to
    /// `execute()` new metrics may appear in subsequent calls.
    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    /// Format this `ExecutionPlan` to `f` in the specified type.
    ///
    /// Should not include a newline
    ///
    /// Note this function prints a placeholder by default to preserve
    /// backwards compatibility.
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ExecutionPlan(PlaceHolder)")
    }

    /// Returns the global output statistics for this `ExecutionPlan` node.
    fn statistics(&self) -> Statistics;
}

#[derive(Debug)]
pub struct ExecutionPlanAdapter(pub Arc<dyn DfExecutionPlan>);

#[async_trait]
impl ExecutionPlan for ExecutionPlanAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        // DataFusion's schema should always able to cast to our schema here, because it's
        // generated by our query engine and used in adaptor.
        Arc::new(self.0.schema().try_into().unwrap())
    }

    fn output_partitioning(&self) -> Partitioning {
        self.0.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        self.0
            .children()
            .into_iter()
            .map(|x| Arc::new(ExecutionPlanAdapter(x)) as _)
            .collect()
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let children = children
            .into_iter()
            .map(|x| Arc::new(DfExecutionPlanAdapter(x)) as _)
            .collect();
        let plan = self
            .0
            .with_new_children(children)
            .context(error::GeneralDataFusionSnafu)?;
        Ok(Arc::new(ExecutionPlanAdapter(plan)))
    }

    async fn execute(
        &self,
        partition: usize,
        runtime: Arc<RuntimeEnv>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self
            .0
            .execute(partition, runtime)
            .await
            .context(error::DataFusionExecutionPlanSnafu)?;
        let stream =
            RecordBatchStreamAdapter::try_new(stream).context(error::GeneralRecordBatchSnafu)?;
        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> Statistics {
        self.0.statistics()
    }
}

#[derive(Debug)]
pub struct DfExecutionPlanAdapter(pub Arc<dyn ExecutionPlan>);

#[async_trait]
impl DfExecutionPlan for DfExecutionPlanAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> DfSchemaRef {
        self.0.schema().arrow_schema().clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.0.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.0.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn DfExecutionPlan>> {
        self.0
            .children()
            .into_iter()
            .map(|x| Arc::new(DfExecutionPlanAdapter(x)) as _)
            .collect()
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn DfExecutionPlan>>,
    ) -> DfResult<Arc<dyn DfExecutionPlan>> {
        let children = children
            .into_iter()
            .map(|x| Arc::new(ExecutionPlanAdapter(x)) as _)
            .collect();
        let plan = self.0.with_new_children(children)?;
        Ok(Arc::new(DfExecutionPlanAdapter(plan)))
    }

    async fn execute(
        &self,
        partition: usize,
        runtime: Arc<RuntimeEnv>,
    ) -> DfResult<DfSendableRecordBatchStream> {
        let stream = self.0.execute(partition, runtime).await?;
        Ok(Box::pin(DfRecordBatchStreamAdapter::new(stream)))
    }

    fn statistics(&self) -> Statistics {
        self.0.statistics()
    }
}
