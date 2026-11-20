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

//! Answers part of a scalar COUNT from metadata.
//!
//! [`PartialAggrRule`] wraps the partial COUNT over a region scan in a
//! [`MetadataAggrExec`] when the region prepared a statistics split: the scan
//! is replaced by its residual scan, and the rows counted from metadata are
//! merged into the COUNT state once.

use std::fmt;
use std::sync::Arc;

use datafusion::config::ConfigOptions;
use datafusion::execution::context::TaskContext;
use datafusion::functions_aggregate::count::Count;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::execution_plan::{ChildrenPropertiesMode, ReplaceChildrenOptions};
use datafusion::physical_plan::expressions::{Column, Literal};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::arrow::array::{ArrayRef, Int64Array};
use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::arrow::record_batch::RecordBatch;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::AggregateUDF;
use datafusion_physical_expr::{EquivalenceProperties, PhysicalExpr};
use futures::{StreamExt, stream};

use crate::table::scan::RegionScanExec;

/// Adds rows counted from metadata to the COUNT states of a wrapped partial
/// aggregate over a residual scan.
///
/// The wrapped plan is a partial scalar COUNT whose state is one `Int64`
/// column. Partition 0 emits one extra state row holding `exact_rows` after
/// the wrapped plan's own rows. The final aggregate sums all state rows, so
/// the metadata rows are counted exactly once, however many partitions the
/// residual scan has and whether or not they emit rows.
#[derive(Debug)]
pub struct MetadataAggrExec {
    input: Arc<dyn ExecutionPlan>,
    exact_rows: usize,
    properties: Arc<PlanProperties>,
}

impl MetadataAggrExec {
    fn new(input: Arc<dyn ExecutionPlan>, exact_rows: usize) -> Self {
        let input_properties = input.properties();
        // The extra state row can differ from the input's rows, so the input's
        // ordering, equivalent values, uniqueness and partition keys do not hold.
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            Partitioning::UnknownPartitioning(
                input_properties.output_partitioning().partition_count(),
            ),
            input_properties.emission_type,
            input_properties.boundedness,
        ));
        Self {
            input,
            exact_rows,
            properties,
        }
    }

    /// Rows counted from metadata.
    pub fn exact_rows(&self) -> usize {
        self.exact_rows
    }

    fn state_row(&self) -> Result<RecordBatch> {
        let rows = i64::try_from(self.exact_rows)
            .map_err(|_| DataFusionError::Internal("metadata row count overflow".to_string()))?;
        let column: ArrayRef = Arc::new(Int64Array::from(vec![rows]));
        Ok(RecordBatch::try_new(self.input.schema(), vec![column])?)
    }
}

impl DisplayAs for MetadataAggrExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MetadataAggrExec: exact_rows={}", self.exact_rows)
    }
}

impl ExecutionPlan for MetadataAggrExec {
    fn name(&self) -> &str {
        "MetadataAggrExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = <[_; 1]>::try_from(children).map_err(|_| {
            DataFusionError::Internal("MetadataAggrExec requires exactly one child".to_string())
        })?;
        Ok(Arc::new(Self::new(input, self.exact_rows)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        if partition != 0 {
            return Ok(input);
        }
        let state = self.state_row()?;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.input.schema(),
            input.chain(stream::once(async move { Ok(state) })),
        )))
    }
}

/// Wraps a scalar COUNT over a region scan in a [`MetadataAggrExec`] when the
/// region prepared a statistics split.
///
/// It matches a partial or single-phase scalar COUNT over a region scan,
/// optionally through a projection. The COUNT must count rows: every argument
/// is a non-null literal or a non-nullable column. A single-phase aggregate
/// is split into a final aggregate over the wrapped partial one. Any other
/// plan, or a scan without a split, is left unchanged.
#[derive(Debug, Default)]
pub struct PartialAggrRule {
    is_count_state: Option<fn(&AggregateUDF) -> bool>,
}

impl PartialAggrRule {
    /// Uses the engine's predicate for a state wrapper around the builtin COUNT.
    pub fn with_count_state(is_count_state: fn(&AggregateUDF) -> bool) -> Self {
        Self {
            is_count_state: Some(is_count_state),
        }
    }

    fn counts_rows(&self, aggregate: &AggregateExec) -> bool {
        if !matches!(
            aggregate.mode(),
            AggregateMode::Partial | AggregateMode::Single | AggregateMode::SinglePartitioned
        ) || !aggregate.group_expr().is_empty()
            || aggregate.aggr_expr().len() != 1
            || aggregate.filter_expr().iter().any(Option::is_some)
        {
            return false;
        }
        let expr = &aggregate.aggr_expr()[0];
        let function = expr.fun();
        if expr.is_distinct()
            || !(function.inner().is::<Count>()
                || self.is_count_state.is_some_and(|matches| matches(function)))
        {
            return false;
        }
        let schema = aggregate.input_schema();
        expr.expressions().iter().all(|arg| {
            if let Some(literal) = arg.downcast_ref::<Literal>() {
                return !literal.value().is_null();
            }
            arg.downcast_ref::<Column>().is_some_and(|column| {
                schema
                    .fields()
                    .get(column.index())
                    .is_some_and(|field| field.name() == column.name() && !field.is_nullable())
            })
        })
    }

    /// Returns the region scan under `input`, through at most one projection.
    fn region_scan(input: &Arc<dyn ExecutionPlan>) -> Option<&RegionScanExec> {
        if let Some(scan) = input.downcast_ref::<RegionScanExec>() {
            return Some(scan);
        }
        input
            .downcast_ref::<ProjectionExec>()?
            .input()
            .downcast_ref::<RegionScanExec>()
    }

    fn partial(
        aggregate: &AggregateExec,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<AggregateExec>> {
        Ok(Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            aggregate.group_expr().clone(),
            aggregate.aggr_expr().to_vec(),
            aggregate.filter_expr().to_vec(),
            input,
            aggregate.input_schema(),
        )?))
    }

    fn rewrite(&self, plan: Arc<dyn ExecutionPlan>) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
        let Some(aggregate) = plan.downcast_ref::<AggregateExec>() else {
            return Ok(Transformed::no(plan));
        };
        if !self.counts_rows(aggregate) {
            return Ok(Transformed::no(plan));
        }
        let input = aggregate.input();
        let Some(scan) = Self::region_scan(input) else {
            return Ok(Transformed::no(plan));
        };
        // The merged state must be one Int64 COUNT state.
        let state = Self::partial(aggregate, input.clone())?.schema();
        if state.fields().len() != 1 || state.field(0).data_type() != &DataType::Int64 {
            return Ok(Transformed::no(plan));
        }
        let Some((exact_rows, residual)) = scan.take_statistics_split()? else {
            return Ok(Transformed::no(plan));
        };
        let residual: Arc<dyn ExecutionPlan> = Arc::new(residual);
        let input = if input.is::<RegionScanExec>() {
            residual
        } else {
            input.clone().replace_children(
                vec![residual],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )?
        };
        let wrapped: Arc<dyn ExecutionPlan> = Arc::new(MetadataAggrExec::new(
            Self::partial(aggregate, input)?,
            exact_rows,
        ));
        if aggregate.mode() == &AggregateMode::Partial {
            return Ok(Transformed::yes(wrapped));
        }
        Ok(Transformed::yes(Arc::new(AggregateExec::try_new(
            AggregateMode::Final,
            aggregate.group_expr().clone(),
            aggregate.aggr_expr().to_vec(),
            aggregate.filter_expr().to_vec(),
            wrapped,
            aggregate.input_schema(),
        )?)))
    }
}

impl PhysicalOptimizerRule for PartialAggrRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| self.rewrite(plan)).data()
    }

    fn name(&self) -> &str {
        "partial_metadata_aggr"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::prelude::SessionContext;
    use datafusion_common::arrow::array::AsArray;
    use datafusion_common::arrow::datatypes::{Field, Int64Type, Schema};

    use super::*;

    #[tokio::test]
    async fn test_metadata_rows_merge_into_one_state_row() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::Int64,
            true,
        )]));
        let state = |count: i64| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![count]))],
            )
            .unwrap()
        };
        // The last partition emits no state row.
        let input = MemorySourceConfig::try_new_exec(
            &[vec![state(5)], vec![state(7)], vec![]],
            schema,
            None,
        )
        .unwrap();
        let plan = Arc::new(MetadataAggrExec::new(input, 10));
        let ctx = SessionContext::new();
        let mut states = Vec::new();
        for partition in 0..3 {
            let batches = datafusion::physical_plan::common::collect(
                plan.execute(partition, ctx.task_ctx()).unwrap(),
            )
            .await
            .unwrap();
            states.push(
                batches
                    .iter()
                    .flat_map(|batch| {
                        batch
                            .column(0)
                            .as_primitive::<Int64Type>()
                            .values()
                            .to_vec()
                    })
                    .collect::<Vec<_>>(),
            );
        }
        assert_eq!(states, [vec![5, 10], vec![7], vec![]]);
    }
}
