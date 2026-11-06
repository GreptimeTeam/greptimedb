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

//! Independent statistics for source-disjoint scans, and COUNT state merging.

use std::fmt;
use std::sync::Arc;

use common_recordbatch::QueryMemoryTracker;
use datafusion::config::ConfigOptions;
use datafusion::execution::context::TaskContext;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::execution_plan::{ChildrenPropertiesMode, ReplaceChildrenOptions};
use datafusion::physical_plan::expressions::lit;
use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs, StatisticsContext};
use datafusion::physical_plan::udaf::StatisticsArgs as AggregateStatisticsArgs;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion_common::{DataFusionError, Result, ScalarValue, Statistics};
use datafusion_physical_expr::PhysicalExpr;
use store_api::region_engine::RegionScannerRef;
use store_api::storage::ScanRequest;

use crate::table::scan::RegionScanExec;

macro_rules! statistics_scan {
    ($name:ident, $exact:literal) => {
        /// A scan retaining the statistics of one source-disjoint subset.
        #[derive(Debug)]
        pub struct $name {
            input: Arc<dyn ExecutionPlan>,
            source_rows: Option<usize>,
        }

        impl DisplayAs for $name {
            fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
                write!(
                    f,
                    "{}: source_rows={:?}",
                    stringify!($name),
                    self.source_rows
                )
            }
        }

        impl ExecutionPlan for $name {
            fn name(&self) -> &str {
                stringify!($name)
            }

            fn properties(&self) -> &Arc<PlanProperties> {
                self.input.properties()
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
                vec![true]
            }

            fn with_new_children(
                self: Arc<Self>,
                children: Vec<Arc<dyn ExecutionPlan>>,
            ) -> Result<Arc<dyn ExecutionPlan>> {
                if children.len() != 1 {
                    return Err(DataFusionError::Internal(
                        "statistics scan requires exactly one child".to_string(),
                    ));
                }
                Ok(Arc::new(Self {
                    input: children[0].clone(),
                    source_rows: self.source_rows,
                }))
            }

            fn execute(
                &self,
                partition: usize,
                context: Arc<TaskContext>,
            ) -> Result<SendableRecordBatchStream> {
                self.input.execute(partition, context)
            }

            fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
                let input = self.input.clone().reset_state()?;
                Ok(Arc::new(Self {
                    input,
                    source_rows: self.source_rows,
                }))
            }

            fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
                vec![if $exact {
                    ChildStats::At(partition)
                } else {
                    ChildStats::Skip
                }]
            }

            fn statistics_from_inputs(
                &self,
                input_stats: &[Arc<Statistics>],
                args: &StatisticsArgs,
            ) -> Result<Arc<Statistics>> {
                if $exact {
                    // The child invalidates prepared counts on dynamic filters.
                    return Ok(input_stats.first().cloned().unwrap_or_else(|| {
                        Arc::new(Statistics::new_unknown(self.schema().as_ref()))
                    }));
                }
                let mut stats = Statistics::new_unknown(self.schema().as_ref());
                if args.partition().is_none() {
                    stats.num_rows = self
                        .source_rows
                        .map(Precision::Inexact)
                        .unwrap_or(Precision::Absent);
                }
                Ok(Arc::new(stats))
            }
        }
    };
}

statistics_scan!(ExactRegionScanExec, true);
statistics_scan!(InexactRegionScanExec, false);

/// Builds the scan plan, replacing the scanner with a union of its exact and
/// inexact parts when the engine prepared a COUNT split.
pub fn statistics_scan_plan(
    mut scanner: RegionScannerRef,
    request: ScanRequest,
    query_memory_tracker: Option<QueryMemoryTracker>,
    explain_verbose: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    let build = |scanner, request, tracker| -> Result<Arc<dyn ExecutionPlan>> {
        let mut scan = RegionScanExec::new(scanner, request, tracker)?;
        scan.set_explain_verbose(explain_verbose);
        Ok(Arc::new(scan))
    };
    let Some(parts) = scanner.take_statistics_split() else {
        return build(scanner, request, query_memory_tracker);
    };
    let source_rows = parts.exact.properties().exact_num_rows();
    let exact: Arc<dyn ExecutionPlan> = Arc::new(ExactRegionScanExec {
        input: build(parts.exact, request.clone(), query_memory_tracker.clone())?,
        source_rows,
    });
    let inexact: Arc<dyn ExecutionPlan> = Arc::new(InexactRegionScanExec {
        input: build(parts.inexact, request, query_memory_tracker)?,
        source_rows: parts.inexact_rows,
    });
    UnionExec::try_new(vec![exact, inexact])
}

/// Distributes scalar COUNT states over the two statistics branches.
///
/// The exact branch becomes one COUNT state, not one input row to be counted.
#[derive(Debug, Default)]
pub struct PartialCountRule {
    count_state_function: Option<String>,
}

impl PartialCountRule {
    /// Recognizes an engine COUNT-state wrapper without depending on its implementation.
    pub fn with_count_state(function: String) -> Self {
        Self {
            count_state_function: Some(function),
        }
    }

    fn inputs(plan: &Arc<dyn ExecutionPlan>) -> Result<Option<Vec<Arc<dyn ExecutionPlan>>>> {
        if let Some(union) = plan.downcast_ref::<UnionExec>() {
            if union
                .inputs()
                .iter()
                .any(|input| input.is::<ExactRegionScanExec>())
                && union.inputs().iter().all(|input| {
                    input.is::<ExactRegionScanExec>() || input.is::<InexactRegionScanExec>()
                })
            {
                return Ok(Some(union.inputs().clone()));
            }
            return Ok(None);
        }
        if let Some(projection) = plan.downcast_ref::<ProjectionExec>()
            && let Some(inputs) = Self::inputs(projection.input())?
        {
            return inputs
                .into_iter()
                .map(|input| {
                    plan.clone().replace_children(
                        vec![input],
                        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
                    )
                })
                .collect::<Result<Vec<_>>>()
                .map(Some);
        }
        Ok(None)
    }

    fn partial(
        aggregate: &AggregateExec,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let stats = StatisticsContext::new().compute(input.as_ref(), &StatisticsArgs::new())?;
        let expr = &aggregate.aggr_expr()[0];
        let args = expr.expressions();
        let field = expr.field();
        let value = expr.fun().value_from_stats(&AggregateStatisticsArgs {
            statistics: &stats,
            return_type: field.data_type(),
            is_distinct: false,
            exprs: args.as_slice(),
        });
        // State-wrapper aggregates return a struct, while their Partial mode
        // emits the inner COUNT state as one scalar field.
        let value = match value {
            Some(ScalarValue::Struct(array))
                if array.num_columns() == 1 && array.column(0).len() == 1 =>
            {
                Some(ScalarValue::try_from_array(array.column(0), 0)?)
            }
            value => value,
        };
        let partial = AggregateExec::try_new(
            AggregateMode::Partial,
            aggregate.group_expr().clone(),
            aggregate.aggr_expr().to_vec(),
            aggregate.filter_expr().to_vec(),
            input,
            aggregate.input_schema(),
        )?;
        if let Some(value) = value
            && partial.schema().fields().len() == 1
            && value.data_type() == *partial.schema().field(0).data_type()
        {
            return Ok(Arc::new(ProjectionExec::try_new(
                vec![(lit(value), partial.schema().field(0).name().clone())],
                Arc::new(PlaceholderRowExec::new(partial.schema())),
            )?));
        }
        Ok(Arc::new(partial))
    }

    fn rewrite(&self, plan: Arc<dyn ExecutionPlan>) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
        let Some(aggregate) = plan.downcast_ref::<AggregateExec>() else {
            return Ok(Transformed::no(plan));
        };
        if !matches!(
            aggregate.mode(),
            AggregateMode::Partial | AggregateMode::Single | AggregateMode::SinglePartitioned
        ) || !aggregate.group_expr().is_empty()
            || aggregate.aggr_expr().len() != 1
            || (aggregate.aggr_expr()[0].fun().name() != "count"
                && self.count_state_function.as_deref()
                    != Some(aggregate.aggr_expr()[0].fun().name()))
            || aggregate.aggr_expr()[0].is_distinct()
            || aggregate.filter_expr().iter().any(Option::is_some)
        {
            return Ok(Transformed::no(plan));
        }
        let Some(inputs) = Self::inputs(aggregate.input())? else {
            return Ok(Transformed::no(plan));
        };
        let states = inputs
            .into_iter()
            .map(|input| Self::partial(aggregate, input))
            .collect::<Result<Vec<_>>>()?;
        let union = UnionExec::try_new(states)?;
        if aggregate.mode() == &AggregateMode::Partial {
            return Ok(Transformed::yes(union));
        }
        Ok(Transformed::yes(Arc::new(AggregateExec::try_new(
            AggregateMode::Final,
            aggregate.group_expr().clone(),
            aggregate.aggr_expr().to_vec(),
            vec![None],
            union,
            aggregate.input_schema(),
        )?)))
    }
}

impl PhysicalOptimizerRule for PartialCountRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| self.rewrite(plan)).data()
    }

    fn name(&self) -> &str {
        "partial_metadata_count"
    }

    fn schema_check(&self) -> bool {
        // Folding COUNT state to a literal can tighten nullability.
        false
    }
}
