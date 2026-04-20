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

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StructArray};
use arrow::record_batch::RecordBatch;
use common_error::ext::BoxedError;
use common_telemetry::debug;
use datafusion::config::ConfigOptions;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DataFusionError, Result as DfResult, ScalarValue};
use datafusion_physical_expr::aggregate::AggregateFunctionExpr;
use datatypes::arrow::datatypes::DataType;
use table::table::scan::{AggregateStatsExplain, RegionScanExec};

mod check;
mod split;
#[cfg(test)]
mod tests;

use check::RewriteCheck;
use split::{common_stats_file_ordinals, filter_stats_by_file_ordinals, partial_state_from_stats};

use crate::metrics::{
    AGGREGATE_STATS_SCANNED_INPUT_FILES_TOTAL, AGGREGATE_STATS_STATS_INPUT_FILES_TOTAL,
};

enum RewriteTarget<'a> {
    SingleStage {
        aggregate_exec: &'a AggregateExec,
        region_scan: &'a RegionScanExec,
    },
    FinalOverPartial {
        final_exec: &'a AggregateExec,
        partial_exec: &'a AggregateExec,
        region_scan: &'a RegionScanExec,
        keep_coalesce: bool,
    },
}

impl<'a> RewriteTarget<'a> {
    fn extract(plan: &'a Arc<dyn ExecutionPlan>) -> Option<Self> {
        let aggregate_exec = plan.as_any().downcast_ref::<AggregateExec>()?;

        if matches!(
            aggregate_exec.mode(),
            AggregateMode::Single | AggregateMode::SinglePartitioned
        ) {
            let region_scan = AggregateStats::extract_region_scan(aggregate_exec.input())?;
            return Some(Self::SingleStage {
                aggregate_exec,
                region_scan,
            });
        }

        if !matches!(aggregate_exec.mode(), AggregateMode::Final) {
            return None;
        }

        if let Some(coalesce) = aggregate_exec
            .input()
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>()
        {
            let partial_exec = coalesce.input().as_any().downcast_ref::<AggregateExec>()?;
            if !matches!(partial_exec.mode(), AggregateMode::Partial) {
                return None;
            }

            let region_scan = AggregateStats::extract_region_scan(partial_exec.input())?;
            return Some(Self::FinalOverPartial {
                final_exec: aggregate_exec,
                partial_exec,
                region_scan,
                keep_coalesce: true,
            });
        }

        let partial_exec = aggregate_exec
            .input()
            .as_any()
            .downcast_ref::<AggregateExec>()?;
        if !matches!(partial_exec.mode(), AggregateMode::Partial) {
            return None;
        }

        let region_scan = AggregateStats::extract_region_scan(partial_exec.input())?;
        Some(Self::FinalOverPartial {
            final_exec: aggregate_exec,
            partial_exec,
            region_scan,
            keep_coalesce: false,
        })
    }

    fn first_stage_aggregate(&self) -> &'a AggregateExec {
        match self {
            RewriteTarget::SingleStage { aggregate_exec, .. } => aggregate_exec,
            RewriteTarget::FinalOverPartial { partial_exec, .. } => partial_exec,
        }
    }

    fn region_scan(&self) -> &'a RegionScanExec {
        match self {
            RewriteTarget::SingleStage { region_scan, .. }
            | RewriteTarget::FinalOverPartial { region_scan, .. } => region_scan,
        }
    }
}

#[derive(Debug)]
pub struct AggregateStats;

/// All supported aggregate from statistics
#[derive(Debug, Clone, PartialEq, Eq)]
enum StatsAgg {
    CountStar,
    CountField {
        column_name: String,
        arg_type: DataType,
    },
    CountTimeIndex {
        arg_type: DataType,
    },
    MinField {
        column_name: String,
        arg_type: DataType,
    },
    MinTimeIndex {
        arg_type: DataType,
    },
    MaxField {
        column_name: String,
        arg_type: DataType,
    },
    MaxTimeIndex {
        arg_type: DataType,
    },
}

impl PhysicalOptimizerRule for AggregateStats {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Self::do_optimize(plan)
    }

    fn name(&self) -> &str {
        "aggregate_stats"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

impl AggregateStats {
    fn do_optimize(plan: Arc<dyn ExecutionPlan>) -> DfResult<Arc<dyn ExecutionPlan>> {
        let result = plan
            .transform_down(|plan| {
                let Some(target) = RewriteTarget::extract(&plan) else {
                    return Ok(Transformed::no(plan));
                };
                let Some(scan_input_stats) = target.region_scan().scan_input_stats()? else {
                    Self::record_rewrite_miss(None);
                    return Ok(Transformed::no(plan));
                };

                let check = RewriteCheck::new(target.first_stage_aggregate(), target.region_scan());
                if let Some(reason) = check.skip_reason()? {
                    debug!("Skip aggregate stats optimization: {reason}");
                    Self::record_rewrite_miss(
                        target
                            .region_scan()
                            .scan_input_stats()
                            .ok()
                            .flatten()
                            .map(|stats| stats.files.len()),
                    );
                    return Ok(Transformed::no(plan));
                }

                let aggs = check.parse_aggs().map_err(|reason| {
                    DataFusionError::Internal(format!(
                        "aggregate stats rewrite became ineligible after eligibility check: {reason}"
                    ))
                })?;

                let excluded_file_ordinals = common_stats_file_ordinals(&aggs, &scan_input_stats);
                if excluded_file_ordinals.is_empty() {
                    debug!(
                        "Skip aggregate stats optimization: no shared stats-covered files across aggregates"
                    );
                    Self::record_rewrite_miss(Some(scan_input_stats.files.len()));
                    return Ok(Transformed::no(plan));
                }

                let rewritten = Self::rewrite_aggregate(
                    &target,
                    &aggs,
                    &scan_input_stats,
                    &excluded_file_ordinals,
                )?;

                Self::record_rewrite_hit(scan_input_stats.files.len(), excluded_file_ordinals.len());

                Ok(Transformed::yes(rewritten))
            })?
            .data;

        Ok(result)
    }

    fn extract_region_scan(plan: &Arc<dyn ExecutionPlan>) -> Option<&RegionScanExec> {
        plan.as_any().downcast_ref::<RegionScanExec>()
    }

    fn rewrite_aggregate(
        target: &RewriteTarget<'_>,
        aggs: &[StatsAgg],
        scan_input_stats: &store_api::scan_stats::RegionScanStats,
        excluded_file_ordinals: &[usize],
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        match target {
            RewriteTarget::SingleStage {
                aggregate_exec,
                region_scan,
            } => Self::rewrite_single_stage(
                aggregate_exec,
                region_scan,
                aggs,
                scan_input_stats,
                excluded_file_ordinals,
            ),
            RewriteTarget::FinalOverPartial {
                final_exec,
                partial_exec,
                region_scan,
                keep_coalesce,
            } => Self::rewrite_final_over_partial(
                final_exec,
                partial_exec,
                region_scan,
                *keep_coalesce,
                aggs,
                scan_input_stats,
                excluded_file_ordinals,
            ),
        }
    }

    fn rewrite_single_stage(
        aggregate_exec: &AggregateExec,
        region_scan: &RegionScanExec,
        aggs: &[StatsAgg],
        scan_input_stats: &store_api::scan_stats::RegionScanStats,
        excluded_file_ordinals: &[usize],
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let union = Self::build_partial_union_source(
            aggregate_exec,
            region_scan,
            aggs,
            scan_input_stats,
            excluded_file_ordinals,
        )?;
        let union = Self::coalesce_if_needed(union);

        Ok(Arc::new(
            AggregateExec::try_new(
                AggregateMode::Final,
                aggregate_exec.group_expr().clone(),
                aggregate_exec.aggr_expr().to_vec(),
                vec![None; aggregate_exec.aggr_expr().len()],
                union,
                aggregate_exec.input_schema(),
            )?
            .with_limit_options(aggregate_exec.limit_options()),
        ))
    }

    fn rewrite_final_over_partial(
        final_exec: &AggregateExec,
        partial_exec: &AggregateExec,
        region_scan: &RegionScanExec,
        keep_coalesce: bool,
        aggs: &[StatsAgg],
        scan_input_stats: &store_api::scan_stats::RegionScanStats,
        excluded_file_ordinals: &[usize],
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let partial_source = Self::build_partial_union_source(
            partial_exec,
            region_scan,
            aggs,
            scan_input_stats,
            excluded_file_ordinals,
        )?;
        let final_input = if keep_coalesce {
            Arc::new(CoalescePartitionsExec::new(partial_source)) as Arc<dyn ExecutionPlan>
        } else if partial_source.output_partitioning().partition_count() > 1 {
            Arc::new(CoalescePartitionsExec::new(partial_source)) as Arc<dyn ExecutionPlan>
        } else {
            partial_source
        };

        Ok(Arc::new(
            AggregateExec::try_new(
                *final_exec.mode(),
                final_exec.group_expr().clone(),
                final_exec.aggr_expr().to_vec(),
                final_exec.filter_expr().to_vec(),
                final_input,
                final_exec.input_schema(),
            )?
            .with_limit_options(final_exec.limit_options()),
        ))
    }

    fn build_partial_union_source(
        aggregate_exec: &AggregateExec,
        region_scan: &RegionScanExec,
        aggs: &[StatsAgg],
        scan_input_stats: &store_api::scan_stats::RegionScanStats,
        excluded_file_ordinals: &[usize],
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let stats_scan_input =
            filter_stats_by_file_ordinals(scan_input_stats, excluded_file_ordinals);
        let stats_states = aggs
            .iter()
            .map(|agg| {
                partial_state_from_stats(agg, &stats_scan_input)?.ok_or_else(|| {
                    DataFusionError::Internal(
                        "missing stats-derived partial state for excluded files".to_string(),
                    )
                })
            })
            .collect::<DfResult<Vec<_>>>()?;

        let filtered_scan = Arc::new(
            region_scan
                .with_excluded_file_ordinals(excluded_file_ordinals.to_vec())
                .map_err(boxed_external)?
                .with_aggregate_stats_explain(AggregateStatsExplain {
                    stats_file_ids: stats_scan_input
                        .files
                        .iter()
                        .map(|file| file.file_id)
                        .collect(),
                }),
        );

        let partial_scan = Arc::new(
            AggregateExec::try_new(
                AggregateMode::Partial,
                aggregate_exec.group_expr().clone(),
                aggregate_exec.aggr_expr().to_vec(),
                aggregate_exec.filter_expr().to_vec(),
                filtered_scan,
                aggregate_exec.input_schema(),
            )?
            .with_limit_options(aggregate_exec.limit_options()),
        );

        let stats_input = Self::build_stats_input(aggregate_exec.aggr_expr(), stats_states)?;
        UnionExec::try_new(vec![partial_scan, stats_input])
    }

    fn coalesce_if_needed(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        if plan.output_partitioning().partition_count() > 1 {
            Arc::new(CoalescePartitionsExec::new(plan))
        } else {
            plan
        }
    }

    fn build_stats_input(
        aggr_exprs: &[Arc<AggregateFunctionExpr>],
        stats_states: Vec<ScalarValue>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let fields = aggr_exprs.iter().try_fold(Vec::new(), |mut fields, expr| {
            fields.extend(expr.state_fields()?);
            Ok::<_, DataFusionError>(fields)
        })?;
        let schema = Arc::new(arrow::datatypes::Schema::new(fields));

        let columns = stats_states
            .into_iter()
            .try_fold(Vec::new(), |mut columns, state| {
                columns.extend(Self::state_columns(state)?);
                Ok::<_, DataFusionError>(columns)
            })?;
        let batch = RecordBatch::try_new(schema.clone(), columns)
            .map_err(|err| DataFusionError::ArrowError(Box::new(err), None))?;

        Ok(DataSourceExec::from_data_source(
            MemorySourceConfig::try_new(&[vec![batch]], schema, None)?,
        ))
    }

    fn state_columns(state: ScalarValue) -> DfResult<Vec<ArrayRef>> {
        let ScalarValue::Struct(array) = state else {
            return Err(DataFusionError::Internal(
                "aggregate stats rewrite expected a struct partial state".to_string(),
            ));
        };

        let struct_array = array
            .as_ref()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "aggregate stats rewrite expected a struct array partial state".to_string(),
                )
            })?;
        Ok(struct_array.columns().to_vec())
    }

    fn record_rewrite_hit(total_files: usize, excluded_files: usize) {
        let fallback_files = total_files.saturating_sub(excluded_files);
        AGGREGATE_STATS_STATS_INPUT_FILES_TOTAL.inc_by(excluded_files as u64);
        AGGREGATE_STATS_SCANNED_INPUT_FILES_TOTAL.inc_by(fallback_files as u64);
    }

    fn record_rewrite_miss(total_files: Option<usize>) {
        if let Some(total_files) = total_files {
            AGGREGATE_STATS_SCANNED_INPUT_FILES_TOTAL.inc_by(total_files as u64);
        }
    }
}

fn boxed_external(err: BoxedError) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}
