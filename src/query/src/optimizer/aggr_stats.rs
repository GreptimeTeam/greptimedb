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

use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_common::Result;
use datafusion_common::tree_node::{Transformed, TreeNode};
use table::table::scan::RegionScanExec;

use crate::optimizer::aggr_stats::support_aggr::SupportStatAggr;

pub(crate) mod stat_scan;
pub(crate) mod support_aggr;

/// Physical optimizer scaffold for aggregate-stats runtime rewrite.
///
/// This pass only owns query-shape eligibility and rewrite shape.
/// Runtime stats lookup/classification is handled during execution.
#[derive(Debug, Default)]
pub struct AggrStatsPhysicalRule;

impl PhysicalOptimizerRule for AggrStatsPhysicalRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Self::rewrite_plan_shape(plan)
    }

    fn name(&self) -> &str {
        "aggr_stats_physical"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

impl AggrStatsPhysicalRule {
    fn rewrite_plan_shape(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|plan| {
            let Some(_rewrite_target) = RewriteTarget::extract(&plan) else {
                return Ok(Transformed::no(plan));
            };
            // impl rewrite in RewriteTarget
            Ok(Transformed::no(plan))
        })
        .map(|res| res.data)
    }
}

/// TODO(discord9): support more kind of aggr
#[allow(unused)]
enum RewriteTarget<'a> {
    FinalOverPartial {
        final_exec: &'a AggregateExec,
        partial_exec: &'a AggregateExec,
        region_scan: &'a RegionScanExec,
        keep_coalesce: bool,
        aggr_exprs: Vec<SupportStatAggr>,
    },
}

#[allow(unused)]
impl<'a> RewriteTarget<'a> {
    fn is_supported_rewrite(&self) -> bool {
        let Self::FinalOverPartial {
            final_exec,
            partial_exec,
            region_scan,
            ..
        } = self;

        final_exec.group_expr().is_empty()
            && final_exec
                .aggr_expr()
                .iter()
                .all(|aggr| !aggr.is_distinct())
            && final_exec
                .filter_expr()
                .iter()
                .all(|filter| filter.is_none())
            && partial_exec
                .filter_expr()
                .iter()
                .all(|filter| filter.is_none())
            && region_scan.append_mode()
    }

    #[allow(unused)]
    fn extract(plan: &'a Arc<dyn ExecutionPlan>) -> Option<Self> {
        let aggregate_exec = plan.as_any().downcast_ref::<AggregateExec>()?;

        if !matches!(aggregate_exec.mode(), AggregateMode::Final) {
            return None;
        }

        let (input, keep_coalesce) = if let Some(coalesce) = aggregate_exec
            .input()
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>(
        ) {
            (coalesce.input(), true)
        } else {
            (aggregate_exec.input(), false)
        };

        let partial_exec = input.as_any().downcast_ref::<AggregateExec>()?;
        if !matches!(partial_exec.mode(), AggregateMode::Partial) {
            return None;
        }

        let region_scan = partial_exec
            .input()
            .as_any()
            .downcast_ref::<RegionScanExec>()?;
        let aggr_exprs = aggregate_exec
            .aggr_expr()
            .iter()
            .map(|aggr_expr| SupportStatAggr::from_aggr_expr(aggr_expr))
            .try_collect()?;
        let zelf = Self::FinalOverPartial {
            final_exec: aggregate_exec,
            partial_exec,
            region_scan,
            keep_coalesce,
            aggr_exprs,
        };
        if zelf.is_supported_rewrite() {
            Some(zelf)
        } else {
            None
        }
    }

    fn first_stage_aggregate(&self) -> &'a AggregateExec {
        match self {
            RewriteTarget::FinalOverPartial { partial_exec, .. } => partial_exec,
        }
    }

    fn region_scan(&self) -> &'a RegionScanExec {
        match self {
            RewriteTarget::FinalOverPartial { region_scan, .. } => region_scan,
        }
    }
}
