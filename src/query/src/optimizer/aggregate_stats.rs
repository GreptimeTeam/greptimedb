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

use common_telemetry::debug;
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion_common::Result as DfResult;
use datafusion_common::tree_node::{Transformed, TreeNode};
use table::table::scan::RegionScanExec;

#[derive(Debug)]
pub struct AggregateStats;

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
                let Some(aggregate_exec) = plan.as_any().downcast_ref::<AggregateExec>() else {
                    return Ok(Transformed::no(plan));
                };

                let Some(region_scan) = Self::extract_region_scan(aggregate_exec) else {
                    return Ok(Transformed::no(plan));
                };

                let eligibility = AggregateStatsEligibility::new(aggregate_exec, region_scan);
                if let Some(reason) = eligibility.ineligible_reason() {
                    debug!("Skip aggregate stats optimization: {reason}");
                    return Ok(Transformed::no(plan));
                }

                // TODO(ruihang): implement mixed stats-plus-scan rewrite in follow-up tasks.
                Ok(Transformed::no(plan))
            })?
            .data;

        Ok(result)
    }

    fn extract_region_scan(aggregate_exec: &AggregateExec) -> Option<&RegionScanExec> {
        let child = aggregate_exec.children().into_iter().next()?;
        child.as_any().downcast_ref::<RegionScanExec>()
    }
}

#[derive(Debug)]
struct AggregateStatsEligibility<'a> {
    aggregate_exec: &'a AggregateExec,
    region_scan: &'a RegionScanExec,
}

impl<'a> AggregateStatsEligibility<'a> {
    fn new(aggregate_exec: &'a AggregateExec, region_scan: &'a RegionScanExec) -> Self {
        Self {
            aggregate_exec,
            region_scan,
        }
    }

    fn ineligible_reason(&self) -> Option<EligibilityRejection> {
        if !self.region_scan.append_mode() {
            return Some(EligibilityRejection::NonAppendOnly);
        }

        if !self.aggregate_exec.group_expr().is_empty() {
            return Some(EligibilityRejection::GroupedAggregate);
        }

        if !self.has_supported_aggregates() {
            return Some(EligibilityRejection::UnsupportedAggregate);
        }

        if !self.has_stats_eligible_candidates() {
            return Some(EligibilityRejection::NoStatsEligibleFiles);
        }

        None
    }

    fn has_supported_aggregates(&self) -> bool {
        let aggr_exprs = self.aggregate_exec.aggr_expr();
        !aggr_exprs.is_empty()
            && aggr_exprs
                .iter()
                .all(|expr| is_supported_aggregate_name(expr.name()))
    }

    fn has_stats_eligible_candidates(&self) -> bool {
        // TODO(ruihang): replace this scaffold with per-file stats classification.
        self.region_scan.total_rows() > 0
    }
}

#[derive(Debug)]
enum EligibilityRejection {
    NonAppendOnly,
    GroupedAggregate,
    UnsupportedAggregate,
    NoStatsEligibleFiles,
}

impl std::fmt::Display for EligibilityRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EligibilityRejection::NonAppendOnly => {
                write!(f, "aggregate stats MVP only supports append-only scans")
            }
            EligibilityRejection::GroupedAggregate => {
                write!(f, "aggregate stats MVP does not support GROUP BY yet")
            }
            EligibilityRejection::UnsupportedAggregate => {
                write!(
                    f,
                    "aggregate stats MVP only supports min/max/count aggregates"
                )
            }
            EligibilityRejection::NoStatsEligibleFiles => {
                write!(
                    f,
                    "aggregate stats rewrite requires at least one stats-eligible file"
                )
            }
        }
    }
}

fn is_supported_aggregate_name(name: &str) -> bool {
    let normalized = name.split('(').next().unwrap_or(name).to_ascii_lowercase();
    matches!(normalized.as_str(), "min" | "max" | "count")
}

#[cfg(test)]
mod tests {
    use super::is_supported_aggregate_name;

    #[test]
    fn test_supported_aggregate_names() {
        assert!(is_supported_aggregate_name("min"));
        assert!(is_supported_aggregate_name("max(value)"));
        assert!(is_supported_aggregate_name("count(*)"));
        assert!(!is_supported_aggregate_name("sum(value)"));
        assert!(!is_supported_aggregate_name("avg(value)"));
    }
}
