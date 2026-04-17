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

use common_telemetry::debug;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion_common::Result as DfResult;
use datafusion_common::utils::expr::COUNT_STAR_EXPANSION;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::aggregate::AggregateFunctionExpr;
use datafusion_physical_expr::expressions::{Column, Literal};
use table::table::scan::RegionScanExec;

use super::StatsAgg;
use super::split::{StatsAggExt, has_partition_expr_mismatch};

#[derive(Debug)]
pub(super) struct RewriteCheck<'a> {
    aggregate_exec: &'a AggregateExec,
    region_scan: &'a RegionScanExec,
}

impl<'a> RewriteCheck<'a> {
    pub(super) fn new(aggregate_exec: &'a AggregateExec, region_scan: &'a RegionScanExec) -> Self {
        Self {
            aggregate_exec,
            region_scan,
        }
    }

    pub(super) fn skip_reason(&self) -> DfResult<Option<RejectReason>> {
        // MVP only handles global first-stage aggregates over append-mode region
        // scans. Anything else falls back to the normal execution path.
        if !self.region_scan.append_mode()
            || !self.aggregate_exec.group_expr().is_empty()
            || !matches!(
                self.aggregate_exec.mode(),
                AggregateMode::Partial | AggregateMode::Single | AggregateMode::SinglePartitioned
            )
            || self
                .aggregate_exec
                .filter_expr()
                .iter()
                .any(|expr| expr.is_some())
        {
            return Ok(Some(RejectReason::UnsupportedPlan));
        }

        let aggs = match self.parse_aggs() {
            Ok(aggs) => aggs,
            Err(reason) => return Ok(Some(reason)),
        };

        let scan_input_stats = self.try_scan_stats();
        if self.stats_unavailable(scan_input_stats.as_ref()) {
            return Ok(Some(RejectReason::StatsUnavailable));
        }

        if !aggs
            .iter()
            .all(|agg| agg.has_stats_files(scan_input_stats.as_ref().unwrap()))
        {
            return Ok(Some(RejectReason::NoStatsFiles));
        }

        Ok(None)
    }

    fn try_scan_stats(&self) -> Option<store_api::scan_stats::RegionScanStats> {
        match self.region_scan.scan_input_stats() {
            Ok(stats) => stats,
            Err(err) => {
                debug!(
                    "Skip aggregate stats optimization: failed to collect scan input stats: {err}"
                );
                None
            }
        }
    }

    pub(super) fn parse_aggs(&self) -> Result<Vec<StatsAgg>, RejectReason> {
        let aggr_exprs = self.aggregate_exec.aggr_expr();
        if aggr_exprs.is_empty() {
            return Err(RejectReason::UnsupportedAggregate);
        }

        aggr_exprs.iter().map(|expr| self.parse_agg(expr)).collect()
    }

    fn parse_agg(&self, expr: &AggregateFunctionExpr) -> Result<StatsAgg, RejectReason> {
        if !is_supported_aggregate_name(expr.fun().name()) {
            return Err(RejectReason::UnsupportedAggregate);
        }

        Self::check_agg_shape(expr)?;

        let inputs = expr.expressions();
        let name = expr.fun().name().to_ascii_lowercase();

        // COUNT(*) is usually rewrite to COUNT(time-index)
        // before this physical optimizer runs, so CountStar is mostly a defensive fallback
        if name == "count" && is_count_star_expr(&inputs) {
            return Ok(StatsAgg::CountStar);
        }

        if inputs.len() != 1 {
            return Err(RejectReason::UnsupportedAggregate);
        }

        let Some(column) = inputs[0].as_any().downcast_ref::<Column>() else {
            return Err(RejectReason::UnsupportedAggregate);
        };
        let column_name = column.name().to_string();
        let arg_type = inputs[0]
            .data_type(self.aggregate_exec.input_schema().as_ref())
            .map_err(|_| RejectReason::UnsupportedAggregate)?;

        if self.is_tag_column(&column_name) {
            return Err(RejectReason::UnsupportedPlan);
        }

        let is_time_index = column_name == self.region_scan.time_index();
        match (name.as_str(), is_time_index) {
            ("count", true) => Ok(StatsAgg::CountTimeIndex { arg_type }),
            ("count", false) => Ok(StatsAgg::CountField {
                column_name,
                arg_type,
            }),
            ("min", true) => Ok(StatsAgg::MinTimeIndex { arg_type }),
            ("min", false) => Ok(StatsAgg::MinField {
                column_name,
                arg_type,
            }),
            ("max", true) => Ok(StatsAgg::MaxTimeIndex { arg_type }),
            ("max", false) => Ok(StatsAgg::MaxField {
                column_name,
                arg_type,
            }),
            _ => Err(RejectReason::UnsupportedAggregate),
        }
    }

    pub(super) fn check_agg_shape(expr: &AggregateFunctionExpr) -> Result<(), RejectReason> {
        if expr.is_distinct() || expr.is_reversed() || !expr.order_bys().is_empty() {
            return Err(RejectReason::UnsupportedAggregate);
        }

        Ok(())
    }

    fn is_tag_column(&self, column_name: &str) -> bool {
        self.region_scan
            .tag_columns()
            .iter()
            .any(|tag| tag == column_name)
    }

    fn stats_unavailable(
        &self,
        scan_input_stats: Option<&store_api::scan_stats::RegionScanStats>,
    ) -> bool {
        // These cases keep the plan shape eligible in principle, but the scan cannot
        // provide metadata that is trustworthy enough for a safe stats-only rewrite.
        self.region_scan.has_predicate_without_region()
            || scan_input_stats.is_none()
            || has_partition_expr_mismatch(scan_input_stats)
    }
}

#[derive(Debug)]
pub(super) enum RejectReason {
    /// The physical plan shape is outside the current safe rewrite envelope.
    UnsupportedPlan,
    /// At least one aggregate function or aggregate shape is unsupported.
    UnsupportedAggregate,
    /// The scan cannot provide trustworthy stats for this rewrite attempt.
    StatsUnavailable,
    /// The aggregate shape is supported, but no file contributes metadata-only results.
    NoStatsFiles,
}

impl std::fmt::Display for RejectReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RejectReason::UnsupportedPlan => write!(
                f,
                "aggregate stats MVP does not support this plan shape safely"
            ),
            RejectReason::UnsupportedAggregate => write!(
                f,
                "aggregate stats MVP only supports the narrowed no-GROUP-BY count/min/max matrix"
            ),
            RejectReason::StatsUnavailable => write!(
                f,
                "aggregate stats MVP found a supported shape, but trustworthy statistics are unavailable for it"
            ),
            RejectReason::NoStatsFiles => write!(
                f,
                "aggregate stats rewrite requires at least one stats-backed file after safety checks"
            ),
        }
    }
}

pub(super) fn is_supported_aggregate_name(name: &str) -> bool {
    matches!(name.to_ascii_lowercase().as_str(), "min" | "max" | "count")
}

fn is_count_star_expr(inputs: &[std::sync::Arc<dyn PhysicalExpr>]) -> bool {
    match inputs {
        // Keep the legacy empty-input shape as a compatibility fallback.
        [] => true,
        [arg] => arg
            .as_any()
            .downcast_ref::<Literal>()
            .is_some_and(|lit| lit.value() == &COUNT_STAR_EXPANSION),
        _ => false,
    }
}
