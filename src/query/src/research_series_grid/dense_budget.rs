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

//! Admission of dense per-series grids over long ranges.
//!
//! Unaggregated fragments and series-level joins keep one full grid per selected
//! series. That is only a problem when many series are selected over a long range
//! (e.g. churned short-lived series), so the decision uses the selected series
//! count instead of the grid length alone.

use std::future::Future;

use arrow_schema::DataType;
use common_query::request::SeriesSourceRequest;
use common_recordbatch::OrderOption;
use datafusion::arrow::compute::SortOptions;
use datafusion_common::Result;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_expr::LogicalPlan;
use promql_parser::parser::Expr;
use session::context::QueryContextRef;

use crate::dist_plan::DistExtensionPlanner;

/// Grid length up to which dense per-series grids are always admitted.
pub(crate) const DENSE_GRID_POINTS: usize = 1024;
/// Upper bound for the dense grids of all selected series of one query.
const DENSE_BYTES_BUDGET: usize = 1 << 30;

tokio::task_local! {
    static LONG_GRID_ADMITTED: bool;
}

/// Runs research planning with the long-grid admission decided for this query.
pub(crate) async fn scope<F: Future>(admitted: bool, planning: F) -> F::Output {
    LONG_GRID_ADMITTED.scope(admitted, planning).await
}

/// Whether a dense per-series grid of `points` may be planned.
pub(crate) fn admits(points: usize) -> bool {
    points <= DENSE_GRID_POINTS
        || LONG_GRID_ADMITTED
            .try_with(|admitted| *admitted)
            .unwrap_or(false)
}

/// Whether planning `expr` may need dense per-series grids: a series-level join, a
/// result that still carries input series, or a top-k selection. Fully aggregated
/// queries skip the selection count, which costs a full series selection.
pub(crate) fn may_need_dense(expr: &Expr) -> bool {
    fn keeps_series(expr: &Expr) -> bool {
        match expr {
            Expr::VectorSelector(_) | Expr::MatrixSelector(_) => true,
            Expr::Paren(paren) => keeps_series(&paren.expr),
            Expr::Unary(unary) => keeps_series(&unary.expr),
            Expr::Subquery(subquery) => keeps_series(&subquery.expr),
            Expr::Binary(binary) => keeps_series(&binary.lhs) || keeps_series(&binary.rhs),
            Expr::Call(call) => call.args.args.iter().any(|arg| keeps_series(arg)),
            Expr::Aggregate(aggregate) => is_top_k(aggregate),
            _ => false,
        }
    }
    fn has_top_k(expr: &Expr) -> bool {
        match expr {
            Expr::Aggregate(aggregate) => is_top_k(aggregate) || has_top_k(&aggregate.expr),
            Expr::Paren(paren) => has_top_k(&paren.expr),
            Expr::Unary(unary) => has_top_k(&unary.expr),
            Expr::Subquery(subquery) => has_top_k(&subquery.expr),
            Expr::Binary(binary) => has_top_k(&binary.lhs) || has_top_k(&binary.rhs),
            Expr::Call(call) => call.args.args.iter().any(|arg| has_top_k(arg)),
            _ => false,
        }
    }
    fn is_top_k(aggregate: &promql_parser::parser::AggregateExpr) -> bool {
        matches!(aggregate.op.to_string().as_str(), "topk" | "bottomk")
    }
    super::online_frontier::has_series_level_join(expr) || keeps_series(expr) || has_top_k(expr)
}

pub(crate) fn fits(series: usize, points: usize) -> bool {
    series
        .saturating_mul(points)
        .saturating_mul(size_of::<Option<f64>>())
        <= DENSE_BYTES_BUDGET
}

/// Counts the series the region sources select for `inputs` (raw selector plans).
/// `None` means the count is unavailable; callers must then keep the length limit.
pub(crate) async fn selected_series(
    planner: &DistExtensionPlanner,
    inputs: &[LogicalPlan],
    query_ctx: &QueryContextRef,
    session_id: &str,
) -> Result<Option<usize>> {
    let header = api::v1::region::RegionRequestHeader {
        tracing_context: common_telemetry::tracing_context::TracingContext::from_json(session_id)
            .to_w3c(),
        dbname: query_ctx.get_db_string(),
        query_context: Some(query_ctx.as_ref().into()),
    };
    let mut total = 0usize;
    for input in inputs {
        let Some(scan) = table_scan(input)? else {
            common_telemetry::debug!("Research long-grid count: no table scan");
            return Ok(None);
        };
        let LogicalPlan::TableScan(table_scan) = &scan else {
            return Ok(None);
        };
        let schema = table_scan.source.schema();
        let fields = schema.fields();
        let (Some(tsid), Some(time)) = (
            fields.iter().find(|field| field.name() == "__tsid"),
            fields
                .iter()
                .find(|field| matches!(field.data_type(), DataType::Timestamp(_, _))),
        ) else {
            common_telemetry::debug!(schema = ?schema, "Research long-grid count: no tsid or time column");
            return Ok(None);
        };
        let ordering = [tsid, time]
            .into_iter()
            .map(|field| OrderOption {
                name: field.name().clone(),
                options: SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            })
            .collect::<Vec<_>>();
        let projection = table_scan
            .projection
            .clone()
            .unwrap_or_else(|| (0..fields.len()).collect());
        let Some(name) = DistExtensionPlanner::extract_full_table_name(&scan)? else {
            return Ok(None);
        };
        for region_id in planner.get_regions(&name, &scan).await? {
            let target = planner
                .region_query_handler
                .select_target(query_ctx.read_preference(), region_id)
                .await
                .map_err(|error| datafusion_common::DataFusionError::External(Box::new(error)))?;
            let request = SeriesSourceRequest {
                header: Some(header.clone()),
                region_id,
                schema: schema.clone(),
                projection: projection.clone(),
                filters: table_scan.filters.clone(),
                ordering: ordering.clone(),
                selector: None,
            };
            let Some(sources) = planner
                .region_query_handler
                .prepare_series_sources(&target, vec![request])
                .await
                .map_err(|error| datafusion_common::DataFusionError::External(Box::new(error)))?
            else {
                common_telemetry::debug!(%region_id, filters = ?table_scan.filters, "Research long-grid count: source declined");
                return Ok(None);
            };
            total = sources.iter().fold(total, |sum, source| {
                sum.saturating_add(source.source.num_series())
            });
        }
    }
    Ok(Some(total))
}

fn table_scan(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
    let mut found = None;
    plan.apply(|node| {
        if matches!(node, LogicalPlan::TableScan(_)) {
            found = Some(node.clone());
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn long_grids_need_an_admission() {
        assert!(admits(DENSE_GRID_POINTS));
        assert!(!admits(DENSE_GRID_POINTS + 1));
        assert!(scope(true, async { admits(2017) }).await);
        assert!(!scope(false, async { admits(2017) }).await);
    }

    #[test]
    fn only_shapes_with_dense_grids_pay_for_a_count() {
        let needs = |query: &str| may_need_dense(&promql_parser::parser::parse(query).unwrap());
        assert!(needs("up{job=\"a\"}"));
        assert!(needs("sum without(instance) (topk(1, up))"));
        assert!(needs(
            "sum by (pod) (rate(a[5m]) * on(pod) group_left() max by (pod) (b))"
        ));
        assert!(!needs("sum by (ns) (max by (ns, pod) (up))"));
        assert!(!needs("sum(rate(a[5m])) by (ns) / sum(b) by (ns)"));
        assert!(!needs("histogram_quantile(0.99, sum(rate(a[5m])) by (le))"));
    }

    #[test]
    fn budget_counts_series_times_points() {
        // 7-day grid: 416 no-churn series fit, a churned million do not.
        assert!(fits(416, 2017));
        assert!(!fits(1_000_000, 2017));
    }
}
