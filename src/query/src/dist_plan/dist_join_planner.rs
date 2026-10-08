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

//! Nested broadcast join rewrite for `INNER` joins of two distributed scans.
//!
//! The cost heuristic is enabled per query when the session sets `experimental_dist_join`:
//! [`DatafusionQueryEngine::create_physical_plan`](crate::datafusion::DatafusionQueryEngine)
//! then fetches the approximate disk sizes of the candidate tables into [`DistJoinStats`], a
//! query-local config extension, and the heuristic rewrites the join only when those statistics
//! favor the right build side. Runs after `DistPlannerAnalyzer`, which wraps the remote scans in
//! `MergeScan` and assigns the remote dynamic filter producer ids preserved here.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use common_meta::datanode::RegionStat;
use common_meta::rpc::router::RegionRoute;
use datafusion::config::{ConfigExtension, ExtensionOptions};
use datafusion::datasource::DefaultTableSource;
use datafusion::error::Result as DfResult;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_expr::utils::{can_hash, find_valid_equijoin_key_pair, split_conjunction};
use datafusion_expr::{
    Expr, ExprSchemable, Join, JoinType, LogicalPlan, Operator, TableScan,
    UserDefinedLogicalNodeCore,
};
use datafusion_optimizer::analyzer::AnalyzerRule;
use datatypes::extension::json::is_json2_extension_type;
use store_api::region_engine::RegionRole;
use store_api::storage::RegionId;
use table::metadata::{TableId, TableType};
use table::table::adapter::DfTableProviderAdapter;

use crate::dist_plan::merge_scan::MergeScanLogicalPlan;

/// Name of [`DistJoinPlanner`]. The query engine looks it up to check that this rewrite, the
/// only consumer of [`DistJoinStats`], runs for the query at all.
pub(crate) const DIST_JOIN_PLANNER_RULE_NAME: &str = "DistJoinPlanner";

/// Region count and total disk size of a table, as the cost heuristic compares them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DistJoinTableStats {
    /// Total approximate disk bytes of all regions of the table.
    pub total_bytes: u64,
    /// Number of regions of the table.
    pub region_count: usize,
}

/// Per-table statistics fetched for the cost heuristic of one query.
///
/// A query-local `ConfigOptions` extension, built when
/// [`DatafusionQueryEngine::create_physical_plan`](crate::datafusion::DatafusionQueryEngine)
/// sees the session opt-in (`SET experimental_dist_join = true`) on a plausible candidate join.
#[derive(Debug, Clone, Default)]
pub(crate) struct DistJoinStats {
    /// Statistics per table id of the candidate tables of the query. A table is only usable as
    /// a probe or build side when it is present here with a known, non-zero size.
    pub tables: BTreeMap<TableId, DistJoinTableStats>,
}

impl ConfigExtension for DistJoinStats {
    const PREFIX: &'static str = "dist_join";
}

/// Only satisfies the [`ConfigExtension`] bound: the statistics are built by the query engine
/// and have no configurable entry.
impl ExtensionOptions for DistJoinStats {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, _key: &str, value: &str) -> DfResult<()> {
        Err(datafusion_common::DataFusionError::NotImplemented(format!(
            "DistJoinStats cannot be set (key: {value})"
        )))
    }

    fn entries(&self) -> Vec<datafusion::config::ConfigEntry> {
        vec![]
    }
}

impl DistJoinStats {
    /// Whether `build` should be the nested build side of a join whose probe side is `probe`.
    ///
    /// The probe side selects the regions the join runs on, and the build side is read from
    /// every one of them. The heuristic compares the disk bytes of the build table per probe
    /// region, `N_probe * B_build`, with the disk bytes of the probe table itself, `P_probe`, and
    /// only takes the rewrite when the product is strictly smaller. Those sizes are the
    /// approximate disk sizes the regions report, not a measured or predicted amount of network
    /// traffic.
    ///
    /// The sides are never swapped, and a missing table, a table without regions or bytes, or an
    /// overflowing product means "no".
    pub fn favors_right_build(&self, probe: TableId, build: TableId) -> bool {
        let (Some(probe), Some(build)) = (self.tables.get(&probe), self.tables.get(&build)) else {
            return false;
        };
        if probe.region_count == 0 || probe.total_bytes == 0 || build.total_bytes == 0 {
            return false;
        }
        let Some(per_probe_region_bytes) =
            (probe.region_count as u64).checked_mul(build.total_bytes)
        else {
            return false;
        };

        per_probe_region_bytes < probe.total_bytes
    }
}

/// The base tables of one plausible candidate join of the cost heuristic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CandidateJoin {
    /// The base table of the left (probe) side.
    pub probe: TableId,
    /// The base table of the right (build) side.
    pub build: TableId,
}

/// Returns the candidate joins of `plan`, i.e. of its `EXPLAIN`/`EXPLAIN ANALYZE` input: the
/// `INNER` joins whose condition compares two sides that each resolve to a single physical base
/// table, with two different tables on the two sides.
///
/// A join of one table with itself, e.g. under two aliases, can never satisfy `N * B < B`, so it
/// is left out and its statistics are never fetched.
///
/// The plan is the one before the analyzers run: DataFusion's SQL planner leaves the whole `ON`
/// clause of `LogicalPlanBuilder::join_on` in `filter`, and `ExtractEquijoinPredicate` only
/// moves the equijoin into `on` later, so both shapes count. This only decides whose statistics
/// are worth fetching; the rewrite still requires the distributed `MergeScan` shape and usable
/// statistics for the two tables.
pub(crate) fn candidate_joins(plan: &LogicalPlan) -> Vec<CandidateJoin> {
    fn collect(plan: &LogicalPlan, candidates: &mut Vec<CandidateJoin>) {
        if let LogicalPlan::Join(join) = plan
            && join.join_type == JoinType::Inner
            && has_join_equality(join)
            && let (Some(probe), Some(build)) = (
                side_base_table_id(&join.left),
                side_base_table_id(&join.right),
            )
            && probe != build
        {
            candidates.push(CandidateJoin { probe, build });
        }

        for input in plan.inputs() {
            collect(input, candidates);
        }
    }

    let plan = match plan {
        LogicalPlan::Explain(explain) => explain.plan.as_ref(),
        LogicalPlan::Analyze(analyze) => analyze.input.as_ref(),
        plan => plan,
    };
    let mut candidates = Vec::new();
    collect(plan, &mut candidates);
    candidates
}

/// Whether the join compares its two inputs with a hashable equality, in `on` or still in
/// `filter`.
///
/// Mirrors `ExtractEquijoinPredicate`, the rule that fills `on`:
/// [`find_valid_equijoin_key_pair`] rejects top-level equalities that do not compare the two
/// inputs (constants, same-side columns), and `can_hash` rejects operand types a hash join
/// cannot use.
fn has_join_equality(join: &Join) -> bool {
    let is_equijoin_key = |left: &Expr, right: &Expr| {
        let Ok(Some((left, right))) =
            find_valid_equijoin_key_pair(left, right, join.left.schema(), join.right.schema())
        else {
            return false;
        };
        let (Ok(left_type), Ok(right_type)) = (
            left.get_type(join.left.schema()),
            right.get_type(join.right.schema()),
        ) else {
            return false;
        };

        can_hash(&left_type) && can_hash(&right_type)
    };

    if join
        .on
        .iter()
        .any(|(left, right)| is_equijoin_key(left, right))
    {
        return true;
    }

    join.filter.as_ref().is_some_and(|filter| {
        split_conjunction(filter).into_iter().any(|expr| {
            matches!(
                expr,
                Expr::BinaryExpr(binary)
                    if binary.op == Operator::Eq && is_equijoin_key(&binary.left, &binary.right)
            )
        })
    })
}

/// The physical base table of a join side, which may be aliased, filtered or projected.
fn side_base_table_id(plan: &LogicalPlan) -> Option<TableId> {
    match plan {
        LogicalPlan::TableScan(scan) => physical_base_table_id(scan),
        LogicalPlan::Projection(projection) => side_base_table_id(&projection.input),
        LogicalPlan::Filter(filter) => side_base_table_id(&filter.input),
        LogicalPlan::SubqueryAlias(alias) => side_base_table_id(&alias.input),
        _ => None,
    }
}

/// The table id of a scan of a physical base table, the only tables [`DistJoinPlanner`]
/// captures.
fn physical_base_table_id(scan: &TableScan) -> Option<TableId> {
    let source = scan.source.downcast_ref::<DefaultTableSource>()?;
    let provider = source
        .table_provider
        .downcast_ref::<DfTableProviderAdapter>()?;
    let table = provider.table();

    (table.table_type() == TableType::Base).then(|| table.table_info().table_id())
}

/// Totals the `approximate_bytes` of the `expected_regions` from `reports`.
///
/// Returns `None`, i.e. no rewrite, unless every expected region has exactly one ordinary
/// leader report with a non-zero size: a missing or duplicate leader report, a zero size, or
/// an overflowing sum does not describe the table well enough to compare it with another one.
pub(crate) fn aggregate_region_stats(
    expected_regions: &[RegionId],
    reports: &[RegionStat],
) -> Option<DistJoinTableStats> {
    let expected = expected_regions.iter().copied().collect::<BTreeSet<_>>();
    if expected.is_empty() || expected.len() != expected_regions.len() {
        return None;
    }

    let mut sizes = BTreeMap::new();
    for report in reports {
        if !expected.contains(&report.id) || report.role != RegionRole::Leader {
            continue;
        }
        // A second leader report leaves the size of the region unknown.
        if sizes.insert(report.id, report.approximate_bytes).is_some() {
            return None;
        }
    }
    if sizes.len() != expected.len() {
        return None;
    }

    let mut total_bytes = 0u64;
    for bytes in sizes.values() {
        if *bytes == 0 {
            return None;
        }
        total_bytes = total_bytes.checked_add(*bytes)?;
    }

    Some(DistJoinTableStats {
        total_bytes,
        region_count: expected.len(),
    })
}

/// The expected region ids of the table with the physical table id `physical_table_id`, taken
/// from its physical table route.
///
/// Returns `None` when the route lists no region or any region belongs to another table: the
/// heuristic compares the complete region list of one table with another table, so a route that
/// does not describe `physical_table_id` (e.g. the id a logical table was resolved to) is not
/// usable.
pub(crate) fn expected_region_ids(
    physical_table_id: TableId,
    routes: &[RegionRoute],
) -> Option<Vec<RegionId>> {
    let mut regions = Vec::with_capacity(routes.len());
    for route in routes {
        if route.region.id.table_id() != physical_table_id {
            return None;
        }
        regions.push(route.region.id);
    }

    (!regions.is_empty()).then_some(regions)
}

/// Nests the build side's `MergeScan` inside the probe side's one, so the join runs on
/// the datanodes holding the probe regions.
///
/// Keeps the join node as is, so its schema, join conditions, residual filter and NULL
/// semantics are preserved; unmatched shapes are returned unchanged.
#[derive(Debug)]
pub struct DistJoinPlanner;

impl AnalyzerRule for DistJoinPlanner {
    fn name(&self) -> &str {
        DIST_JOIN_PLANNER_RULE_NAME
    }

    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> DfResult<LogicalPlan> {
        let Some(stats) = config.extensions.get::<DistJoinStats>() else {
            return Ok(plan);
        };

        let mut rewriter = NestedBroadcastJoinRewriter { stats };
        Ok(plan.rewrite(&mut rewriter)?.data)
    }
}

/// Rewriter nesting the build side `MergeScan` of a supported join inside the probe side's
/// `MergeScan`.
struct NestedBroadcastJoinRewriter<'a> {
    /// Per-table statistics fetched for this query, see [`DistJoinStats`].
    stats: &'a DistJoinStats,
}

impl NestedBroadcastJoinRewriter<'_> {
    /// Returns the rewritten plan when `node` matches the supported shape, otherwise
    /// `None` to keep the plan unchanged.
    ///
    /// A rewrite changes where the join runs, so it must preserve the join output
    /// schema, the row multiplicity, and the remote boundary identities; the guards
    /// below only admit the shape where that holds.
    fn try_rewrite_join(&self, node: &LogicalPlan) -> Option<LogicalPlan> {
        let LogicalPlan::Join(join) = node else {
            return None;
        };

        // Only `INNER` equi-joins of two distributed scans are handled, everything else
        // keeps the existing distributed plan.
        if join.join_type != JoinType::Inner || join.on.is_empty() {
            return None;
        }

        let (probe_plan, probe_merge_scan) = strip_merge_scan(&join.left)?;
        let (_, build_merge_scan) = strip_merge_scan(&join.right)?;
        if probe_merge_scan.is_placeholder() || build_merge_scan.is_placeholder() {
            return None;
        }

        // Replacing the probe input must not change the join input schema.
        if probe_plan.schema() != join.left.schema() {
            return None;
        }

        // A nested boundary is hidden from the remaining analyzer passes, so a boundary
        // schema `JsonSchemaConcretizeRule` still has to fix up cannot be repaired
        // anymore.
        if has_json2_boundary(&probe_merge_scan) || has_json2_boundary(&build_merge_scan) {
            return None;
        }

        // The right input is the only candidate build side, because moving a left build side
        // would change the join schema.
        let (probe_table, build_table) = (
            side_base_table_id(probe_merge_scan.input())?,
            side_base_table_id(build_merge_scan.input())?,
        );
        if !self.stats.favors_right_build(probe_table, build_table) {
            return None;
        }

        // Keep the rewrite at two levels: the probe side must be local and the build side
        // must not contain another distributed boundary.
        if contains_merge_scan(&probe_plan) || contains_merge_scan(build_merge_scan.input()) {
            return None;
        }

        // Join the probe regions against the build plan kept above; the probe table's
        // partition columns route the outer scan.
        let new_join = Join {
            left: Arc::new(probe_plan),
            right: join.right.clone(),
            ..join.clone()
        };
        let mut merge_scan = MergeScanLogicalPlan::new(
            LogicalPlan::Join(new_join),
            false,
            probe_merge_scan.partition_cols().clone(),
        );
        // The outer boundary keeps the probe boundary's remote dynamic filter identity
        // when it has one. A missing id only disables remote dynamic filter pushdown for
        // that boundary (`MergeScanExec` fails open), and serialization drops the ids of
        // the nested payload, so an absent id is not a rewrite blocker.
        if let Some(producer_id) = probe_merge_scan.remote_dyn_filter_producer_id() {
            merge_scan = merge_scan.with_remote_dyn_filter_producer_id(producer_id);
        }

        Some(merge_scan.into_logical_plan())
    }
}

impl TreeNodeRewriter for NestedBroadcastJoinRewriter<'_> {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> DfResult<Transformed<Self::Node>> {
        match self.try_rewrite_join(&node) {
            Some(rewritten) => Ok(Transformed::yes(rewritten)),
            None => Ok(Transformed::no(node)),
        }
    }
}

/// Strips the outermost `MergeScan` of a join input, which may be hidden below
/// pass-through projections, and returns the input plan without the wrapper together
/// with the removed `MergeScan`.
///
/// Returns `None` if the input is not wrapped in a `MergeScan`.
fn strip_merge_scan(plan: &LogicalPlan) -> Option<(LogicalPlan, MergeScanLogicalPlan)> {
    match plan {
        LogicalPlan::Extension(extension) => {
            let merge_scan = extension
                .node
                .as_any()
                .downcast_ref::<MergeScanLogicalPlan>()?;
            Some((merge_scan.input().clone(), merge_scan.clone()))
        }
        LogicalPlan::Projection(projection) => {
            let (input, merge_scan) = strip_merge_scan(&projection.input)?;
            let plan = plan
                .with_new_exprs(projection.expr.clone(), vec![input])
                .ok()?;
            Some((plan, merge_scan))
        }
        _ => None,
    }
}

/// Whether `plan` contains a visible `MergeScan` node.
fn contains_merge_scan(plan: &LogicalPlan) -> bool {
    if let LogicalPlan::Extension(extension) = plan
        && extension.node.as_any().is::<MergeScanLogicalPlan>()
    {
        return true;
    }

    plan.inputs().into_iter().any(contains_merge_scan)
}

/// Returns the local probe input that decides the regions of a supported nested broadcast
/// join, or `None` for any other plan.
///
/// Region pruning clears the collected predicates at a node with several inputs (the
/// join), so the outer `MergeScan` of the rewritten plan would be routed by the join
/// itself and scan every region of the probe table. Routing by the returned side keeps
/// the probe predicate pruning, while the full payload is still dispatched to the
/// selected regions.
///
/// Only the supported shape of [`DistJoinPlanner`] returns a side: an `INNER` equi-join
/// whose left input is a local plan and whose right input reaches its `MergeScan`
/// through projections.
pub(crate) fn nested_broadcast_join_probe_input(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    let LogicalPlan::Join(join) = plan else {
        return None;
    };
    if join.join_type != JoinType::Inner || join.on.is_empty() {
        return None;
    }
    if contains_merge_scan(&join.left) {
        return None;
    }
    let (_, build_merge_scan) = strip_merge_scan(&join.right)?;
    if build_merge_scan.is_placeholder() {
        return None;
    }

    Some(&join.left)
}

/// Whether the `MergeScan` boundary exposes a JSON2 column.
fn has_json2_boundary(merge_scan: &MergeScanLogicalPlan) -> bool {
    merge_scan
        .schema()
        .fields()
        .iter()
        .any(is_json2_extension_type)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_schema::extension::{
        EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY, ExtensionType as _,
    };
    use arrow_schema::{DataType, Field, Fields, Schema};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_meta::datanode::RegionManifestInfo;
    use common_meta::rpc::router::Region;
    use datafusion::datasource::DefaultTableSource;
    use datafusion_common::{DFSchema, DFSchemaRef, JoinConstraint, NullEquality};
    use datafusion_expr::{Expr, LogicalPlanBuilder, build_join_schema, col, lit};
    use datatypes::data_type::ConcreteDataType;
    use datatypes::extension::json::JsonExtensionType;
    use datatypes::schema::{ColumnSchema, SchemaBuilder, SchemaRef};
    use pretty_assertions::assert_eq;
    use table::TableRef;
    use table::metadata::TableId;
    use table::table::adapter::DfTableProviderAdapter;
    use table::test_util::EmptyTable;
    use table::test_util::table_info::test_table_info;

    use super::*;
    use crate::dist_plan::DistPlannerAnalyzer;
    use crate::dist_plan::planner::table_name_of;

    /// Two-column schema (`number`, `host`) shared by the test tables.
    fn test_schema() -> SchemaRef {
        let schema = SchemaBuilder::try_from_columns(vec![
            ColumnSchema::new("number", ConcreteDataType::uint32_datatype(), true),
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
        ])
        .unwrap()
        .build()
        .unwrap();
        Arc::new(schema)
    }

    /// Base table named `name`.
    fn test_table(table_id: TableId, name: &str) -> TableRef {
        let info = test_table_info(
            table_id,
            name,
            DEFAULT_SCHEMA_NAME,
            DEFAULT_CATALOG_NAME,
            test_schema(),
        );
        EmptyTable::from_table_info(&info)
    }

    /// Scan plan whose alias and base table name may differ.
    fn table_scan(alias: &str, table_id: TableId, table_name: &str) -> LogicalPlan {
        let source = Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(test_table(table_id, table_name)),
        )));
        LogicalPlanBuilder::scan_with_filters(alias, source, None, vec![])
            .unwrap()
            .build()
            .unwrap()
    }

    /// `t1 <join_type> JOIN t2` on `number`, i.e. the plan the distributed planner sees.
    fn join_plan(join_type: JoinType) -> LogicalPlan {
        LogicalPlanBuilder::from(table_scan("t1", 1, "t1"))
            .join_on(
                table_scan("t2", 2, "t2"),
                join_type,
                vec![col("t1.number").eq(col("t2.number"))],
            )
            .unwrap()
            .build()
            .unwrap()
    }

    /// Builds an inner equi-join with the join keys in `on`, as the analyzer leaves them.
    fn inner_join(
        left: LogicalPlan,
        right: LogicalPlan,
        left_key: Expr,
        right_key: Expr,
    ) -> LogicalPlan {
        let schema = build_join_schema(left.schema(), right.schema(), &JoinType::Inner).unwrap();
        LogicalPlan::Join(Join {
            left: Arc::new(left),
            right: Arc::new(right),
            on: vec![(left_key, right_key)],
            filter: None,
            join_type: JoinType::Inner,
            join_constraint: JoinConstraint::On,
            schema: DFSchemaRef::new(schema),
            null_equality: NullEquality::NullEqualsNothing,
            null_aware: false,
        })
    }

    /// Distributed plan of `t1 INNER JOIN t2`, as produced by `DistPlannerAnalyzer`.
    fn distributed_join_plan() -> LogicalPlan {
        DistPlannerAnalyzer {}
            .analyze(join_plan(JoinType::Inner), &ConfigOptions::default())
            .unwrap()
    }

    fn rewrite_config(tables: &[(TableId, DistJoinTableStats)]) -> ConfigOptions {
        stats_config(tables)
    }

    fn rewrite(plan: LogicalPlan) -> LogicalPlan {
        DistJoinPlanner {}
            .analyze(
                plan,
                &rewrite_config(&[
                    (1, table_stats(10_000, 2)),
                    (2, table_stats(1_000, 1)),
                    (3, table_stats(500, 1)),
                ]),
            )
            .unwrap()
    }

    /// All `MergeScan`s of `plan`, including the ones nested inside another `MergeScan`.
    fn merge_scans(plan: &LogicalPlan) -> Vec<MergeScanLogicalPlan> {
        let mut scans = Vec::new();
        collect_merge_scans(plan, &mut scans);
        scans
    }

    fn collect_merge_scans(plan: &LogicalPlan, scans: &mut Vec<MergeScanLogicalPlan>) {
        if let LogicalPlan::Extension(extension) = plan
            && let Some(merge_scan) = extension
                .node
                .as_any()
                .downcast_ref::<MergeScanLogicalPlan>()
        {
            scans.push(merge_scan.clone());
            collect_merge_scans(merge_scan.input(), scans);
            return;
        }

        for input in plan.inputs() {
            collect_merge_scans(input, scans);
        }
    }

    /// The join wrapped by the outer `MergeScan` of a rewritten plan.
    fn outer_join(plan: &LogicalPlan) -> &Join {
        let LogicalPlan::Extension(extension) = plan else {
            panic!("expected the outer node to be a MergeScan, got: {plan}");
        };
        let merge_scan = extension
            .node
            .as_any()
            .downcast_ref::<MergeScanLogicalPlan>()
            .unwrap_or_else(|| panic!("expected the outer node to be a MergeScan, got: {plan}"));
        let LogicalPlan::Join(join) = merge_scan.input() else {
            panic!(
                "expected the outer MergeScan to wrap a join, got: {}",
                merge_scan.input()
            );
        };

        join
    }

    /// Without the build table option the join stays above the two `MergeScan`s.
    #[test]
    fn nested_broadcast_join_rewrite_disabled_by_default() {
        let plan = distributed_join_plan();
        let result = DistJoinPlanner {}
            .analyze(plan.clone(), &ConfigOptions::default())
            .unwrap();

        assert_eq!(plan.to_string(), result.to_string());
        assert!(matches!(result, LogicalPlan::Join(_)));
        assert_eq!(2, merge_scans(&result).len());
    }

    /// When statistics favor the right build side, its `MergeScan` becomes the inner scan
    /// of the outer `MergeScan`, which is routed by the probe table's regions.
    ///
    /// The rewrite must only move where the join runs: the join fields, the residual
    /// filter, the NULL semantics and the output schema stay as the join had them.
    #[test]
    fn nested_broadcast_join_rewrite_nests_build_side_merge_scan() {
        let plan = distributed_join_plan();
        let LogicalPlan::Join(original_join) = &plan else {
            panic!("expected a join on top, got: {plan}");
        };
        let plan = LogicalPlan::Join(Join {
            filter: Some(col("t1.number").gt(lit(1u32))),
            null_equality: NullEquality::NullEqualsNull,
            ..original_join.clone()
        });
        let probe_scan = merge_scans(&plan).first().cloned().unwrap();

        let result = rewrite(plan.clone());

        let join = outer_join(&result);
        assert_eq!(JoinType::Inner, join.join_type);
        assert_eq!(original_join.on, join.on);
        assert_eq!(
            Some(col("t1.number").gt(lit(1u32))),
            join.filter,
            "the residual filter must be preserved"
        );
        assert_eq!(original_join.join_constraint, join.join_constraint);
        assert_eq!(NullEquality::NullEqualsNull, join.null_equality);
        assert_eq!(original_join.null_aware, join.null_aware);
        assert!(
            !contains_merge_scan(&join.left),
            "probe side must be local, got: {}",
            join.left
        );
        assert!(
            contains_merge_scan(&join.right),
            "build side must keep its MergeScan, got: {}",
            join.right
        );

        // The outer scan must be routed by the probe table's regions and keep the
        // probe boundary's partition columns.
        assert_eq!(
            "t1",
            table_name_of(&LogicalPlan::Join(join.clone()))
                .unwrap()
                .table_name
        );
        let outer_scan = merge_scans(&result).first().cloned().unwrap();
        assert_eq!(probe_scan.partition_cols(), outer_scan.partition_cols());
        assert_eq!(plan.schema(), result.schema());
    }

    /// Both boundaries keep their remote dynamic filter producer ids.
    #[test]
    fn nested_broadcast_join_rewrite_preserves_remote_dyn_filter_producer_ids() {
        let plan = distributed_join_plan();
        let scans = merge_scans(&plan);
        let probe_id = scans[0].remote_dyn_filter_producer_id().unwrap();
        let build_id = scans[1].remote_dyn_filter_producer_id().unwrap();
        assert_ne!(probe_id, build_id);

        let result = rewrite(plan);

        let scans = merge_scans(&result);
        assert_eq!(2, scans.len());
        assert_eq!(Some(probe_id), scans[0].remote_dyn_filter_producer_id());
        assert_eq!(Some(build_id), scans[1].remote_dyn_filter_producer_id());
    }

    /// Boundaries without a remote dynamic filter producer id are still rewritten: a
    /// missing id only disables remote dynamic filter pushdown for that boundary, and the
    /// outer boundary then simply has none either.
    #[test]
    fn nested_broadcast_join_rewrite_without_producer_ids() {
        let plan = distributed_join_plan_without_producer_ids();
        assert_eq!(
            vec![None, None],
            merge_scans(&plan)
                .iter()
                .map(|scan| scan.remote_dyn_filter_producer_id())
                .collect::<Vec<_>>()
        );

        let result = rewrite(plan.clone());

        let scans = merge_scans(&result);
        assert_eq!(2, scans.len());
        assert_eq!(None, scans[0].remote_dyn_filter_producer_id());
        assert_eq!(None, scans[1].remote_dyn_filter_producer_id());
        assert_eq!(plan.schema(), result.schema());
    }

    /// Distributed plan of `t1 INNER JOIN t2` whose boundaries carry no producer ids.
    fn distributed_join_plan_without_producer_ids() -> LogicalPlan {
        let plan = distributed_join_plan();
        let LogicalPlan::Join(join) = &plan else {
            panic!("expected a join on top, got: {plan}");
        };
        let (probe_plan, probe_scan) = strip_merge_scan(&join.left).unwrap();
        let (build_plan, build_scan) = strip_merge_scan(&join.right).unwrap();
        let probe =
            MergeScanLogicalPlan::new(probe_plan, false, probe_scan.partition_cols().clone());
        let build =
            MergeScanLogicalPlan::new(build_plan, false, build_scan.partition_cols().clone());

        inner_join(
            probe.into_logical_plan(),
            build.into_logical_plan(),
            col("t1.number"),
            col("t2.number"),
        )
    }

    /// A `MergeScan` hidden below a pass-through projection is recognized on both sides.
    #[test]
    fn nested_broadcast_join_rewrite_handles_projection_wrappers() {
        let projected = projected_join_plan();

        let result = rewrite(projected.clone());

        let join = outer_join(&result);
        assert!(matches!(*join.left, LogicalPlan::Projection(_)));
        assert!(!contains_merge_scan(&join.left));
        assert!(matches!(join.right.as_ref(), LogicalPlan::Projection(_)));
        assert!(contains_merge_scan(&join.right));
        assert_eq!(projected.schema(), result.schema());
    }

    /// Re-projects both join inputs of the distributed plan, so each side is a
    /// pass-through projection above its `MergeScan`.
    fn projected_join_plan() -> LogicalPlan {
        let plan = distributed_join_plan();
        let LogicalPlan::Join(join) = &plan else {
            panic!("expected a join on top, got: {plan}");
        };
        let left = LogicalPlanBuilder::from(join.left.as_ref().clone())
            .project(vec![col("t1.number"), col("t1.host")])
            .unwrap()
            .build()
            .unwrap();
        let right = LogicalPlanBuilder::from(join.right.as_ref().clone())
            .project(vec![col("t2.number"), col("t2.host")])
            .unwrap()
            .build()
            .unwrap();
        let schema = build_join_schema(left.schema(), right.schema(), &join.join_type).unwrap();

        LogicalPlan::Join(Join {
            left: Arc::new(left),
            right: Arc::new(right),
            schema: DFSchemaRef::new(schema),
            ..join.clone()
        })
    }

    /// Only `INNER` joins are rewritten.
    #[test]
    fn nested_broadcast_join_rewrite_ignores_non_inner_join() {
        let plan = DistPlannerAnalyzer {}
            .analyze(join_plan(JoinType::Left), &ConfigOptions::default())
            .unwrap();
        let result = rewrite(plan.clone());

        assert_eq!(plan.to_string(), result.to_string());
    }

    /// A build table on the left input is not rewritten: swapping the inputs would change
    /// the join output schema, which the rewrite must preserve.
    #[test]
    fn nested_broadcast_join_rewrite_ignores_left_side_build() {
        let plan = distributed_join_plan();
        let LogicalPlan::Join(join) = &plan else {
            panic!("expected a join on top, got: {plan}");
        };
        let plan = LogicalPlan::Join(Join {
            left: join.right.clone(),
            right: join.left.clone(),
            on: join
                .on
                .iter()
                .map(|(left, right)| (right.clone(), left.clone()))
                .collect(),
            schema: DFSchemaRef::new(
                build_join_schema(join.right.schema(), join.left.schema(), &join.join_type)
                    .unwrap(),
            ),
            ..join.clone()
        });

        let result = rewrite(plan.clone());

        assert_eq!(plan.to_string(), result.to_string());
    }

    /// The rewrite stays at two levels: an already nested probe side is left alone.
    #[test]
    fn nested_broadcast_join_rewrite_ignores_more_than_two_levels() {
        let nested = rewrite(distributed_join_plan());
        let build = DistPlannerAnalyzer {}
            .analyze(table_scan("t3", 3, "t3"), &ConfigOptions::default())
            .unwrap();
        let plan = inner_join(nested, build, col("t1.number"), col("t3.number"));

        let result = rewrite(plan.clone());

        assert_eq!(plan.to_string(), result.to_string());
    }

    /// The routing helper returns the probe side of the supported nested shape and `None`
    /// for the join of two boundaries (the shape before the rewrite).
    #[test]
    fn nested_broadcast_join_probe_input_only_for_supported_shape() {
        let plan = distributed_join_plan();
        let LogicalPlan::Join(join) = &plan else {
            panic!("expected a join on top, got: {plan}");
        };

        assert_eq!(
            None,
            nested_broadcast_join_probe_input(&plan).map(|plan| plan.to_string())
        );

        let result = rewrite(plan.clone());
        let LogicalPlan::Extension(extension) = &result else {
            panic!("expected the rewritten plan to be a MergeScan, got: {result}");
        };
        let merge_scan = extension
            .node
            .as_any()
            .downcast_ref::<MergeScanLogicalPlan>()
            .unwrap();
        let (probe_plan, _) = strip_merge_scan(&join.left).unwrap();
        assert_eq!(
            Some(probe_plan.to_string()),
            nested_broadcast_join_probe_input(merge_scan.input()).map(|plan| plan.to_string())
        );
    }

    /// JSON2 boundaries are not rewritten: the nested build boundary is hidden from
    /// `JsonSchemaConcretizeRule`.
    #[test]
    fn nested_broadcast_join_rewrite_ignores_json2_boundary() {
        let plan = distributed_join_plan();
        let LogicalPlan::Join(join) = &plan else {
            panic!("expected a join on top, got: {plan}");
        };
        let (probe_plan, probe_scan) = strip_merge_scan(&join.left).unwrap();
        let (build_plan, build_scan) = strip_merge_scan(&join.right).unwrap();
        let plan = inner_join(
            MergeScanLogicalPlan::new(probe_plan, false, probe_scan.partition_cols().clone())
                .with_remote_dyn_filter_producer_id(
                    probe_scan.remote_dyn_filter_producer_id().unwrap(),
                )
                .into_logical_plan(),
            MergeScanLogicalPlan::new(build_plan, false, build_scan.partition_cols().clone())
                .with_output_schema(json2_boundary_schema())
                .with_remote_dyn_filter_producer_id(
                    build_scan.remote_dyn_filter_producer_id().unwrap(),
                )
                .into_logical_plan(),
            col("t1.number"),
            col("t2.number"),
        );

        let result = rewrite(plan.clone());

        assert_eq!(plan.to_string(), result.to_string());
    }

    /// A region report of the tests: one ordinary leader with `bytes` approximate disk bytes.
    fn region_stat(region_id: RegionId, bytes: u64, role: RegionRole) -> RegionStat {
        RegionStat {
            id: region_id,
            rcus: 0,
            wcus: 0,
            approximate_bytes: bytes,
            engine: "mito".to_string(),
            role,
            num_rows: 0,
            memtable_size: 0,
            manifest_size: 0,
            sst_size: 0,
            sst_num: 0,
            index_size: 0,
            region_manifest: RegionManifestInfo::Mito {
                manifest_version: 0,
                flushed_entry_id: 0,
                file_removed_cnt: 0,
            },
            written_bytes: 0,
            query_cpu_time: 0,
            query_scanned_bytes: 0,
            data_topic_latest_entry_id: 0,
            metadata_topic_latest_entry_id: 0,
            min_timestamp: None,
            max_timestamp: None,
        }
    }

    /// Statistics of one table, as [`DistJoinStats`] reads them.
    fn table_stats(total_bytes: u64, region_count: usize) -> DistJoinTableStats {
        DistJoinTableStats {
            total_bytes,
            region_count,
        }
    }

    /// Statistics of the given tables.
    fn stats(tables: &[(TableId, DistJoinTableStats)]) -> DistJoinStats {
        DistJoinStats {
            tables: tables.iter().copied().collect(),
        }
    }

    /// Config with the given statistics, as `create_physical_plan` inserts them.
    fn stats_config(tables: &[(TableId, DistJoinTableStats)]) -> ConfigOptions {
        let mut config = ConfigOptions::default();
        config.extensions.insert(stats(tables));
        config
    }

    /// A region route of the tests.
    fn region_route(region_id: RegionId) -> RegionRoute {
        RegionRoute {
            region: Region {
                id: region_id,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// The heuristic takes the rewrite only when the product of the build bytes per probe region
    /// is strictly smaller than the probe bytes, and never with a missing, empty or overflowing
    /// input.
    #[test]
    fn favors_right_build_compares_product_with_probe_bytes() {
        let cases = [
            // 2 probe regions * 1_000 bytes of build = 2_000 < 10_000.
            (
                Some(table_stats(10_000, 2)),
                Some(table_stats(1_000, 1)),
                true,
            ),
            // 10 * 1_000 == 10_000: not strictly smaller.
            (
                Some(table_stats(10_000, 10)),
                Some(table_stats(1_000, 1)),
                false,
            ),
            // 10 * 1_001 > 10_000.
            (
                Some(table_stats(10_000, 10)),
                Some(table_stats(1_001, 1)),
                false,
            ),
            // A table without bytes or without regions is not usable, even when the product
            // would be smaller.
            (Some(table_stats(10_000, 2)), Some(table_stats(0, 1)), false),
            (Some(table_stats(10_000, 0)), Some(table_stats(1, 1)), false),
            (Some(table_stats(0, 2)), Some(table_stats(1, 1)), false),
            // 2 * u64::MAX overflows.
            (
                Some(table_stats(u64::MAX, 2)),
                Some(table_stats(u64::MAX, 1)),
                false,
            ),
        ];

        for (probe, build, expected) in cases {
            let mut tables = BTreeMap::new();
            if let Some(probe) = probe {
                tables.insert(1, probe);
            }
            if let Some(build) = build {
                tables.insert(2, build);
            }

            assert_eq!(
                expected,
                DistJoinStats { tables }.favors_right_build(1, 2),
                "probe: {probe:?}, build: {build:?}"
            );
        }
    }

    /// A table without statistics selects neither side.
    #[test]
    fn favors_right_build_requires_both_tables() {
        let stats = stats(&[(1, table_stats(10_000, 2))]);

        assert!(!stats.favors_right_build(1, 2));
        assert!(!stats.favors_right_build(2, 1));
        assert!(!stats.favors_right_build(3, 4));
    }

    /// The aggregation needs exactly one ordinary leader report per expected region, and ignores
    /// the reports of other tables.
    #[test]
    fn aggregate_region_stats_requires_one_leader_per_region() {
        let regions = vec![RegionId::new(1, 1), RegionId::new(1, 2)];
        let reports = vec![
            region_stat(RegionId::new(1, 1), 100, RegionRole::Leader),
            region_stat(RegionId::new(1, 2), 200, RegionRole::Leader),
            // Reports of other tables and of non-leaders are ignored.
            region_stat(RegionId::new(2, 1), 1_000, RegionRole::Leader),
            region_stat(RegionId::new(1, 3), 7, RegionRole::Follower),
        ];

        assert_eq!(
            Some(table_stats(300, 2)),
            aggregate_region_stats(&regions, &reports)
        );
    }

    /// An incomplete or unusable description of the expected regions is not compared with another
    /// table.
    #[test]
    fn aggregate_region_stats_rejects_incomplete_or_unusable_reports() {
        let regions = vec![RegionId::new(1, 1), RegionId::new(1, 2)];
        let leader = |number: u32, bytes: u64| {
            region_stat(RegionId::new(1, number), bytes, RegionRole::Leader)
        };
        let cases = [
            ("missing report", regions.clone(), vec![leader(1, 100)]),
            (
                "duplicate report",
                regions.clone(),
                vec![leader(1, 100), leader(1, 100), leader(2, 200)],
            ),
            (
                "follower instead of leader",
                regions.clone(),
                vec![
                    region_stat(RegionId::new(1, 1), 100, RegionRole::Follower),
                    leader(2, 200),
                ],
            ),
            (
                "staging leader instead of leader",
                regions.clone(),
                vec![
                    region_stat(RegionId::new(1, 1), 100, RegionRole::StagingLeader),
                    leader(2, 200),
                ],
            ),
            (
                "zero bytes",
                regions.clone(),
                vec![leader(1, 0), leader(2, 200)],
            ),
            (
                "overflowing sum",
                regions.clone(),
                vec![leader(1, u64::MAX), leader(2, 1)],
            ),
            ("no expected region", vec![], vec![leader(1, 100)]),
            (
                "duplicate expected region",
                vec![RegionId::new(1, 1), RegionId::new(1, 1)],
                vec![leader(1, 100)],
            ),
        ];

        for (name, regions, reports) in cases {
            assert_eq!(
                None,
                aggregate_region_stats(&regions, &reports),
                "case: {name}"
            );
        }
    }

    /// A physical table route provides the expected regions, and only when all of them belong to
    /// the resolved physical table.
    #[test]
    fn expected_region_ids_of_physical_route() {
        let routes = vec![
            region_route(RegionId::new(7, 1)),
            region_route(RegionId::new(7, 2)),
        ];

        assert_eq!(
            Some(vec![RegionId::new(7, 1), RegionId::new(7, 2)]),
            expected_region_ids(7, &routes)
        );
        // The route of another physical table, e.g. after the captured logical table id was
        // resolved to a different physical table.
        assert_eq!(None, expected_region_ids(8, &routes));
        assert_eq!(None, expected_region_ids(7, &[]));
        assert_eq!(
            None,
            expected_region_ids(
                7,
                &[
                    region_route(RegionId::new(7, 1)),
                    region_route(RegionId::new(8, 1)),
                ]
            )
        );
    }

    /// The cost heuristic nests the build side only when the statistics favor it: the same
    /// statistics in the other order never swap the sides, and missing statistics keep the plan.
    #[test]
    fn nested_broadcast_join_rewrite_with_stats() {
        let plan = distributed_join_plan();

        // The probe table `t1` (id 1) has two regions and 10_000 bytes, the build table `t2`
        // (id 2) one region and 1_000 bytes: 2 * 1_000 < 10_000.
        let favoring = stats_config(&[(1, table_stats(10_000, 2)), (2, table_stats(1_000, 1))]);
        let result = DistJoinPlanner {}.analyze(plan.clone(), &favoring).unwrap();

        let join = outer_join(&result);
        assert_eq!(2, merge_scans(&result).len());
        assert!(
            !contains_merge_scan(&join.left),
            "probe side must become local, got: {}",
            join.left
        );
        assert!(
            contains_merge_scan(&join.right),
            "build side must keep its MergeScan, got: {}",
            join.right
        );

        // The sides are never swapped: the same statistics in the other order do not favor the
        // right build side.
        let swapped = stats_config(&[(1, table_stats(1_000, 1)), (2, table_stats(10_000, 2))]);
        let result = DistJoinPlanner {}.analyze(plan.clone(), &swapped).unwrap();
        assert_eq!(plan.to_string(), result.to_string());

        // Statistics of the probe table only leave the build side unknown.
        let partial = stats_config(&[(1, table_stats(10_000, 2))]);
        let result = DistJoinPlanner {}.analyze(plan.clone(), &partial).unwrap();
        assert_eq!(plan.to_string(), result.to_string());
    }

    /// Both plan shapes the engine sees are covered: the equality in `on`, and the unanalyzed
    /// `join_on` plan, whose whole `ON` clause is still in `filter`, including the input of an
    /// EXPLAIN.
    #[test]
    fn candidate_joins_of_both_plan_shapes() {
        // The equality in `on`.
        let on_shape = inner_join(
            table_scan("t1", 1, "t1"),
            table_scan("t2", 2, "t2"),
            col("t1.number"),
            col("t2.number"),
        );
        assert!(
            matches!(&on_shape, LogicalPlan::Join(join) if !join.on.is_empty()),
            "expected the equality in `on`, got: {on_shape}"
        );
        assert_eq!(
            vec![CandidateJoin { probe: 1, build: 2 }],
            candidate_joins(&on_shape)
        );

        // The plan of `LogicalPlanBuilder::join_on`, as the SQL planner leaves it: the whole `ON`
        // clause is the `filter`.
        let filter_shape = join_plan(JoinType::Inner);
        assert!(
            matches!(&filter_shape, LogicalPlan::Join(join) if join.on.is_empty() && join.filter.is_some()),
            "expected the unanalyzed join to carry its condition in `filter`, got: {filter_shape}"
        );
        assert_eq!(
            vec![CandidateJoin { probe: 1, build: 2 }],
            candidate_joins(&filter_shape)
        );

        // The input of an EXPLAIN.
        let explain = LogicalPlanBuilder::from(filter_shape.clone())
            .explain(false, false)
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(candidate_joins(&filter_shape), candidate_joins(&explain));

        // A join without an equality is not a candidate, in either shape.
        let cross = LogicalPlanBuilder::from(table_scan("t1", 1, "t1"))
            .join_on(
                table_scan("t2", 2, "t2"),
                JoinType::Inner,
                vec![col("t1.number").gt(col("t2.number"))],
            )
            .unwrap()
            .build()
            .unwrap();
        assert!(candidate_joins(&cross).is_empty());

        // Only `INNER` joins are candidates.
        let left = LogicalPlanBuilder::from(table_scan("t1", 1, "t1"))
            .join_on(
                table_scan("t2", 2, "t2"),
                JoinType::Left,
                vec![col("t1.number").eq(col("t2.number"))],
            )
            .unwrap()
            .build()
            .unwrap();
        assert!(candidate_joins(&left).is_empty());

        // The join of two joins is no candidate itself (its sides are not base tables), while the
        // candidates of its inputs are collected: the rewrite applies at every join node.
        let other = inner_join(
            table_scan("t3", 3, "t3"),
            table_scan("t4", 4, "t4"),
            col("t3.number"),
            col("t4.number"),
        );
        let nested = inner_join(
            join_plan(JoinType::Inner),
            other,
            col("t1.number"),
            col("t3.number"),
        );
        assert_eq!(
            vec![
                CandidateJoin { probe: 1, build: 2 },
                CandidateJoin { probe: 3, build: 4 }
            ],
            candidate_joins(&nested)
        );
    }

    /// Candidate joins need a real cross-input equijoin: constants, same-side equalities and
    /// non-equality conditions are no candidates, while a cross-input equality stays one next to
    /// residual predicates.
    #[test]
    fn candidate_joins_require_cross_input_equijoin() {
        let candidates_of = |on: Vec<Expr>| {
            let plan = LogicalPlanBuilder::from(table_scan("t1", 1, "t1"))
                .join_on(table_scan("t2", 2, "t2"), JoinType::Inner, on)
                .unwrap()
                .build()
                .unwrap();

            candidate_joins(&plan)
        };
        let cases = [
            (
                "cross-input equality",
                vec![col("t1.number").eq(col("t2.number"))],
                true,
            ),
            (
                "reversed cross-input equality",
                vec![col("t2.number").eq(col("t1.number"))],
                true,
            ),
            (
                "cross-input equality with residual predicate",
                vec![
                    col("t1.number").eq(col("t2.number")),
                    col("t1.number").gt(lit(1u32)),
                ],
                true,
            ),
            ("constants", vec![lit(1u32).eq(lit(1u32))], false),
            (
                "same-side equality",
                vec![col("t1.number").eq(col("t1.number"))],
                false,
            ),
            (
                "column against constant",
                vec![col("t1.number").eq(lit(1u32))],
                false,
            ),
            (
                "cross-input non-equality",
                vec![col("t1.number").gt(col("t2.number"))],
                false,
            ),
        ];

        for (name, on, expected) in cases {
            let expected = if expected {
                vec![CandidateJoin { probe: 1, build: 2 }]
            } else {
                Vec::new()
            };
            assert_eq!(expected, candidates_of(on), "case: {name}");
        }
    }

    /// The two sides of a candidate join must be two different physical tables: the aliased
    /// self-join `logs AS l JOIN logs AS r` can never satisfy `N * B < B`, so it is no candidate,
    /// while the same shape over two tables stays one. Rejecting a self-join keeps the candidates
    /// of its inputs.
    #[test]
    fn candidate_joins_reject_same_physical_table() {
        let self_join = inner_join(
            table_scan("l", 7, "logs"),
            table_scan("r", 7, "logs"),
            col("l.number"),
            col("r.number"),
        );
        let aliased_tables = inner_join(
            table_scan("l", 7, "logs"),
            table_scan("r", 8, "archive"),
            col("l.number"),
            col("r.number"),
        );
        let legitimate = inner_join(
            table_scan("t1", 1, "t1"),
            table_scan("t2", 2, "t2"),
            col("t1.number"),
            col("t2.number"),
        );
        let mixed = inner_join(
            self_join.clone(),
            legitimate,
            col("l.number"),
            col("t1.number"),
        );

        let cases = [
            ("aliased self-join", self_join, vec![]),
            (
                "aliased different tables",
                aliased_tables,
                vec![CandidateJoin { probe: 7, build: 8 }],
            ),
            (
                "self-join next to a legitimate join",
                mixed,
                vec![CandidateJoin { probe: 1, build: 2 }],
            ),
        ];

        for (name, plan, expected) in cases {
            assert_eq!(expected, candidate_joins(&plan), "case: {name}");
        }
    }

    /// A one-column JSON2 boundary schema, as `JsonSchemaConcretizeRule` expects to fix up.
    fn json2_boundary_schema() -> DFSchemaRef {
        let field = Field::new("j", DataType::Struct(Fields::empty()), true).with_metadata(
            HashMap::from([
                (
                    EXTENSION_TYPE_NAME_KEY.to_string(),
                    JsonExtensionType::NAME.to_string(),
                ),
                (
                    EXTENSION_TYPE_METADATA_KEY.to_string(),
                    serde_json::json!({ "json_structure_settings": { "Structured": null } })
                        .to_string(),
                ),
            ]),
        );
        let schema = DFSchema::try_from(Schema::new(vec![field])).unwrap();

        DFSchemaRef::new(schema)
    }
}
