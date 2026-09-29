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
//! Opt-in only: enabled when [`DistPlannerOptions::nested_broadcast_join_build_table`]
//! names the build side table. Runs after `DistPlannerAnalyzer`, which wraps the remote
//! scans in `MergeScan` and assigns the remote dynamic filter producer ids preserved
//! here. Not cost based: no statistics are consulted in this slice.

use std::sync::Arc;

use datafusion::error::Result as DfResult;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_expr::{Join, JoinType, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_optimizer::analyzer::AnalyzerRule;
use datatypes::extension::json::is_json2_extension_type;
use table::table_name::TableName;

use crate::dist_plan::analyzer::DistPlannerOptions;
use crate::dist_plan::merge_scan::MergeScanLogicalPlan;
use crate::dist_plan::planner::table_name_of;

/// Nests the build side's `MergeScan` inside the probe side's one, so the join runs on
/// the datanodes holding the probe regions.
///
/// Keeps the join node as is, so its schema, join conditions, residual filter and NULL
/// semantics are preserved; unmatched shapes are returned unchanged.
#[derive(Debug)]
pub struct DistJoinPlanner;

impl AnalyzerRule for DistJoinPlanner {
    fn name(&self) -> &str {
        "DistJoinPlanner"
    }

    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> DfResult<LogicalPlan> {
        let Some(build_table) = config
            .extensions
            .get::<DistPlannerOptions>()
            .and_then(|options| options.nested_broadcast_join_build_table.as_deref())
        else {
            return Ok(plan);
        };

        let mut rewriter = NestedBroadcastJoinRewriter { build_table };
        Ok(plan.rewrite(&mut rewriter)?.data)
    }
}

/// Rewriter nesting the build side `MergeScan` of a supported join inside the probe
/// side's `MergeScan`.
struct NestedBroadcastJoinRewriter<'a> {
    /// Name of the build side table, either bare (`device_limits`) or fully qualified.
    build_table: &'a str,
}

impl NestedBroadcastJoinRewriter<'_> {
    /// Returns the rewritten plan when `node` matches this slice's shape, otherwise
    /// `None` to keep the plan unchanged.
    ///
    /// A rewrite changes where the join runs, so it must preserve the join output
    /// schema, the row multiplicity, and the remote boundary identities; the guards
    /// below only admit the shape where that holds.
    fn try_rewrite_join(&self, node: &LogicalPlan) -> Option<LogicalPlan> {
        let LogicalPlan::Join(join) = node else {
            return None;
        };

        // Scope restriction of this slice: only INNER equi-joins of two distributed
        // scans are handled, everything else keeps the existing distributed plan.
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

        // The configured table must identify exactly one side, and the build side must be
        // the right input: moving a left build side would change the join schema.
        let left_is_build = table_name_of(probe_merge_scan.input())
            .is_some_and(|name| table_name_matches(&name, self.build_table));
        let right_is_build = table_name_of(build_merge_scan.input())
            .is_some_and(|name| table_name_matches(&name, self.build_table));
        if left_is_build || !right_is_build {
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
        .any(|field| is_json2_extension_type(field))
}

/// Whether the configured build side table name refers to `name`. Accepts the bare
/// table name or the fully qualified `catalog.schema.table` form, ignoring case.
fn table_name_matches(name: &TableName, configured: &str) -> bool {
    let configured = configured.trim().trim_matches(&['\'', '"'][..]);
    !configured.is_empty()
        && (name.table_name.eq_ignore_ascii_case(configured)
            || name.to_string().eq_ignore_ascii_case(configured))
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

    fn rewrite_config(build_table: &str) -> ConfigOptions {
        let mut config = ConfigOptions::default();
        config.extensions.insert(DistPlannerOptions {
            nested_broadcast_join_build_table: Some(build_table.to_string()),
            ..Default::default()
        });
        config
    }

    fn rewrite(plan: LogicalPlan, build_table: &str) -> LogicalPlan {
        DistJoinPlanner {}
            .analyze(plan, &rewrite_config(build_table))
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

    /// With the build table option the build side `MergeScan` becomes the inner scan of
    /// the outer `MergeScan`, which is routed by the probe table's regions.
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

        let result = rewrite(plan.clone(), "t2");

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

        let result = rewrite(plan, "t2");

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

        let result = rewrite(plan.clone(), "t2");

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

        let result = rewrite(projected.clone(), "t2");

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

    /// A build table matching neither side keeps the plan unchanged.
    #[test]
    fn nested_broadcast_join_rewrite_ignores_unknown_build_table() {
        let plan = distributed_join_plan();
        let result = rewrite(plan.clone(), "unknown_table");

        assert_eq!(plan.to_string(), result.to_string());
    }

    /// Only `INNER` joins are rewritten.
    #[test]
    fn nested_broadcast_join_rewrite_ignores_non_inner_join() {
        let plan = DistPlannerAnalyzer {}
            .analyze(join_plan(JoinType::Left), &ConfigOptions::default())
            .unwrap();
        let result = rewrite(plan.clone(), "t2");

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

        let result = rewrite(plan.clone(), "t2");

        assert_eq!(plan.to_string(), result.to_string());
    }

    /// Two sides resolving to the configured build table (e.g. a self join) are ambiguous.
    #[test]
    fn nested_broadcast_join_rewrite_ignores_ambiguous_build_side() {
        let plan = DistPlannerAnalyzer {}
            .analyze(
                join_plan_with_matching_side_names(),
                &ConfigOptions::default(),
            )
            .unwrap();
        let result = rewrite(plan.clone(), "t1");

        assert_eq!(plan.to_string(), result.to_string());
    }

    /// `l INNER JOIN r` where `l` and `r` read two base tables both named `t1`.
    fn join_plan_with_matching_side_names() -> LogicalPlan {
        LogicalPlanBuilder::from(table_scan("l", 1, "t1"))
            .join_on(
                table_scan("r", 2, "t1"),
                JoinType::Inner,
                vec![col("l.number").eq(col("r.number"))],
            )
            .unwrap()
            .build()
            .unwrap()
    }

    /// The rewrite stays at two levels: an already nested probe side is left alone.
    #[test]
    fn nested_broadcast_join_rewrite_ignores_more_than_two_levels() {
        let nested = rewrite(distributed_join_plan(), "t2");
        let build = DistPlannerAnalyzer {}
            .analyze(table_scan("t3", 3, "t3"), &ConfigOptions::default())
            .unwrap();
        let plan = inner_join(nested, build, col("t1.number"), col("t3.number"));

        let result = rewrite(plan.clone(), "t3");

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

        let result = rewrite(plan.clone(), "t2");
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

        let result = rewrite(plan.clone(), "t2");

        assert_eq!(plan.to_string(), result.to_string());
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
